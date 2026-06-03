#!/usr/bin/env python3
"""External reliability check for angelus.

Designed for raw cron: no angelus imports, no project dependencies.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path


DEFAULT_WEDGE_THRESHOLD_SEC = 600
DEFAULT_SENTINEL_FILENAME = "belfry-pinged-at"
DEFAULT_FAILCHECK_FILENAME = "belfry-failcheck-at"
DEFAULT_ENV_FILENAME = "angelus.env"
FAILURE_DETAIL_LIMIT = 3
DEFAULT_SYSTEMD_UNIT = "angelus"
SYSTEMCTL_TIMEOUT_SEC = 10

# B12 restart-fixer defaults.  All overridable via env vars.
DEFAULT_RESTART_LOG_FILENAME = "belfry-restart-log"
DEFAULT_NEEDS_SRE_FILENAME = "belfry-needs-sre"
DEFAULT_FIXERS_LOG_FILENAME = "fixers.log"
# At most 3 restarts in a 30-minute window before escalating to the SRE tier.
DEFAULT_MAX_RESTARTS = 3
DEFAULT_RESTART_WINDOW_SEC = 1800
# Wait this long after `systemctl restart` before checking whether the daemon
# came back.  Small enough to not stall the cron tick; big enough for a normal
# startup sequence.
DEFAULT_RECOVER_WAIT_SEC = 3


def _now_iso() -> str:
    """ISO8601 UTC timestamp for log-line prefixes.

    Plain strftime keeps belfry dependency-free (no angelus clock seam, no
    third-party libs). Wall-clock is correct here: these are operator-facing
    log lines, not values fed back into angelus' time logic.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_out(message: str) -> None:
    """Write one timestamped line to stdout (belfry.log via cron redirect)."""
    print(f"{_now_iso()} {message}")


def log_err(message: str) -> None:
    """Write one timestamped line to stderr (belfry.log via cron redirect)."""
    print(f"{_now_iso()} {message}", file=sys.stderr)


def load_env_file(state: Path) -> dict[str, str]:
    """Apply state/angelus.env into os.environ, non-override (B16).

    The same non-secret config the daemon loads, so belfry and the daemon can't
    diverge. Stdlib-only to keep belfry dependency-free. The belfry crontab
    sources this file too (for PATH); loading it here as well means a belfry
    launched any other way still sees the config. Non-override: a name already
    in the environment wins over the file. Missing file is a no-op.
    """
    path = state / DEFAULT_ENV_FILENAME
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    applied: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export ") or line.startswith("export\t"):
            line = line[len("export"):].lstrip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ[key] = value
        applied[key] = value
    return applied


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    root = Path(argv[0] if argv else ".").resolve()
    state = root / "state"

    # Load non-secret config before any env var is read (B16). Keeps belfry's
    # view of ANGELUS_EMAIL_TO / healthcheck URLs / thresholds identical to the
    # daemon's even when cron didn't source the file.
    load_env_file(state)

    # Touch the liveness sentinel on every tick, success or failure. The
    # question this answers is "is belfry firing on schedule?" -- belfry's
    # OWN liveness, separate from whatever angelus health the rest of the
    # tick discovers. A tick that finds angelus down still proves belfry
    # itself is alive and on cron, so the touch must precede the
    # angelus-health checks below and run on every code path. The angelus
    # daemon reads this file's mtime in _op_health; see Section 5b Q2 of
    # brief-20260520-tqov for the sentinel-file-over-sqlite-table rationale
    # (single-writer-to-sqlite invariant preserved by leaving sqlite
    # untouched here).
    touch_sentinel(sentinel_path(state))

    # Five checks, split by what they imply for autoremediation:
    #
    # ABSENCE reasons (daemon not running / not delivering):
    #   pid_failure  -- dead process: restart is the right tool.
    #   wedge_failure -- alive but not firing sources: also absence.
    #
    # OTHER reasons (daemon IS alive; restart is the wrong tool):
    #   failure_surface -- daemon self-reporting live errors: alerting only,
    #       never restart (would mask the root cause and interrupt delivery).
    #   drift_failure -- alive but mis-launched outside the systemd unit:
    #       alerting only (a systemctl restart while a hand-launched instance
    #       holds the pid is messy; that's a human/SRE fix).
    #   stale_deployment -- alive but running code older than the latest
    #       commit: alerting only (a restart is a deliberate deploy action;
    #       auto-restarting could load half-merged code mid-edit).
    #
    # A dead process short-circuits the live-daemon checks: with no daemon a
    # stale sqlite tells us nothing useful, and there is no live pid to
    # compare against systemd.  Otherwise gather every reason so a single
    # DOWN ping names all of them.
    dead_reason = pid_failure(state / "angelus.pid")
    if dead_reason:
        absence_reasons: list[str] = [dead_reason]
        other_reasons: list[str] = []
    else:
        absence_reasons = []
        other_reasons = []
        wedge_reason = wedge_failure(state / "angelus.sqlite3")
        if wedge_reason:
            absence_reasons.append(wedge_reason)
        failure_reason = failure_surface(
            state / "angelus.sqlite3", failcheck_path(state)
        )
        if failure_reason:
            other_reasons.append(failure_reason)
        drift_reason = drift_failure(state / "angelus.pid")
        if drift_reason:
            other_reasons.append(drift_reason)
        stale_reason = stale_deployment(state / "angelus.pid", root)
        if stale_reason:
            other_reasons.append(stale_reason)
        # Delivery SLA: a pipe alive-but-not-delivering. Alert-only (OTHER),
        # never restart -- a stalled pipe is a product/logic failure, and the
        # wedge check already covers a source-firing wedge; restarting here
        # would mask the cause and is the wrong tool per the autoremediation
        # rule (restart only for absence). The exact 2026-05-29 shape: nothing
        # errored, the pipe just silently stopped delivering.
        sla_reason = sla_failure(state / "angelus.sqlite3")
        if sla_reason:
            other_reasons.append(sla_reason)

    all_reasons = absence_reasons + other_reasons
    if all_reasons:
        if absence_reasons:
            # Daemon is absent: attempt a loop-guarded restart unless a drift
            # reason is also present.  Drift means the daemon is alive but
            # launched outside its systemd unit — `systemctl restart` would
            # spawn a second instance alongside the mis-launched one, violating
            # the single-writer invariant.  Drift is always alert-only.
            has_drift = any(r.startswith("drift:") for r in other_reasons)
            if has_drift:
                action_note = (
                    "wedged but drifted — restart withheld, drift is a human/SRE fix"
                )
            else:
                # Attempt the loop-guarded restart.  The action note is appended
                # to the reason string so the DOWN ping and notify() both carry
                # "what we did and what happened."  A successful restart is still
                # alert-worthy (a real problem occurred); the next clean tick
                # pings SUCCESS as normal.
                action_note = _autoremediate_absence(state, absence_reasons)
            reason = "; ".join(all_reasons) + "; " + action_note
        else:
            reason = "; ".join(all_reasons)
        ok = ping_env("ANGELUS_BELFRY_DOWN_URL")
        ok = notify(reason) and ok
        log_err(f"angelus belfry: DOWN: {reason}")
        return 1 if ok else 2

    ok = ping_env("ANGELUS_BELFRY_SUCCESS_URL")
    log_out("angelus belfry: ok")
    return 0 if ok else 2


def sentinel_path(state_dir: Path) -> Path:
    """Resolve the belfry liveness sentinel path.

    ANGELUS_BELFRY_SENTINEL_PATH overrides; default is
    <state_dir>/belfry-pinged-at. The same env var and default are read
    by the angelus daemon in _op_health so both sides stay in sync.
    """
    override = os.environ.get("ANGELUS_BELFRY_SENTINEL_PATH")
    if override:
        return Path(override)
    return state_dir / DEFAULT_SENTINEL_FILENAME


def touch_sentinel(path: Path) -> None:
    """Update the sentinel file's mtime to now, creating it if missing.

    Failures are logged to stderr and swallowed: the sentinel is a
    liveness signal, not a correctness boundary -- a transient EIO must
    not stop belfry from issuing its angelus-health pings on this tick.
    A missing sentinel is itself the "never pinged" signal the daemon
    surfaces, so failing closed (no touch) degrades into the existing
    daemon-side handling.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
        now = time.time()
        os.utime(path, (now, now))
    except OSError as exc:
        log_err(f"angelus belfry: failed to touch sentinel {path}: {exc}")


def failcheck_path(state_dir: Path) -> Path:
    """Resolve the belfry failure-surface watermark path.

    ANGELUS_BELFRY_FAILCHECK_PATH overrides; default is
    <state_dir>/belfry-failcheck-at. This is a sibling of the liveness
    sentinel and follows the same single-file, belfry-owned state pattern
    -- belfry never writes the sqlite (the daemon's single-writer
    invariant), so its "what did I already see" bookmark lives outside the
    database.
    """
    override = os.environ.get("ANGELUS_BELFRY_FAILCHECK_PATH")
    if override:
        return Path(override)
    return state_dir / DEFAULT_FAILCHECK_FILENAME


def restart_log_path(state_dir: Path) -> Path:
    """Path for the rolling restart-timestamp log (loop-guard state).

    ANGELUS_BELFRY_RESTART_PATH overrides; default is
    <state_dir>/belfry-restart-log.  Follows the same single-file,
    belfry-owned pattern as the liveness sentinel and failcheck watermark.
    """
    override = os.environ.get("ANGELUS_BELFRY_RESTART_PATH")
    if override:
        return Path(override)
    return state_dir / DEFAULT_RESTART_LOG_FILENAME


def needs_sre_path(state_dir: Path) -> Path:
    """Path for the needs-sre escalation sentinel.

    ANGELUS_BELFRY_NEEDS_SRE_PATH overrides; default is
    <state_dir>/belfry-needs-sre.  Written when the loop guard blocks a
    restart so an out-of-band SRE runner can consume it.
    """
    override = os.environ.get("ANGELUS_BELFRY_NEEDS_SRE_PATH")
    if override:
        return Path(override)
    return state_dir / DEFAULT_NEEDS_SRE_FILENAME


def fixers_log_path(state_dir: Path) -> Path:
    """Path for the shared fixers audit log.

    ANGELUS_BELFRY_FIXERS_LOG_PATH overrides; default is
    <state_dir>/fixers.log.  Both belfry and (future) in-daemon fixers
    append here so 'belfry restarted twice at 3am' reads back in one place.
    """
    override = os.environ.get("ANGELUS_BELFRY_FIXERS_LOG_PATH")
    if override:
        return Path(override)
    return state_dir / DEFAULT_FIXERS_LOG_FILENAME


def max_restarts() -> int:
    """Maximum restart attempts permitted in the rolling window."""
    raw = os.environ.get("ANGELUS_BELFRY_MAX_RESTARTS")
    if raw is None:
        return DEFAULT_MAX_RESTARTS
    try:
        n = int(raw)
    except ValueError:
        log_err(
            "angelus belfry: invalid ANGELUS_BELFRY_MAX_RESTARTS; "
            "using default"
        )
        return DEFAULT_MAX_RESTARTS
    return max(1, n)


def restart_window_sec() -> int:
    """Rolling window length (seconds) for the restart loop guard."""
    raw = os.environ.get("ANGELUS_BELFRY_RESTART_WINDOW_SEC")
    if raw is None:
        return DEFAULT_RESTART_WINDOW_SEC
    try:
        secs = int(raw)
    except ValueError:
        log_err(
            "angelus belfry: invalid ANGELUS_BELFRY_RESTART_WINDOW_SEC; "
            "using default"
        )
        return DEFAULT_RESTART_WINDOW_SEC
    return max(1, secs)


def recover_wait_sec() -> int:
    """Seconds to wait after systemctl restart before verifying recovery."""
    raw = os.environ.get("ANGELUS_BELFRY_RECOVER_WAIT_SEC")
    if raw is None:
        return DEFAULT_RECOVER_WAIT_SEC
    try:
        secs = int(raw)
    except ValueError:
        log_err(
            "angelus belfry: invalid ANGELUS_BELFRY_RECOVER_WAIT_SEC; "
            "using default"
        )
        return DEFAULT_RECOVER_WAIT_SEC
    return max(0, secs)


def read_failcheck_watermark(path: Path) -> int | None:
    """Read the last-seen dispatch id from the watermark file.

    Returns None when the file is missing or unparseable -- both mean
    "no trustworthy bookmark," which the caller treats as a first run
    (establish the watermark, do NOT replay history).
    """
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError as exc:
        log_err(
            f"angelus belfry: failed to read failcheck watermark {path}: {exc}"
        )
        return None
    try:
        return int(raw)
    except ValueError:
        log_err(
            f"angelus belfry: invalid failcheck watermark {raw!r}; "
            f"treating as first run"
        )
        return None


def write_failcheck_watermark(path: Path, dispatch_id: int) -> None:
    """Persist the highest dispatch id seen this tick.

    Failures are logged and swallowed: a watermark that fails to advance
    only means the next tick re-reports the same already-pinged failures
    (noisy, not wrong), so a transient write error must not stop belfry
    from issuing its pings on this tick.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(dispatch_id), encoding="utf-8")
    except OSError as exc:
        log_err(
            f"angelus belfry: failed to write failcheck watermark {path}: {exc}"
        )


def read_restart_log(path: Path) -> list[float]:
    """Read restart timestamps (Unix epoch floats) from the restart log.

    Returns an empty list when the file is missing or unparseable — both
    mean "no recorded restarts," which the caller treats as a clean slate.
    Unparseable lines are skipped with a log; a single bad line must not
    discard the entire history.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except OSError as exc:
        log_err(f"angelus belfry: failed to read restart log {path}: {exc}")
        return []
    timestamps: list[float] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            timestamps.append(float(stripped))
        except ValueError:
            log_err(
                f"angelus belfry: unreadable restart log line {stripped!r}; "
                f"skipping"
            )
    return timestamps


def write_restart_log(path: Path, timestamps: list[float]) -> bool:
    """Persist the current in-window restart timestamps.

    Returns True on success, False on failure.  A failed write means the
    guard state cannot be durably recorded; the caller MUST NOT restart on
    this tick (fail safe, not fail open) — an un-guarded restart loop is
    worse than a withheld restart.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(f"{ts}\n" for ts in timestamps), encoding="utf-8"
        )
        return True
    except OSError as exc:
        log_err(f"angelus belfry: failed to write restart log {path}: {exc}")
        return False


def restart_daemon() -> bool:
    """Shell `systemctl --user restart <unit>`.

    Returns True on a zero exit (restart dispatched to systemd), False on
    any error (OSError, timeout, nonzero exit).  Never raises: belfry must
    not crash because systemctl is absent or the user bus is unreachable.
    Keeping this as its own function makes it mockable in tests.
    """
    unit = systemd_unit()
    try:
        result = subprocess.run(
            ["systemctl", "--user", "restart", unit],
            check=False,
            capture_output=True,
            text=True,
            timeout=SYSTEMCTL_TIMEOUT_SEC,
            env=_user_bus_env(),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log_err(f"angelus belfry: restart failed (systemctl unavailable): {exc}")
        return False
    if result.returncode != 0:
        detail = result.stderr.strip() or f"exit {result.returncode}"
        log_err(f"angelus belfry: restart systemctl failed: {detail}")
        return False
    log_out(f"angelus belfry: systemctl --user restart {unit} dispatched")
    return True


def verify_recovery(pid_file: Path) -> bool:
    """Wait briefly, then re-check whether the daemon is alive.

    Returns True if pid_failure finds the daemon alive after the wait,
    False otherwise.  The wait is bounded and configurable; it is small
    enough that it does not materially stall the cron tick.
    """
    wait = recover_wait_sec()
    if wait > 0:
        time.sleep(wait)
    return pid_failure(pid_file) is None


def write_needs_sre_sentinel(path: Path, reason: str) -> None:
    """Write the needs-sre sentinel so an out-of-band SRE runner can consume it.

    Overwrites any previous content (a fresh crash-loop entry supersedes an
    older one).  Failures are logged and swallowed.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{_now_iso()} {reason}\n", encoding="utf-8")
    except OSError as exc:
        log_err(
            f"angelus belfry: failed to write needs-sre sentinel {path}: {exc}"
        )


def append_fixers_log(
    path: Path, actor: str, action: str, reason: str, outcome: str
) -> None:
    """Append one structured line to the shared fixers audit log.

    Format: ISO8601 timestamp, actor=belfry, action=restart|escalate,
    reason (quoted), outcome.  Append-only; creates the file if missing.
    Failures are logged and swallowed — the audit log must never crash belfry.
    """
    line = (
        f"{_now_iso()} actor={actor} action={action} "
        f"reason={reason!r} outcome={outcome}\n"
    )
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line)
    except OSError as exc:
        log_err(
            f"angelus belfry: failed to append to fixers log {path}: {exc}"
        )


def _autoremediate_absence(state: Path, absence_reasons: list[str]) -> str:
    """Attempt a restart when the daemon is absent (dead or wedged).

    This is the loop-guarded restart path.  It reads the rolling restart
    log, prunes timestamps outside the window, and either:
      - escalates (loop guard exceeded): writes the needs-sre sentinel,
        appends to fixers.log, returns a loud escalation note.
      - restarts: records this attempt, shells systemctl restart, verifies
        recovery, appends to fixers.log, returns an action note.

    The restart is recorded BEFORE the systemctl call so even a hung or
    partially-completed restart counts toward the window (prevents an
    infinite loop of broken restarts from bypassing the guard).

    A successful auto-restart is still alert-worthy: the caller always
    pings ANGELUS_BELFRY_DOWN_URL and notify() regardless of outcome.
    """
    rlog_path = restart_log_path(state)
    flog_path = fixers_log_path(state)
    nsre_path = needs_sre_path(state)
    n_max = max_restarts()
    window = restart_window_sec()
    now_ts = time.time()
    reason_str = "; ".join(absence_reasons)

    # Prune entries outside the rolling window before counting.
    all_timestamps = read_restart_log(rlog_path)
    window_start = now_ts - window
    in_window = [ts for ts in all_timestamps if ts >= window_start]

    if len(in_window) >= n_max:
        # Loop guard triggered: stop restarting, escalate.
        escalation = (
            f"crash-loop: {len(in_window)} restart(s) in last {window}s "
            f"(max {n_max}); needs-sre sentinel written; human needed"
        )
        write_needs_sre_sentinel(nsre_path, f"{escalation}: {reason_str}")
        append_fixers_log(flog_path, "belfry", "escalate", reason_str, "blocked")
        log_err(f"angelus belfry: loop guard exceeded — {escalation}")
        return escalation

    # Record this attempt before the systemctl call (see docstring).
    # If the persist fails, withhold the restart: the guard cannot track
    # this tick, so restarting now would leave the count unrecorded and
    # allow an unlimited restart loop on the next tick (fail safe > fail open).
    in_window.append(now_ts)
    persisted = write_restart_log(rlog_path, in_window)
    if not persisted:
        guard_fail_note = (
            "restart withheld: loop-guard could not persist state "
            "(disk/permissions error on restart log); human attention needed"
        )
        append_fixers_log(
            flog_path, "belfry", "guard_fail", reason_str, "withheld"
        )
        log_err(f"angelus belfry: {guard_fail_note}")
        return guard_fail_note

    restarted = restart_daemon()
    if restarted:
        recovered = verify_recovery(state / "angelus.pid")
        outcome = "recovered" if recovered else "not_recovered"
    else:
        outcome = "restart_failed"

    append_fixers_log(flog_path, "belfry", "restart", reason_str, outcome)
    log_out(f"angelus belfry: restart attempt: outcome={outcome}")
    return f"auto-restart attempted; outcome={outcome}"


def failure_surface(db_path: Path, state_path: Path) -> str | None:
    """Surface angelus's OWN self-reported failures, generically.

    Two signals, read straight from the schema the daemon already writes,
    with no mention of any specific channel or transport:
      - dispatches.status='failed' rows recorded since the last belfry
        tick (EDGE-triggered via a last-seen dispatch-id watermark, so a
        transient failure pings DOWN once and is not re-alerted forever);
      - open internal/* incidents -- the daemon's self-reported failures,
        whose source begins 'internal/' (LEVEL-triggered on current
        state, so belfry stays red every tick until the incident closes).

    Returns a human-readable reason if anything is surfaced, else None.

    Fails open: any sqlite/OS error is logged and swallowed (returns
    None). The pid/wedge checks remain the liveness backstop; an
    unreadable or schema-incomplete database must never manufacture a
    false DOWN here.
    """
    last_seen = read_failcheck_watermark(state_path)
    quoted = urllib.parse.quote(str(db_path), safe="/:")
    uri = f"file:{quoted}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        log_err(
            f"angelus belfry: failure-surface cannot open {db_path}: {exc}"
        )
        return None
    try:
        max_id = int(
            connection.execute(
                "SELECT COALESCE(max(id), 0) FROM dispatches"
            ).fetchone()[0]
        )
        failed_rows: list[tuple] = []
        if last_seen is not None:
            failed_rows = connection.execute(
                "SELECT id, pipe, channel, last_error FROM dispatches "
                "WHERE status = 'failed' AND id > ? ORDER BY id",
                (last_seen,),
            ).fetchall()
        internal_rows = connection.execute(
            "SELECT source FROM incidents "
            "WHERE status = 'open' AND source LIKE 'internal/%' "
            "ORDER BY opened_at, id"
        ).fetchall()
    except sqlite3.Error as exc:
        log_err(f"angelus belfry: failure-surface query failed: {exc}")
        return None
    finally:
        connection.close()

    # Advance the watermark to the highest id observed even when we are
    # about to report DOWN: those failed dispatches have now been seen, so
    # the next tick reports only newer ones (edge-trigger). Open internal
    # incidents are intentionally NOT bookmarked -- they re-fire each tick
    # until closed.
    write_failcheck_watermark(state_path, max_id)

    reasons: list[str] = []
    if failed_rows:
        details = []
        for _id, pipe, channel, last_error in failed_rows[:FAILURE_DETAIL_LIMIT]:
            detail = f"{pipe}/{channel}"
            if last_error:
                detail += f": {last_error}"
            details.append(detail)
        msg = f"{len(failed_rows)} failed dispatch(es) since last tick"
        if details:
            msg += f" ({'; '.join(details)})"
        reasons.append(msg)
    if internal_rows:
        sources = sorted({row[0] for row in internal_rows})
        reasons.append(
            f"{len(internal_rows)} open internal finding(s) [{', '.join(sources)}]"
        )
    if reasons:
        return "; ".join(reasons)
    return None


def _parse_iso(value: str) -> datetime | None:
    """Parse one of the catalog's ISO8601-Z timestamps to an aware UTC
    datetime. Stdlib only. Returns None on anything unparseable so the SLA
    check can fail open per-pipe rather than manufacture a false DOWN."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def sla_failure(db_path: Path, now: datetime | None = None) -> str | None:
    """Assert each pipe with a declared delivery SLA is delivering on cadence.

    The on-box, all-pipes generalization of the off-box digest dead-man: the
    daemon persists each pipe's expected max interval into the `pipe_sla` table
    (belfry cannot parse the pipes/*.yaml itself -- pure stdlib, no angelus
    imports), and here belfry reads it read-only alongside the last SUCCESSFUL
    (status='sent') dispatch per pipe and pings DOWN if the window lapsed.

    Baseline for "overdue" is max(last successful dispatch, tracking_since), so
    a pipe that has never delivered gets a full window of grace from when it
    was first registered rather than alerting immediately on deploy.

    LEVEL-triggered: re-reports every tick until a delivery resets the window
    (no watermark), mirroring the open-internal-incident signal. Fails open:
    any sqlite/parse error -- including a pre-migration db with no pipe_sla
    table -- is swallowed (returns None); the pid/wedge checks remain the
    liveness backstop and an SLA read must never manufacture a false DOWN.
    """
    now = now or datetime.now(UTC)
    quoted = urllib.parse.quote(str(db_path), safe="/:")
    uri = f"file:{quoted}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as exc:
        log_err(f"angelus belfry: sla cannot open {db_path}: {exc}")
        return None
    try:
        try:
            sla_rows = connection.execute(
                "SELECT pipe_name, max_interval_seconds, tracking_since "
                "FROM pipe_sla"
            ).fetchall()
        except sqlite3.Error:
            # No pipe_sla table yet (db predates the B2 migration): nothing to
            # assert. Fail open.
            return None
        if not sla_rows:
            return None
        last_sent = dict(
            connection.execute(
                "SELECT pipe, max(dispatched_at) FROM dispatches "
                "WHERE status = 'sent' AND dispatched_at IS NOT NULL "
                "GROUP BY pipe"
            ).fetchall()
        )
    except sqlite3.Error as exc:
        log_err(f"angelus belfry: sla query failed: {exc}")
        return None
    finally:
        connection.close()

    overdue: list[str] = []
    for pipe_name, max_seconds, tracking_since in sla_rows:
        delivered = last_sent.get(pipe_name)
        baseline = _parse_iso(delivered or tracking_since)
        if baseline is None:
            continue  # unparseable timestamp -> fail open for this pipe
        age_sec = (now - baseline).total_seconds()
        if age_sec <= max_seconds:
            continue
        age_hr = age_sec / 3600.0
        max_hr = max_seconds / 3600.0
        if delivered is None:
            detail = f"no successful delivery in {age_hr:.1f}h"
        else:
            detail = f"last delivery {age_hr:.1f}h ago"
        overdue.append(f"{pipe_name} overdue: {detail} (max {max_hr:.0f}h)")
    if overdue:
        return "; ".join(overdue)
    return None


def systemd_unit() -> str:
    """The systemd unit name belfry asserts the daemon belongs to.

    ANGELUS_SYSTEMD_UNIT overrides the default 'angelus' (the unit installed
    from deploy/angelus.service), so a differently-named deployment can still
    use the drift check.
    """
    return os.environ.get("ANGELUS_SYSTEMD_UNIT", DEFAULT_SYSTEMD_UNIT)


def _user_bus_env() -> dict[str, str]:
    """Environment for `systemctl --user` so it can reach the user bus.

    A stock crontab sets neither XDG_RUNTIME_DIR nor DBUS_SESSION_BUS_ADDRESS,
    and without one of them `systemctl --user` cannot connect to the user bus
    -- the drift check would then fail open to a silent no-op (the exact
    failure class this effort exists to kill). When both are unset, point
    XDG_RUNTIME_DIR at the invoking user's runtime dir. Never overwrite an
    existing bus var (an explicit value wins). Stdlib only; fail-open is
    unchanged -- if the bus is still unreachable, systemd_main_pid returns None.
    """
    env = dict(os.environ)
    if not env.get("XDG_RUNTIME_DIR") and not env.get("DBUS_SESSION_BUS_ADDRESS"):
        env["XDG_RUNTIME_DIR"] = f"/run/user/{os.getuid()}"
    return env


def systemd_main_pid() -> int | None:
    """Return the MainPID systemd attributes to the angelus unit.

    Returns 0 when the unit is known to systemd but currently inactive
    (systemd's own sentinel for "no main process"). Returns None when we
    cannot determine the answer at all -- systemctl missing, no user bus,
    a non-zero exit, an unparseable value, or a timeout. None is the
    fail-open signal: belfry must never manufacture a DOWN ping just
    because it could not interrogate systemd, so drift_failure treats None
    as "no drift detectable."

    Shells out to `systemctl --user show -p MainPID --value <unit>`; belfry
    stays dependency-free (no angelus imports, no dbus library).
    """
    unit = systemd_unit()
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", "-p", "MainPID", "--value", unit],
            check=False,
            capture_output=True,
            text=True,
            timeout=SYSTEMCTL_TIMEOUT_SEC,
            env=_user_bus_env(),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        # FileNotFoundError (no systemctl), TimeoutExpired, etc. -> fail open.
        log_err(f"angelus belfry: drift check cannot run systemctl: {exc}")
        return None
    if result.returncode != 0:
        # e.g. no user bus / unknown unit on some systemd versions.
        detail = result.stderr.strip() or f"exit {result.returncode}"
        log_err(f"angelus belfry: drift check systemctl failed: {detail}")
        return None
    raw = result.stdout.strip()
    try:
        return int(raw)
    except ValueError:
        log_err(f"angelus belfry: drift check got unparseable MainPID {raw!r}")
        return None


def drift_failure(pid_file: Path) -> str | None:
    """Assert the live daemon IS the systemd-managed instance.

    Called only when a daemon is alive (pid_failure already returned None),
    so the live pid comes straight from the pid file. Compares it to the
    MainPID systemd reports for the unit:

      - systemd MainPID == live pid  -> managed instance, no drift.
      - systemd MainPID == 0 (unit inactive) while a daemon pid is alive
        -> a daemon is running OUTSIDE its unit (someone ran it by hand).
      - systemd MainPID != live pid  -> the alive daemon is not the one
        systemd is supervising; another instance drifted off the unit.

    Fails open: if the live pid is unreadable, or systemd's MainPID cannot
    be determined (systemctl unavailable), returns None. The pid/wedge/
    failure-surface checks remain the liveness backstop; an inability to
    interrogate systemd must never be reported as a false DOWN.
    """
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        # pid_failure owns the dead/missing-pid case; nothing to compare.
        return None

    main_pid = systemd_main_pid()
    if main_pid is None:
        return None
    if main_pid == pid:
        return None
    if main_pid == 0:
        return (
            f"drift: daemon PID {pid} is alive but systemd unit "
            f"'{systemd_unit()}' is inactive (MainPID 0); daemon running "
            f"outside its unit"
        )
    return (
        f"drift: daemon PID {pid} does not match systemd unit "
        f"'{systemd_unit()}' MainPID {main_pid}; an instance is running "
        f"outside its unit"
    )


def last_code_commit_epoch(root: Path) -> float | None:
    """Epoch of the most recent commit to the daemon's runtime package, or None.

    Uses git rather than working-tree mtimes deliberately: an editable
    install runs the files on disk, so flagging on every uncommitted edit
    would nag constantly during development. A commit (and the merge that
    follows) is the deployment event that matters -- it is what landed
    fixer_actions on 2026-05-31 while the daemon kept running pre-merge
    code. Fails open (None) outside a git repo or if git is unavailable, so
    an un-interrogable repo never reports a false DOWN.

    The pathspec is scoped to ``angelus/`` -- the package the daemon imports
    -- NOT a repo-wide ``*.py``. A commit touching only tests/, belfry/, or
    docs/ does not change what the running daemon executes, so it must not
    flip this check to DOWN and nag every tick. Belfry itself runs fresh
    from cron each tick, so belfry.py is never "stale" the way the
    long-lived daemon can be.

    Caveat: %ct is the committer date, used as a proxy for "landing time."
    This repo's shard workflow re-stamps commits at merge (committer date ~=
    landing time), so the proxy holds. A future move to fast-forward merges
    of older-authored commits could carry a stale %ct and under-report (a
    false negative) -- but never a false positive, since that needs a
    future-dated commit. If merge policy changes, revisit this.
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "log", "-1", "--format=%ct", "--", "angelus"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return None
        return float(result.stdout.strip())
    except Exception as exc:
        # Fail open on anything -- a belfry tick must never crash on the
        # stale check, or it would suppress every other health ping. This is
        # the same "never a false DOWN / never abort the tick" contract the
        # other checks honor; the stale check is the lowest-stakes of them.
        # Covers git being absent, a non-repo root, a timeout, and unparseable
        # output (empty/garbled %ct -> ValueError).
        log_err(f"angelus belfry: stale-deploy git failed: {exc}")
        return None


def _starttime_ticks_from_proc_stat(stat_line: str) -> int:
    """Field 22 (starttime, in clock ticks) from a /proc/<pid>/stat line.

    comm (field 2) is wrapped in parens and may itself contain spaces or
    parens, so we split on the text after the FINAL ')'. There, token index
    19 is field 22 (field N -> index N-3, since field 3 -- state -- lands at
    index 0). Pulled out so the parse can be unit-tested against a comm with
    embedded spaces/parens, which a live process named `python3` cannot
    exercise.
    """
    after_comm = stat_line.rpartition(")")[2].split()
    return int(after_comm[19])


def process_start_epoch(pid: int) -> float | None:
    """Wall-clock start time of PID from /proc/<pid>/stat, or None.

    Field 22 (starttime) is in clock ticks since boot; combined with btime
    from /proc/stat it yields an epoch. Linux-only and fail-open, consistent
    with belfry's other checks -- a kernel that does not expose /proc never
    reports a false DOWN.
    """
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        starttime_ticks = _starttime_ticks_from_proc_stat(stat)
        clk_tck = os.sysconf("SC_CLK_TCK")
        if clk_tck <= 0:
            return None
        btime: int | None = None
        for line in Path("/proc/stat").read_text(encoding="utf-8").splitlines():
            if line.startswith("btime "):
                btime = int(line.split()[1])
                break
        if btime is None:
            return None
        return btime + starttime_ticks / clk_tck
    except (OSError, ValueError, IndexError) as exc:
        log_err(f"angelus belfry: stale-deploy /proc read failed: {exc}")
        return None


def stale_deployment(pid_file: Path, root: Path) -> str | None:
    """Flag when committed code is newer than the running daemon.

    Called only when a daemon is alive (pid_failure already returned None).
    Python imports each module once at startup and an editable install does
    NOT hot-reload, so a daemon that started before code landed is executing
    stale code. This is exactly the 2026-06-01 failure: fixer_actions merged
    2026-05-31 but the daemon, up since before the merge, kept rejecting
    daily.yaml every tick.

    Alert-only (an OTHER reason, like drift): a restart deploys current code,
    but that is a deliberate human/SRE action -- auto-restarting could load
    half-merged code mid-edit. Fails open on any unreadable signal.
    """
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return None
    code_at = last_code_commit_epoch(root)
    start_at = process_start_epoch(pid)
    if code_at is None or start_at is None:
        return None
    if code_at <= start_at:
        return None
    code_iso = datetime.fromtimestamp(code_at).strftime("%Y-%m-%d %H:%M")
    start_iso = datetime.fromtimestamp(start_at).strftime("%Y-%m-%d %H:%M")
    return (
        f"stale-deploy: daemon PID {pid} started {start_iso} but Python code "
        f"was last committed {code_iso}; restart to load current code"
    )


def pid_failure(pid_file: Path) -> str | None:
    try:
        raw = pid_file.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return f"dead: missing PID file {pid_file}"
    try:
        pid = int(raw)
    except ValueError:
        return f"dead: invalid PID file contents {raw!r}"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return f"dead: PID {pid} is not running"
    except PermissionError:
        # EPERM still implies the process exists even if cron cannot signal it.
        log_err(
            f"angelus belfry: cannot confirm PID {pid} via os.kill(0): "
            f"permission denied"
        )
        return None
    return None


def wedge_failure(db_path: Path) -> str | None:
    threshold = wedge_threshold()
    try:
        fired_at = latest_fire(db_path)
    except sqlite3.Error as exc:
        return f"wedged: cannot read source_fires from {db_path}: {exc}"

    if fired_at is None:
        return "wedged: source_fires has no rows"
    age = datetime.now(UTC) - fired_at
    if age > threshold:
        return (
            f"wedged: last source fire at {fired_at.isoformat()} "
            f"({int(age.total_seconds())}s ago)"
        )
    return None


def wedge_threshold() -> timedelta:
    raw = os.environ.get("ANGELUS_BELFRY_WEDGE_THRESHOLD_SEC")
    if raw is None:
        return timedelta(seconds=DEFAULT_WEDGE_THRESHOLD_SEC)
    try:
        seconds = int(raw)
    except ValueError:
        log_err(
            "angelus belfry: invalid ANGELUS_BELFRY_WEDGE_THRESHOLD_SEC; "
            "using default"
        )
        seconds = DEFAULT_WEDGE_THRESHOLD_SEC
    return timedelta(seconds=max(1, seconds))


def latest_fire(db_path: Path) -> datetime | None:
    quoted = urllib.parse.quote(str(db_path), safe="/:")
    uri = f"file:{quoted}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        row = connection.execute("SELECT max(fired_at) FROM source_fires").fetchone()
    finally:
        connection.close()
    value = row[0] if row else None
    if value is None:
        return None
    return parse_utc(str(value))


def parse_utc(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def ping_env(name: str) -> bool:
    url = os.environ.get(name)
    if not url:
        log_err(f"angelus belfry: {name} is not set; skipping ping")
        return False
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            status = getattr(response, "status", 200)
        log_out(f"angelus belfry: pinged {name} status={status}")
        return 200 <= int(status) < 300
    except Exception as exc:
        log_err(f"angelus belfry: failed to ping {name}: {exc}")
        return False


def notify(reason: str) -> bool:
    message = f"angelus belfry alert: {reason}"
    command = os.environ.get("ANGELUS_BELFRY_NOTIFY_COMMAND", "notify-pat")
    try:
        result = subprocess.run(
            [command, message],
            check=False,
        )
    except OSError as exc:
        log_err(f"angelus belfry: {command} failed to start: {exc}")
        return False
    if result.returncode != 0:
        log_err(f"angelus belfry: {command} exited {result.returncode}")
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
