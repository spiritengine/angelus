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

    # The checks below answer four distinct questions about angelus, in
    # descending order of "can we even read the rest": is the process
    # alive (pid), is it firing sources (wedge), is it self-reporting
    # delivery failures (failure-surface), and is the live daemon actually
    # the systemd-managed instance (drift). A dead process short-circuits
    # the latter three -- with no daemon, a stale sqlite tells us nothing
    # useful and would only manufacture confusing reasons, and there is no
    # live pid to compare against systemd. Otherwise we gather every reason
    # so a single DOWN ping names all of them.
    dead_reason = pid_failure(state / "angelus.pid")
    if dead_reason:
        reasons = [dead_reason]
    else:
        reasons = []
        wedge_reason = wedge_failure(state / "angelus.sqlite3")
        if wedge_reason:
            reasons.append(wedge_reason)
        failure_reason = failure_surface(
            state / "angelus.sqlite3", failcheck_path(state)
        )
        if failure_reason:
            reasons.append(failure_reason)
        # Drift: a daemon is alive (we are in this branch), but is it the
        # systemd-managed instance? A hand-launched daemon outside its unit
        # is the exact 2026-05-29 failure mode -- the process looked alive
        # to pid/wedge while running detached from systemd and its env.
        drift_reason = drift_failure(state / "angelus.pid")
        if drift_reason:
            reasons.append(drift_reason)

    if reasons:
        reason = "; ".join(reasons)
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
