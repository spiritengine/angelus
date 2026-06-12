#!/usr/bin/env python3
"""Out-of-band SRE escalation runner for angelus.

Watches for the needs-sre sentinel belfry drops when its restart loop guard is
exceeded, then spawns an autonomous SRE agent via spindle to investigate and
(if possible) fix the root cause.

Designed as a SEPARATE unit from belfry: belfry stays dependency-free / pure
stdlib; agent-spawning machinery lives here.  Runs from raw cron as the user
(not root); angelus is a systemctl --user unit so no sudo is needed.
"""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Defaults (all overridable via env vars to match belfry's pattern)
# ---------------------------------------------------------------------------

DEFAULT_SRE_LOCK_FILENAME = "sre-runner.lock"
DEFAULT_SRE_LAST_SPAWN_FILENAME = "sre-last-spawn-at"
DEFAULT_SRE_SPAWN_LOG_FILENAME = "sre-spawn-log"
DEFAULT_SRE_REPORTS_DIRNAME = "sre-reports"
DEFAULT_NEEDS_SRE_FILENAME = "belfry-needs-sre"
DEFAULT_FIXERS_LOG_FILENAME = "fixers.log"
DEFAULT_ENV_FILENAME = "angelus.env"
DEFAULT_SYSTEMD_UNIT = "angelus"

# The engine repo, derived from this file's own location (deploy/sre_runner.py
# lives one level under it). In a split deployment the runner's cwd is the
# lodging root (state/ and the sentinel live there), but the SRE fixer agent
# must land in the repo the daemon's CODE comes from -- spawning it against
# the lodging root hands it a YAML-only repo with no code, no tests, and
# nothing to merge. Same deployment-root/code-root distinction as belfry's
# CODE_ROOT.
CODE_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_MIN_SPAWN_INTERVAL_SEC = 2700    # 45 min between retries on same incident
DEFAULT_MAX_SPAWNS = 3                   # hard cap in rolling window
DEFAULT_SPAWN_WINDOW_SEC = 21600         # 6 h rolling window
DEFAULT_TIMEOUT_SEC = 1800              # 30 min agent timeout


# ---------------------------------------------------------------------------
# Logging (same pattern as belfry — timestamp prefix, stdout/stderr split)
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_out(message: str) -> None:
    print(f"{_now_iso()} {message}")


def log_err(message: str) -> None:
    print(f"{_now_iso()} {message}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Env file loader (mirrors belfry's non-override load for B16 consistency)
# ---------------------------------------------------------------------------

def load_env_file(state: Path) -> None:
    """Apply state/angelus.env into os.environ, non-override."""
    path = state / DEFAULT_ENV_FILENAME
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return
    except OSError:
        return
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


# ---------------------------------------------------------------------------
# Path helpers (env-overridable, matching belfry's pattern)
# ---------------------------------------------------------------------------

def needs_sre_path(state: Path) -> Path:
    override = os.environ.get("ANGELUS_BELFRY_NEEDS_SRE_PATH")
    return Path(override) if override else state / DEFAULT_NEEDS_SRE_FILENAME


def fixers_log_path(state: Path) -> Path:
    override = os.environ.get("ANGELUS_BELFRY_FIXERS_LOG_PATH")
    return Path(override) if override else state / DEFAULT_FIXERS_LOG_FILENAME


def sre_lock_path(state: Path) -> Path:
    override = os.environ.get("ANGELUS_SRE_LOCK_PATH")
    return Path(override) if override else state / DEFAULT_SRE_LOCK_FILENAME


def sre_last_spawn_path(state: Path) -> Path:
    override = os.environ.get("ANGELUS_SRE_LAST_SPAWN_PATH")
    return Path(override) if override else state / DEFAULT_SRE_LAST_SPAWN_FILENAME


def sre_spawn_log_path(state: Path) -> Path:
    override = os.environ.get("ANGELUS_SRE_SPAWN_LOG_PATH")
    return Path(override) if override else state / DEFAULT_SRE_SPAWN_LOG_FILENAME


def sre_reports_dir(state: Path) -> Path:
    override = os.environ.get("ANGELUS_SRE_REPORTS_DIR")
    return Path(override) if override else state / DEFAULT_SRE_REPORTS_DIRNAME


def systemd_unit() -> str:
    return os.environ.get("ANGELUS_SYSTEMD_UNIT", DEFAULT_SYSTEMD_UNIT)


# ---------------------------------------------------------------------------
# Int env-var helpers (mirrors belfry's pattern: parse, warn on bad, default)
# ---------------------------------------------------------------------------

def min_spawn_interval_sec() -> int:
    # Floor at 300s (5 min) — cannot be disabled; default is 2700 (45 min).
    raw = os.environ.get("ANGELUS_SRE_MIN_INTERVAL_SEC")
    if raw is None:
        return DEFAULT_MIN_SPAWN_INTERVAL_SEC
    try:
        return max(300, int(raw))
    except ValueError:
        log_err("sre-runner: invalid ANGELUS_SRE_MIN_INTERVAL_SEC; using default")
        return DEFAULT_MIN_SPAWN_INTERVAL_SEC


def max_spawns_cfg() -> int:
    raw = os.environ.get("ANGELUS_SRE_MAX_SPAWNS")
    if raw is None:
        return DEFAULT_MAX_SPAWNS
    try:
        return max(1, int(raw))
    except ValueError:
        log_err("sre-runner: invalid ANGELUS_SRE_MAX_SPAWNS; using default")
        return DEFAULT_MAX_SPAWNS


def spawn_window_sec() -> int:
    raw = os.environ.get("ANGELUS_SRE_SPAWN_WINDOW_SEC")
    if raw is None:
        return DEFAULT_SPAWN_WINDOW_SEC
    try:
        return max(1, int(raw))
    except ValueError:
        log_err("sre-runner: invalid ANGELUS_SRE_SPAWN_WINDOW_SEC; using default")
        return DEFAULT_SPAWN_WINDOW_SEC


def timeout_sec_cfg() -> int:
    raw = os.environ.get("ANGELUS_SRE_TIMEOUT_SEC")
    if raw is None:
        return DEFAULT_TIMEOUT_SEC
    try:
        return max(60, int(raw))
    except ValueError:
        log_err("sre-runner: invalid ANGELUS_SRE_TIMEOUT_SEC; using default")
        return DEFAULT_TIMEOUT_SEC


# ---------------------------------------------------------------------------
# Guard-state helpers (fail-safe: raise OSError on unreadable so caller blocks)
# ---------------------------------------------------------------------------

def read_last_spawn_ts(path: Path) -> float | None:
    """Read last-spawn Unix timestamp. None = never; OSError -> caller blocks."""
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    # OSError (permissions, EIO, …) propagates — caller treats as fail-safe block.
    try:
        return float(raw)
    except ValueError:
        log_err(f"sre-runner: unparseable last-spawn file {raw!r}; treating as never spawned")
        return None


def write_last_spawn_ts(path: Path, ts: float) -> bool:
    """Persist last-spawn timestamp. Returns True on success, False on failure."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(ts), encoding="utf-8")
        return True
    except OSError as exc:
        log_err(f"sre-runner: failed to write last-spawn file {path}: {exc}")
        return False


def clear_last_spawn_ts(path: Path) -> None:
    """Remove per-incident last-spawn file on resolution. Swallow errors."""
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        log_err(f"sre-runner: failed to clear last-spawn file {path}: {exc}")


def read_spawn_log(path: Path) -> list[float]:
    """Read rolling spawn timestamps. Empty list if missing; OSError propagates."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    # OSError (permissions, EIO, …) propagates — caller treats as fail-safe block.
    timestamps: list[float] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            timestamps.append(float(stripped))
        except ValueError:
            log_err(f"sre-runner: unreadable spawn log line {stripped!r}; skipping")
    return timestamps


def write_spawn_log(path: Path, timestamps: list[float]) -> bool:
    """Persist spawn log. Returns True on success, False on failure."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(f"{ts}\n" for ts in timestamps), encoding="utf-8")
        return True
    except OSError as exc:
        log_err(f"sre-runner: failed to write spawn log {path}: {exc}")
        return False


# ---------------------------------------------------------------------------
# Fixers log (shared with belfry; swallow write errors — same pattern)
# ---------------------------------------------------------------------------

def append_fixers_log(
    path: Path,
    actor: str,
    action: str,
    reason: str,
    outcome: str,
    **extra: str,
) -> None:
    """Append one structured line to the shared fixers audit log."""
    parts = [
        f"{_now_iso()} actor={actor} action={action}",
        f"reason={reason!r}",
        f"outcome={outcome}",
    ]
    for k, v in extra.items():
        parts.append(f"{k}={v}")
    line = " ".join(parts) + "\n"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line)
    except OSError as exc:
        log_err(f"sre-runner: failed to append to fixers log {path}: {exc}")


# ---------------------------------------------------------------------------
# Notification
# ---------------------------------------------------------------------------

def notify_pat(message: str) -> None:
    """Push notification via notify-pat. Log on failure, never raise."""
    command = os.environ.get("ANGELUS_BELFRY_NOTIFY_COMMAND", "notify-pat")
    try:
        result = subprocess.run(
            [command, message],
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        log_err(f"sre-runner: {command} failed to start: {exc}")
        return
    if result.returncode != 0:
        log_err(f"sre-runner: {command} exited {result.returncode}")


# ---------------------------------------------------------------------------
# Daemon health post-check (no daemon imports — mirrors belfry's pid check)
# ---------------------------------------------------------------------------

def check_daemon_healthy(state: Path) -> bool:
    """True if state/angelus.pid exists and the process is alive."""
    pid_file = state / "angelus.pid"
    try:
        raw = pid_file.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return False
    except OSError:
        return False
    try:
        pid = int(raw)
    except ValueError:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # EPERM: process exists even if we can't signal it
        return True


# ---------------------------------------------------------------------------
# Spindle invocation
# ---------------------------------------------------------------------------

def spindle_spin(
    prompt: str,
    working_dir: str,
    tags: str,
    env: dict[str, str] | None = None,
) -> str | None:
    """Invoke `spindle spin --permission auto+shard`. Returns spool_id or None."""
    try:
        result = subprocess.run(
            [
                "spindle", "spin",
                "--permission", "auto+shard",
                "--working-dir", working_dir,
                "--tags", tags,
                prompt,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log_err(f"sre-runner: spindle spin failed: {exc}")
        return None
    if result.returncode != 0:
        log_err(
            f"sre-runner: spindle spin exited {result.returncode}: "
            f"{result.stderr.strip()}"
        )
        return None
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        log_err(f"sre-runner: spindle spin output not JSON: {result.stdout!r}")
        return None
    spool_id = data.get("spool_id")
    if not spool_id:
        log_err(f"sre-runner: spindle spin returned no spool_id: {data}")
        return None
    return spool_id


def spindle_wait(spool_id: str, timeout: int) -> str:
    """Block until spool completes or times out. Returns 'completed' or 'timeout'."""
    try:
        result = subprocess.run(
            ["spindle", "wait", spool_id, "--timeout", str(timeout)],
            capture_output=True,
            text=True,
            # Add 60s outer margin so we don't kill spindle before it can report timeout
            timeout=timeout + 60,
        )
    except subprocess.TimeoutExpired:
        log_err(f"sre-runner: spindle wait outer timeout for spool {spool_id}")
        return "timeout"
    except OSError as exc:
        log_err(f"sre-runner: spindle wait failed: {exc}")
        return "timeout"

    output = result.stdout.strip()
    # Spindle emits "Timeout after Ns. Spools still running: ..." on timeout
    if "Timeout" in output or "still running" in output:
        return "timeout"
    try:
        data = json.loads(output)
        # gather mode: {spool_id: result_text, ...}
        if spool_id in data:
            return "completed"
        # yield mode: {"spool_id": ..., "result": ...} or {"spool_id": ..., "error": ...}
        if "result" in data or "error" in data:
            return "completed"
    except (json.JSONDecodeError, TypeError):
        pass
    # Non-JSON output or unrecognized shape — treat as completed rather than
    # misreporting a timeout; the post-check determines what to do next.
    return "completed"


# ---------------------------------------------------------------------------
# SRE agent prompt
# ---------------------------------------------------------------------------

def build_sre_prompt(sentinel_reason: str, state: Path, report_path: Path) -> str:
    """Construct the explicit, self-contained SRE agent prompt."""
    belfry_log = state / "belfry.log"
    fixers_log = state / "fixers.log"
    angelus_log = state / "angelus.log"

    return (
        f"angelus's belfry watchdog escalated because the daemon is crash-looping / "
        f"would not stay up after automated restarts.\n\n"
        f"Sentinel reason verbatim: {sentinel_reason}\n\n"
        f"Context to read first (absolute paths — canonical state files, not a worktree copy):\n"
        f"- Recent tail of belfry log: {belfry_log}\n"
        f"- Recent fixer actions: {fixers_log}\n"
        f"- Errors and warnings in daemon log: {angelus_log} (grep for ERROR and WARNING lines)\n"
        f"- System design and guardrails: run `skein folio brief-20260531-q9uf`\n\n"
        f"You are an SRE acting autonomously. Diagnose why the angelus daemon will not stay "
        f"running. You are in an isolated git shard (worktree) — fix the root cause there: "
        f"edit code, run the tests (pytest), and if they pass, commit and merge your shard "
        f"to the main branch, then `systemctl --user restart angelus` and verify it comes "
        f"back healthy (pid alive). You may run arbitrary commands; a classifier vets them.\n\n"
        f"Hard limits:\n"
        f"- Do NOT rewrite git history (no force-push, no rebase/reset of shared branches).\n"
        f"- Do NOT auto-rollback config or auto-redeploy.\n"
        f"- Do NOT edit state/angelus.env.\n"
        f"- If the fault is a bad config/env value rather than a code bug, do NOT guess a "
        f"value — report it for a human.\n"
        f"- If you cannot confidently fix it, or the tests do not pass, or you are unsure: "
        f"do NOT merge and do NOT leave the service worse — escalate and write up what you "
        f"found. A wrong fix merged is worse than an honest escalation.\n\n"
        f"Required final action — you MUST write your report to this exact absolute path "
        f"before finishing:\n"
        f"{report_path}\n\n"
        f"The directory is bind-mounted writable in your sandbox — a plain write works. "
        f"Write the file even if you "
        f"could not fix the problem — an unresolved report is required. The file content "
        f"must be exactly this structure:\n"
        f"outcome: resolved | unresolved | escalated-to-human\n"
        f"root-cause: <one or two sentences>\n"
        f"actions-taken: <bullet list>\n"
        f"commits: <sha + branch, or none>\n"
        f"service-state: recovered | not-recovered | unknown\n"
        f"confidence: low | medium | high\n"
        f"follow-ups: <what a human should review or do next>\n\n"
        f"This file is the only record a human reads in the morning. Be accurate, do not "
        f"overstate, list what you changed so it can be reviewed or reverted."
    )


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    root = Path(argv[0] if argv else ".").resolve()
    state = root / "state"

    load_env_file(state)

    # Step 1: best-effort concurrency lock (non-blocking flock)
    lock_path = sre_lock_path(state)
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_fh = lock_path.open("a", encoding="utf-8")
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log_out("sre-runner: another tick is mid-run (lock held); exiting")
        return 0
    except OSError as exc:
        log_err(f"sre-runner: cannot acquire lock {lock_path}: {exc}")
        return 0

    try:
        return _run(state)
    finally:
        try:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)
            lock_fh.close()
        except OSError:
            pass


def _run(state: Path) -> int:
    """Main tick body (called with lock held)."""

    # Step 2: sentinel check
    nsre_path = needs_sre_path(state)
    try:
        sentinel_text = nsre_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        log_out("sre-runner: no needs-sre sentinel; nothing to do")
        return 0
    except OSError as exc:
        log_err(f"sre-runner: cannot read needs-sre sentinel {nsre_path}: {exc}")
        return 0

    sentinel_reason = sentinel_text
    log_err(f"sre-runner: needs-sre sentinel active: {sentinel_reason}")

    flog_path = fixers_log_path(state)
    last_spawn_path = sre_last_spawn_path(state)
    spawn_log = sre_spawn_log_path(state)
    now_ts = time.time()

    # Step 3a: MIN_SPAWN_INTERVAL guard
    try:
        last_ts = read_last_spawn_ts(last_spawn_path)
    except OSError:
        log_err("sre-runner: cannot read last-spawn state; blocking spawn (fail-safe)")
        return 0

    min_interval = min_spawn_interval_sec()
    if last_ts is not None:
        since = now_ts - last_ts
        if since < min_interval:
            remaining = int(min_interval - since)
            log_err(
                f"sre-runner: throttled — last spawn {int(since)}s ago "
                f"(min interval {min_interval}s, {remaining}s remaining)"
            )
            return 0

    # Step 3b: MAX_SPAWNS_PER_WINDOW guard
    try:
        all_spawn_ts = read_spawn_log(spawn_log)
    except OSError:
        log_err("sre-runner: cannot read spawn log; blocking spawn (fail-safe)")
        return 0

    n_max = max_spawns_cfg()
    window = spawn_window_sec()
    window_start = now_ts - window
    in_window = [ts for ts in all_spawn_ts if ts >= window_start]

    if len(in_window) >= n_max:
        msg = (
            f"sre-runner: escalation budget exhausted: {len(in_window)} spawn(s) "
            f"in last {window}s (max {n_max}); leaving sentinel for human; paging"
        )
        log_err(msg)
        notify_pat(
            f"angelus sre-runner: escalation budget exhausted "
            f"({len(in_window)}/{n_max} in {window}s window). "
            f"Daemon still down. Sentinel retained. Human needed."
        )
        append_fixers_log(
            flog_path,
            "sre-runner",
            "sre-exhausted",
            sentinel_reason,
            "blocked-budget-exhausted",
        )
        return 0

    # Step 4: record spawn BEFORE invoking spindle
    # (a spawn that hangs or fails still counts toward guards — same as B12)
    in_window.append(now_ts)
    if not write_spawn_log(spawn_log, in_window):
        log_err("sre-runner: cannot persist spawn log; blocking spawn (fail-safe)")
        return 0
    if not write_last_spawn_ts(last_spawn_path, now_ts):
        log_err(
            "sre-runner: cannot persist last-spawn timestamp; "
            "rolling back spawn log entry and blocking (fail-safe)"
        )
        in_window.pop()
        if not write_spawn_log(spawn_log, in_window):
            log_err("sre-runner: spawn-log rollback also failed; window cap may be off by one")
        return 0

    # Build report path (timestamp-based, generated by the runner, passed into prompt)
    report_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    # Resolve once so the path is absolute under every configuration -- a
    # relative ANGELUS_SRE_REPORTS_DIR would otherwise reach the prompt
    # unresolved, and the agent (in a shard of the engine repo, not the
    # runner's cwd) would resolve it somewhere outside the bound directory.
    reports_dir = sre_reports_dir(state).resolve()
    report_path = reports_dir / f"{report_ts}.md"

    # Directory must exist before spawn; spindle silently skips non-existent bind targets.
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Grant the shard sandbox write access to the reports directory.
    reports_abs = str(reports_dir)
    child_env = os.environ.copy()
    existing_binds = child_env.get("SPINDLE_SHARD_WRITABLE_BINDS", "")
    if existing_binds:
        child_env["SPINDLE_SHARD_WRITABLE_BINDS"] = existing_binds + ":" + reports_abs
    else:
        child_env["SPINDLE_SHARD_WRITABLE_BINDS"] = reports_abs

    prompt = build_sre_prompt(sentinel_reason, state, report_path)
    log_out(f"sre-runner: spawning SRE agent; expected report path: {report_path}")
    spool_id = spindle_spin(
        prompt, str(CODE_ROOT), tags="angelus-sre", env=child_env
    )

    if spool_id is None:
        log_err("sre-runner: spindle spin failed; spawn counted toward guards")
        append_fixers_log(
            flog_path,
            "sre-runner",
            "spawn",
            sentinel_reason,
            "spawn-failed",
            spool_id="none",
            report_path=str(report_path),
        )
        return 1

    log_out(f"sre-runner: spool {spool_id} started")

    # Step 5: wait (bounded, for post-check only — not to harvest agent output)
    t_out = timeout_sec_cfg()
    completion_status = spindle_wait(spool_id, t_out)
    log_out(f"sre-runner: spool {spool_id} completion_status={completion_status}")

    # Step 6: audit the spawn fact in fixers.log
    append_fixers_log(
        flog_path,
        "sre-runner",
        "spawn",
        sentinel_reason,
        completion_status,
        spool_id=spool_id,
        report_path=str(report_path),
    )

    # Step 8: resolution / sentinel clear
    if completion_status == "timeout":
        log_err(
            f"sre-runner: agent timed out after {t_out}s; "
            f"sentinel retained for next tick"
        )
        notify_pat(
            f"angelus sre-runner: SRE agent timed out after {t_out}s. "
            f"Spool: {spool_id}. Sentinel retained."
        )
        return 0

    healthy = check_daemon_healthy(state)
    if healthy:
        log_out(
            "sre-runner: post-check: daemon healthy; "
            "clearing sentinel and resetting per-incident spawn state"
        )
        try:
            nsre_path.unlink(missing_ok=True)
        except OSError as exc:
            log_err(f"sre-runner: failed to clear needs-sre sentinel: {exc}")
        clear_last_spawn_ts(last_spawn_path)
        append_fixers_log(
            flog_path, "sre-runner", "resolved", sentinel_reason, "cleared"
        )
    else:
        log_err(
            "sre-runner: post-check: daemon still unhealthy; "
            "sentinel retained; next tick reconsiders"
        )
        notify_pat(
            f"angelus sre-runner: SRE agent ran (spool {spool_id}) but daemon "
            f"is still down. Report: {report_path}. Sentinel retained."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
