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
import importlib.metadata
import importlib.util
import json
import os
import subprocess
import sys
import time
import urllib.parse
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

# Locating the engine repo the SRE fixer agent must work in.
#
# Pre-cutover this runner lived inside the engine repo (deploy/sre_runner.py),
# so `Path(__file__).parent.parent` was the engine root. Post-cutover `make
# deploy` copies this file to <lodging>/bin/, so that derivation now resolves
# to the LODGING root -- a YAML-only repo with no angelus package, no tests,
# and nothing to deploy. Spawning the fixer there is the exact deployment-root/
# code-root conflation that has bitten repeatedly. cwd is no help either (cron
# sets it to the lodging root).
#
# So we read the provenance pip itself recorded for the installed `angelus`
# distribution. PEP 610 direct_url.json carries the `file://` URL the package
# was installed from -- deploy.py does `pip install git+file://<repo>@<sha>`,
# so that URL IS the engine repo. The source of truth is the install that is
# actually running: it cannot drift from reality and needs no separate pinned
# path to keep in sync.


def _direct_url_repo() -> Path | None:
    """Engine repo from the installed package's PEP 610 provenance.

    deploy.py installs prod as `pip install git+file://<repo>@<sha>`, so
    direct_url.json carries `url: file://<repo>` -- the authoritative source of
    truth for a non-editable install. Absent for a legacy egg-info editable
    install (the dev tree), which the __file__ fallback below covers instead.
    """
    try:
        dist = importlib.metadata.distribution("angelus")
        raw = dist.read_text("direct_url.json")
    except (importlib.metadata.PackageNotFoundError, OSError):
        return None
    if not raw:
        return None
    try:
        url = json.loads(raw).get("url", "")
    except (json.JSONDecodeError, AttributeError):
        return None
    # A malformed direct_url must yield None (caller fails loud), never raise:
    # a non-string url breaks urlsplit, and a bad authority (e.g. "file://[")
    # raises ValueError. Either would otherwise escape and crash the tick,
    # skipping the fail-loud page.
    if not isinstance(url, str):
        return None
    try:
        parsed = urllib.parse.urlsplit(url)
    except ValueError:
        return None
    if parsed.scheme != "file" or not parsed.path:
        return None
    return Path(urllib.parse.unquote(parsed.path))


def _imported_package_repo() -> Path | None:
    """Engine repo inferred from where the `angelus` package is imported from.

    Covers an editable/dev checkout, where the package runs straight from the
    repo (``<repo>/angelus/__init__.py``) and carries no direct_url. Uses
    find_spec so the package is LOCATED, not executed. Correctly yields the
    wrong answer for a non-editable install (site-packages/angelus -> walks up
    to site-packages, which is_valid_engine_repo then rejects), so it can only
    ever ADD a valid candidate, never override the authoritative provenance.
    """
    try:
        spec = importlib.util.find_spec("angelus")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.origin:
        return None
    return Path(spec.origin).resolve().parent.parent


def resolve_engine_repo() -> Path | None:
    """The angelus engine git repo the SRE fixer agent must work in, or None
    (the caller then fails loud). Tries the installed package's pip provenance
    first (authoritative for the deployed non-editable install), then the
    imported package's own location (the editable dev tree). Returns the first
    candidate that is a real engine checkout, so a stale or codeless path is
    never handed to the agent.
    """
    for candidate in (_direct_url_repo(), _imported_package_repo()):
        if candidate is not None and is_valid_engine_repo(candidate):
            return candidate
    return None


def is_valid_engine_repo(path: Path) -> bool:
    """A usable engine repo is a git checkout carrying the angelus package and
    the Makefile -- the structural fingerprint of the engine tree, where the
    fixer agent can branch, edit, and run tests. Guards against a provenance/
    import path that points somewhere stale, codeless (the lodging root), or
    half-removed."""
    return (
        path.is_dir()
        and (path / ".git").exists()
        and (path / "angelus" / "__init__.py").is_file()
        and (path / "Makefile").is_file()
    )

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
    except OSError as exc:
        log_err(f"sre-runner: cannot read env file {path}: {exc}")
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


def report_claims_recovery(report_path: Path) -> bool:
    """True only if the SRE report's own header affirmatively states the
    service recovered AND the incident was resolved.

    build_sre_prompt mandates a fixed header the agent must write in EVERY
    outcome (including an honest 'couldn't fix it'):

        outcome: resolved | unresolved | escalated-to-human
        service-state: recovered | not-recovered | unknown

    The report's mere EXISTENCE is not proof of a fix — the prompt requires one
    even when the agent escalates with service-state: not-recovered. So a clean
    'resolved' is credited only when the content reads exactly
    `outcome: resolved` AND `service-state: recovered`. Every other shape —
    an escalated-to-human/not-recovered report, a contradiction
    (resolved+not-recovered or escalated+recovered), an unknown service-state,
    a missing or duplicated header field, junk values, or an unreadable file —
    returns False so the caller routes to the not-resolved/page path.

    Fail-loud by construction: ambiguity never credits a resolution. A false
    negative merely pages a human about a daemon that is in fact healthy
    (annoying, safe); a false positive would log a dead/honest-failure agent as
    resolved and stay silent (the bug this closes). Exact-token match on the
    lower-cased value enforces that direction — a decorated value like
    'recovered (daemon up)' does not match and pages rather than credits.
    """
    try:
        text = report_path.read_text(encoding="utf-8")
    except OSError as exc:
        log_err(f"sre-runner: cannot read SRE report {report_path}: {exc}")
        return False

    fields: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        key, sep, value = line.partition(":")
        if not sep:
            continue
        key = key.strip().lower()
        if key in ("outcome", "service-state"):
            if key in fields:
                # A duplicated header field is ambiguous — a later occurrence
                # may contradict (or merely echo) the first. Either way we
                # cannot trust the header, so fail closed regardless of value
                # agreement, per the fail-loud posture.
                return False
            fields[key] = value.strip().lower()

    return fields.get("outcome") == "resolved" and fields.get("service-state") == "recovered"


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


def query_spool_status(spool_id: str) -> str:
    """Read spindle's authoritative typed status for a spool. Returns
    'completed' or 'errored'.

    Status is read from the typed per-spool `status` field that spindle
    persists ('complete'/'error'/'running'/'pending'/'timeout'), surfaced by the
    `spindle spools` subcommand as JSON by default (a dict keyed by spool_id,
    each value carrying a `status` field). This replaces inferring status from
    a wait result's freeform text: in gather mode a failed spool and a
    successful agent whose result text begins "Error:" serialize identically,
    so the text is fundamentally ambiguous and cannot be trusted.

    Mapping: spindle 'complete' -> 'completed', 'error' -> 'errored'. Anything
    else — a non-terminal status (running/pending), the spool_id absent from
    the output, or a query/JSON failure — maps to 'errored'. That is the
    conservative direction: 'errored' can never falsely credit a resolution
    (it routes through the report-existence backstop in Step 8), whereas a
    spurious 'completed' could.
    """
    try:
        result = subprocess.run(
            ["spindle", "spools"],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        log_err(f"sre-runner: spindle spools query failed for {spool_id}: {exc}")
        return "errored"

    try:
        data = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError):
        log_err(f"sre-runner: could not parse spindle spools JSON for {spool_id}")
        return "errored"
    if not isinstance(data, dict):
        log_err(f"sre-runner: spindle spools JSON not an object for {spool_id}")
        return "errored"

    info = data.get(spool_id)
    if not isinstance(info, dict):
        log_err(f"sre-runner: spool {spool_id} absent from spindle spools output")
        return "errored"

    status = info.get("status")
    if status == "complete":
        return "completed"
    if status == "error":
        return "errored"
    # Non-terminal (running/pending) or unrecognized: the spool did not reach a
    # terminal success state we can vouch for. Treat conservatively as errored.
    log_err(
        f"sre-runner: spool {spool_id} status non-terminal/unknown: {status!r}"
    )
    return "errored"


def spindle_wait(spool_id: str, timeout: int) -> str:
    """Block until the spool finishes. Returns 'completed', 'errored', or 'timeout'.

    The bounded `spindle wait` call is used only as a BARRIER — block until the
    spool reaches a terminal state or the timeout fires. Its result payload is
    NOT trusted for status: in gather mode (the runner's mode) a failed spool
    and a successful agent whose result text begins "Error:" serialize
    identically, so the text is ambiguous. Once the barrier returns
    non-timeout, the actual completion status is read from spindle's typed
    per-spool status via query_spool_status(). The authoritative check on
    whether the agent did real work remains whether it wrote its report file,
    verified by the caller in Step 8.
    """
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
    # Spindle emits "Timeout after Ns. Spools still running: ..." on timeout —
    # the spool has NOT reached a terminal state, so report it as timeout and
    # let the caller retain the sentinel for the next tick.
    if "Timeout" in output or "still running" in output:
        return "timeout"

    # Barrier returned non-timeout: the spool reached a terminal state. Read the
    # authoritative typed status rather than parsing wait's ambiguous payload.
    return query_spool_status(spool_id)


# ---------------------------------------------------------------------------
# SRE agent prompt
# ---------------------------------------------------------------------------

def build_sre_prompt(
    sentinel_reason: str, state: Path, report_path: Path, engine_repo: Path
) -> str:
    """Construct the explicit, self-contained SRE agent prompt."""
    belfry_log = state / "belfry.log"
    fixers_log = state / "fixers.log"
    angelus_log = state / "angelus.log"
    deploys_log = state / "deploys.log"

    return (
        f"angelus's belfry watchdog escalated because the daemon is crash-looping / "
        f"would not stay up after automated restarts.\n\n"
        f"Sentinel reason verbatim: {sentinel_reason}\n\n"
        f"Context to read first (absolute paths — canonical state files, not a worktree copy):\n"
        f"- Recent tail of belfry log: {belfry_log}\n"
        f"- Recent fixer actions: {fixers_log}\n"
        f"- Errors and warnings in daemon log: {angelus_log} (grep for ERROR and WARNING lines)\n"
        f"- Recent deploys (sha + timestamps — a recent one may be the cause): {deploys_log}\n"
        f"- System design and guardrails: run `skein folio brief-20260531-q9uf`\n\n"
        f"You are an SRE acting autonomously, in an isolated git shard (worktree) of the "
        f"angelus ENGINE repo at {engine_repo}. Diagnose why the daemon will not stay running. "
        f"If you identify the root cause, write the fix and run the tests (pytest) IN YOUR "
        f"WORKTREE. You may run arbitrary commands; a classifier vets them.\n\n"
        f"YOU DO NOT DEPLOY. The sandbox is read-only outside your worktree, and angelus runs "
        f"from a non-editable install — a restart would not load your fix anyway. Your job is "
        f"to hand a human a ready fix, not to ship it:\n"
        f"- If you found and fixed the root cause: COMMIT it to your shard branch and confirm "
        f"the tests pass. Name the branch and commit sha in the report's `commits:` field. Do "
        f"NOT merge to master and do NOT run `make deploy` or `systemctl` — a human reviews "
        f"your branch and deploys it.\n"
        f"- If the fault is a bad config/env value rather than a code bug, or you cannot "
        f"confidently fix it: do NOT guess — diagnose it as precisely as you can and escalate "
        f"in the report. A precise diagnosis a human can act on beats a guessed fix.\n\n"
        f"Hard limits:\n"
        f"- Do NOT rewrite git history (no force-push, no rebase/reset of shared branches).\n"
        f"- Do NOT merge to master, run `make deploy`, or restart the daemon — applying the "
        f"fix is the human's decision after reading your report.\n"
        f"- Do NOT edit, guess, or roll back lodging config or state/angelus.env.\n"
        f"- Leave the service no worse than you found it.\n\n"
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
        append_fixers_log(
            fixers_log_path(state),
            "sre-runner",
            "blocked-lock-error",
            f"{lock_path}: {exc}",
            "escalation-blocked",
        )
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
    flog_path = fixers_log_path(state)
    try:
        sentinel_text = nsre_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        log_out("sre-runner: no needs-sre sentinel; nothing to do")
        return 0
    except OSError as exc:
        log_err(f"sre-runner: cannot read needs-sre sentinel {nsre_path}: {exc}")
        append_fixers_log(
            flog_path,
            "sre-runner",
            "blocked-sentinel-read",
            f"{nsre_path}: {exc}",
            "escalation-blocked",
        )
        return 0

    sentinel_reason = sentinel_text
    log_err(f"sre-runner: needs-sre sentinel active: {sentinel_reason}")

    last_spawn_path = sre_last_spawn_path(state)
    spawn_log = sre_spawn_log_path(state)
    now_ts = time.time()

    # Step 3a: MIN_SPAWN_INTERVAL guard
    try:
        last_ts = read_last_spawn_ts(last_spawn_path)
    except OSError as exc:
        log_err("sre-runner: cannot read last-spawn state; blocking spawn (fail-safe)")
        append_fixers_log(
            flog_path,
            "sre-runner",
            "blocked-last-spawn-read",
            f"{last_spawn_path}: {exc}",
            "escalation-blocked",
        )
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
    except OSError as exc:
        log_err("sre-runner: cannot read spawn log; blocking spawn (fail-safe)")
        append_fixers_log(
            flog_path,
            "sre-runner",
            "blocked-spawn-log-read",
            f"{spawn_log}: {exc}",
            "escalation-blocked",
        )
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

    # Precondition: locate the engine repo the fixer agent must work in.
    # Resolved from the installed package's pip provenance (see
    # resolve_engine_repo) -- neither cwd nor __file__ points at it post-cutover.
    # A miss here means the fixer would land in a wrong or codeless tree, which
    # is WORSE than not spawning: it would burn the escalation budget, thrash
    # with nothing to fix, and could write a misleading report a later tick
    # credits as resolved. Fail LOUD and leave the sentinel for a human.
    engine_repo = resolve_engine_repo()
    if engine_repo is None or not is_valid_engine_repo(engine_repo):
        msg = (
            f"sre-runner: cannot locate a valid angelus engine repo to fix in "
            f"(resolved: {engine_repo}); refusing to spawn an SRE agent into a "
            f"wrong/codeless tree. Daemon still down; needs-sre sentinel retained; "
            f"human intervention needed."
        )
        log_err(msg)
        notify_pat(msg)
        append_fixers_log(
            flog_path,
            "sre-runner",
            "blocked-no-engine-repo",
            sentinel_reason,
            "blocked-no-engine-repo",
        )
        # Rate-limit the page to the spawn interval WITHOUT consuming the
        # escalation budget: a missing engine repo is a deploy/environment
        # fault, not an escalation attempt, so it must not exhaust the
        # max-spawns cap -- but it also must not re-page every cron tick. The
        # MIN_SPAWN_INTERVAL guard reads this timestamp, so writing it here
        # suppresses re-paging for one interval. If the write fails the page
        # repeats next tick -- acceptable, since unwritable state/ is itself a
        # page-worthy fault, so the loudness degrades safe.
        write_last_spawn_ts(last_spawn_path, now_ts)
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

    prompt = build_sre_prompt(sentinel_reason, state, report_path, engine_repo)
    log_out(
        f"sre-runner: spawning SRE agent in engine repo {engine_repo}; "
        f"expected report path: {report_path}"
    )
    spool_id = spindle_spin(
        prompt, str(engine_repo), tags="angelus-sre", env=child_env
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
        # A healthy daemon is necessary but NOT sufficient to credit the SRE
        # agent. A human can recover the daemon out-of-band (manual migration
        # apply + restart) while the spool dies after read-only diagnostics —
        # exactly the 2026-06-12 incident, where a dead spool got logged as a
        # clean resolved/cleared because the post-check only looked at daemon
        # health. Credit the agent only when its spool COMPLETED, it wrote its
        # report, AND that report's own header affirmatively claims the service
        # recovered.
        #
        # report_path is the host-side path of the file the agent writes: the
        # reports dir is bind-mounted writable into the shard sandbox at the
        # same absolute path (SPINDLE_SHARD_WRITABLE_BINDS, set above the
        # spawn), so the agent's write lands on the host at report_path and the
        # runner can stat it directly.
        #
        # Report EXISTENCE is necessary but still not sufficient: the prompt
        # mandates a report in EVERY outcome, including an honest escalated-to-
        # human / service-state: not-recovered. So an agent that correctly
        # reports "couldn't fix it" while the daemon recovers out-of-band would,
        # under an existence-only check, be falsely logged as resolved
        # (issue-20260613-oe3x). Gate on the report's CONTENT: only the agent's
        # own outcome: resolved + service-state: recovered credits a clean
        # resolution. A missing/unparseable/contradictory header fails closed
        # (report_claims_recovery -> False -> page).
        report_written = report_path.is_file()
        recovery_claimed = report_written and report_claims_recovery(report_path)
        agent_verified = completion_status == "completed" and recovery_claimed

        # Clear the sentinel either way: the daemon is up, so leaving it would
        # just re-spawn an SRE agent every tick against a healthy daemon.
        try:
            nsre_path.unlink(missing_ok=True)
        except OSError as exc:
            log_err(f"sre-runner: failed to clear needs-sre sentinel: {exc}")
        clear_last_spawn_ts(last_spawn_path)

        if agent_verified:
            log_out(
                "sre-runner: post-check: daemon healthy and agent's report "
                "confirms recovery; clearing sentinel (agent-verified resolution)"
            )
            append_fixers_log(
                flog_path, "sre-runner", "resolved", sentinel_reason, "cleared",
                spool_id=spool_id,
            )
        else:
            # Daemon recovered but the agent cannot be credited: its spool
            # errored/died, it wrote no report, or its report does not claim
            # recovery (an honest escalation, a contradiction, or a malformed
            # header). Clear the sentinel (daemon is up) but log it honestly —
            # NOT a resolved/cleared line that credits an uncredited agent —
            # and page a human, because this almost always means the SRE
            # machinery failed and someone recovered the daemon by hand.
            if completion_status != "completed":
                detail = f"spool {completion_status}"
            elif not report_written:
                detail = "no report written"
            else:
                detail = "report does not confirm recovery"
            log_err(
                "sre-runner: post-check: daemon healthy but recovery is "
                f"UNVERIFIED ({detail}; completion_status={completion_status}, "
                f"report_written={str(report_written).lower()}, "
                f"recovery_claimed={str(recovery_claimed).lower()}); clearing "
                "sentinel but NOT crediting the agent"
            )
            append_fixers_log(
                flog_path,
                "sre-runner",
                "cleared-unverified",
                sentinel_reason,
                "daemon-healthy-agent-unverified",
                spool_id=spool_id,
                completion_status=completion_status,
                report_written=str(report_written).lower(),
                recovery_claimed=str(recovery_claimed).lower(),
            )
            notify_pat(
                f"angelus sre-runner: daemon recovered but WITHOUT a verified "
                f"SRE agent fix (spool {spool_id}: {detail}). The SRE machinery "
                f"likely failed and a human probably stepped in. Sentinel "
                f"cleared; please verify. Report expected: {report_path}"
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
