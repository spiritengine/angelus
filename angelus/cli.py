"""Command-line entry point for Angelus.

Read commands talk to the running daemon over its control socket
(state/angelus.sock). If the daemon is down or unreachable, health,
incident-list and mute-list fall back to reading sqlite in true read-only
mode (file:...?mode=ro) -- the CLI can never write the database, preserving
the single-writer invariant. "The daemon is down" is a successful health
report, not a CLI error: the fallback path exits 0.

Write commands (mute add / incident close / replay / reprocess) are the
inverse:
they MUST go through the daemon (the single sqlite writer) and have NO
sqlite fallback -- a fallback would reintroduce a second writer. A
missing/refused/garbled socket is a hard, non-zero exit with a clear
message.

Output is operator-facing and read aloud by a screen reader: plain text, one
item per line, "label: value" and simple indented lists. No tables, columns,
or box-drawing.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sqlite3
import urllib.parse
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import click
import yaml

from angelus.clock import SYSTEM_CLOCK
from angelus.daemon import HEALTH_FAILED_DISPATCH_WINDOW_HOURS
from angelus.daemon import main as daemon_main
from angelus.lodging.config import _load_dependencies
from angelus.sources import run_dep_check
from angelus.storage import Catalog

_ROOT_OPTION = click.option(
    "--root",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("."),
    show_default=True,
    help="Angelus root directory (where state/ lives).",
)

_SOCKET_TIMEOUT = 5.0

# Window duration units for `timeline --window`, mirroring the mute-duration
# parser in daemon.py: a unit suffix is required so a bare integer can never
# silently mean "some unit".
_WINDOW_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

# Cap the response we will buffer from the daemon. The daemon caps inbound
# requests at control.MAX_REQUEST_BYTES; this is the symmetric client-side
# bound so a buggy or compromised daemon cannot make the CLI buffer without
# limit. Generous relative to any legitimate health/incident-list response;
# exceeding it is treated as a garbled response (fall back to read-only sqlite).
_MAX_RESPONSE_BYTES = 1024 * 1024


def _socket_path(root: Path) -> Path:
    return root / "state" / "angelus.sock"


def _request(root: Path, op: str, args: dict[str, Any]) -> dict[str, Any] | None:
    """Send one op over the control socket and return the parsed response.

    Returns None if the socket is absent, the connection is refused, or the
    daemon does not return a well-formed JSON line (e.g. killed mid-write,
    leaving a truncated or empty buffer) -- the signal for callers to fall
    back to read-only sqlite. The contract is: any failure to get a complete,
    parseable response is "daemon unreachable", which is exit-0 success with
    the sqlite fallback, never a CLI traceback.
    """
    sock_path = _socket_path(root)
    if not sock_path.exists():
        return None
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(_SOCKET_TIMEOUT)
            conn.connect(str(sock_path))
            conn.sendall((json.dumps({"op": op, "args": args}) + "\n").encode("utf-8"))
            buffer = b""
            while not buffer.endswith(b"\n"):
                chunk = conn.recv(65536)
                if not chunk:
                    break
                buffer += chunk
                if len(buffer) > _MAX_RESPONSE_BYTES:
                    # Oversized/never-terminated response: treat as garbled,
                    # same as a daemon that died mid-write.
                    return None
    except (ConnectionError, FileNotFoundError, OSError):
        return None
    if not buffer:
        return None
    try:
        return json.loads(buffer.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Truncated/garbled buffer: daemon died mid-write. Treat exactly like
        # connection-refused -- fall through to the read-only sqlite path.
        return None


def _ro_connect(db_path: Path) -> sqlite3.Connection | None:
    """Open the catalog database read-only. Returns None if it does not
    exist. The mode=ro URI makes writes impossible: any attempted write
    raises sqlite3.OperationalError. This is what keeps the CLI from ever
    becoming a second sqlite writer."""
    if not db_path.exists():
        return None
    quoted = urllib.parse.quote(str(db_path), safe="/:")
    connection = sqlite3.connect(f"file:{quoted}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _pid_status(pid_file: Path) -> tuple[str, int | None]:
    """Classify the daemon when the socket is unreachable.

    not running  -> no pid file, or pid file present but process gone (stale)
    not reachable -> process alive but the control socket is not answering
    """
    try:
        raw = pid_file.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return "not running", None
    try:
        pid = int(raw)
    except ValueError:
        return "not running", None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return "not running", pid
    except PermissionError:
        # Process exists, we just cannot signal it.
        return "not reachable", pid
    return "not reachable", pid


def _require_daemon(
    root: Path, op: str, args: dict[str, Any], command: str
) -> dict[str, Any]:
    """Send a write op that REQUIRES the daemon and return its result.

    The inverse of the health/incident-list read path: write commands
    have NO read-only sqlite fallback -- a fallback would make the CLI a
    second sqlite writer and break the single-writer invariant. So a
    missing/refused/garbled socket (any _request -> None) is a hard,
    non-zero exit with a clear message, and a structured {"ok": false}
    from the daemon is surfaced verbatim and also exits non-zero.
    """
    response = _request(root, op, args)
    if response is None:
        click.echo(
            f"angelus daemon is not running; {command} requires the daemon",
            err=True,
        )
        raise SystemExit(1)
    if not response.get("ok"):
        click.echo(f"error: {response.get('error')}", err=True)
        raise SystemExit(1)
    return response["result"]


@click.group()
def main() -> None:
    """Angelus scheduling and escalation spine."""


@main.command()
@_ROOT_OPTION
def daemon(root: Path) -> None:
    """Start the Angelus daemon."""
    daemon_main(root.resolve())


@main.command()
@_ROOT_OPTION
def health(root: Path) -> None:
    """Daemon status, sources, queue depths."""
    root = root.resolve()
    response = _request(root, "health", {})
    if response is None:
        _render_health_fallback(root)
        return
    if not response.get("ok"):
        click.echo(f"error: {response.get('error')}", err=True)
        raise SystemExit(1)
    _render_health(response["result"])


@main.group()
def incident() -> None:
    """Inspect incidents."""


@incident.command("list")
@_ROOT_OPTION
def incident_list(root: Path) -> None:
    """List open and recently-closed incidents."""
    root = root.resolve()
    response = _request(root, "incident_list", {})
    if response is not None:
        if not response.get("ok"):
            click.echo(f"error: {response.get('error')}", err=True)
            raise SystemExit(1)
        _render_incidents(response["result"])
        return
    _render_incidents_fallback(root)


@incident.command("close")
@click.argument("incident_id", type=int)
@click.option("--comment", default=None, help="Closure note.")
@_ROOT_OPTION
def incident_close(incident_id: int, comment: str | None, root: Path) -> None:
    """Explicitly close an incident (requires the daemon)."""
    root = root.resolve()
    result = _require_daemon(
        root,
        "incident_close",
        {"id": incident_id, "comment": comment},
        "incident close",
    )
    outcome = result["outcome"]
    if outcome == "closed":
        click.echo(f"incident {incident_id} closed")
    elif outcome == "already_closed":
        # Desired end state already reached -> exit 0 (idempotent).
        click.echo(f"incident {incident_id} was already closed")
    else:
        click.echo(f"no incident with id {incident_id}", err=True)
        raise SystemExit(1)


@main.group()
def mute() -> None:
    """Mute findings, or list active mutes."""


@mute.command("add")
@click.argument("dedup_key")
@click.argument("duration")
@click.option("--comment", default=None, help="Why this is muted.")
@_ROOT_OPTION
def mute_add(
    dedup_key: str, duration: str, comment: str | None, root: Path
) -> None:
    """Mute findings with DEDUP_KEY for DURATION (e.g. 30m, 4h, 2d).

    Requires the daemon. DURATION is <int><unit> with unit s/m/h/d; a
    bare integer is rejected.
    """
    root = root.resolve()
    result = _require_daemon(
        root,
        "mute",
        {"dedup_key": dedup_key, "duration": duration, "comment": comment},
        "mute add",
    )
    click.echo(f"muted {result['dedup_key']} until {result['expires_at']}")


@mute.command("list")
@_ROOT_OPTION
def mute_list(root: Path) -> None:
    """List active mutes (read-only; the daemon is optional).

    Symmetric with `incident list`: goes through the control socket
    but falls back to a read-only sqlite read when the daemon is down,
    since listing in-effect mutes is safe without the writer.
    """
    root = root.resolve()
    response = _request(root, "mute_list", {})
    if response is not None:
        if not response.get("ok"):
            click.echo(f"error: {response.get('error')}", err=True)
            raise SystemExit(1)
        _render_mutes(response["result"])
        return
    _render_mutes_fallback(root)


@main.command()
@click.argument("finding_id", type=int)
@_ROOT_OPTION
def replay(finding_id: int, root: Path) -> None:
    """Re-dispatch a finding to its target pipes (requires the daemon)."""
    root = root.resolve()
    result = _require_daemon(
        root, "replay", {"finding_id": finding_id}, "replay"
    )
    outcome = result["outcome"]
    if outcome == "requeued":
        click.echo(
            f"finding {finding_id} re-queued to {','.join(result['pipes'])}"
        )
    elif outcome == "already_queued":
        # Already queued for every target pipe -> exit 0 (idempotent).
        click.echo(f"finding {finding_id} already queued, no action")
    else:
        click.echo(f"no finding with id {finding_id}", err=True)
        raise SystemExit(1)


@main.command()
@click.argument("source")
@_ROOT_OPTION
def reprocess(source: str, root: Path) -> None:
    """Re-run triage for a source's observations (requires the daemon)."""
    root = root.resolve()
    result = _require_daemon(
        root, "reprocess", {"source": source}, "reprocess"
    )
    count = result["observations"]
    if count:
        click.echo(
            f"reprocess: {count} observations from {source} "
            "will be re-triaged"
        )
    else:
        # Empty is a valid end state, not an error -> exit 0.
        click.echo(f"reprocess: no observations found for source {source}")


@main.command("dep-record")
@click.argument("name")
@click.argument("status", type=click.Choice(["healthy", "unhealthy"]))
@click.option("--detail", default=None, help="Probe output or error.")
@_ROOT_OPTION
def dep_record(
    name: str, status: str, detail: str | None, root: Path
) -> None:
    """Record a dependency health result (requires the daemon).

    A WRITE: it goes through the daemon (the single sqlite writer) over
    the control socket, never opens sqlite, and has no read-only
    fallback. Normally invoked by `dep-check`; exposed directly for
    testing and manual override.
    """
    root = root.resolve()
    result = _require_daemon(
        root,
        "dep_record",
        {"name": name, "status": status, "detail": detail},
        "dep-record",
    )
    click.echo(f"recorded {result['name']}: {result['status']}")


@main.command("dep-check")
@click.argument("name")
@_ROOT_OPTION
def dep_check(name: str, root: Path) -> None:
    """Run a dependency's check and report it (requires the daemon).

    The cron-run probe. It loads only dependencies/<name>.yaml (not the
    rest of the lodging -- a dep check must not couple to unrelated
    lodging or to daemon APScheduler liveness), runs the check command
    with the shared kill-on-timeout subprocess helper, then sends the
    result as a dep_record over the control socket. It never opens
    sqlite; the daemon performs the write. A missing/refused socket is a
    hard non-zero exit (the cron tick fails loudly; the next tick
    retries) -- the same daemon-required contract as the other writes.
    """
    root = root.resolve()
    dependencies = _load_dependencies(root)
    dependency = dependencies.get(name)
    if dependency is None:
        click.echo(
            f"no enabled dependency lodged as {name!r} "
            f"(looked in {root / 'dependencies'})",
            err=True,
        )
        raise SystemExit(1)
    status, detail = asyncio.run(run_dep_check(dependency))
    result = _require_daemon(
        root,
        "dep_record",
        {"name": name, "status": status, "detail": detail},
        "dep-check",
    )
    click.echo(f"{result['name']}: {result['status']} ({detail})")


@main.command("fault-inject")
@click.argument("channel", required=False)
@click.option(
    "--clear", "clear_channel", default=None,
    help="Clear the fault armed on this channel.",
)
@click.option("--clear-all", is_flag=True, help="Clear all armed faults.")
@click.option("--list", "list_faults", is_flag=True, help="List armed faults.")
@_ROOT_OPTION
def fault_inject(
    channel: str | None,
    clear_channel: str | None,
    clear_all: bool,
    list_faults: bool,
    root: Path,
) -> None:
    """Force CHANNEL's dispatch to fail on demand (B28).

    An armed channel's next send raises a transport-shaped failure, so the
    real detection/failover/escalation machinery runs without touching channel
    config. Faults are in-memory on the daemon -- a restart clears them, and
    every action (including --list) requires the daemon, since there is no
    persisted state to fall back to.

      angelus fault-inject email             arm a fault on email
      angelus fault-inject --clear email     clear the fault on email
      angelus fault-inject --clear-all       clear every armed fault
      angelus fault-inject --list            list armed faults
    """
    root = root.resolve()
    # Exactly one mode per invocation -- arm (a bare CHANNEL), --clear, --clear-all,
    # or --list. Reject combinations up front so a typo cannot silently arm and
    # list in one ambiguous call.
    modes = [bool(channel), bool(clear_channel), clear_all, list_faults]
    if sum(modes) != 1:
        click.echo(
            "fault-inject takes exactly one of: a channel to arm, "
            "--clear <channel>, --clear-all, or --list",
            err=True,
        )
        raise SystemExit(1)

    if list_faults:
        result = _require_daemon(root, "fault_inject", {"action": "list"}, "fault-inject")
        _render_armed(result)
        return
    if clear_all:
        result = _require_daemon(
            root, "fault_inject", {"action": "clear_all"}, "fault-inject"
        )
        click.echo("cleared all faults")
        _render_armed(result)
        return
    if clear_channel:
        result = _require_daemon(
            root,
            "fault_inject",
            {"action": "clear", "channel": clear_channel},
            "fault-inject",
        )
        click.echo(f"cleared fault: {clear_channel}")
        _render_armed(result)
        return
    result = _require_daemon(
        root, "fault_inject", {"action": "arm", "channel": channel}, "fault-inject"
    )
    click.echo(f"armed fault: {channel}")
    _render_armed(result)


@main.command()
@click.argument("pipe")
@_ROOT_OPTION
def drain(pipe: str, root: Path) -> None:
    """Run PIPE's drain now and report the dispatch summary (B25).

    Forces a named pipe's drain on demand instead of waiting for its cron
    cadence -- a daily digest, the immediate `now` pipe, any kind. A WRITE: it
    goes through the daemon (the single sqlite writer, and the only process
    that can actually send), so it REQUIRES the daemon and has no read-only
    fallback. The summary counts channel send ATTEMPTS this drain: `dispatched`
    succeeded, `failed` raised (skips -- a muted finding, an already-unhealthy
    channel, a rate-limit overflow -- are neither).
    """
    root = root.resolve()
    result = _require_daemon(root, "drain", {"pipe": pipe}, "drain")
    click.echo(f"pipe: {result['pipe']}")
    click.echo(f"dispatched: {result['dispatched']}")
    click.echo(f"failed: {result['failed']}")


@main.command("fire-source")
@click.argument("name")
@_ROOT_OPTION
def fire_source(name: str, root: Path) -> None:
    """Run source NAME's check once now and report the observation (B25).

    Forces a source's scheduled check on demand instead of waiting for its
    cadence. Under observation collapse it produces an observation ONLY if the
    source's state changed since its last check; an unchanged fire writes no
    observation (it just bumps the last-checked heartbeat), and the command
    reports that as "no change". A WRITE: it goes through the daemon (the single
    sqlite writer, and the only process that runs scheduled checks), so it
    REQUIRES the daemon and has no read-only fallback. `outcome` is `ok` on a
    clean check or `check_failed` on a non-zero/timeout/bad-payload check.
    """
    root = root.resolve()
    result = _require_daemon(root, "fire_source", {"name": name}, "fire-source")
    click.echo(f"source: {result['source']}")
    observation_id = result["observation_id"]
    if observation_id is None:
        click.echo("observation: none (state unchanged, collapsed)")
    else:
        click.echo(f"observation: {observation_id}")
    click.echo(f"outcome: {result['outcome']}")


def _parse_sim_instant(value: Any) -> datetime:
    """Parse a sim script's ISO8601 instant (with a trailing 'Z' or offset).
    A naive value is read as UTC, matching FakeClock's own convention."""
    if not isinstance(value, str) or not value:
        raise click.ClickException("sim: 'start'/'set_time' must be an ISO8601 string")
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise click.ClickException(f"sim: bad instant {value!r}: {exc}") from exc
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _parse_sim_duration(value: Any) -> timedelta:
    """Parse an 'advance' step's duration -- an integer with a unit suffix
    (s/m/h/d), reusing the same units as `timeline --window` so a bare number
    can never silently mean 'some unit'."""
    if not isinstance(value, str) or len(value) < 2 or value[-1] not in _WINDOW_UNITS:
        raise click.ClickException(
            "sim: 'advance' must be <int><unit>, unit one of s/m/h/d (e.g. 1d)"
        )
    try:
        amount = int(value[:-1])
    except ValueError as exc:
        raise click.ClickException(f"sim: bad duration {value!r}: {exc}") from exc
    return timedelta(seconds=amount * _WINDOW_UNITS[value[-1]])


def _sim_step(step: Any) -> tuple[str, Any]:
    """Normalise one script step to (verb, arg). A step is a single-key mapping
    (``{fire_source: scheduled/watch}``) or a bare verb string for the no-arg
    steps (``run_triage``)."""
    if isinstance(step, str):
        return step, None
    if isinstance(step, dict) and len(step) == 1:
        (verb, arg), = step.items()
        return str(verb), arg
    raise click.ClickException(f"sim: malformed step {step!r}")


async def _run_sim(harness: Any, steps: list[Any]) -> list[str]:
    """Drive the harness through the scripted steps, returning a plain-text
    report (one value per line, screen-reader friendly). Each step reuses the
    matching production path via the harness; this only narrates the cycle."""
    report: list[str] = []
    for step in steps:
        verb, arg = _sim_step(step)
        if verb == "set_time":
            instant = _parse_sim_instant(arg)
            harness.set_time(instant)
            report.append(f"set_time: now {_format_instant(instant)}")
        elif verb == "advance":
            harness.advance(_parse_sim_duration(arg))
            report.append(f"advance {arg}: now {harness.clock.now_iso()}")
        elif verb == "fire_source":
            observation_id, outcome = await harness.fire_source(str(arg))
            if observation_id is None:
                report.append(
                    f"fire_source {arg}: no change (collapsed) outcome {outcome}"
                )
            else:
                report.append(
                    f"fire_source {arg}: observation {observation_id} outcome {outcome}"
                )
        elif verb == "inject":
            if not isinstance(arg, dict) or "source_ref" not in arg:
                raise click.ClickException(
                    "sim: 'inject' needs a mapping with source_ref (and payload)"
                )
            observation_id = harness.inject_observation(
                str(arg["source_ref"]),
                arg.get("payload") or {},
                arg.get("meta"),
            )
            report.append(
                f"inject {arg['source_ref']}: observation {observation_id}"
            )
        elif verb == "run_triage":
            triaged = await harness.run_triage()
            report.append(f"run_triage: {triaged} observations triaged")
        elif verb == "drain":
            summary = await harness.drain(str(arg))
            report.append(
                f"drain {arg}: dispatched {summary.dispatched} "
                f"failed {summary.failed}"
            )
        else:
            raise click.ClickException(f"sim: unknown step verb {verb!r}")
    dispatched = harness.dispatches()
    report.append(f"dispatches: {len(dispatched)}")
    for line in dispatched:
        report.append(f"  {line}")
    return report


@main.command()
@click.argument(
    "script", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@_ROOT_OPTION
def sim(script: Path, root: Path) -> None:
    """Replay a scripted source -> dispatch cycle offline (B26).

    Runs the production pipeline -- fire a source / inject an observation,
    triage, drain a pipe -- against the lodging at --root with NO cron and NO
    real waiting: time is pinned to the script's `start` and moved only by
    `advance`/`set_time` steps, so a simulated day passes in seconds. Sends are
    forced to dry-run, landing in `dispatches.log` instead of paging a phone, so
    this is safe to run against a scratch copy of a lodging.

    SCRIPT is a YAML (or JSON) step list:

    \b
        start: "2026-06-06T12:00:00Z"
        steps:
          - fire_source: scheduled/watch
          - run_triage
          - advance: 1d
          - drain: now
          - drain: daily

    Step verbs: set_time (ISO instant), advance (<int>s/m/h/d), fire_source
    (source name), inject ({source_ref, payload, meta}), run_triage, drain (pipe
    name). The report prints what each step produced, one value per line.
    """
    from angelus.sim import SimHarness

    root = root.resolve()
    try:
        document = yaml.safe_load(script.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        # A syntactically broken script is operator error, not a crash: turn the
        # raw parser traceback into a clean, non-zero ClickException line.
        raise click.ClickException(f"sim: invalid YAML: {exc}") from exc
    if not isinstance(document, dict) or "start" not in document:
        raise click.ClickException(
            "sim: script must be a mapping with a 'start' instant and 'steps'"
        )
    start = _parse_sim_instant(document["start"])
    steps = document.get("steps") or []
    if not isinstance(steps, list):
        raise click.ClickException("sim: 'steps' must be a list")

    click.echo(f"sim start: {_format_instant(start)}")
    with SimHarness(root, start) as harness:
        report = asyncio.run(_run_sim(harness, steps))
    for line in report:
        click.echo(line)
    click.echo("sim complete")


def _render_armed(result: dict[str, Any]) -> None:
    """Armed faults, one channel per line (screen-reader plain text)."""
    armed = result.get("armed") or []
    click.echo("armed faults:")
    if not armed:
        click.echo("  none")
        return
    for name in armed:
        click.echo(f"  {name}")


def _format_instant(value: datetime) -> str:
    """Render a datetime in the same '...Z' millisecond format the storage
    layer writes, so window bounds compare lexicographically against the
    stored timestamps."""
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _parse_window_seconds(window: str) -> int:
    """Parse a window like '90s', '30m', '24h', '2d' into seconds.

    A unit suffix (s/m/h/d) is required and the magnitude must be positive,
    matching the mute-duration footgun guard: `--window 24` is rejected so it
    cannot silently mean 24 of some unit."""
    text = window.strip().lower()
    for suffix, scale in _WINDOW_UNITS.items():
        if text.endswith(suffix) and len(text) > len(suffix):
            magnitude_text = text[: -len(suffix)].strip()
            try:
                magnitude = int(magnitude_text)
            except ValueError:
                raise click.BadParameter(
                    f"invalid window {window!r}: expected <int><unit> (s, m, h, d)"
                ) from None
            if magnitude <= 0:
                raise click.BadParameter(
                    f"invalid window {window!r}: must be positive"
                )
            return magnitude * scale
    raise click.BadParameter(
        f"invalid window {window!r}: expected a unit suffix (s, m, h, d), "
        "e.g. '24h'"
    )


def _parse_instant(value: str) -> datetime:
    """Parse a user-supplied --since/--until bound into a UTC datetime.

    Accepts a bare date ('2026-05-29'), a date+time, or a full ISO timestamp
    with or without a trailing 'Z'. A value with no timezone is assumed UTC."""
    text = value.strip()
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        raise click.BadParameter(
            f"invalid timestamp {value!r}: expected a date (2026-05-29) or "
            "ISO timestamp (2026-05-29T12:00:00Z)"
        ) from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


@main.command()
@click.option("--since", default=None, help="Window start (date or ISO timestamp).")
@click.option("--until", default=None, help="Window end (date or ISO timestamp).")
@click.option(
    "--window",
    default=None,
    help="Look back this far from --until/now (e.g. 24h, 2d). "
    "Mutually exclusive with --since.",
)
@_ROOT_OPTION
def timeline(
    since: str | None, until: str | None, window: str | None, root: Path
) -> None:
    """Reconstruct the ordered story for a time window.

    Interleaves source fires, observations, findings, and dispatches
    (including failures) by timestamp, one event per line, for fast
    postmortems. Reads sqlite read-only; the daemon does not need to be
    running.

    The window is [since, until]. --until defaults to now. The start is
    --since if given, else --until minus --window, else 24h before --until.
    """
    root = root.resolve()
    if since is not None and window is not None:
        raise click.BadParameter("pass --since or --window, not both")

    until_dt = _parse_instant(until) if until is not None else SYSTEM_CLOCK.now()
    if since is not None:
        since_dt = _parse_instant(since)
    else:
        seconds = _parse_window_seconds(window) if window is not None else 86400
        since_dt = until_dt - timedelta(seconds=seconds)
    if since_dt > until_dt:
        raise click.BadParameter("--since is after --until")

    since_str = _format_instant(since_dt)
    until_str = _format_instant(until_dt)

    connection = _ro_connect(root / "state" / "angelus.sqlite3")
    if connection is None:
        click.echo("sqlite: unavailable")
        raise SystemExit(1)
    try:
        catalog = Catalog(connection, root)
        events = catalog.timeline_events(since_str, until_str)
    finally:
        connection.close()
    _render_timeline(since_str, until_str, events)


def _render_timeline(
    since: str, until: str, events: list[dict[str, Any]]
) -> None:
    """Plain text, one event per line; screen-reader friendly (no tables,
    no columns). Each line is '<timestamp> <kind> <details>'."""
    click.echo(f"timeline from {since} to {until}")
    click.echo(f"events: {len(events)}")
    if not events:
        click.echo("  none")
        return
    for event in events:
        click.echo(_format_timeline_event(event))


def _format_timeline_event(event: dict[str, Any]) -> str:
    ts = event["ts"]
    kind = event["kind"]
    # No "fire" kind: observation collapse means a fire only surfaces as the
    # observation it writes on a state change (timeline_events no longer emits
    # per-tick fire events). The heartbeat lives in watch_state, read by health.
    if kind == "observation":
        return f"{ts} observation {event['source']} ({event['status']})"
    if kind == "finding":
        severity = event.get("severity") or "none"
        return (
            f"{ts} finding {event['source']} {event['type']} "
            f"{event['entity']} (severity {severity})"
        )
    # dispatch
    line = f"{ts} dispatch {event['pipe']}/{event['channel']} {event['status']}"
    if event.get("error"):
        line += f": {event['error']}"
    return line


def _render_health(result: dict[str, Any]) -> None:
    daemon_info = result["daemon"]
    click.echo("daemon: running")
    click.echo(f"pid: {daemon_info['pid']}")
    click.echo("sources:")
    if not result["sources"]:
        click.echo("  none")
    for source in result["sources"]:
        click.echo(f"  {source['name']}")
        click.echo(f"    last fire: {source['last_fire_at'] or 'never'}")
        click.echo(f"    next fire: {source['next_fire_at'] or 'unknown'}")
        blocked_by = source.get("blocked_by_unhealthy_deps") or []
        if blocked_by:
            click.echo(f"    blocked by: {', '.join(blocked_by)}")
    queues = result["queues"]
    click.echo(
        f"observations pending triage: {queues['observations_pending_triage']}"
    )
    click.echo("findings pending dispatch:")
    pending = queues["findings_pending_dispatch"]
    if not pending:
        click.echo("  none")
    for pipe in sorted(pending):
        click.echo(f"  {pipe}: {pending[pipe]}")
    _render_delivery(result.get("delivery") or {})
    _render_belfry(result["belfry"])
    _render_deps(result.get("deps") or [])
    _render_channels(result.get("channels") or {})
    _render_fault_injection(result.get("fault_injection") or {})


def _render_delivery(delivery: dict[str, Any]) -> None:
    """Delivery surface (B5): plain text, one item per line, screen-reader
    friendly (no tables/columns). Answers "is it WORKING", not just running."""
    click.echo("delivery:")
    if not delivery:
        click.echo("  unavailable")
        return
    last_sent = delivery.get("last_successful_send") or {}
    click.echo("  last successful send:")
    if not last_sent:
        click.echo("    none")
    for pipe in sorted(last_sent):
        click.echo(f"    {pipe}: {last_sent[pipe] or 'never'}")
    failed = delivery.get("failed_dispatches") or {}
    # Default the window so a partial dict never renders "(last Noneh)". The
    # daemon's _delivery_surface always populates window_hours; this only
    # guards a hand-built/old-shape dict.
    window = failed.get("window_hours", HEALTH_FAILED_DISPATCH_WINDOW_HOURS)
    count = failed.get("count", 0)
    click.echo(f"  failed dispatches (last {window}h): {count}")
    click.echo(
        f"  open internal incidents: {delivery.get('open_internal_incidents', 0)}"
    )
    _render_dead_letter(delivery.get("dead_letter") or {})


def _render_dead_letter(dead_letter: dict[str, Any]) -> None:
    """Dead-letter section of the delivery surface (B15): findings whose
    redelivery ladder exhausted undelivered now sit in the terminal
    'dead_letter' state. Surfaces WHAT was abandoned and how to recover it
    (`angelus replay <finding_id>`) so an exhausted dispatch is loud and
    replayable, not silently pending -- the 2026-05-29 anti-pattern.

    Plain text, one item per line, screen-reader friendly (no tables/columns):
    a human-readable description first, the finding id last, matching the rest
    of the delivery surface. The count is the true total; the item list is
    capped upstream (HEALTH_DEAD_LETTER_DISPLAY_LIMIT), so a count exceeding the
    rendered rows tells the operator more are dead-lettered than shown.
    """
    count = dead_letter.get("count", 0)
    click.echo(f"  dead-letter (exhausted, replayable): {count}")
    for item in dead_letter.get("items") or []:
        entity = item.get("entity") or "?"
        ftype = item.get("type") or "?"
        pipe = item.get("pipe") or "?"
        last_error = item.get("last_error") or "no error recorded"
        finding_id = item.get("finding_id")
        # Description first, then the actionable replay id last -- a screen
        # reader reads the id at the end, and `angelus replay <id>` is the
        # recovery the line points the operator to.
        click.echo(
            f"    {entity} {ftype} on {pipe}, last error: {last_error}; "
            f"replay finding {finding_id}"
        )


def _render_belfry(belfry: dict[str, Any] | None) -> None:
    """Plain text, two lines (timestamp and freshness). Screen-reader
    friendly: no tables, no embedded markup, one fact per line. The
    daemon now always returns a dict shape; bare-None is the legacy
    surface from pre-slice-8 daemons and is rendered explicitly so a
    stale binary still produces a readable line."""
    if belfry is None:
        click.echo("last belfry ping: not recorded")
        return
    last = belfry.get("last_pinged_at") or "never"
    click.echo(f"last belfry ping: {last}")
    click.echo(f"belfry stale: {'yes' if belfry.get('stale') else 'no'}")


def _render_deps(deps: list[dict[str, Any]]) -> None:
    """Plain text, one dependency per line; detail only when unhealthy.
    Screen-reader friendly: no tables or columns."""
    click.echo("dependencies:")
    if not deps:
        click.echo("  none")
    for dep in deps:
        click.echo(
            f"  {dep['dependency_name']}: {dep['status']} "
            f"(last check {dep['last_check_at']})"
        )
        if dep["status"] == "unhealthy" and dep.get("detail"):
            click.echo(f"    detail: {dep['detail']}")
        mute = dep.get("mute")
        if dep["status"] == "unhealthy" and mute:
            comment = f" ({mute['comment']})" if mute.get("comment") else ""
            click.echo(f"    muted until: {mute['until']}{comment}")


def _deps_with_active_mutes(catalog: Catalog) -> list[dict[str, Any]]:
    """Dep health rows enriched with the effective active mute, if any."""
    deps = catalog.all_dep_health()
    for dep in deps:
        if dep["status"] != "unhealthy":
            continue
        mute = catalog.active_mute_for(
            f"internal/dep:dependency_unhealthy:{dep['dependency_name']}"
        )
        if mute is not None:
            dep["mute"] = {"until": mute["expires_at"], "comment": mute["comment"]}
    return deps


def _render_channels(channels: dict[str, Any]) -> None:
    """Plain text channel health + digest attempt ladder."""
    click.echo("channels:")
    channel_health = channels.get("health") or []
    attempts = channels.get("attempts") or []
    immediate_attempts = channels.get("immediate_attempts") or []
    if not channel_health and not attempts and not immediate_attempts:
        click.echo("  none")
        return
    if channel_health:
        click.echo("  health:")
        for row in channel_health:
            click.echo(f"    {row['channel']}: {row['status']}")
            if row.get("last_error"):
                click.echo(f"      error: {row['last_error']}")
    if attempts:
        click.echo("  digest attempts:")
        for row in attempts:
            click.echo(
                f"    {row['pipe']}/{row['channel']}: {row['attempts']} attempts"
            )
            if row.get("last_error"):
                click.echo(f"      last error: {row['last_error']}")
    if immediate_attempts:
        click.echo("  immediate attempts:")
        for row in immediate_attempts:
            click.echo(
                f"    {row['pipe']}/{row['channel']}: {row['attempts']} attempts"
            )
            if row.get("last_error"):
                click.echo(f"      last error: {row['last_error']}")


def _render_fault_injection(fault_injection: dict[str, Any]) -> None:
    """Armed fault-injection channels (B28), one per line, screen-reader
    friendly. The registry is in-memory on the live daemon, so the daemon-down
    fallback always renders 'none' -- a restart clears every armed fault by
    construction, so there is nothing to read off sqlite."""
    armed = fault_injection.get("armed") or []
    click.echo("fault injection:")
    if not armed:
        click.echo("  none")
        return
    for name in armed:
        click.echo(f"  {name}")


def _render_health_fallback(root: Path) -> None:
    from angelus.daemon import _belfry_status, _delivery_surface
    from angelus.lodging.config import _enabled_yaml_files

    status, pid = _pid_status(root / "state" / "angelus.pid")
    click.echo(f"daemon: {status}")
    if pid is not None:
        click.echo(f"pid: {pid}")
    connection = _ro_connect(root / "state" / "angelus.sqlite3")
    if connection is None:
        click.echo("sqlite: unavailable")
        # Belfry sentinel is a plain file read -- surface it even if sqlite
        # is unavailable. The whole point of belfry is to be useful when
        # angelus is unhealthy, and "is belfry alive too?" is exactly the
        # question this fallback exists to answer.
        _render_belfry(_belfry_status(root))
        return
    try:
        catalog = Catalog(connection, root)
        click.echo(
            "observations pending triage: "
            f"{catalog.observations_pending_triage_count()}"
        )
        click.echo("findings pending dispatch:")
        pending = catalog.findings_pending_dispatch_by_pipe()
        if not pending:
            click.echo("  none")
        for pipe in sorted(pending):
            click.echo(f"  {pipe}: {pending[pipe]}")
        # Delivery surface, read-only, daemon-down path. Pipe names come from
        # the pipes/ dir (a plain file listing -- no cross-ref validation that
        # could raise on a half-broken config) unioned with any pipe that has
        # actually dispatched, so a never-delivered pipe still shows 'never'.
        pipe_names = sorted(
            {path.stem for path in _enabled_yaml_files(root / "pipes")}
            | set(catalog.last_successful_dispatch_per_pipe())
        )
        _render_delivery(_delivery_surface(catalog, pipe_names))
        # dep_health is a plain table read; surface it in the daemon-down
        # path too (read-only), so dep status is visible without the daemon.
        _render_deps(_deps_with_active_mutes(catalog))
        _render_channels(
            {
                "health": catalog.all_channel_health(),
                "attempts": catalog.digest_channel_attempts(),
                "immediate_attempts": catalog.immediate_channel_attempts(),
            }
        )
    finally:
        connection.close()
    _render_belfry(_belfry_status(root))
    # In-memory only: a down daemon holds no armed faults, so this always
    # renders 'none' on the fallback path -- present for output symmetry with
    # the live health surface.
    _render_fault_injection({})
    click.echo("(sources and next-fire times need the daemon)")


def _render_incidents(result: dict[str, Any]) -> None:
    click.echo("open incidents:")
    open_incidents = result["open"]
    if not open_incidents:
        click.echo("  none")
    for inc in open_incidents:
        click.echo(
            f"  #{inc['id']} {inc['source']} {inc['type']} {inc['entity']}"
        )
        click.echo(f"    opened: {inc['opened_at']}")
    click.echo("recently closed incidents:")
    closed = result["recently_closed"]
    if not closed:
        click.echo("  none")
    for inc in closed:
        click.echo(
            f"  #{inc['id']} {inc['source']} {inc['type']} {inc['entity']}"
        )
        click.echo(f"    closed: {inc['closed_at']}")


def _render_incidents_fallback(root: Path) -> None:
    status, _pid = _pid_status(root / "state" / "angelus.pid")
    connection = _ro_connect(root / "state" / "angelus.sqlite3")
    if connection is None:
        click.echo(f"daemon: {status}")
        click.echo("sqlite: unavailable")
        return
    click.echo(f"daemon: {status} (reading sqlite read-only)")
    try:
        catalog = Catalog(connection, root)
        _render_incidents(
            {
                "open": catalog.open_incidents(),
                "recently_closed": catalog.recently_closed_incidents(days=7),
            }
        )
    finally:
        connection.close()


def _render_mutes(result: dict[str, Any]) -> None:
    click.echo("active mutes:")
    active = result["active"]
    if not active:
        click.echo("  none")
    for m in active:
        comment = m["comment"] if m["comment"] else "(none)"
        click.echo(
            f"  {m['dedup_key']}  expires {m['expires_at']}  comment: {comment}"
        )


def _render_mutes_fallback(root: Path) -> None:
    status, _pid = _pid_status(root / "state" / "angelus.pid")
    connection = _ro_connect(root / "state" / "angelus.sqlite3")
    if connection is None:
        click.echo(f"daemon: {status}")
        click.echo("sqlite: unavailable")
        return
    click.echo(f"daemon: {status} (reading sqlite read-only)")
    try:
        catalog = Catalog(connection, root)
        _render_mutes({"active": catalog.active_mutes()})
    finally:
        connection.close()
