"""Command-line entry point for Angelus.

Read commands talk to the running daemon over its control socket
(state/angelus.sock). If the daemon is down or unreachable, health and
incident-list fall back to reading sqlite in true read-only mode
(file:...?mode=ro) -- the CLI can never write the database, preserving the
single-writer invariant. "The daemon is down" is a successful health report,
not a CLI error: the fallback path exits 0.

Write commands (mute / incident close / replay / reprocess) are the inverse:
they MUST go through the daemon (the single sqlite writer) and have NO
sqlite fallback -- a fallback would reintroduce a second writer. A
missing/refused/garbled socket is a hard, non-zero exit with a clear
message.

Output is operator-facing and read aloud by a screen reader: plain text, one
item per line, "label: value" and simple indented lists. No tables, columns,
or box-drawing.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import urllib.parse
from pathlib import Path
from typing import Any

import click

from angelus.daemon import main as daemon_main
from angelus.storage import Catalog

_ROOT_OPTION = click.option(
    "--root",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("."),
    show_default=True,
    help="Angelus root directory (where state/ lives).",
)

_SOCKET_TIMEOUT = 5.0

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


@main.command()
@click.argument("dedup_key")
@click.argument("duration")
@click.option("--comment", default=None, help="Why this is muted.")
@_ROOT_OPTION
def mute(dedup_key: str, duration: str, comment: str | None, root: Path) -> None:
    """Mute findings with DEDUP_KEY for DURATION (e.g. 30m, 4h, 2d).

    Requires the daemon. DURATION is <int><unit> with unit s/m/h/d; a
    bare integer is rejected.
    """
    root = root.resolve()
    result = _require_daemon(
        root,
        "mute",
        {"dedup_key": dedup_key, "duration": duration, "comment": comment},
        "mute",
    )
    click.echo(f"muted {result['dedup_key']} until {result['expires_at']}")


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
    belfry = result["belfry"]
    click.echo(
        f"last belfry ping: {belfry if belfry is not None else 'not recorded'}"
    )


def _render_health_fallback(root: Path) -> None:
    status, pid = _pid_status(root / "state" / "angelus.pid")
    click.echo(f"daemon: {status}")
    if pid is not None:
        click.echo(f"pid: {pid}")
    connection = _ro_connect(root / "state" / "angelus.sqlite3")
    if connection is None:
        click.echo("sqlite: unavailable")
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
    finally:
        connection.close()
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
