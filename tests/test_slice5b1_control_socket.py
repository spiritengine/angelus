"""Slice 5b-1: daemon control socket transport + health / incident_list ops.

Protocol tests construct an AngelusDaemon and start only its ControlServer
(a real asyncio unix server, real handlers) rather than the full run() loop --
that is a genuine socket round-trip without the scheduler/reloader weight.
The stale-socket and lifecycle tests do exercise the full run() path, since
those behaviours live in run()/finally.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket as socketlib
import sqlite3
import stat
import threading
from pathlib import Path

import pytest
from click.testing import CliRunner

from angelus.cli import _ro_connect, main
from angelus.control import MAX_REQUEST_BYTES
from angelus.daemon import AngelusDaemon
from angelus.storage import Catalog, init_db


def _write_lodging(root: Path) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "triagers" / "handlers").mkdir(parents=True)
    (root / "triagers" / "handlers" / "noop.py").write_text(
        "import json\nprint(json.dumps({'findings': [], 'new_state': {}}))\n",
        encoding="utf-8",
    )
    (root / "triagers" / "noop.yaml").write_text(
        "inputs:\n  source: scheduled/watch\n"
        "handler:\n  kind: python\n  path: triagers/handlers/noop.py\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )


async def _ask(sock_path: Path, payload: dict | None, *, raw: bytes | None = None):
    reader, writer = await asyncio.open_unix_connection(str(sock_path))
    try:
        writer.write(raw if raw is not None else (json.dumps(payload) + "\n").encode())
        await writer.drain()
        line = await reader.readline()
    finally:
        writer.close()
        await asyncio.gather(writer.wait_closed(), return_exceptions=True)
    return json.loads(line.decode("utf-8"))


def _seed_incidents(catalog: Catalog) -> None:
    catalog.write_finding(
        None,
        {"source": "scheduled/watch", "type": "down", "entity": "open.example", "target_pipes": []},
        set(),
    )
    catalog.write_finding(
        None,
        {"source": "scheduled/watch", "type": "down", "entity": "closed.example", "target_pipes": []},
        set(),
    )
    catalog.write_finding(
        None,
        {"source": "scheduled/watch", "type": "clearance", "entity": "closed.example", "target_pipes": []},
        set(),
    )


# --- protocol / transport -------------------------------------------------


def test_health_round_trip(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            response = await _ask(daemon.socket_path, {"op": "health"})
        finally:
            await daemon.control.stop()
            daemon.connection.close()

        assert response["ok"] is True
        result = response["result"]
        assert result["daemon"] == {"running": True, "pid": __import__("os").getpid()}
        assert {s["name"] for s in result["sources"]} == {"scheduled/watch"}
        assert "observations_pending_triage" in result["queues"]
        assert "findings_pending_dispatch" in result["queues"]
        assert result["belfry"] is None

    asyncio.run(driver())


def test_unknown_op(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            response = await _ask(daemon.socket_path, {"op": "bogus"})
        finally:
            await daemon.control.stop()
            daemon.connection.close()
        assert response == {"ok": False, "error": "unknown op: bogus"}

    asyncio.run(driver())


def test_malformed_request_then_server_survives(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            bad = await _ask(daemon.socket_path, None, raw=b"not json\n")
            assert bad["ok"] is False
            assert "malformed" in bad["error"]
            # A fresh connection still works -> one bad client did not wedge it.
            good = await _ask(daemon.socket_path, {"op": "health"})
            assert good["ok"] is True
        finally:
            await daemon.control.stop()
            daemon.connection.close()

    asyncio.run(driver())


def test_oversized_request_refused_and_server_survives(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            oversized = b"x" * (MAX_REQUEST_BYTES + 4096) + b"\n"
            refused = await _ask(daemon.socket_path, None, raw=oversized)
            assert refused == {"ok": False, "error": "request too large"}
            good = await _ask(daemon.socket_path, {"op": "health"})
            assert good["ok"] is True
        finally:
            await daemon.control.stop()
            daemon.connection.close()

    asyncio.run(driver())


def test_incident_list_round_trip(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        _seed_incidents(daemon.catalog)
        await daemon.control.start()
        try:
            response = await _ask(daemon.socket_path, {"op": "incident_list"})
        finally:
            await daemon.control.stop()
            daemon.connection.close()

        assert response["ok"] is True
        result = response["result"]
        assert [i["entity"] for i in result["open"]] == ["open.example"]
        assert [i["entity"] for i in result["recently_closed"]] == ["closed.example"]

    asyncio.run(driver())


# --- daemon-down CLI fallbacks -------------------------------------------


def test_daemon_down_health_reads_sqlite(tmp_path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    connection = init_db(state / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    catalog.write_observation("scheduled/watch", {"url": "x"}, {"source": "scheduled/watch"})
    # Slice 5c (Contract D): dep_health is a mandatory reader on the
    # daemon-DOWN path too -- _render_health_fallback surfaces it over
    # the read-only sqlite connection. This fails if cli.py's
    # _render_deps(catalog.all_dep_health()) fallback line is removed.
    catalog.record_dep_health(
        "skein", "healthy", "2026-05-19T00:00:00.000Z", "ok"
    )
    catalog.record_dep_health(
        "patbot-email", "unhealthy",
        "2026-05-19T00:05:00.000Z", "exit 2: auth failed"
    )
    connection.close()

    result = CliRunner().invoke(main, ["health", "--root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "daemon: not running" in result.output
    assert "observations pending triage: 1" in result.output
    assert "skein: healthy" in result.output
    assert "patbot-email: unhealthy" in result.output
    assert "detail: exit 2: auth failed" in result.output


def test_daemon_down_incident_list_reads_sqlite(tmp_path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    connection = init_db(state / "angelus.sqlite3")
    _seed_incidents(Catalog(connection, tmp_path))
    connection.close()

    result = CliRunner().invoke(main, ["incident", "list", "--root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "open.example" in result.output
    assert "closed.example" in result.output


def test_readonly_fallback_cannot_write(tmp_path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    init_db(state / "angelus.sqlite3").close()

    connection = _ro_connect(state / "angelus.sqlite3")
    assert connection is not None
    try:
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("CREATE TABLE intruder (x)")
            connection.commit()
    finally:
        connection.close()


# --- run() integration: stale socket + lifecycle -------------------------


def test_stale_socket_removed_on_startup(tmp_path) -> None:
    _write_lodging(tmp_path)
    state = tmp_path / "state"
    state.mkdir()
    stale = state / "angelus.sock"
    stale.write_text("stale regular file, not a socket", encoding="utf-8")

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        task = asyncio.create_task(daemon.run())
        try:
            for _ in range(60):
                if daemon.socket_path.exists():
                    try:
                        response = await _ask(daemon.socket_path, {"op": "health"})
                        break
                    except (ConnectionError, OSError):
                        pass
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("control socket never answered")
            assert response["ok"] is True
            daemon.request_stop()
            await asyncio.wait_for(task, timeout=2.0)
        finally:
            if not task.done():
                daemon.request_stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(driver())


def test_socket_removed_on_shutdown(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        task = asyncio.create_task(daemon.run())
        try:
            for _ in range(60):
                if daemon.socket_path.exists():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("control socket never bound")
            assert daemon.socket_path.exists()
            daemon.request_stop()
            await asyncio.wait_for(task, timeout=2.0)
            assert not daemon.socket_path.exists()
        finally:
            if not task.done():
                daemon.request_stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(driver())


# --- trust boundary: owner-only socket + state dir -----------------------


def test_socket_and_state_dir_are_owner_only(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            sock_mode = stat.S_IMODE(os.stat(daemon.socket_path).st_mode)
            dir_mode = stat.S_IMODE(os.stat(tmp_path / "state").st_mode)
        finally:
            await daemon.control.stop()
            daemon.connection.close()

        # No group/other bits on either: owner read/write only on the socket,
        # owner rwx only on the directory it lives in.
        assert sock_mode == 0o600, oct(sock_mode)
        assert dir_mode == 0o700, oct(dir_mode)

    asyncio.run(driver())


# --- read deadline + stalled-client shutdown -----------------------------


def test_stalled_client_gets_timeout_and_server_survives(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        daemon.control._read_timeout = 0.2
        await daemon.control.start()
        try:
            # Connect, send nothing, leave it open: server must answer with a
            # structured timeout rather than block on readuntil forever.
            reader, writer = await asyncio.open_unix_connection(
                str(daemon.socket_path)
            )
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=2.0)
            finally:
                writer.close()
                await asyncio.gather(writer.wait_closed(), return_exceptions=True)
            assert json.loads(line.decode()) == {
                "ok": False,
                "error": "request timed out",
            }
            # A fresh client still works -> the stalled one did not wedge it.
            good = await _ask(daemon.socket_path, {"op": "health"})
            assert good["ok"] is True
        finally:
            await daemon.control.stop()
            daemon.connection.close()

    asyncio.run(driver())


def test_stalled_client_does_not_wedge_daemon_shutdown(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        task = asyncio.create_task(daemon.run())
        try:
            for _ in range(60):
                if daemon.socket_path.exists():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("control socket never bound")

            # Open a connection and never send a newline; keep it open across
            # the whole shutdown. Against the un-fixed code stop() blocks in
            # wait_closed() on this handler and run() never finishes.
            reader, writer = await asyncio.open_unix_connection(
                str(daemon.socket_path)
            )
            try:
                assert daemon.pid_file.exists()
                # No sleep before request_stop: this deliberately also covers
                # the accept race (handler not yet spawned/tracked). Bound is
                # generous enough for the wait_closed cap on that path; the
                # un-fixed code hangs unboundedly here.
                daemon.request_stop()
                await asyncio.wait_for(task, timeout=15.0)
                assert not daemon.socket_path.exists()
                assert not daemon.pid_file.exists()
            finally:
                writer.close()
                await asyncio.gather(writer.wait_closed(), return_exceptions=True)
        finally:
            if not task.done():
                daemon.request_stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(driver())


def test_connection_closed_mid_request_server_survives(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            # Connect and close before sending a newline. The handler must
            # exit cleanly (IncompleteReadError) without leaking or crashing.
            reader, writer = await asyncio.open_unix_connection(
                str(daemon.socket_path)
            )
            writer.close()
            await asyncio.gather(writer.wait_closed(), return_exceptions=True)
            await asyncio.sleep(0.05)
            assert not daemon.control._handlers, "handler task leaked"
            good = await _ask(daemon.socket_path, {"op": "health"})
            assert good["ok"] is True
        finally:
            await daemon.control.stop()
            daemon.connection.close()

    asyncio.run(driver())


@pytest.mark.parametrize(
    "raw,expected_error",
    [
        (b"[]\n", "expected a JSON object"),
        (b"42\n", "expected a JSON object"),
        (b'{"args": {}}\n', "missing or invalid op"),
        (b'{"op": "health", "args": []}\n', "args must be an object"),
    ],
)
def test_bad_payload_shape_structured_error(tmp_path, raw, expected_error) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            bad = await _ask(daemon.socket_path, None, raw=raw)
            assert bad["ok"] is False
            assert expected_error in bad["error"]
            # Server survives the bad client.
            good = await _ask(daemon.socket_path, {"op": "health"})
            assert good["ok"] is True
        finally:
            await daemon.control.stop()
            daemon.connection.close()

    asyncio.run(driver())


# --- CLI fallback when daemon returns a truncated response ---------------


def test_cli_falls_back_on_truncated_socket_response(tmp_path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    connection = init_db(state / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    catalog.write_observation(
        "scheduled/watch", {"url": "x"}, {"source": "scheduled/watch"}
    )
    connection.close()

    sock_path = state / "angelus.sock"
    listener = socketlib.socket(socketlib.AF_UNIX, socketlib.SOCK_STREAM)
    listener.bind(str(sock_path))
    listener.listen(1)
    listener.settimeout(5.0)

    def serve_truncated() -> None:
        # Accept, then close after a partial (un-terminated, un-parseable)
        # JSON line: this is a daemon killed mid-write.
        conn, _ = listener.accept()
        try:
            conn.recv(4096)
            conn.sendall(b'{"ok": tr')
        finally:
            conn.close()

    server_thread = threading.Thread(target=serve_truncated, daemon=True)
    server_thread.start()
    try:
        result = CliRunner().invoke(main, ["health", "--root", str(tmp_path)])
    finally:
        server_thread.join(timeout=5.0)
        listener.close()

    assert result.exit_code == 0, result.output
    assert result.exception is None, result.exception
    assert "daemon: not running" in result.output
    assert "observations pending triage: 1" in result.output


def test_cli_falls_back_on_oversized_unterminated_response(tmp_path) -> None:
    # A daemon that streams forever without a newline must not make the CLI
    # buffer without bound. Against an uncapped read loop this hangs
    # indefinitely (recv keeps returning data so the inactivity timeout never
    # fires); the client-side response cap turns it into a clean fallback.
    state = tmp_path / "state"
    state.mkdir()
    connection = init_db(state / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    catalog.write_observation(
        "scheduled/watch", {"url": "x"}, {"source": "scheduled/watch"}
    )
    connection.close()

    sock_path = state / "angelus.sock"
    listener = socketlib.socket(socketlib.AF_UNIX, socketlib.SOCK_STREAM)
    listener.bind(str(sock_path))
    listener.listen(1)
    listener.settimeout(5.0)
    stop = threading.Event()

    def serve_flood() -> None:
        conn, _ = listener.accept()
        try:
            conn.recv(4096)
            blob = b"x" * 65536  # no newline, ever
            while not stop.is_set():
                try:
                    conn.sendall(blob)
                except OSError:
                    break
        finally:
            conn.close()

    server_thread = threading.Thread(target=serve_flood, daemon=True)
    server_thread.start()

    result_box: dict[str, object] = {}

    def run_cli() -> None:
        result_box["result"] = CliRunner().invoke(
            main, ["health", "--root", str(tmp_path)]
        )

    cli_thread = threading.Thread(target=run_cli, daemon=True)
    cli_thread.start()
    cli_thread.join(timeout=15.0)
    completed = not cli_thread.is_alive()
    stop.set()
    server_thread.join(timeout=5.0)
    listener.close()

    assert completed, "CLI did not return: response read is unbounded"
    result = result_box["result"]
    assert result.exit_code == 0, result.output
    assert result.exception is None, result.exception
    assert "daemon: not running" in result.output
    assert "observations pending triage: 1" in result.output
