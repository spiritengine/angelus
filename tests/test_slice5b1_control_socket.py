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
import sqlite3
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
    connection.close()

    result = CliRunner().invoke(main, ["health", "--root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "daemon: not running" in result.output
    assert "observations pending triage: 1" in result.output


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
