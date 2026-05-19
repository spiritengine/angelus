"""Slice 5b-2: control-socket write ops + mutes.

The four write ops (mute / incident_close / replay / reprocess) run inside
the daemon -- the single sqlite writer. These tests drive them over a real
control-socket round trip (constructing an AngelusDaemon and starting only
its ControlServer, as the 5b-1 suite does) and assert on the resulting
sqlite state. The contract this slice must hold:

  * every op is idempotent under at-least-once delivery on natural state
    (no request-id/dedup cache anywhere);
  * write CLI commands REQUIRE the daemon -- no read-only sqlite fallback;
  * write handlers do not hold a transaction across an await
    (cancel-safety);
  * the mutes table is actually consulted by _drain_immediate, and an
    EXPIRED mute does not suppress.

Each test fails against the absence of the thing it guards.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import angelus.pipes.runner as pipe_runner
from angelus.cli import main
from angelus.daemon import AngelusDaemon, _mute_duration_seconds
from angelus.lodging import Channel, Pipe
from angelus.pipes import PipeDrain
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


async def _ask(sock_path: Path, payload: dict) -> dict:
    reader, writer = await asyncio.open_unix_connection(str(sock_path))
    try:
        writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()
        line = await reader.readline()
    finally:
        writer.close()
        await asyncio.gather(writer.wait_closed(), return_exceptions=True)
    return json.loads(line.decode("utf-8"))


def _serve(tmp_path: Path):
    """Build a daemon, start only its control server, hand it to the test."""

    def decorator(body):
        async def driver() -> dict:
            _write_lodging(tmp_path)
            daemon = AngelusDaemon(tmp_path)
            await daemon.control.start()
            try:
                return await body(daemon)
            finally:
                await daemon.control.stop()
                daemon.connection.close()

        return asyncio.run(driver())

    return decorator


def _now_pipe() -> Pipe:
    return Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{type}:{entity}:{body}",
        channels=["push"],
    )


def _write_now_finding(catalog: Catalog, dedup_key: str) -> int:
    return catalog.write_finding(
        None,
        {
            "source": "scheduled/watch",
            "type": "down",
            "entity": "example",
            "severity": "high",
            "dedup_key": dedup_key,
            "target_pipes": ["now"],
        },
        {"now"},
    )


# --- mute duration parser ------------------------------------------------


@pytest.mark.parametrize(
    "text,seconds",
    [("90s", 90), ("30m", 1800), ("4h", 14400), ("2d", 172800)],
)
def test_mute_duration_parser_accepts_units(text, seconds) -> None:
    assert _mute_duration_seconds(text) == seconds


@pytest.mark.parametrize("text", ["30", "0h", "-1d", "h", "abc", ""])
def test_mute_duration_parser_rejects_bad(text) -> None:
    with pytest.raises(ValueError):
        _mute_duration_seconds(text)


def test_bad_mute_duration_surfaces_as_socket_error_not_crash(tmp_path) -> None:
    @_serve(tmp_path)
    async def _(daemon):
        bad = await _ask(
            daemon.socket_path,
            {"op": "mute", "args": {"dedup_key": "k", "duration": "30"}},
        )
        assert bad["ok"] is False
        assert "invalid mute duration" in bad["error"]
        # Daemon survived the bad op -> a fresh op still works.
        good = await _ask(
            daemon.socket_path,
            {"op": "mute", "args": {"dedup_key": "k", "duration": "5m"}},
        )
        assert good["ok"] is True


# --- mute op: round trip + idempotency -----------------------------------


def test_mute_op_round_trip_and_idempotent(tmp_path) -> None:
    @_serve(tmp_path)
    async def _(daemon):
        first = await _ask(
            daemon.socket_path,
            {
                "op": "mute",
                "args": {
                    "dedup_key": "scheduled/dead_link/home",
                    "duration": "2d",
                    "comment": "known flaky",
                },
            },
        )
        assert first["ok"] is True
        assert first["result"]["dedup_key"] == "scheduled/dead_link/home"
        expires_at = first["result"]["expires_at"]
        assert expires_at.endswith("Z")

        # At-least-once retry: same op again. A second overlapping row is
        # inserted; still muted; no error -> idempotent on natural state.
        second = await _ask(
            daemon.socket_path,
            {
                "op": "mute",
                "args": {
                    "dedup_key": "scheduled/dead_link/home",
                    "duration": "2d",
                },
            },
        )
        assert second["ok"] is True

        rows = list(
            daemon.connection.execute(
                "SELECT COUNT(*) AS n FROM mutes WHERE dedup_key = ?",
                ("scheduled/dead_link/home",),
            )
        )
        assert rows[0]["n"] == 2
        assert daemon.catalog.is_muted("scheduled/dead_link/home") is True


# --- mute is load-bearing in _drain_immediate ----------------------------


def test_active_mute_suppresses_now_and_is_audited(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    finding_id = _write_now_finding(catalog, "scheduled/down/example")
    catalog.add_mute("scheduled/down/example", 3600, "muted for test")

    drain = PipeDrain(
        catalog,
        _now_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now"},
    )
    # If _drain_immediate ignored the mutes table this would be called and
    # fail the test -> this is the load-bearing guard.
    monkeypatch.setattr(pipe_runner, "send_push", pytest.fail)

    try:
        asyncio.run(drain.drain_once())

        dispatches = list(
            connection.execute(
                "SELECT status, channel FROM dispatches WHERE finding_ids = ?",
                (json.dumps([finding_id]),),
            )
        )
        queue = list(
            connection.execute(
                "SELECT status FROM pipe_queues WHERE finding_id = ?",
                (finding_id,),
            )
        )
        # A muted dispatch row preserves the audit trail.
        assert [(d["status"], d["channel"]) for d in dispatches] == [
            ("muted", "(muted)")
        ]
        # Marked handled, not 'suppressed' (suppressed is the digest's
        # rate-limit state; a muted finding must not surface there).
        assert [q["status"] for q in queue] == ["dispatched"]

        # Second drain: the item must not reappear in pending and must not
        # send (send_push is still pytest.fail).
        asyncio.run(drain.drain_once())
        assert not catalog.pending_pipe_items("now")
    finally:
        connection.close()


def test_expired_mute_does_not_suppress(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    finding_id = _write_now_finding(catalog, "scheduled/down/example")
    # A mute row whose expires_at is firmly in the past. is_muted compares
    # expires_at > now lexicographically on identical ISO8601-UTC strings,
    # so this must NOT match -- the test fails if the consultation ignores
    # expires_at.
    connection.execute(
        """
        INSERT INTO mutes (dedup_key, expires_at, created_at, comment)
        VALUES (?, ?, ?, ?)
        """,
        (
            "scheduled/down/example",
            "2000-01-01T00:00:00.000Z",
            "2000-01-01T00:00:00.000Z",
            "long expired",
        ),
    )
    connection.commit()

    sent: list[str] = []

    async def fake_push(channel, message, workdir):
        sent.append(message)

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)

    drain = PipeDrain(
        catalog,
        _now_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now"},
    )
    try:
        asyncio.run(drain.drain_once())
        assert len(sent) == 1
        statuses = [
            r["status"]
            for r in connection.execute(
                "SELECT status FROM dispatches WHERE finding_ids = ?",
                (json.dumps([finding_id]),),
            )
        ]
        assert statuses == ["sent"]
    finally:
        connection.close()


# --- incident close op ---------------------------------------------------


def _open_incident(catalog: Catalog) -> int:
    catalog.write_finding(
        None,
        {
            "source": "scheduled/watch",
            "type": "down",
            "entity": "host.example",
            "target_pipes": [],
        },
        set(),
    )
    row = catalog.connection.execute(
        "SELECT id FROM incidents WHERE status = 'open'"
    ).fetchone()
    return int(row["id"])


def test_incident_close_op_and_idempotent(tmp_path) -> None:
    @_serve(tmp_path)
    async def _(daemon):
        incident_id = _open_incident(daemon.catalog)

        first = await _ask(
            daemon.socket_path,
            {
                "op": "incident_close",
                "args": {"id": incident_id, "comment": "handled manually"},
            },
        )
        assert first["ok"] is True
        assert first["result"]["outcome"] == "closed"

        row = daemon.connection.execute(
            "SELECT status, closed_at, close_comment FROM incidents WHERE id = ?",
            (incident_id,),
        ).fetchone()
        assert row["status"] == "closed"
        assert row["close_comment"] == "handled manually"
        closed_at = row["closed_at"]
        assert closed_at is not None

        # Second close (at-least-once retry): already_closed, row unchanged.
        second = await _ask(
            daemon.socket_path,
            {
                "op": "incident_close",
                "args": {"id": incident_id, "comment": "different note"},
            },
        )
        assert second["ok"] is True
        assert second["result"]["outcome"] == "already_closed"
        row2 = daemon.connection.execute(
            "SELECT closed_at, close_comment FROM incidents WHERE id = ?",
            (incident_id,),
        ).fetchone()
        assert row2["closed_at"] == closed_at
        assert row2["close_comment"] == "handled manually"

        # Unknown id -> clean not_found (CLI turns this into a non-zero exit).
        missing = await _ask(
            daemon.socket_path,
            {"op": "incident_close", "args": {"id": 999999}},
        )
        assert missing["ok"] is True
        assert missing["result"]["outcome"] == "not_found"


# --- replay op -----------------------------------------------------------


def test_replay_op_requeues_and_guards_double(tmp_path, monkeypatch) -> None:
    @_serve(tmp_path)
    async def _(daemon):
        catalog = daemon.catalog
        finding_id = _write_now_finding(catalog, "scheduled/down/example")

        sent: list[str] = []

        async def fake_push(channel, message, workdir):
            sent.append(message)

        monkeypatch.setattr(pipe_runner, "send_push", fake_push)
        drain = PipeDrain(
            catalog,
            _now_pipe(),
            {"push": Channel(name="push", kind="push", command="notify-pat")},
            tmp_path,
            {"now"},
        )
        # First drain dispatches the finding (pipe_queues -> dispatched).
        await drain.drain_once()
        assert len(sent) == 1

        replayed = await _ask(
            daemon.socket_path,
            {"op": "replay", "args": {"finding_id": finding_id}},
        )
        assert replayed["ok"] is True
        assert replayed["result"]["outcome"] == "requeued"
        assert replayed["result"]["pipes"] == ["now"]

        # Re-dispatched on the next drain.
        await drain.drain_once()
        assert len(sent) == 2

        # Re-queue it again, then a SECOND replay while still 'pending'
        # must NOT double-queue (the mandatory NOT-EXISTS guard).
        again = await _ask(
            daemon.socket_path,
            {"op": "replay", "args": {"finding_id": finding_id}},
        )
        assert again["result"]["outcome"] == "requeued"
        double = await _ask(
            daemon.socket_path,
            {"op": "replay", "args": {"finding_id": finding_id}},
        )
        assert double["result"]["outcome"] == "already_queued"
        assert double["result"]["pipes"] == []
        rows = daemon.connection.execute(
            "SELECT COUNT(*) AS n FROM pipe_queues WHERE finding_id = ? AND pipe = 'now'",
            (finding_id,),
        ).fetchone()
        assert rows["n"] == 1

        missing = await _ask(
            daemon.socket_path,
            {"op": "replay", "args": {"finding_id": 999999}},
        )
        assert missing["result"]["outcome"] == "not_found"


# --- reprocess op --------------------------------------------------------


def test_reprocess_op_rebudgets_triage_and_idempotent(tmp_path) -> None:
    @_serve(tmp_path)
    async def _(daemon):
        catalog = daemon.catalog
        # Two observations from the target source, one from another.
        oid_a1 = catalog.write_observation(
            "scheduled/watch", {"v": 1}, {"source": "scheduled/watch"}
        )
        oid_a2 = catalog.write_observation(
            "scheduled/watch", {"v": 2}, {"source": "scheduled/watch"}
        )
        oid_b = catalog.write_observation(
            "scheduled/other", {"v": 3}, {"source": "scheduled/other"}
        )
        for oid in (oid_a1, oid_a2):
            catalog.mark_triage_processing(oid, "noop")
            catalog.mark_triage_success(oid, "noop")
        catalog.mark_triage_processing(oid_b, "noop")
        catalog.mark_triage_success(oid_b, "noop")

        # Triaged observations are excluded from the triage queue.
        assert catalog.ready_observations_for("noop", "scheduled/watch") == []

        result = await _ask(
            daemon.socket_path,
            {"op": "reprocess", "args": {"source": "scheduled/watch"}},
        )
        assert result["ok"] is True
        assert result["result"]["observations"] == 2

        # The two source observations are eligible again; the other
        # source's triage row is untouched.
        re_eligible = {
            r["id"]
            for r in catalog.ready_observations_for("noop", "scheduled/watch")
        }
        assert re_eligible == {oid_a1, oid_a2}
        other = daemon.connection.execute(
            "SELECT COUNT(*) AS n FROM observation_triage WHERE observation_id = ?",
            (oid_b,),
        ).fetchone()
        assert other["n"] == 1

        # Second reprocess: nothing left to delete -> no-op (idempotent).
        again = await _ask(
            daemon.socket_path,
            {"op": "reprocess", "args": {"source": "scheduled/watch"}},
        )
        assert again["result"]["observations"] == 0


# --- contract: write CLI commands require the daemon ----------------------


def test_daemon_down_write_commands_exit_nonzero_no_write(tmp_path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    connection = init_db(state / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    incident_id = _open_incident(catalog)
    connection.close()

    runner = CliRunner()

    muted = runner.invoke(
        main, ["mute", "scheduled/x", "30m", "--root", str(tmp_path)]
    )
    assert muted.exit_code != 0
    assert "daemon is not running" in muted.output
    assert "mute requires the daemon" in muted.output

    closed = runner.invoke(
        main, ["incident", "close", str(incident_id), "--root", str(tmp_path)]
    )
    assert closed.exit_code != 0
    assert "daemon is not running" in closed.output

    # No fallback writer touched the db: no mute rows, incident still open.
    check = init_db(state / "angelus.sqlite3")
    try:
        assert check.execute("SELECT COUNT(*) AS n FROM mutes").fetchone()["n"] == 0
        row = check.execute(
            "SELECT status FROM incidents WHERE id = ?", (incident_id,)
        ).fetchone()
        assert row["status"] == "open"
    finally:
        check.close()


# --- contract: cancel-safety (no await across the write) -----------------


def test_write_handlers_hold_no_await_across_the_write() -> None:
    """A write handler must not hold a sqlite transaction across an await
    (5b-1 cancel-safety). The handlers are synchronous in body and the
    catalog write methods they call are synchronous and self-committing;
    asserting neither contains `await`/`async def` proves the property by
    construction."""
    handlers = [
        AngelusDaemon._op_mute,
        AngelusDaemon._op_incident_close,
        AngelusDaemon._op_replay,
        AngelusDaemon._op_reprocess,
    ]
    for handler in handlers:
        body = inspect.getsource(handler)
        # The only async surface is the `async def` signature line; no
        # await anywhere in the body.
        assert "await " not in body, handler.__name__

    for method in (
        Catalog.add_mute,
        Catalog.close_incident,
        Catalog.replay_finding,
        Catalog.reprocess_source,
        Catalog.is_muted,
    ):
        src = inspect.getsource(method)
        assert "await " not in src, method.__name__
        assert "async def" not in src, method.__name__
