from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest

import angelus.daemon as daemon_module
import angelus.pipes.runner as pipe_runner
from angelus.daemon import AngelusDaemon
from angelus.lodging import Channel, Pipe, Triager
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db
from angelus.triage import run_python_triager


def _write_trust_lodging(root: Path) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "a.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "sources" / "scheduled" / "b.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "triagers" / "handlers").mkdir(parents=True)
    (root / "triagers" / "handlers" / "noop.py").write_text(
        "import json\nprint(json.dumps({'findings': [], 'new_state': {}}))\n",
        encoding="utf-8",
    )
    (root / "triagers" / "ta.yaml").write_text(
        "inputs:\n  source: scheduled/a\n"
        "handler:\n  kind: python\n  path: triagers/handlers/noop.py\n",
        encoding="utf-8",
    )
    (root / "triagers" / "tb.yaml").write_text(
        "inputs:\n  source: scheduled/b\n"
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
        "kind: push\ncommand: notify-pat\n",
        encoding="utf-8",
    )


def _force_due(catalog: Catalog, observation_id: int, triager_name: str) -> None:
    catalog.connection.execute(
        """
        UPDATE observation_triage
        SET next_attempt_at = '2000-01-01T00:00:00.000Z'
        WHERE observation_id = ? AND triager_name = ?
        """,
        (observation_id, triager_name),
    )
    catalog.connection.commit()


def test_python_triager_timeout_kills_subprocess(tmp_path) -> None:
    pid_file = tmp_path / "handler.pid"
    handler = tmp_path / "sleepy.py"
    handler.write_text(
        "import os, time\n"
        f"{str(pid_file)!r} and open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
        "time.sleep(5)\n",
        encoding="utf-8",
    )
    triager = Triager(
        name="sleepy",
        source_ref="scheduled/test",
        handler_path=handler,
        timeout_seconds=1.0,
    )

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="timed out"):
        asyncio.run(run_python_triager(triager, {}, {}))
    assert time.monotonic() - started < 3

    pid = int(pid_file.read_text(encoding="utf-8"))
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_triage_retry_schedule_and_internal_finding(tmp_path, monkeypatch) -> None:
    _write_trust_lodging(tmp_path)
    angelus = AngelusDaemon(tmp_path)
    observation_id = angelus.catalog.write_observation(
        "scheduled/a", {"x": 1}, {"source": "scheduled/a"}
    )

    async def boom(*_args, **_kwargs):
        raise RuntimeError("triager exploded")

    monkeypatch.setattr(daemon_module, "run_python_triager", boom)

    async def attempt_once() -> None:
        rows = angelus.catalog.ready_observations_for("ta", "scheduled/a")
        assert len(rows) == 1
        angelus.catalog.mark_triage_processing(rows[0]["id"], "ta")
        await angelus._triage_under_semaphore(rows[0], "ta")

    try:
        for expected_attempt in (2, 3, 4, 5):
            asyncio.run(attempt_once())
            row = angelus.connection.execute(
                """
                SELECT status, attempt, next_attempt_at
                FROM observation_triage
                WHERE observation_id = ? AND triager_name = 'ta'
                """,
                (observation_id,),
            ).fetchone()
            assert row["status"] == "failed"
            assert row["attempt"] == expected_attempt
            assert row["next_attempt_at"] is not None
            assert angelus.catalog.ready_observations_for("ta", "scheduled/a") == []
            _force_due(angelus.catalog, observation_id, "ta")

        asyncio.run(attempt_once())
        observation = angelus.connection.execute(
            "SELECT status FROM observations WHERE id = ?", (observation_id,)
        ).fetchone()
        internal = angelus.connection.execute(
            """
            SELECT source, type, entity, severity, target_pipes
            FROM findings
            WHERE source = 'internal/triage'
            """
        ).fetchone()
        queued = angelus.connection.execute(
            """
            SELECT status
            FROM pipe_queues
            WHERE finding_id = (
                SELECT id FROM findings WHERE source = 'internal/triage'
            ) AND pipe = 'now'
            """
        ).fetchone()
    finally:
        angelus.connection.close()

    assert observation["status"] == "triage_failed"
    assert internal["type"] == "triage_failed"
    assert internal["entity"] == "ta"
    assert internal["severity"] == "high"
    assert internal["target_pipes"] == '["now"]'
    assert queued["status"] == "pending"


def test_dispatch_retry_schedule_and_internal_finding(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    pipe = Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{type}:{entity}:{body}",
        channels=["push"],
    )
    channel = Channel(name="push", kind="push", command="notify-pat")
    drain = PipeDrain(catalog, pipe, {"push": channel}, tmp_path, {"now"})
    observation_id = catalog.write_observation("scheduled/a", {}, {"source": "scheduled/a"})
    finding_id = catalog.write_finding(
        observation_id,
        {
            "source": "scheduled/a",
            "type": "down",
            "entity": "example",
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now"},
    )

    async def fail_send(*_args, **_kwargs):
        raise RuntimeError("push broke")

    monkeypatch.setattr(pipe_runner, "send_push", fail_send)
    try:
        for expected_attempt in (1, 2, 3, 4):
            asyncio.run(drain.drain_once())
            row = connection.execute(
                """
                SELECT attempts, next_attempt_at, status
                FROM pipe_queues
                WHERE finding_id = ? AND pipe = 'now'
                """,
                (finding_id,),
            ).fetchone()
            assert row["attempts"] == expected_attempt
            assert row["status"] == "pending"
            assert row["next_attempt_at"] is not None
            connection.execute(
                """
                UPDATE pipe_queues
                SET next_attempt_at = '2000-01-01T00:00:00.000Z'
                WHERE finding_id = ? AND pipe = 'now'
                """,
                (finding_id,),
            )
            connection.commit()

        asyncio.run(drain.drain_once())
        queue = connection.execute(
            """
            SELECT attempts, status, next_attempt_at
            FROM pipe_queues
            WHERE finding_id = ? AND pipe = 'now'
            """,
            (finding_id,),
        ).fetchone()
        health = connection.execute(
            "SELECT status, last_error FROM channel_health WHERE channel = 'push'"
        ).fetchone()
        internal = connection.execute(
            """
            SELECT source, type, entity, severity
            FROM findings
            WHERE source = 'internal/dispatch'
            """
        ).fetchone()
    finally:
        connection.close()

    assert queue["attempts"] == 5
    assert queue["status"] == "failed"
    assert queue["next_attempt_at"] is None
    assert health["status"] == "unhealthy"
    assert health["last_error"] == "push broke"
    assert internal["type"] == "channel_unhealthy"
    assert internal["entity"] == "push"
    assert internal["severity"] == "high"


def test_drain_skips_unhealthy_channel_and_leaves_queue_pending(
    tmp_path, monkeypatch
) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    pipe = Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{type}:{entity}:{body}",
        channels=["push"],
    )
    channel = Channel(name="push", kind="push", command="notify-pat")
    drain = PipeDrain(catalog, pipe, {"push": channel}, tmp_path, {"now"})
    observation_id = catalog.write_observation("scheduled/a", {}, {"source": "scheduled/a"})
    finding_id = catalog.write_finding(
        observation_id,
        {
            "source": "scheduled/a",
            "type": "down",
            "entity": "example",
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now"},
    )
    catalog.mark_channel_unhealthy("push", "still down")
    connection.commit()

    attempts = 0

    async def fail_if_called(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        raise AssertionError("send_push should not run for unhealthy channels")

    monkeypatch.setattr(pipe_runner, "send_push", fail_if_called)
    try:
        asyncio.run(drain.drain_once())
        queue = connection.execute(
            """
            SELECT status, attempts, next_attempt_at
            FROM pipe_queues
            WHERE finding_id = ? AND pipe = 'now'
            """,
            (finding_id,),
        ).fetchone()
        dispatch_count = connection.execute(
            "SELECT COUNT(*) AS n FROM dispatches"
        ).fetchone()
    finally:
        connection.close()

    assert attempts == 0
    assert queue["status"] == "pending"
    assert queue["attempts"] == 0
    assert queue["next_attempt_at"] is None
    assert dispatch_count["n"] == 0


def test_triager_source_mutex_serializes_same_pair_but_not_different_pairs(tmp_path) -> None:
    _write_trust_lodging(tmp_path)
    angelus = AngelusDaemon(tmp_path)
    angelus.triage_semaphore = asyncio.Semaphore(3)

    for source_ref in ("scheduled/a", "scheduled/a", "scheduled/b"):
        angelus.catalog.write_observation(source_ref, {}, {"source": source_ref})

    active_by_key: dict[tuple[str, str], int] = {}
    max_by_key: dict[tuple[str, str], int] = {}
    total_active = 0
    total_peak = 0
    starts: list[tuple[str, str]] = []
    release = asyncio.Event()

    async def fake_run(row, triager_name: str) -> None:
        nonlocal total_active, total_peak
        triager = angelus.lodging.triagers[triager_name]
        key = (triager.name, triager.source_ref)
        active_by_key[key] = active_by_key.get(key, 0) + 1
        max_by_key[key] = max(max_by_key.get(key, 0), active_by_key[key])
        total_active += 1
        total_peak = max(total_peak, total_active)
        starts.append(key)
        try:
            await release.wait()
            angelus.catalog.mark_triage_success(int(row["id"]), triager_name)
        finally:
            active_by_key[key] -= 1
            total_active -= 1

    angelus._run_triager = fake_run  # type: ignore[method-assign]

    async def driver() -> None:
        loop_task = asyncio.create_task(angelus._triage_loop())
        for _ in range(40):
            await asyncio.sleep(0.05)
            if total_peak >= 2 and starts.count(("ta", "scheduled/a")) == 1:
                break
        release.set()
        for _ in range(40):
            await asyncio.sleep(0.05)
            if starts.count(("ta", "scheduled/a")) == 2:
                break
        angelus.stop_event.set()
        await asyncio.wait_for(loop_task, timeout=2.0)

    try:
        asyncio.run(driver())
    finally:
        angelus.connection.close()

    assert max_by_key[("ta", "scheduled/a")] == 1
    assert total_peak >= 2
    assert starts.count(("ta", "scheduled/a")) == 2
    assert ("tb", "scheduled/b") in starts


def test_startup_recovery_marks_writing_rows_by_body_presence(tmp_path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    connection = init_db(state_dir / "angelus.sqlite3")
    ready_body = tmp_path / "observations" / "2026-05-14" / "1" / "body.json"
    ready_body.parent.mkdir(parents=True)
    ready_body.write_text("{}\n", encoding="utf-8")
    connection.execute(
        """
        INSERT INTO observations (id, source, status, body_ref)
        VALUES (1, 'scheduled/a', 'writing', 'observations/2026-05-14/1/body.json')
        """
    )
    connection.execute(
        """
        INSERT INTO observations (id, source, status, body_ref)
        VALUES (2, 'scheduled/b', 'writing', 'observations/2026-05-14/2/body.json')
        """
    )
    connection.commit()
    connection.close()

    angelus = AngelusDaemon(tmp_path)

    async def run_briefly() -> None:
        run_task = asyncio.create_task(angelus.run())
        await asyncio.sleep(0.05)
        angelus.request_stop()
        await asyncio.wait_for(run_task, timeout=2.0)

    asyncio.run(run_briefly())

    connection = init_db(state_dir / "angelus.sqlite3")
    try:
        rows = {
            row["id"]: row["status"]
            for row in connection.execute("SELECT id, status FROM observations")
        }
    finally:
        connection.close()

    assert rows == {1: "ready", 2: "failed"}


def test_startup_clears_channel_health(tmp_path) -> None:
    _write_trust_lodging(tmp_path)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    connection = init_db(state_dir / "angelus.sqlite3")
    connection.execute(
        """
        INSERT INTO channel_health (channel, status, last_error)
        VALUES ('push', 'unhealthy', 'broke earlier')
        """
    )
    connection.commit()
    connection.close()

    angelus = AngelusDaemon(tmp_path)

    async def run_briefly() -> None:
        run_task = asyncio.create_task(angelus.run())
        await asyncio.sleep(0.05)
        angelus.request_stop()
        await asyncio.wait_for(run_task, timeout=2.0)

    asyncio.run(run_briefly())

    connection = init_db(state_dir / "angelus.sqlite3")
    try:
        rows = list(connection.execute("SELECT channel, status FROM channel_health"))
    finally:
        connection.close()

    assert rows == []


def test_startup_clears_digest_channel_attempts(tmp_path) -> None:
    _write_trust_lodging(tmp_path)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    connection = init_db(state_dir / "angelus.sqlite3")
    # Seed the per-channel digest attempt counter at one below the threshold
    # (MAX_RETRY_ATTEMPTS = 5, so attempts = 4). If startup leaves these rows
    # in place while wiping channel_health, the next digest failure on either
    # row crosses the ladder immediately on the fresh daemon generation --
    # breaking the operator-restart-re-enables-a-channel contract.
    connection.execute(
        """
        INSERT INTO digest_channel_attempts (pipe, channel, attempts, last_error)
        VALUES ('daily', 'email', 4, 'smtp refused')
        """
    )
    connection.execute(
        """
        INSERT INTO digest_channel_attempts (pipe, channel, attempts, last_error)
        VALUES ('weekly', 'push', 4, 'pushd hung')
        """
    )
    connection.commit()
    connection.close()

    angelus = AngelusDaemon(tmp_path)

    async def run_briefly() -> None:
        run_task = asyncio.create_task(angelus.run())
        await asyncio.sleep(0.05)
        angelus.request_stop()
        await asyncio.wait_for(run_task, timeout=2.0)

    asyncio.run(run_briefly())

    connection = init_db(state_dir / "angelus.sqlite3")
    try:
        rows = list(
            connection.execute(
                "SELECT pipe, channel, attempts FROM digest_channel_attempts"
            )
        )
    finally:
        connection.close()

    assert rows == []


def test_startup_clears_immediate_channel_attempts(tmp_path) -> None:
    """Restart-scope parity for the immediate-path per-channel counter (B7
    fell-r1 Finding 3). It feeds the same channel_health ladder _drain_immediate
    uses, so leaving it populated while startup wipes channel_health would let
    the first post-restart failure cross threshold immediately -- breaking the
    operator-restart-re-enables-a-channel contract, exactly as for
    digest_channel_attempts."""
    _write_trust_lodging(tmp_path)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    connection = init_db(state_dir / "angelus.sqlite3")
    # Seed one below threshold (MAX_RETRY_ATTEMPTS = 5 -> attempts = 4).
    connection.execute(
        """
        INSERT INTO immediate_channel_attempts (pipe, channel, attempts, last_error)
        VALUES ('now', 'push', 4, 'pushd hung')
        """
    )
    connection.execute(
        """
        INSERT INTO immediate_channel_attempts (pipe, channel, attempts, last_error)
        VALUES ('now', 'email', 4, 'smtp refused')
        """
    )
    connection.commit()
    connection.close()

    angelus = AngelusDaemon(tmp_path)

    async def run_briefly() -> None:
        run_task = asyncio.create_task(angelus.run())
        await asyncio.sleep(0.05)
        angelus.request_stop()
        await asyncio.wait_for(run_task, timeout=2.0)

    asyncio.run(run_briefly())

    connection = init_db(state_dir / "angelus.sqlite3")
    try:
        rows = list(
            connection.execute(
                "SELECT pipe, channel, attempts FROM immediate_channel_attempts"
            )
        )
    finally:
        connection.close()

    assert rows == []
