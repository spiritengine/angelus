from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from apscheduler.triggers.cron import CronTrigger

from angelus.daemon import AngelusDaemon, _cadence_seconds, _make_trigger
from angelus.lodging import load_lodging
from angelus.storage import Catalog, init_db
from angelus.triage import run_python_triager


def test_lodging_loads_cross_references() -> None:
    lodging = load_lodging(Path.cwd())

    assert "scheduled/iotaschool-watch" in lodging.sources
    assert lodging.triagers["dead-link"].source_ref == "scheduled/iotaschool-watch"
    assert lodging.pipes["now"].channels == ["push"]
    assert lodging.channels["push"].command == "notify-pat"


def test_dead_link_handler_emits_down_finding() -> None:
    lodging = load_lodging(Path.cwd())
    triager = lodging.triagers["dead-link"]

    findings, state = asyncio.run(
        run_python_triager(
            triager,
            {"url": "https://example.invalid", "status_code": 503},
            {"last_status": 200},
        )
    )

    assert state == {"last_status": 503}
    assert findings[0]["type"] == "down"
    assert findings[0]["entity"] == "iotaschool.com"
    assert findings[0]["target_pipes"] == ["now"]


def test_observation_and_finding_write_order(tmp_path) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    try:
        observation_id = catalog.write_observation(
            "scheduled/test",
            {"url": "https://example.invalid", "status_code": 503},
            {"source": "scheduled/test"},
        )
        finding_id = catalog.write_finding(
            observation_id,
            {
                "source": "scheduled/test",
                "type": "down",
                "entity": "https://example.invalid",
                "severity": "high",
                "target_pipes": ["now"],
            },
            {"now"},
        )
        observation = connection.execute(
            "SELECT status, body_ref FROM observations WHERE id = ?", (observation_id,)
        ).fetchone()
        finding = connection.execute(
            "SELECT status, body_ref FROM findings WHERE id = ?", (finding_id,)
        ).fetchone()
        queue = connection.execute(
            "SELECT status FROM pipe_queues WHERE finding_id = ? AND pipe = 'now'",
            (finding_id,),
        ).fetchone()
    finally:
        connection.close()

    assert observation["status"] == "ready"
    assert (tmp_path / observation["body_ref"]).exists()
    assert finding["status"] == "ready"
    assert (tmp_path / finding["body_ref"]).exists()
    assert queue["status"] == "pending"


def test_writing_row_visible_when_body_write_fails(tmp_path) -> None:
    """Spec §Storage: recovery scans `writing` rows. The first commit must land
    before the body write, so a crash mid-write leaves a recoverable row."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)

    def boom(*_args, **_kwargs):
        raise RuntimeError("simulated mid-write crash")

    catalog._write_body = boom  # type: ignore[method-assign]
    try:
        with pytest.raises(RuntimeError):
            catalog.write_observation(
                "scheduled/test",
                {"k": "v"},
                {"source": "scheduled/test"},
            )

        rows = list(
            connection.execute(
                "SELECT status FROM observations WHERE source = 'scheduled/test'"
            )
        )
    finally:
        connection.close()

    assert len(rows) == 1
    assert rows[0]["status"] == "writing"


def test_cadence_parser_requires_unit_suffix() -> None:
    assert _cadence_seconds("15m") == 900
    assert _cadence_seconds("30s") == 30
    assert _cadence_seconds("2h") == 7200
    assert _cadence_seconds("15min") == 900

    with pytest.raises(ValueError, match="unit suffix"):
        _cadence_seconds("15")
    with pytest.raises(ValueError, match="positive"):
        _cadence_seconds("0s")


def test_make_trigger_accepts_crontab_cadence() -> None:
    assert isinstance(_make_trigger("0 8 * * *"), CronTrigger)


def _write_minimal_lodging(root: Path) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    for name in ("fanout-a", "fanout-b", "fanout-c"):
        (root / "sources" / "scheduled" / f"{name}.yaml").write_text(
            "cadence: 1h\n"
            "check:\n"
            "  kind: shell\n"
            "  command: 'echo {}'\n"
        )
    (root / "triagers" / "handlers").mkdir(parents=True)
    for name in ("fanout-a", "fanout-b", "fanout-c"):
        (root / "triagers" / f"{name}.yaml").write_text(
            "inputs:\n"
            f"  source: scheduled/{name}\n"
            "handler:\n"
            "  kind: python\n"
            "  path: triagers/handlers/fanout.py\n"
        )
    (root / "triagers" / "handlers" / "fanout.py").write_text(
        "import json, sys\n"
        "print(json.dumps({'findings': [], 'new_state': {}}))\n"
    )


def _measure_triage_peak_concurrency(
    tmp_path: Path, loop_impl
) -> int:
    """Run `loop_impl` as a daemon's `_triage_loop` and return observed peak
    in-flight concurrency. `loop_impl` is an async coroutine function bound to
    a daemon, so callers can compare fan-out vs inline-await shapes."""
    daemon = AngelusDaemon(tmp_path)
    sem_size = 3
    daemon.triage_semaphore = asyncio.Semaphore(sem_size)

    for source_ref in daemon.lodging.sources:
        daemon.catalog.write_observation(
            source_ref, {"x": 1}, {"source": source_ref}
        )

    in_flight = 0
    peak = 0
    release = asyncio.Event()

    async def fake_run_triager(_row, _triager_name: str) -> None:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        try:
            await release.wait()
        finally:
            in_flight -= 1

    daemon._run_triager = fake_run_triager  # type: ignore[method-assign]

    async def driver() -> None:
        loop_task = asyncio.create_task(loop_impl(daemon))
        for _ in range(30):
            await asyncio.sleep(0.05)
            if in_flight >= sem_size:
                break
        daemon.stop_event.set()
        release.set()
        try:
            await asyncio.wait_for(loop_task, timeout=2.0)
        except asyncio.TimeoutError:
            loop_task.cancel()

    asyncio.run(driver())
    daemon.connection.close()
    return peak


async def _inline_await_triage_loop(daemon) -> None:
    """The pre-cleanup shape: await `_run_triager` inline under the semaphore.
    Mirrors what slice 1 originally shipped. Should produce peak concurrency 1."""
    while not daemon.stop_event.is_set():
        did_work = False
        for triager in daemon.lodging.triagers.values():
            rows = daemon.catalog.ready_observations_for(
                triager.name, triager.source_ref
            )
            for row in rows:
                did_work = True
                daemon.catalog.mark_triage_processing(row["id"], triager.name)
                async with daemon.triage_semaphore:
                    await daemon._run_triager(row, triager.name)
        if not did_work:
            await asyncio.sleep(0.05)


def test_triage_loop_fans_out_concurrently(tmp_path) -> None:
    """The triage loop must spawn tasks rather than await inline; otherwise the
    triage semaphore caps nothing. The current `_triage_loop` should observe
    peak concurrency equal to the semaphore size; the inline-await shape should
    observe peak 1. Asserting both makes the test discriminating, not tautological."""
    _write_minimal_lodging(tmp_path)

    fanout_peak = _measure_triage_peak_concurrency(
        tmp_path, AngelusDaemon._triage_loop
    )
    assert fanout_peak == 3, (
        f"current _triage_loop did not fan out: peak={fanout_peak}, expected 3"
    )

    inline_peak = _measure_triage_peak_concurrency(
        tmp_path, _inline_await_triage_loop
    )
    assert inline_peak == 1, (
        f"inline-await control case unexpectedly concurrent: peak={inline_peak}"
    )


def test_daemon_writes_and_removes_pid_file(tmp_path) -> None:
    _write_minimal_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        task = asyncio.create_task(daemon.run())
        pid_file = tmp_path / "state" / "angelus.pid"
        try:
            for _ in range(30):
                if pid_file.exists():
                    break
                await asyncio.sleep(0.05)
            assert pid_file.read_text(encoding="utf-8") == str(os.getpid())
            daemon.request_stop()
            await asyncio.wait_for(task, timeout=2.0)
            assert not pid_file.exists()
        finally:
            if not task.done():
                daemon.request_stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(driver())


def test_mark_triage_processing_requeues_failed_attempt(tmp_path) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    try:
        observation_id = catalog.write_observation(
            "scheduled/test", {}, {"source": "scheduled/test"}
        )
        catalog.mark_triage_processing(observation_id, "dead-link")
        catalog.mark_triage_failed(observation_id, "dead-link", "boom")
        catalog.mark_triage_processing(observation_id, "dead-link")
        row = connection.execute(
            """
            SELECT status, attempt, next_attempt_at
            FROM observation_triage
            WHERE observation_id = ? AND triager_name = 'dead-link'
            """,
            (observation_id,),
        ).fetchone()
    finally:
        connection.close()

    assert row["status"] == "processing"
    assert row["attempt"] == 2
    assert row["next_attempt_at"] is None
