from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from angelus.daemon import _cadence_seconds
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
    with pytest.raises(ValueError, match="cron"):
        _cadence_seconds("0 8 * * *")
    with pytest.raises(ValueError, match="positive"):
        _cadence_seconds("0s")


def test_triage_semaphore_bounds_concurrency() -> None:
    """The triage loop must fan out so the semaphore actually bounds parallelism.
    The slice-1-merged version awaited inline, making the semaphore a no-op."""
    sem_size = 3
    job_count = 8
    sem = asyncio.Semaphore(sem_size)
    in_flight = 0
    peak = 0
    release = asyncio.Event()

    async def fake_handler() -> None:
        nonlocal in_flight, peak
        async with sem:
            in_flight += 1
            peak = max(peak, in_flight)
            await release.wait()
            in_flight -= 1

    async def driver() -> None:
        tasks = [asyncio.create_task(fake_handler()) for _ in range(job_count)]
        await asyncio.sleep(0.05)
        assert in_flight == sem_size, f"semaphore did not cap at {sem_size}"
        release.set()
        await asyncio.gather(*tasks)

    asyncio.run(driver())
    assert peak == sem_size


def test_mark_triage_processing_raises_on_duplicate(tmp_path) -> None:
    """Slice 1's loop never double-marks (the JOIN filters processed rows), so
    swallowing IntegrityError hid logic bugs. After cleanup, a duplicate raises
    loudly so a regression in the routing surfaces immediately."""
    import sqlite3

    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    try:
        observation_id = catalog.write_observation(
            "scheduled/test", {}, {"source": "scheduled/test"}
        )
        catalog.mark_triage_processing(observation_id, "dead-link")
        with pytest.raises(sqlite3.IntegrityError):
            catalog.mark_triage_processing(observation_id, "dead-link")
    finally:
        connection.close()
