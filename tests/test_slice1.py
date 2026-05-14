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
