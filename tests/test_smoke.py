from __future__ import annotations

import sqlite3

import angelus
from angelus.storage import init_db


EXPECTED_TABLES = {
    "source_fires",
    "observations",
    "findings",
    "incidents",
    "triager_state",
    "pipe_queues",
    "dispatches",
    "dep_health",
    "schedule_registry",
    "backoff_store",
}


def test_import_and_migrate_temp_db(tmp_path) -> None:
    assert angelus.__version__

    db_path = tmp_path / "angelus.sqlite3"
    connection = init_db(db_path)
    try:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        observation_columns = _columns(connection, "observations")
        finding_columns = _columns(connection, "findings")
    finally:
        connection.close()

    assert journal_mode == "wal"
    assert EXPECTED_TABLES <= tables
    assert "status" in observation_columns
    assert "status" in finding_columns


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
