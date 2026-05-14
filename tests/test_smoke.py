from __future__ import annotations

import sqlite3

import angelus
import pytest
from angelus.storage import init_db
from angelus.storage.migrations import migrate


EXPECTED_TABLES = {
    "source_fires",
    "observations",
    "findings",
    "incidents",
    "triager_state",
    "pipe_queues",
    "dispatches",
    "pipe_state",
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


def test_migration_bookkeeping_is_atomic(tmp_path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_atomicity.sql").write_text(
        "CREATE TABLE example (id INTEGER PRIMARY KEY);\n"
        "INSERT INTO missing_table VALUES (1);\n",
        encoding="utf-8",
    )

    connection = sqlite3.connect(tmp_path / "atomic.sqlite3")
    connection.row_factory = sqlite3.Row
    try:
        with pytest.raises(sqlite3.Error):
            migrate(connection, migrations_dir)

        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        applied = list(connection.execute("SELECT version FROM schema_migrations"))
    finally:
        connection.close()

    assert "example" not in tables
    assert applied == []


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
