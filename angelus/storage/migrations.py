"""Hand-rolled SQLite migration runner."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MIGRATIONS_DIR = PROJECT_ROOT / "migrations"
MIGRATION_RE = re.compile(r"^\d{4}_.+\.sql$")


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open a SQLite connection and put file-backed databases in WAL mode."""
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    return connection


def migrate(
    connection: sqlite3.Connection,
    migrations_dir: str | Path = DEFAULT_MIGRATIONS_DIR,
) -> None:
    """Apply SQL migrations from a directory in filename order."""
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        )
        """
    )
    applied = {
        row["version"]
        for row in connection.execute("SELECT version FROM schema_migrations")
    }

    migration_paths = sorted(
        path
        for path in Path(migrations_dir).iterdir()
        if path.is_file() and MIGRATION_RE.match(path.name)
    )
    for path in migration_paths:
        if path.name in applied:
            continue
        connection.execute("BEGIN")
        try:
            for statement in _iter_sql_statements(path.read_text(encoding="utf-8")):
                connection.execute(statement)
            connection.execute(
                "INSERT INTO schema_migrations (version) VALUES (?)",
                (path.name,),
            )
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()


def _iter_sql_statements(sql: str) -> list[str]:
    """Split a migration file into complete SQLite statements."""
    statements: list[str] = []
    buffer: list[str] = []
    for line in sql.splitlines():
        buffer.append(line)
        candidate = "\n".join(buffer).strip()
        if candidate and sqlite3.complete_statement(candidate):
            statements.append(candidate)
            buffer.clear()

    trailing = "\n".join(buffer).strip()
    if trailing:
        raise sqlite3.ProgrammingError("incomplete SQL statement in migration")

    return statements


def init_db(
    db_path: str | Path,
    migrations_dir: str | Path = DEFAULT_MIGRATIONS_DIR,
) -> sqlite3.Connection:
    """Open a database, enable WAL, and apply pending migrations."""
    connection = connect(db_path)
    migrate(connection, migrations_dir)
    return connection
