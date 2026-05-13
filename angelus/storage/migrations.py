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
        with connection:
            connection.executescript(path.read_text(encoding="utf-8"))
            connection.execute(
                "INSERT INTO schema_migrations (version) VALUES (?)",
                (path.name,),
            )


def init_db(
    db_path: str | Path,
    migrations_dir: str | Path = DEFAULT_MIGRATIONS_DIR,
) -> sqlite3.Connection:
    """Open a database, enable WAL, and apply pending migrations."""
    connection = connect(db_path)
    migrate(connection, migrations_dir)
    return connection
