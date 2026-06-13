"""Hand-rolled SQLite migration runner."""

from __future__ import annotations

import re
import sqlite3
from importlib.resources import files
from pathlib import Path

# Resolve the migrations dir package-relative so it works identically under an
# editable install (dev tree) and a non-editable wheel install (site-packages).
# The dir lives INSIDE the angelus package (angelus/migrations/) and ships as
# package-data; importlib.resources.files("angelus") returns the package root in
# both layouts. A __file__-relative form (parents[1] / "migrations") would also
# work for unzipped installs, but files() is robust against the wheel/zip layout
# differences that made the old parents[2] / top-level "migrations" resolution
# crash-loop a non-editable install (finding-20260613-mbfc). Wheels install
# unzipped, so files() yields a concrete filesystem Path that iterdir()/glob()
# work on directly -- as_file() would only matter for a zipimport install, which
# is not how the daemon is deployed.
DEFAULT_MIGRATIONS_DIR = Path(str(files("angelus") / "migrations"))
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
        # Table-rebuild migrations (e.g. 0015) DROP a parent table whose rows are
        # still referenced by child rows on a live DB (findings -> observations).
        # Under PRAGMA foreign_keys=ON that DROP fails with a FOREIGN KEY violation,
        # which is exactly what crash-looped the 2026-06-12 deploy. SQLite's
        # documented table-rebuild procedure is to drop FK enforcement for the
        # duration and verify integrity with PRAGMA foreign_key_check before
        # committing. PRAGMA foreign_keys is a no-op inside an open transaction, so
        # the toggle has to bracket the BEGIN: off before, restored ON only after
        # the transaction closes. The restore lives in a finally so neither a
        # broken migration nor a failed integrity check can leave the connection
        # FK-off for whatever runs next.
        connection.execute("PRAGMA foreign_keys = OFF")
        try:
            connection.execute("BEGIN")
            try:
                for statement in _iter_sql_statements(path.read_text(encoding="utf-8")):
                    connection.execute(statement)
                violations = connection.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    raise sqlite3.IntegrityError(
                        f"migration {path.name} left foreign key violations: "
                        f"{_format_fk_violations(violations)}"
                    )
                connection.execute(
                    "INSERT INTO schema_migrations (version) VALUES (?)",
                    (path.name,),
                )
            except Exception:
                connection.rollback()
                raise
            else:
                connection.commit()
        finally:
            connection.execute("PRAGMA foreign_keys = ON")


def _format_fk_violations(rows: list[sqlite3.Row]) -> str:
    """Render PRAGMA foreign_key_check rows for an error message.

    Each row is (table, rowid, referred_table, fk_index); rowid is NULL for
    WITHOUT ROWID tables. Naming the offending table and the parent it dangles
    from is what makes a rolled-back migration diagnosable in production.
    """
    parts = []
    for row in rows:
        table, rowid, referred, _fk_index = row
        where = f" rowid {rowid}" if rowid is not None else ""
        parts.append(f"{table}{where} -> {referred}")
    return "; ".join(parts)


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
