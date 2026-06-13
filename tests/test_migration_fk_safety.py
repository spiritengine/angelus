"""The migration runner must apply table-rebuild migrations safely on a live DB.

Backstory: the 2026-06-12 deploy crash-looped because migration 0015 rebuilds
the observations table (DROP + rename to widen a CHECK) while the live DB's
findings rows still referenced observations. Under PRAGMA foreign_keys=ON the
DROP's implicit delete tripped a FOREIGN KEY constraint. The runner now applies
every migration with FK enforcement deferred and runs PRAGMA foreign_key_check
before commit -- so a rebuild that preserves referenced rows succeeds, and one
that orphans a child rolls back instead of half-applying.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from angelus.storage import init_db
from angelus.storage.migrations import DEFAULT_MIGRATIONS_DIR, migrate

# The real migrations now live inside the package; resolve via the runner's
# package-relative constant rather than re-deriving the path.
MIGRATIONS = Path(DEFAULT_MIGRATIONS_DIR)


def _migrations_through(tmp_path: Path, last: str) -> Path:
    """Copy migration files in filename order up to and including `last` into a
    fresh temp dir, so a DB can be opened at an intermediate schema version."""
    dst = tmp_path / "migrations"
    dst.mkdir()
    for path in sorted(MIGRATIONS.glob("*.sql")):
        shutil.copy(path, dst / path.name)
        if path.name == last:
            break
    return dst


def _seed_observation_with_finding(connection: sqlite3.Connection) -> None:
    """The exact production shape: a finding whose observation_id references a
    real observations row. This is what makes 0015's DROP TABLE observations a
    FK hazard on a live DB."""
    connection.execute(
        "INSERT INTO observations (id, source, status) VALUES (1, 'web.example', 'ready')"
    )
    connection.execute(
        "INSERT INTO findings "
        "(observation_id, source, type, entity, dedup_key, target_pipes, status) "
        "VALUES (1, 'web.example', 'change', 'acme.example', 'dk-1', 'now', 'ready')"
    )
    connection.commit()


def test_0015_rebuild_preserves_referencing_findings(tmp_path) -> None:
    """The outage shape: a DB at 0014 carrying a finding that references an
    observation migrates through 0015 cleanly. The observations rebuild (DROP +
    rename) would trip a FOREIGN KEY constraint under FK enforcement; the runner
    defers it and foreign_key_check confirms the referenced row survived.

    Discrimination: the finding is asserted to still JOIN to its observation
    after the rebuild and foreign_key_check is asserted empty -- so the row was
    carried across the rebuild with its id intact, not merely that the DDL ran.
    """
    pre_dir = _migrations_through(tmp_path, "0014_source_sla.sql")
    db = tmp_path / "angelus.sqlite3"
    connection = init_db(db, migrations_dir=pre_dir)
    _seed_observation_with_finding(connection)

    # Stage 0015 alongside and apply the rest of the chain through it via init_db.
    shutil.copy(
        MIGRATIONS / "0015_observation_consumed.sql",
        pre_dir / "0015_observation_consumed.sql",
    )
    migrate(connection, pre_dir)

    applied = {
        row["version"] for row in connection.execute("SELECT version FROM schema_migrations")
    }
    assert "0015_observation_consumed.sql" in applied, "0015 was recorded"

    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    assert violations == [], f"no dangling references after the rebuild: {violations}"

    # The finding still resolves to its observation through the rebuilt table.
    joined = connection.execute(
        "SELECT o.source AS osource FROM findings f "
        "JOIN observations o ON o.id = f.observation_id WHERE f.dedup_key = 'dk-1'"
    ).fetchone()
    assert joined is not None, "the finding still joins to its observation"
    assert joined["osource"] == "web.example", "the referenced observation is preserved"

    # The widened CHECK is in effect (the point of 0015): 'consumed' is storable.
    connection.execute("UPDATE observations SET status = 'consumed' WHERE id = 1")
    connection.commit()
    assert (
        connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    ), "the connection is left with foreign_keys ON"
    connection.close()


def test_migration_breaking_fk_rolls_back_and_raises(tmp_path) -> None:
    """A migration that genuinely orphans a child row is rolled back: migrate()
    raises naming the violation, schema_migrations is unchanged, the data is
    untouched, and the connection is left with foreign_keys ON.

    Discrimination: the fixture migration DELETEs the parent observation that a
    finding references -- under deferred FK enforcement the DELETE itself
    succeeds, so only the pre-commit foreign_key_check catches it. The parent row
    is asserted to still exist afterwards, proving the whole transaction (not just
    the bookkeeping insert) rolled back.
    """
    db = tmp_path / "angelus.sqlite3"
    connection = init_db(db)
    _seed_observation_with_finding(connection)

    before = {
        row["version"] for row in connection.execute("SELECT version FROM schema_migrations")
    }

    break_dir = tmp_path / "break_migrations"
    break_dir.mkdir()
    (break_dir / "9999_orphan_finding.sql").write_text(
        "DELETE FROM observations WHERE id = 1;\n", encoding="utf-8"
    )

    with pytest.raises(sqlite3.IntegrityError) as excinfo:
        migrate(connection, break_dir)
    assert "findings" in str(excinfo.value), "the violation names the offending child table"

    after = {
        row["version"] for row in connection.execute("SELECT version FROM schema_migrations")
    }
    assert after == before, "schema_migrations is unchanged after the rollback"
    assert "9999_orphan_finding.sql" not in after, "the broken migration was not recorded"

    parent = connection.execute(
        "SELECT source FROM observations WHERE id = 1"
    ).fetchone()
    assert parent is not None, "the parent observation was restored by the rollback"
    assert parent["source"] == "web.example", "the parent row is untouched"

    finding = connection.execute(
        "SELECT observation_id FROM findings WHERE dedup_key = 'dk-1'"
    ).fetchone()
    assert finding is not None and finding["observation_id"] == 1, "the finding is untouched"

    assert (
        connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    ), "a failed migration leaves the connection with foreign_keys ON"
    connection.close()
