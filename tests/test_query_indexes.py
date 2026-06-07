"""EXPLAIN QUERY PLAN guards for the two hot-loop indexes added in migration
0013.

These two queries run on the daemon's single asyncio event loop and were
profiled doing unindexed scans that degrade with the observation backlog:

  * observations_pending_triage_count -- a correlated NOT EXISTS whose inner
    subquery, lacking (observation_id, status), scanned the whole triage set per
    ready observation (O(n^2); 6,809 ms measured live).
  * ready_observations_for -- filtered the entire `ready` set by source per row
    instead of seeking (source, status) (2.50 ms -> 0.05 ms with the index).

The assertions below run EXPLAIN QUERY PLAN against the catalog's ACTUAL query
text -- captured by intercepting the connection at the moment the catalog method
issues it -- not a copy pasted here. That makes the test break two ways: if the
new index is removed (the plan falls back to a status-only scan) OR if the query
is later rewritten so it stops using the index. Both are regressions of the
thing migration 0013 exists to prevent.
"""

from __future__ import annotations

import sqlite3

from angelus.storage import Catalog, init_db


class _RecordingConnection:
    """Transparent proxy over a sqlite3.Connection that remembers the (sql,
    params) of every execute() call. We hand this to a Catalog so we can recover
    the exact SQL string a method runs -- including the parameters it builds
    internally (e.g. ready_observations_for's `now`) -- and then re-run it under
    EXPLAIN QUERY PLAN. Anything other than execute() delegates straight through
    so the Catalog behaves normally."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, sql, parameters=(), /):  # noqa: ANN001 - mirrors sqlite3
        self.calls.append((sql, tuple(parameters)))
        return self._connection.execute(sql, parameters)

    def __getattr__(self, name):  # noqa: ANN001 - delegate everything else
        return getattr(self._connection, name)


def _explain(connection: sqlite3.Connection, sql: str, params: tuple) -> str:
    """Join an EXPLAIN QUERY PLAN into one searchable blob of step details."""
    rows = connection.execute("EXPLAIN QUERY PLAN " + sql, params)
    return "\n".join(row["detail"] for row in rows)


def _last_call_touching(recorder: _RecordingConnection, table: str) -> tuple[str, tuple]:
    """The most recent recorded execute whose SQL references `table` -- i.e. the
    real query the catalog method just ran."""
    for sql, params in reversed(recorder.calls):
        if table in sql:
            return sql, params
    raise AssertionError(f"catalog issued no query against {table}")


def test_pending_triage_count_subquery_seeks_by_observation_id(tmp_path) -> None:
    """The correlated anti-join must seek observation_triage by
    (observation_id=? AND status=?) using the new composite index -- not scan it
    by status alone, which is the O(n^2) plan migration 0013 removes."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    recorder = _RecordingConnection(connection)
    catalog = Catalog(recorder, tmp_path)

    catalog.observations_pending_triage_count()
    sql, params = _last_call_touching(recorder, "observation_triage")
    plan = _explain(connection, sql, params)
    connection.close()

    assert (
        "USING COVERING INDEX idx_observation_triage_observation_id_status "
        "(observation_id=? AND status=?)" in plan
    ), plan
    # The pre-0013 regression plan: the subquery falling back to the status-only
    # index. If that string appears the index is missing or unused.
    assert "idx_observation_triage_status (status=?)" not in plan, plan


def test_ready_observations_for_seeks_by_source_and_status(tmp_path) -> None:
    """ready_observations_for must seek observations by (source=? AND status=?)
    using the new composite index, with id satisfying the ORDER BY -- not scan
    the whole `ready` set via the status-only index."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    recorder = _RecordingConnection(connection)
    catalog = Catalog(recorder, tmp_path)

    catalog.ready_observations_for("triager-x", "source-y")
    sql, params = _last_call_touching(recorder, "observations")
    plan = _explain(connection, sql, params)
    connection.close()

    assert (
        "USING INDEX idx_observations_source_status_id "
        "(source=? AND status=?)" in plan
    ), plan
    # The pre-0013 regression plan: scanning by status alone.
    assert "idx_observations_status (status=?)" not in plan, plan


def test_migration_0013_creates_exactly_the_two_indexes(tmp_path) -> None:
    """0013 is purely additive: the two named indexes exist on a fresh DB and
    carry the exact columns (and order) the planner needs."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    try:
        applied = {
            row["version"]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }
        triage_cols = _index_columns(connection, "idx_observation_triage_observation_id_status")
        obs_cols = _index_columns(connection, "idx_observations_source_status_id")
    finally:
        connection.close()

    assert "0013_triage_count_and_ready_indexes.sql" in applied
    assert triage_cols == ["observation_id", "status"]
    assert obs_cols == ["source", "status", "id"]


def _index_columns(connection: sqlite3.Connection, index_name: str) -> list[str]:
    """Indexed column names, in index order, for a named index."""
    return [
        row["name"]
        for row in connection.execute(f"PRAGMA index_info('{index_name}')")
    ]
