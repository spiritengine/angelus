"""Retention prune for terminal observations (brief-20260607-6qsq, Stage 2).

The durable bound on row count and on-disk bodies: a daemon-internal periodic
prune deletes observations that are (a) in a terminal status ('consumed' or
'triage_failed'), (b) strictly older than the retention horizon
(ANGELUS_OBSERVATION_RETENTION_DAYS, default 90, measured on the injected
clock), and (c) referenced by NO finding. Triage rows + observation row go in
one commit; the body file is unlinked best-effort after. These tests pin every
invariant from the brief's Section 4 / Stage 2 -- nothing un-triaged or
retrying is touched, the finding anti-join protects the audit spine (and
transitively open incidents and replay), row+triage+body vanish together, an
orphan body is tolerated but a bodyless row is never produced, the horizon is
FakeClock-driven, and the prune is idempotent.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import angelus.daemon as daemon_module
from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon
from angelus.storage import Catalog, init_db

PINNED = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
RETENTION = 90


def _catalog(tmp_path: Path, clock: FakeClock) -> Catalog:
    connection = init_db(tmp_path / "angelus.sqlite3")
    return Catalog(connection, tmp_path, clock=clock)


def _status(catalog: Catalog, observation_id: int) -> str | None:
    row = catalog.connection.execute(
        "SELECT status FROM observations WHERE id = ?", (observation_id,)
    ).fetchone()
    return row["status"] if row is not None else None


def _make_terminal(
    catalog: Catalog, source: str, *, status: str = "consumed", payload=None
) -> int:
    """Write an observation (status 'ready', body on disk) and flip it to a
    terminal status directly -- these tests exercise the prune, not the consume
    path that produces the terminal status (test_observation_consumed covers
    that)."""
    oid = catalog.write_observation(
        source, payload or {"x": 1}, {"source": source}
    )
    catalog.connection.execute(
        "UPDATE observations SET status = ? WHERE id = ?", (status, oid)
    )
    catalog.connection.commit()
    return oid


def _body_path(catalog: Catalog, observation_id: int) -> Path:
    row = catalog.connection.execute(
        "SELECT body_ref FROM observations WHERE id = ?", (observation_id,)
    ).fetchone()
    return catalog.root / row["body_ref"]


def _add_finding(catalog: Catalog, observation_id: int) -> int:
    """Insert a finding pointing at an observation, bypassing the B30 emission
    gate (a raw INSERT) so the FK edge exists regardless of incident state."""
    cursor = catalog.connection.execute(
        """
        INSERT INTO findings (
            observation_id, source, type, entity, dedup_key, target_pipes,
            status, created_at
        )
        VALUES (?, 'scheduled/a', 'down', 'e1', 'scheduled/a:down:e1', ?,
                'ready', ?)
        """,
        (observation_id, '["now"]', catalog._clock.now_iso()),
    )
    catalog.connection.commit()
    return int(cursor.lastrowid)


def _write_lodging(root: Path) -> None:
    """Minimal lodging for the daemon-level tests: one scheduled source `a`
    with one triager `ta` (mirrors test_observation_consumed)."""
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "a.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "triagers" / "handlers").mkdir(parents=True)
    (root / "triagers" / "handlers" / "noop.py").write_text(
        "import json\nprint(json.dumps({'findings': [], 'new_state': {}}))\n",
        encoding="utf-8",
    )
    (root / "triagers" / "ta.yaml").write_text(
        "inputs:\n  source: scheduled/a\n"
        "handler:\n  kind: python\n  path: triagers/handlers/noop.py\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n",
        encoding="utf-8",
    )


# --- core predicate ----------------------------------------------------------


def test_prunes_only_old_terminal_unreferenced(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        consumed = _make_terminal(catalog, "scheduled/a", status="consumed")
        failed = _make_terminal(catalog, "scheduled/b", status="triage_failed")
        # All written at PINNED; push them past the horizon.
        clock.advance(timedelta(days=RETENTION + 1))
        # A within-horizon terminal row (created after the advance).
        recent = _make_terminal(catalog, "scheduled/c", status="consumed")
        # A still-ready (un-triaged / retrying) row, old but non-terminal.
        ready_oid = catalog.write_observation(
            "scheduled/d", {"x": 9}, {"source": "scheduled/d"}
        )
        catalog.connection.execute(
            "UPDATE observations SET created_at = ? WHERE id = ?",
            ("2026-01-01T00:00:00.000Z", ready_oid),
        )
        catalog.connection.commit()

        deleted, body_refs = catalog.prune_observations(RETENTION)

        assert deleted == 2
        assert len(body_refs) == 2
        assert _status(catalog, consumed) is None
        assert _status(catalog, failed) is None
        # Within horizon: kept. Non-terminal (ready), however old: kept.
        assert _status(catalog, recent) == "consumed"
        assert _status(catalog, ready_oid) == "ready"
    finally:
        catalog.connection.close()


def test_finding_referenced_observation_is_never_pruned(tmp_path) -> None:
    """The audit-spine guard, and the mutation target: a terminal, old
    observation a finding points at survives. Loosening the NOT EXISTS clause
    in _PRUNE_PREDICATE turns this assertion red (and, with foreign_keys=ON,
    would raise IntegrityError on the DELETE)."""
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        referenced = _make_terminal(catalog, "scheduled/a", status="consumed")
        _add_finding(catalog, referenced)
        unreferenced = _make_terminal(catalog, "scheduled/a", status="consumed")
        clock.advance(timedelta(days=RETENTION + 1))

        deleted, _ = catalog.prune_observations(RETENTION)

        assert deleted == 1
        assert _status(catalog, referenced) == "consumed"
        assert _status(catalog, unreferenced) is None
    finally:
        catalog.connection.close()


def test_horizon_boundary_is_strict_and_clock_driven(tmp_path) -> None:
    """created_at < cutoff is strict: a row stamped exactly at the horizon
    boundary survives; one a moment older prunes. The horizon is measured on
    the injected clock alone."""
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        oid = _make_terminal(catalog, "scheduled/a", status="consumed")
        # Advance exactly the horizon: cutoff == created_at, so `<` excludes it.
        clock.advance(timedelta(days=RETENTION))
        deleted, _ = catalog.prune_observations(RETENTION)
        assert deleted == 0
        assert _status(catalog, oid) == "consumed"

        # One second further: created_at is now strictly older than cutoff.
        clock.advance(timedelta(seconds=1))
        deleted, _ = catalog.prune_observations(RETENTION)
        assert deleted == 1
        assert _status(catalog, oid) is None
    finally:
        catalog.connection.close()


# --- atomicity: row + triage + body together ---------------------------------


def test_row_triage_and_body_pruned_together(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        oid = _make_terminal(catalog, "scheduled/a", status="consumed")
        catalog.mark_triage_processing(oid, "ta")
        catalog.mark_triage_success(oid, "ta")
        body = _body_path(catalog, oid)
        assert body.exists()
        triage_before = catalog.connection.execute(
            "SELECT COUNT(*) AS n FROM observation_triage WHERE observation_id = ?",
            (oid,),
        ).fetchone()["n"]
        assert triage_before == 1

        clock.advance(timedelta(days=RETENTION + 1))
        deleted, _ = catalog.prune_observations(RETENTION)

        assert deleted == 1
        assert _status(catalog, oid) is None
        assert not body.exists()
        # the per-id directory was reclaimed too
        assert not body.parent.exists()
        triage_after = catalog.connection.execute(
            "SELECT COUNT(*) AS n FROM observation_triage WHERE observation_id = ?",
            (oid,),
        ).fetchone()["n"]
        assert triage_after == 0
    finally:
        catalog.connection.close()


def test_foreign_keys_enforced_throughout(tmp_path) -> None:
    """foreign_keys=ON before and after the prune, and the guard is real: a
    raw DELETE of a finding-referenced observation raises, so the predicate's
    exclusion of those rows is what keeps the prune from ever cascading."""
    import sqlite3

    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        assert catalog.connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        referenced = _make_terminal(catalog, "scheduled/a", status="consumed")
        _add_finding(catalog, referenced)
        clock.advance(timedelta(days=RETENTION + 1))

        # The prune skips the referenced row -> no raise.
        catalog.prune_observations(RETENTION)
        assert _status(catalog, referenced) == "consumed"

        # Proof the FK edge is live: deleting it directly DOES raise.
        try:
            catalog.connection.execute(
                "DELETE FROM observations WHERE id = ?", (referenced,)
            )
            raised = False
        except sqlite3.IntegrityError:
            raised = True
        finally:
            catalog.connection.rollback()
        assert raised
        assert catalog.connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    finally:
        catalog.connection.close()


# --- body-unlink failure: orphan tolerated, never a bodyless row -------------


def test_body_unlink_failure_leaves_orphan_not_bodyless_row(tmp_path) -> None:
    """If the body unlink fails (here: the per-id directory is made
    unwritable), the row is still deleted and the prune still reports it. The
    body is orphaned on disk -- tolerated, swept by a later prune -- but the
    inverse (a surviving row with a missing body) is never produced."""
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    body = None
    id_dir = None
    try:
        oid = _make_terminal(catalog, "scheduled/a", status="consumed")
        body = _body_path(catalog, oid)
        id_dir = body.parent
        clock.advance(timedelta(days=RETENTION + 1))

        # Removing a file needs write permission on its directory; strip it so
        # the unlink inside prune_observations raises (and is swallowed).
        id_dir.chmod(0o500)

        deleted, body_refs = catalog.prune_observations(RETENTION)

        assert deleted == 1
        assert len(body_refs) == 1
        # Row gone...
        assert _status(catalog, oid) is None
        # ...body orphaned, never the reverse.
        assert body.exists()
    finally:
        if id_dir is not None:
            id_dir.chmod(0o700)
        catalog.connection.close()


# --- interactions: reprocess, replay, idempotency ----------------------------


def test_reprocess_after_prune_touches_only_survivors(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        old = _make_terminal(catalog, "scheduled/a", status="consumed")
        clock.advance(timedelta(days=RETENTION + 1))
        survivor = _make_terminal(catalog, "scheduled/a", status="consumed")

        deleted, _ = catalog.prune_observations(RETENTION)
        assert deleted == 1
        assert _status(catalog, old) is None

        # reprocess only reaches un-pruned history: the survivor returns to
        # 'ready'; the pruned row is simply gone.
        assert catalog.reprocess_source("scheduled/a") == 1
        assert _status(catalog, survivor) == "ready"
        assert _status(catalog, old) is None
        picked = {r["id"] for r in catalog.ready_observations_for("ta", "scheduled/a")}
        assert picked == {survivor}
    finally:
        catalog.connection.close()


def test_replay_unaffected_by_prune(tmp_path) -> None:
    """Replay is finding-scoped; the prune never touches findings or
    pipe_queues, so a finding still replays after unrelated observations are
    pruned."""
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        kept_obs = _make_terminal(catalog, "scheduled/a", status="consumed")
        finding_id = _add_finding(catalog, kept_obs)
        old = _make_terminal(catalog, "scheduled/b", status="triage_failed")
        clock.advance(timedelta(days=RETENTION + 1))

        deleted, _ = catalog.prune_observations(RETENTION)
        assert deleted == 1
        assert _status(catalog, old) is None
        # The finding's observation is finding-referenced -> preserved.
        assert _status(catalog, kept_obs) == "consumed"

        result = catalog.replay_finding(finding_id, {"now"})
        assert result["outcome"] == "requeued"
        assert result["pipes"] == ["now"]
    finally:
        catalog.connection.close()


def test_idempotent_second_run(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        _make_terminal(catalog, "scheduled/a", status="consumed")
        _make_terminal(catalog, "scheduled/b", status="triage_failed")
        clock.advance(timedelta(days=RETENTION + 1))

        first, _ = catalog.prune_observations(RETENTION)
        assert first == 2
        second, body_refs = catalog.prune_observations(RETENTION)
        assert second == 0
        assert body_refs == []
    finally:
        catalog.connection.close()


def test_prunable_count_matches_predicate(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        _make_terminal(catalog, "scheduled/a", status="consumed")
        referenced = _make_terminal(catalog, "scheduled/a", status="consumed")
        _add_finding(catalog, referenced)
        catalog.write_observation("scheduled/a", {"x": 1}, {"source": "scheduled/a"})
        clock.advance(timedelta(days=RETENTION + 1))

        # Only the unreferenced terminal row is prunable.
        assert catalog.prunable_observations_count(RETENTION) == 1
        catalog.prune_observations(RETENTION)
        assert catalog.prunable_observations_count(RETENTION) == 0
    finally:
        catalog.connection.close()


# --- daemon wiring -----------------------------------------------------------


def test_daemon_prune_once_records_stats(tmp_path) -> None:
    clock = FakeClock(PINNED)
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path, clock=clock)
    try:
        oid = _make_terminal(daemon.catalog, "scheduled/a", status="consumed")
        clock.advance(timedelta(days=RETENTION + 1))

        daemon._prune_once()

        assert _status(daemon.catalog, oid) is None
        assert daemon._last_prune_deleted == 1
        assert daemon._last_prune_at == clock.now_iso()

        # Idempotent at the daemon level too.
        daemon._prune_once()
        assert daemon._last_prune_deleted == 0
    finally:
        daemon.connection.close()


def test_retention_days_env_override(tmp_path, monkeypatch) -> None:
    _write_lodging(tmp_path)
    monkeypatch.setenv("ANGELUS_OBSERVATION_RETENTION_DAYS", "7")
    daemon = AngelusDaemon(tmp_path)
    try:
        assert daemon._observation_retention_days == 7
    finally:
        daemon.connection.close()

    for bad in ("bogus", "0", "-5"):
        monkeypatch.setenv("ANGELUS_OBSERVATION_RETENTION_DAYS", bad)
        fallback = AngelusDaemon(tmp_path)
        try:
            assert (
                fallback._observation_retention_days
                == daemon_module.DEFAULT_OBSERVATION_RETENTION_DAYS
            )
        finally:
            fallback.connection.close()


def test_prune_loop_registered_in_tasks(tmp_path) -> None:
    """The loop must live in self.tasks so the 13w0 shared teardown budget's
    final reap stage cancels and awaits it (the consume-sweep precedent). Boot
    the daemon, confirm the named task is registered, then request a clean stop
    bounded by wait_for so a wiring regression that wedges shutdown fails loud
    rather than hanging the suite."""
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        run_task = asyncio.create_task(daemon.run())
        try:
            registered = False
            for _ in range(400):
                if any(t.get_name() == "prune-loop" for t in daemon.tasks):
                    registered = True
                    break
                await asyncio.sleep(0.02)
            assert registered, "prune-loop never registered in self.tasks"
            daemon.request_stop()
            await asyncio.wait_for(run_task, timeout=15.0)
        finally:
            if not run_task.done():
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)
            daemon.connection.close()

    asyncio.run(driver())
