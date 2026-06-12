"""Terminal 'consumed' observation status (brief-20260607-6qsq, Stage 1).

An observation leaves 'ready' exactly once: when EVERY lodged triager
matching its source is terminal (success, or failed with retries exhausted)
-- consume_observation_if_terminal, driven from the daemon's triage path --
or, for a source with no live triager, when the grace period expires
(consume_observations_without_triager, driven from the daemon's sweep loop).
These tests pin the invariants: no triager loses work because a sibling
exhausted first, no-triager observations survive the grace window for a
newly-lodged triager to claim, consumed rows leave the hot-set readers
immediately but still render in the timeline, and reprocess_source is the
documented road back from terminal. The terminal rule is also re-driven
whenever the lodged triager set shrinks (apply_lodging reconciliation +
sweep backstop) so a hot-removed blocker cannot wedge an observation in
'ready' forever.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import angelus.daemon as daemon_module
from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon
from angelus.lodging.config import load_lodging
from angelus.storage import Catalog, init_db

PINNED = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)


def _catalog(tmp_path: Path, clock: FakeClock) -> Catalog:
    connection = init_db(tmp_path / "angelus.sqlite3")
    return Catalog(connection, tmp_path, clock=clock)


def _observation_status(catalog: Catalog, observation_id: int) -> str:
    row = catalog.connection.execute(
        "SELECT status FROM observations WHERE id = ?", (observation_id,)
    ).fetchone()
    return row["status"]


def _exhaust(catalog: Catalog, observation_id: int, triager_name: str) -> None:
    """Drive one triager's retry ladder to exhaustion (MAX_RETRY_ATTEMPTS=5:
    four scheduled retries, then the terminal fifth failure)."""
    catalog.mark_triage_processing(observation_id, triager_name)
    for _ in range(4):
        assert not catalog.mark_triage_failed(observation_id, triager_name, "boom")
    assert catalog.mark_triage_failed(observation_id, triager_name, "boom")


def _write_lodging(root: Path) -> None:
    """Minimal lodging: one scheduled source `a` with one triager `ta`."""
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


def _write_extra_triager(root: Path, name: str) -> None:
    """A second triager on scheduled/a, alongside _write_lodging's `ta`."""
    (root / "triagers" / f"{name}.yaml").write_text(
        "inputs:\n  source: scheduled/a\n"
        "handler:\n  kind: python\n  path: triagers/handlers/noop.py\n",
        encoding="utf-8",
    )


# --- consume_observation_if_terminal ---------------------------------------


def test_flips_only_when_all_expected_triagers_terminal(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    expected = {"ta", "tb"}
    try:
        oid = catalog.write_observation("scheduled/a", {"x": 1}, {"source": "scheduled/a"})

        # No triage rows at all: nothing flips.
        assert catalog.consume_observation_if_terminal(oid, expected) is None
        assert _observation_status(catalog, oid) == "ready"

        # One of two terminal: still not settled.
        catalog.mark_triage_processing(oid, "ta")
        catalog.mark_triage_success(oid, "ta")
        assert catalog.consume_observation_if_terminal(oid, expected) is None
        assert _observation_status(catalog, oid) == "ready"
        assert len(catalog.ready_observations_for("tb", "scheduled/a")) == 1

        # A 'processing' row is not terminal either.
        catalog.mark_triage_processing(oid, "tb")
        assert catalog.consume_observation_if_terminal(oid, expected) is None

        # Both terminal (all successes): consumed.
        catalog.mark_triage_success(oid, "tb")
        assert catalog.consume_observation_if_terminal(oid, expected) == "consumed"
        assert _observation_status(catalog, oid) == "consumed"

        # Settled rows leave both hot-set readers immediately.
        assert catalog.ready_observations_for("ta", "scheduled/a") == []
        assert catalog.ready_observations_for("tb", "scheduled/a") == []
        assert catalog.observations_pending_triage_count() == 0

        # Repeat call on a non-ready row is a no-op.
        assert catalog.consume_observation_if_terminal(oid, expected) is None
    finally:
        catalog.connection.close()


def test_retrying_triager_blocks_consumption(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        oid = catalog.write_observation("scheduled/a", {"x": 1}, {"source": "scheduled/a"})
        catalog.mark_triage_processing(oid, "ta")
        catalog.mark_triage_success(oid, "ta")
        # tb failed once: scheduled retry, NOT terminal.
        catalog.mark_triage_processing(oid, "tb")
        assert not catalog.mark_triage_failed(oid, "tb", "boom")

        assert catalog.consume_observation_if_terminal(oid, {"ta", "tb"}) is None
        assert _observation_status(catalog, oid) == "ready"

        # Once tb's retry is due, tb can still pick the observation up.
        clock.advance(timedelta(minutes=2))
        assert len(catalog.ready_observations_for("tb", "scheduled/a")) == 1
    finally:
        catalog.connection.close()


def test_one_exhausted_triager_does_not_drop_observation_for_others(tmp_path) -> None:
    """The superseded latent bug: the FIRST triager to exhaust used to flip
    the whole row to triage_failed, dropping the observation out of `ready`
    for every OTHER triager on the same source. Now exhaustion is terminal
    per-triager only, and the whole-row transition waits for all of them."""
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    expected = {"ta", "tb"}
    try:
        oid = catalog.write_observation("scheduled/a", {"x": 1}, {"source": "scheduled/a"})
        _exhaust(catalog, oid, "ta")

        # ta exhausting must not take the observation away from tb.
        assert _observation_status(catalog, oid) == "ready"
        assert len(catalog.ready_observations_for("tb", "scheduled/a")) == 1
        assert catalog.consume_observation_if_terminal(oid, expected) is None

        # ...but ta must not re-pick its own dead work.
        assert catalog.ready_observations_for("ta", "scheduled/a") == []

        # tb finishing settles the row; one exhausted triager keeps the
        # distinct terminal status.
        catalog.mark_triage_processing(oid, "tb")
        catalog.mark_triage_success(oid, "tb")
        assert catalog.consume_observation_if_terminal(oid, expected) == "triage_failed"
        assert _observation_status(catalog, oid) == "triage_failed"
    finally:
        catalog.connection.close()


def test_empty_expected_set_never_flips(tmp_path) -> None:
    """A momentary lodging gap (triager hot-removed) must defer to the
    grace-period sweep, never consume instantly."""
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        oid = catalog.write_observation("scheduled/a", {"x": 1}, {"source": "scheduled/a"})
        assert catalog.consume_observation_if_terminal(oid, set()) is None
        assert _observation_status(catalog, oid) == "ready"
    finally:
        catalog.connection.close()


# --- consume_observations_without_triager (grace sweep) --------------------


def test_no_triager_observation_consumed_only_past_grace(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    grace = 86_400
    try:
        orphan = catalog.write_observation(
            "scheduled/orphan", {"x": 1}, {"source": "scheduled/orphan"}
        )
        covered = catalog.write_observation(
            "scheduled/a", {"x": 2}, {"source": "scheduled/a"}
        )
        assert catalog.observations_pending_triage_count() == 2

        # Inside the grace window: a newly-lodged triager could still claim
        # it, so nothing is consumed.
        clock.advance(timedelta(hours=23))
        assert catalog.consume_observations_without_triager({"scheduled/a"}, grace) == 0
        assert _observation_status(catalog, orphan) == "ready"

        # Past the grace: the orphan is consumed; the covered source's
        # observation is untouched regardless of age.
        clock.advance(timedelta(hours=2))
        assert catalog.consume_observations_without_triager({"scheduled/a"}, grace) == 1
        assert _observation_status(catalog, orphan) == "consumed"
        assert _observation_status(catalog, covered) == "ready"
        assert catalog.observations_pending_triage_count() == 1

        # Idempotent: nothing left to consume.
        assert catalog.consume_observations_without_triager({"scheduled/a"}, grace) == 0
    finally:
        catalog.connection.close()


# --- consumed rows stay visible where they must -----------------------------


def test_timeline_still_shows_consumed_observations(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        oid = catalog.write_observation("scheduled/a", {"x": 1}, {"source": "scheduled/a"})
        catalog.mark_triage_processing(oid, "ta")
        catalog.mark_triage_success(oid, "ta")
        assert catalog.consume_observation_if_terminal(oid, {"ta"}) == "consumed"

        since = "2026-06-01T00:00:00.000Z"
        until = "2026-06-02T00:00:00.000Z"
        events = [
            e for e in catalog.timeline_events(since, until)
            if e["kind"] == "observation"
        ]
        assert [(e["id"], e["status"]) for e in events] == [(oid, "consumed")]
    finally:
        catalog.connection.close()


def test_reprocess_returns_terminal_observations_to_ready(tmp_path) -> None:
    """reprocess_source flips consumed/triage_failed back to 'ready' (and
    deletes triage rows) -- ready_observations_for filters status='ready',
    so without the flip a consumed observation would be unreachable forever.
    This is the documented road back from terminal."""
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    try:
        consumed = catalog.write_observation(
            "scheduled/a", {"x": 1}, {"source": "scheduled/a"}
        )
        catalog.mark_triage_processing(consumed, "ta")
        catalog.mark_triage_success(consumed, "ta")
        assert catalog.consume_observation_if_terminal(consumed, {"ta"}) == "consumed"

        failed = catalog.write_observation(
            "scheduled/a", {"x": 2}, {"source": "scheduled/a"}
        )
        _exhaust(catalog, failed, "ta")
        assert catalog.consume_observation_if_terminal(failed, {"ta"}) == "triage_failed"

        other = catalog.write_observation(
            "scheduled/other", {"x": 3}, {"source": "scheduled/other"}
        )

        assert catalog.reprocess_source("scheduled/a") == 2
        assert _observation_status(catalog, consumed) == "ready"
        assert _observation_status(catalog, failed) == "ready"
        assert _observation_status(catalog, other) == "ready"
        picked = {r["id"] for r in catalog.ready_observations_for("ta", "scheduled/a")}
        assert picked == {consumed, failed}

        # Idempotent: a second apply finds nothing terminal and no triage rows.
        assert catalog.reprocess_source("scheduled/a") == 0
    finally:
        catalog.connection.close()


# --- daemon wiring -----------------------------------------------------------


def test_daemon_triage_success_consumes_observation(tmp_path, monkeypatch) -> None:
    """The live path: _run_triager's success arm settles the observation via
    the lodged triager set (sole triager ta on scheduled/a -> consumed)."""
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)

    async def ok(*_args, **_kwargs):
        return [], {}

    monkeypatch.setattr(daemon_module, "run_python_triager", ok)

    try:
        oid = daemon.catalog.write_observation(
            "scheduled/a", {"x": 1}, {"source": "scheduled/a"}
        )

        async def run_once() -> None:
            rows = daemon.catalog.ready_observations_for("ta", "scheduled/a")
            assert len(rows) == 1
            daemon.catalog.mark_triage_processing(rows[0]["id"], "ta")
            await daemon._triage_under_semaphore(rows[0], "ta")

        asyncio.run(run_once())
        assert _observation_status(daemon.catalog, oid) == "consumed"
        assert daemon.catalog.ready_observations_for("ta", "scheduled/a") == []
    finally:
        daemon.connection.close()


def test_daemon_consume_sweep_uses_lodged_sources_and_clock(tmp_path) -> None:
    clock = FakeClock(PINNED)
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path, clock=clock)
    try:
        orphan = daemon.catalog.write_observation(
            "scheduled/orphan", {"x": 1}, {"source": "scheduled/orphan"}
        )
        covered = daemon.catalog.write_observation(
            "scheduled/a", {"x": 2}, {"source": "scheduled/a"}
        )

        daemon._consume_sweep_once()
        assert _observation_status(daemon.catalog, orphan) == "ready"

        clock.advance(timedelta(hours=25))
        daemon._consume_sweep_once()
        assert _observation_status(daemon.catalog, orphan) == "consumed"
        # scheduled/a has a lodged triager (ta): shielded from the sweep.
        assert _observation_status(daemon.catalog, covered) == "ready"
    finally:
        daemon.connection.close()


# --- lodging-shrink reconciliation -------------------------------------------


def test_hot_removed_blocking_triager_settles_observation_on_reload(tmp_path) -> None:
    """The leak this reconciliation exists for: source with lodged {ta, tb};
    ta settles an observation, which correctly stays 'ready' waiting on tb;
    tb is then hot-removed before settling. No triage event will ever
    re-evaluate the observation (ta's terminal row excludes it from
    ready_observations_for, and the no-triager sweep skips sources with a
    lodged triager), so apply_lodging's shrink reconciliation must settle
    it at reload time -- no grace, every remaining lodged triager is
    already terminal."""
    _write_lodging(tmp_path)
    _write_extra_triager(tmp_path, "tb")
    clock = FakeClock(PINNED)
    daemon = AngelusDaemon(tmp_path, clock=clock)
    try:
        assert set(daemon.lodging.triagers) == {"ta", "tb"}
        oid = daemon.catalog.write_observation(
            "scheduled/a", {"x": 1}, {"source": "scheduled/a"}
        )
        daemon.catalog.mark_triage_processing(oid, "ta")
        daemon.catalog.mark_triage_success(oid, "ta")
        # The live triage path runs this after ta's success; tb still owns
        # work, so the observation rightly stays ready.
        daemon._maybe_consume_observation(oid, "scheduled/a")
        assert _observation_status(daemon.catalog, oid) == "ready"

        (tmp_path / "triagers" / "tb.yaml").unlink()
        asyncio.run(daemon.apply_lodging(load_lodging(tmp_path)))

        assert _observation_status(daemon.catalog, oid) == "consumed"
        assert daemon.catalog.ready_observations_for("ta", "scheduled/a") == []
    finally:
        daemon.connection.close()


def test_sweep_settles_shrunk_lodging_without_grace(tmp_path) -> None:
    """A restart with a smaller lodging never calls apply_lodging, so the
    periodic sweep is the backstop: the daemon comes up lodging only ta
    while the DB holds an observation ta already exhausted, plus a leftover
    retrying row from the no-longer-lodged tb. tb's row must neither block
    nor satisfy; the sweep settles to triage_failed with NO grace elapsed
    (every lodged triager is terminal -- nothing live to wait for), which
    also releases the stuck observations_pending_triage_count. A fresh
    un-triaged observation of the same source is untouched."""
    _write_lodging(tmp_path)  # lodges only ta
    clock = FakeClock(PINNED)
    daemon = AngelusDaemon(tmp_path, clock=clock)
    try:
        stuck = daemon.catalog.write_observation(
            "scheduled/a", {"x": 1}, {"source": "scheduled/a"}
        )
        _exhaust(daemon.catalog, stuck, "ta")
        # tb's row from the prior daemon generation: failed with a
        # scheduled retry, i.e. non-terminal -- but tb is not lodged, so it
        # must not block settlement.
        daemon.catalog.mark_triage_processing(stuck, "tb")
        assert not daemon.catalog.mark_triage_failed(stuck, "tb", "boom")
        fresh = daemon.catalog.write_observation(
            "scheduled/a", {"x": 2}, {"source": "scheduled/a"}
        )
        assert daemon.catalog.observations_pending_triage_count() == 2

        daemon._consume_sweep_once()  # clock NOT advanced: no grace involved

        assert _observation_status(daemon.catalog, stuck) == "triage_failed"
        assert _observation_status(daemon.catalog, fresh) == "ready"
        assert daemon.catalog.observations_pending_triage_count() == 1
    finally:
        daemon.connection.close()


def test_removing_last_triager_defers_to_grace_sweep(tmp_path) -> None:
    """Shrunk-to-zero is NOT the reconciliation's case: removing a source's
    last triager must leave its ready observations to the no-triager sweep's
    grace window (a newly-lodged triager can still claim recent work), never
    consume them at reload time."""
    _write_lodging(tmp_path)
    clock = FakeClock(PINNED)
    daemon = AngelusDaemon(tmp_path, clock=clock)
    try:
        oid = daemon.catalog.write_observation(
            "scheduled/a", {"x": 1}, {"source": "scheduled/a"}
        )
        (tmp_path / "triagers" / "ta.yaml").unlink()
        asyncio.run(daemon.apply_lodging(load_lodging(tmp_path)))
        assert _observation_status(daemon.catalog, oid) == "ready"

        # And the sweep's terminal-rule arm skips it too (empty lodged set
        # never flips); only the grace expiry consumes it.
        daemon._consume_sweep_once()
        assert _observation_status(daemon.catalog, oid) == "ready"
    finally:
        daemon.connection.close()


def test_no_triager_grace_env_override(tmp_path, monkeypatch) -> None:
    _write_lodging(tmp_path)
    monkeypatch.setenv("ANGELUS_NO_TRIAGER_CONSUME_GRACE_SEC", "60")
    daemon = AngelusDaemon(tmp_path)
    try:
        assert daemon._no_triager_consume_grace_sec == 60
    finally:
        daemon.connection.close()

    monkeypatch.setenv("ANGELUS_NO_TRIAGER_CONSUME_GRACE_SEC", "bogus")
    fallback = AngelusDaemon(tmp_path)
    try:
        assert (
            fallback._no_triager_consume_grace_sec
            == daemon_module.DEFAULT_NO_TRIAGER_CONSUME_GRACE_SEC
        )
    finally:
        fallback.connection.close()
