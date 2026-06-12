"""B14 durability fix (brief-20260604-f324) -- a dead_letter row can never
silently outlive its rung-3 internal/delivery incident across a daemon crash.

The defect: the immediate-path exhaustion edge wrote its two durable halves in
two separate commits -- record_pipe_finding_undelivered committed the
pipe_queues row's flip to the terminal 'dead_letter', then the runner
separately committed the internal/delivery `delivery_exhausted` incident via
write_internal_finding. A hard crash (SIGKILL/host loss) between the commits
left a dead_letter row with NO paired incident. Because the pending reads
(pending_pipe_items / findings_pending_dispatch_by_pipe) exclude dead_letter,
the edge never re-fired for that row on restart, so the incident was never
emitted and belfry -- which pages off-box on open internal/* incidents --
stayed GREEN while the content sat abandoned. The exact silent-failure class
angelus exists to prevent.

Two halves, both pinned here:
  - ORDERING: the emission moved inside record_pipe_finding_undelivered
    (page_known_pipes) and commits BEFORE the row flips, so a crash between
    the commits leaves the self-healing half-state (incident open, row still
    retryable) and never the silent one (terminal row, no incident);
  - STARTUP RE-PAIR: daemon._reconcile_dead_letter_incidents re-emits the
    incident for any dead_letter row without one -- healing state written
    before the ordering fix, or by whatever failure mode in-process ordering
    cannot survive -- gated so an already-paired row is untouched and an
    internal/* row stays intentionally unpaired.

Helpers mirror test_b15_dead_letter.py / test_b14_escalation_ladder.py.
"""

from __future__ import annotations

import asyncio
import importlib.util
from datetime import UTC, datetime
from pathlib import Path

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db
from angelus.storage.catalog import MAX_RETRY_ATTEMPTS

PINNED = datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC)

REPO_ROOT = Path(__file__).resolve().parents[1]
BELFRY_PATH = REPO_ROOT / "belfry" / "belfry.py"


# --------------------------------------------------------------------------
# Doubles / fixtures.
# --------------------------------------------------------------------------


class _Recorder:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    async def __call__(self, channel, *_args, **_kwargs):
        self.calls.append(channel.name)
        if self.fail:
            raise RuntimeError(f"{channel.name} transport down")


def _write_lodging(root: Path) -> None:
    """Minimal now(push) lodging for the daemon-level tests."""
    (root / "pipes").mkdir(parents=True)
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )


def _write_finding(catalog: Catalog, entity: str) -> int:
    """A NON-internal product finding routed to `now`."""
    observation_id = catalog.write_observation(
        "scheduled/a", {}, {"source": "scheduled/a"}
    )
    return catalog.write_finding(
        observation_id,
        {
            "source": "scheduled/a",
            "type": "down",
            "entity": entity,
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now"},
    )


def _queue_status(catalog: Catalog, finding_id: int) -> str | None:
    row = catalog.connection.execute(
        "SELECT status FROM pipe_queues WHERE finding_id = ? AND pipe = 'now'",
        (finding_id,),
    ).fetchone()
    return None if row is None else row["status"]


def _open_delivery_incidents(catalog: Catalog) -> list[str]:
    """Entities of open internal/delivery delivery_exhausted incidents."""
    rows = catalog.connection.execute(
        "SELECT entity FROM incidents WHERE status = 'open' "
        "AND source = 'internal/delivery' AND type = 'delivery_exhausted' "
        "ORDER BY id"
    ).fetchall()
    return [row["entity"] for row in rows]


def _delivery_finding_count(catalog: Catalog) -> int:
    return int(
        catalog.connection.execute(
            "SELECT COUNT(*) AS n FROM findings WHERE source = 'internal/delivery'"
        ).fetchone()["n"]
    )


def _orphan_dead_letter(catalog: Catalog, entity: str) -> int:
    """Construct the crash-window half-state directly: a terminal dead_letter
    row with NO internal/delivery incident -- exactly what a SIGKILL between
    the pre-fix edge's two commits left on disk. The bare (no paging) call IS
    the pre-fix row flip, so this is state construction, not simulation
    hand-waving."""
    finding_id = _write_finding(catalog, entity)
    for _ in range(MAX_RETRY_ATTEMPTS):
        catalog.record_pipe_finding_undelivered("now", finding_id, "boom", None)
    assert _queue_status(catalog, finding_id) == "dead_letter"
    assert str(finding_id) not in _open_delivery_incidents(catalog), (
        "precondition: the crash window left no incident for this finding"
    )
    return finding_id


def _load_belfry():
    spec = importlib.util.spec_from_file_location("belfry_under_test", BELFRY_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# (a) Startup re-pair: an orphaned dead_letter row gets its incident back, and
#     belfry's open-internal read goes red.
# --------------------------------------------------------------------------


def test_startup_repair_re_emits_missing_incident(tmp_path) -> None:
    """The crash-window state (dead_letter row, no incident) is invisible to
    every live edge: the pending reads exclude the row, so no drain re-fires
    exhaustion, and belfry's open-internal read counts zero (GREEN over
    abandoned content). _reconcile_dead_letter_incidents re-emits the
    incident, flipping belfry red -- proven against belfry's REAL
    failure_surface read, not just the catalog mirror.

    Discrimination: before the sweep the orphan is asserted green on the very
    read belfry pages from (failure_surface returns None) AND absent from the
    pending reads (so 'the edge re-fires eventually' cannot be the rescue);
    after the sweep exactly the (internal/delivery, delivery_exhausted,
    str(fid)) incident is open and failure_surface reports it. On pre-fix
    code the sweep does not exist and the orphan stays green forever.
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    belfry = _load_belfry()
    db_path = tmp_path / "state" / "angelus.sqlite3"
    watermark = tmp_path / "state" / "belfry-failcheck"
    try:
        finding_id = _orphan_dead_letter(daemon.catalog, "x.com")

        # The orphan is the SILENT half-state: terminal, unpaged, unreachable.
        assert daemon.catalog.pending_pipe_items("now") == [], (
            "dead_letter is excluded from pending -- no drain will re-fire it"
        )
        assert daemon.catalog.findings_pending_dispatch_by_pipe() == {}
        assert daemon.catalog.open_internal_incident_count() == 0
        assert belfry.failure_surface(db_path, watermark) is None, (
            "belfry is GREEN over the abandoned content -- the defect"
        )

        daemon._reconcile_dead_letter_incidents()

        assert _open_delivery_incidents(daemon.catalog) == [str(finding_id)], (
            "the sweep re-paired the row with its rung-3 incident"
        )
        assert daemon.catalog.open_internal_incident_count() == 1
        reason = belfry.failure_surface(db_path, watermark)
        assert reason is not None and "internal/delivery" in reason, (
            f"belfry must page off-box on the re-paired incident; got {reason!r}"
        )
    finally:
        daemon.connection.close()


def test_startup_repair_is_idempotent_and_skips_paired_rows(tmp_path) -> None:
    """A dead_letter row whose incident IS open (the normal post-exhaustion
    state, here produced by the live paging edge itself) is untouched: no
    second incident, no second internal/delivery finding row. Running the
    sweep twice over an orphan likewise converges to exactly one of each.

    Discrimination: finding-row and open-incident counts are pinned to exact
    values after each pass -- a sweep missing the open-incident pre-filter
    (or a gate regression) would grow one of them.
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        # Normal path at the catalog seam: the live edge pages as it flips.
        finding_id = _write_finding(daemon.catalog, "paired.com")
        for _ in range(MAX_RETRY_ATTEMPTS):
            daemon.catalog.record_pipe_finding_undelivered(
                "now", finding_id, "boom", None, page_known_pipes={"now"}
            )
        assert _queue_status(daemon.catalog, finding_id) == "dead_letter"
        assert _open_delivery_incidents(daemon.catalog) == [str(finding_id)], (
            "the normal edge emits exactly one incident"
        )
        assert _delivery_finding_count(daemon.catalog) == 1

        daemon._reconcile_dead_letter_incidents()
        daemon._reconcile_dead_letter_incidents()

        assert _open_delivery_incidents(daemon.catalog) == [str(finding_id)], (
            "an already-paired row must not gain a duplicate incident"
        )
        assert _delivery_finding_count(daemon.catalog) == 1, (
            "no duplicate internal/delivery finding row either"
        )

        # And over a genuine orphan alongside it: one repair on the first
        # pass, a no-op on the second -- the paired row stays at one
        # throughout.
        orphan_id = _orphan_dead_letter(daemon.catalog, "orphan.com")
        daemon._reconcile_dead_letter_incidents()
        daemon._reconcile_dead_letter_incidents()
        assert sorted(_open_delivery_incidents(daemon.catalog)) == sorted(
            [str(finding_id), str(orphan_id)]
        ), "the orphan gains exactly one incident; the paired row gains none"
        assert _delivery_finding_count(daemon.catalog) == 2
    finally:
        daemon.connection.close()


def test_startup_repair_skips_internal_findings(tmp_path) -> None:
    """An internal/* finding's dead_letter row is INTENTIONALLY unpaired (the
    live edge guards rung 3 on `not _is_internal`: its own original incident
    already pages, and a delivery incident for it would be never-clearing
    cruft). The sweep must honour the same guard.

    Discrimination: the internal finding's row is asserted dead_letter (so
    the skip is exercised, not vacuously absent) and after the sweep there is
    still no internal/delivery incident of any entity -- while the original
    internal/render incident stays open as the off-box signal.
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        finding_id = daemon.catalog.write_internal_finding(
            "internal/render", "render_failed", "digest", "boom", {"now"}
        )
        for _ in range(MAX_RETRY_ATTEMPTS):
            daemon.catalog.record_pipe_finding_undelivered(
                "now", finding_id, "boom", None
            )
        assert _queue_status(daemon.catalog, finding_id) == "dead_letter"

        daemon._reconcile_dead_letter_incidents()

        assert _open_delivery_incidents(daemon.catalog) == [], (
            "an internal finding's dead_letter row must stay unpaired"
        )
        row = daemon.catalog.connection.execute(
            "SELECT 1 FROM incidents WHERE status = 'open' "
            "AND source = 'internal/render' AND entity = 'digest'"
        ).fetchone()
        assert row is not None, "the ORIGINAL internal incident is the page"
    finally:
        daemon.connection.close()


# --------------------------------------------------------------------------
# (b) Ordering: the incident commits BEFORE the dead_letter flip, so a crash
#     between the two leaves the retryable half-state, never the silent one.
# --------------------------------------------------------------------------


class _CrashBeforeFlip:
    """Connection proxy that dies on the dead_letter UPDATE -- the hard-crash
    seam between the exhaustion edge's two writes. Everything else passes
    through, so the incident emission ahead of the flip runs for real."""

    def __init__(self, real) -> None:
        self._real = real
        self.armed = False

    def execute(self, sql, *args):
        if self.armed and "'dead_letter'" in sql and sql.lstrip().startswith(
            "UPDATE pipe_queues"
        ):
            raise RuntimeError("simulated crash before the dead_letter flip")
        return self._real.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_incident_commits_before_dead_letter_flip(tmp_path) -> None:
    """Crash the threshold-crossing call at the exact seam between its two
    writes and assert which half survived: the incident IS committed, the row
    is NOT dead_letter. That is the self-healing half-state -- the row stays
    retryable, so the next drain re-walks the edge; re-exhaustion completes
    the flip with the B30 gate dropping the duplicate emission (asserted
    below), and a redelivery instead would close the incident via the
    runner's existing clearance. The pre-fix order (flip committed first,
    incident second) inverts both assertions into the silent orphan.

    Discrimination: pre-fix code rejects page_known_pipes outright
    (TypeError); a mutant that flips before emitting leaves status
    'dead_letter' with no incident and fails both halves; a mutant that
    re-emits on the completing call breaks the exactly-one incident count.
    """
    clock = FakeClock(PINNED)
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path, clock=clock)
    finding_id = _write_finding(catalog, "x.com")

    # Walk to one short of the threshold, then arm the crash for the
    # crossing call only (the proxy must not eat the earlier backoff writes).
    for _ in range(MAX_RETRY_ATTEMPTS - 1):
        catalog.record_pipe_finding_undelivered(
            "now", finding_id, "boom", None, page_known_pipes={"now"}
        )
    assert _open_delivery_incidents(catalog) == [], "no premature emission"
    proxy = _CrashBeforeFlip(connection)
    catalog.connection = proxy
    proxy.armed = True
    with pytest.raises(RuntimeError, match="simulated crash"):
        catalog.record_pipe_finding_undelivered(
            "now", finding_id, "final boom", None, page_known_pipes={"now"}
        )
    catalog.connection = connection

    assert _open_delivery_incidents(catalog) == [str(finding_id)], (
        "the incident must already be durable when the flip can still be lost"
    )
    status = _queue_status(catalog, finding_id)
    assert status != "dead_letter", (
        f"the flip must not precede the incident commit; row is {status!r}"
    )

    # Convergence: the row is still retryable, so the edge re-fires; the gate
    # dedups the emission and the flip completes. Exactly one incident, one
    # internal/delivery finding row.
    exhausted = catalog.record_pipe_finding_undelivered(
        "now", finding_id, "boom again", None, page_known_pipes={"now"}
    )
    assert exhausted is True
    assert _queue_status(catalog, finding_id) == "dead_letter"
    assert _open_delivery_incidents(catalog) == [str(finding_id)]
    assert _delivery_finding_count(catalog) == 1


def test_bare_call_still_flips_without_paging(tmp_path) -> None:
    """page_known_pipes=None (the default, and what the runner passes for
    internal/* findings) preserves the bare row flip: dead_letter, no
    incident. Pins that the new emission is opt-in, so the live internal
    guard and every direct catalog caller keep their semantics.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    finding_id = _write_finding(catalog, "x.com")
    for _ in range(MAX_RETRY_ATTEMPTS):
        catalog.record_pipe_finding_undelivered("now", finding_id, "boom", None)
    assert _queue_status(catalog, finding_id) == "dead_letter"
    assert _open_delivery_incidents(catalog) == []


# --------------------------------------------------------------------------
# (c) The repaired incident clears through the EXISTING recovery edge: replay
#     re-arms the row, the next drain delivers, the clearance closes it.
# --------------------------------------------------------------------------


def test_replay_clears_repaired_incident(tmp_path, monkeypatch) -> None:
    """End to end over the daemon's own catalog: orphan -> startup re-pair
    (incident open, belfry red) -> `angelus replay <fid>` (_op_replay) ->
    a real drain delivers over a healthy channel -> the row leaves
    dead-letter and the re-paired incident closes (belfry green again).

    Discrimination: the incident a RE-PAIRED emission opened must be exactly
    what the existing `if delivered` clearance keys on (entity =
    str(finding_id)) -- a re-pair that drifted the key (wrong type, wrong
    entity shape) would leave the incident open here and belfry red forever
    after recovery.
    """
    _write_lodging(tmp_path)
    clock = FakeClock(PINNED)
    daemon = AngelusDaemon(tmp_path, clock=clock)
    try:
        finding_id = _orphan_dead_letter(daemon.catalog, "x.com")
        daemon._reconcile_dead_letter_incidents()
        assert _open_delivery_incidents(daemon.catalog) == [str(finding_id)]

        result = asyncio.run(daemon._op_replay({"finding_id": finding_id}))
        assert result["outcome"] == "requeued"
        assert _queue_status(daemon.catalog, finding_id) == "pending"

        # Drain `now` for real over a healthy push channel.
        push = _Recorder()
        drain = PipeDrain(
            daemon.catalog,
            daemon.lodging.pipes["now"],
            daemon.lodging.channels,
            tmp_path,
            set(daemon.lodging.pipes),
            clock=clock,
        )
        monkeypatch.setattr(pipe_runner, "send_push", push)
        asyncio.run(drain.drain_once())

        assert "push" in push.calls, "the replayed content was redelivered"
        assert _queue_status(daemon.catalog, finding_id) == "dispatched"
        assert daemon.catalog.dead_letter_count() == 0
        assert _open_delivery_incidents(daemon.catalog) == [], (
            "redelivery must close the re-paired incident -- belfry green"
        )
    finally:
        daemon.connection.close()
