"""B15 dead-letter-queue -- exhausted dispatches surface LOUDLY and are
REPLAYABLE, never stuck silently 'pending'.

The 2026-05-29 incident: the daily email silently failed and 9/10 findings sat
'pending' with nothing surfacing it. B15 gives the per-finding give-up its own
explicit, queryable terminal state ('dead_letter', renamed from the ambiguous
'failed' that collided with the dispatches table's transient per-channel
'failed'), surfaces WHAT was abandoned on the health surface, and leans on the
existing replay op to pull it back.

What these tests pin:
  - migration 0011 renames every existing pipe_queues 'failed' row to
    'dead_letter' (lossless, columns preserved) AND forbids 'failed' going
    forward (the CHECK asserts the invariant);
  - the exhaustion edge (record_pipe_finding_undelivered) writes 'dead_letter',
    not 'failed';
  - 'dead_letter' is TERMINAL: pending_pipe_items drops it and
    findings_pending_dispatch_by_pipe excludes it, so it is never re-drained
    until replayed (exactly as the old 'failed' terminal behaved);
  - the readers dead_letter_count / dead_letter_items return the right shape
    with enough context to be actionable;
  - the health surface (_delivery_surface + _op_health + the CLI render and the
    daemon-down fallback render) carries the dead-letter section, plain-text
    one-item-per-line (screen-reader friendly);
  - replay (catalog.replay_finding and the daemon _op_replay control op) re-arms
    a dead_letter row to 'pending';
  - ACCEPTANCE end-to-end: a finding exhausts -> lands in dead_letter (not
    'failed', not stuck 'pending') -> shows in health -> replay re-queues it ->
    the next drain delivers it -> it leaves dead-letter AND the B14
    internal/delivery incident clears.
"""

from __future__ import annotations

import asyncio
import shutil
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.clock import FakeClock
from angelus.daemon import (
    HEALTH_DEAD_LETTER_DISPLAY_LIMIT,
    AngelusDaemon,
    _delivery_surface,
)
from angelus.lodging import Channel, Pipe
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db
from angelus.storage.catalog import MAX_RETRY_ATTEMPTS
from angelus.storage.migrations import migrate

PINNED = datetime(2026, 6, 3, 12, 0, 0, tzinfo=UTC)

# Clears the longest TRUST_RETRY_DELAYS step (8h), so advancing it between drains
# re-arms the same finding in pending_pipe_items and lets one finding fail across
# enough drains to exhaust its ladder. Mirrors test_b14's _PAST_BACKOFF.
_PAST_BACKOFF = timedelta(hours=9)

MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"


# --------------------------------------------------------------------------
# Doubles / fixtures, mirroring test_b14_escalation_ladder.py.
# --------------------------------------------------------------------------


class _Recorder:
    """Channel sender double; ``fail`` is mutable so one recorder can flip from
    down to healthy between drains (down through exhaustion, healed for replay)."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    async def __call__(self, channel, *_args, **_kwargs):
        self.calls.append(channel.name)
        if self.fail:
            raise RuntimeError(f"{channel.name} transport down")


def _solo_drain(
    tmp_path, *, max_delivery_attempts: int | None = None
) -> tuple[Catalog, PipeDrain, FakeClock]:
    """A now-pipe routing to a single `email` channel on a FakeClock, so the
    per-finding backoff window can be advanced between drains. No channel name is
    hardcoded in the runner -- the policy lives entirely in this config."""
    clock = FakeClock(PINNED)
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path, clock=clock)
    channels = {
        "email": Channel(name="email", kind="email", command="patbot-email", to="x@e"),
        "push": Channel(name="push", kind="push", command="notify-pat"),
    }
    pipe = Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{severity} {type}: {entity} {body}",
        channels=["email"],
        max_delivery_attempts=max_delivery_attempts,
    )
    drain = PipeDrain(catalog, pipe, channels, tmp_path, {"now"}, clock=clock)
    return catalog, drain, clock


def _write_finding(catalog: Catalog, entity: str) -> int:
    """A NON-internal product finding routed to `now` (no B7 fan)."""
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


def _delivery_incident_open(catalog: Catalog, finding_id: int) -> bool:
    return (
        catalog.connection.execute(
            "SELECT 1 FROM incidents WHERE status = 'open' "
            "AND source = 'internal/delivery' AND type = 'delivery_exhausted' "
            "AND entity = ?",
            (str(finding_id),),
        ).fetchone()
        is not None
    )


def _exhaust(catalog, drain, clock, entity, drains) -> int:
    finding_id = _write_finding(catalog, entity)
    for i in range(drains):
        if i:
            clock.advance(_PAST_BACKOFF)
        asyncio.run(drain.drain_once())
    return finding_id


# --------------------------------------------------------------------------
# (a) Migration 0011: rename existing 'failed' -> 'dead_letter' losslessly, and
#     forbid 'failed' on pipe_queues going forward.
# --------------------------------------------------------------------------


def _migrations_through(tmp_path: Path, last: str) -> Path:
    """Copy migration files in filename order up to and including `last` into a
    fresh temp dir, so a DB can be opened at a pre-0011 schema."""
    dst = tmp_path / "migrations"
    dst.mkdir()
    for path in sorted(MIGRATIONS.glob("*.sql")):
        shutil.copy(path, dst / path.name)
        if path.name == last:
            break
    return dst


def test_migration_renames_failed_to_dead_letter(tmp_path) -> None:
    """A DB migrated only through 0010 still uses 'failed' as the pipe_queues
    exhaustion terminal. After 0011 the row is 'dead_letter' with its other
    columns (attempts, last_error) intact, and the CHECK now rejects 'failed'.

    Discrimination: the row is asserted 'failed' BEFORE 0011 (so the rename is
    real, not a no-op on an already-dead_letter row), 'dead_letter' AFTER, and a
    direct UPDATE to 'failed' raises IntegrityError -- proving 0011 both moved the
    data and tightened the constraint. attempts/last_error are checked equal
    across the migration so the rebuild is shown lossless, not just status-only.
    """
    pre_dir = _migrations_through(tmp_path, "0010_immediate_channel_attempts.sql")
    db = tmp_path / "angelus.sqlite3"
    connection = init_db(db, migrations_dir=pre_dir)
    catalog = Catalog(connection, tmp_path)
    finding_id = _write_finding(catalog, "old.example")
    # Old terminal, allowed under the pre-0011 CHECK. Set sibling columns too so
    # the rebuild's column-preservation is observable.
    connection.execute(
        "UPDATE pipe_queues SET status='failed', attempts=5, last_error='smtp down' "
        "WHERE finding_id=? AND pipe='now'",
        (finding_id,),
    )
    connection.commit()
    assert _queue_status(catalog, finding_id) == "failed", "pre-0011 terminal is 'failed'"

    # Stage 0011 alongside and apply only it (0001..0010 are already recorded).
    shutil.copy(
        MIGRATIONS / "0011_dead_letter.sql", pre_dir / "0011_dead_letter.sql"
    )
    migrate(connection, pre_dir)

    row = connection.execute(
        "SELECT status, attempts, last_error FROM pipe_queues "
        "WHERE finding_id=? AND pipe='now'",
        (finding_id,),
    ).fetchone()
    assert row["status"] == "dead_letter", "0011 renamed the terminal"
    assert row["attempts"] == 5, "attempts preserved across the table rebuild"
    assert row["last_error"] == "smtp down", "last_error preserved across the rebuild"

    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE pipe_queues SET status='failed' WHERE finding_id=?",
            (finding_id,),
        )
    connection.close()


def test_migration_leaves_non_terminal_rows_untouched(tmp_path) -> None:
    """Only 'failed' rows are rewritten: a 'pending', 'dispatched', and
    'suppressed' row all survive 0011 unchanged. Guards against a migration that
    over-broadly rewrote status.

    Discrimination: three distinct non-failed states are seeded pre-0011 and each
    is asserted identical post-0011. A CASE that matched too widely (or a typo'd
    target) would flip one of them.
    """
    pre_dir = _migrations_through(tmp_path, "0010_immediate_channel_attempts.sql")
    connection = init_db(tmp_path / "angelus.sqlite3", migrations_dir=pre_dir)
    catalog = Catalog(connection, tmp_path)
    states = {"pending": "a.com", "dispatched": "b.com", "suppressed": "c.com"}
    ids: dict[str, int] = {}
    for status, entity in states.items():
        fid = _write_finding(catalog, entity)
        connection.execute(
            "UPDATE pipe_queues SET status=? WHERE finding_id=? AND pipe='now'",
            (status, fid),
        )
        ids[status] = fid
    connection.commit()

    shutil.copy(MIGRATIONS / "0011_dead_letter.sql", pre_dir / "0011_dead_letter.sql")
    migrate(connection, pre_dir)

    for status, fid in ids.items():
        assert _queue_status(catalog, fid) == status, f"{status} row must be untouched"
    connection.close()


# --------------------------------------------------------------------------
# (b) The exhaustion edge writes 'dead_letter', and it is TERMINAL.
# --------------------------------------------------------------------------


def test_exhaustion_writes_dead_letter_not_failed(tmp_path) -> None:
    """record_pipe_finding_undelivered crosses its threshold and sets the row to
    'dead_letter'. Driven at the catalog seam so the state write is pinned
    independently of the runner.

    Discrimination: the terminal is asserted to be exactly 'dead_letter' AND not
    'failed' -- the rename is the whole point; an implementation that still wrote
    'failed' (or any other string) fails here.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    finding_id = _write_finding(catalog, "x.com")
    returns = [
        catalog.record_pipe_finding_undelivered("now", finding_id, "boom", 3)
        for _ in range(3)
    ]
    assert returns == [False, False, True], "exhausts on the 3rd call"
    assert _queue_status(catalog, finding_id) == "dead_letter"
    assert _queue_status(catalog, finding_id) != "failed"


def test_dead_letter_is_terminal_not_redrained(tmp_path) -> None:
    """A dead_letter row is dropped by pending_pipe_items and excluded from
    findings_pending_dispatch_by_pipe, so the next drain never re-picks it --
    exactly the terminal behaviour the old 'failed' had. This is the
    'not stuck pending, not re-drained' half of the acceptance.

    Discrimination: pending_pipe_items returns [] and the by-pipe pending map has
    no 'now' entry once the row is dead_letter, even though the finding itself is
    still 'ready'. An implementation that treated 'dead_letter' as drainable (or
    counted it as pending) would surface it in one of these reads.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    finding_id = _write_finding(catalog, "x.com")
    # Still pending before exhaustion -> visible to both reads.
    assert [r["finding_id"] for r in catalog.pending_pipe_items("now")] == [finding_id]
    assert catalog.findings_pending_dispatch_by_pipe() == {"now": 1}

    for _ in range(MAX_RETRY_ATTEMPTS):
        catalog.record_pipe_finding_undelivered("now", finding_id, "boom", None)
    assert _queue_status(catalog, finding_id) == "dead_letter"

    assert catalog.pending_pipe_items("now") == [], "dead_letter is not drainable"
    assert catalog.findings_pending_dispatch_by_pipe() == {}, (
        "a dead_letter row is not 'pending dispatch'"
    )


# --------------------------------------------------------------------------
# (c) Readers: dead_letter_count and dead_letter_items shape.
# --------------------------------------------------------------------------


def test_dead_letter_readers_shape_and_count(tmp_path) -> None:
    """dead_letter_count counts only dead_letter rows; dead_letter_items returns
    one actionable dict per row (finding_id, pipe, last_error, attempts,
    dead_lettered_at, source/type/entity/severity), oldest-first, and respects
    the limit.

    Discrimination: two findings are dead-lettered and a third left pending; the
    count is 2 (not 3), the items carry the dead-letterer's last_error and the
    finding's entity (so the row is actionable without a second lookup), and
    limit=1 returns exactly the oldest. A reader that counted pending rows, or
    dropped the finding context, or mis-ordered, fails one of these.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    first = _write_finding(catalog, "first.com")
    second = _write_finding(catalog, "second.com")
    _write_finding(catalog, "still-pending.com")  # stays pending

    for _ in range(MAX_RETRY_ATTEMPTS):
        catalog.record_pipe_finding_undelivered("now", first, "first boom", None)
    for _ in range(MAX_RETRY_ATTEMPTS):
        catalog.record_pipe_finding_undelivered("now", second, "second boom", None)

    assert catalog.dead_letter_count() == 2, "only the two exhausted rows count"

    items = catalog.dead_letter_items()
    assert [it["finding_id"] for it in items] == [first, second], "oldest-first"
    head = items[0]
    assert head["pipe"] == "now"
    assert head["last_error"] == "first boom"
    assert head["attempts"] == MAX_RETRY_ATTEMPTS
    assert head["entity"] == "first.com", "the finding's own identity is carried"
    assert head["type"] == "down"
    assert head["source"] == "scheduled/a"
    assert head["severity"] == "high"
    assert head["dead_lettered_at"], "the moment it dead-lettered is recorded"

    assert [it["finding_id"] for it in catalog.dead_letter_items(limit=1)] == [first], (
        "limit caps the detail list to the longest-stuck items"
    )


# --------------------------------------------------------------------------
# (d) Replay re-arms a dead_letter row to pending (catalog + control op).
# --------------------------------------------------------------------------


def test_replay_rearms_dead_letter_row(tmp_path) -> None:
    """catalog.replay_finding resets a dead_letter row to 'pending' (and zeroes
    its attempts/next_attempt_at) so the next drain re-dispatches it. The reset
    is generic ('status != pending'), so dead_letter is covered without a special
    case -- this verifies that explicitly.

    Discrimination: the row is dead_letter (with attempts burned) before replay
    and pending with attempts=0 after; the outcome is 'requeued'. A replay that
    only matched the old 'failed' literal would leave a dead_letter row stuck.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    finding_id = _write_finding(catalog, "x.com")
    for _ in range(MAX_RETRY_ATTEMPTS):
        catalog.record_pipe_finding_undelivered("now", finding_id, "boom", None)
    assert _queue_status(catalog, finding_id) == "dead_letter"

    outcome = catalog.replay_finding(finding_id, {"now"})
    assert outcome == {"outcome": "requeued", "finding_id": finding_id, "pipes": ["now"]}
    row = connection.execute(
        "SELECT status, attempts, next_attempt_at FROM pipe_queues "
        "WHERE finding_id=? AND pipe='now'",
        (finding_id,),
    ).fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 0, "replay clears the ladder so it starts fresh"
    assert row["next_attempt_at"] is None


# --------------------------------------------------------------------------
# (e) Health surface carries the dead-letter section.
# --------------------------------------------------------------------------


def test_delivery_surface_includes_dead_letter(tmp_path) -> None:
    """_delivery_surface gains a dead_letter block: an exact count plus the
    actionable items (capped at HEALTH_DEAD_LETTER_DISPLAY_LIMIT).

    Discrimination: with one dead-lettered finding the count is 1 and the item's
    finding_id/entity are present; with none it is 0 with an empty item list.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    finding_id = _write_finding(catalog, "x.com")

    empty = _delivery_surface(catalog, ["now"])["dead_letter"]
    assert empty == {"count": 0, "items": []}

    for _ in range(MAX_RETRY_ATTEMPTS):
        catalog.record_pipe_finding_undelivered("now", finding_id, "boom", None)

    surface = _delivery_surface(catalog, ["now"])["dead_letter"]
    assert surface["count"] == 1
    assert [it["finding_id"] for it in surface["items"]] == [finding_id]
    assert surface["items"][0]["entity"] == "x.com"


def test_op_health_includes_dead_letter(tmp_path) -> None:
    """The live control-socket health op carries the dead-letter section end to
    end (daemon -> _delivery_surface -> dict).

    Discrimination: after exhausting one finding the daemon's _op_health delivery
    block reports dead_letter count 1 with the finding listed. A surface wired
    only into the CLI render (not the daemon op) would leave this empty.
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        finding_id = _write_finding(daemon.catalog, "x.com")
        for _ in range(MAX_RETRY_ATTEMPTS):
            daemon.catalog.record_pipe_finding_undelivered(
                "now", finding_id, "boom", None
            )
        result = asyncio.run(daemon._op_health({}))
        dead_letter = result["delivery"]["dead_letter"]
        assert dead_letter["count"] == 1
        assert [it["finding_id"] for it in dead_letter["items"]] == [finding_id]
    finally:
        daemon.connection.close()


def _write_lodging(root: Path) -> None:
    """Minimal now(push)+daily(push) lodging for the daemon-level tests."""
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


# --------------------------------------------------------------------------
# (f) CLI render: plain text, one item per line, screen-reader friendly.
# --------------------------------------------------------------------------


def test_render_dead_letter_is_plain_and_one_per_line(capsys) -> None:
    """The dead-letter render is plain text (no tables/columns), one item per
    line, count first, and each line ends with the actionable replay finding id.

    Discrimination: the count line and a per-finding line are both present, the
    line names the entity/pipe/error and ends with 'replay finding <id>', and the
    output has no pipe glyphs or tabs. A table/column render, or one that dropped
    the replay id, fails.
    """
    from angelus.cli import _render_delivery

    _render_delivery(
        {
            "last_successful_send": {"now": None},
            "failed_dispatches": {"window_hours": 24, "count": 0},
            "open_internal_incidents": 1,
            "dead_letter": {
                "count": 2,
                "items": [
                    {
                        "finding_id": 7,
                        "pipe": "now",
                        "entity": "example.com",
                        "type": "down",
                        "last_error": "smtp down",
                        "attempts": 5,
                        "dead_lettered_at": "2026-06-03T12:00:00.000Z",
                        "source": "scheduled/a",
                        "severity": "high",
                    }
                ],
            },
        }
    )
    out = capsys.readouterr().out
    lines = out.splitlines()
    assert "  dead-letter (exhausted, replayable): 2" in lines
    assert any(
        "example.com" in ln and "now" in ln and "smtp down" in ln
        and ln.rstrip().endswith("replay finding 7")
        for ln in lines
    ), f"expected an actionable one-per-line dead-letter row; got {lines}"
    assert "|" not in out
    assert "\t" not in out


def test_render_dead_letter_count_without_items(capsys) -> None:
    """A count with no item list (e.g. all items dropped, or a partial dict)
    still renders the count line and emits no per-item lines.

    Discrimination: the count line is present and no 'replay finding' line is --
    so a non-empty count never silently renders as nothing.
    """
    from angelus.cli import _render_delivery

    _render_delivery({"dead_letter": {"count": 3, "items": []}})
    out = capsys.readouterr().out
    assert "  dead-letter (exhausted, replayable): 3" in out
    assert "replay finding" not in out


def test_render_delivery_missing_dead_letter_defaults_to_zero(capsys) -> None:
    """An old/partial delivery dict with no dead_letter key renders 'count: 0'
    rather than crashing -- the render guards the key like the others.

    Discrimination: the count line shows 0; a KeyError or a 'None' render would
    fail. Pins backward-compat with a pre-B15 surface dict.
    """
    from angelus.cli import _render_delivery

    _render_delivery({"last_successful_send": {"now": None}})
    out = capsys.readouterr().out
    assert "  dead-letter (exhausted, replayable): 0" in out


def test_display_limit_is_positive() -> None:
    """The inline render cap is a sane positive bound; the count is always exact
    regardless, so this only guards the detail list from flooding."""
    assert HEALTH_DEAD_LETTER_DISPLAY_LIMIT > 0


# --------------------------------------------------------------------------
# (g) ACCEPTANCE end-to-end: exhaust -> dead_letter -> health -> replay ->
#     deliver -> leaves dead-letter and the B14 incident clears.
# --------------------------------------------------------------------------


def test_acceptance_exhaust_surface_replay_deliver(tmp_path, monkeypatch) -> None:
    """The full B15 contract through the real runner:

    1. email (the now-pipe's sole channel) fails every drain; with
       max_delivery_attempts=2 the finding exhausts on the 2nd drain and lands in
       'dead_letter' -- NOT 'failed', and NOT stuck 'pending'.
    2. The dead-letter shows on the health surface (count 1, the finding listed
       with its entity and last_error) -- it is LOUD, not silently pending. The
       findings-pending-dispatch surface does NOT list it (terminal, not pending).
    3. The B14 rung-3 internal/delivery incident is open (low threshold keeps the
       per-channel counter < 5, so email stays healthy and the incident is the
       only open internal one).
    4. `replay` (via the daemon _op_replay control op) re-arms the row to pending.
    5. email recovers; the next drain delivers the content -> the row leaves
       dead-letter (now 'dispatched'), the health dead-letter count returns to 0,
       and the internal/delivery incident clears (belfry goes green).

    Discrimination: each stage asserts the state transition AND its negation --
    dead_letter is checked to be neither 'failed' nor 'pending'; the health
    surface is checked to list it then NOT list it; the incident open then closed.
    A regression at any rung (wrong terminal, not surfaced, replay not re-arming
    dead_letter, incident not clearing) flips exactly one assertion.
    """
    catalog, drain, clock = _solo_drain(tmp_path, max_delivery_attempts=2)
    email = _Recorder(fail=True)  # down through exhaustion, healed before replay
    monkeypatch.setattr(pipe_runner, "send_email", email)
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder())

    # 1. Exhaust -> dead_letter (not failed, not pending).
    finding_id = _exhaust(catalog, drain, clock, "example.com", 2)
    assert email.calls == ["email", "email"], "the primary was tried each drain"
    status = _queue_status(catalog, finding_id)
    assert status == "dead_letter", f"the finding must dead-letter, got {status!r}"
    assert status != "failed"
    assert status != "pending"

    # 2. LOUD on health: listed in the dead-letter section, absent from pending.
    surface = _delivery_surface(catalog, ["now"])
    assert surface["dead_letter"]["count"] == 1
    item = surface["dead_letter"]["items"][0]
    assert item["finding_id"] == finding_id
    assert item["entity"] == "example.com"
    assert item["last_error"], "the operator sees WHY it was abandoned"
    assert "now" not in surface["last_successful_send"] or (
        surface["last_successful_send"]["now"] is None
    ), "never successfully delivered"
    # The PRODUCT finding must NOT sit silently 'pending' -- it is terminal. (The
    # rung-3 internal/delivery alarm finding rung 3 just wrote IS legitimately
    # pending here, awaiting its own next drain; that is the loud signal, not the
    # silent anti-pattern, so we scope this to the product finding's own row.)
    pending_ids = [r["finding_id"] for r in catalog.pending_pipe_items("now", limit=None)]
    assert finding_id not in pending_ids, (
        "dead-lettered content must NOT read as silently 'pending' -- the anti-pattern"
    )

    # 3. The B14 rung-3 incident is open and belfry would page on it.
    assert _delivery_incident_open(catalog, finding_id), "rung 3 opened the incident"
    assert not catalog.is_channel_unhealthy("email"), (
        "the low per-finding threshold keeps the channel healthy so replay can redeliver"
    )

    # 4. Replay re-arms the dead_letter row (the live `angelus replay <fid>` path,
    #    catalog.replay_finding, which the daemon _op_replay control op wraps --
    #    the op delegation is pinned separately in test_op_replay_rearms_dead_letter).
    outcome = catalog.replay_finding(finding_id, {"now"})
    assert outcome["outcome"] == "requeued"
    assert _queue_status(catalog, finding_id) == "pending", "replay re-armed the row"

    # 5. The transport recovers; the next drain delivers and clears everything.
    email.fail = False
    asyncio.run(drain.drain_once())
    assert email.calls[-1] == "email", "the recovered channel redelivered the content"
    assert _queue_status(catalog, finding_id) == "dispatched", "it left dead-letter"
    assert catalog.dead_letter_count() == 0, "the dead-letter surface is empty again"
    assert not _delivery_incident_open(catalog, finding_id), (
        "a successful redelivery clears the B14 internal/delivery incident"
    )


def test_op_replay_rearms_dead_letter(tmp_path) -> None:
    """The wired daemon _op_replay control op (the `angelus replay <fid>` server
    side) re-arms a dead_letter row to 'pending'. Driven on the daemon's own
    single connection: a finding is written and exhausted through daemon.catalog,
    then replayed through daemon._op_replay.

    Discrimination: the op returns {"outcome": "requeued", ...} and the row flips
    dead_letter -> pending. Also pins the op's arg validation -- a non-integer
    finding_id raises ValueError (caught upstream and returned as an error,
    never crashing the daemon). A replay op that didn't reach dead_letter rows,
    or skipped validation, fails one of these.
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        finding_id = _write_finding(daemon.catalog, "x.com")
        for _ in range(MAX_RETRY_ATTEMPTS):
            daemon.catalog.record_pipe_finding_undelivered(
                "now", finding_id, "boom", None
            )
        assert _queue_status(daemon.catalog, finding_id) == "dead_letter"

        result = asyncio.run(daemon._op_replay({"finding_id": finding_id}))
        assert result["outcome"] == "requeued"
        assert _queue_status(daemon.catalog, finding_id) == "pending"

        with pytest.raises(ValueError, match="finding_id"):
            asyncio.run(daemon._op_replay({"finding_id": "not-an-int"}))
    finally:
        daemon.connection.close()
