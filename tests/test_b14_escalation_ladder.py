"""B14 escalation-ladder -- failure gets LOUDER over time, never loops quietly.

The complete ladder the immediate (`now`) path walks for an undelivered finding:
  rung 1 -- retry with backoff (the per-FINDING redelivery counter on
            pipe_queues + next_attempt_at);
  rung 2 -- after a channel degrades, fail the finding over to that channel's
            backup (B13, exercised in test_b13_transport_failover.py);
  rung 3 -- after the finding exhausts its retry budget WITHOUT ever reaching
            ANY transport (primary or any backup), PAGE OUT-OF-BAND (this item).

Rung 3's signal (out-of-band model 3): when
Catalog.record_pipe_finding_undelivered returns True (the finding crossed its
threshold to status='failed' undelivered) on the `not delivered` reconciliation,
the daemon logs an ERROR and raises a DISTINCT, durable internal finding --
source internal/delivery, type delivery_exhausted, entity = the finding id. That
opens an incident belfry's open-internal-incident read carries off-box; the
daemon never pings a healthcheck itself. The incident is per-FINDING and durable
("we permanently gave up delivering THIS content"), deliberately distinct from
the per-CHANNEL, transient internal/dispatch channel_unhealthy alarm rung 2
raises ("a transport is degraded").

These tests pin:
  - the acceptance: a persistently-failing dispatch walks the full ladder and
    ends with the rung-3 durable incident opened + the ERROR log;
  - rung 3 is distinct from (and co-exists with) the channel_unhealthy alarm;
  - rung 3 does NOT fire when the finding eventually delivers (incl. via the
    B13 failover) or on a pure-skip drain (every channel skipped, nothing tried);
  - the configurable per-pipe threshold (max_delivery_attempts) actually moves
    the exhaustion point, and defaults to MAX_RETRY_ATTEMPTS=5 when unset;
  - belfry's failure_surface read surfaces the rung-3 internal/delivery incident
    off-box.
"""

from __future__ import annotations

import asyncio
import importlib.util
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.clock import FakeClock
from angelus.daemon import _RESTART_RECONCILED_INTERNAL_SOURCES, AngelusDaemon
from angelus.lodging import Channel, Pipe
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db
from angelus.storage.catalog import MAX_RETRY_ATTEMPTS, TRUST_RETRY_DELAYS

PINNED = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)

# A bound that always clears the longest backoff in TRUST_RETRY_DELAYS (8h), so
# advancing it between drains re-arms the same finding in pending_pipe_items
# (which gates on next_attempt_at <= now). This is what lets one finding fail
# across enough drains to walk its per-finding ladder to exhaustion.
_PAST_BACKOFF = timedelta(hours=9)


# --------------------------------------------------------------------------
# Fixtures / doubles, mirroring test_b13_transport_failover.py.
# --------------------------------------------------------------------------


class _Recorder:
    """Channel sender double. ``fail`` is mutable so one recorder can flip
    between down and healthy across drains."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    async def __call__(self, channel, *_args, **_kwargs):
        self.calls.append(channel.name)
        if self.fail:
            raise RuntimeError(f"{channel.name} transport down")


def _solo_drain(
    tmp_path, *, max_delivery_attempts: int | None = None, backup: str | None = None
) -> tuple[Catalog, PipeDrain, FakeClock]:
    """A now-pipe routing to a single `email` channel (optionally with a backup),
    on a FakeClock so the per-finding backoff window can be advanced between
    drains. No channel name is hardcoded in the runner/loader -- the policy is
    entirely in this config."""
    clock = FakeClock(PINNED)
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path, clock=clock)
    channels = {
        "email": Channel(
            name="email", kind="email", command="patbot-email", to="x@e", backup=backup
        ),
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


def _open_incidents(catalog: Catalog) -> list[tuple[str, str, str]]:
    return [
        (row["source"], row["type"], row["entity"])
        for row in catalog.connection.execute(
            "SELECT source, type, entity FROM incidents "
            "WHERE status = 'open' ORDER BY id"
        )
    ]


def _exhausted_entities(catalog: Catalog) -> list[str]:
    """Entity (finding id, as text) of every rung-3 internal/delivery finding."""
    return [
        row["entity"]
        for row in catalog.connection.execute(
            """
            SELECT entity FROM findings
            WHERE source = 'internal/delivery' AND type = 'delivery_exhausted'
            ORDER BY id
            """
        )
    ]


def _queue_status(catalog: Catalog, finding_id: int) -> str | None:
    row = catalog.connection.execute(
        "SELECT status FROM pipe_queues WHERE finding_id = ? AND pipe = 'now'",
        (finding_id,),
    ).fetchone()
    return None if row is None else row["status"]


def _walk_to_exhaustion(catalog, drain, clock, entity: str, drains: int) -> int:
    """Write a finding, then drain `drains` times advancing past the backoff
    each time, so the SAME finding fails across drains and walks its ladder.
    Returns the finding id."""
    finding_id = _write_finding(catalog, entity)
    for i in range(drains):
        if i:
            clock.advance(_PAST_BACKOFF)
        asyncio.run(drain.drain_once())
    return finding_id


# --------------------------------------------------------------------------
# (a) Acceptance: a persistently-failing dispatch walks the full ladder and
#     ends at the rung-3 out-of-band page (durable internal/delivery incident)
#     + the ERROR log. Default threshold (max_delivery_attempts unset -> 5).
# --------------------------------------------------------------------------


def test_acceptance_persistent_failure_walks_ladder_to_out_of_band_page(
    tmp_path, monkeypatch, caplog
) -> None:
    """email is the now-pipe's sole channel and fails every drain, with no
    backup to fail over to. The finding cannot reach any transport, so it walks
    rung 1 (retry/backoff) MAX_RETRY_ATTEMPTS times and, on the crossing drain,
    rung 3 fires: a durable internal/delivery `delivery_exhausted` incident keyed
    on the finding id is opened and an ERROR names the finding/pipe/last_error.

    Discrimination:
    - email is attempted on every one of the 5 drains (it is the sole primary,
      never skipped until it is finally marked unhealthy on the 5th), proving the
      finding really walked the retry rung rather than short-circuiting.
    - the rung-3 incident appears EXACTLY on the exhausting drain, not before:
      _exhausted_entities is empty through drain 4 and holds the finding id after
      drain 5. A signal that fired on every undelivered drain would show up at
      drain 1; one that never fired would leave it empty.
    - the finding's queue row is terminal ('failed'), the durable signature of
      crossing the threshold -- the hook rung 3 keys off.
    """
    catalog, drain, clock = _solo_drain(tmp_path)  # backup=None -> ladder dead-ends
    email = _Recorder(fail=True)
    push = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_email", email)
    monkeypatch.setattr(pipe_runner, "send_push", push)

    finding_id = _write_finding(catalog, "example.com")

    # Drains 1..(threshold-1): each fails, advances the per-finding ladder, and
    # leaves the finding retryable. Rung 3 must NOT have fired yet.
    for i in range(MAX_RETRY_ATTEMPTS - 1):
        if i:
            clock.advance(_PAST_BACKOFF)
        asyncio.run(drain.drain_once())
    assert _exhausted_entities(catalog) == [], "rung 3 must not fire before exhaustion"
    assert _queue_status(catalog, finding_id) == "pending"

    # The exhausting drain: the per-finding ladder crosses its threshold
    # undelivered -> rung 3 pages out-of-band.
    clock.advance(_PAST_BACKOFF)
    with caplog.at_level(logging.ERROR, logger="angelus.pipes.runner"):
        asyncio.run(drain.drain_once())

    assert email.calls == ["email"] * MAX_RETRY_ATTEMPTS, "the primary was tried each drain"
    assert push.calls == [], "no backup configured -- nothing failed over"
    assert _queue_status(catalog, finding_id) == "failed", "the finding exhausted"
    # The acceptance: a durable internal/delivery incident, keyed on the finding
    # id, is open.
    assert ("internal/delivery", "delivery_exhausted", str(finding_id)) in _open_incidents(
        catalog
    )
    assert _exhausted_entities(catalog) == [str(finding_id)]
    # The ERROR log names the finding and the out-of-band escalation.
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any(
        "internal/delivery" in m and str(finding_id) in m and "out-of-band" in m
        for m in errors
    ), f"expected a rung-3 ERROR naming the finding; got {errors}"


# --------------------------------------------------------------------------
# (b) Rung 3 is DISTINCT from the channel_unhealthy alarm. On the acceptance
#     run the per-channel and per-finding counters cross together (one finding
#     failing one channel each drain), so BOTH incidents end up open -- and they
#     are different (source, type), which is the whole point: belfry/health can
#     tell "a transport degraded" apart from "this content was abandoned".
# --------------------------------------------------------------------------


def test_rung3_distinct_from_channel_unhealthy(tmp_path, monkeypatch) -> None:
    """Same single-channel exhaustion as (a). The per-CHANNEL counter
    (immediate_channel_attempts) and the per-FINDING counter (pipe_queues) both
    reach 5 on the 5th drain, so email is marked unhealthy (internal/dispatch
    channel_unhealthy) AND the finding is abandoned (internal/delivery
    delivery_exhausted) on the same drain. The two incidents must be separate
    rows with distinct (source, type) -- not one collapsed signal.

    Discrimination: asserting BOTH keys are open, and that they differ on source,
    inverts if rung 3 reused the internal/dispatch channel_unhealthy
    source/type (it would then collide with the channel alarm under the B30 gate
    -- different entity, so two rows, but indistinguishable to a reader filtering
    by source/type) or if it failed to open at all.
    """
    catalog, drain, clock = _solo_drain(tmp_path)
    monkeypatch.setattr(pipe_runner, "send_email", _Recorder(fail=True))
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder())

    finding_id = _walk_to_exhaustion(
        catalog, drain, clock, "example.com", MAX_RETRY_ATTEMPTS
    )

    open_incidents = _open_incidents(catalog)
    assert ("internal/delivery", "delivery_exhausted", str(finding_id)) in open_incidents
    assert ("internal/dispatch", "channel_unhealthy", "email") in open_incidents
    # The two distress signals live on different sources -- a reader can route
    # "transport degraded" and "content abandoned" separately.
    sources = {src for src, _type, _entity in open_incidents}
    assert {"internal/delivery", "internal/dispatch"} <= sources


# --------------------------------------------------------------------------
# (c) Rung 3 does NOT fire when the finding eventually delivers via B13 failover.
# --------------------------------------------------------------------------


def test_no_rung3_when_delivered_via_failover(tmp_path, monkeypatch) -> None:
    """email is degraded (already unhealthy) but declares push as its backup, and
    push is healthy. The finding is delivered over the failover backup, so it is
    never undelivered -- the reconciliation takes the `delivered` branch and rung
    3 is never reached.

    Discrimination: push delivers and the queue row is 'dispatched', so no
    internal/delivery finding is ever written. A rung-3 signal that fired on
    "the primary failed" rather than "the finding reached NO transport" would
    wrongly open an incident here even though the content got out.
    """
    catalog, drain, clock = _solo_drain(tmp_path, backup="push")
    email = _Recorder(fail=True)  # would fail if attempted -- it is skipped
    push = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_email", email)
    monkeypatch.setattr(pipe_runner, "send_push", push)

    catalog.mark_channel_unhealthy("email", "smtp down")  # degraded -> fail over
    finding_id = _write_finding(catalog, "example.com")
    asyncio.run(drain.drain_once())

    assert email.calls == [], "the degraded primary is skipped"
    assert push.calls == ["push"], "delivered over the failover backup"
    assert _queue_status(catalog, finding_id) == "dispatched"
    assert _exhausted_entities(catalog) == [], "delivered content must not page out-of-band"


# --------------------------------------------------------------------------
# (d) Rung 3 does NOT fire on a pure-skip drain. Every channel is skipped as
#     unhealthy (nothing attempted, last_error stays None), so the per-finding
#     ladder is never advanced and the finding stays retryable forever -- a skip
#     is absence of a delivery attempt, not a failed delivery.
# --------------------------------------------------------------------------


def test_no_rung3_on_pure_skip_drain(tmp_path, monkeypatch, caplog) -> None:
    """email (the sole channel, no backup) is unhealthy before the drain, so it
    is skipped and never attempted. record_pipe_finding_undelivered is not called
    (last_error is None), the per-finding ladder never advances, and rung 3 never
    fires -- even across many drains.

    Discrimination: attempts stays 0, status stays 'pending', and no
    internal/delivery finding or ERROR is produced. An implementation that
    advanced the redelivery ladder on a skip (or fired rung 3 on any undelivered
    finding regardless of whether anything was tried) would eventually exhaust
    and page here -- exactly the false page this guards against.
    """
    catalog, drain, clock = _solo_drain(tmp_path)
    email = _Recorder(fail=True)  # would fail if attempted -- it must be skipped
    monkeypatch.setattr(pipe_runner, "send_email", email)
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder())

    catalog.mark_channel_unhealthy("email", "smtp down")
    finding_id = _write_finding(catalog, "example.com")
    with caplog.at_level(logging.ERROR, logger="angelus.pipes.runner"):
        for i in range(MAX_RETRY_ATTEMPTS + 3):  # well past the threshold
            if i:
                clock.advance(_PAST_BACKOFF)
            asyncio.run(drain.drain_once())

    assert email.calls == [], "the unhealthy channel is never attempted"
    row = catalog.connection.execute(
        "SELECT attempts, status FROM pipe_queues WHERE finding_id = ? AND pipe = 'now'",
        (finding_id,),
    ).fetchone()
    assert row["attempts"] == 0, "a pure-skip drain must not advance the redelivery ladder"
    assert row["status"] == "pending", "the finding stays retryable, never abandoned"
    assert _exhausted_entities(catalog) == [], "a skip is not a delivery failure"
    assert [r for r in caplog.records if r.levelno == logging.ERROR] == []


# --------------------------------------------------------------------------
# (e) The configurable threshold moves the exhaustion point, and defaults to
#     MAX_RETRY_ATTEMPTS (5) when unset. Driven directly at the catalog seam so
#     the threshold arithmetic is pinned independently of the runner wiring.
# --------------------------------------------------------------------------


def test_configurable_threshold_changes_exhaustion_point(tmp_path) -> None:
    """record_pipe_finding_undelivered exhausts on the Nth call when
    max_attempts=N, and on the MAX_RETRY_ATTEMPTS-th call when max_attempts is
    None (the default). Two separate findings keep the per-(finding, pipe)
    counters independent.

    Discrimination: the True return lands on call 3 for the configured pipe and
    call 5 for the default one. A threshold that ignored the argument would
    exhaust both at 5; one that mis-defaulted would not exhaust the unset finding
    at 5.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    tuned = _write_finding(catalog, "tuned")
    default = _write_finding(catalog, "default")

    # Configured threshold = 3: False, False, then True on the 3rd.
    returns_tuned = [
        catalog.record_pipe_finding_undelivered("now", tuned, "boom", 3)
        for _ in range(3)
    ]
    assert returns_tuned == [False, False, True]
    assert _queue_status(catalog, tuned) == "failed"

    # Unset (None) -> falls back to MAX_RETRY_ATTEMPTS: True only on the 5th.
    returns_default = [
        catalog.record_pipe_finding_undelivered("now", default, "boom", None)
        for _ in range(MAX_RETRY_ATTEMPTS)
    ]
    assert returns_default == [False] * (MAX_RETRY_ATTEMPTS - 1) + [True]
    assert _queue_status(catalog, default) == "failed"


def test_pipe_threshold_parsed_and_defaults(tmp_path) -> None:
    """parse_pipe reads max_delivery_attempts when present and leaves it None
    when absent (so the catalog default of 5 applies); a non-positive value
    fails the load loudly rather than silently disabling the give-up point."""
    from angelus.lodging import parse_pipe

    base = "cadence: immediate\nrender:\n  kind: dumb-alert\n  template: t\nchannels: [push]\n"

    with_field = tmp_path / "now.yaml"
    with_field.write_text(base + "max_delivery_attempts: 8\n", encoding="utf-8")
    assert parse_pipe(with_field).max_delivery_attempts == 8

    without = tmp_path / "daily.yaml"
    without.write_text(base, encoding="utf-8")
    assert parse_pipe(without).max_delivery_attempts is None

    bad = tmp_path / "bad.yaml"
    bad.write_text(base + "max_delivery_attempts: 0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="max_delivery_attempts"):
        parse_pipe(bad)


# --------------------------------------------------------------------------
# (f) belfry carries the rung-3 incident off-box. belfry's failure_surface read
#     (the B1 open-internal-incident query) sees the internal/delivery incident
#     and returns a DOWN reason -- the load-bearing delivery of the signal, since
#     the channels themselves just failed.
# --------------------------------------------------------------------------


def _load_belfry():
    belfry_path = Path(__file__).resolve().parent.parent / "belfry" / "belfry.py"
    spec = importlib.util.spec_from_file_location("belfry_b14_under_test", belfry_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_belfry_surfaces_rung3_incident_off_box(tmp_path, monkeypatch) -> None:
    """A finding exhausted to rung 3 opens an internal/delivery incident; belfry's
    failure_surface (read-only, pure-stdlib, no angelus import) surfaces it as a
    DOWN reason naming internal/delivery. A low max_delivery_attempts (2) exhausts
    the finding BEFORE the per-channel counter reaches 5, so email is NOT marked
    unhealthy and internal/delivery is the only open internal incident -- the read
    is isolated to the rung-3 signal.

    Discrimination: failure_surface returns a reason mentioning internal/delivery
    (not None), and the only open internal source is internal/delivery. If rung 3
    failed to open a durable incident, the read would return None (no DOWN), the
    silent-failure the whole item exists to kill.
    """
    catalog, drain, clock = _solo_drain(tmp_path, max_delivery_attempts=2)
    monkeypatch.setattr(pipe_runner, "send_email", _Recorder(fail=True))
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder())

    finding_id = _walk_to_exhaustion(catalog, drain, clock, "example.com", 2)
    assert _exhausted_entities(catalog) == [str(finding_id)]
    # The low threshold kept the per-channel counter below 5, so the ONLY open
    # internal/* incident (the slice belfry reads) is the rung-3 one -- no
    # internal/dispatch channel_unhealthy alarm. (The product finding opens its
    # own non-internal scheduled/a incident; belfry ignores that.)
    internal = [
        (src, typ, ent)
        for src, typ, ent in _open_incidents(catalog)
        if src.startswith("internal/")
    ]
    assert internal == [("internal/delivery", "delivery_exhausted", str(finding_id))]

    belfry = _load_belfry()
    db_path = tmp_path / "angelus.sqlite3"
    state_path = tmp_path / "belfry-failcheck-at"
    reason = belfry.failure_surface(db_path, state_path)
    assert reason is not None, "belfry must surface the open rung-3 incident as DOWN"
    assert "internal/delivery" in reason


def _delivery_incident_open(catalog: Catalog, finding_id: int) -> bool:
    """Is the rung-3 internal/delivery incident for this finding currently open?"""
    return ("internal/delivery", "delivery_exhausted", str(finding_id)) in _open_incidents(
        catalog
    )


def _write_lodging(root: Path) -> None:
    """On-disk lodging for the restart test, which needs a real AngelusDaemon to
    run its actual startup orphan-reconcile (_reconcile_orphaned_internal_
    incidents). Mirrors the minimal now(push)+daily(email) shape the other
    daemon-level tests use; the daemon keeps its catalog under root/state, so it
    is independent of the _solo_drain catalogs above."""
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n", encoding="utf-8"
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [email]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )
    (root / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: person@example.com\n", encoding="utf-8"
    )


# --------------------------------------------------------------------------
# (g) Recovery edge (LIVE, not a deferred seam): replaying an exhausted finding
#     re-delivers its content, and the `if delivered:` reconciliation fires the
#     paired internal/delivery clearance that closes the rung-3 incident. This is
#     the edge Finding 1 of the fell adds; before the fix the incident stays open
#     forever even after the content is successfully re-delivered.
# --------------------------------------------------------------------------


def test_replay_redelivers_and_clears_incident(tmp_path, monkeypatch) -> None:
    """A finding exhausts to rung 3 (durable internal/delivery incident open),
    then the live `angelus replay <fid>` path (catalog.replay_finding, which the
    daemon _op_replay control op wraps) resets its failed queue row to pending.
    The channel recovers, the next drain delivers the content, and the
    `if delivered:` reconciliation clears the incident -- belfry goes green.

    max_delivery_attempts=2 exhausts the per-FINDING ladder at the 2nd drain
    while the per-CHANNEL counter is still at 2 (< 5), so email is NOT marked
    unhealthy. That matters: on the post-replay drain email must be eligible
    (a healthy channel) so the redelivery actually happens -- the whole point of
    the clear edge.

    Discrimination: the incident is asserted OPEN after exhaustion and CLOSED
    after the replayed redelivery. Before Finding 1's fix the `if delivered:`
    branch wrote no clearance, so the incident would still be open here and the
    final assertion inverts -- the exact "belfry stays red forever after a
    successful recovery" defect this pins.
    """
    catalog, drain, clock = _solo_drain(tmp_path, max_delivery_attempts=2)
    email = _Recorder(fail=True)  # down through exhaustion, healed before replay
    monkeypatch.setattr(pipe_runner, "send_email", email)
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder())

    finding_id = _walk_to_exhaustion(catalog, drain, clock, "example.com", 2)
    assert _queue_status(catalog, finding_id) == "failed", "the finding exhausted"
    assert _delivery_incident_open(catalog, finding_id), "rung 3 opened the incident"
    assert not catalog.is_channel_unhealthy("email"), (
        "the low per-finding threshold must leave the channel healthy so replay "
        "can redeliver"
    )

    # The transport recovers and an operator replays the dead-lettered finding.
    email.fail = False
    outcome = catalog.replay_finding(finding_id, {"now"})
    assert outcome["outcome"] == "requeued"
    assert _queue_status(catalog, finding_id) == "pending", "replay re-armed the row"

    # The next drain delivers the content -> the recovery edge closes the incident.
    asyncio.run(drain.drain_once())
    assert email.calls[-1] == "email", "the recovered channel redelivered the content"
    assert _queue_status(catalog, finding_id) == "dispatched"
    assert not _delivery_incident_open(catalog, finding_id), (
        "a successful redelivery must clear the rung-3 incident (Finding 1)"
    )


# --------------------------------------------------------------------------
# (h) An INTERNAL finding that cannot be delivered must NOT spawn a rung-3
#     internal/delivery incident. internal/* findings already fan to every
#     channel (B7) and belfry already carries their ORIGINAL incident off-box;
#     a second internal/delivery incident keyed on the internal finding's id is
#     a false "content lost" premise with no redelivery path of its own to clear
#     it. This is the guard Finding 2 of the fell adds, mirroring rung 2's
#     internal exclusion in the channel loop.
# --------------------------------------------------------------------------


def test_internal_finding_does_not_spawn_rung3(tmp_path, monkeypatch) -> None:
    """An internal/render finding fans to BOTH channels (B7), both fail every
    drain, and the per-finding ladder walks to exhaustion (status 'failed'). The
    exhaustion edge IS reached -- but rung 3 is guarded on
    `not _is_internal(source)`, so NO internal/delivery finding or incident is
    created. The original internal/render incident is what belfry carries off-box.

    Discrimination: the queue row reaching 'failed' proves the ladder genuinely
    crossed its threshold (so this is not a vacuous pass), while
    _exhausted_entities stays empty. Before Finding 2's guard, the same exhaustion
    edge would fire write_internal_finding('internal/delivery', ...) and the
    internal/delivery assertion inverts -- the redundant, never-clearing incident
    the guard exists to prevent.
    """
    catalog, drain, clock = _solo_drain(tmp_path)
    monkeypatch.setattr(pipe_runner, "send_email", _Recorder(fail=True))
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder(fail=True))

    # An internal finding -- angelus's OWN distress signal -- routed to `now`,
    # which the B7 fan delivers to every channel. Both channels are down.
    finding_id = catalog.write_internal_finding(
        "internal/render", "render_failed", "digest", "boom", {"now"}
    )
    for i in range(MAX_RETRY_ATTEMPTS):
        if i:
            clock.advance(_PAST_BACKOFF)
        asyncio.run(drain.drain_once())

    assert _queue_status(catalog, finding_id) == "failed", (
        "the per-finding ladder must reach the exhaustion edge -- otherwise the "
        "test would pass for the wrong reason (rung 3 simply never evaluated)"
    )
    assert _exhausted_entities(catalog) == [], (
        "an internal finding must not spawn a rung-3 internal/delivery incident "
        "(Finding 2)"
    )
    assert not any(
        src == "internal/delivery" for src, _t, _e in _open_incidents(catalog)
    ), "no internal/delivery incident of any entity"
    # The original internal/render incident is still open -- that is the signal
    # belfry carries off-box; rung 3 adds nothing for it.
    assert ("internal/render", "render_failed", "digest") in _open_incidents(catalog)


# --------------------------------------------------------------------------
# (i) Restart persistence: an open internal/delivery incident must SURVIVE the
#     startup orphan-reconcile. internal/delivery is correctly absent from
#     _RESTART_RECONCILED_INTERNAL_SOURCES -- it recovers only off a real
#     redelivery (the live clear edge), never on a blind startup sweep, since a
#     restart does not redeliver the content. Blind-clearing it would re-green
#     belfry while the content is still lost.
# --------------------------------------------------------------------------


def test_exhausted_incident_survives_startup_reconcile(tmp_path) -> None:
    """Open an internal/delivery incident, then run the daemon's REAL
    _reconcile_orphaned_internal_incidents (the exact startup sweep) and assert
    the incident is still open afterwards.

    Discrimination: the sweep clears every source in
    _RESTART_RECONCILED_INTERNAL_SOURCES and leaves the rest. The companion
    assertion pins internal/delivery's ABSENCE from that tuple, so if a future
    edit added it (blind-clearing the incident on every boot and re-greening
    belfry while the content is still undelivered) both this survival assertion
    and the membership assertion fail.
    """
    assert "internal/delivery" not in _RESTART_RECONCILED_INTERNAL_SOURCES, (
        "internal/delivery must recover only off a real redelivery, never a "
        "blind startup sweep"
    )
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    known_pipes = set(daemon.lodging.pipes)
    daemon.catalog.write_internal_finding(
        "internal/delivery", "delivery_exhausted", "123", "pipe=now last_error=boom",
        known_pipes,
    )
    assert ("internal/delivery", "delivery_exhausted", "123") in _open_incidents(
        daemon.catalog
    )

    daemon._reconcile_orphaned_internal_incidents()

    assert ("internal/delivery", "delivery_exhausted", "123") in _open_incidents(
        daemon.catalog
    ), "the rung-3 incident must survive the startup reconcile"


# --------------------------------------------------------------------------
# (j) Repeat suppression: once a finding is exhausted, further drains and a
#     repeat rung-3 emission for the same key do not grow the incident count --
#     the B30 gate keeps it at exactly one open incident / one finding row.
# --------------------------------------------------------------------------


def test_repeat_exhaustion_emits_single_incident(tmp_path, monkeypatch) -> None:
    """Exhaust a finding (one internal/delivery incident), then drain several
    more times AND fire a second explicit rung-3 emission for the same finding
    key. The B30 gate drops both repeats: still exactly one internal/delivery
    finding row and one open incident.

    Discrimination: counting findings AND open incidents == 1 after the repeats.
    A missing/ineffective gate would either re-enqueue on each drain or open a
    second incident on the explicit repeat -- the count would climb past one.
    """
    catalog, drain, clock = _solo_drain(tmp_path)
    monkeypatch.setattr(pipe_runner, "send_email", _Recorder(fail=True))
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder())

    finding_id = _walk_to_exhaustion(
        catalog, drain, clock, "example.com", MAX_RETRY_ATTEMPTS
    )
    assert _exhausted_entities(catalog) == [str(finding_id)]

    # Drain well past exhaustion: the failed row is no longer pending, so it is
    # never re-picked, and no new internal/delivery finding is produced.
    for _ in range(3):
        clock.advance(_PAST_BACKOFF)
        asyncio.run(drain.drain_once())

    # An explicit repeat for the SAME key is dropped by the B30 gate (no new
    # row, no second incident) -- this is the path that would re-fire if a future
    # redelivery attempt re-exhausted before the incident was cleared.
    catalog.write_internal_finding(
        "internal/delivery", "delivery_exhausted", str(finding_id), "again", {"now"}
    )

    assert _exhausted_entities(catalog) == [str(finding_id)], "exactly one finding row"
    delivery_incidents = [
        (src, typ, ent)
        for src, typ, ent in _open_incidents(catalog)
        if src == "internal/delivery"
    ]
    assert delivery_incidents == [
        ("internal/delivery", "delivery_exhausted", str(finding_id))
    ], "exactly one open internal/delivery incident, no growth"


# --------------------------------------------------------------------------
# (k) Threshold above the backoff schedule: a pipe configuring
#     max_delivery_attempts greater than the 4-step TRUST_RETRY_DELAYS must
#     exercise the index clamp without IndexError and still exhaust on the Nth
#     call. Driven at the catalog seam so the clamp arithmetic is pinned directly.
# --------------------------------------------------------------------------


def test_threshold_above_schedule_length(tmp_path) -> None:
    """record_pipe_finding_undelivered with max_attempts=8 -- beyond the 4-entry
    TRUST_RETRY_DELAYS -- walks calls 5..7 past the end of the delay schedule.
    The clamp (min(next_attempt-1, len-1)) holds the backoff at its longest step
    instead of indexing out of range, and the finding exhausts on the 8th call.

    Discrimination: 8 > len(TRUST_RETRY_DELAYS) is asserted up front so this stays
    a genuine over-the-end case; the returns are [False]*7 + [True]. An unclamped
    TRUST_RETRY_DELAYS[next_attempt - 1] would raise IndexError on call 5, before
    ever reaching the exhausting 8th -- so the test simply completing past the
    clamp is itself the discriminator, with the exhaustion point pinned on top.
    """
    assert 8 > len(TRUST_RETRY_DELAYS), "the threshold must exceed the schedule"
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    finding_id = _write_finding(catalog, "wide")

    returns = [
        catalog.record_pipe_finding_undelivered("now", finding_id, "boom", 8)
        for _ in range(8)
    ]
    assert returns == [False] * 7 + [True], "exhausts on the 8th, no IndexError before"
    assert _queue_status(catalog, finding_id) == "failed"
