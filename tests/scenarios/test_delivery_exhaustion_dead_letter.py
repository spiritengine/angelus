"""S4 delivery_exhaustion_dead_letter -- B14/B15 contract, brief-20260612-p3ct.

The master brief's named anti-pattern: "findings 9/10 sat pending silently."
A finding whose every transport is down must NOT loop quietly forever -- the
B14/B15 contract is that exhaustion gets LOUDER: the finding lands in the
terminal ``dead_letter`` state (not an indistinguishable 'pending'), a durable
internal/delivery incident opens so belfry pages off-box, the on-box health
surface shows WHAT was abandoned, and the content is REPLAYABLE once a transport
returns.

This scenario drives the immediate `now` pipe (email primary, push backup) with
BOTH email AND push faulted, so the B13 failover hop cannot save the finding:
the email primary degrades and fails the finding over to push, push is faulted
too, and the per-finding redelivery ladder walks to exhaustion. It asserts the
loud terminal state (dead_letter_count==1, an open internal/delivery incident,
belfry red, the health dead-letter section populated), then completes the round
trip that is the actual new coverage: clear the faults, RESTART (rebuild the
harness on the same root -- channel_health is restart-scoped by design, so a
restart is the supported path back from an unhealthy channel; it also re-pairs
the dead_letter row with its still-open incident), REPLAY through the daemon's
replay op, and drain -- the recovered email delivers, the incident closes,
dead_letter_count returns to 0, and belfry goes green. The full red -> green in
one script is the point.

Harness/path notes (flagged for the fell): the replay uses the daemon control
op directly (``sim.daemon._op_replay`` -- the same handler ``angelus replay``
drives); the harness exposes no ``replay`` inspector. The restart is load-
bearing and the spec's "clear faults, replay, drain" understates it: because
channel_health is restart-scoped, a degraded email stays SKIPPED until the
daemon restarts, so a same-process replay would redeliver over the backup and
leave the internal/dispatch(email) alarm open (belfry still red). Rebuilding the
harness is the faithful restart.

Mutation-verified: reverting the rung-3 durability emission -- the
``page_known_pipes`` argument threaded into ``record_pipe_finding_undelivered``
forced to None on the now pipe, so exhaustion flips the row to dead_letter
WITHOUT opening the internal/delivery incident (the pre-brief-20260604-f324
flip-first orphan shape) -- turns this scenario red: the dead-letter lands but
no incident opens, so belfry stays green over abandoned content and the
detection assertion fails. Confirmed red with that edit; restored after.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from angelus.sim import SimHarness


def _sent_over_email(db_path, finding_id: int) -> int:
    """How many times ``finding_id`` was delivered ('sent') over email -- the
    duplicate-page guard: the contract is >=1 delivery, never two."""
    connection = sqlite3.connect(db_path)
    try:
        return sum(
            1
            for row in connection.execute(
                "SELECT finding_ids FROM dispatches "
                "WHERE channel = 'email' AND status = 'sent'"
            )
            if finding_id in json.loads(row[0])
        )
    finally:
        connection.close()


def _delivery_incidents(sim) -> list[dict]:
    """Open internal/delivery delivery_exhausted incidents -- the durable
    per-finding rung-3 page belfry carries off-box."""
    return [
        incident
        for incident in sim.open_incidents()
        if incident["source"] == "internal/delivery"
    ]


# A clock jump past the longest per-finding backoff (TRUST_RETRY_DELAYS tops out
# at 8h), so every redelivery the ladder schedules is due on the next drain.
_PAST_BACKOFF = timedelta(hours=9)


@pytest.mark.scenario
def test_delivery_exhaustion_dead_letter(
    tmp_path, monkeypatch, reference_lodging, belfry, belfry_paths
):
    # The immediate `now` pipe routes to email, which declares push as its B13
    # failover backup. Faulting BOTH is what makes failover unable to save the
    # finding -- the spec's "fault primary AND backup" shape.
    reference_lodging(
        tmp_path,
        sources=(("scheduled/watch", "1h"),),
        status_code=503,
        daily_max_interval=None,
        now_channels=("email",),
        email_backup="push",
    )
    # Arm both transports before construction so the daemon's
    # FaultRegistry.from_env picks them up; cleared for the restart below.
    monkeypatch.setenv("ANGELUS_FAULT_INJECT", "email,push")

    paths = belfry_paths(tmp_path)
    start = datetime.now(UTC)

    async def scenario() -> None:
        # -- OUTAGE: drive the finding to dead-letter --------------------------
        with SimHarness(tmp_path, start) as sim:
            await sim.fire_source("scheduled/watch")
            assert await sim.run_triage() == 1
            assert _delivery_incidents(sim) == []
            assert sim.dead_letter_count() == 0

            # Drain the now pipe across the retry schedule. email fails every
            # drain; on the threshold-crossing drain it degrades and fails the
            # finding over to push, which is also faulted -- so the per-finding
            # ladder keeps walking until it exhausts to dead_letter.
            for i in range(5):
                if i:
                    sim.advance(_PAST_BACKOFF)
                summary = await sim.drain("now")
                assert summary.dispatched == 0, summary  # nothing ever delivered

            # -- DETECTION: the loud terminal state ---------------------------
            assert sim.dead_letter_count() == 1, sim.dead_letter_items()
            incidents = _delivery_incidents(sim)
            assert len(incidents) == 1, sim.open_incidents()
            # belfry pages off-box on the open internal/delivery incident.
            surface = belfry.failure_surface(paths.db_path, paths.failcheck)
            assert surface is not None and "internal/delivery" in surface, surface
            # The on-box health surface shows WHAT was abandoned (replay target).
            health = await sim.health()
            dead_letter = health["delivery"]["dead_letter"]
            assert dead_letter["count"] == 1, dead_letter
            assert dead_letter["items"], "health must name the dead-lettered finding"
            fid = int(dead_letter["items"][0]["finding_id"])

        # -- RECOVERY: clear faults, restart, replay, drain -------------------
        # Restart semantics: a fresh harness on the same root clears
        # channel_health (so email is eligible again) and re-pairs the
        # dead_letter row with its still-open internal/delivery incident.
        monkeypatch.delenv("ANGELUS_FAULT_INJECT", raising=False)
        with SimHarness(tmp_path, start + _PAST_BACKOFF * 6) as sim:
            # The incident survived the restart (content is still abandoned);
            # the dead_letter row is still terminal.
            assert len(_delivery_incidents(sim)) == 1, sim.open_incidents()
            assert sim.dead_letter_count() == 1

            # Replay through the daemon control op -- the same path
            # `angelus replay <fid>` drives -- re-arms the dead_letter row.
            result = await sim.daemon._op_replay({"finding_id": fid})
            assert result["outcome"] == "requeued", result

            # The recovered email now delivers the replayed finding. (This drain
            # also flushes the internal/dispatch + internal/delivery ALARM
            # findings that queued to `now` during the outage and never reached
            # a live transport -- the distress signals finally get out, which is
            # the loud contract, not a bug. So the tally is >1; what matters is
            # the down finding delivers EXACTLY once -- the >=1-delivery-never-
            # two contract -- over the recovered primary.)
            summary = await sim.drain("now")
            assert summary.dispatched >= 1 and summary.failed == 0, summary
            assert any("down" in line for line in sim.dispatches()), sim.dispatches()
            assert _sent_over_email(paths.db_path, fid) == 1, (
                "the replayed finding must deliver over email exactly once"
            )

            # -- RED -> GREEN: the round trip closed ---------------------------
            assert sim.dead_letter_count() == 0, sim.dead_letter_items()
            assert _delivery_incidents(sim) == [], (
                f"redelivery must close the rung-3 incident: {sim.open_incidents()}"
            )
            assert belfry.failure_surface(paths.db_path, paths.failcheck) is None, (
                "belfry must go green once the content is redelivered"
            )

    asyncio.run(scenario())
