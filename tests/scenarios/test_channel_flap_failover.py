"""S3 channel_flap_failover -- B13/B14 design surface, brief-20260612-p3ct.

A delivery channel does not die cleanly; it FLAPS -- down for a stretch, then
back, then down again. The contract the failover/escalation ladder owes for
that shape is three-fold and easy to get subtly wrong: while the channel is
down the content must still get out over a second transport; the operator must
be alarmed exactly ONCE per outage (not once per failed drain -- the flood the
B30 emission gate exists to stop); and when the channel recovers the alarm must
clear on its own and re-arm, so the NEXT flap alarms again instead of going
silent forever.

This scenario replays that on the daily digest pipe, whose resilience to a dead
channel is ADDITIVE routing rather than per-channel failover: the digest renders
to email AND push, so a faulted email still ships over push without any failover
hop (runner._drain_digest spells out why the digest leans on additive routing --
"strictly better than failover" for a multi-channel pipe -- and leaves
``backup`` to the immediate path). The digest path is also where recovery is
restart-free: it attempts even a channel marked unhealthy, so clearing the fault
and draining again delivers over the recovered email and fires the
internal/dispatch clearance that closes the incident -- no daemon restart
needed, exactly the "drain again -> returns to email" shape the class wants.
(The immediate-path B13 failover hop -- email primary, push backup -- is
exercised end to end in S4, where it cannot save an exhausting finding.)

The flap is driven through the FaultRegistry seam (arm / clear / re-arm across
drains), never a monkeypatched sender. The script asserts: the down finding
ships over push while email is faulted (push 'sent', email 'failed', no email
log line); the channel-degraded alarm emits exactly ONE internal/dispatch
channel_unhealthy FINDING ROW and a second failing cycle adds no duplicate ROW
(the B30 emission gate), while incident-level upsert dedup independently keeps a
single open incident; clearing the fault and draining returns delivery to email
and closes the incident via clearance; re-arming emits exactly one FRESH
channel_unhealthy row (the gate re-armed -- 2 rows total). Belfry
failure_surface is red while degraded and green after recovery.

Two independent mechanisms are mutation-verified here, because two assertions
guard two different things:

  - The B30 EMISSION GATE is pinned by the channel_unhealthy FINDING-ROW count,
    NOT the incident count. Forcing the gate's duplicate pre-check false in
    Catalog.write_finding (``existing_open = None`` after the lookup at
    catalog.py:632) makes the second failing drain re-emit: the finding-row
    count climbs 1 -> 2 and the assertion goes red. Confirmed red with that
    edit; restored. The incident count does NOT discriminate the gate -- it
    stays at 1 under that same mutation, because _upsert_incident's partial
    unique index (ON CONFLICT(source,type,entity) WHERE status='open') collapses
    the repeats to a single open incident regardless. An incident-count
    assertion claiming to prove the gate would be a tautology; this scenario's
    earlier revision made exactly that mistake.

  - The RECOVERY CLEARANCE is pinned by the post-recovery belfry-green
    assertion. Deleting the ``write_internal_clearance("internal/dispatch",
    channel.name, ...)`` in _drain_digest's success branch (fires when email
    sends again) leaves the channel-degraded incident open, so delivery
    "returning to email" leaves belfry stuck red. Confirmed red with that edit;
    restored.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from angelus.pipes import PipeDrain
from angelus.sim import SimHarness


def _dispatch_rows(db_path) -> list[tuple[str, str]]:
    """(channel, status) for every dispatches row, oldest first -- the
    channel-level receipt the dry-run log cannot disambiguate."""
    connection = sqlite3.connect(db_path)
    try:
        return [
            (row[0], row[1])
            for row in connection.execute(
                "SELECT channel, status FROM dispatches ORDER BY id"
            )
        ]
    finally:
        connection.close()


def _dispatch_incidents(sim) -> list[dict]:
    """Open internal/dispatch channel_unhealthy incidents naming the email
    channel -- the channel-degraded alarm, isolated from the source `down`
    incident the fired source also opens."""
    return [
        incident
        for incident in sim.open_incidents()
        if incident["source"] == "internal/dispatch"
        and incident["entity"] == "email"
    ]


def _channel_unhealthy_findings(db_path, channel: str = "email") -> int:
    """Count of internal/dispatch channel_unhealthy FINDING ROWS for ``channel``
    -- the count the B30 emission gate actually governs.

    This is the discriminator the incident count is NOT. The single OPEN
    INCIDENT invariant is enforced by _upsert_incident's partial unique index
    (ON CONFLICT(source,type,entity) WHERE status='open'), so an incident-count
    assertion stays at 1 even with the gate disabled. The gate's job is to drop
    the duplicate FINDING ROW (and its dispatch) while the incident is open --
    so this count stays at 1 across repeated failing drains WITH the gate, and
    climbs 1 -> 2 -> ... per failing cycle WITHOUT it. The query mirrors
    test_b13_immediate_channel_escalation._escalation_findings."""
    connection = sqlite3.connect(db_path)
    try:
        return connection.execute(
            """
            SELECT COUNT(*) FROM findings
            WHERE source = 'internal/dispatch'
              AND type = 'channel_unhealthy'
              AND entity = ?
            """,
            (channel,),
        ).fetchone()[0]
    finally:
        connection.close()


@pytest.mark.scenario
def test_channel_flap_failover(tmp_path, monkeypatch, reference_lodging, belfry, belfry_paths):
    # The daily digest routes to email AND push (additive). email flaps; push is
    # the always-live second transport the content rides while email is down.
    reference_lodging(
        tmp_path,
        sources=(("scheduled/watch", "1h"),),
        status_code=503,
        daily_max_interval=None,
        daily_channels=("email", "push"),
    )

    # The digest renders an LLM synthesis before it sends; stub it so the drain
    # exercises the send path (where the fault lives) without a horizon cast.
    async def _fake_llm(self, _pipe, _structured):
        return "synthesis paragraph.", None

    monkeypatch.setattr(PipeDrain, "_render_llm_body", _fake_llm)

    paths = belfry_paths(tmp_path)
    start = datetime.now(UTC)

    async def scenario() -> None:
        with SimHarness(tmp_path, start) as sim:
            # A real down finding to carry: fire the source down and triage so
            # the canary handler queues a `down` finding to `now` and `daily`.
            await sim.fire_source("scheduled/watch")
            assert await sim.run_triage() == 1
            # Baseline: nothing degraded yet, belfry sees no self-reported fault.
            assert belfry.failure_surface(paths.db_path, paths.failcheck) is None
            assert _dispatch_incidents(sim) == []

            # -- FLAP 1: email faults; the digest ships over push --------------
            sim.daemon.faults.arm("email")
            first = await sim.drain("daily")
            # One channel delivered (push), one failed (email): additive routing.
            assert (first.dispatched, first.failed) == (1, 1), first
            rows = _dispatch_rows(paths.db_path)
            assert ("push", "sent") in rows, rows
            assert ("email", "failed") in rows, rows
            assert ("email", "sent") not in rows, rows
            # The content got out: the dry-run log carries the push send, and
            # email -- faulted before its dry-run write -- left no line.
            assert sim.dispatches(), "the digest must ship over push while email is down"

            # The channel-degraded alarm opened exactly once: one
            # channel_unhealthy FINDING ROW (the gate's open transition) and
            # one open incident.
            assert _channel_unhealthy_findings(paths.db_path) == 1, _dispatch_rows(
                paths.db_path
            )
            assert len(_dispatch_incidents(sim)) == 1, sim.open_incidents()
            # Belfry sees angelus's own self-reported dispatch failure: red.
            degraded = belfry.failure_surface(paths.db_path, paths.failcheck)
            assert degraded is not None and "internal/dispatch" in degraded, degraded

            # -- FLAP 1 continues: a SECOND failing cycle, no duplicate -------
            # A new UTC day forces the digest to send again with email still
            # faulted; the emission gate drops the repeat internal/dispatch
            # finding, so the incident count stays at one.
            sim.advance(timedelta(hours=25))
            second = await sim.drain("daily")
            assert (second.dispatched, second.failed) == (1, 1), second
            # THE GATE DISCRIMINATOR: the emission gate drops the repeat
            # channel_unhealthy FINDING on this second failing cycle, so the
            # finding-row count stays at 1. This is what genuinely pins the
            # gate -- with the gate disabled this count climbs to 2 (one fresh
            # row per failing drain). An incident-count assertion alone is a
            # tautology here: it stays at 1 even with the gate off.
            assert _channel_unhealthy_findings(paths.db_path) == 1, (
                "the B30 emission gate must suppress the duplicate "
                "channel_unhealthy finding on a second failing drain: "
                f"{_dispatch_rows(paths.db_path)}"
            )
            # The single OPEN INCIDENT is enforced by incident-level upsert
            # dedup (_upsert_incident's partial unique index on
            # (source,type,entity) WHERE status='open'), NOT the gate -- kept
            # as its own invariant, with the prose no longer crediting the gate.
            assert len(_dispatch_incidents(sim)) == 1, (
                "incident-level upsert dedup must collapse the repeat to one "
                f"open channel-degraded incident: {sim.open_incidents()}"
            )
            assert belfry.failure_surface(paths.db_path, paths.failcheck) is not None

            # -- RECOVERY: clear the fault; delivery returns to email ----------
            sim.daemon.faults.clear("email")
            sim.advance(timedelta(hours=25))
            recovered = await sim.drain("daily")
            # Both channels delivered this cycle: email is back.
            assert (recovered.dispatched, recovered.failed) == (2, 0), recovered
            assert ("email", "sent") in _dispatch_rows(paths.db_path)
            # The successful email send fired the internal/dispatch clearance:
            # the incident closes and the gate re-arms.
            assert _dispatch_incidents(sim) == [], (
                f"a recovered email must close the incident: {sim.open_incidents()}"
            )
            # Belfry is green now -- no open internal incident, no new failed
            # dispatch since the last tick.
            assert belfry.failure_surface(paths.db_path, paths.failcheck) is None, (
                "belfry must clear once email recovers and the incident closes"
            )

            # -- FLAP 2: re-arm; exactly one FRESH incident (gate re-armed) ----
            sim.daemon.faults.arm("email")
            sim.advance(timedelta(hours=25))
            reflap = await sim.drain("daily")
            assert (reflap.dispatched, reflap.failed) == (1, 1), reflap
            # The gate RE-ARMED on recovery: this fresh failure emits a NEW
            # channel_unhealthy finding row (1 -> 2 across the whole scenario --
            # one for each flap, none for the suppressed repeat or the
            # clearance). Had the gate stayed latched, this flap would add no
            # row and the alarm would go silent forever. That second row, not
            # the incident count, is the re-arm proof.
            assert _channel_unhealthy_findings(paths.db_path) == 2, (
                "recovery must re-arm the gate so the second flap emits a fresh "
                f"channel_unhealthy finding: {_dispatch_rows(paths.db_path)}"
            )
            # Single open incident again -- incident-level upsert dedup, as on
            # the first flap (not the gate).
            assert len(_dispatch_incidents(sim)) == 1, (
                "the second flap must leave exactly one open channel-degraded "
                f"incident (incident-level upsert dedup): {sim.open_incidents()}"
            )
            assert belfry.failure_surface(paths.db_path, paths.failcheck) is not None

    asyncio.run(scenario())
