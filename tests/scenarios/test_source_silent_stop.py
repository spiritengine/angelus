"""S5 source_silent_stop -- brief-20260607-g991, brief-20260612-p3ct (#1 review finding).

iotaschool.com silently went unwatched. Its scheduled check had stopped firing
-- an APScheduler job vanished on a hot-reload edge -- but every OTHER source
kept checking on cadence. belfry's wedge check reads a GLOBAL
max(last_checked_at), so the fresh sources kept the global heartbeat young and
belfry stayed green: one healthy source masked the dead one. Both holistic
reviewers flagged this HIGH; the activating fix was a per-source check SLA
(source_overdue_failure) the daemon persists to sqlite so belfry can assert each
source individually instead of trusting the global max.

This scenario replays the masking on three sources of different cadences: fire
all once, advance past ONE fast source's window while re-firing only the others,
and leave a slow source comfortably inside its own window. It asserts
source_overdue_failure names exactly the stalled source -- and, the heart of the
incident, that wedge_failure stays None because the global heartbeat is fresh.
The masking assertion IS the incident: a check that only proved the source was
overdue, without proving the coarse wedge was fooled, would not reproduce why
this went unnoticed.

Mutation-verified: reverting the per-source detection -- ``source_overdue_failure``
made to ``return None`` unconditionally (the world before brief-20260607-g991,
where only the global wedge existed) -- turns this scenario red: the stalled
source is no longer named and the masking goes unnoticed exactly as it did in
production. Confirmed red with that edit; restored after.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest

from angelus.sim import SimHarness


@pytest.mark.scenario
def test_source_silent_stop(tmp_path, monkeypatch, reference_lodging, belfry, belfry_paths):
    # Three sources, three cadences. iota is the one that goes silent; beta is a
    # peer that keeps firing (and masks the global heartbeat); slow is a slow
    # source that stays comfortably inside its OWN window the whole time.
    sources = (
        ("scheduled/iota", "1h"),   # window = 1h + max(1h, 5m) = 2h
        ("scheduled/beta", "1h"),   # window = 2h; re-fired, stays fresh
        ("scheduled/slow", "12h"),  # window = 12h + 12h = 24h; never overdue here
    )
    reference_lodging(tmp_path, sources=sources, daily_max_interval=None)
    paths = belfry_paths(tmp_path)

    # START at real-now so the re-fired sources stamp wall-fresh heartbeats; the
    # per-source SLA math itself runs in sim time (source_overdue is called with
    # sim.clock.now()), so advancing 3h of SIM time drives iota's window.
    start = datetime.now(UTC)

    async def scenario() -> None:
        with SimHarness(tmp_path, start) as sim:
            # All three check once at START.
            for source_ref, _cadence in sources:
                await sim.fire_source(source_ref)

            # Three hours pass. iota's job has vanished -- it is NOT re-fired --
            # while its peers keep checking. beta re-fires (fresh); slow does not
            # need to (still inside its 24h window).
            sim.advance(timedelta(hours=3))
            await sim.fire_source("scheduled/beta")

            # Daemon alive and OLD (up far longer than belfry's startup grace),
            # so the per-source overdue is not grace-suppressed -- a genuine
            # stall, not a just-booted daemon still establishing heartbeats.
            paths.write_pid(os.getpid())
            monkeypatch.setattr(belfry, "process_start_epoch", lambda _pid: 1_000.0)

            # -- DETECTION: the per-source SLA names the stalled source --------
            overdue = belfry.source_overdue_failure(
                paths.db_path, paths.pid_file, now=sim.clock.now()
            )
            assert overdue is not None, "iota's stalled check must be detected"
            assert "scheduled/iota" in overdue, overdue
            # The masking peer and the slow-but-in-window source are NOT named.
            assert "scheduled/beta" not in overdue, overdue
            assert "scheduled/slow" not in overdue, overdue

            # -- THE MASKING (the incident): the global wedge is fooled --------
            # max(last_checked_at) is beta's fresh heartbeat, so the wall-clock
            # wedge reads green even though iota is dead. This green-while-red is
            # exactly why the stall went unnoticed in production.
            assert belfry.wedge_failure(paths.db_path) is None, (
                "the global heartbeat is fresh -- wedge must stay green (the mask)"
            )

            # -- CLASSIFICATION: alert-only, never a restart -------------------
            # source_overdue_failure is wired as an OTHER reason in belfry.main:
            # one stale source is not a dead daemon, and the only restart-worthy
            # (absence) checks are pid_failure and wedge_failure, both green here.
            assert belfry.pid_failure(paths.pid_file) is None

    asyncio.run(scenario())
