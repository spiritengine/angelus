"""S1 silent_delivery_death -- 2026-05-29, brief-20260612-p3ct (founding incident).

The daily email had been dead for roughly twenty-four hours and nobody knew.
The daemon process was alive the whole time -- belfry's coarse liveness checks
were green -- so no page fired. What had actually happened: the daemon drifted
off its systemd unit (a hand-launched instance held the pid) and in doing so
lost ANGELUS_EMAIL_TO, so every daily digest send failed silently while the
immediate `now` push path kept working. The lesson that built this whole B27
layer: a restart was the WRONG tool (the process was fine); the failure was
delivery going red-silent under green liveness, and the detection ladder
(delivery SLA + self-reported failure surface + drift) is what should have
caught it.

This scenario replays that: a live, partially-working daemon (push delivers),
a daily pipe whose email channel is fault-injected, 25h of sim time across a
failed daily drain. It asserts the three detectors fire (sla_failure names the
overdue daily pipe; failure_surface sees the daemon's own internal/dispatch
incident; drift_failure names the off-unit pid) WHILE the two absence checks
stay green (pid alive, heartbeat fresh) -- so the incident is classified
alert-only, never a restart.

Mutation-verified: reverting the delivery-SLA detection in belfry --
``sla_failure`` made to ``return None`` unconditionally -- turns this scenario
red (the daily-overdue assertion fails). Confirmed red with that one-line edit;
restored after.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest

from angelus.pipes import PipeDrain
from angelus.sim import SimHarness


@pytest.mark.scenario
def test_silent_delivery_death(tmp_path, monkeypatch, reference_lodging, belfry, belfry_paths):
    reference_lodging(
        tmp_path,
        sources=(("scheduled/watch", "1h"),),
        status_code=503,
        # 24h delivery SLA: 25h of silence is overdue, a single late run is not.
        daily_max_interval="24h",
    )

    # Channel failure ONLY via the fault seam, armed before construction so the
    # daemon's FaultRegistry.from_env() picks it up. Email sends now raise a
    # transport-shaped error; push is untouched (the daemon stays partly alive).
    monkeypatch.setenv("ANGELUS_FAULT_INJECT", "email")

    # The daily digest renders an LLM body before it sends; stub it so the drain
    # exercises the send (where the fault lives) without burning a horizon cast.
    async def _fake_llm(self, _pipe, _structured):
        return "synthesis paragraph.", None

    monkeypatch.setattr(PipeDrain, "_render_llm_body", _fake_llm)

    paths = belfry_paths(tmp_path)
    # Pin START at real-now: the wall-clock wedge check it must read green reads
    # datetime.now(UTC), so the fire below has to stamp a wall-fresh heartbeat.
    # The SLA math runs in sim time regardless (sla_failure is called with
    # sim.clock.now()), so advancing 25h of SIM time still drives the overdue.
    start = datetime.now(UTC)

    async def scenario() -> None:
        with SimHarness(tmp_path, start) as sim:
            # The daemon is demonstrably alive and partially working: fire,
            # triage, and the immediate push path delivers.
            await sim.fire_source("scheduled/watch")
            assert await sim.run_triage() == 1
            now_summary = await sim.drain("now")
            assert (now_summary.dispatched, now_summary.failed) == (1, 0), now_summary
            assert any("down" in line for line in sim.dispatches()), sim.dispatches()

            # A full day passes; the daily digest drain fails on the faulted
            # email channel (the only daily channel). The digest path records a
            # failed dispatch AND opens an internal/dispatch incident.
            sim.advance(timedelta(hours=25))
            daily_summary = await sim.drain("daily")
            assert (daily_summary.dispatched, daily_summary.failed) == (0, 1), daily_summary

            # Belfry reads the same root. Pid alive (this process) so the
            # liveness checks are honestly green; systemd reports a DIFFERENT
            # MainPID than the pid file -> the 05-29 root cause, drift.
            paths.write_pid(os.getpid())
            monkeypatch.setattr(belfry, "systemd_main_pid", lambda: os.getpid() + 9999)

            # -- DETECTION: the ladder fires --------------------------------
            sla = belfry.sla_failure(paths.db_path, now=sim.clock.now())
            assert sla is not None and "daily" in sla and "overdue" in sla, sla

            surface = belfry.failure_surface(paths.db_path, paths.failcheck)
            assert surface is not None and "internal/dispatch" in surface, surface

            drift = belfry.drift_failure(paths.pid_file)
            assert drift is not None and "drift" in drift, drift

            # -- GREEN: the coarse liveness checks are NOT fooled ------------
            # This is the point of the incident: the daemon IS alive (fresh
            # heartbeat from the fire) and firing, so neither absence check
            # fires -- there is nothing here a restart would fix.
            assert belfry.pid_failure(paths.pid_file) is None
            assert belfry.wedge_failure(paths.db_path, paths.pid_file) is None

            # -- CLASSIFICATION: alert-only, never a restart ----------------
            # belfry's only absence (restart-worthy) reasons are pid_failure and
            # wedge_failure; both are None above. The three reasons that DID
            # fire are all alert-only OTHER reasons, so main() would page, not
            # restart -- restart was the wrong tool in this incident.

    asyncio.run(scenario())
