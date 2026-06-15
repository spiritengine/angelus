"""S7 triage_death_not_silent -- opus holistic-review finding, brief-20260612-p3ct.

Since observation collapse, a down-transition whose triage exhausts its retry
ladder LOSES that product finding until the source's next DISTINCT state change
(the collapsed re-fires write no new observation, so the dead triage is never
retried for that state). The review flagged this as an untested residual gap and
asked for a regression test. The promise the system makes is narrow but real: it
does not lose that finding SILENTLY. Triage exhaustion must raise an
internal/triage incident and turn belfry red, so the loss is paged even though
the product content is gone.

This scenario rigs a triager handler to fail (nonzero exit), fires a state CHANGE
so an observation is written, and runs triage across the full retry ladder
(advancing sim time past each scheduled retry between sweeps) until the triager
exhausts. It asserts the detection floor holds -- an internal/triage incident
opens and belfry failure_surface returns a reason naming the failure -- WHILE the
coarse liveness checks stay green (the daemon is alive; a dead triager is not a
dead daemon, so this is alert-only, never a restart). And it asserts the loss
HONESTLY: no product finding was ever queued. That missing finding is the
residual gap this fixture documents; proving the gap coexists with a firing
detection floor is the point (a fixture that only showed the loss, or only the
detection, would miss the relationship the review named).

Mutation-verified: reverting the triage-exhaustion emission -- the
``write_internal_finding("internal/triage", "triage_failed", ...)`` branch in
``AngelusDaemon._run_triager`` deleted, so an exhausted triager settles its
observation silently -- turns this scenario red: no internal/triage incident
opens and belfry stays green over the lost finding (the exact silent-loss the
review warned about). Confirmed red with that edit; restored after.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest

from angelus.sim import SimHarness

# A clock jump past the longest triage backoff (TRUST_RETRY_DELAYS tops out at
# 8h), so each scheduled retry is due on the next sweep.
_PAST_BACKOFF = timedelta(hours=9)

# A triager handler rigged to fail: it reads its stdin request (so the failure is
# a genuine nonzero exit of a real subprocess, not a malformed-protocol error)
# and exits 1. run_python_triager raises on the nonzero exit, walking the retry
# ladder exactly as a real flaky/broken handler would.
_FAILING_HANDLER = (
    "import sys, json\n"
    "json.load(sys.stdin)\n"
    "sys.stderr.write('rigged triage failure\\n')\n"
    "sys.exit(1)\n"
)


def _triage_incidents(sim) -> list[dict]:
    """Open internal/triage triage_failed incidents -- the detection floor."""
    return [
        incident
        for incident in sim.open_incidents()
        if incident["source"] == "internal/triage"
    ]


@pytest.mark.scenario
def test_triage_death_not_silent(
    tmp_path, monkeypatch, reference_lodging, belfry, belfry_paths
):
    reference_lodging(
        tmp_path,
        sources=(("scheduled/watch", "1h"),),
        status_code=503,
        daily_max_interval=None,
    )
    # Rig the triager: overwrite the lodged handler the reference triager points
    # at with the failing script. Every triage attempt now exits nonzero.
    (tmp_path / "triagers" / "handlers" / "canary_watch.py").write_text(
        _FAILING_HANDLER, encoding="utf-8"
    )

    paths = belfry_paths(tmp_path)
    start = datetime.now(UTC)

    async def scenario() -> None:
        with SimHarness(tmp_path, start) as sim:
            # A state CHANGE writes one observation (the check runs cleanly --
            # the 503 rides its payload for the triager; the rigged handler is
            # what fails downstream, in triage).
            observation_id, _outcome = await sim.fire_source("scheduled/watch")
            assert observation_id is not None, "the state change must write an observation"
            assert _triage_incidents(sim) == []

            # Walk the triage retry ladder to exhaustion. Each sweep re-picks the
            # observation (its scheduled retry is due after the advance) and the
            # rigged handler fails it again, until the 5th attempt exhausts.
            for i in range(5):
                if i:
                    sim.advance(_PAST_BACKOFF)
                triaged = await sim.run_triage()
                assert triaged == 1, f"sweep {i}: the failing observation must be re-picked"

            # -- DETECTION FLOOR: the loss is paged, not silent ----------------
            incidents = _triage_incidents(sim)
            assert len(incidents) == 1, sim.open_incidents()
            assert incidents[0]["entity"], "the incident must name the dead triager"
            surface = belfry.failure_surface(paths.db_path, paths.failcheck)
            assert surface is not None and "internal/triage" in surface, surface

            # -- GREEN: a dead triager is not a dead daemon --------------------
            # The daemon is alive (the fire stamped a wall-fresh heartbeat) and
            # its pid is live, so the absence checks stay green: this is
            # alert-only, never a restart. (The only restart-worthy belfry
            # reasons are pid_failure and wedge_failure, both None here.)
            paths.write_pid(os.getpid())
            assert belfry.pid_failure(paths.pid_file) is None
            assert belfry.wedge_failure(paths.db_path, paths.pid_file) is None

            # -- THE HONEST LOSS (the residual gap) ----------------------------
            # The down finding the handler would have emitted on this state edge
            # is GONE -- exhausted triage queued no PRODUCT content. (`now` does
            # carry the internal/triage alarm finding -- that IS the detection
            # floor firing; the loss is the product `down` that never landed.)
            for pipe in ("now", "daily"):
                product = [
                    finding
                    for finding in sim.findings_for_pipe(pipe)
                    if not finding["source"].startswith("internal/")
                ]
                assert product == [], (
                    f"no product finding must reach {pipe} -- the lost content: {product}"
                )

    asyncio.run(scenario())
