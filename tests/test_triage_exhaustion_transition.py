"""Regression: the observation-collapse retry-redundancy loss
(brief-20260612-29he; filed from the 2026-06-07 holistic review).

Before source-side change-detection (migration 0012, "observation collapse"),
every fire wrote an observation, so a triager that exhausted its retry ladder
(MAX_RETRY_ATTEMPTS) on one observation got a fresh chance at the SAME state
on the next fire -- retry redundancy via re-observation. After collapse, a
state transition writes exactly ONE observation; if that observation's triage
exhausts all retries (flaky triager subprocess, transient resource
exhaustion), the product finding for the transition is never produced, and no
new observation arrives until the NEXT distinct state change. For a down
transition that means the down-alert is lost until the site comes back up --
exactly inverted value.

This is a KNOWN gap, and it is NOT silent: exhaustion opens an
internal/triage incident, which belfry's open-internal-incident read
(failure_surface) keeps red until cleared. These tests pin both halves so the
behavior cannot degrade further (e.g. a refactor accidentally making
exhaustion quiet):

- the loud path: exhaustion opens the internal/triage incident, and belfry's
  failure_surface reports it;
- the gap, honestly: no product finding ever exists for the lost transition,
  re-fires of the unchanged state collapse to no observation (no retry
  redundancy), and the recovery transition both succeeds AND clears the
  internal incident -- so after recovery there is no remaining trace that the
  down-alert was lost.

Interplay with brief-20260607-6qsq Stage 1 (terminal 'consumed' observation
status, commit e42c0fc, in flight on another shard when this was written):
with a SINGLE lodged triager, an exhausted observation is whole-row terminal
('triage_failed') under both the pre-6qsq first-exhaust flip and the
post-6qsq all-triagers-terminal rule, so everything in the main regression
test holds under both semantics. The one behavior 6qsq changes -- whether
catalog.reprocess_source heals an exhausted observation back to 'ready' --
is pinned in the disposition test at the bottom, which forks explicitly on
the observation's post-reprocess status and asserts each world's
consequences. When 6qsq merges, the pre-6qsq branch becomes dead code and
the test can be simplified to the post-6qsq branch only.

Scenario mechanics: a SimHarness (B26) drives the production step methods
under a FakeClock. The source's check `cat`s a JSON fixture the test rewrites
between fires (up <-> down transitions); the triager is a real subprocess
handler that emits a `down` product finding on the up->down edge but exits
non-zero while a `wedge` sentinel file exists -- the deliberately-flaky
triager from the brief. Retry backoff is crossed by advancing the FakeClock
past each TRUST_RETRY_DELAYS rung.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import textwrap
from datetime import UTC, datetime, timedelta
from pathlib import Path

from angelus.sim import SimHarness
from angelus.storage.catalog import MAX_RETRY_ATTEMPTS, TRUST_RETRY_DELAYS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BELFRY_PATH = PROJECT_ROOT / "belfry" / "belfry.py"

START = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
SOURCE = "scheduled/site"
TRIAGER = "site"
ENTITY = "site.example"


def _load_belfry():
    spec = importlib.util.spec_from_file_location("belfry_under_test", BELFRY_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# The handler: a real triager subprocess. Emits the product `down` finding on
# the up->down edge (and a clearance on down->up), tracked via prior_state --
# the same shape as examples' canary_watch. While the wedge sentinel next to
# the handler exists, a DOWN observation makes it exit non-zero: the flaky
# triager whose retries the scenario exhausts. UP observations always
# succeed, so the wedge models a failure mode tied to processing the down
# state (e.g. the classifier it shells to for outages being itself unwell).
_HANDLER = textwrap.dedent(
    """\
    import json
    import sys
    from pathlib import Path

    request = json.load(sys.stdin)
    obs = request.get("observation") or {}
    prior = request.get("prior_state") or {}
    state = obs["state"]
    if state == "down" and (Path(__file__).resolve().parent / "wedge").exists():
        print("simulated transient triager failure", file=sys.stderr)
        sys.exit(17)
    findings = []
    last = prior.get("last")
    if state == "down" and last != "down":
        findings.append({
            "source": obs["source_ref"],
            "type": "down",
            "entity": obs["entity"],
            "severity": "high",
            "target_pipes": ["now"],
            "body": {"text": f"{obs['entity']} is down"},
        })
    elif state == "up" and last == "down":
        findings.append({
            "source": obs["source_ref"],
            "type": "clearance",
            "entity": obs["entity"],
            "severity": "info",
            "target_pipes": [],
            "body": {"text": f"{obs['entity']} recovered"},
        })
    json.dump({"findings": findings, "new_state": {"last": state}}, sys.stdout)
    """
)


def _write_lodging(root: Path) -> tuple[Path, Path]:
    """One fixture-driven source, the wedgeable triager above, an immediate
    `now` pipe, a push channel. Returns (fixture, wedge) -- rewrite the
    fixture to drive transitions, create/delete the wedge to break/heal the
    triager."""
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    fixture = root / "payload.json"
    (scheduled / "site.yaml").write_text(
        f"cadence: 1h\ncheck:\n  kind: shell\n  command: 'cat {fixture}'\n",
        encoding="utf-8",
    )
    handlers = root / "triagers" / "handlers"
    handlers.mkdir(parents=True)
    (handlers / "site.py").write_text(_HANDLER, encoding="utf-8")
    (root / "triagers" / "site.yaml").write_text(
        "inputs:\n  source: scheduled/site\n"
        "handler:\n  kind: python\n  path: triagers/handlers/site.py\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )
    return fixture, handlers / "wedge"


def _set_state(fixture: Path, state: str) -> None:
    fixture.write_text(
        json.dumps({"source_ref": SOURCE, "entity": ENTITY, "state": state}),
        encoding="utf-8",
    )


def _product_findings(sim: SimHarness) -> list[dict]:
    return [
        dict(row)
        for row in sim.daemon.connection.execute(
            "SELECT * FROM findings WHERE source = ? ORDER BY id", (SOURCE,)
        )
    ]


def _open_internal_triage(sim: SimHarness) -> list[dict]:
    return [
        incident
        for incident in sim.open_incidents()
        if incident["source"] == "internal/triage"
    ]


def _observation_status(sim: SimHarness, observation_id: int) -> str:
    row = sim.daemon.connection.execute(
        "SELECT status FROM observations WHERE id = ?", (observation_id,)
    ).fetchone()
    return str(row["status"])


async def _exhaust_down_transition(sim: SimHarness, fixture: Path) -> int:
    """Drive the doomed transition: flip the fixture to down, fire (writes
    exactly one observation), and run the triager through its full retry
    ladder to exhaustion. Returns the doomed observation's id."""
    _set_state(fixture, "down")
    observation_id, outcome = await sim.fire_source(SOURCE)
    assert outcome == "ok"
    assert observation_id is not None, "a state transition must write an observation"

    # Attempt 1 fails immediately; each retry becomes due only after its
    # backoff rung, crossed by advancing the FakeClock. 1 + len(delays)
    # attempts is exactly MAX_RETRY_ATTEMPTS -- pinned so a ladder resize
    # shows up here as a loud arithmetic failure, not a silently shorter test.
    assert MAX_RETRY_ATTEMPTS == 1 + len(TRUST_RETRY_DELAYS)
    assert await sim.run_triage() == 1
    for delay in TRUST_RETRY_DELAYS:
        sim.advance(delay + timedelta(seconds=1))
        assert await sim.run_triage() == 1, "a due retry must be re-picked"

    # The ladder is spent: nothing is ready no matter how far time advances.
    sim.advance(timedelta(days=2))
    assert await sim.run_triage() == 0, "exhaustion must end the retry ladder"
    return observation_id


def test_exhausted_triage_on_transition_loses_product_alert_but_is_loud(
    tmp_path: Path,
) -> None:
    """THE regression (brief item 1). Control first: a clean down transition
    produces the product `down` finding. Then the wedged transition: triage
    exhausts, the product finding never appears, re-fires collapse (no retry
    redundancy), the internal/triage incident is open and belfry's
    failure_surface reports it. Finally recovery: the up transition triages
    fine, clears the internal incident -- and the lost down-alert still never
    existed."""
    fixture, wedge = _write_lodging(tmp_path)
    belfry = _load_belfry()
    db_path = tmp_path / "state" / "angelus.sqlite3"
    watermark = tmp_path / "state" / "belfry.failcheck"

    async def scenario(sim: SimHarness) -> None:
        # -- Control: the product path works when the triager is healthy. --
        _set_state(fixture, "up")
        first_id, _ = await sim.fire_source(SOURCE)
        assert first_id is not None  # first sighting
        assert await sim.run_triage() == 1

        _set_state(fixture, "down")
        control_down_id, _ = await sim.fire_source(SOURCE)
        assert control_down_id is not None
        assert await sim.run_triage() == 1
        control = _product_findings(sim)
        assert [f["type"] for f in control] == ["down"], (
            "control: a healthy triager must turn the down transition into "
            "the product finding"
        )
        assert control[0]["observation_id"] == control_down_id

        # Recover, and close the control's product incident via the
        # clearance, so the emission gate (B30) is re-armed: if the wedged
        # transition below DID produce a finding, the gate could not be what
        # hides it.
        _set_state(fixture, "up")
        recovery_id, _ = await sim.fire_source(SOURCE)
        assert recovery_id is not None
        assert await sim.run_triage() == 1
        assert not [
            i for i in sim.open_incidents() if i["source"] == SOURCE
        ], "the control down incident must be closed before the wedged run"
        findings_before = len(_product_findings(sim))

        # -- The doomed transition: triager wedged, retries exhaust. --------
        wedge.touch()
        doomed_id = await _exhaust_down_transition(sim, fixture)

        # (a) The loud path: internal/triage incident is open...
        internal = _open_internal_triage(sim)
        assert len(internal) == 1, "exhaustion must open the internal/triage incident"
        assert internal[0]["entity"] == TRIAGER

        # ...and belfry's failure_surface (the open-internal-incident read
        # that drives its red ping) reports it. Level-triggered: a second
        # tick stays red.
        for _tick in range(2):
            reason = belfry.failure_surface(db_path, watermark)
            assert reason is not None and "internal/triage" in reason, (
                "belfry must stay red while the internal/triage incident is open"
            )

        # (b) The gap, pinned honestly: the product finding for the doomed
        # transition was never produced. The down-alert is lost.
        assert len(_product_findings(sim)) == findings_before
        assert not [
            i for i in sim.open_incidents() if i["source"] == SOURCE
        ], "no product incident exists for the lost down transition"

        # (c) No retry redundancy via re-observation: the site is still down,
        # but the unchanged state collapses to no observation, so nothing new
        # ever reaches the triager. This is the post-collapse loss itself.
        still_down_id, outcome = await sim.fire_source(SOURCE)
        assert outcome == "ok"
        assert still_down_id is None, "unchanged state must collapse (0012 invariant)"
        assert await sim.run_triage() == 0, (
            "a collapsed fire must not hand the exhausted triager a second chance"
        )

        # The exhausted observation is whole-row terminal -- true both
        # pre-6qsq (mark_triage_failed's first-exhaust flip) and post-6qsq
        # (all-triagers-terminal with this single lodged triager).
        assert _observation_status(sim, doomed_id) == "triage_failed"

        # -- Recovery transition: exactly inverted value. -------------------
        # The next observation arrives only when the site comes back UP. That
        # triage succeeds (the wedge only breaks down-handling) and clears
        # the internal/triage incident -- so belfry goes green again -- yet
        # the down-alert for the doomed transition never existed and now
        # never will.
        _set_state(fixture, "up")
        back_up_id, _ = await sim.fire_source(SOURCE)
        assert back_up_id is not None, "the recovery transition writes the next observation"
        assert await sim.run_triage() == 1
        assert not _open_internal_triage(sim), (
            "a later triage success clears the internal/triage incident"
        )
        assert belfry.failure_surface(db_path, watermark) is None
        doomed = [
            f
            for f in _product_findings(sim)
            if f["observation_id"] == doomed_id
        ]
        assert doomed == [], (
            "the product finding for the exhausted transition is permanently lost"
        )

    with SimHarness(tmp_path, START) as sim:
        asyncio.run(scenario(sim))


def test_reprocess_is_the_heal_seam_disposition(tmp_path: Path) -> None:
    """Brief item 2 (disposition): the natural heal for an exhausted
    transition is catalog.reprocess_source -- delete the observation_triage
    rows so the loop re-picks the observation. This test pins what reprocess
    actually does to an exhausted observation, forking on the one behavior
    brief-20260607-6qsq Stage 1 changes:

    - pre-6qsq (this shard's base): exhaustion flipped the observation row to
      'triage_failed', ready_observations_for only surfaces status='ready',
      and reprocess_source does NOT reset the row -- so the heal seam is
      BROKEN for exactly the observation that needs it. Reprocess deletes
      the triage rows, the loop re-picks nothing, the alert stays lost.
    - post-6qsq (commit e42c0fc): reprocess_source returns consumed /
      triage_failed observations to 'ready', the loop re-picks the doomed
      observation, and a now-healthy triager finally emits the product
      finding.

    The fork is on the observation's post-reprocess STATUS (the structural
    semantic 6qsq changes), and each branch asserts its world's downstream
    consequence, so the test is honest under both and the pre-6qsq branch
    goes dead -- not silently green -- when 6qsq merges. Disposition
    recommendation lives in the shard tender: any exhaustion-recovery wiring
    (operator runbook or a fixers/ auto-reprocess) only works post-6qsq, so
    it must land after that shard merges."""
    fixture, wedge = _write_lodging(tmp_path)

    async def scenario(sim: SimHarness) -> None:
        _set_state(fixture, "up")
        await sim.fire_source(SOURCE)
        assert await sim.run_triage() == 1

        wedge.touch()
        doomed_id = await _exhaust_down_transition(sim, fixture)
        assert _open_internal_triage(sim), "precondition: exhaustion was loud"

        # The transient failure passes (the brief's flaky-subprocess /
        # resource-exhaustion framing), and an operator -- prompted by the
        # internal/triage alert -- reaches for the existing heal seam.
        wedge.unlink()
        reprocessed = sim.daemon.catalog.reprocess_source(SOURCE)
        assert reprocessed >= 1, "reprocess must report the re-budgeted observations"

        status = _observation_status(sim, doomed_id)
        if status == "ready":
            # post-6qsq: the heal completes -- the re-picked observation
            # triages cleanly and the lost product finding finally exists.
            await sim.run_triage()
            healed = [
                f
                for f in _product_findings(sim)
                if f["observation_id"] == doomed_id and f["type"] == "down"
            ]
            assert healed, (
                "post-6qsq reprocess must let the exhausted transition "
                "produce its product finding"
            )
        else:
            # pre-6qsq: the seam is broken for exactly the observation that
            # needs it. Reprocess deleted its triage rows, but the
            # 'triage_failed' row status keeps it invisible to
            # ready_observations_for forever -- while the already-triaged up
            # observation (still status 'ready') IS re-surfaced and gets
            # pointlessly re-run.
            assert status == "triage_failed"
            ready_ids = {
                row["id"]
                for row in sim.daemon.catalog.ready_observations_for(
                    TRIAGER, SOURCE
                )
            }
            assert doomed_id not in ready_ids, (
                "pre-6qsq: reprocess does not surface the exhausted observation"
            )
            await sim.run_triage()
            assert not [
                f
                for f in _product_findings(sim)
                if f["observation_id"] == doomed_id
            ], "pre-6qsq: the alert stays lost even after reprocess"

    with SimHarness(tmp_path, START) as sim:
        asyncio.run(scenario(sim))
