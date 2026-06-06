"""B26 sim harness -- offline, scriptable replay of a full source -> dispatch
cycle under a pinned clock, no cron and no real waiting (master brief
brief-20260529-fv9n, deps B24 clock seam + B25 drain/fire ops).

The harness (angelus/sim.py) drives the REAL production step methods --
AngelusDaemon._fire_source, the shared _discover_ready_triage sweep +
_triage_under_semaphore path, and _run_drain_job -- under a FakeClock, never
starting the scheduler / control socket / any loop. These tests pin:

  - ACCEPTANCE: one synchronous test runs fire -> triage -> drain and a real
    dispatch lands, with no scheduler and no real sleep.
  - TIME CONTROL: advancing the clock a full day moves the digest's rendered
    date -- "a simulated day in seconds".
  - DETERMINISM: run_triage() completes (findings written) before drain() sees
    them, so a following drain can't race.
  - THE CLOCK SEAM: a daemon built with a FakeClock stamps row timestamps from
    it; a daemon built with no clock keeps the real Clock (production unchanged).
  - NO REAL NOTIFICATION: a sim send writes to dispatches.log and never shells
    the channel command (notify-pat).
  - CLI: `angelus sim <script>` runs a scripted cycle and exits 0 with a
    plain-text report.

Each test's discrimination (what reverting the behavior breaks) is called out in
its docstring; the module's mutation log is in the B26 deliverable note.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

from click.testing import CliRunner

import angelus.channels.push as push_channel
from angelus.cli import main
from angelus.clock import Clock, FakeClock
from angelus.daemon import AngelusDaemon
from angelus.pipes import PipeDrain
from angelus.sim import SimHarness

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# A fixed, far-from-now start instant so a leaked real-wall-clock timestamp is
# unmistakable: a 2026-06-06 noon-UTC row cannot be confused for `now`.
START = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)


def _write_fixture(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_lodging(root: Path, status_code: int = 503) -> Path:
    """A self-contained lodging: one shell source whose check `cat`s a JSON
    status fixture, the real canary_watch triager (which emits a `down` finding
    on the up->down edge and routes it to BOTH `now` and `daily`), an immediate
    `now` pipe to push, and a `daily` digest pipe to email. Mirrors the shape
    the m2/b25 tests use so the sim exercises the production triager/pipe
    surface, not a double.

    Returns the fixture path so a test can rewrite the status between fires.
    """
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    fixtures = root / "fixtures"
    fixtures.mkdir()
    fixture = fixtures / "watch.json"
    _write_fixture(
        fixture,
        {
            "source_ref": "scheduled/watch",
            "entity": "site.example",
            "url": "https://site.example",
            "status_code": status_code,
        },
    )
    (scheduled / "watch.yaml").write_text(
        f"cadence: 1h\ncheck:\n  kind: shell\n  command: 'cat {fixture}'\n",
        encoding="utf-8",
    )

    (root / "triagers" / "handlers").mkdir(parents=True)
    shutil.copy(
        PROJECT_ROOT / "triagers" / "handlers" / "canary_watch.py",
        root / "triagers" / "handlers" / "canary_watch.py",
    )
    (root / "triagers" / "watch.yaml").write_text(
        "inputs:\n  source: scheduled/watch\n"
        "handler:\n  kind: python\n  path: triagers/handlers/canary_watch.py\n",
        encoding="utf-8",
    )

    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n"
        "  template: '[now] {source} {type}:{entity}'\n",
        encoding="utf-8",
    )
    (root / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [email]\n"
        "render:\n  preamble: []\n  body:\n    kind: llm\n"
        "    mantle: chronicler\n    inputs:\n      - findings_since_last_drain\n",
        encoding="utf-8",
    )

    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )
    (root / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: person@example.com\n",
        encoding="utf-8",
    )
    return fixture


# --------------------------------------------------------------------------
# ACCEPTANCE: a full source -> dispatch cycle, synchronously, under a FakeClock,
# with no scheduler and no real sleep.
# --------------------------------------------------------------------------


def test_full_cycle_fire_triage_drain_dispatches(tmp_path) -> None:
    """The headline acceptance: fire a source, triage the observation, drain the
    `now` pipe, and a real dispatch lands -- all in one synchronous test driving
    the production step methods under a pinned clock.

    Discrimination: the down finding reaches the `now` queue (one finding), the
    drain reports dispatched=1, the queue row flips to 'dispatched', and the
    dry-run send writes the rendered alert to dispatches.log. If any production
    step were stubbed rather than reused -- the fire not writing an observation,
    triage not running, the drain not draining -- the dispatch would be absent.
    No scheduler is started (the sim never calls run()) and nothing sleeps.
    """
    _write_lodging(tmp_path, status_code=503)

    async def scenario() -> None:
        with SimHarness(tmp_path, START) as sim:
            # No scheduler/loop is running: the harness only constructed the
            # daemon, it never called run().
            assert not sim.daemon.scheduler.running

            observation_id, outcome = await sim.fire_source("scheduled/watch")
            assert isinstance(observation_id, int)
            assert outcome == "ok"

            triaged = await sim.run_triage()
            assert triaged == 1, f"expected one observation triaged, got {triaged}"

            now_findings = sim.findings_for_pipe("now")
            assert len(now_findings) == 1, (
                "the down-edge finding must be queued to `now` before drain"
            )

            summary = await sim.drain("now")
            assert (summary.dispatched, summary.failed) == (1, 0), summary

            queue_status = sim.daemon.catalog.connection.execute(
                "SELECT status FROM pipe_queues WHERE pipe = 'now'"
            ).fetchone()["status"]
            assert queue_status == "dispatched"

            dispatched = sim.dispatches()
            assert dispatched == ["[now] scheduled/watch down:site.example"], (
                f"the dry-run send must land in dispatches.log; got {dispatched}"
            )

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# TIME CONTROL: advancing the clock a full day moves the digest's rendered date.
# --------------------------------------------------------------------------


def test_advancing_a_day_moves_the_digest_date(tmp_path, monkeypatch) -> None:
    """Pin the clock, queue a finding, jump the clock a full day, then drain the
    daily digest: the rendered subject carries the ADVANCED calendar date, not
    the start date -- proving a simulated day passed with no real waiting.

    Discrimination: the subject is built from the drain-time clock
    (PipeDrain._drain_digest reads self._clock.now_local()), which is the
    injected FakeClock. After advancing one day the rendered day-of-month is the
    advanced day, distinct from the start day. If advance() did not move the
    clock the render reads (or the clock were not threaded into the drain), the
    subject would still show the start date and the assertion fails.
    """
    _write_lodging(tmp_path, status_code=503)

    async def fake_llm(self, _pipe, _structured):
        # Render-only stub: a real chronicler body would burn a horizon cast.
        return "synthesis paragraph.", None

    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    async def scenario() -> None:
        with SimHarness(tmp_path, START) as sim:
            await sim.fire_source("scheduled/watch")
            await sim.run_triage()

            start_day = sim.clock.now_local().day
            sim.advance(timedelta(days=1))
            advanced = sim.clock.now_local()
            assert advanced.day != start_day, "sanity: a day boundary was crossed"

            summary = await sim.drain("daily")
            assert summary.dispatched == 1, summary

            expected_subject = (
                f"Angelus Observances for {advanced.strftime('%A %B')} "
                f"{advanced.day}, {advanced.year}"
            )
            email_lines = [
                line for line in sim.dispatches() if line.startswith("email:")
            ]
            assert len(email_lines) == 1, email_lines
            assert expected_subject in email_lines[0], (
                f"digest subject must render the advanced date; "
                f"expected {expected_subject!r} in {email_lines[0]!r}"
            )
            # And the start date is NOT what shipped -- the clock genuinely moved.
            assert f", {advanced.year}" in email_lines[0]
            assert f"{advanced.strftime('%A %B')} {start_day}," not in email_lines[0]

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# DETERMINISM: run_triage() completes before drain() can see the findings.
# --------------------------------------------------------------------------


def test_run_triage_completes_before_returning(tmp_path) -> None:
    """run_triage() must fully finish every ready observation -- findings written
    -- before it returns, so a following drain never races a half-run triage.

    Discrimination: immediately after run_triage() returns, and before any
    drain, the down finding already exists in the catalog (findings table count
    is 1 and the `now`/`daily` queues are populated). If run_triage() returned
    after merely discovering/marking the observations without awaiting their
    triage to completion, the findings would not yet exist and this fails
    synchronously.
    """
    _write_lodging(tmp_path, status_code=503)

    async def scenario() -> None:
        with SimHarness(tmp_path, START) as sim:
            await sim.fire_source("scheduled/watch")
            triaged = await sim.run_triage()
            assert triaged == 1

            # Synchronous: no await between run_triage returning and this read.
            finding_count = sim.daemon.catalog.connection.execute(
                "SELECT COUNT(*) AS n FROM findings"
            ).fetchone()["n"]
            assert finding_count == 1, (
                "run_triage must have written the finding before returning; "
                f"found {finding_count}"
            )
            # Both target pipes are queued, ready for an immediate drain.
            assert len(sim.findings_for_pipe("now")) == 1
            assert len(sim.findings_for_pipe("daily")) == 1

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# THE CLOCK SEAM: injected FakeClock stamps rows; no clock keeps the real one.
# --------------------------------------------------------------------------


def test_injected_clock_stamps_rows_default_keeps_real_clock(tmp_path) -> None:
    """A daemon built with a FakeClock stamps every catalog timestamp from it;
    a daemon built with NO clock keeps the real wall Clock (production path,
    unchanged). This is the one production seam B26 adds.

    Discrimination: under the harness's FakeClock pinned to START, an injected
    observation row's written_at is exactly START's ISO string -- a value the
    real clock could never produce for a 2026-06-06 noon pin. And a bare
    AngelusDaemon(root) (the production constructor, no clock kwarg) has a plain
    Clock on its catalog, not a FakeClock. Reverting the constructor seam to
    hardcode `self.clock = Clock()` makes the injected case stamp real-now and
    this fails.
    """
    _write_lodging(tmp_path, status_code=200)

    with SimHarness(tmp_path, START) as sim:
        observation_id = sim.inject_observation(
            "scheduled/watch",
            {"source_ref": "scheduled/watch", "entity": "x", "status_code": 200},
        )
        written_at = sim.daemon.catalog.connection.execute(
            "SELECT written_at FROM observations WHERE id = ?",
            (observation_id,),
        ).fetchone()["written_at"]
        assert written_at == "2026-06-06T12:00:00.000Z", (
            "the injected FakeClock must stamp the observation timestamp; "
            f"got {written_at!r}"
        )
        assert isinstance(sim.daemon.clock, FakeClock)

    # Production constructor: no clock kwarg -> real Clock, not a FakeClock.
    production = AngelusDaemon(tmp_path)
    try:
        assert type(production.clock) is Clock
        assert type(production.catalog._clock) is Clock
        # The drains share that same real clock, threaded in __init__.
        for drain in production.pipe_drains.values():
            assert type(drain._clock) is Clock
    finally:
        production.connection.close()


# --------------------------------------------------------------------------
# NO REAL NOTIFICATION: a sim send writes to dispatches.log, never shells the
# channel command.
# --------------------------------------------------------------------------


def test_sim_send_never_shells_the_channel_command(tmp_path, monkeypatch) -> None:
    """A sim drain must deliver via the dry-run path (a dispatches.log line) and
    NEVER actually invoke notify-pat -- a scenario can't page Patrick's phone.

    Discrimination: the real-send branch of send_push goes through
    asyncio.create_subprocess_exec; we replace it with a spy that records any
    call. After a full cycle the spy is empty (the dry-run branch was taken) and
    dispatches.log carries the alert. If the harness did NOT force dry-run, the
    drain would hit create_subprocess_exec -- the spy would fire and the
    dispatches.log line would be absent.
    """
    _write_lodging(tmp_path, status_code=503)

    shelled: list[tuple] = []
    # send_push and the triager runner both reach create_subprocess_exec on the
    # shared asyncio module. Only the channel command (notify-pat) is the "real
    # notification" we must never shell; the triager subprocess is faithful and
    # fine offline, so the spy intercepts only notify-pat and delegates the rest.
    real_exec = push_channel.asyncio.create_subprocess_exec

    async def spy_exec(*args, **kwargs):
        if args and args[0] == "notify-pat":
            shelled.append(args)
            raise AssertionError("notify-pat was shelled in a sim")
        return await real_exec(*args, **kwargs)

    monkeypatch.setattr(push_channel.asyncio, "create_subprocess_exec", spy_exec)

    async def scenario() -> None:
        with SimHarness(tmp_path, START) as sim:
            await sim.fire_source("scheduled/watch")
            await sim.run_triage()
            summary = await sim.drain("now")
            assert summary.dispatched == 1
            assert sim.dispatches() == ["[now] scheduled/watch down:site.example"]

    asyncio.run(scenario())
    assert shelled == [], "no channel command should have been shelled"


# --------------------------------------------------------------------------
# CLI: `angelus sim <script>` runs a scripted cycle and exits 0 with a
# plain-text report.
# --------------------------------------------------------------------------


def test_cli_sim_runs_scripted_cycle(tmp_path) -> None:
    """`angelus sim <script>` parses a YAML step list, drives the harness through
    a full fire -> triage -> drain cycle offline, and prints a plain-text report
    (one value per line) exiting 0.

    Discrimination: the report carries the fire outcome, the triage count, the
    drain summary, and the dispatched alert line -- each on its own line. If the
    command did not actually run the cycle (e.g. drained nothing), the
    dispatched line would be absent and `dispatched 1` would read `dispatched 0`.
    The script pins `start`, so nothing depends on real time.
    """
    _write_lodging(tmp_path, status_code=503)
    script = tmp_path / "scenario.yaml"
    script.write_text(
        "start: '2026-06-06T12:00:00Z'\n"
        "steps:\n"
        "  - fire_source: scheduled/watch\n"
        "  - run_triage\n"
        "  - drain: now\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        main, ["sim", str(script), "--root", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    out = result.output
    assert "sim start: 2026-06-06T12:00:00.000Z" in out
    assert "fire_source scheduled/watch: observation 1 outcome ok" in out
    assert "run_triage: 1 observations triaged" in out
    assert "drain now: dispatched 1 failed 0" in out
    assert "[now] scheduled/watch down:site.example" in out
    assert "sim complete" in out
