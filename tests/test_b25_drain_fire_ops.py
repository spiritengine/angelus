"""B25 drain / fire-source control ops -- force scheduled work on demand.

Two new control-socket ops let an operator (or agent) run scheduled work
immediately instead of editing cron:

  - ``drain <pipe>`` runs a named pipe's drain now and returns a
    dispatched/failed summary;
  - ``fire_source <name>`` runs a source's check once, producing an
    observation, and returns what it produced.

The acceptance (master brief): "`angelus drain daily` triggers a real daily
drain and returns a summary (dispatched/failed counts); `angelus fire-source
<name>` produces an observation; covered by a test against a running in-process
daemon or the control layer." Both require the daemon -- only the live process
can drain or fire -- so there is no sqlite fallback.

These tests pin:
  - the count semantic: DrainSummary counts CHANNEL SEND ATTEMPTS, not findings
    -- one normal finding fanned to a two-channel pipe is dispatched=2, and a
    fault-failed send is failed=1 (the immediate path records no 'failed'
    dispatch ROW, so a row-count semantic would silently report failed=0);
  - the acceptance drain over the real control socket (delivery happens AND the
    summary is right) and the digest path's own tally;
  - drain rejects an unknown pipe with a structured error;
  - fire_source produces an observation and surfaces its id + outcome, on both
    the ok and check_failed branches, and rejects an unknown source;
  - the manual drain participates in the shutdown reap: it runs inside a task
    tracked in self._drain_tasks (intricacy #2 -- guards the <8s no-hang bound);
  - the existing scheduler callers (_run_drain_job, _fire_source) still work
    after the additive return-type changes.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

import angelus.cli as cli
import angelus.pipes.runner as pipe_runner
from angelus.cli import main
from angelus.daemon import AngelusDaemon
from angelus.pipes import DrainSummary, PipeDrain


def _write_lodging(root: Path) -> None:
    """Lodging with two sources (one clean, one failing), an immediate `now`
    pipe routing to BOTH channels (so the per-send-action count is observable),
    a digest `daily` pipe, and the push/email channels both reference."""
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    (scheduled / "watch.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    # A source whose shell check exits non-zero -> the check_failed branch.
    (scheduled / "failing.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'exit 2'\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    # `now` routes to push AND email so a single normal finding produces two
    # send attempts -- the case that distinguishes a per-send-action count from
    # a per-finding one.
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push, email]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n"
        "rate_limit:\n  per_channel: 6/hr\n  per_source: 4/hr\n  overflow: daily\n",
        encoding="utf-8",
    )
    # A minimal valid digest pipe: empty preamble (no render-templates needed)
    # and an llm body (monkeypatched in the digest test).
    (root / "pipes" / "daily.yaml").write_text(
        "cadence: '0 7 * * *'\nchannels: [email]\n"
        "render:\n  preamble: []\n  body:\n    kind: llm\n"
        "    mantle: chronicler\n    inputs:\n      - findings_since_last_drain\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )
    (root / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: person@example.com\n", encoding="utf-8"
    )


class _Recorder:
    """Channel sender double that succeeds and records which channel it was
    called for -- the same shape B28's tests use so a fault short-circuit is
    visible as an absent call."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, channel, *_args, **_kwargs):
        self.calls.append(channel.name)


def _patch_senders(monkeypatch) -> tuple[_Recorder, _Recorder]:
    push = _Recorder()
    email = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)
    return push, email


def _seed_finding(daemon: AngelusDaemon, entity: str, pipe: str) -> int:
    observation_id = daemon.catalog.write_observation(
        "scheduled/watch", {}, {"source": "scheduled/watch"}
    )
    return daemon.catalog.write_finding(
        observation_id,
        {
            "source": "scheduled/watch",
            "type": "down",
            "entity": entity,
            "severity": "high",
            "target_pipes": [pipe],
        },
        set(daemon.lodging.pipes),
    )


def _queue_status(daemon: AngelusDaemon, finding_id: int, pipe: str) -> str | None:
    row = daemon.catalog.connection.execute(
        "SELECT status FROM pipe_queues WHERE finding_id = ? AND pipe = ?",
        (finding_id, pipe),
    ).fetchone()
    return None if row is None else row["status"]


async def _ask(sock_path: Path, payload: dict) -> dict:
    reader, writer = await asyncio.open_unix_connection(str(sock_path))
    try:
        writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()
        line = await reader.readline()
    finally:
        writer.close()
        await asyncio.gather(writer.wait_closed(), return_exceptions=True)
    return json.loads(line.decode("utf-8"))


# --------------------------------------------------------------------------
# drain: acceptance (real socket), count semantic, failure tally, unknown pipe.
# --------------------------------------------------------------------------


def test_op_drain_delivers_and_counts_send_actions(tmp_path, monkeypatch) -> None:
    """The acceptance drain over the REAL control socket: a pending finding on
    the `now` pipe (which routes to push AND email) is actually delivered, and
    the response summary counts both send attempts.

    Discrimination -- pins the count semantic: ONE finding fanned to TWO healthy
    channels reports dispatched=2 / failed=0, and both channels delivered (queue
    'dispatched'). A per-FINDING count would report dispatched=1; a count that
    missed the fan would report 1. If the op did not actually drain, the queue
    would stay 'pending' and both recorders would be empty.
    """
    _write_lodging(tmp_path)
    push, email = _patch_senders(monkeypatch)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        finding_id = _seed_finding(daemon, "site.example", "now")
        await daemon.control.start()
        try:
            response = await _ask(
                daemon.socket_path, {"op": "drain", "args": {"pipe": "now"}}
            )
            # Read the queue state before tearing the connection down.
            queue_status = _queue_status(daemon, finding_id, "now")
        finally:
            await daemon.control.stop()
            daemon.connection.close()

        assert response["ok"] is True, response
        assert response["result"] == {"pipe": "now", "dispatched": 2, "failed": 0}
        assert sorted(push.calls + email.calls) == ["email", "push"]
        assert queue_status == "dispatched"

    asyncio.run(driver())


def test_op_drain_summary_counts_failures(tmp_path, monkeypatch) -> None:
    """With a fault armed on a channel the drain's send fails, and the summary
    reports it. The `now` pipe routes to push AND email; arming both makes every
    send attempt fail.

    Discrimination: both channel send attempts raise, so the summary is
    dispatched=0 / failed=2 and the finding stays 'pending' (undelivered,
    retryable). This pins that failures are counted at the send-attempt site:
    the IMMEDIATE path records no 'failed' dispatch ROW (it ladders via
    record_immediate_send_failure), so a dispatch-row count would report
    failed=0 here -- the test fails if the tally is reverted to count rows.
    """
    _write_lodging(tmp_path)
    push, email = _patch_senders(monkeypatch)
    daemon = AngelusDaemon(tmp_path)
    try:
        daemon.faults.arm("push")
        daemon.faults.arm("email")
        finding_id = _seed_finding(daemon, "down.example", "now")

        result = asyncio.run(daemon._op_drain({"pipe": "now"}))

        assert result == {"pipe": "now", "dispatched": 0, "failed": 2}
        # The fault short-circuits before the real senders.
        assert push.calls == [] and email.calls == []
        assert _queue_status(daemon, finding_id, "now") == "pending"
    finally:
        daemon.connection.close()


def test_op_drain_daily_digest_returns_summary(tmp_path, monkeypatch) -> None:
    """The acceptance `drain daily`: a digest pipe is drained on demand and the
    summary counts its single channel send. The digest path is a distinct code
    path from the immediate ladder, so its tally is pinned separately.

    Discrimination: one digest send to email reports dispatched=1 / failed=0 and
    email delivered. The llm body and the same-UTC-day guard are stubbed so the
    digest actually ships; if _drain_digest did not tally its send, dispatched
    would be 0 despite the delivery.
    """
    _write_lodging(tmp_path)
    push, email = _patch_senders(monkeypatch)

    async def fake_llm(self, _pipe, _structured):
        return "synthesis paragraph.", None

    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)
    monkeypatch.setattr(pipe_runner, "_is_same_utc_day", lambda *_args: False)

    daemon = AngelusDaemon(tmp_path)
    try:
        finding_id = _seed_finding(daemon, "digest.example", "daily")

        result = asyncio.run(daemon._op_drain({"pipe": "daily"}))

        assert result == {"pipe": "daily", "dispatched": 1, "failed": 0}
        assert email.calls == ["email"]
        assert push.calls == []
        assert _queue_status(daemon, finding_id, "daily") == "dispatched"
    finally:
        daemon.connection.close()


def test_op_drain_unknown_pipe_is_rejected(tmp_path) -> None:
    """Draining a pipe that is not configured is a structured error, not a
    silent no-op -- mirroring fault_inject's unknown-channel rejection.

    Discrimination: the op raises ValueError naming the unknown pipe (which
    ControlServer._dispatch turns into ok=False), and a bad/missing pipe arg is
    likewise rejected. A handler that drained any name would not raise.
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        with pytest.raises(ValueError, match="unknown pipe: ghost"):
            asyncio.run(daemon._op_drain({"pipe": "ghost"}))
        with pytest.raises(ValueError, match="non-empty pipe name"):
            asyncio.run(daemon._op_drain({"pipe": ""}))
        with pytest.raises(ValueError, match="non-empty pipe name"):
            asyncio.run(daemon._op_drain({}))
    finally:
        daemon.connection.close()


# --------------------------------------------------------------------------
# fire_source: acceptance (real socket), check_failed branch, unknown source.
# --------------------------------------------------------------------------


def test_op_fire_source_produces_observation(tmp_path) -> None:
    """The acceptance fire over the REAL control socket: firing a source runs
    its shell check and writes an observation; the response carries the new
    observation id and outcome "ok".

    Discrimination: a fresh observations row exists for the source with status
    'ready', the response observation_id points at it, and outcome is "ok". If
    _fire_source did not return what it produced, the op could not surface the
    id; if the fire did not run, no observation row would appear.
    """
    _write_lodging(tmp_path)

    async def driver() -> dict:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            response = await _ask(
                daemon.socket_path,
                {"op": "fire_source", "args": {"name": "scheduled/watch"}},
            )
            row = daemon.catalog.connection.execute(
                "SELECT source, status FROM observations WHERE id = ?",
                (response["result"]["observation_id"],),
            ).fetchone()
        finally:
            await daemon.control.stop()
            daemon.connection.close()
        return response, dict(row) if row is not None else None

    response, row = asyncio.run(driver())
    assert response["ok"] is True, response
    result = response["result"]
    assert result["source"] == "scheduled/watch"
    assert result["outcome"] == "ok"
    assert isinstance(result["observation_id"], int)
    assert row == {"source": "scheduled/watch", "status": "ready"}


def test_op_fire_source_check_failed_still_observes(tmp_path) -> None:
    """A source whose shell check exits non-zero still writes an observation,
    with the check_failed shape, and the op surfaces outcome "check_failed".

    Discrimination: the response outcome is "check_failed" and the written
    observation body carries type 'check_failed' -- mirroring _fire_source's
    existing failure branch. A handler that only surfaced successful fires, or
    one that skipped writing on failure, would diverge here.
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        result = asyncio.run(daemon._op_fire_source({"name": "scheduled/failing"}))

        assert result["source"] == "scheduled/failing"
        assert result["outcome"] == "check_failed"
        row = daemon.catalog.connection.execute(
            "SELECT body_ref FROM observations WHERE id = ?",
            (result["observation_id"],),
        ).fetchone()
        body = daemon.catalog.read_body(row["body_ref"])
        assert body["type"] == "check_failed"
    finally:
        daemon.connection.close()


def test_op_fire_source_unknown_source_is_rejected(tmp_path) -> None:
    """Firing a source that is not configured is a structured error.

    Discrimination: the op raises ValueError naming the unknown source, and a
    bad/missing name is rejected too. A handler that fired any name would not
    raise (and run_shell_source would KeyError on the missing source instead of
    a clean message).
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        with pytest.raises(ValueError, match="unknown source: ghost"):
            asyncio.run(daemon._op_fire_source({"name": "ghost"}))
        with pytest.raises(ValueError, match="non-empty source name"):
            asyncio.run(daemon._op_fire_source({"name": ""}))
        with pytest.raises(ValueError, match="non-empty source name"):
            asyncio.run(daemon._op_fire_source({}))
    finally:
        daemon.connection.close()


# --------------------------------------------------------------------------
# Intricacy #2: the manual drain participates in the shutdown reap. It must run
# inside a task tracked in self._drain_tasks, so run()'s finally cancels and
# awaits it on shutdown -- guarding the <8s no-hang bound.
# --------------------------------------------------------------------------


def test_op_drain_runs_in_a_tracked_task(tmp_path, monkeypatch) -> None:
    """A drain triggered via the op runs inside a task registered in
    self._drain_tasks for its whole lifetime -- the same tracking a scheduled
    drain gets, which is what lets run()'s shutdown finally reap it.

    Discrimination: the running drain coroutine observes its own task in
    daemon._drain_tasks. If the op called drain.drain_once() raw (bypassing
    _run_drain_job's create_task + tracking), the running task would be the
    caller's, absent from _drain_tasks, and this fails -- exactly the regression
    that could let a manual drain outlive shutdown.
    """
    _write_lodging(tmp_path)
    _patch_senders(monkeypatch)
    daemon = AngelusDaemon(tmp_path)
    try:
        _seed_finding(daemon, "tracked.example", "now")
        drain = daemon.pipe_drains["now"]
        original = drain.drain_once
        seen: dict[str, object] = {}

        async def tracking_drain():
            current = asyncio.current_task()
            seen["tracked"] = current in daemon._drain_tasks
            seen["nonempty"] = len(daemon._drain_tasks) >= 1
            return await original()

        monkeypatch.setattr(drain, "drain_once", tracking_drain)

        asyncio.run(daemon._op_drain({"pipe": "now"}))

        assert seen["tracked"] is True, "the running drain was not in _drain_tasks"
        assert seen["nonempty"] is True
        # And the tracking is balanced: discarded once the drain returns.
        assert daemon._drain_tasks == set()
    finally:
        daemon.connection.close()


# --------------------------------------------------------------------------
# No regression: the existing scheduler callers still work after the additive
# return-type changes (drain_once -> DrainSummary, _fire_source -> tuple).
# --------------------------------------------------------------------------


def test_scheduler_callers_unaffected_by_return_changes(tmp_path, monkeypatch) -> None:
    """The exact callables APScheduler invokes -- _run_drain_job (pipe job) and
    _fire_source (source job) -- still perform their work after the return-type
    changes; the scheduler simply ignores the new return values.

    Discrimination: _run_drain_job delivers the finding (queue 'dispatched') and
    now returns a DrainSummary; _fire_source writes an observation and now
    returns (id, "ok"). If the additive change had broken either scheduled path,
    the delivery/observation side effects would be missing.
    """
    _write_lodging(tmp_path)
    push, email = _patch_senders(monkeypatch)
    daemon = AngelusDaemon(tmp_path)
    try:
        finding_id = _seed_finding(daemon, "sched.example", "now")

        summary = asyncio.run(daemon._run_drain_job("now"))
        assert isinstance(summary, DrainSummary)
        assert (summary.dispatched, summary.failed) == (2, 0)
        assert _queue_status(daemon, finding_id, "now") == "dispatched"
        assert daemon._drain_tasks == set(), "the job discarded its own task"

        fire = asyncio.run(daemon._fire_source("scheduled/watch"))
        assert fire is not None
        observation_id, outcome = fire
        assert outcome == "ok"
        row = daemon.catalog.connection.execute(
            "SELECT source, status FROM observations WHERE id = ?",
            (observation_id,),
        ).fetchone()
        assert row["source"] == "scheduled/watch" and row["status"] == "ready"
    finally:
        daemon.connection.close()


# --------------------------------------------------------------------------
# The click command bodies: each maps to the right control request and renders
# its result as screen-reader plain text (one value per line). The request
# layer is stubbed -- both commands REQUIRE the daemon, so there is no sqlite
# fallback to exercise.
# --------------------------------------------------------------------------


def test_cli_drain_and_fire_source_command_dispatch(tmp_path, monkeypatch) -> None:
    """`angelus drain <pipe>` and `angelus fire-source <name>` send exactly one
    request with the right op (note the op key is `fire_source`, the CLI command
    is `fire-source`) and render the result one value per line.

    Discrimination: drain sends {"op": "drain", "pipe": ...} and prints
    dispatched/failed on their own lines; fire-source sends
    {"op": "fire_source", "name": ...} and prints the observation id + outcome.
    If a command sent the wrong op key (e.g. `fire-source` instead of
    `fire_source`) the recorded calls invert; a tabular renderer would put
    values on one line.
    """
    calls: list[tuple[str, dict]] = []

    def fake_request(root, op, args):
        calls.append((op, dict(args)))
        if op == "drain":
            return {"ok": True, "result": {"pipe": args["pipe"], "dispatched": 2, "failed": 1}}
        return {
            "ok": True,
            "result": {"source": args["name"], "observation_id": 7, "outcome": "ok"},
        }

    monkeypatch.setattr(cli, "_request", fake_request)
    runner = CliRunner()
    root_args = ["--root", str(tmp_path)]

    drained = runner.invoke(main, ["drain", "now", *root_args])
    assert drained.exit_code == 0, drained.output
    assert "pipe: now" in drained.output
    assert "dispatched: 2" in drained.output
    assert "failed: 1" in drained.output

    fired = runner.invoke(main, ["fire-source", "scheduled/watch", *root_args])
    assert fired.exit_code == 0, fired.output
    assert "source: scheduled/watch" in fired.output
    assert "observation: 7" in fired.output
    assert "outcome: ok" in fired.output

    assert calls == [
        ("drain", {"pipe": "now"}),
        ("fire_source", {"name": "scheduled/watch"}),
    ]
