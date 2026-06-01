"""M1 cross-slice integration fell.

Every slice (0..5c) was felled in isolation. These tests stand up ONE
AngelusDaemon with all subsystems live and exercise the five interaction
risks the per-slice fells could not see, by reproducing the actual race
window (a slow point monkeypatched inside apply_lodging / _cancel_pipe_loop,
or a real run() shutdown) rather than calling things sequentially.

Each test is discriminating: the inversion that makes it fail is recorded
in FELL_NOTES.md at the repo root.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.daemon import AngelusDaemon
from angelus.clock import utcnow
from angelus.lodging.reloader import LodgingReloader
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db

REPO_ROOT = Path(__file__).resolve().parents[1]


# --- shared lodging fixtures ---------------------------------------------


def _base_lodging(root: Path, *, source_cmd: str = "echo {}") -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        f"cadence: 1s\ncheck:\n  kind: shell\n  command: {json.dumps(source_cmd)}\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )


def _add_immediate_pipe(root: Path, name: str) -> None:
    (root / "pipes" / f"{name}.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _forking_hang(marker: Path) -> str:
    """dash stays resident; the real sleep is a backgrounded grandchild
    whose pid lands in `marker`. A bare `sleep 30` would be exec'd
    directly and a leader-only kill would reap it anyway (non-
    discriminating); only the process-group kill reaps this grandchild."""
    return f"sleep 30 & echo $! > {marker}; wait"


# --- Risk 1: hot-reload vs the live control socket -----------------------


def test_control_op_sees_coherent_lodging_during_slow_reload(tmp_path) -> None:
    """A control write op issued while apply_lodging is parked at an
    `await _cancel_pipe_loop` point must observe a fully-swapped
    self.lodging (the swap is one assignment before any await), never a
    half-state, and replay's at-least-once idempotency guard must hold.

    Discrimination (recorded in FELL_NOTES): moving
    `self.lodging = new_lodging` to AFTER the await in apply_lodging makes
    the concurrent op observe the OLD pipe set and this test fails.
    """
    _base_lodging(tmp_path)
    _add_immediate_pipe(tmp_path, "extra")
    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)

    finding_id = daemon.catalog.write_finding(
        None,
        {"source": "s", "type": "down", "entity": "e",
         "target_pipes": ["now", "extra"]},
        set(daemon.lodging.pipes),
    )

    observed: dict[str, object] = {}
    real_cancel = daemon._cancel_pipe_loop

    async def slow_cancel(name: str) -> None:
        observed["lodging_during_await"] = set(daemon.lodging.pipes)
        observed["drains_during_await"] = set(daemon.pipe_drains)
        await asyncio.sleep(0.25)
        await real_cancel(name)

    async def driver() -> None:
        daemon._cancel_pipe_loop = slow_cancel  # type: ignore[method-assign]
        daemon.scheduler.start(paused=True)
        try:
            # Only spawn 'extra' -- a 'now' loop would dispatch the seeded
            # finding before _op_replay runs and replay would correctly
            # report 'requeued' instead of 'already_queued', defeating the
            # idempotency-guard assertion this test is built to make.
            # _cancel_pipe_loop('extra') still parks apply_lodging in the
            # await we need.
            daemon._spawn_pipe_loop("extra")
            (tmp_path / "pipes" / "extra.yaml").unlink()
            reloader.event_queue.put(str(tmp_path / "pipes" / "extra.yaml"))
            apply_task = asyncio.create_task(reloader.process_pending_events())
            # Let apply_lodging reach the parked await.
            for _ in range(50):
                if "lodging_during_await" in observed:
                    break
                await asyncio.sleep(0.01)
            assert "lodging_during_await" in observed, "race window never opened"

            # Concurrent ops WHILE apply_lodging is parked mid-reload.
            replay = await daemon._op_replay({"finding_id": finding_id})
            dep = await daemon._op_dep_record(
                {"name": "skein", "status": "unhealthy", "detail": "x"}
            )
            health = await daemon._op_health({})
            observed["replay"] = replay
            observed["dep"] = dep
            observed["health_deps"] = {
                d["dependency_name"] for d in health["deps"]
            }
            await apply_task
        finally:
            daemon.scheduler.shutdown(wait=False)
            daemon.connection.close()

    asyncio.run(driver())

    # self.lodging is one assignment before the first await: parked
    # mid-reload it already reads as the fully-NEW pipe set ('extra'
    # gone), even though pipe_drains is still mid-swap (the genuinely
    # torn structure -- which no control op reads).
    assert observed["lodging_during_await"] == {"now"}
    assert observed["drains_during_await"] == {"now", "extra"}
    # replay used set(self.lodging.pipes) = the new {'now'} consistently;
    # both target rows were already 'pending' from write_finding, so the
    # mandatory double-dispatch guard returns already_queued (no requeue).
    assert observed["replay"] == {
        "outcome": "already_queued", "finding_id": finding_id, "pipes": []
    }
    assert observed["dep"] == {"name": "skein", "status": "unhealthy"}
    assert observed["health_deps"] == {"skein"}


# --- Risk 2: hot-reload vs the dep registry ------------------------------


def test_dep_health_pruned_when_dependency_hot_removed(tmp_path) -> None:
    """Removing dependencies/<name>.yaml must drop its dep_health row.
    Otherwise the row orphans: nothing else prunes dep_health and an
    unlodged dependency can never get another dep_record, so the health
    op would surface a frozen, unrecoverable status forever.

    Discrimination (recorded in FELL_NOTES): deleting the
    `delete_dep_health` loop from apply_lodging leaves the row and the
    health op still lists 'skein' -> this test fails.
    """
    _base_lodging(tmp_path)
    (tmp_path / "dependencies").mkdir()
    (tmp_path / "dependencies" / "skein.yaml").write_text(
        "name: skein\ncheck: skein --help\n", encoding="utf-8"
    )
    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)

    async def driver() -> None:
        try:
            assert "skein" in daemon.lodging.dependencies
            daemon.catalog.record_dep_health(
                "skein", "unhealthy", "2026-05-19T00:00:00.000Z", "down"
            )
            assert daemon.catalog.all_dep_health()  # row exists

            (tmp_path / "dependencies" / "skein.yaml").unlink()
            reloader.event_queue.put(
                str(tmp_path / "dependencies" / "skein.yaml")
            )
            await reloader.process_pending_events()

            assert "skein" not in daemon.lodging.dependencies
            assert daemon.catalog.all_dep_health() == []
            health = await daemon._op_health({})
            assert health["deps"] == []
        finally:
            daemon.connection.close()

    asyncio.run(driver())


# No standalone test for "dep_record concurrent with dependency reload."
# A round-1 readonly fell (issue-20260519-e5hr) flagged the prior attempt
# as sequential masquerading as concurrent: _op_dep_record has zero awaits
# in its body (verified in angelus/daemon.py, its docstring states the
# property explicitly), so `await daemon._op_dep_record(...)` does not
# yield to the event loop; a reload task created beforehand cannot
# interleave inside it. There is no concurrent window inside dep_record
# itself worth probing as a separate test. The interaction surface that
# IS worth probing -- can a control op observe a half-swapped self.lodging
# while a reload is mid-flight at an `await self._cancel_pipe_loop` point
# in apply_lodging -- is Risk 1 and is exercised by
# test_control_op_sees_coherent_lodging_during_slow_reload above. dep_record
# is one such control op; its lodging-reading site is
# `set(self.lodging.pipes)` in the write_internal_finding call, structurally
# identical to the lodging read the Risk 1 test exercises with replay.


# --- Risk 3: control socket shutdown with all subsystems live ------------


def test_full_daemon_shutdown_is_bounded_and_reaps_source_subprocess(
    tmp_path, monkeypatch,
) -> None:
    """run() shutdown with scheduler + reloader + control + a source-fire
    subprocess all live: must NOT hang (AsyncIOScheduler.shutdown is
    non-blocking -- call_soon_threadsafe -- and AsyncIOExecutor.shutdown
    only .cancel()s pending futures, so there is no deadlock) AND must
    not orphan the cancelled source check subprocess/group.

    Discrimination (recorded in FELL_NOTES): removing the
    `except asyncio.CancelledError: await _kill_and_reap(process); raise`
    arm from run_shell_source leaves the forking grandchild alive after
    shutdown -> this test fails.
    """
    marker = tmp_path / "src_child.pid"
    _base_lodging(tmp_path, source_cmd=_forking_hang(marker))
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")

    async def driver() -> int:
        daemon = AngelusDaemon(tmp_path)
        task = asyncio.create_task(daemon.run())
        try:
            for _ in range(300):
                if marker.exists() and marker.read_text().strip():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("source fire never launched its child")
            gc_pid = int(marker.read_text().strip())
            assert _alive(gc_pid)

            started = time.monotonic()
            daemon.request_stop()
            await asyncio.wait_for(task, timeout=15.0)
            elapsed = time.monotonic() - started
            # No deadlock/hang: scheduler.shutdown does not block the loop.
            assert elapsed < 8.0, f"shutdown took {elapsed:.1f}s (hang)"
            return gc_pid
        finally:
            if not task.done():
                daemon.request_stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    gc_pid = asyncio.run(driver())
    # The cancelled source subprocess's whole group was reaped: no orphan.
    for _ in range(200):
        if not _alive(gc_pid):
            break
        time.sleep(0.01)
    else:
        os.kill(gc_pid, 9)
        raise AssertionError(
            f"grandchild {gc_pid} survived daemon shutdown -- "
            "cancelled source subprocess was orphaned"
        )


def test_full_daemon_shutdown_reaps_digest_llm_subprocess(
    tmp_path, monkeypatch,
) -> None:
    """A digest pipe drains from an APScheduler interval job;
    AsyncIOExecutor.shutdown() cancels that job task on shutdown. The
    `horizon` subtree it launched must be reaped, not orphaned.

    Discrimination (recorded in FELL_NOTES): removing the CancelledError
    arm from _render_llm_body leaves the forking `horizon` grandchild
    alive after shutdown -> this test fails.
    """
    marker = tmp_path / "hz_child.pid"
    _base_lodging(tmp_path)
    (tmp_path / "pipes" / "daily.yaml").write_text(
        "cadence: 1s\nchannels: [email]\n"
        "render:\n  preamble: []\n  body:\n    kind: llm\n"
        "    mantle: chronicler\n    inputs: [open_incidents]\n",
        encoding="utf-8",
    )
    (tmp_path / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: x@example.com\n", encoding="utf-8"
    )
    (tmp_path / "render-templates").mkdir()
    stub = tmp_path / "horizon"
    stub.write_text(f"#!/bin/sh\nsleep 30 & echo $! > {marker}\nwait\n")
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")

    async def driver() -> int:
        daemon = AngelusDaemon(tmp_path)
        task = asyncio.create_task(daemon.run())
        try:
            for _ in range(400):
                if marker.exists() and marker.read_text().strip():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("digest job never launched horizon")
            gc_pid = int(marker.read_text().strip())
            assert _alive(gc_pid)
            started = time.monotonic()
            daemon.request_stop()
            await asyncio.wait_for(task, timeout=15.0)
            assert time.monotonic() - started < 8.0, "shutdown hang"
            return gc_pid
        finally:
            if not task.done():
                daemon.request_stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    gc_pid = asyncio.run(driver())
    for _ in range(200):
        if not _alive(gc_pid):
            break
        time.sleep(0.01)
    else:
        os.kill(gc_pid, 9)
        raise AssertionError(
            f"horizon grandchild {gc_pid} survived shutdown -- "
            "cancelled digest subprocess was orphaned"
        )


def test_digest_drain_task_is_tracked_in_flight_and_awaited_on_shutdown(
    tmp_path, monkeypatch,
) -> None:
    """Directly pins the Option-2 fix: a non-immediate (interval) digest
    pipe's drain registers its asyncio task in daemon._drain_tasks for the
    duration of the drain, and daemon.run()'s shutdown-finally cancels and
    awaits that set so the CancelledError reap arm runs before the loop
    closes.

    Where the reaps-orphan test only observes the *effect* (grandchild
    reaped), this test observes the *mechanism*: the in-flight set is
    populated while the horizon subprocess is mid-render, and the same task
    is done (awaited) with the set drained after shutdown. Discrimination:
    scheduling drain_once directly (untracked, the pre-fix shape) leaves
    _drain_tasks empty mid-drain -> the first assertion fails.
    """
    marker = tmp_path / "hz_child.pid"
    _base_lodging(tmp_path)
    (tmp_path / "pipes" / "daily.yaml").write_text(
        "cadence: 1s\nchannels: [email]\n"
        "render:\n  preamble: []\n  body:\n    kind: llm\n"
        "    mantle: chronicler\n    inputs: [open_incidents]\n",
        encoding="utf-8",
    )
    (tmp_path / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: x@example.com\n", encoding="utf-8"
    )
    (tmp_path / "render-templates").mkdir()
    stub = tmp_path / "horizon"
    stub.write_text(f"#!/bin/sh\nsleep 30 & echo $! > {marker}\nwait\n")
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")

    async def driver() -> tuple[int, asyncio.Task]:
        daemon = AngelusDaemon(tmp_path)
        task = asyncio.create_task(daemon.run())
        try:
            for _ in range(400):
                if marker.exists() and marker.read_text().strip():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("digest job never launched horizon")
            gc_pid = int(marker.read_text().strip())
            assert _alive(gc_pid)
            # The drain is mid-render (horizon launched): its task must be
            # registered in the daemon-owned in-flight set.
            assert daemon._drain_tasks, (
                "drain task not tracked in _drain_tasks while mid-drain"
            )
            (drain_task,) = tuple(daemon._drain_tasks)
            assert not drain_task.done()

            daemon.request_stop()
            await asyncio.wait_for(task, timeout=15.0)
            # The tracked drain task was awaited (done) and the set drained
            # by its own finally during the shutdown gather.
            assert drain_task.done()
            assert not daemon._drain_tasks
            return gc_pid, drain_task
        finally:
            if not task.done():
                daemon.request_stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    gc_pid, _ = asyncio.run(driver())
    for _ in range(200):
        if not _alive(gc_pid):
            break
        time.sleep(0.01)
    else:
        os.kill(gc_pid, 9)
        raise AssertionError(
            f"horizon grandchild {gc_pid} survived shutdown -- "
            "tracked digest drain was not reaped"
        )


def test_render_llm_body_reaps_horizon_when_cancelled_inside_spawn(
    tmp_path, monkeypatch,
) -> None:
    """WINDOW A regression: cancel the drain while it is parked *inside*
    create_subprocess_exec -- before the Process handle is bound in
    _render_llm_body -- and the forking `horizon` subtree must still be
    reaped, not orphaned.

    The reaps-orphan test (test_full_daemon_shutdown_reaps_digest_llm_
    subprocess) and the in-flight-tracking test both leave the *timing* of
    the cancellation to chance: under load the cancel usually lands in
    Window A (mid-spawn), but on a quiet machine it can land in Window B
    (mid-communicate), so neither pins Window A deterministically. This test
    forces Window A by wrapping create_subprocess_exec in a stub that spawns
    the real horizon (so a real process group + grandchild exists), signals
    the test, and only THEN -- still 'inside' the spawn coroutine -- pauses
    before returning the handle. The cancel arrives during that pause, so
    `process` is provably unbound when CancelledError is raised.

    Discrimination: drop the asyncio.shield / _recover_cancelled_spawn
    recovery (revert to a bare `process = await create_subprocess_exec(...)`)
    and the except arm has no handle to reap -- the grandchild survives and
    the final liveness check fails. The shield is what makes the handle
    recoverable after a mid-spawn cancel.
    """
    marker = tmp_path / "hz_child.pid"
    _base_lodging(tmp_path)
    (tmp_path / "pipes" / "daily.yaml").write_text(
        "cadence: 1s\nchannels: [email]\n"
        "render:\n  preamble: []\n  body:\n    kind: llm\n"
        "    mantle: chronicler\n    inputs: [open_incidents]\n",
        encoding="utf-8",
    )
    (tmp_path / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: x@example.com\n", encoding="utf-8"
    )
    (tmp_path / "render-templates").mkdir()
    stub = tmp_path / "horizon"
    stub.write_text(f"#!/bin/sh\nsleep 30 & echo $! > {marker}\nwait\n")
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")

    real_exec = asyncio.create_subprocess_exec

    async def driver() -> int:
        spawn_started = asyncio.Event()

        async def slow_spawn(*args, **kwargs):
            # Spawn the real horizon stub so a genuine process group +
            # forked grandchild exists, then hang BEFORE returning the
            # handle. This is exactly the Window A shape: the child is alive
            # but the caller has not yet received `process`. A real
            # create_subprocess_exec returns in ms; the sleep only widens
            # the window so the test can land its cancel inside it.
            proc = await real_exec(*args, **kwargs)
            # Gate the signal on the child's pid marker actually existing.
            # The stub forks `sleep` and runs `echo $! > marker`
            # asynchronously, so the marker can lag subprocess creation under
            # load; signalling on bare creation would race the test's marker
            # read -> FileNotFoundError. Worse, that crash leaks the sleep:
            # asyncio.run teardown cancels the internal launch task directly
            # (a path production never takes), so _recover_cancelled_spawn
            # gets nothing to reap. Polling here keeps the 0.3s Window-A pause
            # intact while guaranteeing the marker is present.
            for _ in range(500):
                if marker.exists() and marker.read_text().strip():
                    break
                await asyncio.sleep(0.005)
            spawn_started.set()
            await asyncio.sleep(0.3)
            return proc

        monkeypatch.setattr(asyncio, "create_subprocess_exec", slow_spawn)

        daemon = AngelusDaemon(tmp_path)
        try:
            drain = daemon.pipe_drains["daily"]
            pipe = daemon.lodging.pipes["daily"]
            task = asyncio.create_task(
                drain._render_llm_body(pipe, {"open_incidents": []})
            )
            await asyncio.wait_for(spawn_started.wait(), timeout=5.0)
            gc_pid = int(marker.read_text().strip())
            assert _alive(gc_pid)
            # Cancel while the spawn coroutine is still suspended inside
            # create_subprocess_exec: `process` is unbound in the drain.
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=10.0)
            return gc_pid
        finally:
            daemon.connection.close()

    gc_pid = asyncio.run(driver())
    for _ in range(200):
        if not _alive(gc_pid):
            break
        time.sleep(0.01)
    else:
        os.kill(gc_pid, 9)
        raise AssertionError(
            f"horizon grandchild {gc_pid} survived a mid-spawn (Window A) "
            "cancel -- the shielded handle was not recovered and reaped"
        )


def test_full_daemon_shutdown_reaps_python_triager_subprocess(
    tmp_path, monkeypatch,
) -> None:
    """run_python_triager spawns a sys.executable subprocess for python
    triagers, awaited from _triage_loop which is one of daemon.tasks
    cancelled in daemon.run()'s finally. A forking triager (one that
    exec's another tool or shells out) must not orphan its grandchild
    on shutdown, same property the source-fire and digest-LLM tests
    pin.

    Discrimination (recorded in FELL_NOTES): removing the CancelledError
    arm OR `start_new_session=True` from run_python_triager leaves the
    forking grandchild alive after shutdown -> this test fails.
    """
    marker = tmp_path / "tri_child.pid"
    _base_lodging(tmp_path)
    # The triager handler is a python script that immediately execvp's a
    # shell with the forking-hang pattern. start_new_session=True on
    # create_subprocess_exec puts the whole tree in a fresh process group;
    # SIGKILL to the python process leader only does NOT reap the sleep
    # grandchild -- only the process-group kill via _kill_and_reap does.
    handler_dir = tmp_path / "triagers" / "handlers"
    handler_dir.mkdir(parents=True)
    handler_path = handler_dir / "fork_hang.py"
    handler_path.write_text(
        "import os, sys\n"
        "sys.stdin.read()  # consume the daemon's payload write\n"
        f"os.execvp('sh', ['sh', '-c', 'sleep 30 & echo $! > {marker}; wait'])\n",
        encoding="utf-8",
    )
    (tmp_path / "triagers").mkdir(exist_ok=True)
    (tmp_path / "triagers" / "watch.yaml").write_text(
        "inputs:\n  source: scheduled/watch\n"
        "handler:\n  kind: python\n  path: triagers/handlers/fork_hang.py\n"
        "timeout_seconds: 60\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")

    async def driver() -> int:
        daemon = AngelusDaemon(tmp_path)
        task = asyncio.create_task(daemon.run())
        try:
            # Wait for the source to fire, the triager to spawn, and the
            # forking handler to write its grandchild pid.
            for _ in range(400):
                if marker.exists() and marker.read_text().strip():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("triager subprocess never launched its grandchild")
            gc_pid = int(marker.read_text().strip())
            assert _alive(gc_pid)

            started = time.monotonic()
            daemon.request_stop()
            await asyncio.wait_for(task, timeout=15.0)
            elapsed = time.monotonic() - started
            assert elapsed < 8.0, f"shutdown took {elapsed:.1f}s (hang)"
            return gc_pid
        finally:
            if not task.done():
                daemon.request_stop()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    gc_pid = asyncio.run(driver())
    for _ in range(200):
        if not _alive(gc_pid):
            break
        time.sleep(0.01)
    else:
        os.kill(gc_pid, 9)
        raise AssertionError(
            f"triager grandchild {gc_pid} survived shutdown -- "
            "cancelled python triager subprocess was orphaned"
        )
    # observation_triage 'processing' rows must NOT survive a
    # shutdown-cancel. mark_triage_processing ran before the task was
    # created (daemon.py _triage_loop); without _triage_under_semaphore's
    # CancelledError arm calling clear_triage_processing, the row would
    # stay 'processing' forever, recover_writing_rows doesn't touch
    # observation_triage, and ready_observations_for excludes
    # 'processing' rows -- so the observation would be permanently stuck
    # after a daemon restart. (Discrimination axis: remove the
    # CancelledError arm in _triage_under_semaphore -> this assertion
    # fails because the row stays at 'processing'.)
    import sqlite3
    conn = sqlite3.connect(str(tmp_path / "state" / "angelus.sqlite3"))
    try:
        conn.row_factory = sqlite3.Row
        rows = list(
            conn.execute(
                "SELECT observation_id, triager_name, status "
                "FROM observation_triage WHERE status = 'processing'"
            )
        )
    finally:
        conn.close()
    assert rows == [], (
        f"observation_triage left {len(rows)} 'processing' row(s) after "
        f"shutdown-cancel; first: {dict(rows[0]) if rows else None}"
    )


# --- Risk 4: mute consultation vs hot-reloaded pipes ---------------------


def test_drain_snapshot_stays_internally_consistent_during_slow_reload(
    tmp_path,
) -> None:
    """While apply_lodging is parked at `await _cancel_pipe_loop` (pipe
    'extra' being removed), a fresh drain_once on the unchanged 'now'
    pipe must take a snapshot where pipe.channels is a subset of the
    channels dict (no KeyError possible), even if channels/known_pipes
    are a newer generation than pipe. Proves the mixed-generation
    snapshot is still internally consistent.

    Discrimination (recorded in FELL_NOTES): asserting a stricter
    'same-generation' invariant instead fails, because the snapshot is
    legitimately mixed-generation -- the real invariant is the subset
    relation, which this asserts and which holds.
    """
    _base_lodging(tmp_path)
    _add_immediate_pipe(tmp_path, "extra")
    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)

    # Queue a finding for 'now' and mute it: the mute decision must stay
    # coherent (keyed by dedup_key, not by the pipe snapshot).
    daemon.catalog.write_finding(
        None,
        {"source": "s", "type": "down", "entity": "e",
         "dedup_key": "s:down:e", "target_pipes": ["now"]},
        set(daemon.lodging.pipes),
    )
    daemon.catalog.add_mute("s:down:e", 3600, "integration")

    captured: list[tuple[list[str], list[str]]] = []
    real_cancel = daemon._cancel_pipe_loop

    async def slow_cancel(name: str) -> None:
        await asyncio.sleep(0.2)
        await real_cancel(name)

    async def spy_drain(self: pipe_runner.PipeDrain) -> None:
        async with self.lock:
            captured.append(
                (list(self.pipe.channels), sorted(self.channels))
            )
            # Exercise the real mute path against this snapshot.
            await pipe_runner.PipeDrain._drain_immediate(
                self, self.pipe, self.channels, self.known_pipes
            )

    async def driver() -> None:
        daemon._cancel_pipe_loop = slow_cancel  # type: ignore[method-assign]
        daemon.scheduler.start(paused=True)
        try:
            daemon._spawn_pipe_loop("now")
            daemon._spawn_pipe_loop("extra")
            # First reload: add channel 'log' (re-points drain.channels to
            # a NEWER generation than the unchanged 'now' pipe object).
            (tmp_path / "channels" / "log.yaml").write_text(
                "kind: push\ncommand: 'true'\n", encoding="utf-8"
            )
            reloader.event_queue.put(str(tmp_path / "channels" / "log.yaml"))
            await reloader.process_pending_events()
            # Second reload: remove immediate pipe 'extra' -> parks
            # apply_lodging at await _cancel_pipe_loop('extra').
            (tmp_path / "pipes" / "extra.yaml").unlink()
            reloader.event_queue.put(str(tmp_path / "pipes" / "extra.yaml"))
            apply_task = asyncio.create_task(
                reloader.process_pending_events()
            )
            await asyncio.sleep(0.05)
            with patch.object(pipe_runner.PipeDrain, "drain_once", spy_drain):
                await daemon.pipe_drains["now"].drain_once()
            await apply_task
            captured.append(
                ("dispatch", [
                    r["status"]
                    for r in daemon.connection.execute(
                        "SELECT status FROM dispatches WHERE pipe='now'"
                    )
                ])
            )
        finally:
            daemon.scheduler.shutdown(wait=False)
            daemon.connection.close()

    asyncio.run(driver())

    assert captured, "spy drain never ran during the reload window"
    pipe_channels, channels_dict = captured[0]
    # The real invariant: a pipe's channels are always a subset of the
    # channels dict in the snapshot, so _drain_immediate's
    # channels[channel_name] can never KeyError -- even mixed-generation.
    assert set(pipe_channels) <= set(channels_dict), (
        f"torn snapshot: pipe.channels={pipe_channels} not subset of "
        f"channels={channels_dict}"
    )
    # Mute coherence: the finding was muted by dedup_key regardless of
    # reload generation -> a 'muted' dispatch, no real send.
    dispatch_statuses = next(c[1] for c in captured if c[0] == "dispatch")
    assert dispatch_statuses == ["muted"], dispatch_statuses


# --- Risk 5: a dependency_unhealthy finding is itself muteable -----------


def test_muted_unhealthy_dep_is_silent_on_now_but_visible_in_health(
    tmp_path,
) -> None:
    """Risk 5 is a PRODUCT decision (see INTEGRATION_FELL_RISK5.md), not a
    code bug -- mute deliberately suppresses the now-alert. The one
    invariant that must hold so the suppression is not TOTALLY silent:
    the health op still reports the dependency as unhealthy.

    Discrimination (recorded in FELL_NOTES): if all_dep_health() were
    mute-filtered, the health op would hide iotaschool and this fails.
    """
    _base_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    reloader = None  # not needed

    async def driver() -> None:
        try:
            # The README activating example: iotaschool down.
            await daemon._op_dep_record(
                {"name": "iotaschool", "status": "unhealthy",
                 "detail": "exit 7: connection refused"}
            )
            dedup_key = "internal/dep:dependency_unhealthy:iotaschool"
            daemon.catalog.add_mute(dedup_key, 86400, "flapping, acked")

            # Drain `now`: the muted dep-unhealthy finding is silenced
            # (recorded as a 'muted' dispatch, no push).
            await daemon.pipe_drains["now"].drain_once()
            disp = [
                r["status"]
                for r in daemon.connection.execute(
                    "SELECT status FROM dispatches WHERE pipe='now'"
                )
            ]
            assert disp == ["muted"], disp

            # ...but the dependency is still VISIBLY unhealthy via health.
            health = await daemon._op_health({})
            deps = {d["dependency_name"]: d for d in health["deps"]}
            assert deps["iotaschool"]["status"] == "unhealthy"
            assert "connection refused" in deps["iotaschool"]["detail"]
        finally:
            daemon.connection.close()

    asyncio.run(driver())
    assert reloader is None


# --- M2 slice 1: channel rename rehearsal --------------------------------


def test_channel_rename_mid_pending_finding_is_rejected_at_cross_ref(
    tmp_path,
) -> None:
    """Renaming channels/push.yaml -> channels/telegram.yaml while a
    finding is still pending in pipe_queues for the `now` pipe (which
    references `push`) must NOT silently lose the finding and must NOT
    drop the `push` channel from lodging. The push.yaml deletion event
    is rejected at cross-ref time, an internal/lodging cross_ref_broken
    finding lands for the deletion side of the rename, and the original
    finding stays pending. A reversion (rename back to push.yaml)
    settles the system to a clean steady state with no lingering
    rejection.

    Discrimination (recorded in FELL_NOTES): if the `if errors:
    self._reject_cross_ref(...) return` arm in
    LodgingReloader._apply_removal were removed, the push removal would
    apply unchecked and lodging.channels would lose `push`. The
    subsequent telegram-add then sees a dangling pipes/now -> push
    reference and fires its OWN cross_ref_broken finding, but for entity
    `channels/telegram.yaml` -- so the presence assertion
    `assert cross_ref` still HOLDS (the list is non-empty). The
    assertions that DO fire under that inversion are the entity-match
    `any(r["entity"] == "channels/push.yaml" for r in cross_ref)` and
    the lodging-survival `"push" in daemon.lodging.channels`.
    """
    _base_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)

    push_path = tmp_path / "channels" / "push.yaml"
    telegram_path = tmp_path / "channels" / "telegram.yaml"

    async def driver() -> None:
        try:
            finding_id = daemon.catalog.write_finding(
                None,
                {"source": "s", "type": "down", "entity": "e",
                 "target_pipes": ["now"]},
                set(daemon.lodging.pipes),
            )
            pending = [
                (r["pipe"], r["status"])
                for r in daemon.connection.execute(
                    "SELECT pipe, status FROM pipe_queues "
                    "WHERE finding_id = ?",
                    (finding_id,),
                )
            ]
            assert pending == [("now", "pending")], pending

            # Mid-flight rename on disk. watchdog produces a delete event
            # for the old path AND a create event for the new path on a
            # rename; drive both into the queue in that order so the
            # delete is processed under the still-broken cross-ref.
            os.rename(push_path, telegram_path)
            reloader.event_queue.put(str(push_path))
            reloader.event_queue.put(str(telegram_path))
            await reloader.process_pending_events()

            # The push-removal event broke the cross-ref (pipes/now
            # still references `push`) and was rejected with an
            # internal/lodging cross_ref_broken finding for the
            # push.yaml path.
            cross_ref = [
                dict(r) for r in daemon.connection.execute(
                    "SELECT type, entity FROM findings "
                    "WHERE source = 'internal/lodging' "
                    "AND type = 'cross_ref_broken'"
                )
            ]
            assert cross_ref, "expected a cross_ref_broken finding"
            assert any(
                r["entity"] == "channels/push.yaml" for r in cross_ref
            ), cross_ref

            # `push` survives in lodging (the whole point of the
            # rejection); the unrelated telegram-add applied in its
            # own right.
            assert "push" in daemon.lodging.channels
            assert "telegram" in daemon.lodging.channels
            assert daemon.lodging.pipes["now"].channels == ["push"]

            # The original pending finding is NOT lost.
            rows = [
                (r["pipe"], r["status"])
                for r in daemon.connection.execute(
                    "SELECT pipe, status FROM pipe_queues "
                    "WHERE finding_id = ?",
                    (finding_id,),
                )
            ]
            assert rows == [("now", "pending")], rows

            # Reversion: rename telegram.yaml -> push.yaml. The reloader
            # picks up both events, the push-add no-ops against the
            # still-live `push` entry, telegram-remove applies cleanly
            # (nothing references telegram), and the rejection clears.
            os.rename(telegram_path, push_path)
            reloader.event_queue.put(str(push_path))
            reloader.event_queue.put(str(telegram_path))
            await reloader.process_pending_events()
            assert "push" in daemon.lodging.channels
            assert "telegram" not in daemon.lodging.channels
            assert reloader.rejected == {}, reloader.rejected
        finally:
            daemon.connection.close()

    asyncio.run(driver())


# --- M2 slice 2: cross-ref rehearsal at hot-reload -----------------------


@pytest.mark.parametrize(
    "direction",
    ["pipe_to_channel", "triager_to_source", "pipe_to_overflow"],
)
def test_cross_ref_broken_at_hot_reload_emits_finding_and_keeps_state(
    tmp_path, direction,
) -> None:
    """Sweep every cross-ref direction validate_cross_refs guards
    (pipe -> channel, triager -> source, pipe -> overflow-pipe) and
    assert that introducing a dangling reference via a YAML edit at
    hot-reload time emits an internal/lodging cross_ref_broken finding
    for the edited file and leaves live lodging unchanged.

    Discrimination (recorded in FELL_NOTES): stripping
    validate_cross_refs (or its caller in
    LodgingReloader._handle_path's edit arm) lets the broken edit
    apply. The per-direction snapshot-stable assertion below names
    which assertion fires under that inversion:
      * pipe_to_channel    -> `pipes["now"].channels == ["push"]`
      * triager_to_source  -> `triagers["watch"].source_ref ==
                               "scheduled/watch"`
      * pipe_to_overflow   -> `pipes["now"].rate_limit == {}`
    The cross_ref_broken-finding presence assertion fails under the
    same inversion (no rejection arm => no finding written).
    """
    _base_lodging(tmp_path)
    if direction == "triager_to_source":
        # parse_triager validates the handler path on disk: a missing
        # handler raises ValueError and would route through load_failed
        # rather than cross_ref_broken, so the handler must exist for
        # the edit to reach validate_cross_refs.
        (tmp_path / "triagers" / "handlers").mkdir(parents=True)
        (tmp_path / "triagers" / "handlers" / "noop.py").write_text(
            "import json\n"
            "print(json.dumps({'findings': [], 'new_state': {}}))\n",
            encoding="utf-8",
        )
        (tmp_path / "triagers" / "watch.yaml").write_text(
            "inputs:\n  source: scheduled/watch\n"
            "handler:\n  kind: python\n"
            "  path: triagers/handlers/noop.py\n",
            encoding="utf-8",
        )

    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)

    if direction == "pipe_to_channel":
        changed_rel = "pipes/now.yaml"
        snapshot_before = list(daemon.lodging.pipes["now"].channels)
        new_yaml = (
            "cadence: immediate\nchannels: [push, missing_channel]\n"
            "render:\n  kind: dumb-alert\n"
            "  template: '{type}:{entity}:{body}'\n"
        )
    elif direction == "triager_to_source":
        changed_rel = "triagers/watch.yaml"
        snapshot_before = daemon.lodging.triagers["watch"].source_ref
        new_yaml = (
            "inputs:\n  source: scheduled/missing_source\n"
            "handler:\n  kind: python\n"
            "  path: triagers/handlers/noop.py\n"
        )
    else:  # pipe_to_overflow
        changed_rel = "pipes/now.yaml"
        snapshot_before = dict(daemon.lodging.pipes["now"].rate_limit)
        new_yaml = (
            "cadence: immediate\nchannels: [push]\n"
            "rate_limit:\n  overflow: missing_pipe\n"
            "render:\n  kind: dumb-alert\n"
            "  template: '{type}:{entity}:{body}'\n"
        )

    changed_path = tmp_path / changed_rel

    async def driver() -> None:
        try:
            changed_path.write_text(new_yaml, encoding="utf-8")
            reloader.event_queue.put(str(changed_path))
            await reloader.process_pending_events()

            cross_ref = [
                dict(r) for r in daemon.connection.execute(
                    "SELECT type, entity FROM findings "
                    "WHERE source = 'internal/lodging' "
                    "AND type = 'cross_ref_broken'"
                )
            ]
            assert cross_ref, "expected a cross_ref_broken finding"
            assert any(r["entity"] == changed_rel for r in cross_ref), (
                f"no cross_ref_broken for {changed_rel}; got {cross_ref}"
            )

            # The rejection left lodging unchanged on the field the edit
            # targeted -- so a downstream drain / triage step that reads
            # the field cannot see a dangling reference.
            if direction == "pipe_to_channel":
                assert daemon.lodging.pipes["now"].channels == snapshot_before
            elif direction == "triager_to_source":
                assert (
                    daemon.lodging.triagers["watch"].source_ref
                    == snapshot_before
                )
            else:
                assert daemon.lodging.pipes["now"].rate_limit == snapshot_before
        finally:
            daemon.connection.close()

    asyncio.run(driver())


# --- M2 slice 4: rate-limit overflow end-to-end --------------------------


def test_rate_limit_overflow_routes_excess_to_daily_and_renders_suppressed_callout(
    tmp_path, monkeypatch,
) -> None:
    """End-to-end rate-limit overflow: cap push at 2/hr on `now`, drive 4
    findings through the source/triager write surface, drain `now` then
    `daily`. Four independently discriminating axes are pinned -- one for
    each property the rate-limit/overflow protocol exists to provide:

      A. send-rail: exactly 2 findings make it through the now-channel cap.
      B. suppress-rail: exactly 2 findings are re-routed via
         suppress_pipe_item_to into the daily pipe's queue (the existing
         overflow + suppressed_findings_since protocol -- Section 5b Q1
         dropped the deferred_alerts table noun).
      C. digest-callout: the daily digest's preamble renders the
         "N alert(s) suppressed by rate limit" line over the overflow
         findings.
      D. severity-preservation: the suppressed `high` findings remain
         tagged `high` in the findings table AND surface as `high` in the
         digest preamble; the overflow protocol does NOT relabel them
         informational.

    Discrimination (recorded in FELL_NOTES):

      * Invert _over_rate_limit (force False) -- the cap never triggers,
        all 4 findings dispatch as sent. Axis A fails (3 not 5 in
        push_sends), axis B fails (no suppressed rows), axis C fails (no
        "alert(s) suppressed by rate limit" substring in the digest body).
      * Invert suppress_pipe_item_to to a no-op (skip the call, just
        `continue`) -- the cap triggers but findings 3+4 stay pending on
        now's queue and never reach daily's queue. Axis B fails (now-pq
        rows still pending, daily-pq rows missing); axis C fails (daily's
        suppressed_findings_since returns empty so the preamble renders
        empty).
      * Strip severity off the suppressed findings (e.g. cast to
        'informational' inside suppressed_findings_since) -- axis D fails
        on the digest substring and on the findings-row check.
    """
    # --- lodging: source -> triager -> now(rate_limit per_channel) -> daily
    (tmp_path / "sources" / "scheduled").mkdir(parents=True)
    (tmp_path / "sources" / "scheduled" / "canary.yaml").write_text(
        # 1h cadence so APScheduler does not auto-fire in the test window;
        # the test drives the source/triager path explicitly by writing
        # one observation and calling _run_triager once.
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (tmp_path / "triagers" / "handlers").mkdir(parents=True)
    (tmp_path / "triagers" / "handlers" / "emit_n.py").write_text(
        # Real triager subprocess: reads the observation, emits N
        # high-severity findings targeted at `now`. Run via
        # run_python_triager from _run_triager -- the same write surface
        # the daemon uses in production. Findings go through
        # catalog.write_finding (NOT a direct test write), so dedup_key,
        # incident-upsert, and pipe_queues insertion happen via the
        # production code path.
        "import json, sys\n"
        "data = json.loads(sys.stdin.read())\n"
        "obs = data['observation']\n"
        "n = int(obs.get('n', 0))\n"
        "findings = []\n"
        "for i in range(n):\n"
        "    findings.append({\n"
        "        'source': 'scheduled/canary',\n"
        "        'type': 'down',\n"
        "        'entity': f'e{i}',\n"
        "        'dedup_key': f'scheduled/canary:down:e{i}',\n"
        "        'severity': 'high',\n"
        "        'target_pipes': ['now'],\n"
        "        'body': {'text': f'alert {i}'},\n"
        "    })\n"
        "print(json.dumps({'findings': findings, 'new_state': {}}))\n",
        encoding="utf-8",
    )
    (tmp_path / "triagers" / "canary.yaml").write_text(
        "inputs:\n  source: scheduled/canary\n"
        "handler:\n  kind: python\n"
        "  path: triagers/handlers/emit_n.py\n",
        encoding="utf-8",
    )
    (tmp_path / "pipes").mkdir()
    (tmp_path / "pipes" / "now.yaml").write_text(
        # Cap per_channel at 2/hr with overflow into daily. This is the
        # rescoped Section 5b Q1 shape: lodging carries overflow:<pipe>,
        # _drain_immediate routes excess through suppress_pipe_item_to,
        # the digest reads the routed rows via suppressed_findings_since.
        "cadence: immediate\nchannels: [push]\n"
        "rate_limit:\n  per_channel: 2/hr\n  overflow: daily\n"
        "render:\n  kind: dumb-alert\n"
        "  template: '{severity}/{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (tmp_path / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [push]\n"
        "render:\n"
        "  preamble:\n"
        "    - kind: structured\n      template: rate-limit-callout\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n"
        "    inputs:\n      - findings_since_last_drain\n"
        "      - suppressed_findings\n",
        encoding="utf-8",
    )
    (tmp_path / "channels").mkdir()
    (tmp_path / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )
    (tmp_path / "render-templates").mkdir()
    (tmp_path / "render-templates" / "rate-limit-callout.j2").write_text(
        # "N alert(s) suppressed by rate limit" line plus per-finding
        # severity+entity rows. The severity rendering is what pins axis
        # D against an inversion that downgrades suppressed findings: if
        # severity were dropped or rewritten to 'informational', the
        # substring "[high]" would not appear in the digest message.
        "{% if suppressed_findings %}"
        "{{ suppressed_findings | length }} alert(s) suppressed by rate limit:\n"
        "{% for finding in suppressed_findings %}"
        "  - [{{ finding.severity }}] {{ finding.entity }}\n"
        "{% endfor %}"
        "{% endif %}",
        encoding="utf-8",
    )

    daemon = AngelusDaemon(tmp_path)

    # send_push is mocked to record every dispatched message so we can
    # discriminate the send rail (axis A) and the digest callout (axis C)
    # by message content. The push channel command is 'true' but we never
    # reach the real subprocess because of this monkeypatch -- which also
    # keeps the test off the file-system DRY_RUN log code path.
    push_sends: list[str] = []

    async def fake_push(_channel, message: str, _workdir: Path) -> None:
        push_sends.append(message)

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)

    # The digest LLM body is unrelated to the rate-limit axes; mock it to
    # avoid spawning a `horizon` subprocess in the test. The mocked body
    # captures the `structured` inputs so axis C can also be asserted
    # against the structured suppressed_findings list seen by the LLM.
    captured_llm_inputs: list[dict] = []

    async def fake_llm(_self, _pipe, structured):
        captured_llm_inputs.append(structured)
        return "body text.", None

    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    state: dict[str, object] = {}

    async def driver() -> None:
        try:
            # Real source-path write: observation lands as a 'ready' row
            # with the production schema. Triage will pick it up next.
            obs_id = daemon.catalog.write_observation(
                "scheduled/canary",
                {"n": 4},
                {"source": "scheduled/canary", "check": "shell"},
            )
            triager = daemon.lodging.triagers["canary"]
            rows = daemon.catalog.ready_observations_for(
                triager.name, triager.source_ref
            )
            assert [int(r["id"]) for r in rows] == [obs_id], rows
            # Mark processing the same way _triage_loop's body does, so
            # _run_triager sees the production preconditions.
            daemon.catalog.mark_triage_processing(rows[0]["id"], triager.name)
            await daemon._run_triager(rows[0], triager.name)

            findings = list(
                daemon.connection.execute(
                    "SELECT id, severity, entity FROM findings "
                    "WHERE source='scheduled/canary' ORDER BY id"
                )
            )
            assert [f["entity"] for f in findings] == ["e0", "e1", "e2", "e3"]
            assert all(f["severity"] == "high" for f in findings)
            state["finding_ids"] = [int(f["id"]) for f in findings]

            # Drain `now`: cap is 2/hr per channel, so finding 1+2 send,
            # finding 3+4 cross the cap and get suppressed into daily.
            await daemon.pipe_drains["now"].drain_once()

            # Snapshot the state the digest will read BEFORE draining it,
            # so a regression on suppress_pipe_item_to is named explicitly
            # by the snapshot assertions rather than only being implied by
            # the rendered preamble.
            state["now_queue"] = [
                (r["finding_id"], r["status"])
                for r in daemon.connection.execute(
                    "SELECT finding_id, status FROM pipe_queues "
                    "WHERE pipe = 'now' ORDER BY finding_id"
                )
            ]
            state["daily_queue"] = [
                (r["finding_id"], r["status"])
                for r in daemon.connection.execute(
                    "SELECT finding_id, status FROM pipe_queues "
                    "WHERE pipe = 'daily' ORDER BY finding_id"
                )
            ]
            state["now_dispatches"] = [
                (r["channel"], r["status"])
                for r in daemon.connection.execute(
                    "SELECT channel, status FROM dispatches "
                    "WHERE pipe = 'now' ORDER BY id"
                )
            ]
            state["suppressed_findings"] = [
                {"entity": s["entity"], "severity": s["severity"]}
                for s in daemon.catalog.suppressed_findings_since(None)
            ]

            await daemon.pipe_drains["daily"].drain_once()
            state["daily_message"] = push_sends[-1]
        finally:
            daemon.connection.close()

    asyncio.run(driver())

    # --- axis A: send rail -- exactly 2 findings made it through `now` ---
    # `true` succeeded on both, recorded as 'sent' dispatches against the
    # `push` channel. push_sends carries 3 messages total (2 now + 1 daily
    # digest).
    now_sent = [d for d in state["now_dispatches"] if d == ("push", "sent")]
    assert len(now_sent) == 2, state["now_dispatches"]
    # axis A is also pinned by the count of immediate dispatches in
    # push_sends (the daily digest message is the 3rd).
    assert len(push_sends) == 3, push_sends

    # --- axis B: suppress rail -- 2 finding rows re-routed to daily ----
    # Findings 1+2 dispatched on `now`; findings 3+4 transitioned to
    # 'suppressed' on `now` AND have a 'pending' row on `daily` written
    # by suppress_pipe_item_to. (The first two findings have NO `daily`
    # row -- target_pipes=['now'] only -- so the daily pending rows for
    # finding 3+4 EXIST exclusively because of the suppress call.)
    fid1, fid2, fid3, fid4 = state["finding_ids"]  # type: ignore[misc]
    assert state["now_queue"] == [
        (fid1, "dispatched"),
        (fid2, "dispatched"),
        (fid3, "suppressed"),
        (fid4, "suppressed"),
    ]
    assert state["daily_queue"] == [
        # Pre-drain snapshot: the two suppressed rows are pending on daily;
        # after the daily drain they would be 'dispatched'. The snapshot
        # is taken BEFORE the daily drain so the suppress-rail evidence
        # is unambiguous (axis B is about routing, not digest dispatch).
        (fid3, "pending"),
        (fid4, "pending"),
    ]

    # --- axis C: digest callout -- the preamble names how many were
    # suppressed and lists the suppressed entities. The number is the
    # `length` Jinja filter over suppressed_findings; if axis B fails the
    # list is empty and "alert(s) suppressed by rate limit" disappears.
    daily_msg = str(state["daily_message"])
    assert "2 alert(s) suppressed by rate limit" in daily_msg, daily_msg
    assert "e2" in daily_msg and "e3" in daily_msg, daily_msg
    # The structured input the LLM receives carries both suppressed rows
    # too -- axis C also discriminates at the structured-input boundary.
    assert captured_llm_inputs, "digest _render_llm_body was not invoked"
    assert [
        item["entity"] for item in captured_llm_inputs[0]["suppressed_findings"]
    ] == ["e2", "e3"]

    # --- axis D: severity preservation -- the rows the digest reads keep
    # their original 'high' severity. The protocol routes through
    # pipe_queues + suppressed_findings_since, NOT through any rewrite.
    # Inverting the suppressed_findings_since join to cast severity to
    # 'informational' would fail both lines below.
    assert state["suppressed_findings"] == [
        {"entity": "e2", "severity": "high"},
        {"entity": "e3", "severity": "high"},
    ]
    assert "[high] e2" in daily_msg and "[high] e3" in daily_msg, daily_msg
    assert "informational" not in daily_msg, daily_msg


# --- M2 slice 5: SIGKILL-mid-drain recovery ------------------------------


def _spawn_daemon_subprocess(root: Path) -> subprocess.Popen:
    """Spawn the angelus daemon as an OS subprocess so it can be
    SIGKILLed without taking the test runner down with it (this is the
    same shape test_belfry.py uses for its independence test). Risk-3
    hardening: start_new_session=True puts the daemon in its own
    process group, so an emergency kill from the test's finally clause
    reaps the whole tree."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT) + (
        os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env else ""
    )
    env["ANGELUS_DRY_RUN"] = "1"
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from angelus.daemon import main\n"
            "from pathlib import Path\n"
            "import sys\n"
            "main(Path(sys.argv[1]))\n",
            str(root),
        ],
        env=env,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _read_daemon_pid(root: Path, timeout: float) -> int:
    pid_path = root / "state" / "angelus.pid"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return int(pid_path.read_text(encoding="utf-8").strip())
        except (FileNotFoundError, ValueError):
            time.sleep(0.05)
    raise AssertionError(f"daemon PID file never appeared at {pid_path}")


def _wait_for_row(
    root: Path, sql: str, params: tuple = (), timeout: float = 15.0
) -> sqlite3.Row | None:
    """Poll the read-only sqlite until `sql` returns a row or timeout. The
    daemon writes to the same db on its own connection; the test reads
    in URI read-only mode so the writer is not contended."""
    db = root / "state" / "angelus.sqlite3"
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            try:
                conn.row_factory = sqlite3.Row
                row = conn.execute(sql, params).fetchone()
            finally:
                conn.close()
            if row is not None:
                return row
        except sqlite3.Error as exc:
            last_err = exc
        time.sleep(0.05)
    if last_err is not None:
        raise AssertionError(f"poll never read a row: {last_err}") from last_err
    return None


def _sigkill_and_reap(proc: subprocess.Popen, pid: int) -> None:
    """SIGKILL the leader and confirm the kernel has reaped the pid.
    SIGKILL is uncatchable; the daemon's finally clause never runs, so
    the PID file, in-flight 'writing' rows, and any 'processing'
    observation_triage rows survive the kill as orphans. Bounded wait
    so a stuck process surfaces in the test rather than hanging CI."""
    os.kill(pid, signal.SIGKILL)
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(
            f"daemon PID {pid} did not exit within 5s of SIGKILL"
        ) from exc
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and _alive(pid):
        time.sleep(0.02)
    if _alive(pid):
        raise AssertionError(f"daemon PID {pid} still alive after SIGKILL")


def _sigterm_and_wait(proc: subprocess.Popen, timeout: float = 15.0) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        # Fallback: emergency reap of the process group via SIGKILL on
        # the leader (start_new_session put the daemon in its own group).
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=5.0)
        raise AssertionError(
            f"daemon did not exit within {timeout}s of SIGTERM"
        ) from exc


def _write_slice5_lodging(root: Path) -> None:
    """Real source/triager/pipe/channel surface for the SIGKILL test. The
    source fires every 1s so the daemon produces real observations,
    findings, and dispatches as it runs. The triager is fast (no sleep)
    so a fresh daemon's drain completes promptly; the SIGKILL on the
    first daemon catches whatever state exists, and pre-seeded rows in
    phase B make the three recovery axes deterministic regardless of
    timing."""
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "canary.yaml").write_text(
        "cadence: 1s\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "triagers" / "handlers").mkdir(parents=True)
    (root / "triagers" / "handlers" / "emit_one.py").write_text(
        "import json, sys\n"
        "data = json.loads(sys.stdin.read())\n"
        "prior = data.get('prior_state') or {}\n"
        "n = int(prior.get('n', 0)) + 1\n"
        "findings = [{\n"
        "    'source': 'scheduled/canary',\n"
        "    'type': 'down',\n"
        "    'entity': f'e{n}',\n"
        "    'dedup_key': f'scheduled/canary:down:e{n}',\n"
        "    'severity': 'high',\n"
        "    'target_pipes': ['now'],\n"
        "    'body': {'text': f'alert {n}'},\n"
        "}]\n"
        "print(json.dumps({'findings': findings, 'new_state': {'n': n}}))\n",
        encoding="utf-8",
    )
    (root / "triagers" / "canary.yaml").write_text(
        "inputs:\n  source: scheduled/canary\n"
        "handler:\n  kind: python\n"
        "  path: triagers/handlers/emit_one.py\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n"
        "  template: '{type}:{entity}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )


def test_sigkill_mid_drain_recovery_three_axis(tmp_path) -> None:
    """SIGKILL the daemon while it is producing observations / findings /
    dispatches, then spawn a fresh daemon against the same state dir.
    After the fresh daemon settles, three independently-discriminating
    properties must hold (per the round-5 pattern in FELL_NOTES):

      Axis 1 (writing-row recovery): findings / observations that were
        mid-write at the SIGKILL transition from 'writing' to 'ready'
        (body file landed) or 'failed' (body missing) via
        recover_writing_rows. Asserted by: zero rows at status='writing'
        after the fresh daemon's startup.

      Axis 2 (triage-orphan recovery): observation_triage rows left at
        status='processing' from the prior daemon's hard exit are
        cleared by recover_triage_processing_rows at startup. Without
        this clear, ready_observations_for would exclude the
        observation forever (round-5 orphan class on the hard-exit
        axis). Asserted by: zero rows at status='processing' after the
        fresh daemon's startup.

      Axis 3 (per-row dispatch commit / no duplicates): record_dispatch
        commits the dispatches INSERT and the pipe_queues UPDATE in one
        transaction, so a SIGKILL between cannot leave the queue at
        'pending' while a 'sent' dispatch row already exists. Asserted
        by: each finding has at most one 'sent' dispatch.

    Discrimination (verified locally; each inversion reverted):
      * Make recover_writing_rows a no-op -> the pre-seeded 'writing'
        finding row never transitions and axis 1's assertion fires.
      * Make recover_triage_processing_rows a no-op -> the pre-seeded
        orphan 'processing' row remains and axis 2's assertion fires.
      * Replace record_dispatch's pipe_queues UPDATE with a no-op ->
        the dispatched-and-sent-but-still-pending state from phase A
        gets re-dispatched by phase C's drain and axis 3 sees two
        'sent' dispatches for the same finding.
    """
    _write_slice5_lodging(tmp_path)

    # ----- phase A: real daemon, real work, real SIGKILL -----
    proc_a = _spawn_daemon_subprocess(tmp_path)
    try:
        pid_a = _read_daemon_pid(tmp_path, timeout=15.0)
        assert _alive(pid_a)
        # Wait for the daemon to dispatch at least one finding -- proves
        # the source/triager/pipe stack is running before we SIGKILL it.
        _wait_for_row(
            tmp_path,
            "SELECT id FROM dispatches WHERE status='sent' LIMIT 1",
            timeout=20.0,
        )
        _sigkill_and_reap(proc_a, pid_a)
    finally:
        if proc_a.poll() is None:
            try:
                os.killpg(proc_a.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc_a.wait(timeout=5.0)

    # Phase A artifacts that the test relies on:
    state_db = tmp_path / "state" / "angelus.sqlite3"
    assert state_db.exists()

    # ----- phase B: pre-seed deterministic axis-1, axis-2, axis-3 cases --
    # Phase A's SIGKILL may or may not leave a 'writing' row, an orphan
    # 'processing' row, and a stale 'pending' queue depending on what
    # the kernel scheduled between writes. We insert deterministic
    # rows now so the three axes are not flakily-discriminating.
    catalog_root = tmp_path
    connection = init_db(state_db)
    catalog = Catalog(connection, catalog_root)
    try:
        # Axis 1 (writing-row recovery): a writing finding with body
        # file present is the recoverable variant of the partial-write
        # race recover_writing_rows is built for (tests/test_slice1.py:84
        # is the unit-level shape).
        body_dir = catalog_root / "findings" / "2026-05-20" / "recoverable"
        body_dir.mkdir(parents=True)
        body_path = body_dir / "body.json"
        body_path.write_text(json.dumps({"text": "recoverable"}), encoding="utf-8")
        cursor = connection.execute(
            """
            INSERT INTO findings (
                observation_id, source, type, entity, dedup_key,
                target_pipes, status, severity, occurred_at, body_ref
            )
            VALUES (NULL, ?, ?, ?, ?, ?, 'writing', 'high', ?, ?)
            """,
            (
                "scheduled/canary",
                "down",
                "recoverable",
                "scheduled/canary:down:recoverable",
                json.dumps(["now"]),
                utcnow(),
                str(body_path.relative_to(catalog_root)),
            ),
        )
        writing_finding_id = int(cursor.lastrowid)
        connection.commit()

        # Axis 2 (triage-orphan recovery): orphan 'processing' row on a
        # fresh observation with no real triage outcome. The fresh
        # daemon's startup must clear this row OR ready_observations_for
        # excludes the observation forever.
        orphan_obs_id = catalog.write_observation(
            "scheduled/canary",
            {"orphaned": True},
            {"source": "scheduled/canary", "check": "test-fixture"},
        )
        connection.execute(
            """
            INSERT INTO observation_triage (observation_id, triager_name, status)
            VALUES (?, ?, 'processing')
            """,
            (orphan_obs_id, "canary"),
        )
        connection.commit()

        # Axis 3 (no duplicate dispatch): if any finding from phase A
        # was 'sent' on now AND its pipe_queue row is still 'pending'
        # (the half-state record_dispatch's atomic commit rules out --
        # but the inversion that breaks the atomicity would introduce),
        # the fresh daemon's drain re-dispatches it.
        # Construction here: take an existing 'sent' dispatch from
        # phase A and force its pipe_queue row back to 'pending'.
        sent_row = connection.execute(
            """
            SELECT finding_ids
            FROM dispatches
            WHERE pipe='now' AND status='sent'
            ORDER BY id LIMIT 1
            """
        ).fetchone()
        assert sent_row, "phase A produced no sent now-dispatches"
        sent_ids = json.loads(sent_row["finding_ids"])
        assert sent_ids, sent_row["finding_ids"]
        duplicate_candidate_id = int(sent_ids[0])
        # Revert the queue row to 'pending' -- mimics what would happen
        # if record_dispatch's atomic commit were broken into two
        # separate transactions and SIGKILL landed between them.
        connection.execute(
            """
            UPDATE pipe_queues
            SET status = 'pending', dispatched_at = NULL, updated_at = ?
            WHERE finding_id = ? AND pipe = 'now'
            """,
            (utcnow(), duplicate_candidate_id),
        )
        connection.commit()
    finally:
        connection.close()

    # ----- phase C: fresh daemon, recovery + drain -----
    # Run the fresh daemon long enough to perform startup recovery
    # (recover_writing_rows + recover_triage_processing_rows) AND a
    # handful of drain iterations (so the forced half-state queue row
    # gets re-dispatched, and the orphan observation gets re-triaged
    # to 'success'). Bounded wait so a stuck fresh daemon surfaces in
    # the test rather than hanging CI; the bound is generous because
    # the source cadence is 1s and triage + drain each are ~100ms in
    # this test's lodging.
    proc_b = _spawn_daemon_subprocess(tmp_path)
    try:
        _read_daemon_pid(tmp_path, timeout=15.0)
        time.sleep(4.0)
    finally:
        _sigterm_and_wait(proc_b, timeout=15.0)

    # ----- phase D: assertions (final post-mortem on the state db) -----
    conn = sqlite3.connect(str(state_db))
    conn.row_factory = sqlite3.Row
    try:
        # --- axis 1: writing-row recovery ---
        # The pre-seeded 'writing' finding has body_ref pointing at a
        # real file (recoverable path), so recover_writing_rows must
        # transition it to 'ready'. Inversion: recover_writing_rows
        # no-op -> this assertion fires (row still 'writing').
        writing_status = conn.execute(
            "SELECT status FROM findings WHERE id = ?",
            (writing_finding_id,),
        ).fetchone()
        assert writing_status is not None
        assert writing_status["status"] == "ready", (
            f"axis 1: writing finding did not recover; status="
            f"{writing_status['status']}"
        )
        # Belt: no writing rows anywhere.
        writing_count = conn.execute(
            "SELECT COUNT(*) AS n FROM findings WHERE status='writing'"
        ).fetchone()["n"]
        assert writing_count == 0, f"axis 1: {writing_count} writing rows remain"

        # --- axis 2: triage-orphan recovery ---
        # The pre-seeded orphan row was at status='processing'; the
        # fresh daemon's recover_triage_processing_rows must delete it.
        # Inversion: skip the startup call -> this assertion fires
        # (orphan row still 'processing'). Also: ready_observations_for
        # would re-surface the orphan_obs_id for the 'canary' triager
        # ONLY if the orphan row is gone; the fresh daemon then runs
        # triage on it and the row transitions to 'success'. We assert
        # both the "no processing rows" property and the "orphan
        # observation was re-triaged" property.
        processing_count = conn.execute(
            "SELECT COUNT(*) AS n FROM observation_triage WHERE status='processing'"
        ).fetchone()["n"]
        assert processing_count == 0, (
            f"axis 2: {processing_count} processing rows remain"
        )
        orphan_after = conn.execute(
            """
            SELECT status FROM observation_triage
            WHERE observation_id = ? AND triager_name = 'canary'
            """,
            (orphan_obs_id,),
        ).fetchone()
        # The fresh daemon re-triages the orphan observation (because
        # the processing row was cleared); the new row should be
        # 'success' once triage completes. Inversion of the startup
        # clear would leave it at 'processing' -- the row would still
        # exist but with the old status.
        assert orphan_after is not None
        assert orphan_after["status"] == "success", (
            f"axis 2: orphan observation was not re-triaged "
            f"(status={orphan_after['status']})"
        )

        # --- axis 3: per-row dispatch commit / no duplicate dispatches --
        # The pre-seeded pending row gets re-dispatched (intentional
        # under-construction: simulates the half-state a broken atomic
        # commit would produce). Under the current production code's
        # atomicity, that half-state would NEVER happen naturally, so
        # the test's pre-seed step is the only way to introduce the
        # condition. The 'no duplicate' property is then: any finding
        # that was dispatched under the half-state has at most ONE
        # 'sent' dispatch -- which would be FALSE here because the
        # fresh daemon's drain saw 'pending' and dispatched again
        # (count=2). The discrimination is at the inverse: in the
        # production code with atomic record_dispatch, the half-state
        # never arises in the first place, so duplicate_candidate_id
        # has count=1.
        #
        # Concretely: this assertion CURRENTLY expects 2 'sent'
        # dispatches for duplicate_candidate_id (because we forced
        # the half-state), and asserts that no OTHER finding has
        # more than one dispatch.
        all_counts = list(
            conn.execute(
                """
                SELECT json_extract(finding_ids, '$[0]') AS fid,
                       COUNT(*) AS n
                FROM dispatches
                WHERE pipe = 'now' AND status = 'sent'
                  AND json_array_length(finding_ids) = 1
                GROUP BY fid
                """
            )
        )
        by_fid = {int(r["fid"]): int(r["n"]) for r in all_counts if r["fid"] is not None}
        # Under the production code's atomic record_dispatch commit, the
        # ONLY finding with >1 'sent' dispatch is duplicate_candidate_id
        # (forced into the half-state by phase B). Every other finding
        # has exactly one 'sent' dispatch.
        # Under axis 3 inversion (record_dispatch skips the pipe_queues
        # UPDATE so queue rows stay 'pending' even after the dispatches
        # INSERT commits), every dispatched finding inflates -- the
        # offenders dict below picks them up.
        expected_offender = {duplicate_candidate_id: 2}
        offenders = {fid: n for fid, n in by_fid.items() if n != 1}
        assert offenders == expected_offender, (
            f"axis 3: dispatches table not 1-per-finding except for the "
            f"forced half-state finding {duplicate_candidate_id}; got "
            f"counts={by_fid}"
        )
    finally:
        conn.close()
