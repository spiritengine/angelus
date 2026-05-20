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
import time
from pathlib import Path
from unittest.mock import patch

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.daemon import AngelusDaemon
from angelus.lodging.reloader import LodgingReloader


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
    apply unchecked, lodging.channels would lose `push`, and no
    cross_ref_broken finding would be written. The assertion
    `"push" in daemon.lodging.channels` (and equally the
    `cross_ref_broken`-finding presence assertion) fires under that
    inversion.
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
