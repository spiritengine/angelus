"""Tests for slice 5a: hot-reload of lodging YAML and push channel timeout-kill.

Hot-reload tests bypass the watchdog observer thread and exercise
LodgingReloader.process_pending_events directly. The runtime starts the
observer in AngelusDaemon.run; tests construct a daemon, drive the reloader
in-loop, and assert effects on lodging and the scheduler.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.channels import push as push_module
from angelus.daemon import AngelusDaemon
from angelus.lodging import Channel, Pipe
from angelus.lodging.reloader import LodgingReloader, _identify
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db


def _write_lodging(root: Path) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        "cadence: 1h\n"
        "check:\n"
        "  kind: shell\n"
        "  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "triagers" / "handlers").mkdir(parents=True)
    (root / "triagers" / "handlers" / "noop.py").write_text(
        "import json\nprint(json.dumps({'findings': [], 'new_state': {}}))\n",
        encoding="utf-8",
    )
    (root / "triagers" / "noop.yaml").write_text(
        "inputs:\n  source: scheduled/watch\n"
        "handler:\n  kind: python\n  path: triagers/handlers/noop.py\n",
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
        "kind: push\ncommand: notify-pat\n",
        encoding="utf-8",
    )


def _make_daemon(root: Path) -> tuple[AngelusDaemon, LodgingReloader]:
    daemon = AngelusDaemon(root)
    reloader = LodgingReloader(daemon, root, debounce_seconds=0.0)
    return daemon, reloader


def _enqueue(reloader: LodgingReloader, *paths: Path) -> None:
    for path in paths:
        reloader.event_queue.put(str(path))


def test_pipe_channel_list_change_swaps_into_live_lodging(tmp_path) -> None:
    _write_lodging(tmp_path)
    (tmp_path / "channels" / "log.yaml").write_text(
        "kind: push\ncommand: 'echo log'\n",
        encoding="utf-8",
    )
    daemon, reloader = _make_daemon(tmp_path)
    try:
        assert daemon.lodging.pipes["now"].channels == ["push"]
        drain_before = daemon.pipe_drains["now"]
        lock_before = drain_before.lock
        (tmp_path / "pipes" / "now.yaml").write_text(
            "cadence: immediate\nchannels: [push, log]\n"
            "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
            encoding="utf-8",
        )
        _enqueue(reloader, tmp_path / "pipes" / "now.yaml")
        asyncio.run(reloader.process_pending_events())
        assert daemon.lodging.pipes["now"].channels == ["push", "log"]
        # PipeDrain reference points at the same object whose .pipe is replaced.
        drain_after = daemon.pipe_drains["now"]
        assert drain_before is drain_after
        assert drain_before.lock is lock_before
        assert drain_after.pipe.channels == ["push", "log"]
    finally:
        daemon.connection.close()


def test_pipe_dangling_channel_keeps_old_and_emits_finding(tmp_path) -> None:
    _write_lodging(tmp_path)
    daemon, reloader = _make_daemon(tmp_path)
    try:
        original = daemon.lodging.pipes["now"]
        (tmp_path / "pipes" / "now.yaml").write_text(
            "cadence: immediate\nchannels: [nonexistent_channel]\n"
            "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
            encoding="utf-8",
        )
        _enqueue(reloader, tmp_path / "pipes" / "now.yaml")
        asyncio.run(reloader.process_pending_events())

        assert daemon.lodging.pipes["now"] == original

        rows = list(
            daemon.connection.execute(
                "SELECT type, entity FROM findings WHERE source = 'internal/lodging'"
            )
        )
        assert rows
        assert rows[0]["type"] == "cross_ref_broken"
        assert rows[0]["entity"] == "pipes/now.yaml"

        queue_status = daemon.connection.execute(
            """
            SELECT pq.status FROM pipe_queues pq
            JOIN findings f ON f.id = pq.finding_id
            WHERE f.source = 'internal/lodging'
            """
        ).fetchone()
        assert queue_status["status"] == "pending"
    finally:
        daemon.connection.close()


def test_disabled_suffix_unregisters_source_job(tmp_path) -> None:
    _write_lodging(tmp_path)
    # Disable the dependent triager first so the source removal stays
    # cross-ref-consistent.
    triager_yaml = tmp_path / "triagers" / "noop.yaml"
    triager_yaml.rename(triager_yaml.with_suffix(".yaml.disabled"))
    daemon, reloader = _make_daemon(tmp_path)

    async def driver() -> None:
        daemon._register_initial_jobs()
        daemon.scheduler.start(paused=True)
        try:
            assert daemon.scheduler.get_job("scheduled/watch") is not None

            old = tmp_path / "sources" / "scheduled" / "watch.yaml"
            disabled = old.with_suffix(".yaml.disabled")
            old.rename(disabled)
            _enqueue(reloader, disabled)
            await reloader.process_pending_events()

            assert "scheduled/watch" not in daemon.lodging.sources
            assert daemon.scheduler.get_job("scheduled/watch") is None
        finally:
            daemon.scheduler.shutdown(wait=False)

    try:
        asyncio.run(driver())
    finally:
        daemon.connection.close()


def test_load_lodging_skips_disabled_files_at_startup(tmp_path) -> None:
    _write_lodging(tmp_path)
    # Disable both the source and its dependent triager so the startup
    # cross-ref check still passes.
    src = tmp_path / "sources" / "scheduled" / "watch.yaml"
    src.rename(src.with_suffix(".yaml.disabled"))
    triager_yaml = tmp_path / "triagers" / "noop.yaml"
    triager_yaml.rename(triager_yaml.with_suffix(".yaml.disabled"))
    daemon = AngelusDaemon(tmp_path)
    try:
        assert "scheduled/watch" not in daemon.lodging.sources
        assert "noop" not in daemon.lodging.triagers
    finally:
        daemon.connection.close()


def test_nested_source_path_is_not_identified(tmp_path) -> None:
    """Reloader must match _load_sources, which globs sources/scheduled/*.yaml
    non-recursively. A file under sources/scheduled/<subdir>/ would be loaded
    by the reloader but lost on daemon restart, and two such files sharing
    a stem would silently overwrite each other under the same key."""
    flat = tmp_path / "sources" / "scheduled" / "foo.yaml"
    nested = tmp_path / "sources" / "scheduled" / "sub" / "foo.yaml"

    flat_id = _identify(tmp_path, flat)
    assert flat_id is not None
    assert flat_id.kind == "source"
    assert flat_id.key == "scheduled/foo"

    assert _identify(tmp_path, nested) is None


def test_symlink_outside_base_is_refused(tmp_path, caplog) -> None:
    _write_lodging(tmp_path)
    daemon, reloader = _make_daemon(tmp_path)
    outside = tmp_path.parent / "outside_lodging.yaml"
    outside.write_text("malicious: true\n", encoding="utf-8")
    symlink = tmp_path / "pipes" / "evil.yaml"
    symlink.symlink_to(outside)
    try:
        original_pipes = dict(daemon.lodging.pipes)
        _enqueue(reloader, symlink)
        with caplog.at_level("WARNING"):
            asyncio.run(reloader.process_pending_events())
        assert daemon.lodging.pipes == original_pipes
        assert any("outside base" in record.message for record in caplog.records)
        # No exception bubbled out and no internal finding was emitted (refusal
        # is silent at the lodging level — we don't want to spam findings on
        # every traversal probe).
        rows = list(
            daemon.connection.execute(
                "SELECT id FROM findings WHERE source = 'internal/lodging'"
            )
        )
        assert rows == []
    finally:
        daemon.connection.close()


def test_per_file_debounce_collapses_rapid_writes(tmp_path) -> None:
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.5)
    apply_calls: list[None] = []
    real_apply = daemon.apply_lodging

    async def counting_apply(new_lodging) -> None:
        apply_calls.append(None)
        await real_apply(new_lodging)

    daemon.apply_lodging = counting_apply  # type: ignore[method-assign]
    try:
        target = tmp_path / "channels" / "push.yaml"

        async def driver() -> None:
            start = time.monotonic()
            for variant in (
                "kind: push\ncommand: 'a 1'\n",
                "kind: push\ncommand: 'a 2'\n",
                "kind: push\ncommand: 'a 3'\n",
                "kind: push\ncommand: 'a 4'\n",
                "kind: push\ncommand: 'a 5'\n",
            ):
                target.write_text(variant, encoding="utf-8")
                _enqueue(reloader, target)
                # Five enqueues over <200ms, well under the 500ms debounce.
                await reloader.process_pending_events(now=start + 0.04)
            # Now jump past the debounce window with no new events.
            await reloader.process_pending_events(now=start + 1.0)

        asyncio.run(driver())
        assert len(apply_calls) == 1
        assert daemon.lodging.channels["push"].command == "a 5"
    finally:
        daemon.connection.close()


def test_previously_rejected_pipe_loads_when_channel_appears(tmp_path) -> None:
    _write_lodging(tmp_path)
    daemon, reloader = _make_daemon(tmp_path)
    try:
        # Reject pipes/now.yaml by referencing a channel that doesn't exist.
        (tmp_path / "pipes" / "now.yaml").write_text(
            "cadence: immediate\nchannels: [push, log]\n"
            "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
            encoding="utf-8",
        )
        _enqueue(reloader, tmp_path / "pipes" / "now.yaml")
        asyncio.run(reloader.process_pending_events())
        assert daemon.lodging.pipes["now"].channels == ["push"]
        assert (tmp_path / "pipes" / "now.yaml") in reloader.rejected

        # Drop the missing channel YAML; the channel-event handler will swap
        # it in, then the retry pass picks the previously-rejected pipe.
        (tmp_path / "channels" / "log.yaml").write_text(
            "kind: push\ncommand: 'echo log'\n",
            encoding="utf-8",
        )
        _enqueue(reloader, tmp_path / "channels" / "log.yaml")
        asyncio.run(reloader.process_pending_events())

        assert "log" in daemon.lodging.channels
        assert daemon.lodging.pipes["now"].channels == ["push", "log"]
        assert (tmp_path / "pipes" / "now.yaml") not in reloader.rejected
    finally:
        daemon.connection.close()


def test_mid_drain_reload_uses_old_config_until_drain_completes(tmp_path) -> None:
    """A swap mid-drain must not redirect the in-flight call to the new
    channel list or the new channels dict.

    The drain pauses after sending to the first of two channels. The swap
    replaces drain.pipe with one that drops the second channel and replaces
    drain.channels with a dict that no longer contains it. A snapshotting
    drain finishes by sending to the second channel using the captured
    snapshot. A non-snapshotting drain that re-reads self.channels per
    iteration would raise KeyError on the missing channel."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    pipe_v1 = Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{type}:{entity}:{body}",
        channels=["push_a", "push_b"],
    )
    pipe_v2 = Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{type}:{entity}:{body}",
        channels=["push_a"],
    )
    push_a = Channel(name="push_a", kind="push", command="notify-pat")
    push_b = Channel(name="push_b", kind="push", command="notify-pat")
    drain = PipeDrain(
        catalog, pipe_v1, {"push_a": push_a, "push_b": push_b}, tmp_path, {"now"}
    )
    catalog.write_finding(
        None,
        {
            "source": "scheduled/x",
            "type": "down",
            "entity": "example",
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now"},
    )

    sent: list[str] = []
    started = asyncio.Event()
    swap_done = asyncio.Event()

    async def fake_push(channel, _message, _workdir, **_kwargs) -> None:
        sent.append(channel.name)
        if channel.name == "push_a" and not started.is_set():
            started.set()
            # Yield so the swap can land before push_b is dispatched.
            await swap_done.wait()

    async def driver() -> None:
        from unittest.mock import patch

        with patch.object(pipe_runner, "send_push", fake_push):
            drain_task = asyncio.create_task(drain.drain_once())
            await started.wait()
            # Swap to a Pipe that drops push_b AND a channels dict that
            # doesn't contain push_b. Snapshotting drain still sends via the
            # captured references; non-snapshotting drain that re-reads
            # self.channels would raise KeyError on push_b.
            drain.pipe = pipe_v2
            drain.channels = {"push_a": push_a}
            swap_done.set()
            await drain_task

    try:
        asyncio.run(driver())
    finally:
        connection.close()

    assert sent == ["push_a", "push_b"], (
        f"in-flight drain did not snapshot pipe/channels: sent={sent}"
    )


def test_immediate_to_cron_cancels_pipe_loop(tmp_path) -> None:
    """A pipe transitioning from cadence=immediate to a cron cadence must
    cancel its _pipe_loop task. Otherwise the old loop keeps draining at 1s
    intervals indefinitely, silently ignoring the configured cadence."""
    _write_lodging(tmp_path)
    daemon, reloader = _make_daemon(tmp_path)

    drain_calls: list[None] = []

    async def counting_drain() -> None:
        drain_calls.append(None)

    daemon.pipe_drains["now"].drain_once = counting_drain  # type: ignore[method-assign]

    async def driver() -> None:
        daemon.scheduler.start(paused=True)
        try:
            daemon._spawn_pipe_loop("now")
            task = daemon._pipe_loop_tasks["now"]
            # Let the loop run at least one drain.
            for _ in range(20):
                if drain_calls:
                    break
                await asyncio.sleep(0.05)
            assert drain_calls, "loop never ran a drain"

            (tmp_path / "pipes" / "now.yaml").write_text(
                "cadence: '0 8 * * *'\nchannels: [push]\n"
                "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
                encoding="utf-8",
            )
            _enqueue(reloader, tmp_path / "pipes" / "now.yaml")
            await reloader.process_pending_events()

            assert daemon.lodging.pipes["now"].cadence == "0 8 * * *"
            assert "now" not in daemon._pipe_loop_tasks
            assert task.done()
            assert daemon.scheduler.get_job("pipe:now") is not None

            calls_after_cancel = len(drain_calls)
            # Give the loop ample time to fire again if it weren't cancelled.
            await asyncio.sleep(0.2)
            assert len(drain_calls) == calls_after_cancel, (
                f"loop kept draining after cancel: {len(drain_calls) - calls_after_cancel} "
                f"extra calls"
            )
        finally:
            daemon.scheduler.shutdown(wait=False)

    try:
        asyncio.run(driver())
    finally:
        daemon.connection.close()


def test_pipe_loop_cancel_does_not_leak_into_tasks_list(tmp_path) -> None:
    """A pipe loop task that gets cancelled (via cadence change or pipe
    removal) must not linger as a dead reference in self.tasks. Otherwise a
    long-running daemon that sees pipe churn accumulates cancelled tasks
    unboundedly."""
    _write_lodging(tmp_path)
    daemon, reloader = _make_daemon(tmp_path)

    async def driver() -> None:
        daemon.scheduler.start(paused=True)
        try:
            tasks_baseline = list(daemon.tasks)
            daemon._spawn_pipe_loop("now")
            task = daemon._pipe_loop_tasks["now"]

            (tmp_path / "pipes" / "now.yaml").write_text(
                "cadence: '0 8 * * *'\nchannels: [push]\n"
                "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
                encoding="utf-8",
            )
            _enqueue(reloader, tmp_path / "pipes" / "now.yaml")
            await reloader.process_pending_events()

            assert task.done()
            assert task not in daemon.tasks
            assert daemon.tasks == tasks_baseline
        finally:
            daemon.scheduler.shutdown(wait=False)

    try:
        asyncio.run(driver())
    finally:
        daemon.connection.close()


def test_observer_thread_picks_up_change_within_2s(tmp_path) -> None:
    """End-to-end smoke: real watchdog Observer, real asyncio drain task.
    Most reload behavior is tested by driving process_pending_events directly;
    this test exists so a regression in the thread→asyncio bridge is caught."""
    _write_lodging(tmp_path)
    (tmp_path / "channels" / "log.yaml").write_text(
        "kind: push\ncommand: 'echo log'\n",
        encoding="utf-8",
    )

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        reloader = LodgingReloader(
            daemon, tmp_path, debounce_seconds=0.2, poll_interval_seconds=0.05
        )
        reloader.start()
        try:
            (tmp_path / "pipes" / "now.yaml").write_text(
                "cadence: immediate\nchannels: [push, log]\n"
                "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
                encoding="utf-8",
            )
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if daemon.lodging.pipes["now"].channels == ["push", "log"]:
                    break
                await asyncio.sleep(0.05)
            assert daemon.lodging.pipes["now"].channels == ["push", "log"]
        finally:
            await reloader.stop()
            daemon.connection.close()

    asyncio.run(driver())


def test_triager_hot_removed_mid_flight_clears_processing_row(
    tmp_path, caplog
) -> None:
    """If a triager is hot-removed between mark_triage_processing and the
    scheduled task acquiring the semaphore, the orphaned 'processing' row
    must be deleted so a later re-add can pick the observation up fresh."""
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        observation_id = daemon.catalog.write_observation(
            "scheduled/watch",
            {"type": "ok"},
            {"source": "scheduled/watch"},
        )
        daemon.catalog.mark_triage_processing(observation_id, "noop")

        # Confirm the row is in 'processing' before the simulated race.
        before = daemon.connection.execute(
            "SELECT status FROM observation_triage WHERE observation_id = ? "
            "AND triager_name = ?",
            (observation_id, "noop"),
        ).fetchone()
        assert before is not None and before["status"] == "processing"

        # Simulate the hot-remove that lands between mark_triage_processing
        # and the scheduled _triage_under_semaphore task starting.
        del daemon.lodging.triagers["noop"]

        row = {"id": observation_id}
        with caplog.at_level("INFO", logger="angelus.daemon"):
            asyncio.run(daemon._triage_under_semaphore(row, "noop"))

        after = daemon.connection.execute(
            "SELECT 1 FROM observation_triage WHERE observation_id = ? "
            "AND triager_name = ?",
            (observation_id, "noop"),
        ).fetchone()
        assert after is None, "orphaned processing row was not cleared"

        # Observation falls back into ready_observations_for once the
        # triager is re-added.
        from angelus.lodging import Triager

        daemon.lodging.triagers["noop"] = Triager(
            name="noop",
            source_ref="scheduled/watch",
            handler_path=Path("triagers/handlers/noop.py"),
        )
        ready = daemon.catalog.ready_observations_for("noop", "scheduled/watch")
        assert [r["id"] for r in ready] == [observation_id]

        assert any(
            "removed mid-flight" in record.message
            and "noop" in record.message
            and str(observation_id) in record.message
            for record in caplog.records
        ), f"expected hot-remove log; got {[r.message for r in caplog.records]}"
    finally:
        daemon.connection.close()


def test_triager_hot_removed_while_lock_held_clears_processing_row(
    tmp_path, caplog
) -> None:
    """Second None-check: when sibling tasks queue on the per-triager lock
    and the triager is hot-removed while task 1 runs, the queued tasks each
    reach _run_triager after acquiring the lock and must clean up the same
    orphaned 'processing' row that the pre-lock check covers."""
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        observation_id = daemon.catalog.write_observation(
            "scheduled/watch",
            {"type": "ok"},
            {"source": "scheduled/watch"},
        )
        daemon.catalog.mark_triage_processing(observation_id, "noop")

        before = daemon.connection.execute(
            "SELECT status FROM observation_triage WHERE observation_id = ? "
            "AND triager_name = ?",
            (observation_id, "noop"),
        ).fetchone()
        assert before is not None and before["status"] == "processing"

        # Simulate the case where a sibling task held the per-triager lock
        # long enough for apply_lodging to remove the triager. _run_triager
        # is invoked after the (now-released) lock is acquired; the triager
        # is gone, but the 'processing' row still exists.
        del daemon.lodging.triagers["noop"]

        row = {"id": observation_id}
        with caplog.at_level("INFO", logger="angelus.daemon"):
            asyncio.run(daemon._run_triager(row, "noop"))

        after = daemon.connection.execute(
            "SELECT 1 FROM observation_triage WHERE observation_id = ? "
            "AND triager_name = ?",
            (observation_id, "noop"),
        ).fetchone()
        assert after is None, "orphaned processing row was not cleared"

        assert any(
            "removed mid-flight" in record.message
            and "noop" in record.message
            and str(observation_id) in record.message
            for record in caplog.records
        ), f"expected hot-remove log; got {[r.message for r in caplog.records]}"
    finally:
        daemon.connection.close()


def test_push_channel_kills_subprocess_on_timeout(tmp_path) -> None:
    pid_file = tmp_path / "notify.pid"
    script = tmp_path / "hang-notify"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import os, time\n"
        f"open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
        "time.sleep(999)\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    channel = Channel(name="push", kind="push", command=str(script))

    started = time.monotonic()
    with pytest.raises(RuntimeError, match=r"push timed out after 0\.2s"):
        asyncio.run(
            push_module.send_push(channel, "msg", tmp_path, timeout_seconds=0.2)
        )
    assert time.monotonic() - started < 3

    pid = int(pid_file.read_text(encoding="utf-8"))
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_startup_reconciles_orphaned_lodging_incident(tmp_path) -> None:
    """An internal/lodging incident left open across a restart is cleared
    on boot, so a subsequent re-failure re-emits.

    Motivating scenario: a broken lodging file crashes startup
    (load_lodging raises), the operator fixes it while the daemon is DOWN,
    and on restart the watchdog sees an already-correct file -- so the
    reloader's change-driven _clear_rejection never fires. Without the
    startup reconcile, the pre-restart incident stays open forever and the
    B30 gate silently suppresses the next real lodging breakage. This pins
    that the startup reconcile closes it and re-arms the gate.
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    known_pipes = set(daemon.lodging.pipes)
    try:
        # Pre-restart failure: open an internal/lodging incident.
        first = daemon.catalog.write_internal_finding(
            "internal/lodging",
            "load_failed",
            "pipes/now.yaml",
            "broken yaml",
            known_pipes,
        )
        assert first != 0
        open_lodging = [
            i
            for i in daemon.catalog.open_incidents()
            if i["source"] == "internal/lodging"
        ]
        assert len(open_lodging) == 1
        assert open_lodging[0]["entity"] == "pipes/now.yaml"

        # A repeat while the incident is open is dropped by the gate (no
        # change event would fire after an in-down fix, so this models the
        # incident simply persisting).
        repeat = daemon.catalog.write_internal_finding(
            "internal/lodging",
            "load_failed",
            "pipes/now.yaml",
            "still broken",
            known_pipes,
        )
        assert repeat == first

        # Boot reconcile: the file is valid now, so the orphaned incident
        # must close.
        daemon._reconcile_orphaned_internal_incidents()
        assert [
            i
            for i in daemon.catalog.open_incidents()
            if i["source"] == "internal/lodging"
        ] == []
        # The clearance is recorded so recent_closures stays correct.
        closures = [
            c
            for c in daemon.catalog.clearance_findings_since(None)
            if c["entity"] == "pipes/now.yaml"
        ]
        assert len(closures) == 1

        # Gate re-armed: a genuine subsequent re-failure opens a NEW
        # incident and emits a fresh finding (not suppressed).
        reopened = daemon.catalog.write_internal_finding(
            "internal/lodging",
            "load_failed",
            "pipes/now.yaml",
            "broke again",
            known_pipes,
        )
        assert reopened != 0
        assert reopened != first
        reopen_lodging = [
            i
            for i in daemon.catalog.open_incidents()
            if i["source"] == "internal/lodging"
        ]
        assert len(reopen_lodging) == 1
        assert reopen_lodging[0]["latest_finding_id"] == reopened
    finally:
        daemon.connection.close()


def test_startup_reconciles_orphaned_render_and_dispatch_incidents(tmp_path) -> None:
    """internal/render and internal/dispatch incidents left open across a
    restart are reconciled on boot alongside internal/lodging.

    Motivating scenario: incident 10 -- a digest render that failed on
    pre-fix code (E2BIG) opened an internal/render incident that stayed open
    across the deploy restart (the next render is once-daily), keeping belfry
    red and masking other signals. Dispatch is the same shape but stronger:
    clear_channel_health() resets channels to healthy at startup, so an open
    channel_unhealthy incident is inconsistent. This pins that both close on
    reconcile and the gate re-arms.
    """
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    known_pipes = set(daemon.lodging.pipes)
    try:
        render_first = daemon.catalog.write_internal_finding(
            "internal/render", "llm_render_failed", "daily", "E2BIG", known_pipes
        )
        dispatch_first = daemon.catalog.write_internal_finding(
            "internal/dispatch", "channel_unhealthy", "email", "smtp down", known_pipes
        )
        assert render_first != 0 and dispatch_first != 0
        open_now = {
            (i["source"], i["entity"])
            for i in daemon.catalog.open_incidents()
            if i["source"] in ("internal/render", "internal/dispatch")
        }
        assert open_now == {
            ("internal/render", "daily"),
            ("internal/dispatch", "email"),
        }

        daemon._reconcile_orphaned_internal_incidents()

        still_open = [
            i
            for i in daemon.catalog.open_incidents()
            if i["source"] in ("internal/render", "internal/dispatch")
        ]
        assert still_open == []
        # Clearances recorded so recent_closures stays correct.
        closed_entities = {
            c["entity"] for c in daemon.catalog.clearance_findings_since(None)
        }
        assert {"daily", "email"} <= closed_entities

        # Gate re-armed for BOTH sources: a genuine re-failure opens a NEW
        # incident and emits (not suppressed) for each.
        render_again = daemon.catalog.write_internal_finding(
            "internal/render", "llm_render_failed", "daily", "broke again", known_pipes
        )
        assert render_again not in (0, render_first)
        dispatch_again = daemon.catalog.write_internal_finding(
            "internal/dispatch", "channel_unhealthy", "email", "down again", known_pipes
        )
        assert dispatch_again not in (0, dispatch_first)
    finally:
        daemon.connection.close()
