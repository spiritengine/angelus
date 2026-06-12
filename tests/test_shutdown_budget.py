"""Shared shutdown-teardown budget fell (brief-20260608-13w0).

The daemon promises a sub-8s, no-hang shutdown so systemd's SIGTERM->SIGKILL
grace is never hit. Pre-fix, run()'s finally violated that three ways: the
drain-task reap and the fixer-loop reap each had an INDEPENDENT ~6s deadline
(so two simultaneous hangs stacked to ~12s), the final gather over the
long-lived loops was unbounded (a wedged task hung shutdown forever), and the
immediate-pipe drain loops were awaited but never cancelled (a hung channel
send stalled teardown for the full send timeout).

These tests model the hangs with controllable awaitables (an Event-gated
wedge that swallows cancellation, or an Event-gated send that honours it) --
not real long sleeps -- and shrink the budget via ANGELUS_SHUTDOWN_BUDGET_SEC
so the timing assertions are about the budget mechanism, not luck. Each
test's discrimination against the pre-fix logic is noted in its docstring;
the double-hang test is THE one the sequential-deadline bug fails.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time
from pathlib import Path

import pytest

from angelus.daemon import (
    DEFAULT_SHUTDOWN_BUDGET_SEC,
    AngelusDaemon,
    _shutdown_budget_seconds,
)


def _minimal_lodging(root: Path, *, source_cmd: str = "echo {}") -> None:
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


class _Wedge:
    """A controllable awaitable modelling a task wedged against cancellation:
    it swallows every CancelledError thrown into it until the test sets
    `release`. This is the worst case the teardown budget must bound -- a
    task that not only hangs but actively refuses to die -- and it is exactly
    the case wait_for(gather(...)) could NOT bound (the cancelled gather
    future never resolves while a child stays pending)."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.cancels_swallowed = 0

    async def run(self) -> None:
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancels_swallowed += 1


async def _wait_for_daemon_up(daemon: AngelusDaemon) -> None:
    """Poll until run() has built its long-lived tasks (the fixer loop is
    created last among them, right before the immediate-pipe loops and the
    stop_event wait), so a test can splice wedges in before stopping."""
    for _ in range(400):
        if daemon._fixer_loop_task is not None and daemon._pipe_loop_tasks:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("daemon never finished starting its loops")


def test_double_hang_drain_and_fixer_share_one_budget(
    tmp_path, monkeypatch
) -> None:
    """THE sequential-deadline test: a wedged scheduled drain AND a wedged
    fixer at the same time must complete teardown within the ONE shared
    budget, not the sum of per-stage deadlines.

    Discrimination (mutation-verified): under the pre-fix logic each stage
    had its own _DRAIN_SHUTDOWN_TIMEOUT=6.0s bound (ANGELUS_SHUTDOWN_BUDGET_SEC
    ignored), so this teardown took 6s in the drain stage alone -- and a
    cancel-swallowing task additionally wedged wait_for(gather(...)) forever,
    so run() never returned and the 15s wait_for here timed out -> red either
    way. Post-fix the drain stage consumes the whole 1.5s budget, the fixer
    stage gets remaining ~0 and is abandoned immediately, and teardown lands
    well under the 4s assertion."""
    _minimal_lodging(tmp_path)
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")
    monkeypatch.setenv("ANGELUS_SHUTDOWN_BUDGET_SEC", "1.5")

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        run_task = asyncio.create_task(daemon.run())
        drain_wedge = _Wedge()
        fixer_wedge = _Wedge()
        wedged_drain: asyncio.Task | None = None
        wedged_fixer: asyncio.Task | None = None
        real_fixer: asyncio.Task | None = None
        try:
            await _wait_for_daemon_up(daemon)
            # A wedged scheduled drain: tracked in _drain_tasks exactly like
            # a cancelled-but-stuck digest drain job.
            wedged_drain = asyncio.create_task(drain_wedge.run())
            daemon._drain_tasks.add(wedged_drain)
            # A wedged fixer loop: stands in for a fixer whose cancellation
            # arm is stuck. The real loop is kept aside; it exits cleanly on
            # stop_event and is awaited below.
            real_fixer = daemon._fixer_loop_task
            wedged_fixer = asyncio.create_task(fixer_wedge.run())
            daemon._fixer_loop_task = wedged_fixer

            started = time.monotonic()
            daemon.request_stop()
            await asyncio.wait_for(run_task, timeout=15.0)
            elapsed = time.monotonic() - started
            assert elapsed < 4.0, (
                f"double-hang teardown took {elapsed:.1f}s -- the drain and "
                "fixer reap stages stacked instead of sharing one budget"
            )
        finally:
            drain_wedge.release.set()
            fixer_wedge.release.set()
            await asyncio.gather(
                *(t for t in (wedged_drain, wedged_fixer, real_fixer) if t),
                return_exceptions=True,
            )
            if not run_task.done():
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)
        # Both wedges were actually cancelled (the teardown tried to kill
        # them; they refused) -- proves the bound came from the budget, not
        # from the wedges quietly finishing.
        assert drain_wedge.cancels_swallowed >= 1
        assert fixer_wedge.cancels_swallowed >= 1

    asyncio.run(driver())


def test_hung_immediate_send_is_cancelled_not_waited_out(
    tmp_path, monkeypatch
) -> None:
    """A channel send hung inside the immediate `now` pipe's drain loop at
    shutdown must be CANCELLED, not awaited until the send timeout. The hung
    send here honours cancellation (a real SMTP/HTTP send does -- its
    CancelledError arm reaps the transport subprocess); what it never does is
    finish on its own.

    Discrimination: pre-fix the pipe loop was awaited in the final gather
    without being cancelled, so run() blocked on the hung send forever (here:
    until the 15s wait_for trips -> red; in production: the full 30s send
    timeout)."""
    _minimal_lodging(tmp_path)
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")
    monkeypatch.setenv("ANGELUS_SHUTDOWN_BUDGET_SEC", "2.0")

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        send_started = asyncio.Event()
        hang = asyncio.Event()  # never set during teardown: the hung send
        state = {"cancelled": False}

        async def hung_drain_once():
            send_started.set()
            try:
                await hang.wait()
            except asyncio.CancelledError:
                state["cancelled"] = True
                raise

        monkeypatch.setattr(
            daemon.pipe_drains["now"], "drain_once", hung_drain_once
        )
        run_task = asyncio.create_task(daemon.run())
        try:
            await asyncio.wait_for(send_started.wait(), timeout=10.0)
            started = time.monotonic()
            daemon.request_stop()
            await asyncio.wait_for(run_task, timeout=15.0)
            elapsed = time.monotonic() - started
            assert elapsed < 5.0, (
                f"teardown took {elapsed:.1f}s with a hung immediate send -- "
                "the pipe loop was awaited instead of cancelled"
            )
            assert state["cancelled"], (
                "hung immediate send was never cancelled -- teardown must "
                "have waited it out or skipped it"
            )
        finally:
            hang.set()
            if not run_task.done():
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)

    asyncio.run(driver())


def test_wedged_pending_task_is_bounded_by_remaining_budget(
    tmp_path, monkeypatch
) -> None:
    """A wedged task in the final pending wait (self.tasks) must be bounded
    by the remaining shared budget, then force-cancelled and abandoned.

    Discrimination: pre-fix the final gather had NO timeout at all, so this
    teardown never returned and the 15s wait_for tripped -> red."""
    _minimal_lodging(tmp_path)
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")
    monkeypatch.setenv("ANGELUS_SHUTDOWN_BUDGET_SEC", "1.5")

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        run_task = asyncio.create_task(daemon.run())
        wedge = _Wedge()
        wedged: asyncio.Task | None = None
        try:
            await _wait_for_daemon_up(daemon)
            wedged = asyncio.create_task(wedge.run())
            daemon.tasks.append(wedged)

            started = time.monotonic()
            daemon.request_stop()
            await asyncio.wait_for(run_task, timeout=15.0)
            elapsed = time.monotonic() - started
            assert elapsed < 4.0, (
                f"teardown took {elapsed:.1f}s with a wedged pending task -- "
                "the final wait is not bounded by the remaining budget"
            )
        finally:
            wedge.release.set()
            if wedged is not None:
                await asyncio.gather(wedged, return_exceptions=True)
            if not run_task.done():
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)
        # The deadline force-cancelled it (self.tasks members are not
        # pre-cancelled; only the budget expiry cancels them).
        assert wedge.cancels_swallowed >= 1

    asyncio.run(driver())


def test_second_sigterm_mid_shutdown_is_idempotent(
    tmp_path, monkeypatch
) -> None:
    """A second SIGTERM landing mid-teardown (operator double-tap, or systemd
    re-signalling) must be a no-op: the loop's signal handler is still
    installed during the finally and request_stop only re-sets stop_event.
    run() must complete cleanly within the budget, no double-cancel error."""
    _minimal_lodging(tmp_path)
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")
    monkeypatch.setenv("ANGELUS_SHUTDOWN_BUDGET_SEC", "1.5")

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        run_task = asyncio.create_task(daemon.run())
        wedge = _Wedge()
        wedged_drain: asyncio.Task | None = None
        try:
            await _wait_for_daemon_up(daemon)
            # A wedged drain holds teardown open long enough for the second
            # signal to land genuinely mid-shutdown.
            wedged_drain = asyncio.create_task(wedge.run())
            daemon._drain_tasks.add(wedged_drain)

            started = time.monotonic()
            daemon.request_stop()
            await asyncio.sleep(0.3)
            assert not run_task.done(), "teardown over before second signal"
            # The real signal path: the handler installed by
            # _install_signal_handlers fires request_stop again.
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.wait_for(run_task, timeout=15.0)
            elapsed = time.monotonic() - started
            assert elapsed < 4.0, f"teardown took {elapsed:.1f}s"
            # Clean completion: no exception out of run().
            assert run_task.result() is None
        finally:
            wedge.release.set()
            if wedged_drain is not None:
                await asyncio.gather(wedged_drain, return_exceptions=True)
            if not run_task.done():
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)

    asyncio.run(driver())


def test_cancelled_drain_reap_arm_still_runs_within_budget(
    tmp_path, monkeypatch
) -> None:
    """The budget must not starve the reap: a digest drain cancelled
    mid-render still runs its _kill_and_reap arm (the horizon process GROUP
    is reaped, no orphaned grandchild) even while a wedged fixer is consuming
    the rest of the budget -- and the whole teardown stays under 8s.

    Uses a real forking subprocess (the same horizon-stub shape as the m1
    integration tests) because the property under test IS the subprocess
    reap; the budget is set to 6s -- above _REAP_TIMEOUT (5s), per the
    constants' invariant -- so the drain stage has room for its reap arm and
    the wedged fixer then eats the remainder."""
    marker = tmp_path / "hz_child.pid"
    _minimal_lodging(tmp_path)
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
    monkeypatch.setenv("ANGELUS_SHUTDOWN_BUDGET_SEC", "6.0")

    def _alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False

    async def driver() -> int:
        daemon = AngelusDaemon(tmp_path)
        run_task = asyncio.create_task(daemon.run())
        fixer_wedge = _Wedge()
        wedged_fixer: asyncio.Task | None = None
        real_fixer: asyncio.Task | None = None
        try:
            for _ in range(400):
                if marker.exists() and marker.read_text().strip():
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("digest job never launched horizon")
            gc_pid = int(marker.read_text().strip())
            assert _alive(gc_pid)
            assert daemon._drain_tasks, "drain not tracked mid-render"

            real_fixer = daemon._fixer_loop_task
            wedged_fixer = asyncio.create_task(fixer_wedge.run())
            daemon._fixer_loop_task = wedged_fixer

            started = time.monotonic()
            daemon.request_stop()
            await asyncio.wait_for(run_task, timeout=15.0)
            elapsed = time.monotonic() - started
            assert elapsed < 8.0, (
                f"teardown took {elapsed:.1f}s -- past the no-hang bound"
            )
            return gc_pid
        finally:
            fixer_wedge.release.set()
            await asyncio.gather(
                *(t for t in (wedged_fixer, real_fixer) if t),
                return_exceptions=True,
            )
            if not run_task.done():
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)

    gc_pid = asyncio.run(driver())
    # The cancelled drain's reap arm ran inside the budget: the horizon
    # grandchild's whole process group is dead, not orphaned.
    for _ in range(200):
        if not _alive(gc_pid):
            break
        time.sleep(0.01)
    else:
        os.kill(gc_pid, 9)
        raise AssertionError(
            f"horizon grandchild {gc_pid} survived shutdown -- the shared "
            "budget starved the cancelled drain's _kill_and_reap arm"
        )


def test_shutdown_budget_env_parsing(monkeypatch) -> None:
    """ANGELUS_SHUTDOWN_BUDGET_SEC overrides; junk and non-positive values
    fall back to the default so shutdown never crashes on a bad env."""
    monkeypatch.delenv("ANGELUS_SHUTDOWN_BUDGET_SEC", raising=False)
    assert _shutdown_budget_seconds() == DEFAULT_SHUTDOWN_BUDGET_SEC
    monkeypatch.setenv("ANGELUS_SHUTDOWN_BUDGET_SEC", "2.5")
    assert _shutdown_budget_seconds() == 2.5
    monkeypatch.setenv("ANGELUS_SHUTDOWN_BUDGET_SEC", "banana")
    assert _shutdown_budget_seconds() == DEFAULT_SHUTDOWN_BUDGET_SEC
    monkeypatch.setenv("ANGELUS_SHUTDOWN_BUDGET_SEC", "0")
    assert _shutdown_budget_seconds() == DEFAULT_SHUTDOWN_BUDGET_SEC
    monkeypatch.setenv("ANGELUS_SHUTDOWN_BUDGET_SEC", "-3")
    assert _shutdown_budget_seconds() == DEFAULT_SHUTDOWN_BUDGET_SEC


def test_default_budget_leaves_room_for_a_reap_arm() -> None:
    """The constants' invariant, pinned: the default shared budget must
    exceed _kill_and_reap's _REAP_TIMEOUT (so one cancelled reap arm can
    always finish) and stay under the integration fell's 8s no-hang bound."""
    from angelus.sources.runner import _REAP_TIMEOUT

    assert DEFAULT_SHUTDOWN_BUDGET_SEC > _REAP_TIMEOUT
    assert DEFAULT_SHUTDOWN_BUDGET_SEC < 8.0
