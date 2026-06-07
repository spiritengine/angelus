"""Startup immediate-first-fire (Fix A of the belfry restart-loop hardening).

The incident: source-side change-detection upserts a per-source watch_state row
on every fire, and a fresh/wiped DB has the table but ZERO rows until the first
fire. Source jobs used to first fire at start+interval (the IntervalTrigger /
crontab default), so for a whole interval after boot watch_state stayed empty --
and belfry's wedge detector, reading max(last_checked_at) FROM watch_state, saw
"no rows", declared the daemon wedged, and restarted it ~2s before its first
fire. Restart loop.

Fix A brings each source's FIRST fire forward to the daemon's startup instant
via APScheduler's next_run_time (the clean "fire now, then keep the cadence"
idiom), so every source populates its watch_state heartbeat within seconds of
boot while the steady-state interval is unchanged.

These tests drive the real registration path (_register_initial_jobs ->
_add_source_job). The matching mutation that breaks each is noted in the shard
report (revert _add_source_job to not pass next_run_time, i.e. back to
start+interval).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon

PINNED = datetime(2026, 6, 6, 12, 0, 0, tzinfo=UTC)
SOURCE = "scheduled/s"


def _lodge(root: Path) -> Path:
    """Minimal lodging: one source on a 1h cadence whose check echoes a fixed
    JSON state, plus a token pipe/channel so load_lodging is happy. The 1h
    cadence is the discriminator -- under start+interval the first fire is an
    hour out, so the behavioral test below can only pass if the first fire was
    brought forward to startup."""
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    (scheduled / "s.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n"
        "  command: 'echo {\\\"state\\\": \\\"200\\\"}'\n",
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
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )
    return scheduled / "s.yaml"


def test_startup_schedules_first_fire_at_boot_instant(tmp_path: Path) -> None:
    """_register_initial_jobs schedules each source's FIRST fire at the daemon's
    startup instant (the injected clock's now), NOT start+interval. Asserted
    structurally off the added job's next_run_time.

    Discrimination: with Fix A reverted (no next_run_time passed) the job is
    pending with no next_run_time on the not-yet-started scheduler, so
    getattr(...) is None != PINNED and this fails. The interval assertion
    independently pins that the steady-state cadence is left untouched."""
    _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path, clock=FakeClock(PINNED))
    try:
        daemon._register_initial_jobs()
        job = daemon.scheduler.get_job(SOURCE)
        assert job is not None
        # First fire is the boot instant (clock-now), not now+interval.
        assert getattr(job, "next_run_time", None) == PINNED
        # Steady-state cadence is unchanged: still the configured 1h interval.
        assert job.trigger.interval == timedelta(hours=1)
    finally:
        daemon.connection.close()


def test_startup_fire_populates_watch_state_without_waiting_interval(
    tmp_path: Path,
) -> None:
    """Behavioral end-to-end on the REAL scheduler: bring the daemon up and the
    source's watch_state heartbeat appears within seconds, though the cadence is
    1h. This is the heartbeat belfry's wedge detector reads -- establishing it
    fast is the whole point of Fix A.

    Discrimination: with Fix A reverted the first fire is scheduled an hour out,
    so watch_state stays empty for the whole 5s window and the assertion fails
    (real Clock here, no FakeClock -- the scheduler keeps wall time)."""
    _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path)  # real Clock; scheduler runs on wall time

    async def _bring_up_and_wait() -> bool:
        daemon._register_initial_jobs()
        daemon.scheduler.start()
        try:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + 5.0
            while loop.time() < deadline:
                if daemon.catalog.watch_state_for(SOURCE) is not None:
                    return True
                await asyncio.sleep(0.05)
            return False
        finally:
            daemon.scheduler.shutdown(wait=False)

    try:
        populated = asyncio.run(_bring_up_and_wait())
    finally:
        daemon.connection.close()

    assert populated, (
        "watch_state was not populated within 5s of startup; the source's "
        "first fire was not brought forward to boot (cadence is 1h)"
    )


def test_immediate_fires_stay_bounded_by_scheduler_semaphore(
    tmp_path: Path,
) -> None:
    """The boot burst runs through the same _fire_source body as every fire, so
    the existing self.scheduler_semaphore still bounds it -- a fresh-boot fan-out
    of many sources cannot run unbounded. Pinned by confirming the immediate
    fire acquires that semaphore: held at zero, the fire blocks; released, it
    completes and writes the heartbeat.

    Discrimination: if a future change scheduled the boot fires off a path that
    bypassed scheduler_semaphore, the fire would complete while the semaphore is
    held and the `still empty while held` assertion would fail."""
    _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path, clock=FakeClock(PINNED))

    async def _drive() -> None:
        # Drain the semaphore to zero so any semaphore-bounded fire must wait.
        for _ in range(daemon.scheduler_semaphore._value):
            await daemon.scheduler_semaphore.acquire()
        fire = asyncio.create_task(daemon._fire_source(SOURCE))
        await asyncio.sleep(0.1)
        assert daemon.catalog.watch_state_for(SOURCE) is None, (
            "fire completed while the scheduler_semaphore was fully held -- "
            "the fire is not bounded by it"
        )
        daemon.scheduler_semaphore.release()
        await asyncio.wait_for(fire, timeout=5.0)
        assert daemon.catalog.watch_state_for(SOURCE) is not None, (
            "fire did not complete after the semaphore was released"
        )

    try:
        asyncio.run(_drive())
    finally:
        daemon.connection.close()
