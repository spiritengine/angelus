"""Angelus daemon for the slice-1 vertical path."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from angelus.lodging import Lodging, ScheduledSource, load_lodging
from angelus.lodging.reloader import LodgingReloader
from angelus.pipes import PipeDrain
from angelus.sources import run_shell_source
from angelus.storage import Catalog, init_db
from angelus.triage import run_python_triager

LOGGER = logging.getLogger(__name__)


class AngelusDaemon:
    def __init__(self, root: Path) -> None:
        self.root = root
        state_dir = root / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        self.pid_file = state_dir / "angelus.pid"
        self.connection = init_db(state_dir / "angelus.sqlite3")
        self.catalog = Catalog(self.connection, root)
        self.lodging: Lodging = load_lodging(root)
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.scheduler_semaphore = asyncio.Semaphore(10)
        self.triage_semaphore = asyncio.Semaphore(10)
        self.triager_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self.stop_event = asyncio.Event()
        self.tasks: list[asyncio.Task[None]] = []
        # Sole tracking site for per-pipe immediate-cadence loop tasks. Kept
        # separate from self.tasks so apply_lodging can cancel one pipe's loop
        # on cadence change without disturbing others, and so cancelled tasks
        # don't accumulate in self.tasks across pipe churn.
        self._pipe_loop_tasks: dict[str, asyncio.Task[None]] = {}
        self.pipe_drains: dict[str, PipeDrain] = {
            name: PipeDrain(
                self.catalog, pipe, self.lodging.channels, root, set(self.lodging.pipes)
            )
            for name, pipe in self.lodging.pipes.items()
        }
        self.reloader = LodgingReloader(self, root)

    async def run(self) -> None:
        scheduler_started = False
        reloader_started = False
        try:
            self.pid_file.write_text(str(os.getpid()), encoding="utf-8")
            LOGGER.info(
                "loaded lodging: %d sources, %d triagers, %d pipes, %d channels",
                len(self.lodging.sources),
                len(self.lodging.triagers),
                len(self.lodging.pipes),
                len(self.lodging.channels),
            )
            self._install_signal_handlers()
            recovered, failed = self.catalog.recover_writing_rows()
            LOGGER.info("startup recovery: %d ready, %d failed", recovered, failed)
            # Channels stay unhealthy only until daemon restart (slice 2 scope).
            self.catalog.clear_channel_health()
            self._register_initial_jobs()
            self.scheduler.start()
            scheduler_started = True
            LOGGER.info("scheduler started with %d jobs", len(self.scheduler.get_jobs()))
            self.tasks.append(asyncio.create_task(self._triage_loop(), name="triage-loop"))
            for pipe_name, pipe in self.lodging.pipes.items():
                if pipe.cadence != "immediate":
                    continue
                self._spawn_pipe_loop(pipe_name)
            self.reloader.start()
            reloader_started = True
            await self.stop_event.wait()
            LOGGER.info("shutdown requested")
        finally:
            if reloader_started:
                await self.reloader.stop()
            if scheduler_started:
                self.scheduler.shutdown(wait=True)
            pending = [*self.tasks, *self._pipe_loop_tasks.values()]
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            try:
                self.pid_file.unlink(missing_ok=True)
            except OSError:
                LOGGER.warning("failed to remove PID file %s", self.pid_file, exc_info=True)
            self.connection.close()

    def request_stop(self) -> None:
        self.stop_event.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.request_stop)

    def _register_initial_jobs(self) -> None:
        for source in self.lodging.sources.values():
            self._add_source_job(source)
        for pipe in self.lodging.pipes.values():
            if pipe.cadence == "immediate":
                continue
            self._add_pipe_job(pipe.name, pipe.cadence)

    def _add_source_job(self, source: ScheduledSource) -> None:
        trigger = _make_trigger(source.cadence)
        self.scheduler.add_job(
            self._fire_source,
            trigger,
            args=[source.source_ref],
            id=source.source_ref,
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        LOGGER.info(
            "registered scheduled source %s on %s",
            source.source_ref,
            source.cadence,
        )

    def _add_pipe_job(self, pipe_name: str, cadence: str) -> None:
        trigger = _make_trigger(cadence)
        self.scheduler.add_job(
            self.pipe_drains[pipe_name].drain_once,
            trigger,
            id=f"pipe:{pipe_name}",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        LOGGER.info("registered pipe %s on %s", pipe_name, cadence)

    def _remove_job(self, job_id: str) -> None:
        try:
            self.scheduler.remove_job(job_id)
        except JobLookupError:
            pass

    async def apply_lodging(self, new_lodging: Lodging) -> None:
        """Atomically swap in a new Lodging snapshot. Adjusts scheduler jobs
        for source/pipe add/remove/cadence-change, swaps Pipe objects on
        existing PipeDrain instances, and re-points every drain at the new
        channels and known_pipes."""
        old = self.lodging
        self.lodging = new_lodging

        old_sources = old.sources
        new_sources = new_lodging.sources
        for ref in set(old_sources) - set(new_sources):
            self._remove_job(ref)
            LOGGER.info("unregistered scheduled source %s", ref)
        for ref in set(new_sources) & set(old_sources):
            if old_sources[ref].cadence != new_sources[ref].cadence:
                self._remove_job(ref)
                self._add_source_job(new_sources[ref])
        for ref in set(new_sources) - set(old_sources):
            self._add_source_job(new_sources[ref])

        old_pipes = old.pipes
        new_pipes = new_lodging.pipes
        new_known = set(new_pipes)
        for name in set(old_pipes) - set(new_pipes):
            if old_pipes[name].cadence == "immediate":
                await self._cancel_pipe_loop(name)
            else:
                self._remove_job(f"pipe:{name}")
            self.pipe_drains.pop(name, None)
            LOGGER.info("unregistered pipe %s", name)
        for name in set(new_pipes) & set(old_pipes):
            new_pipe = new_pipes[name]
            old_pipe = old_pipes[name]
            drain = self.pipe_drains[name]
            drain.pipe = new_pipe
            if old_pipe.cadence != new_pipe.cadence:
                if old_pipe.cadence == "immediate":
                    await self._cancel_pipe_loop(name)
                else:
                    self._remove_job(f"pipe:{name}")
                if new_pipe.cadence == "immediate":
                    self._spawn_pipe_loop(name)
                else:
                    self._add_pipe_job(name, new_pipe.cadence)
        for name in set(new_pipes) - set(old_pipes):
            new_pipe = new_pipes[name]
            self.pipe_drains[name] = PipeDrain(
                self.catalog,
                new_pipe,
                new_lodging.channels,
                self.root,
                new_known,
            )
            if new_pipe.cadence == "immediate":
                self._spawn_pipe_loop(name)
            else:
                self._add_pipe_job(name, new_pipe.cadence)

        for drain in self.pipe_drains.values():
            drain.channels = new_lodging.channels
            drain.known_pipes = new_known

    def _spawn_pipe_loop(self, pipe_name: str) -> None:
        task = asyncio.create_task(self._pipe_loop(pipe_name), name=f"pipe-{pipe_name}")
        self._pipe_loop_tasks[pipe_name] = task

    async def _cancel_pipe_loop(self, pipe_name: str) -> None:
        task = self._pipe_loop_tasks.pop(pipe_name, None)
        if task is None:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    async def _fire_source(self, source_ref: str) -> None:
        source = self.lodging.sources.get(source_ref)
        if source is None:
            LOGGER.info("scheduled source %s vanished before fire", source_ref)
            return
        async with self.scheduler_semaphore:
            ok, payload = await run_shell_source(source)
            outcome = "ok" if ok else "check_failed"
            # scheduled_at left NULL: APScheduler does not pass planned-fire time
            # into the job body. Belt overdue (slice 2) reads fired_at; belfry
            # wedge detection (slice 4) reads fired_at. Wire the planned time
            # via a job listener if a real consumer appears.
            self.catalog.record_source_fire(source.source_ref, None, outcome)
            if ok:
                observation_id = self.catalog.write_observation(
                    source.source_ref,
                    payload,
                    {"source": source.source_ref, "check": "shell"},
                )
                LOGGER.info("observation %s ready for %s", observation_id, source.source_ref)
            else:
                observation_id = self.catalog.write_observation(
                    source.source_ref,
                    {"type": "check_failed", **payload},
                    {"source": source.source_ref, "check": "shell", "outcome": outcome},
                )
                LOGGER.warning("source %s failed: %s", source.source_ref, payload)

    async def _triage_loop(self) -> None:
        in_flight: set[asyncio.Task[None]] = set()
        try:
            while not self.stop_event.is_set():
                self._reap_triage_tasks(in_flight)
                did_work = False
                for triager in self.lodging.triagers.values():
                    rows = self.catalog.ready_observations_for(
                        triager.name, triager.source_ref
                    )
                    for row in rows:
                        did_work = True
                        self.catalog.mark_triage_processing(row["id"], triager.name)
                        task = asyncio.create_task(
                            self._triage_under_semaphore(row, triager.name)
                        )
                        in_flight.add(task)
                await asyncio.sleep(0.1 if did_work or in_flight else 1)
        finally:
            if in_flight:
                await asyncio.gather(*in_flight, return_exceptions=True)
                self._reap_triage_tasks(in_flight)

    def _reap_triage_tasks(self, in_flight: set[asyncio.Task[None]]) -> None:
        for task in [t for t in in_flight if t.done()]:
            in_flight.discard(task)
            exc = task.exception()
            if exc is not None:
                LOGGER.error("triage task crashed", exc_info=exc)

    async def _triage_under_semaphore(self, row, triager_name: str) -> None:
        async with self.triage_semaphore:
            triager = self.lodging.triagers.get(triager_name)
            if triager is None:
                # Triager hot-removed between mark_triage_processing and now.
                # The 'processing' row would otherwise orphan: ready_observations_for
                # excludes it, and recover_writing_rows doesn't touch
                # observation_triage. Delete so a later re-add of the same
                # triager can pick the observation up fresh; no attempt
                # consumed since the triager never ran.
                observation_id = int(row["id"])
                self.catalog.clear_triage_processing(observation_id, triager_name)
                LOGGER.info(
                    "triager %s removed mid-flight; cleared processing row for observation %d",
                    triager_name,
                    observation_id,
                )
                return
            lock_key = (triager.name, triager.source_ref)
            lock = self.triager_locks.setdefault(lock_key, asyncio.Lock())
            async with lock:
                await self._run_triager(row, triager_name)

    async def _run_triager(self, row, triager_name: str) -> None:
        triager = self.lodging.triagers.get(triager_name)
        if triager is None:
            return
        observation_id = int(row["id"])
        try:
            observation = self.catalog.read_body(row["body_ref"])
            prior_state = self.catalog.prior_state(triager.name, triager.source_ref)
            findings, new_state = await run_python_triager(
                triager, observation, prior_state
            )
            self.catalog.update_triager_state(
                triager.name, triager.source_ref, new_state
            )
            for finding in findings:
                finding_id = self.catalog.write_finding(
                    observation_id, finding, set(self.lodging.pipes)
                )
                LOGGER.info("finding %s ready from observation %s", finding_id, observation_id)
            self.catalog.mark_triage_success(observation_id, triager.name)
        except Exception as exc:
            LOGGER.exception("triage failed for observation %s", observation_id)
            exhausted = self.catalog.mark_triage_failed(
                observation_id, triager.name, str(exc)
            )
            if exhausted:
                self.catalog.write_internal_finding(
                    "internal/triage",
                    "triage_failed",
                    triager.name,
                    str(exc),
                    set(self.lodging.pipes),
                )

    async def _pipe_loop(self, pipe_name: str) -> None:
        while not self.stop_event.is_set():
            drain = self.pipe_drains.get(pipe_name)
            if drain is None:
                return
            await drain.drain_once()
            await asyncio.sleep(1)


_CADENCE_UNITS = {
    "s": 1,
    "sec": 1,
    "m": 60,
    "min": 60,
    "h": 3600,
    "hr": 3600,
}


def _cadence_seconds(cadence: str) -> int:
    """Parse interval cadence strings like '15m', '30s', '2h'.

    A unit suffix is required so 'cadence: 15' cannot silently mean 15 seconds.
    """
    text = cadence.strip().lower()
    for suffix in sorted(_CADENCE_UNITS, key=len, reverse=True):
        if text.endswith(suffix):
            value = text[: -len(suffix)].strip()
            try:
                magnitude = int(value)
            except ValueError as exc:
                raise ValueError(f"invalid cadence {cadence!r}: {exc}") from None
            if magnitude <= 0:
                raise ValueError(f"invalid cadence {cadence!r}: must be positive")
            return magnitude * _CADENCE_UNITS[suffix]
    raise ValueError(
        f"invalid cadence {cadence!r}: expected unit suffix (s, m, h)"
    )


def _make_trigger(cadence: str):
    """Build an APScheduler trigger from an interval or crontab cadence."""
    if any(char.isspace() for char in cadence.strip()):
        return CronTrigger.from_crontab(cadence)
    return IntervalTrigger(seconds=_cadence_seconds(cadence))


def main(root: Path | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(AngelusDaemon(root or Path.cwd()).run())
