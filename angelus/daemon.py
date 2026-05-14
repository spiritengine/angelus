"""Angelus daemon for the slice-1 vertical path."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from angelus.lodging import Lodging, ScheduledSource, load_lodging
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
        self.stop_event = asyncio.Event()
        self.tasks: list[asyncio.Task[None]] = []
        self.pipe_drains = {
            name: PipeDrain(self.catalog, pipe, self.lodging.channels, root)
            for name, pipe in self.lodging.pipes.items()
        }

    async def run(self) -> None:
        scheduler_started = False
        self.pid_file.write_text(str(os.getpid()), encoding="utf-8")
        try:
            LOGGER.info(
                "loaded lodging: %d sources, %d triagers, %d pipes, %d channels",
                len(self.lodging.sources),
                len(self.lodging.triagers),
                len(self.lodging.pipes),
                len(self.lodging.channels),
            )
            self._install_signal_handlers()
            self._register_sources()
            self.scheduler.start()
            scheduler_started = True
            LOGGER.info("scheduler started with %d jobs", len(self.scheduler.get_jobs()))
            self.tasks.append(asyncio.create_task(self._triage_loop(), name="triage-loop"))
            for pipe_name in self.pipe_drains:
                self.tasks.append(
                    asyncio.create_task(self._pipe_loop(pipe_name), name=f"pipe-{pipe_name}")
                )
            await self.stop_event.wait()
            LOGGER.info("shutdown requested")
        finally:
            if scheduler_started:
                self.scheduler.shutdown(wait=True)
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
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

    def _register_sources(self) -> None:
        for source in self.lodging.sources.values():
            trigger = IntervalTrigger(seconds=_cadence_seconds(source.cadence))
            self.scheduler.add_job(
                self._fire_source,
                trigger,
                args=[source],
                id=source.source_ref,
                max_instances=1,
                coalesce=True,
            )
            LOGGER.info(
                "registered scheduled source %s every %s",
                source.source_ref,
                source.cadence,
            )

    async def _fire_source(self, source: ScheduledSource) -> None:
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
            await self._run_triager(row, triager_name)

    async def _run_triager(self, row, triager_name: str) -> None:
        triager = self.lodging.triagers[triager_name]
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
            self.catalog.mark_triage_failed(observation_id, triager.name, str(exc))

    async def _pipe_loop(self, pipe_name: str) -> None:
        drain = self.pipe_drains[pipe_name]
        while not self.stop_event.is_set():
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

    Cron expressions are not yet supported (slice 3). A unit suffix is required —
    bare integers are rejected so 'cadence: 15' cannot silently mean 15 seconds.
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
        f"invalid cadence {cadence!r}: expected unit suffix (s, m, h); "
        "cron expressions land in slice 3"
    )


def main(root: Path | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(AngelusDaemon(root or Path.cwd()).run())
