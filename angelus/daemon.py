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

from angelus.control import ControlServer
from angelus.lodging import Lodging, ScheduledSource, load_lodging
from angelus.lodging.reloader import LodgingReloader
from angelus.pipes import PipeDrain
from angelus.sources import run_shell_source
from angelus.storage import Catalog, init_db, utcnow
from angelus.triage import run_python_triager

LOGGER = logging.getLogger(__name__)


class AngelusDaemon:
    def __init__(self, root: Path) -> None:
        self.root = root
        state_dir = root / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        # Owner-only. mkdir's mode is masked by umask and only applies on
        # creation; chmod unconditionally so an existing 0755 dir is tightened
        # too. The owner-only control socket lives here -- a world-traversable
        # parent dir is the other half of that exposure (issue-20260519-cd8z).
        state_dir.chmod(0o700)
        self.pid_file = state_dir / "angelus.pid"
        self.socket_path = state_dir / "angelus.sock"
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
        self.control = ControlServer(
            self.socket_path,
            {
                "health": self._op_health,
                "incident_list": self._op_incident_list,
                "mute_list": self._op_mute_list,
                "mute": self._op_mute,
                "incident_close": self._op_incident_close,
                "replay": self._op_replay,
                "reprocess": self._op_reprocess,
                "dep_record": self._op_dep_record,
            },
        )

    async def run(self) -> None:
        scheduler_started = False
        reloader_started = False
        control_started = False
        try:
            self.pid_file.write_text(str(os.getpid()), encoding="utf-8")
            await self.control.start()
            control_started = True
            LOGGER.info("control socket listening at %s", self.socket_path)
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
            if control_started:
                try:
                    await self.control.stop()
                except OSError:
                    LOGGER.warning(
                        "failed to stop control server", exc_info=True
                    )
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
            try:
                self.socket_path.unlink(missing_ok=True)
            except OSError:
                LOGGER.warning(
                    "failed to remove control socket %s",
                    self.socket_path,
                    exc_info=True,
                )
            self.connection.close()

    def request_stop(self) -> None:
        self.stop_event.set()

    async def _op_health(self, _args: dict) -> dict:
        """health control op. Runs on the daemon's event loop so it can read
        next-fire times off the live APScheduler (only the daemon knows
        these -- this is why health goes through the socket)."""
        last_fires = self.catalog.latest_source_fires()
        sources = []
        for ref in sorted(self.lodging.sources):
            job = self.scheduler.get_job(ref)
            next_fire = (
                job.next_run_time.isoformat()
                if job is not None and job.next_run_time is not None
                else None
            )
            sources.append(
                {
                    "name": ref,
                    "last_fire_at": last_fires.get(ref),
                    "next_fire_at": next_fire,
                }
            )
        return {
            "daemon": {"running": True, "pid": os.getpid()},
            "sources": sources,
            "queues": {
                "observations_pending_triage": (
                    self.catalog.observations_pending_triage_count()
                ),
                "findings_pending_dispatch": (
                    self.catalog.findings_pending_dispatch_by_pipe()
                ),
            },
            # Belfry is a separate external process (belfry/belfry.py); it
            # pings healthchecks.io and reads source_fires read-only but does
            # not persist a liveness timestamp anywhere angelus can read. No
            # storage is invented to fill this -- null until a real source
            # exists. Reported in the 5b-1 tender.
            "belfry": None,
            # dep_health's mandatory reader (slice 5c): every row the
            # dep_record write op upserts is surfaced here so a written dep
            # status is never dead config. Read-only SELECT.
            "deps": self.catalog.all_dep_health(),
        }

    async def _op_incident_list(self, _args: dict) -> dict:
        return {
            "open": self.catalog.open_incidents(),
            "recently_closed": self.catalog.recently_closed_incidents(days=7),
        }

    async def _op_mute_list(self, _args: dict) -> dict:
        """mute_list control op. A READ op -- a synchronous catalog
        SELECT of the active mutes, no write. Routed through the same
        socket as the write ops (owner-only perms gate the lot), but
        like health/incident_list it has a read-only sqlite fallback in
        the CLI when the daemon is down."""
        return {"active": self.catalog.active_mutes()}

    # The four write ops below run inside the daemon -- the single sqlite
    # writer. Each handler is synchronous in body: it validates args and
    # makes exactly one synchronous, self-committing catalog call. There is
    # deliberately no `await` between the catalog write and its commit, so
    # shutdown-cancellation can only land at the socket boundary, never with
    # a write transaction open (the 5b-1 cancel-safety property, preserved).
    # A ValueError raised here is caught by ControlServer._dispatch and
    # returned as {"ok": false, "error": ...}; it never crashes the daemon.

    async def _op_mute(self, args: dict) -> dict:
        dedup_key = args.get("dedup_key")
        duration = args.get("duration")
        comment = args.get("comment")
        if not isinstance(dedup_key, str) or not dedup_key:
            raise ValueError("mute requires a non-empty dedup_key")
        if not isinstance(duration, str) or not duration:
            raise ValueError("mute requires a duration")
        if comment is not None and not isinstance(comment, str):
            raise ValueError("mute comment must be a string")
        seconds = _mute_duration_seconds(duration)
        expires_at = self.catalog.add_mute(dedup_key, seconds, comment)
        return {"dedup_key": dedup_key, "expires_at": expires_at}

    async def _op_incident_close(self, args: dict) -> dict:
        incident_id = args.get("id")
        comment = args.get("comment")
        if not isinstance(incident_id, int) or isinstance(incident_id, bool):
            raise ValueError("incident close requires an integer id")
        if comment is not None and not isinstance(comment, str):
            raise ValueError("incident close comment must be a string")
        outcome = self.catalog.close_incident(incident_id, comment)
        return {"id": incident_id, "outcome": outcome}

    async def _op_replay(self, args: dict) -> dict:
        finding_id = args.get("finding_id")
        if not isinstance(finding_id, int) or isinstance(finding_id, bool):
            raise ValueError("replay requires an integer finding_id")
        return self.catalog.replay_finding(
            finding_id, set(self.lodging.pipes)
        )

    async def _op_reprocess(self, args: dict) -> dict:
        source = args.get("source")
        if not isinstance(source, str) or not source:
            raise ValueError("reprocess requires a non-empty source")
        count = self.catalog.reprocess_source(source)
        return {"source": source, "observations": count}

    async def _op_dep_record(self, args: dict) -> dict:
        """Record a dependency probe result (slice 5c).

        A WRITE op, same construction as the four 5b-2 ops above: async
        in signature only, no `await` in the body, so cancellation can
        only land at the socket boundary and never with a write
        transaction open. The dep-check cron probe never writes sqlite --
        it sends this op and the daemon (single writer) does the upsert.

        last_check_at is stamped here with utcnow() (the same ISO8601-UTC
        format helper the rest of the schema uses): the probe sends the
        result the instant its check finishes, so record time is the
        check time, and stamping daemon-side keeps one clock and avoids
        trusting a format from the unprivileged probe process.

        On status='unhealthy' an internal/dep finding is emitted to `now`
        AFTER the upsert (still no `await`). Each unhealthy record emits a
        fresh finding -- repeats are NOT deduped, mirroring the slice-3
        digest-failure contract: the operator keeps being told a
        dependency is down until it recovers.
        """
        name = args.get("name")
        status = args.get("status")
        detail = args.get("detail")
        if not isinstance(name, str) or not name:
            raise ValueError("dep_record requires a non-empty name")
        if status not in ("healthy", "unhealthy"):
            raise ValueError(
                "dep_record status must be 'healthy' or 'unhealthy'"
            )
        if detail is not None and not isinstance(detail, str):
            raise ValueError("dep_record detail must be a string")
        self.catalog.record_dep_health(name, status, utcnow(), detail)
        if status == "unhealthy":
            self.catalog.write_internal_finding(
                "internal/dep",
                "dependency_unhealthy",
                name,
                detail or "",
                set(self.lodging.pipes),
            )
        return {"name": name, "status": status}

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

        # A hot-removed dependency must not leave a frozen dep_health row.
        # Nothing else prunes dep_health, and an unlodged dependency can
        # never get another dep_record (the dep-check probe exits non-zero
        # for it), so the health op would surface a stale, unrecoverable
        # status forever. Prune here -- a synchronous, self-committing
        # catalog call with no await before its commit, so this stays on
        # the cancel-safe side of the reload like every other write.
        for name in set(old.dependencies) - set(new_lodging.dependencies):
            self.catalog.delete_dep_health(name)
            LOGGER.info("pruned dep_health for removed dependency %s", name)

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
                # Triager subprocesses (run_python_triager) await
                # process.communicate() and have no external canceller
                # (APScheduler cancels source-fire and pipe-digest tasks
                # but not these). Without cancelling here, a triager
                # stuck waiting on its subprocess hangs shutdown until
                # the triager's own timeout_seconds fires. Cancel first,
                # then gather: each task's CancelledError arm runs
                # _kill_and_reap on the subprocess (triage/runner.py),
                # so the subprocess tree is reaped before we return.
                for task in in_flight:
                    task.cancel()
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
                self._clear_triage_for_removed_triager(int(row["id"]), triager_name)
                return
            lock_key = (triager.name, triager.source_ref)
            lock = self.triager_locks.setdefault(lock_key, asyncio.Lock())
            async with lock:
                await self._run_triager(row, triager_name)

    def _clear_triage_for_removed_triager(
        self, observation_id: int, triager_name: str
    ) -> None:
        self.catalog.clear_triage_processing(observation_id, triager_name)
        LOGGER.info(
            "triager %s removed mid-flight; cleared processing row for observation %d",
            triager_name,
            observation_id,
        )

    async def _run_triager(self, row, triager_name: str) -> None:
        triager = self.lodging.triagers.get(triager_name)
        if triager is None:
            # Triager hot-removed while a sibling task held the per-triager
            # lock; same orphan risk as the pre-lock check above.
            self._clear_triage_for_removed_triager(int(row["id"]), triager_name)
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


# Deliberately separate from _CADENCE_UNITS / _cadence_seconds. Mute
# durations are an operator-facing alert-silencing grammar with a 'd'
# (days) unit; scheduling cadence is a different domain with no 'd'.
# Entangling the two parsers would couple unrelated concerns, so this is
# its own grammar with its own units.
_MUTE_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _mute_duration_seconds(duration: str) -> int:
    """Parse a mute duration like '90s', '30m', '4h', '2d'.

    A unit suffix (s/m/h/d) is required: a bare integer is rejected so
    `mute <key> 30` cannot silently mean 30 of some unit (the same
    silent-units footgun the cadence parser refuses). Non-positive
    magnitudes are rejected too.
    """
    text = duration.strip().lower()
    for suffix, scale in _MUTE_DURATION_UNITS.items():
        if text.endswith(suffix) and len(text) > len(suffix):
            value = text[: -len(suffix)].strip()
            try:
                magnitude = int(value)
            except ValueError:
                raise ValueError(
                    f"invalid mute duration {duration!r}: "
                    "expected <int><unit> (s, m, h, d)"
                ) from None
            if magnitude <= 0:
                raise ValueError(
                    f"invalid mute duration {duration!r}: must be positive"
                )
            return magnitude * scale
    raise ValueError(
        f"invalid mute duration {duration!r}: expected a unit suffix "
        "(s, m, h, d), e.g. '30m'"
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
