"""Angelus daemon for the slice-1 vertical path."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from datetime import UTC, datetime
from pathlib import Path

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from angelus.clock import Clock
from angelus.control import ControlServer
from angelus.envfile import load_env_file, resolve_op_refs
from angelus.fixers.runner import run_python_fixer
from angelus.logging_config import configure_logging
from angelus.lodging import (
    Fixer,
    FixerCondition,
    Lodging,
    ScheduledSource,
    load_lodging,
    missing_channel_config,
)
from angelus.lodging.reloader import LodgingReloader
from angelus.pipes import PipeDrain
from angelus.sources import run_shell_source
from angelus.storage import Catalog, init_db
from angelus.triage import run_python_triager

LOGGER = logging.getLogger(__name__)

DEFAULT_BELFRY_SENTINEL_FILENAME = "belfry-pinged-at"
DEFAULT_BELFRY_STALE_AFTER_SEC = 1200

# Recent-window for the health surface's failed-dispatch count (B5). A nonzero
# count over this window says "delivery is actively breaking now", distinct
# from the open-internal-incident tally (which can persist across the window).
HEALTH_FAILED_DISPATCH_WINDOW_HOURS = 24

# Internal incident sources reconciled at daemon startup. Each recovers only
# off a live edge a restart can skip, so an incident open across a restart
# orphans and the B30 gate then suppresses the next genuine failure. See
# AngelusDaemon._reconcile_orphaned_internal_incidents for the per-source
# justification (and why internal/render is the weakest of the three).
#
# The other two internal sources are DELIBERATELY excluded: internal/dep
# persists its unhealthy state in dep_health across a restart (nothing wipes
# that table at boot) and recovers off an external dep_record push the restart
# does not skip -- an open dep incident is consistent, not orphaned.
# internal/triage recovers off the next observation the triager handles, which
# a restart does not skip while the source keeps firing -- so it is not blind-
# cleared (which would reintroduce the false-green render accepts). The one
# residual: a one-shot / removed / very-long-cadence triager whose observation
# went terminal can orphan like render; bounded by source cadence, left as-is.
_RESTART_RECONCILED_INTERNAL_SOURCES = (
    "internal/lodging",
    "internal/dispatch",
    "internal/render",
)

# Hard ceiling on awaiting cancelled digest-drain tasks during shutdown.
# Set above _kill_and_reap's own _REAP_TIMEOUT (5.0s) so a drain cancelled
# mid-render gets to run its reap arm to completion, while a genuinely
# wedged drain still cannot hang shutdown past this bound. Kept comfortably
# under the integration fell's 8.0s no-hang assertion.
_DRAIN_SHUTDOWN_TIMEOUT = 6.0

# How often the fixer-evaluation loop (B11) re-checks live conditions. The
# guardrails (max_attempts/window/backoff) -- not this interval -- bound how
# often any single fixer actually fires, so this only needs to be frequent
# enough that a remediable condition is acted on promptly. Overridable for
# tests/alternate deployments; tests drive _evaluate_fixers directly and do
# not depend on it.
_FIXER_POLL_INTERVAL_SEC = 15.0
DEFAULT_FIXERS_LOG_FILENAME = "fixers.log"


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
        # Single real clock for the process (B24). Threaded into the catalog
        # and every PipeDrain so all timestamp/window logic shares one notion
        # of "now"; a sim/test build swaps this for a FakeClock. apscheduler
        # keeps real time (B25 handles forcing work without time-travelling
        # the scheduler).
        self.clock = Clock()
        self.catalog = Catalog(self.connection, root, clock=self.clock)
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
        # In-flight non-immediate (cron/interval) digest-drain job tasks.
        # AsyncIOExecutor.shutdown() cancels these on shutdown but does not
        # await them, and they are not in `pending` -- so a cancelled drain's
        # reap arm (_render_llm_body -> _kill_and_reap) would race event-loop
        # teardown and orphan the horizon cast subtree. Each drain registers
        # its task here on entry and discards on exit; run()'s finally cancels
        # and awaits the set. Mirrors _triage_loop's in_flight handling.
        self._drain_tasks: set[asyncio.Task[None]] = set()
        # The fixer-evaluation loop (B11). Held separately from self.tasks
        # because, unlike the triage loop (whose body only awaits sleep and
        # spawns child tasks), this loop blocks INLINE on a fixer handler
        # subprocess -- so shutdown must CANCEL it, not merely await it, or a
        # slow handler hangs teardown. Cancellation propagates into
        # run_python_fixer's reap arm. None until run() starts it.
        self._fixer_loop_task: asyncio.Task[None] | None = None
        self.pipe_drains: dict[str, PipeDrain] = {
            name: PipeDrain(
                self.catalog,
                pipe,
                self.lodging.channels,
                root,
                set(self.lodging.pipes),
                clock=self.clock,
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
                "loaded lodging: %d sources, %d triagers, %d pipes, "
                "%d channels, %d fixers",
                len(self.lodging.sources),
                len(self.lodging.triagers),
                len(self.lodging.pipes),
                len(self.lodging.channels),
                len(self.lodging.fixers),
            )
            # Log the resolved local TZ at startup. The digest subject and
            # all rendered timestamps use the clock's local-now (system TZ).
            # If the daemon ever runs in a
            # container without tzdata or with `Environment=TZ=UTC`, the
            # digest silently shifts a calendar day -- fell-r1 CONSIDER #2
            # for the email-cleanup pass. A startup log line surfaces the
            # misconfig in journalctl without adding a config knob.
            _local_now = self.clock.now_local()
            LOGGER.info(
                "resolved display timezone: %s (current local: %s)",
                _local_now.tzinfo,
                # rstrip in case the resolved TZ has no abbreviation
                # (slim container without tzdata): %Z renders empty and
                # would leave a trailing space in journalctl.
                _local_now.strftime("%Y-%m-%d %H:%M %Z").rstrip(),
            )
            self._install_signal_handlers()
            recovered, failed = self.catalog.recover_writing_rows()
            # Same round-5 orphan class as the in-process graceful-cancel
            # arm in _triage_under_semaphore, but for the hard-exit axis
            # (SIGKILL / host crash bypass Python shutdown handlers, so
            # the in-process arm never fires). Clears any
            # observation_triage row left at 'processing' from the prior
            # daemon. ready_observations_for would otherwise exclude the
            # observation forever.
            triage_orphans = self.catalog.recover_triage_processing_rows()
            LOGGER.info(
                "startup recovery: %d ready, %d failed, %d triage orphans cleared",
                recovered,
                failed,
                triage_orphans,
            )
            # Channels stay unhealthy only until daemon restart (slice 2 scope).
            self.catalog.clear_channel_health()
            # Same restart-scope for the per-channel digest attempt counter
            # that feeds the channel_health threshold ladder on the digest
            # path -- if a daemon restart wipes channel_health, leaving the
            # counter populated would let a single subsequent failure cross
            # the threshold immediately on the new generation.
            self.catalog.clear_digest_channel_attempts()
            # And the same for the immediate path's per-channel attempt counter
            # (B7 fell-r1 Finding 3): it feeds the identical channel_health
            # ladder for _drain_immediate, so it must reset alongside
            # channel_health for restart-scope parity -- otherwise a populated
            # counter from the prior generation would cross threshold on the
            # first post-restart failure.
            self.catalog.clear_immediate_channel_attempts()
            self._reconcile_orphaned_internal_incidents()
            self._validate_channel_config()
            self._sync_pipe_sla()
            self._register_initial_jobs()
            self.scheduler.start()
            scheduler_started = True
            LOGGER.info("scheduler started with %d jobs", len(self.scheduler.get_jobs()))
            self.tasks.append(asyncio.create_task(self._triage_loop(), name="triage-loop"))
            self._fixer_loop_task = asyncio.create_task(
                self._fixer_loop(), name="fixer-loop"
            )
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
            # AsyncIOExecutor.shutdown() above cancels in-flight digest-drain
            # job tasks but does not await them, and they are not in `pending`.
            # Cancel (idempotent) and await them here so each cancelled drain's
            # reap arm (_render_llm_body -> _kill_and_reap) runs before the loop
            # closes -- otherwise the horizon cast subtree is orphaned. Same
            # cancel-then-gather shape _triage_loop uses for its in-flight
            # tasks; bounded by wait_for so a wedged drain cannot hang shutdown
            # forever (the reap itself is already bounded at _REAP_TIMEOUT).
            if self._drain_tasks:
                in_flight = list(self._drain_tasks)
                for task in in_flight:
                    task.cancel()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*in_flight, return_exceptions=True),
                        timeout=_DRAIN_SHUTDOWN_TIMEOUT,
                    )
                except TimeoutError:
                    LOGGER.warning(
                        "drain task shutdown exceeded %.1fs; %d still in-flight",
                        _DRAIN_SHUTDOWN_TIMEOUT,
                        len(self._drain_tasks),
                    )
            # Cancel the fixer loop before the final gather. Its body blocks
            # inline on a handler subprocess (run_python_fixer.communicate),
            # which setting stop_event does not interrupt; cancelling lands in
            # that runner's `except CancelledError: await _kill_and_reap`, so
            # the handler's process group is reaped instead of hanging
            # teardown until its timeout. Bounded by the same budget as the
            # drain reap so a wedged handler still can't hang shutdown past it
            # (and under the integration fell's 8.0s no-hang assertion).
            if self._fixer_loop_task is not None:
                self._fixer_loop_task.cancel()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(
                            self._fixer_loop_task, return_exceptions=True
                        ),
                        timeout=_DRAIN_SHUTDOWN_TIMEOUT,
                    )
                except TimeoutError:
                    LOGGER.warning(
                        "fixer loop shutdown exceeded %.1fs",
                        _DRAIN_SHUTDOWN_TIMEOUT,
                    )
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

    def _reconcile_orphaned_internal_incidents(self) -> None:
        """Clear internal incidents left open across a restart.

        Internal incidents recover off LIVE edges (a filesystem change, a
        successful render, a successful send) -- never at startup. So an
        incident opened before a restart can orphan: the recovery edge that
        would close it fires only from a later event a restart can skip, and
        the B30 gate then silently suppresses the next genuine failure of that
        (source, entity). This sweep reconciles the restart-scoped internal
        sources after the startup state is re-established:

        - internal/lodging: load_lodging succeeded in __init__, which PROVES
          every watched lodging file is currently valid (the original FIX 1
          case: a file fixed while the daemon was DOWN emits no change event,
          so the reloader's change-driven _clear_rejection never fires).
        - internal/dispatch: clear_channel_health() just reset every channel to
          healthy (channel health is restart-scoped, slice 2), so an open
          channel_unhealthy incident is now inconsistent and orphaned. The next
          send re-opens it if the channel is still broken -- fast when the
          channel is on the `now` pipe; a digest-only channel re-verifies at the
          next daily digest (the inconsistency-with-reset-health is the
          justification regardless of re-verify speed).
        - internal/render: a digest render failure from a now-replaced
          execution context (e.g. incident 10 -- a pre-E2BIG-fix render that
          stayed open across the deploy restart). This is the weakest claim:
          the render is NOT re-verified at startup, so it is cleared on the bet
          that a restart is a clean slate, and the next digest re-opens it if
          the render is still broken (up to one cadence later). A brief
          false-green is preferred over a stale incident keeping belfry red and
          masking every OTHER signal until the once-daily digest runs.

        Each closes through write_internal_clearance -- the same path the live
        recovery edges use, so recent_closures stays correct and the gate
        re-arms. Gate-safe and idempotent: a no-op for any (source, entity)
        with nothing open. Must run AFTER clear_channel_health so the dispatch
        reconcile matches the freshly-reset health.
        """
        known_pipes = set(self.lodging.pipes)
        reconciled: dict[str, int] = {}
        for incident in self.catalog.open_incidents():
            source = incident["source"]
            if source not in _RESTART_RECONCILED_INTERNAL_SOURCES:
                continue
            self.catalog.write_internal_clearance(
                source,
                incident["entity"],
                f"{incident['entity']} reconciled at startup",
                known_pipes,
            )
            reconciled[source] = reconciled.get(source, 0) + 1
        if reconciled:
            LOGGER.info(
                "startup recovery: reconciled orphaned internal incidents (%s)",
                ", ".join(f"{s}={n}" for s, n in sorted(reconciled.items())),
            )

    def _validate_channel_config(self) -> None:
        """B18: a misconfigured daemon must not come up silently healthy.

        Validates that every channel a pipe routes to has its required env
        config present (domain-agnostic: derived from each channel's
        `$env:NAME` markers via missing_channel_config, so nothing about
        email or a specific var is hardcoded).

        Degraded-mode-and-alarm, NOT refuse-to-start. The systemd unit is
        Restart=on-failure/RestartSec=5 (deploy/angelus.service), so a
        nonzero exit on missing config would crash-loop every 5s and never
        reach a live transport -- the brief's named exception to
        refuse-to-start. Instead the daemon comes up, logs ERROR, and opens a
        high-severity internal/config incident routed to `now`, which is
        push-only (B6) -- deliberately off the very email transport a missing
        ANGELUS_EMAIL_TO would break (don't-share-fate).

        Edge-triggered and self-reconciling: every referenced channel whose
        config IS present fires write_internal_clearance, which is a no-op
        unless an incident is open and otherwise closes it -- so a config
        fixed while the daemon was down clears on the next startup and the B30
        emission gate re-arms. The audit guard in test_b30_emission_gate.py
        enforces that this clearance exists for the internal/config source.

        Runs AFTER _reconcile_orphaned_internal_incidents and before the now
        pipe loop spawns; the finding lands in the pipe queue and drains to
        push once that loop starts.
        """
        known_pipes = set(self.lodging.pipes)
        missing = missing_channel_config(self.lodging)
        referenced = sorted(
            {channel for pipe in self.lodging.pipes.values() for channel in pipe.channels}
        )
        for name in referenced:
            if name not in self.lodging.channels:
                # A pipe referencing an absent channel is a cross-ref error
                # load_lodging already raised on; never reached at runtime.
                continue
            if name in missing:
                detail = ", ".join(missing[name])
                LOGGER.error(
                    "channel %s missing required config (%s); starting in "
                    "degraded mode -- dispatches over this channel will fail",
                    name,
                    detail,
                )
                # The incident body is the open-EDGE snapshot: under the B30
                # gate write_internal_finding only persists a row when it opens
                # a NEW incident, so a later partial fix (a channel needing two
                # $env vars, one now set) does not rewrite the body -- it would
                # still list both. That is intentional: the incident stays open
                # (correct -- the channel is still degraded) and the live ERROR
                # line above always carries the currently-missing set, so the
                # operator never reads a stale specifics list as live. Only the
                # full fix closes the incident, via the clearance branch below.
                self.catalog.write_internal_finding(
                    "internal/config",
                    "channel_config_missing",
                    name,
                    f"channel {name!r} missing required env: {detail}",
                    known_pipes,
                )
            else:
                self.catalog.write_internal_clearance(
                    "internal/config",
                    name,
                    f"channel {name!r} required config present",
                    known_pipes,
                )

    def _sync_pipe_sla(self) -> None:
        """B2: persist each pipe's declared delivery SLA to sqlite so belfry --
        the out-of-band, pure-stdlib layer -- can read the contract and assert
        the pipe is actually delivering on cadence.

        Only pipes that declare `max_interval` are tracked; the immediate `now`
        pipe (no cadence to lapse against) opts out by leaving it unset.
        Reconciles the whole set so a removed/reclassified pipe's stale row is
        dropped. The belfry SLA check is the on-box, all-pipes generalization
        of the off-box digest dead-man.
        """
        slas = {
            name: pipe.max_interval_seconds
            for name, pipe in self.lodging.pipes.items()
            if pipe.max_interval_seconds is not None
        }
        self.catalog.sync_pipe_sla(slas)
        if slas:
            LOGGER.info(
                "pipe delivery SLAs tracked: %s",
                ", ".join(
                    f"{name}={seconds}s" for name, seconds in sorted(slas.items())
                ),
            )

    def request_stop(self) -> None:
        self.stop_event.set()

    async def _op_health(self, _args: dict) -> dict:
        """health control op. Runs on the daemon's event loop so it can read
        next-fire times off the live APScheduler (only the daemon knows
        these -- this is why health goes through the socket)."""
        last_fires = self.catalog.latest_source_fires()
        deps = self.catalog.all_dep_health()
        for dep in deps:
            if dep["status"] != "unhealthy":
                continue
            mute = self.catalog.active_mute_for(
                f"internal/dep:dependency_unhealthy:{dep['dependency_name']}"
            )
            if mute is not None:
                dep["mute"] = {
                    "until": mute["expires_at"],
                    "comment": mute["comment"],
                }
        unhealthy_deps = {
            dep["dependency_name"] for dep in deps if dep["status"] == "unhealthy"
        }
        sources = []
        for ref in sorted(self.lodging.sources):
            source = self.lodging.sources[ref]
            job = self.scheduler.get_job(ref)
            next_fire = (
                job.next_run_time.isoformat()
                if job is not None and job.next_run_time is not None
                else None
            )
            blocked_by = [
                dependency_name
                for dependency_name in source.depends_on
                if dependency_name in unhealthy_deps
            ]
            sources.append(
                {
                    "name": ref,
                    "last_fire_at": last_fires.get(ref),
                    "next_fire_at": next_fire,
                    "blocked_by_unhealthy_deps": blocked_by,
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
            # Belfry is a separate external process (belfry/belfry.py) that
            # cannot write sqlite (single-writer-to-sqlite invariant). On each
            # cron tick belfry touches a sentinel file; the daemon reads its
            # mtime here. Missing file -> never-pinged shape. Per Section 5b
            # Q2 of brief-20260520-tqov.
            "belfry": _belfry_status(self.root, self.clock),
            # dep_health's mandatory reader (slice 5c): every row the
            # dep_record write op upserts is surfaced here so a written dep
            # status is never dead config. Read-only SELECT.
            "deps": deps,
            # Operator-facing channel rail: channel_health stays visible even
            # if the corresponding internal/dispatch finding is muted on the
            # now pipe, and the digest retry ladder is surfaced before the
            # unhealthy threshold is crossed.
            "channels": {
                "health": self.catalog.all_channel_health(),
                "attempts": self.catalog.digest_channel_attempts(),
                # Immediate-path per-channel ladder, surfaced alongside the
                # digest ladder so a channel climbing toward unhealthy on the
                # _drain_immediate path is visible before channel_health flips
                # (B7 fell-r1 Finding 3). Read-only SELECT.
                "immediate_attempts": self.catalog.immediate_channel_attempts(),
            },
            # Delivery surface (B5): is each pipe actually getting content out,
            # how many dispatches failed recently, and how many of angelus's own
            # failures are open. The "is it WORKING" answer the 2026-05-29
            # incident proved liveness alone does not give.
            "delivery": _delivery_surface(
                self.catalog, list(self.lodging.pipes)
            ),
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

        last_check_at is stamped here off the injected clock (the same
        ISO8601-UTC format the rest of the schema uses): the probe sends the
        result the instant its check finishes, so record time is the
        check time, and stamping daemon-side keeps one clock and avoids
        trusting a format from the unprivileged probe process.

        On status='unhealthy' an internal/dep finding is emitted to `now`
        AFTER the upsert (still no `await`). Under the B30 emission gate the
        first unhealthy record opens the internal/dep incident and emits; a
        repeat while it stays open is dropped at the catalog. On a healthy
        record a clearance is emitted (also gate-dropped to a no-op when no
        dependency_unhealthy incident is open), which closes the incident and
        re-arms the gate so a later genuine re-failure alerts again. Without
        that clearance the dependency would alert once and then go silent
        forever.
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
        self.catalog.record_dep_health(name, status, self.clock.now_iso(), detail)
        if status == "unhealthy":
            self.catalog.write_internal_finding(
                "internal/dep",
                "dependency_unhealthy",
                name,
                detail or "",
                set(self.lodging.pipes),
            )
        else:
            self.catalog.write_internal_clearance(
                "internal/dep",
                name,
                detail or f"{name} healthy",
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
            self._run_drain_job,
            trigger,
            args=[pipe_name],
            id=f"pipe:{pipe_name}",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        LOGGER.info("registered pipe %s on %s", pipe_name, cadence)

    async def _run_drain_job(self, pipe_name: str) -> None:
        """Scheduler job body for a non-immediate (cron/interval) pipe.

        Wraps drain.drain_once() so the running asyncio task is tracked in
        self._drain_tasks for its whole lifetime. AsyncIOExecutor.shutdown()
        cancels this task on daemon shutdown but does not await it, and it is
        not in run()'s `pending`; the tracking lets run()'s finally cancel and
        await it so the CancelledError reap arm runs before the loop closes.
        """
        drain = self.pipe_drains.get(pipe_name)
        if drain is None:
            return
        task = asyncio.current_task()
        if task is not None:
            self._drain_tasks.add(task)
        try:
            await drain.drain_once()
        finally:
            if task is not None:
                self._drain_tasks.discard(task)

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

        # Re-sync the delivery-SLA table to the new pipe set (B2). Same
        # rationale as the dep_health prune above: a hot-changed max_interval
        # must take effect and a hot-removed pipe's stale SLA row must not keep
        # belfry red. Synchronous, self-committing, no await before its commit
        # -- cancel-safe like the rest of the reload.
        self._sync_pipe_sla()

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
                clock=self.clock,
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
        observation_id = int(row["id"])
        # Cancellation by _triage_loop's shutdown-finally can land at any
        # await in this method: queueing on the semaphore, queueing on
        # the per-triager lock, or inside _run_triager itself. In every
        # one of those cases mark_triage_processing has already written
        # the row (it runs synchronously in _triage_loop's body just
        # before this task is created), so we must clear it on the way
        # out -- recover_writing_rows does not touch observation_triage
        # and ready_observations_for excludes 'processing' rows, so an
        # unrecovered row would orphan the observation across daemon
        # restarts. clear_triage_processing is bounded to
        # status='processing', so a transition that legitimately landed
        # at 'success'/'failed' (which can only have happened inside
        # _run_triager BEFORE the cancellation arrived) is not
        # clobbered. The outer arm catches all three cancellation
        # landing points uniformly; an inner arm in _run_triager is
        # unnecessary because CancelledError propagates out through
        # async with and lands here.
        try:
            async with self.triage_semaphore:
                triager = self.lodging.triagers.get(triager_name)
                if triager is None:
                    # Triager hot-removed between mark_triage_processing and now.
                    # Same orphan risk as the cancel arm above; the helper
                    # logs and delegates to catalog.clear_triage_processing.
                    self._clear_triage_for_removed_triager(observation_id, triager_name)
                    return
                lock_key = (triager.name, triager.source_ref)
                lock = self.triager_locks.setdefault(lock_key, asyncio.Lock())
                async with lock:
                    await self._run_triager(row, triager_name)
        except asyncio.CancelledError:
            self.catalog.clear_triage_processing(observation_id, triager_name)
            raise

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
            # Recovery edge for the internal/triage incident: a triager whose
            # retries were exhausted (below) opened a triage_failed incident;
            # a later successful run clears it so the gate re-arms. Dropped to
            # a no-op by the recovery gate when nothing is open.
            self.catalog.write_internal_clearance(
                "internal/triage",
                triager.name,
                f"{triager.name} triage succeeded",
                set(self.lodging.pipes),
            )
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

    # -- Fixers (B11) ------------------------------------------------------
    #
    # The in-daemon autoremediation layer. Each pass evaluates every lodged
    # fixer's condition against live catalog state and, for each matched
    # condition instance the guardrails permit, runs the fixer's handler
    # subprocess and records the attempt + an audit line. Reads
    # self.lodging.fixers fresh each pass, so a hot-added/removed fixer takes
    # effect on the next pass with no per-fixer scheduler job to manage (unlike
    # sources/pipes, a fixer has no cadence -- it fires off condition, not time).

    async def _fixer_loop(self) -> None:
        try:
            while not self.stop_event.is_set():
                try:
                    await self._evaluate_fixers()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A bug evaluating one pass must not kill the loop; the
                    # condition stays live and is retried next pass.
                    LOGGER.exception("fixer evaluation pass crashed")
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(), timeout=_FIXER_POLL_INTERVAL_SEC
                    )
                except TimeoutError:
                    continue
        except asyncio.CancelledError:
            return

    async def _evaluate_fixers(self) -> None:
        """One evaluation pass over all fixers. Serial by design: a remediation
        storm is the opposite of what this layer is for, and the guardrails
        already throttle each fixer, so a single in-flight handler at a time
        (bounded by its timeout) is the safe default for the registry's first
        cut. Tests call this directly."""
        for fixer in list(self.lodging.fixers.values()):
            for condition_key, context in self._match_fixer_condition(
                fixer.condition
            ):
                if not self._fixer_allowed(fixer, condition_key):
                    continue
                await self._run_fixer(fixer, condition_key, context)

    def _match_fixer_condition(
        self, condition: FixerCondition
    ) -> list[tuple[str, dict]]:
        """Return (condition_key, handler_context) for each live instance of a
        condition. The key uniquely identifies one condition instance so the
        guardrail budget accumulates per instance; the context is the JSON the
        handler receives describing what it is being asked to remediate."""
        matches: list[tuple[str, dict]] = []
        if condition.kind == "open_internal_incident":
            for incident in self.catalog.open_incidents():
                if incident.get("source") != condition.source:
                    continue
                if (
                    condition.incident_type is not None
                    and incident.get("type") != condition.incident_type
                ):
                    continue
                if (
                    condition.entity is not None
                    and incident.get("entity") != condition.entity
                ):
                    continue
                # source/type/entity is the open-incident identity (the unique
                # open index in 0001), so this key is stable across passes for
                # the same open incident -- attempts accumulate against it.
                key = (
                    f"open_internal_incident:{incident.get('source')}:"
                    f"{incident.get('type')}:{incident.get('entity')}"
                )
                matches.append(
                    (key, {"kind": condition.kind, "incident": incident})
                )
        elif condition.kind == "channel_unhealthy":
            for row in self.catalog.all_channel_health():
                if row.get("status") != "unhealthy":
                    continue
                if (
                    condition.channel is not None
                    and row.get("channel") != condition.channel
                ):
                    continue
                key = f"channel_unhealthy:{row.get('channel')}"
                matches.append((key, {"kind": condition.kind, "channel": row}))
        return matches

    def _fixer_allowed(self, fixer: Fixer, condition_key: str) -> bool:
        """Guardrail gate: cap attempts within the window and enforce backoff.

        When the cap is hit the fixer simply stops firing for that condition --
        deliberately quiet, not an escalation. The underlying condition (the
        open incident / unhealthy channel) stays live and is already surfaced by
        belfry and `angelus health`, so the problem remains loud through the
        detection layer; making the fixer's *giving up* itself page is the
        escalation ladder's job (B14), not the registry's.

        Both skip paths log at DEBUG: a live-but-blocked condition is
        re-evaluated every poll, so a higher level would emit hundreds of
        identical lines per hour into state/angelus.log -- exactly the noise the
        logging unification (B21+B22) made ERROR/WARNING meaningful to avoid.
        The attempts that DID run are in fixers.log and the daily digest."""
        count = self.catalog.fixer_attempt_count_in_window(
            fixer.name, condition_key, fixer.window_seconds
        )
        if count >= fixer.max_attempts:
            LOGGER.debug(
                "fixer %s guard: %d attempt(s) in %ds window for %s; skipping",
                fixer.name,
                count,
                fixer.window_seconds,
                condition_key,
            )
            return False
        if fixer.backoff_seconds > 0:
            last = self.catalog.last_fixer_attempt_at(fixer.name, condition_key)
            if last is not None:
                last_dt = _parse_iso(last)
                if last_dt is not None:
                    elapsed = (self.clock.now() - last_dt).total_seconds()
                    if elapsed < fixer.backoff_seconds:
                        LOGGER.debug(
                            "fixer %s backoff: %.0fs since last attempt for %s "
                            "(< %ds); skipping",
                            fixer.name,
                            elapsed,
                            condition_key,
                            fixer.backoff_seconds,
                        )
                        return False
        return True

    async def _run_fixer(
        self, fixer: Fixer, condition_key: str, context: dict
    ) -> None:
        note: str | None = None
        try:
            result = await run_python_fixer(fixer, {**context, "condition_key": condition_key})
            outcome = result["outcome"]
            raw_note = result.get("note")
            note = raw_note if isinstance(raw_note, str) and raw_note else None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Any handler failure (timeout, non-zero exit, bad output) is a
            # recorded outcome, not a crash: it must count against the guardrail
            # so a persistently-failing fixer backs off like any other.
            outcome = "error"
            note = str(exc)
            LOGGER.error(
                "fixer %s failed on %s: %s", fixer.name, condition_key, exc
            )
        # Record the attempt AFTER running so the ledger carries the real
        # outcome. The belfry restart-guard records BEFORE its systemctl call
        # because a daemon crash-loop would otherwise bypass the count; here the
        # crash-loop axis is belfry's (B12), the handler is timeout-bounded, and
        # at most one handler runs per condition per pass, so record-after
        # cannot create an unbounded loop -- the worst case is one uncounted
        # attempt if the daemon is killed mid-handler.
        self.catalog.record_fixer_attempt(fixer.name, condition_key, outcome)
        self._append_fixers_log(fixer.name, condition_key, outcome, note)
        LOGGER.info(
            "fixer %s ran on %s -> %s", fixer.name, condition_key, outcome
        )

    def _append_fixers_log(
        self, fixer_name: str, condition_key: str, outcome: str, note: str | None
    ) -> None:
        """Append one line to the shared fixers.log audit trail (B11).

        Same file and key=value line format belfry's B12 restart-fixer writes,
        so an in-daemon fixer's actions flow into the daily digest's
        fixer_actions input and any postmortem with zero extra plumbing. actor
        is the fixer name (distinct from belfry's actor=belfry). Best-effort:
        an audit-log IO error is logged and swallowed, never failing a fixer."""
        path = _fixers_log_path(self.root)
        # Second-precision wall format matches belfry's lines in the same file;
        # sourced from the injected clock so a FakeClock test controls it.
        ts = self.clock.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        line = (
            f"{ts} actor={fixer_name} action=fix "
            f"reason={condition_key!r} outcome={outcome}"
        )
        if note is not None:
            line += f" note={note!r}"
        line += "\n"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(line)
        except OSError:
            LOGGER.warning("failed to append to fixers log %s", path, exc_info=True)


def _parse_iso(value: str) -> datetime | None:
    """Parse a catalog ISO8601 timestamp (``...Z``) to an aware UTC datetime,
    or None if it does not parse. Used for fixer backoff spacing."""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _fixers_log_path(root: Path) -> Path:
    """Path to the shared fixers.log. Honors ANGELUS_BELFRY_FIXERS_LOG_PATH --
    the same override belfry and the digest's reader use -- so all three agree
    on one file in tests and alternate deployments."""
    override = os.environ.get("ANGELUS_BELFRY_FIXERS_LOG_PATH")
    if override:
        return Path(override)
    return root / "state" / DEFAULT_FIXERS_LOG_FILENAME


def _belfry_sentinel_path(root: Path) -> Path:
    """Resolve the belfry liveness sentinel path on the daemon side.

    ANGELUS_BELFRY_SENTINEL_PATH overrides; default is
    <root>/state/belfry-pinged-at. Belfry uses the same env var and
    default (belfry/belfry.py:sentinel_path) so the two sides stay in
    sync. A drifted path on either side would surface as permanent
    "never pinged" -- the mandatory-reader contract catches that at
    integration-test time.
    """
    override = os.environ.get("ANGELUS_BELFRY_SENTINEL_PATH")
    if override:
        return Path(override)
    return root / "state" / DEFAULT_BELFRY_SENTINEL_FILENAME


def _belfry_stale_after_seconds() -> int:
    """How old the sentinel mtime can get before we call belfry stale.

    ANGELUS_BELFRY_STALE_AFTER_SEC overrides; default is 1200s (20min).
    The default is roughly 2x belfry's typical cron cadence (5-15min) so
    a single skipped tick is not enough to flip stale=true; two-plus
    skipped ticks are. Invalid or non-positive overrides fall back to
    the default so health never crashes on a misconfigured env.
    """
    raw = os.environ.get("ANGELUS_BELFRY_STALE_AFTER_SEC")
    if raw is None:
        return DEFAULT_BELFRY_STALE_AFTER_SEC
    try:
        seconds = int(raw)
    except ValueError:
        LOGGER.warning(
            "invalid ANGELUS_BELFRY_STALE_AFTER_SEC=%r; using default", raw
        )
        return DEFAULT_BELFRY_STALE_AFTER_SEC
    if seconds <= 0:
        LOGGER.warning(
            "ANGELUS_BELFRY_STALE_AFTER_SEC=%d must be positive; using default",
            seconds,
        )
        return DEFAULT_BELFRY_STALE_AFTER_SEC
    return seconds


def _belfry_status(root: Path, clock: Clock | None = None) -> dict:
    """Liveness shape for the health op's belfry field.

    Returns {"last_pinged_at": <iso8601-Z>|None, "stale": <bool>}. The
    dict shape (never bare None) is deliberate -- the CLI render can
    branch on the boolean without first guarding the outer value, which
    matches how 'sources' and 'deps' are surfaced.

    Missing sentinel -> {"last_pinged_at": None, "stale": True}. Belfry
    has never run on this root, which is the most-stale state possible.
    """
    path = _belfry_sentinel_path(root)
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return {"last_pinged_at": None, "stale": True}
    except OSError as exc:
        LOGGER.warning("failed to stat belfry sentinel %s: %s", path, exc)
        return {"last_pinged_at": None, "stale": True}
    pinged_at = datetime.fromtimestamp(mtime, tz=UTC)
    age_sec = ((clock or Clock()).now() - pinged_at).total_seconds()
    stale = age_sec > _belfry_stale_after_seconds()
    return {
        "last_pinged_at": pinged_at.isoformat().replace("+00:00", "Z"),
        "stale": stale,
    }


def _delivery_surface(catalog: Catalog, pipe_names: list[str]) -> dict:
    """Delivery half of the health surface (B5): "is it WORKING", not just
    "is it running". Built from the dispatch/incident schema the daemon
    already writes, so it works on both the live control-socket path and the
    daemon-down read-only CLI fallback.

    - last_successful_send: every configured pipe -> its most recent 'sent'
      dispatch timestamp, or None ('never'). Keyed on the passed pipe set so a
      pipe that has never delivered is still listed (the silent gap the
      2026-05-29 incident hid).
    - failed_dispatches: count of failed dispatches in the recent window.
    - open_internal_incidents: angelus's own open self-reported failures.
    """
    last_sent = catalog.last_successful_dispatch_per_pipe()
    return {
        "last_successful_send": {
            name: last_sent.get(name) for name in sorted(pipe_names)
        },
        "failed_dispatches": {
            "window_hours": HEALTH_FAILED_DISPATCH_WINDOW_HOURS,
            "count": catalog.failed_dispatch_count(
                HEALTH_FAILED_DISPATCH_WINDOW_HOURS
            ),
        },
        "open_internal_incidents": catalog.open_internal_incident_count(),
    }


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
    root = root or Path.cwd()
    # Single canonical log destination regardless of launch method (B21+B22):
    # a rotating file at state/angelus.log, written by the app rather than by
    # stdout redirection, so systemd and a hand-launched daemon log
    # identically. Configure logging first so the env-load line below is
    # captured. See angelus/logging_config.py and docs/logging.md.
    configure_logging(root)
    # Load non-secret config from state/angelus.env (B16). systemd's
    # EnvironmentFile= already does this for the managed unit; doing it in code
    # too means a hand-launched daemon -- the 2026-05-29 incident -- inherits
    # the same config instead of silently losing it. Non-override: anything
    # already in the environment wins over the file.
    applied = load_env_file(root)
    if applied:
        LOGGER.info(
            "loaded %d var(s) from state/angelus.env: %s",
            len(applied),
            ", ".join(sorted(applied)),
        )
    # Resolve any op:// secret references (e.g. the digest heartbeat URL) via the
    # read-only angelus-daemon service-account token the systemd unit injects.
    # Daemon-only: belfry has its own stdlib loader and never resolves refs, so
    # the belt layer keeps no 1Password dependency. Fail-safe -- an unresolved
    # ref is left unset (the consumer degrades) rather than crashing startup.
    resolved = resolve_op_refs()
    if resolved:
        LOGGER.info(
            "resolved %d secret ref(s) via service account: %s",
            len(resolved),
            ", ".join(sorted(resolved)),
        )
    asyncio.run(AngelusDaemon(root).run())
