"""Angelus daemon for the slice-1 vertical path."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from angelus.clock import Clock
from angelus.control import ControlServer
from angelus.envfile import load_env_file, resolve_op_refs
from angelus.faults import FaultRegistry
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
from angelus.pipes import DrainSummary, PipeDrain
from angelus.sources import run_shell_source
from angelus.storage import Catalog, init_db
from angelus.triage import run_python_triager

LOGGER = logging.getLogger(__name__)


def _change_signature(payload: dict[str, Any], outcome: str) -> str:
    """The comparison token for observation collapse: a fire writes an
    observation only when this value differs from the source's stored
    last_state. The whole reliability invariant rides on this -- angelus
    exists to catch state transitions, so the rule is NEVER MISS A
    TRANSITION; over-writing (a redundant observation) is harmless, skipping
    one is not.

    The token is the SIMPLE state, not a whole-payload diff:
      * If the payload carries an explicit `state` field, that is the token
        (e.g. "200", "503", "success", "failure"). This is the preferred path
        and the reason the watch checks emit `state`: it means a CI run that
        stays green across a new sha/run_started does NOT churn an observation,
        while a green->red conclusion always does. A whole-payload diff would
        get this exactly wrong.
      * Otherwise (an unconfigured check with no `state`) fall back to a
        canonical hash of the full payload, so identical payloads still
        collapse and nothing is silently lost.

    OUTCOME is folded in so a check that starts failing or recovers is ALWAYS
    a change even if the state token is unchanged: a check_failed fire is
    prefixed so it can never collide with any ok signature. (An ok 200 and a
    check_failed fire that happened to carry status 200 must read as different
    states -- the latter means "we could not actually check".)

    Raising is the caller's signal to fail safe: _fire_source treats ANY
    exception here as a change and writes the observation.
    """
    if "state" in payload:
        token = str(payload["state"])
    else:
        token = hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()
    if outcome != "ok":
        return f"{outcome}:{token}"
    return token


DEFAULT_BELFRY_SENTINEL_FILENAME = "belfry-pinged-at"
DEFAULT_BELFRY_STALE_AFTER_SEC = 1200

# Recent-window for the health surface's failed-dispatch count (B5). A nonzero
# count over this window says "delivery is actively breaking now", distinct
# from the open-internal-incident tally (which can persist across the window).
HEALTH_FAILED_DISPATCH_WINDOW_HOURS = 24

# Cap on the dead-letter items the health surface renders inline (B15). The
# count is always exact; this only bounds the per-item detail list so a large
# backlog cannot flood the screen-reader output. The longest-stuck items sort
# first, so the cap drops the freshest rather than the most overdue.
HEALTH_DEAD_LETTER_DISPLAY_LIMIT = 20

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

# How often the consume sweep re-checks for ready observations whose source
# has no live triager (catalog.consume_observations_without_triager). The
# grace period below -- not this interval -- decides when an observation is
# old enough to consume, so the cadence only bounds how stale the `ready`
# set can get after the grace expires. Tests drive the sweep directly via
# _consume_sweep_once and do not depend on it.
_CONSUME_SWEEP_INTERVAL_SEC = 60.0

# Grace before a ready observation whose source has no live triager is
# consumed. Long enough that lodging a triager for an already-firing source
# picks up a day of history rather than finding it already consumed.
DEFAULT_NO_TRIAGER_CONSUME_GRACE_SEC = 86_400


def _no_triager_consume_grace_seconds() -> int:
    """Grace before no-triager observations are consumed by the sweep.

    ANGELUS_NO_TRIAGER_CONSUME_GRACE_SEC overrides; default is 86400s (24h).
    Invalid or non-positive overrides fall back to the default so the sweep
    never crashes on a misconfigured env -- same contract as
    ANGELUS_BELFRY_STALE_AFTER_SEC below.
    """
    raw = os.environ.get("ANGELUS_NO_TRIAGER_CONSUME_GRACE_SEC")
    if raw is None:
        return DEFAULT_NO_TRIAGER_CONSUME_GRACE_SEC
    try:
        seconds = int(raw)
    except ValueError:
        LOGGER.warning(
            "invalid ANGELUS_NO_TRIAGER_CONSUME_GRACE_SEC=%r; using default", raw
        )
        return DEFAULT_NO_TRIAGER_CONSUME_GRACE_SEC
    if seconds <= 0:
        LOGGER.warning(
            "ANGELUS_NO_TRIAGER_CONSUME_GRACE_SEC=%d must be positive; using default",
            seconds,
        )
        return DEFAULT_NO_TRIAGER_CONSUME_GRACE_SEC
    return seconds


class AngelusDaemon:
    def __init__(self, root: Path, *, clock: Clock | None = None) -> None:
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
        # Single clock for the process (B24). Threaded into the catalog and
        # every PipeDrain below so all timestamp/window logic shares one notion
        # of "now". Defaults to the real wall Clock so production
        # (`angelus daemon`, which passes no clock) is unchanged; the sim
        # harness (B26) injects a FakeClock here, which then reaches the
        # catalog and every PipeDrain automatically because both are built from
        # self.clock in this same __init__ -- one injection point pins time
        # everywhere. apscheduler keeps real time regardless (B25 forces work
        # on demand without time-travelling the scheduler; the sim never starts
        # it).
        self.clock = clock or Clock()
        self.catalog = Catalog(self.connection, root, clock=self.clock)
        # Fault-injection registry (B28). One per process, threaded into every
        # PipeDrain below so a live `fault_inject` control op arms a fault
        # across all pipes at once -- the same shared-seam threading as the
        # clock. Seeded from ANGELUS_FAULT_INJECT so a scenario harness can
        # bring the daemon up with a channel already failing. In-memory only:
        # never persisted, cleared on restart by construction.
        self.faults = FaultRegistry.from_env()
        self.lodging: Lodging = load_lodging(root)
        # Read once at construction (like FaultRegistry.from_env above): the
        # knob is deployment config, not something to mutate on a live daemon.
        self._no_triager_consume_grace_sec = _no_triager_consume_grace_seconds()
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
        # In-flight drain job tasks: the scheduled (cron/interval) digest-drain
        # jobs, plus any manually-triggered drain from the `drain` control op
        # (_op_drain), which can target ANY pipe kind -- the immediate `now`
        # pipe as well as a digest pipe. AsyncIOExecutor.shutdown() cancels
        # these on shutdown but does not await them, and they are not in
        # `pending` -- so a cancelled drain's reap arm (the digest path's
        # _render_llm_body -> _kill_and_reap horizon cast subtree, or an
        # immediate send's transport subprocess) would race event-loop teardown
        # and orphan the child tree. Each drain registers its task here on entry
        # and discards on exit; run()'s finally cancels and awaits the set.
        # Mirrors _triage_loop's in_flight handling.
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
                faults=self.faults,
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
                "fault_inject": self._op_fault_inject,
                "drain": self._op_drain,
                "fire_source": self._op_fire_source,
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
            self._sync_source_sla()
            self._register_initial_jobs()
            self.scheduler.start()
            scheduler_started = True
            LOGGER.info("scheduler started with %d jobs", len(self.scheduler.get_jobs()))
            self.tasks.append(asyncio.create_task(self._triage_loop(), name="triage-loop"))
            self.tasks.append(
                asyncio.create_task(
                    self._consume_sweep_loop(), name="consume-sweep-loop"
                )
            )
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

    def _sync_source_sla(self) -> None:
        """Persist each source's check SLA to sqlite so belfry -- the
        out-of-band, pure-stdlib layer -- can read the contract and assert each
        source is still being CHECKED on cadence (0014). The input-side mirror
        of _sync_pipe_sla.

        belfry's wedge check reads a GLOBAL max(last_checked_at); it only fires
        when EVERY source goes stale, so one healthy source masks a single stale
        one. This per-source SLA closes that gap: belfry pings DOWN (alert-only,
        never a restart) when any one source's heartbeat lapses past its window.

        The window is cadence + max(cadence, SOURCE_SLA_SLACK_FLOOR_SECONDS) --
        2x cadence for normal cadences ("missed an entire extra cycle"), floored
        so a sub-floor cadence (e.g. 30s) still gets cadence + floor of slack and
        can't flap belfry on transient boot-burst scheduler jitter. The
        last_checked_at heartbeat bumps on EVERY fire (observation collapse
        always advances it), so a live source's heartbeat age is at most ~one
        cadence plus tiny jitter; 2x never false-alarms a slow daily/4h/12h
        source that fires a few minutes late.

        For an INTERVAL cadence ("4h") the "cadence" is the parsed seconds.
        For a CRONTAB cadence ("0 3 * * *") there is no single interval, so the
        "cadence" is the MAX gap between consecutive fires of the SAME trigger
        the scheduler uses (_crontab_max_gap_seconds): a daily cron -> 86400, a
        weekday-only cron -> 259200 (the Fri->Mon weekend gap, which MUST drive
        the window so a weekday source does not false-alarm over a weekend),
        weekly/monthly -> their period. A daily cron therefore gets a 2-day
        window (86400 + 86400). All cron reasoning lives daemon-side; belfry only
        ever reads the resulting max_interval_seconds.

        FAIL-SAFE (per source, symmetric across both cadence kinds): if bounding
        a source's cadence fails -- a crontab whose max-gap can't be proven (no
        fire times, a zero/negative gap, or the fire cap exhausted before a full
        period is observed: a too-dense / DST-stalling cron), OR a malformed
        interval cadence that won't parse -- that ONE source falls back to
        skip-with-warning: left to the global wedge backstop and named in a
        warning so the gap is visible, never silent. _sync_source_sla never
        crashes on any cadence, never writes a too-small bound that would
        false-alarm, and never loses coverage for the OTHER sources over one bad
        one; a failed source is simply not tracked per-source.

        depends_on is advisory today -- _fire_source does not actually block on
        an unhealthy dep, so a dep-"blocked" source still fires and stays fresh
        and will NOT false-alarm here. If dep-blocking is ever made real,
        revisit: a truly blocked source would stop firing and this check would
        correctly flag it.
        """
        slas: dict[str, int] = {}
        skipped: list[str] = []
        # Per-source robustness: BOTH cadence branches (crontab max-gap walk and
        # interval parse) run under the same try, so a single bad source --
        # whichever kind -- is skipped-with-warning and every OTHER source is
        # still tracked. Without this, a malformed interval cadence ('banana')
        # would raise straight out of the loop and abort the whole sync, dropping
        # per-source SLA tracking for ALL sources over one bad source.
        # (A truly malformed cadence will still crash the daemon later at
        # _add_source_job/_make_trigger; that pre-existing load-time gap is out
        # of scope here -- this only keeps _sync_source_sla itself from losing
        # ALL coverage over one bad source.)
        for ref, source in self.lodging.sources.items():
            try:
                if _is_crontab_cadence(source.cadence):
                    cadence_seconds = _crontab_max_gap_seconds(source.cadence)
                else:
                    cadence_seconds = _cadence_seconds(source.cadence)
            except Exception as exc:  # noqa: BLE001 -- fail safe, never crash
                # Could not bound this source. Fall back to the global wedge
                # backstop for this one source rather than crash the whole sync
                # or write a bad (possibly too-small) bound.
                LOGGER.warning(
                    "source check SLA could not bound cadence %r for %s (%s); "
                    "leaving it to the global wedge backstop",
                    source.cadence,
                    ref,
                    exc,
                )
                skipped.append(ref)
                continue
            slas[ref] = cadence_seconds + max(
                cadence_seconds, SOURCE_SLA_SLACK_FLOOR_SECONDS
            )
        self.catalog.sync_source_sla(slas)
        if slas:
            LOGGER.info(
                "source check SLAs tracked: %s",
                ", ".join(
                    f"{ref}={seconds}s" for ref, seconds in sorted(slas.items())
                ),
            )
        if skipped:
            LOGGER.warning(
                "source check SLA NOT tracked for source(s) whose cadence could "
                "not be bounded -- covered only by the global wedge backstop, "
                "not per-source: %s",
                ", ".join(sorted(skipped)),
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
            # Fault-injection safety rail (B28): any channel armed to fail via
            # the live `fault_inject` op (or the ANGELUS_FAULT_INJECT seed) is
            # surfaced here so an armed fault on the running daemon is
            # impossible to silently forget. In-memory only -- a restart clears
            # it, so the daemon-down health fallback correctly shows none.
            "fault_injection": {"armed": self.faults.armed()},
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

    async def _op_fault_inject(self, args: dict) -> dict:
        """Arm, clear, or list in-memory channel faults (B28).

        Same construction as the write ops above -- async in signature only,
        no `await` in the body -- but it touches NO sqlite at all: the fault
        registry is pure in-memory process state, never persisted, so arming a
        fault here is cleared on the next daemon restart by construction. A
        ValueError is turned into {"ok": false, "error": ...} by
        ControlServer._dispatch.

        `action` selects the operation:
          - "arm"/"clear" require `channel`. For "arm" it must name a
            CONFIGURED channel (rejected otherwise so a typo cannot silently
            arm nothing). "clear" accepts ANY name unconditionally: the
            registry discard is idempotent, and a channel can be hot-reloaded
            out of config while a stale armed entry lingers -- requiring it to
            be configured would make that entry un-clearable individually.
          - "clear_all" drops every armed fault;
          - "list" returns the current set with no change.
        Every action returns the resulting armed set (sorted) so the caller can
        render the live state without a second round-trip.
        """
        action = args.get("action")
        if action not in ("arm", "clear", "clear_all", "list"):
            raise ValueError(
                "fault_inject action must be 'arm', 'clear', 'clear_all', "
                "or 'list'"
            )
        if action in ("arm", "clear"):
            channel = args.get("channel")
            if not isinstance(channel, str) or not channel:
                raise ValueError(
                    f"fault_inject {action} requires a non-empty channel"
                )
            if action == "arm":
                if channel not in self.lodging.channels:
                    raise ValueError(f"unknown channel: {channel}")
                self.faults.arm(channel)
            else:
                self.faults.clear(channel)
        elif action == "clear_all":
            self.faults.clear_all()
        return {"armed": self.faults.armed()}

    async def _op_drain(self, args: dict) -> dict:
        """Run a named pipe's drain on demand and return its summary (B25).

        Unlike the cancel-safe write ops above this one genuinely AWAITS work
        (the drain sends over real transports), so it must not break the
        project's <8s no-hang shutdown bound. It does NOT call drain_once()
        raw: it runs _run_drain_job in its OWN asyncio task, which registers
        that task in self._drain_tasks for its whole lifetime -- the exact
        shape a scheduled (cron/interval) drain has. run()'s finally cancels
        and awaits self._drain_tasks on shutdown, so a manual drain in flight
        when shutdown lands is reaped with the scheduled ones (its
        CancelledError reap arm runs) instead of outliving teardown. drain_once
        already takes the per-pipe self.lock, so a manual drain serialises with
        a concurrent scheduled drain on the same pipe -- no new locking here.

        The named pipe may be ANY kind, including the immediate `now` pipe;
        drain_once branches to the digest or immediate path itself. An unknown
        pipe name is rejected before any task is spawned (mirroring
        fault_inject's unknown-channel rejection) so a typo surfaces loudly
        rather than draining nothing. A None summary means the pipe was
        hot-reloaded out between validation and the drain -- the same
        vanished-target race, surfaced the same way.
        """
        pipe_name = args.get("pipe")
        if not isinstance(pipe_name, str) or not pipe_name:
            raise ValueError("drain requires a non-empty pipe name")
        if pipe_name not in self.pipe_drains:
            raise ValueError(f"unknown pipe: {pipe_name}")
        summary = await asyncio.create_task(self._run_drain_job(pipe_name))
        if summary is None:
            raise ValueError(f"unknown pipe: {pipe_name}")
        return {
            "pipe": pipe_name,
            "dispatched": summary.dispatched,
            "failed": summary.failed,
        }

    async def _op_fire_source(self, args: dict) -> dict:
        """Run a source's check once on demand and return what it produced (B25).

        Reuses _fire_source -- the same body the scheduler runs -- so the
        manual fire is indistinguishable from a scheduled one: it acquires
        self.scheduler_semaphore (bounding it against concurrent scheduled
        fires), runs the shell check, records the check, and writes an
        observation IF the state changed. _fire_source returns the
        (observation_id, outcome) the op shapes its response from; the scheduler
        ignores that return. observation_id is None when the fire collapsed (no
        state change, no observation written) -- the CLI renders that as "no
        change" rather than an id.

        An unknown source name is rejected before the fire (mirroring drain's
        unknown-pipe rejection). A None result means the source was
        hot-reloaded out between validation and the fire -- the vanished-target
        race, surfaced as unknown.
        """
        name = args.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("fire_source requires a non-empty source name")
        if name not in self.lodging.sources:
            raise ValueError(f"unknown source: {name}")
        result = await self._fire_source(name)
        if result is None:
            raise ValueError(f"unknown source: {name}")
        observation_id, outcome = result
        return {
            "source": name,
            "observation_id": observation_id,
            "outcome": outcome,
        }

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.request_stop)

    def _register_initial_jobs(self) -> None:
        # Startup-only sources fire immediately so each populates its
        # watch_state heartbeat within seconds of boot (see _add_source_job's
        # `immediate`). On a fresh/wiped DB watch_state is empty until the first
        # fire; belfry's wedge detector reads max(last_checked_at) and, if it
        # ticks in that gap, restarts a daemon that is merely still coming up --
        # the restart-loop incident this guards against. apply_lodging's
        # hot-add path deliberately does NOT pass immediate=True: that is a
        # steady-state cadence change, not a (re)start, so it keeps the normal
        # start+interval first fire.
        for source in self.lodging.sources.values():
            self._add_source_job(source, immediate=True)
        for pipe in self.lodging.pipes.values():
            if pipe.cadence == "immediate":
                continue
            self._add_pipe_job(pipe.name, pipe.cadence)

    def _add_source_job(
        self, source: ScheduledSource, *, immediate: bool = False
    ) -> None:
        trigger = _make_trigger(source.cadence)
        # `immediate` (startup only) brings the FIRST fire forward to now via
        # next_run_time -- the clean APScheduler idiom for "fire now, then keep
        # the trigger's cadence". Without it IntervalTrigger defaults its first
        # fire to now+interval (and a crontab to its next match), so a fresh DB
        # has no watch_state heartbeat for a whole interval after boot. The
        # immediate fire runs the same _fire_source body, so it is still bounded
        # by self.scheduler_semaphore (the boot burst of ~25 sources cannot fan
        # out unbounded) and honours the collapse semantics (first fire on a
        # fresh DB has no prior row -> writes an observation). Steady-state
        # cadence is the trigger's and is unchanged: after the first fire
        # IntervalTrigger computes previous_fire + interval. The injected clock
        # supplies "now" (production real Clock; the sim never starts the
        # scheduler, so this never reads the wall clock there).
        extra: dict[str, Any] = (
            {"next_run_time": self.clock.now()} if immediate else {}
        )
        self.scheduler.add_job(
            self._fire_source,
            trigger,
            args=[source.source_ref],
            id=source.source_ref,
            max_instances=1,
            coalesce=True,
            replace_existing=True,
            **extra,
        )
        LOGGER.info(
            "registered scheduled source %s on %s%s",
            source.source_ref,
            source.cadence,
            " (immediate first fire)" if immediate else "",
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

    async def _run_drain_job(self, pipe_name: str) -> DrainSummary | None:
        """Scheduler job body for a non-immediate (cron/interval) pipe.

        Wraps drain.drain_once() so the running asyncio task is tracked in
        self._drain_tasks for its whole lifetime. AsyncIOExecutor.shutdown()
        cancels this task on daemon shutdown but does not await it, and it is
        not in run()'s `pending`; the tracking lets run()'s finally cancel and
        await it so the CancelledError reap arm runs before the loop closes.

        Returns drain_once's DrainSummary (B25) so the manual `drain` op can
        report dispatched/failed counts. The scheduler ignores the return; the
        op runs this in its own create_task so a manually-triggered drain is a
        standalone tracked task reaped on shutdown exactly like a scheduled one.
        None is returned only if the pipe was hot-removed before the drain ran.
        """
        drain = self.pipe_drains.get(pipe_name)
        if drain is None:
            return None
        task = asyncio.current_task()
        if task is not None:
            self._drain_tasks.add(task)
        try:
            return await drain.drain_once()
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
        # Same for the per-source check SLA (0014): a hot source add/remove or
        # cadence change must be reflected so belfry tracks the live source set
        # -- a hot-added source becomes monitored, a hot-removed one's stale row
        # is dropped, and a cadence change resizes its window. Synchronous,
        # self-committing, no await before its commit -- cancel-safe like the
        # rest of the reload.
        self._sync_source_sla()

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
                faults=self.faults,
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

    async def _fire_source(self, source_ref: str) -> tuple[int | None, str] | None:
        """Run a source's shell check and write an observation ONLY when the
        source's state changed (observation collapse).

        Returns (observation_id, outcome). outcome is "ok" on a clean check and
        "check_failed" on a non-zero/timeout/bad-payload check. observation_id
        is the new observation's id when the fire was a CHANGE (or first
        sighting), or None on a collapsed (unchanged) tick where no observation
        was written -- the manual `fire_source` op (B25) surfaces both, and the
        scheduler caller ignores the return. Returns None (the whole tuple) only
        when the source was hot-removed before the fire (the vanished-target
        race), matching how the op treats it.

        Why collapse: nearly every tick is byte-identical to the prior one (a
        web check returning 200 again), and writing an observation + a ledger
        row for each grew the DB unboundedly for zero signal. We compare a
        simple state signature against the source's stored last_state and write
        an observation only on a difference -- but we ALWAYS bump watch_state's
        last_checked_at heartbeat (the overwrite-in-place that replaces the old
        source_fires append, and what belfry's wedge detection / health read).

        The overriding invariant is NEVER MISS A STATE TRANSITION: the very
        thing angelus exists to catch (a site going up->down within the check
        cadence) must always produce an observation. So a missing prior row
        (first sighting), an outcome flip (ok<->check_failed, folded into the
        signature), and a signature-computation ERROR (fail-safe below) all
        count as changes. The only thing we suppress is a fire that is provably
        identical to the last one we already recorded.
        """
        source = self.lodging.sources.get(source_ref)
        if source is None:
            LOGGER.info("scheduled source %s vanished before fire", source_ref)
            return None
        async with self.scheduler_semaphore:
            ok, payload = await run_shell_source(source)
            outcome = "ok" if ok else "check_failed"
            # Fail-safe signature: ANY error computing it is treated as a change
            # (signature=None forces the write below). Missing a transition is
            # the one unacceptable failure, so a malformed payload must never
            # silently skip the observation -- it errs toward writing.
            try:
                signature: str | None = _change_signature(payload, outcome)
            except Exception:
                LOGGER.exception(
                    "change-signature failed for %s; writing observation "
                    "(fail-safe: never skip a possible transition)",
                    source.source_ref,
                )
                signature = None

            prior = self.catalog.watch_state_for(source.source_ref)
            changed = (
                signature is None
                or prior is None
                or prior.get("last_state") != signature
            )

            observation_id: int | None = None
            if changed:
                if ok:
                    observation_id = self.catalog.write_observation(
                        source.source_ref,
                        payload,
                        {"source": source.source_ref, "check": "shell"},
                    )
                    LOGGER.info(
                        "observation %s ready for %s (state changed)",
                        observation_id,
                        source.source_ref,
                    )
                else:
                    observation_id = self.catalog.write_observation(
                        source.source_ref,
                        {"type": "check_failed", **payload},
                        {
                            "source": source.source_ref,
                            "check": "shell",
                            "outcome": outcome,
                        },
                    )
                    LOGGER.warning("source %s failed: %s", source.source_ref, payload)
            else:
                LOGGER.debug(
                    "source %s unchanged (state %s); collapsed, no observation",
                    source.source_ref,
                    signature,
                )

            # ALWAYS record the check: the last_checked_at bump is the heartbeat
            # belfry/health read, and replaces the source_fires append. On a
            # collapsed tick observation_id is None, so record_watch_check
            # preserves last_state/last_changed_at (the last real transition).
            self.catalog.record_watch_check(
                source.source_ref, signature, outcome, observation_id
            )
            return observation_id, outcome

    def _discover_ready_triage(self) -> list[tuple]:
        """One discovery sweep across every lodged triager: the ready
        observations each can pick up, each marked 'processing' so a
        concurrent/repeat sweep won't re-dispatch it.

        Returns the (observation_row, triager_name) pairs to run, in
        triager-then-observation-id order. Marking happens here, synchronously,
        with no await between the read and the mark -- so the set of work and
        the processing rows are written atomically from the loop's point of
        view, exactly as the inline body did before.

        Shared by the live _triage_loop (which spawns each pair under a task and
        reaps across iterations) and the B26 sim harness (which awaits each pair
        to completion before returning). Keeping the discover+mark step in one
        place means the sim can never drift from the daemon's notion of "what is
        ready" -- a sim that triaged a different set than production would be
        worse than no sim. The per-observation execution path
        (_triage_under_semaphore) is likewise shared, not forked.
        """
        work: list[tuple] = []
        for triager in self.lodging.triagers.values():
            rows = self.catalog.ready_observations_for(
                triager.name, triager.source_ref
            )
            for row in rows:
                self.catalog.mark_triage_processing(row["id"], triager.name)
                work.append((row, triager.name))
        return work

    async def _triage_loop(self) -> None:
        in_flight: set[asyncio.Task[None]] = set()
        try:
            while not self.stop_event.is_set():
                self._reap_triage_tasks(in_flight)
                work = self._discover_ready_triage()
                for row, triager_name in work:
                    task = asyncio.create_task(
                        self._triage_under_semaphore(row, triager_name)
                    )
                    in_flight.add(task)
                await asyncio.sleep(0.1 if work or in_flight else 1)
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
        # the row (_discover_ready_triage marks every row synchronously while
        # building the work list, before this method runs for any of them), so
        # we must clear it on the way
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
            self._maybe_consume_observation(observation_id, row["source"])
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
                self._maybe_consume_observation(observation_id, row["source"])
                self.catalog.write_internal_finding(
                    "internal/triage",
                    "triage_failed",
                    triager.name,
                    str(exc),
                    set(self.lodging.pipes),
                )

    def _maybe_consume_observation(self, observation_id: int, source_ref: str) -> None:
        """Settle one observation after a triager reaches a terminal state
        (success, or failed with retries exhausted). The expected-triager set
        is the lodged truth -- only the daemon has it, which is why the
        catalog takes it as an argument instead of deciding alone. An empty
        set (triager hot-removed between the triage run and now) defers to
        the grace-period sweep rather than consuming instantly."""
        expected = {
            triager.name
            for triager in self.lodging.triagers.values()
            if triager.source_ref == source_ref
        }
        new_status = self.catalog.consume_observation_if_terminal(
            observation_id, expected
        )
        if new_status is not None:
            LOGGER.info(
                "observation %d settled to %s (all %d triager(s) terminal)",
                observation_id,
                new_status,
                len(expected),
            )

    async def _pipe_loop(self, pipe_name: str) -> None:
        while not self.stop_event.is_set():
            drain = self.pipe_drains.get(pipe_name)
            if drain is None:
                return
            await drain.drain_once()
            await asyncio.sleep(1)

    def _consume_sweep_once(self) -> None:
        """One pass of the no-triager consume sweep: observations whose
        source has no lodged triager flip ready->consumed once past the
        grace period. The per-observation exit for sources WITH triagers is
        _maybe_consume_observation on the triage path; this sweep is only
        for work nothing will ever pick up. Reads self.lodging.triagers
        fresh each pass (the _fixer_loop pattern) so lodging a triager
        immediately shields its source's observations from the sweep."""
        sources_with_triagers = {
            triager.source_ref for triager in self.lodging.triagers.values()
        }
        consumed = self.catalog.consume_observations_without_triager(
            sources_with_triagers, self._no_triager_consume_grace_sec
        )
        if consumed:
            LOGGER.info(
                "consumed %d observation(s) with no live triager "
                "(grace %ds elapsed)",
                consumed,
                self._no_triager_consume_grace_sec,
            )

    async def _consume_sweep_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._consume_sweep_once()
            except Exception:
                # A bug in one pass must not kill the loop; the condition
                # stays live and is retried next pass (the _fixer_loop
                # contract).
                LOGGER.exception("consume sweep pass crashed")
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=_CONSUME_SWEEP_INTERVAL_SEC
                )
            except TimeoutError:
                continue

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
    - dead_letter: findings whose redelivery ladder exhausted undelivered and
      now sit in the terminal 'dead_letter' state (B15) -- WHAT was abandoned
      (so an operator can `angelus replay <id>` it) plus the true total count.
      This is the "surface loudly, not silently pending" answer the 2026-05-29
      incident demanded: 9/10 findings sat 'pending' with nothing showing it.
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
        "dead_letter": {
            "count": catalog.dead_letter_count(),
            "items": catalog.dead_letter_items(
                limit=HEALTH_DEAD_LETTER_DISPLAY_LIMIT
            ),
        },
    }


_CADENCE_UNITS = {
    "s": 1,
    "sec": 1,
    "m": 60,
    "min": 60,
    "h": 3600,
    "hr": 3600,
}

# Slack floor for the per-source check SLA (0014). The window is
# cadence + max(cadence, this), i.e. 2x cadence except for sub-floor cadences,
# which get cadence + this so a fast source can't flap belfry on boot-burst
# scheduler jitter. See AngelusDaemon._sync_source_sla.
SOURCE_SLA_SLACK_FLOOR_SECONDS = 300


def _is_crontab_cadence(cadence: str) -> bool:
    """True for a crontab cadence ('0 7 * * *'), False for an interval ('4h').

    The single source of truth for the crontab/interval split, shared by
    _make_trigger (which builds a CronTrigger vs IntervalTrigger) and
    _sync_source_sla (which converts an interval cadence directly and a crontab
    cadence via _crontab_max_gap_seconds). Keeping one predicate guarantees a
    source is never classified one way for scheduling and the other for SLA
    tracking. A crontab cadence is any string containing whitespace between its
    fields.
    """
    return any(char.isspace() for char in cadence.strip())


# Bounds on the consecutive-fire walk in _crontab_max_gap_seconds. The walk
# stops at whichever comes first: spanning more than _CRONTAB_GAP_SPAN_SECONDS
# of fire time (so every gap shape of any 5-field cron -- whose pattern repeats
# within a year -- has been seen, and the max gap is PROVEN), or
# _CRONTAB_GAP_FIRE_CAP fires (a backstop). The contract: a bound is returned
# ONLY when the SPAN is reached. If the FIRE CAP trips first the walk has NOT
# observed a full period, so the max gap is unproven and _crontab_max_gap_seconds
# RAISES -- _sync_source_sla then fail-safe-skips that one source rather than
# persist a possibly-too-small window. (Reverting that distinction reintroduces
# the bug a dense month-restricted cron like '* * * 1 *' triggers: 12000 fires
# all land inside January, the walk never leaves January, and it would otherwise
# return a 60s max gap -> a 360s window -> a healthy source false-alarms 11
# months a year.)
#
# Why a MODERATE cap and not a huge one (option b, not a): the scheduler builds
# triggers in the LOCAL timezone (CronTrigger.from_crontab, no tz arg), and at
# the autumn DST fall-back the consecutive-fire walk for any cron that fires
# inside the ambiguous 01:00-01:59 hour OSCILLATES (01:59-04:00 -> 01:00-05:00
# and back) instead of advancing -- so sub-hourly and 1am-touching crons can
# NEVER reach the span no matter how high the cap is set (measured: '* * * * *'
# and '*/15 * * * *' stall at ~305 days from the 2020-01-01 anchor even at
# 700,000 fires). Raising the cap toward ~600k therefore cannot bound them; it
# would only burn startup time (~20s for a stalled 1/min cron) before skipping
# anyway. So the cap is sized as a pure backstop: comfortably above the densest
# cadence that DOES make monotonic DST-safe progress to the span -- every-two-
# hours '0 */2 * * *' reaches it in ~4,800 fires; production's daily crons in
# ~401 -- while bounding a stalled/too-dense cron to ~0.5s of wasted walk.
# Anything denser than the cap-over-span resolves (sub-hourly, or hourly/half-
# hourly cadences that touch the fall-back hour) is INTENTIONALLY left untracked
# per-source: it is fail-safe-skipped with a visible warning and covered by the
# global wedge backstop -- the safe direction (never a too-small bound).
_CRONTAB_GAP_SPAN_SECONDS = 400 * 86400
_CRONTAB_GAP_FIRE_CAP = 12000

# A fixed, clock-independent anchor for the crontab fire walk. The MAX gap
# between consecutive fires is a property of the cron's shape, not of where the
# walk starts, so anchoring at a hardcoded epoch (instead of clock.now()) makes
# the computed bound stable run-to-run and identical under the sim's FakeClock
# and in tests. (DST can stretch or shrink one individual gap by an hour; that
# is negligible against the slack the window adds and never changes which gap is
# the max.)
_CRONTAB_GAP_ANCHOR = datetime(2020, 1, 1, tzinfo=UTC)


def _crontab_max_gap_seconds(cadence: str) -> int:
    """The MAX gap, in seconds, between consecutive fires of a crontab cadence.

    Used by _sync_source_sla to give a crontab-cadence source the same kind of
    "cadence" an interval source has: the longest a healthy source can go
    between checks. Walking by fire TIMES (not fixed time steps) captures the
    steady-state max inter-fire gap for any cron shape -- a daily cron yields
    86400, a weekday-only cron yields 259200 (the Fri->Mon weekend gap, the one
    that MUST be captured so a weekday source does not false-alarm over a
    weekend), weekly/monthly/yearly their period.

    The trigger is built the SAME way scheduling builds it (_make_trigger ->
    CronTrigger.from_crontab) so the SLA cadence and the actual fire schedule can
    never disagree. The walk is anchored at the fixed _CRONTAB_GAP_ANCHOR and
    bounded by _CRONTAB_GAP_SPAN_SECONDS / _CRONTAB_GAP_FIRE_CAP (see those).

    The max gap is only PROVEN once the walk has spanned a full period
    (_CRONTAB_GAP_SPAN_SECONDS of fire time). Raises if the trigger yields no
    fire times, a non-positive max gap, OR the fire cap is exhausted before the
    span is reached (the period was never observed, so the max gap could be far
    too small -- e.g. a dense month-restricted cron whose 12000 fires all land in
    one month). In every raising case _sync_source_sla fails safe
    (skip-with-warning) rather than persist a bad/too-small bound.
    """
    trigger = _make_trigger(cadence)
    prev = trigger.get_next_fire_time(None, _CRONTAB_GAP_ANCHOR)
    if prev is None:
        raise ValueError(f"crontab cadence {cadence!r} yields no fire times")
    start = prev
    max_gap = 0.0
    fires = 0
    spanned = False
    # `now` is nudged one microsecond past `prev` so get_next_fire_time returns
    # the STRICTLY next fire, not `prev` again.
    while fires < _CRONTAB_GAP_FIRE_CAP:
        nxt = trigger.get_next_fire_time(prev, prev + timedelta(microseconds=1))
        if nxt is None:
            break
        gap = (nxt - prev).total_seconds()
        if gap > max_gap:
            max_gap = gap
        prev = nxt
        fires += 1
        if (prev - start).total_seconds() > _CRONTAB_GAP_SPAN_SECONDS:
            spanned = True
            break
    # The bound is trustworthy ONLY if the walk reached the span: it has then
    # seen at least one full ~400-day window, so the true max gap of any cron
    # whose period fits the span has been observed. If instead the fire cap ran
    # out first (a sub-hourly cron, or one that stalls at the DST fall-back) the
    # period was NOT observed -- refuse to bound it rather than emit a window
    # that could be far too small and false-alarm a healthy source.
    if not spanned:
        raise ValueError(
            f"crontab cadence {cadence!r} did not complete a full period within "
            f"{_CRONTAB_GAP_FIRE_CAP} fires (too dense, or it stalls at the DST "
            f"fall-back); its max gap is unproven, so it is left to the global "
            f"wedge backstop rather than bounded with a possibly-too-small window"
        )
    if max_gap <= 0:
        raise ValueError(
            f"crontab cadence {cadence!r} yields no positive inter-fire gap"
        )
    return int(max_gap)


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
    if _is_crontab_cadence(cadence):
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
