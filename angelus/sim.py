"""Offline sim harness for a full source -> dispatch cycle (B26).

The daemon's pipeline is a set of reusable step units that the production
async loops and the APScheduler jobs drive (a source fire, a triage pass, a
pipe drain). Tests already drive those units directly with no scheduler and no
real time -- see tests/test_m2_multicadence.py, whose docstring spells it out:
"source fires are invoked directly via daemon._fire_source ... so the
production cadences run in milliseconds." This module packages exactly that
pattern into a reusable harness so a scenario can run "a simulated day offline
in seconds" (master brief brief-20260529-fv9n, B26).

The harness pins time with a FakeClock (B24) and forces work on demand with the
same step methods production uses (B25's _fire_source / drain plus the shared
_discover_ready_triage sweep). It NEVER starts the scheduler, the control
socket, or any background loop -- constructing an AngelusDaemon is inert by
construction (those all live in AngelusDaemon.run(), which the harness never
calls), so a sim cannot drift from production by reimplementing pipeline logic.
A sim that triaged or drained differently than the daemon would be worse than
no sim, so every step below reuses a production code path verbatim.

B27 (scenario-fixtures) is the real consumer: it drives a SimHarness directly
from pytest, one scenario per fragility class. The CLI (`angelus sim`) is a
thin wrapper for an ad-hoc scripted run. The harness is therefore the core
deliverable; build a scenario as a short, legible script of harness calls.

Dry-run is guaranteed by the harness, not left to the caller: on construction
it sets ANGELUS_DRY_RUN=1 (restored on close, with a weakref-finalizer backstop
that restores it on GC / interpreter exit even if the caller forgets both
``with`` and close()), so every channel send writes a line to ``dispatches.log``
instead of shelling ``notify-pat`` / email. A scenario can never accidentally
page a real phone. Triagers still run as real subprocesses -- faithful and fine
offline.
"""

from __future__ import annotations

import os
import weakref
from datetime import datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any

from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon
from angelus.pipes import DrainSummary

_DRY_RUN_ENV = "ANGELUS_DRY_RUN"


def _restore_env(key: str, prior: str | None) -> None:
    """Restore an env var to a captured prior value -- None means it was
    unset and is popped, otherwise it is put back verbatim. Module-level (not
    a bound method) so a weakref finalizer can hold it without keeping the
    SimHarness alive."""
    if prior is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = prior


class SimHarness:
    """Drive a full source -> dispatch cycle offline, under a pinned clock.

    Construct against a root dir with a lodging layout (a tmp/scratch dir for a
    scenario, or a copy of a real deployment's config). The harness builds an
    AngelusDaemon with a FakeClock pinned to ``start`` and runs the daemon's
    startup-recovery / clear steps for a clean catalog -- but never run(), so no
    scheduler, control socket, or loop is started.

    Usable as a context manager (``with SimHarness(root, start) as sim:``) so
    the dry-run env override and the sqlite connection are released
    deterministically; or call ``close()`` explicitly.

    The async step primitives (fire_source / run_triage / drain / health) are
    coroutines because the production paths they reuse are async; a scenario
    awaits them inside one ``asyncio.run`` driver. Nothing sleeps -- simulated
    time moves only when the scenario calls set_time / advance.
    """

    def __init__(self, root: Path, start: datetime) -> None:
        self.root = Path(root)
        # The pinned clock. Injected into AngelusDaemon's constructor, which
        # threads it into the catalog and every PipeDrain in that same __init__
        # (B26 seam) -- so this one object is the single notion of "now" for
        # every timestamp, since-last-drain window, and rendered digest date in
        # the sim. Held here too so set_time / advance can move it.
        self.clock = FakeClock(start)
        # Guarantee dry-run for this process before any drain can send. The
        # channel wrappers (channels/push.py, channels/email.py) read
        # ANGELUS_DRY_RUN at send time, so setting it here -- before the
        # scenario triggers a drain -- routes every send to dispatches.log.
        # Prior value is restored on close so the harness leaves the env as it
        # found it (important when many scenarios run in one pytest process).
        self._prior_dry_run = os.environ.get(_DRY_RUN_ENV)
        os.environ[_DRY_RUN_ENV] = "1"
        # Backstop: register the restore as a weakref finalizer the instant
        # after the env is mutated. A caller who constructs the harness without
        # `with` and never calls close() would otherwise leak ANGELUS_DRY_RUN=1
        # into the whole process (a latent footgun for the B27 consumer). The
        # finalizer runs the restore when the harness is garbage-collected or at
        # interpreter exit, so the leak is impossible regardless of how the
        # caller manages the object. It captures only the prior value (str|None)
        # via _restore_env, never self, so it does not keep the harness alive;
        # weakref.finalize guarantees it runs at most once, so the explicit
        # close() path and the GC path never double-restore. Registered before
        # building the daemon so even a constructor failure restores the env.
        self._restore_dry_run = weakref.finalize(
            self, _restore_env, _DRY_RUN_ENV, self._prior_dry_run
        )
        self.daemon = AngelusDaemon(self.root, clock=self.clock)
        self._closed = False
        self._startup_recovery()

    # -- lifecycle ---------------------------------------------------------

    def _startup_recovery(self) -> None:
        """Mirror the catalog recover/clear block AngelusDaemon.run() performs
        at startup, so a harness built on a reused root starts from the same
        clean state a freshly-started daemon would -- without starting the
        scheduler, control socket, or any loop (the rest of run()).

        These are the synchronous, loop-free catalog calls run() makes before
        ``self.scheduler.start()``; the orphan-reconciliation and job-
        registration that follow it are scheduler/loop concerns the sim does
        not have. On a fresh tmp root every call is a no-op, but a B27 scenario
        that reuses a root (a restart-recovery scenario, say) gets faithful
        startup semantics.
        """
        self.daemon.catalog.recover_writing_rows()
        self.daemon.catalog.recover_triage_processing_rows()
        self.daemon.catalog.clear_channel_health()
        self.daemon.catalog.clear_digest_channel_attempts()
        self.daemon.catalog.clear_immediate_channel_attempts()

    def close(self) -> None:
        """Close the sqlite connection and restore the dry-run env override.
        Idempotent."""
        if self._closed:
            return
        self._closed = True
        self.daemon.connection.close()
        # Run the finalizer explicitly for a deterministic restore now. It is
        # idempotent (weakref.finalize fires at most once) and detaches itself
        # after running, so the GC backstop won't restore a second time.
        self._restore_dry_run()

    def __enter__(self) -> SimHarness:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> bool:
        self.close()
        return False

    # -- time control ------------------------------------------------------

    def set_time(self, instant: datetime) -> None:
        """Pin the simulated clock to an absolute instant (FakeClock.set)."""
        self.clock.set(instant)

    def advance(self, delta: timedelta) -> None:
        """Jump the simulated clock forward by ``delta`` (FakeClock.advance).
        No real sleep happens -- a day passes in microseconds."""
        self.clock.advance(delta)

    # -- step primitives (each reuses a production path) -------------------

    async def fire_source(self, name: str) -> tuple[int, str]:
        """Run a lodged source's shell check once and write its observation,
        via the exact path APScheduler and the `fire_source` op use
        (AngelusDaemon._fire_source). Returns (observation_id, outcome) where
        outcome is "ok" or "check_failed".

        Raises KeyError on an unknown source -- a scenario typo should fail
        loudly rather than silently do nothing.
        """
        if name not in self.daemon.lodging.sources:
            raise KeyError(f"unknown source: {name}")
        result = await self.daemon._fire_source(name)
        # _fire_source returns None only on the hot-removed race, which cannot
        # happen in a single-threaded sim with no reloader running.
        assert result is not None, name
        return result

    def inject_observation(
        self,
        source_ref: str,
        payload: dict[str, Any],
        meta: dict[str, Any] | None = None,
    ) -> int:
        """Write a raw observation with no shell source -- the same
        catalog.write_observation call _fire_source makes internally -- for a
        scenario that wants to inject an observation directly (e.g. a fixed
        payload it controls, with no check command to lodge). Returns the new
        observation id; the next run_triage() picks it up like any other ready
        observation.
        """
        provenance = meta or {"source": source_ref, "check": "injected"}
        return self.daemon.catalog.write_observation(
            source_ref, payload, provenance
        )

    async def run_triage(self) -> int:
        """Run ONE full triage pass over every currently-ready observation to
        completion, then return the number triaged.

        Reuses the daemon's shared _discover_ready_triage sweep (which marks
        each ready observation 'processing') and its per-observation
        _triage_under_semaphore path (semaphore + per-triager lock + the
        cancel/hot-remove safety) -- the exact production execution, awaited to
        completion here rather than spawned-and-reaped as the live loop does.

        Completeness is the determinism contract: every observation that was
        ready is fully triaged and its findings written before this returns, so
        a following drain() sees them and the scenario is not flaky. One sweep
        is exhaustive because triagers consume observations and emit findings
        (never new observations), so nothing the pass produces becomes newly
        triage-ready -- observations only appear via fire_source / inject, which
        the scenario drives explicitly.
        """
        work = self.daemon._discover_ready_triage()
        for row, triager_name in work:
            await self.daemon._triage_under_semaphore(row, triager_name)
        return len(work)

    async def drain(self, pipe: str) -> DrainSummary:
        """Drain a named pipe now and return its DrainSummary (B25).

        Reuses AngelusDaemon._run_drain_job -- the exact body the scheduler and
        the `drain` op run, including the _drain_tasks tracking -- so the sim
        drain is indistinguishable from a scheduled one. Any pipe kind works;
        drain_once branches to the immediate or digest path itself. Under
        dry-run every send lands in dispatches.log.

        Raises KeyError on an unknown pipe, matching fire_source's loud-failure
        contract.
        """
        if pipe not in self.daemon.pipe_drains:
            raise KeyError(f"unknown pipe: {pipe}")
        summary = await self.daemon._run_drain_job(pipe)
        # None only on the hot-removed race, impossible in the sim (no reloader).
        assert summary is not None, pipe
        return summary

    # -- inspectors (read off the catalog / health surface) ----------------
    #
    # Thin pass-throughs to existing catalog readers and the health op so a
    # scenario asserts against the same data production reports -- no new query
    # logic here.

    def open_incidents(self) -> list[dict[str, Any]]:
        """Open incidents (catalog.open_incidents)."""
        return self.daemon.catalog.open_incidents()

    def findings_for_pipe(
        self, pipe: str, since: str | None = None
    ) -> list[dict[str, Any]]:
        """Ready, non-suppressed findings queued to ``pipe`` since ``since``
        (catalog.findings_for_pipe_since). ``since`` None means all-time."""
        return self.daemon.catalog.findings_for_pipe_since(pipe, since)

    def dead_letter_count(self) -> int:
        """Count of permanently-undelivered queue rows (catalog.dead_letter_count)."""
        return self.daemon.catalog.dead_letter_count()

    def dead_letter_items(
        self, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Dead-lettered findings, oldest-abandoned-first (catalog.dead_letter_items)."""
        return self.daemon.catalog.dead_letter_items(limit)

    async def health(self) -> dict[str, Any]:
        """The full health surface the daemon reports over its control socket
        (AngelusDaemon._op_health). Next-fire times read None in the sim
        because the scheduler is never started; everything else is live.
        """
        return await self.daemon._op_health({})

    def dispatches(self) -> list[str]:
        """The lines written to ``dispatches.log`` by the dry-run send path --
        one per delivered send. Empty list if nothing has been dispatched (or
        the file does not exist yet). This is the offline stand-in for a real
        notification: a scenario asserts a send "landed" by finding its line
        here, proving notify-pat / email was never shelled.
        """
        log_path = self.root / "dispatches.log"
        if not log_path.exists():
            return []
        return [
            line
            for line in log_path.read_text(encoding="utf-8").splitlines()
            if line
        ]
