# M1 cross-slice integration fell — notes

Single daemon, all subsystems live, exercising the five interaction
risks per the brief. Findings, fixes, and discrimination evidence below.
Final test count: **127 passed** (was 120; 7 added).

## Risk 1 — hot-reload vs the live control socket

### Verdict: NO BUG.

### What was measured
A reload that removes immediate pipe `extra` was driven through
`LodgingReloader.process_pending_events` with `_cancel_pipe_loop`
monkeypatched to sleep 0.25s, parking `apply_lodging` at the
`await self._cancel_pipe_loop("extra")` point. Concurrent control ops
were dispatched during the parked window:

* `_op_replay({finding_id})` for a finding originally queued to
  `["now","extra"]` → returned `{"outcome":"already_queued","pipes":[]}`
  (the mandatory double-dispatch guard held); the replay used
  `set(self.lodging.pipes) = {"now"}`, the fully-NEW pipe set.
* `_op_dep_record({"name":"skein","status":"unhealthy"})` → succeeded;
  the now-finding it wrote also used `set(self.lodging.pipes)` and saw
  the new pipe set.
* `_op_health({})` → succeeded.

The race window WAS opened (verified by the test harness recording the
state mid-await):

      lodging.pipes during await:  {'now'}        # fully-NEW (atomic swap)
      pipe_drains during await:    {'now','extra'}# genuinely torn

`self.lodging = new_lodging` is one assignment at the top of
`apply_lodging` before any `await`, so a coroutine entering during a
parked await reads the new state atomically. The genuinely torn
structure is `self.pipe_drains` (mid pop + final re-point) — **no
control op reads `self.pipe_drains`**, so the torn structure is not
observable through the socket. Catalog calls in the handlers are
synchronous and self-committing, and each handler has no `await`
between an op-arg read and the catalog write (5b-1/5b-2 cancel-safety
property, preserved).

### Discriminating test
`tests/test_m1_integration.py::test_control_op_sees_coherent_lodging_during_slow_reload`

### Discrimination evidence
Inverted the atomic-swap property in `angelus/daemon.py` by moving
`self.lodging = new_lodging` from before the dependency-prune loop to
AFTER the pipe-removal loop (i.e. after the `await _cancel_pipe_loop`).
The test failed at:

      assert observed["lodging_during_await"] == {"now"}
      # got {"now","extra"} (still-OLD pipe set)

Reverted; the test passes deterministically.

---

## Risk 2 — hot-reload vs the dep registry

### Verdict: BUG FIXED.

### What was measured
With `dependencies/skein.yaml` lodged, called
`Catalog.record_dep_health("skein","unhealthy",…)` (as the
`dep_record` op would), then hot-removed the YAML and processed reload
events. Result before the fix:

      lodging.dependencies = {}              # entry gone, as expected
      dep_health (table)   = [("skein","unhealthy")]   # ORPHANED
      _op_health()["deps"] = [{"dependency_name":"skein",
                               "status":"unhealthy",...}]  # surfaced forever

`cli.py dep_check` for an unlodged name exits non-zero, so the orphan
can never receive another `dep_record` and never recovers. This is the
same orphan bug class slice 5a fixed for `observation_triage`
(`clear_triage_for_removed_triager`): a hot-removed lodging entry
leaving a frozen, unrecoverable row visible to operators.

### Fix
* `Catalog.delete_dep_health(name)` — synchronous, self-committing
  (matches the rest of the class; cancel-safe by construction).
* `AngelusDaemon.apply_lodging` now iterates
  `set(old.dependencies) - set(new_lodging.dependencies)` and calls
  `delete_dep_health` for each removed dependency. Done BEFORE any
  `await`, so the prune commits before cancellation can land.
  `all_dep_health` stays the mandatory reader (Contract D: written → has
  a reader in this slice).

### Discriminating test
`tests/test_m1_integration.py::test_dep_health_pruned_when_dependency_hot_removed`

Also: `test_dep_record_concurrent_with_dependency_reload_is_consistent`
proves the daemon-side `dep_record` write stays coherent (one row, one
now-finding) when the same dependency's file is being hot-reloaded.

### Discrimination evidence
Removed the prune loop from `apply_lodging`. The test failed at:

      assert daemon.catalog.all_dep_health() == []
      # got [{'dependency_name':'skein','status':'unhealthy',...}]

Reverted; test passes.

---

## Risk 3 — control socket shutdown with all subsystems live

### Verdict: BUG FIXED (two sites). NO deadlock / hang.

### What was measured

**Source-fire path** (the named live scheduled-fire path):

A real `AngelusDaemon.run()` was brought up with a forking source check
(`sleep 30 & echo $! > marker; wait`), and `request_stop()` was called
while the source-fire subprocess was mid-flight.

      shutdown latency:               ~1.0 s
      grandchild alive after stop:    YES   (LEAKED, before fix)

**Digest LLM path** (additional site found by integration):

Same shape but with a forking `horizon` stub on PATH and a 1-second
interval digest pipe. The digest job (APScheduler interval job) was
mid `_render_llm_body` when `request_stop()` fired.

      shutdown latency:               ~1.0 s
      horizon grandchild alive after: YES   (LEAKED, before fix)

### Why NO hang (the deadlock concern)
Read the APScheduler source in
`apscheduler/schedulers/asyncio.py`:

      def shutdown(self, wait=True):
          if not self.running: raise SchedulerNotRunningError
          self._shutdown(wait)        # <-- @run_in_event_loop:
                                      # call_soon_threadsafe -- NON-blocking

and `apscheduler/executors/asyncio.py`:

      def shutdown(self, wait=True):
          # There is no way to honor wait=True without converting this
          # method into a coroutine method
          for f in self._pending_futures:
              if not f.done():
                  f.cancel()
          self._pending_futures.clear()

So `AsyncIOScheduler.shutdown(wait=True)` does NOT block the event
loop. It schedules `_shutdown` via `call_soon_threadsafe`, and
`AsyncIOExecutor.shutdown` only `.cancel()`s pending job futures. There
is no scheduler-imposed multi-second hang and no deadlock with a
pipe-drain job mid-await on the same loop. Confirmed empirically:
~1 s shutdown latency end-to-end.

### Why the orphan
`AsyncIOExecutor.shutdown` cancels the APScheduler job task (e.g. a
source-fire task or a digest-drain task). The task is then run to
completion by `asyncio.run()`'s loop teardown, raising
`asyncio.CancelledError` at the inner `await asyncio.wait_for(
process.communicate(), ...)`. Pre-fix, only `TimeoutError` was caught
and routed through `_kill_and_reap`; `CancelledError` unwound the
coroutine without touching the child. With `start_new_session=True`,
the child plus its process group survived the daemon as an orphan
(reparented to init). Mirrors the original `_kill_and_reap` motivation
(the pipe-EOF timeout-wait hang) but on the cancellation axis.

### Fix
Symmetric to the existing timeout hardening, at every daemon-subprocess
site that can be reached by an APScheduler-job cancellation on
shutdown:

* `angelus/sources/runner.py` `run_shell_source` and `run_dep_check`:
  add `except asyncio.CancelledError: await _kill_and_reap(process);
  raise`. Updated `_kill_and_reap`'s docstring to name both axes
  (timeout AND cancel).
* `angelus/pipes/runner.py` `_render_llm_body`: added
  `start_new_session=True` (it was missing — the existing timeout path
  was already incomplete for a forking `horizon`), routed the timeout
  path through `_kill_and_reap` for the process-group reap, added the
  `CancelledError` arm that reaps and re-raises.
* `angelus/channels/push.py` `send_push` and
  `angelus/channels/email.py` `send_email`: same uniform fix
  (`start_new_session=True` + `_kill_and_reap` on timeout + cancel arm).
  A digest job that reaches send before the LLM body cancellation
  window closes would otherwise orphan a `notify-pat`/`patbot-email`
  subprocess; uniformity also removes the latent pre-existing
  non-process-group weakness those two had even on timeout.

`_kill_process_group` SIGKILLs the group synchronously BEFORE the
bounded await reap, so even if the cancel handler's own await is
re-cancelled by loop teardown the group is already dead — no orphan,
worst case a defunct entry.

### Discriminating tests
* `tests/test_m1_integration.py::test_full_daemon_shutdown_is_bounded_and_reaps_source_subprocess`
* `tests/test_m1_integration.py::test_full_daemon_shutdown_reaps_digest_llm_subprocess`

Both assert `shutdown latency < 8s` (no hang) AND
`grandchild not alive after stop` (no orphan).

### Discrimination evidence
* Source: removed the `except asyncio.CancelledError: …` arm from
  `run_shell_source`. The source test failed at
  `raise AssertionError(f"grandchild {gc_pid} survived daemon shutdown")`.
* Digest: removed the `except asyncio.CancelledError: …` arm from
  `_render_llm_body`. The digest test failed at the analogous
  `horizon grandchild ... survived shutdown` assertion.

Both reverted; both tests pass.

---

## Risk 4 — mute consultation vs hot-reloaded pipes

### Verdict: NO BUG (snapshot stays internally consistent; mute coherent).
###          FINDING + FIX (misleading docstring in PipeDrain).

### What was measured
Two successive reloads were applied with `_cancel_pipe_loop`
monkeypatched slow:

1. Add channel `log` — re-points `drain.channels` to a NEWER generation.
2. Remove immediate pipe `extra` — parks `apply_lodging` at the
   `await _cancel_pipe_loop("extra")` point.

During the parked window, a fresh `drain_once` on the unchanged `now`
pipe was driven with a spy that records the snapshot under lock.
Observed snapshot:

      pipe.channels: ['push']                # OLD now pipe object
      channels:      ['log','push']          # NEW channels dict
      known_pipes:   ['extra','now']         # OLD known_pipes
                                             # (re-pointed after the await)

The snapshot is MIXED-generation, but the cross-ref single-entry
invariant guarantees `set(pipe.channels) <= set(channels)` always, so
`_drain_immediate`'s `channels[channel_name]` cannot `KeyError`. A muted
finding still records exactly one `(muted)` dispatch — `is_muted` is
keyed on the finding's `dedup_key`, independent of the snapshot.

A targeted inversion (`drain.pipe = dataclasses.replace(new_pipe,
channels=new_pipe.channels + ["__ghost__"])`) injects a genuinely torn
pipe referencing an unknown channel and the subset assertion fires,
proving the test detects incoherence even though the real code never
produces it.

### Why it is safe even though `drain.lock` is not taken by apply_lodging
* `drain_once`'s top-of-method snapshot reads `pipe`, `channels`,
  `known_pipes` in three consecutive await-free statements, so the
  event loop cannot interleave `apply_lodging` mid-snapshot.
* `apply_lodging` only swaps `drain.pipe` for a *new* `Pipe` object on a
  pipe that is simultaneously being torn down (removed or cadence-moved
  off immediate). The pipe's loop is being cancelled, so no fresh
  `drain_once` for that pipe interleaves.
* Reloads are single-entry and cross-ref-validated, so for any pipe
  drained during the await window, the pipe's channels are a subset of
  the channels dict of either reload generation.

### Finding & fix
The pre-fell `PipeDrain` docstring claimed the lock provided the
consistent view, which is false (`apply_lodging` never takes
`drain.lock`). Replaced with the real safety argument (three properties
above). No code change to the locking — taking `drain.lock` in
`apply_lodging` would stall reloads on a long in-flight digest (≤120s
horizon subprocess), a regression worse than the latent fragility the
current invariant already rules out.

### Discriminating test
`tests/test_m1_integration.py::test_drain_snapshot_stays_internally_consistent_during_slow_reload`

### Discrimination evidence
Injected a genuinely torn pipe via a temporary
`dataclasses.replace(new_pipe, channels=…+["__ghost__"])` in
`apply_lodging`'s pipe-rename loop. The test failed at:

      assert set(pipe_channels) <= set(channels_dict)
      # got pipe.channels={'log','push','__ghost__'}
      # not subset of channels={'log','push'}

Reverted; the test passes.

---

## Risk 5 — a `dependency_unhealthy` finding is itself muteable

### Verdict: PRODUCT DECISION. See `INTEGRATION_FELL_RISK5.md`.

### What was measured / locked in
The collision is documented in `INTEGRATION_FELL_RISK5.md` (mechanism,
both readings, recommendation). No product behaviour was changed.

The integration test verifies the **saving rail** — the muted
`internal/dep:dependency_unhealthy:iotaschool` finding is silenced on
the `now` channel (a `(muted)` dispatch is recorded, no push goes out)
AND the dependency is still reported `unhealthy` by the `health` op's
`deps` block.

### Discriminating test
`tests/test_m1_integration.py::test_muted_unhealthy_dep_is_silent_on_now_but_visible_in_health`

### Discrimination evidence
Inverted `_op_health` to mute-filter the `deps` it returns:

      "deps": [d for d in self.catalog.all_dep_health()
               if not self.catalog.is_muted(
                   f"internal/dep:dependency_unhealthy:{d['dependency_name']}")],

The test failed at the `deps["iotaschool"]` lookup with `KeyError:
'iotaschool'` — the health op no longer surfaced the muted unhealthy
dep. Reverted; the test passes.

---

## Files touched

Product code:

* `angelus/sources/runner.py` — Risk 3 source fix (cancel arm,
  docstring).
* `angelus/pipes/runner.py` — Risk 3 digest fix (`start_new_session`,
  `_kill_and_reap` on timeout + cancel) + Risk 4 docstring correction.
* `angelus/channels/push.py` — Risk 3 uniform fix.
* `angelus/channels/email.py` — Risk 3 uniform fix.
* `angelus/storage/catalog.py` — Risk 2 `delete_dep_health` writer.
* `angelus/daemon.py` — Risk 2 `apply_lodging` dep_health prune.

Tests:

* `tests/test_m1_integration.py` (NEW, 7 tests).

Docs:

* `INTEGRATION_FELL_RISK5.md` (NEW).
* `FELL_NOTES.md` (this file, NEW).

## Unresolved

None.

## Round 2 — readonly fell findings + fixes

A strict readonly fell against the round-1 diff filed three blocking
findings. All three addressed in this same shard.

### issue-20260519-e5hr — non-discriminating test removed

Round-1 added `test_dep_record_concurrent_with_dependency_reload_is_consistent`.
The readonly fell flagged it as sequential masquerading as concurrent:
`_op_dep_record`'s body has zero awaits (verified in `angelus/daemon.py`;
the op's own docstring states the property), so `await daemon._op_dep_record(...)`
runs to completion without yielding to the event loop. A reload task created
beforehand with `asyncio.create_task` cannot interleave inside `_op_dep_record`.

Decided option (b): no real concurrent window exists inside `_op_dep_record`
that is worth a separate test. dep_record is structurally one of the
control ops whose `set(self.lodging.pipes)` read is exactly the lodging
read Risk 1 exercises via replay. The test is replaced with a comment
in `tests/test_m1_integration.py` pointing to
`test_control_op_sees_coherent_lodging_during_slow_reload` and stating
why the dep-specific test reduced to that one. Test count: 127 → 126.

Discrimination evidence for the comment: not applicable (deletion).
The pointed-to test (Risk 1) is itself discriminating — round-1 inversion
already recorded above: moving `self.lodging = new_lodging` after the
`await _cancel_pipe_loop` makes it fail at `{'extra','now'} != {'now'}`.

### issue-20260519-e6xz — PipeDrain docstring property (2) corrected

Round-1 PipeDrain `__init__` docstring claim (2) stated drain.pipe is
only swapped on pipes simultaneously being torn down or moved off
immediate cadence. Reading `apply_lodging`: `drain.pipe = new_pipe`
runs UNCONDITIONALLY in the intersection loop, not gated on cadence
change or removal. The actual safety, in the common content-only edit
case, is that apply_lodging has no awaits at all; in the multi-pipe
cadence-change case, an `await self._cancel_pipe_loop(...)` for another
pipe can interleave a drain_once on an untouched drain whose .pipe is
already new but .channels/.known_pipes are still old. That mixed-
generation snapshot stays safe by property (3): single-entry +
cross-ref-validated reload makes any pipe's channels a subset of the
channels dict of the same reload generation, and the existing
`test_drain_snapshot_stays_internally_consistent_during_slow_reload`
pins the cross-generation case empirically. Rewritten the property (2)
text to state this accurately.

Discrimination evidence: no test change; the existing inversion (ghost
channel into new_pipe.channels) still fails the subset assertion.

### issue-20260519-93p7 — _kill_and_reap docstring caller list de-rotted

After round-1 the helper was wired into five callers (sources/runner
twice, channels/push, channels/email, pipes/runner) but the docstring
still said "Called from … run_shell_source / run_dep_check." Rephrased
to a durable area-level statement (sources / channels / pipes) with an
explicit note that the prior name enumeration rotted the moment new
sites adopted the helper, so new sites should adopt the helper rather
than grow yet another shape. No code change beyond the docstring.

### Round 2 final pytest

`PYTHONPATH=$PWD python -m pytest` — **126 passed**. (127 → 126 by
design: the non-discriminating test was removed without a replacement,
the comment in its place explains why.)

### Original three real bug fixes — still intact

* Risk 2 dep_health prune: `Catalog.delete_dep_health` + apply_lodging
  prune loop unchanged. Covered by
  `test_dep_health_pruned_when_dependency_hot_removed`.
* Risk 3 CancelledError + `_kill_and_reap` at five subprocess sites:
  unchanged except for the docstring at the helper site itself.
  Covered by `test_full_daemon_shutdown_is_bounded_and_reaps_source_subprocess`
  and `test_full_daemon_shutdown_reaps_digest_llm_subprocess`.
* All three original inversion records (Risk 1, 2, 3, 4) still hold
  against the post-round-2 code.

## Round 3 — soft-fell rescue caught (issue-20260519-df5h)

Round-3 readonly fell of the round-2 delta came back CLEAN on e5hr /
e6xz / 93p7 but enumerated a separate docstring inaccuracy in
`_kill_and_reap`: the prior round-2 wording said "every daemon-driven
subprocess site" and listed dep-check among them, but `run_dep_check`
is invoked from `cli.py` by cron (a CLI process, not the daemon). The
fell self-talked-down from filing ("below my threshold"). The brief is
strict that anything worth flagging is a finding and "no middle
category"; the self-talk-down is the soft-fell pattern Patrick rejects.
spook-0519 filed `issue-20260519-df5h` and fixed it in `1b426e6`:
rephrased to "every subprocess site angelus runs" with a per-site
cancellation-source paragraph (daemon shutdown for source-fire and
pipe-digest, drain-task cancel for push/email, operator interrupt for
the cron-fired dep-check probe). No code change, docstring only.

## Round 4 — sixth subprocess site missed (issue-20260519-n59k)

The round-3 docstring broadening from "every daemon-driven" to "every
subprocess site angelus runs" claimed universal coverage and was
contradicted by a sixth site round-1 had missed: `run_python_triager`
in `angelus/triage/runner.py`. It was using naive `process.kill()` +
`process.wait()` on timeout, had no `start_new_session=True`, no
`CancelledError` arm, and routed through neither `_kill_and_reap` nor
`_kill_process_group`. Round-4 fell filed `issue-20260519-n59k`.

Writing the discriminating test surfaced a second, more fundamental
bug: `_triage_loop`'s finally gathered the in-flight triager tasks
WITHOUT cancelling them. The existing Risk-3 source-fire and
digest-LLM tests pass because APScheduler's executor cancels their
tasks externally on shutdown; the triage loop has no such external
canceller. A triager stuck in `process.communicate()` hangs daemon
shutdown indefinitely (the test fails with TimeoutError on
`await asyncio.wait_for(task, timeout=15.0)`). The CancelledError arm
in `run_python_triager` never fires because the wrapping task is
never cancelled.

### Fix (round 4, single logical change across coupled files)

* `angelus/triage/runner.py` `run_python_triager`: imported
  `_kill_and_reap` from `angelus.sources.runner`; added
  `start_new_session=True` to `create_subprocess_exec`; replaced the
  naive timeout kill with `await _kill_and_reap(process)`; added the
  `except asyncio.CancelledError: await _kill_and_reap(process); raise`
  arm.
* `angelus/daemon.py` `_triage_loop`: finally block now cancels every
  in-flight triager task before gathering, so the CancelledError arm
  actually fires and the subprocess tree is reaped before shutdown
  returns.
* `angelus/sources/runner.py` `_kill_and_reap` docstring: area
  enumeration extended to include triage/ alongside sources, channels,
  pipes.

### Discriminating test

`tests/test_m1_integration.py::test_full_daemon_shutdown_reaps_python_triager_subprocess`.

Two-axis discrimination (both inversions performed in the worktree,
each reverted after observation):

* Invert the cancel-before-gather in `_triage_loop`'s finally
  (restore the round-1 gather-only shape) -> the test fails at
  `await asyncio.wait_for(task, timeout=15.0)` with a TimeoutError —
  shutdown hangs because the triager task is never cancelled and the
  CancelledError arm never fires.
* Restore the cancel loop, then remove the `except asyncio.CancelledError`
  arm from `run_python_triager` -> the test fails at the post-driver
  poll: the sleep grandchild survives shutdown
  (`AssertionError: triager grandchild N survived shutdown -- cancelled
  python triager subprocess was orphaned`).

Both axes are necessary; either one alone leaves a real bug. After
restore, 127 passed in 27.44s.

### Final state

Risk 3 hardening now covers SIX subprocess sites uniformly: source-fire,
dep-check probe, push channel, email channel, digest LLM render, and
python triager. The cancel-before-gather pattern is documented inline
in `_triage_loop`. The `_kill_and_reap` docstring's area enumeration
matches the code.
