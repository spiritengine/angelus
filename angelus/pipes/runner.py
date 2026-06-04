"""Pipe rendering and draining."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from angelus.channels import send_email, send_push
from angelus.clock import Clock
from angelus.lodging import Channel, Pipe
from angelus.sources.runner import _kill_and_reap
from angelus.storage import Catalog

LOGGER = logging.getLogger(__name__)

# Prior wording said "see structured data above" -- correct under the
# old (preamble, body) order. After the cleanup reversed to (body,
# preamble), the structured data is BELOW the synthesis paragraph, so
# this message points downward. If a future change re-reverses the
# order, flip this string too. fell-r1 BLOCK #1.
LLM_FALLBACK_FOOTER = "LLM digest body unavailable — see structured data below."

# Where each digest drain stages the chronicler prompt, and how many to keep.
DEFAULT_DIGEST_STAGING_DIRNAME = "digest-staging"
DEFAULT_DIGEST_STAGING_KEEP = 30

# Defense-in-depth backstop (B30): the per-collection item budget for every
# digest input. The B30 emission gate stops floods at the source, so in normal
# operation no collection comes close to this; the cap only bites if some
# upstream path ever produces a runaway list (the 2026-06-01 15MB chronicler
# prompt came from ~114k findings_since_last_drain rows). Each over-budget
# collection is truncated to this many items with a trailing marker row so the
# omission is visible in both the preamble and the chronicler prompt.
DEFAULT_DIGEST_MAX_ITEMS_PER_INPUT = 200

# Per-section item budget for the compact push (telegram) digest. Telegram caps
# a message at 4096 chars and notify-pat splits longer bodies on newlines, so
# the push leg lists at most this many items per section and prints a "+N more"
# tail; the full item list always rides the email leg. Compact is a heartbeat-
# plus-headlines summary, not the full report.
DEFAULT_COMPACT_MAX_ITEMS_PER_SECTION = 10

# Env var holding the healthchecks.io (or any URL) dead-man ping for the daily
# digest. Pinged once per successful digest drain so an off-box third party
# alerts if the digest ever stops firing (the "digest silently never ran"
# gap belfry can't see). Unset -> the ping is skipped, so the feature is inert
# until an operator provisions the check and exports the URL.
DIGEST_HEARTBEAT_URL_ENV = "ANGELUS_DIGEST_HEARTBEAT_URL"

# Per-operation socket timeout for the dead-man ping (passed to urlopen, which
# applies it to each connect/recv -- NOT as a single total wall-clock bound).
# Kept short because the ping is best-effort and runs inside the digest drain,
# which the daemon awaits on shutdown: a fast endpoint (healthchecks.io) returns
# well within this, so the drain (and shutdown's bounded await of it) is not
# delayed in practice. A pathological trickle from the operator's own healthcheck
# URL is the only residual -- bounded per-recv, capped read below, accepted.
DIGEST_HEARTBEAT_TIMEOUT_SEC = 5.0


class PipeDrain:
    def __init__(
        self,
        catalog: Catalog,
        pipe: Pipe,
        channels: dict[str, Channel],
        workdir: Path,
        known_pipes: set[str],
        clock: Clock | None = None,
    ) -> None:
        self.catalog = catalog
        # Time seam (B24). Defaults to the catalog's clock so the runner's
        # drain windows / subject date and the rows the catalog stamps share
        # one notion of "now"; the daemon passes its shared clock explicitly
        # and tests pass a FakeClock.
        self._clock = clock or catalog._clock
        # `pipe`, `channels`, and `known_pipes` are mutated across hot-reloads
        # by AngelusDaemon.apply_lodging, which does NOT take self.lock --
        # so the lock is NOT what serialises a drain against a reload (it
        # only serialises drain_once calls with each other and with the
        # per-pipe loop's re-entrancy). What actually keeps a drain's view
        # consistent is three properties together: (1) drain_once reads the
        # three fields in consecutive await-free statements, so the event
        # loop cannot interleave apply_lodging mid-snapshot; (2) inside
        # apply_lodging itself drain.pipe is reassigned UNCONDITIONALLY for
        # every pipe in the old/new intersection (one statement near the
        # top of the intersection loop, not gated on cadence change or
        # tear-down), and the bottom for-loop re-points every drain's
        # channels/known_pipes afterwards; the only `await` points inside a
        # single apply_lodging invocation are the `await self._cancel_pipe_loop(...)`
        # calls in remove and cadence-change branches, so in the common
        # content-only edit case apply_lodging has no awaits at all and a
        # drain_once cannot interleave any of its mutations -- when other
        # pipes in the same reload DO trigger a cancel await, a drain_once
        # on an untouched pipe may snapshot a (new-pipe, old-channels,
        # old-known_pipes) mix, which is safe by (3); (3) a reload is
        # single-entry and cross-ref-validated, so any pipe's channels are
        # a subset of the channels dict of the same reload generation, and
        # the test test_drain_snapshot_stays_internally_consistent_during_slow_reload
        # pins this empirically (inverting it by injecting a ghost channel
        # into new_pipe.channels at the intersection loop fails the subset
        # assertion). The is_muted check is keyed by the finding's
        # dedup_key, independent of this snapshot, so reload churn cannot
        # make it incoherent.
        self.pipe = pipe
        self.channels = channels
        self.workdir = workdir
        self.known_pipes = known_pipes
        self.lock = asyncio.Lock()

    async def drain_once(self) -> None:
        async with self.lock:
            pipe = self.pipe
            channels = self.channels
            known_pipes = self.known_pipes
            if pipe.render_kind == "digest":
                await self._drain_digest(pipe, channels, known_pipes)
                return
            await self._drain_immediate(pipe, channels, known_pipes)

    async def _drain_immediate(
        self,
        pipe: Pipe,
        channels: dict[str, Channel],
        known_pipes: set[str],
    ) -> None:
        rows = self.catalog.pending_pipe_items(pipe.name)
        for row in rows:
            finding_id = int(row["id"])
            message = self._render(pipe, row)
            subject = f"[angelus] {row['entity']}: {row['type']}"
            # internal/* findings bypass the immediate rate-limit suppression
            # entirely -- they must never be shunted off `now` onto the daily
            # digest. An internal finding IS the system's distress signal
            # (a dep down, a channel unhealthy, a render failed); deferring it
            # to a once-a-day digest means it never pages immediately and, on
            # this path, never reaches the fan below -- and the digest may
            # itself be the broken thing. The rate limit predates the fan and
            # mis-handles internal findings two ways: (a) per_source double-
            # counts, because every fanned channel records a `sent` dispatch
            # stamped with the finding's source, so each internal finding
            # burns 2+ of the 4/hr budget; (b) per_channel keys on
            # pipe.channels (push only), so during an alert storm push hitting
            # 6/hr suppresses later findings even though email is wide open.
            # The B30 emission gate (drops repeats of the same
            # source/type/entity) is the correct flood control for internal
            # findings; distinct internal findings -- several deps down,
            # push+email both unhealthy -- are routine and must all get out.
            # Detection is the same domain-agnostic source-prefix check the
            # fan uses (_is_internal), factored once so the two cannot drift.
            if not _is_internal(row["source"]) and self._over_rate_limit(pipe, row):
                self.catalog.suppress_pipe_item_to(
                    finding_id,
                    pipe.name,
                    pipe.rate_limit["overflow"],
                )
                continue
            if self.catalog.is_muted(row["dedup_key"]):
                # Mute silences the immediate/now alert path only. Record
                # a 'muted' dispatch so the decision stays auditable
                # (slice-3 issue-20260514-wh1k: every dispatch decision
                # leaves a row), then mark this pipe item handled exactly
                # as the success path does (pipe_queues -> 'dispatched')
                # so it does not reappear in pending_pipe_items on the
                # next drain. We do NOT mark it 'suppressed': suppressed
                # is the rate-limit-overflow state the daily digest reads;
                # a muted finding must not surface there.
                #
                # Incident lifecycle is untouched here by construction,
                # not by added code: _upsert_incident runs at
                # write_finding time, BEFORE any dispatch, so suppressing
                # dispatch cannot affect an incident's open/close state.
                #
                # Scope: _drain_immediate only. _drain_digest is
                # deliberately NOT mute-checked -- the daily digest is the
                # consolidation/audit surface and must stay complete. A
                # finding targeting `now` has its own pipe_queues row;
                # marking that row does not touch a separate `daily` row,
                # so the digest is unaffected by construction. Filtering
                # the digest too would also risk the cross-zone dedup trap
                # that bit slice 3.
                self.catalog.record_dispatch(
                    pipe.name,
                    "(muted)",
                    [finding_id],
                    "muted",
                    source=row["source"],
                )
                self.catalog.mark_pipe_items_dispatched(
                    pipe.name, [finding_id]
                )
                continue
            # B7 fans internal/* findings to N channels through ONE pipe_queues
            # row. That conflated two concerns the old single-counter path could
            # not separate: per-FINDING redelivery (should THIS finding retry
            # later?) and per-CHANNEL health escalation (is THIS channel
            # unhealthy?). They are now split -- per-channel escalation runs off
            # immediate_channel_attempts inside the loop; per-finding redelivery
            # is reconciled ONCE after the loop from these two flags, so the
            # single pipe_queues row advances at most one step per drain
            # regardless of how many channels failed (no +N inflation).
            delivered = False  # did >=1 live channel get this finding out?
            last_error: str | None = None  # last attempted-channel failure, if any
            for channel_name in self._dispatch_channels(pipe, row, channels):
                if self.catalog.is_channel_unhealthy(channel_name):
                    # An unhealthy channel is SKIPPED, not attempted -- so it is
                    # not a delivery attempt and must not advance the finding's
                    # redelivery ladder below (last_error stays None on a
                    # skip-only drain). Pre-fan behaviour preserved: a skipped
                    # sole channel leaves the finding pending for a later drain
                    # or a daemon restart, never burns its retry budget.
                    continue
                channel = channels[channel_name]
                try:
                    await self._send_channel(channel, message, subject)
                except Exception as exc:
                    last_error = str(exc)
                    # Per-CHANNEL health escalation, driven by the per-(pipe,
                    # channel) counter -- NOT the finding's pipe_queues row. This
                    # is what makes a co-fanned channel's failures ladder to
                    # threshold even when another channel succeeded and marked
                    # the finding delivered (defect b; see
                    # record_immediate_send_failure).
                    channel_exhausted = self.catalog.record_immediate_send_failure(
                        pipe.name,
                        channel.name,
                        finding_id,
                        last_error,
                    )
                    if channel_exhausted:
                        # THIS channel's retries are exhausted: it is now marked
                        # unhealthy and an internal/dispatch finding is written.
                        # Log at ERROR -- a delivery the system has given up on
                        # for this channel (B22). The finding itself may still be
                        # delivered over another (live) fanned channel this drain.
                        LOGGER.error(
                            "pipe %s: channel %s exhausted retries (finding %s "
                            "was the latest failure); marking channel unhealthy: "
                            "%s",
                            pipe.name,
                            channel.name,
                            finding_id,
                            exc,
                        )
                        self.catalog.write_internal_finding(
                            "internal/dispatch",
                            "channel_unhealthy",
                            channel.name,
                            last_error,
                            known_pipes,
                        )
                    else:
                        # This channel will retry on a later drain. WARNING, not
                        # ERROR -- a single transient failure is expected to
                        # recover.
                        LOGGER.warning(
                            "pipe %s: dispatch of finding %s over channel %s "
                            "failed, will retry: %s",
                            pipe.name,
                            finding_id,
                            channel.name,
                            exc,
                        )
                else:
                    delivered = True
                    # mark_queue=True: the finding's pipe_queues row is marked
                    # 'dispatched' atomically, in the SAME transaction as this
                    # 'sent' dispatch insert, before the remaining fanned channels
                    # are attempted. That atomicity is load-bearing against
                    # duplicate redelivery: _send_channel for a later channel
                    # (e.g. SMTP) can take multiple seconds, and a SIGKILL in that
                    # window must not leave a committed 'sent' row beside a still-
                    # 'pending' pipe_queues row -- on restart the finding would
                    # re-drain and RE-DELIVER to this channel (duplicate page).
                    # Deferring the mark to the post-loop reconciliation widened
                    # exactly that crash window.
                    #
                    # This does NOT starve a co-fanned channel's escalation: that
                    # now runs off the per-(pipe, channel) immediate_channel_attempts
                    # counter (record_immediate_send_failure), called on every
                    # channel failure regardless of the pipe_queues row's status.
                    # The counter split alone fixes defect (b); the old shared-
                    # counter coupling that made mark_queue=True starve escalation
                    # no longer exists.
                    self.catalog.record_dispatch(
                        pipe.name,
                        channel.name,
                        [finding_id],
                        "sent",
                        source=row["source"],
                    )
                    # Per-channel recovery edge: a successful send resets this
                    # channel's escalation counter (only CONSECUTIVE failures
                    # ladder to unhealthy).
                    self.catalog.record_immediate_send_success(
                        pipe.name, channel.name
                    )
                    # Recovery edge for internal/dispatch channel_unhealthy: a
                    # successful send proves the channel is back. The gate
                    # drops this to a no-op unless an incident is open, and the
                    # first send after recovery closes it; later sends in the
                    # same drain find nothing open and no-op. Load-bearing
                    # after a daemon restart, when channel_health is cleared
                    # (so this path runs again) but the incident is still open.
                    self.catalog.write_internal_clearance(
                        "internal/dispatch",
                        channel.name,
                        f"{channel.name} delivery recovered",
                        known_pipes,
                    )
            # Per-finding redelivery reconciliation -- the step the digest path
            # never needed (it carries one channel per cycle). The channel loop
            # above owns per-CHANNEL health; this owns the orthogonal per-FINDING
            # question: did this finding reach a live transport this drain, and
            # if not, should it be retried later?
            if delivered:
                # >=1 live channel got the finding out. The pipe_queues row was
                # already marked 'dispatched' atomically by the first successful
                # record_dispatch (mark_queue=True) above; this is an idempotent
                # backstop that re-asserts the terminal state and keeps the
                # delivered/undelivered reconciliation symmetric and explicit.
                self.catalog.mark_pipe_items_dispatched(pipe.name, [finding_id])
            elif last_error is not None:
                # No channel delivered it AND at least one channel was actually
                # attempted (last_error set) -> the finding is undelivered;
                # advance its per-finding redelivery ladder exactly once. The
                # +N-per-drain inflation is gone: each failed channel advanced
                # its OWN per-channel counter above, while the finding's ladder
                # moves a single step here. If every channel was SKIPPED as
                # unhealthy (last_error is None) the row is left pending and
                # untouched, exactly as the pre-fan path did.
                self.catalog.record_pipe_finding_undelivered(
                    pipe.name, finding_id, last_error
                )

    def _dispatch_channels(
        self,
        pipe: Pipe,
        row,
        channels: dict[str, Channel],
    ) -> list[str]:
        """Channel names a finding dispatches over on the immediate path.

        A normal finding dispatches to exactly the pipe's own configured
        channels (unchanged behaviour). An *internal* finding -- angelus's
        OWN self-reported failure, identified domain-agnostically by the
        ``internal/`` source prefix -- fans instead to the UNION of every
        configured channel (B7).

        The system's distress signal must not ride a single shared-fate
        transport. internal/* findings route with ``target_pipes=["now"]``,
        and `now` carries one channel (push); so a dead push would silently
        swallow the alert that something is wrong -- the exact 2026-05-29
        failure class this project exists to prevent. Fanning to all channels
        and dispatching them independently (the caller attempts each in its
        own try/except, so a failure on one does not skip the rest) means a
        channel being down can't swallow the signal as long as one OTHER
        transport is live.

        Detection is the source prefix (via the shared _is_internal helper),
        never a channel name, so the rule stays domain-agnostic: a channel
        added under channels/ is fanned to for free, and no email/push
        special-case lives here. The same helper gates the immediate rate-
        limit bypass in _drain_immediate, so "what counts as internal" is
        defined in exactly one place and the two sites cannot drift. The union
        is ordered pipe-channels-first so the urgent transport (push, on `now`)
        is attempted before the long-form ones, and de-duplicated via
        dict.fromkeys so an overlap between the pipe's channels and the wider
        fan set sends once, not twice.

        Clearances never reach this method: write_internal_clearance routes
        them with ``target_pipes=[]`` (they page nothing), so they never
        enqueue on a pipe and never drain. The emission gate, dedup, and the
        mute check all run upstream in _drain_immediate before the channel
        loop; the rate-limit check there is bypassed for internal findings
        (the emission gate is their flood control), so fanning changes only
        which transports a finding that is *already cleared to dispatch*
        reaches.
        """
        if not _is_internal(row["source"]):
            return list(pipe.channels)
        return list(dict.fromkeys([*pipe.channels, *channels]))

    def _render(self, pipe: Pipe, row) -> str:
        body = self.catalog.read_body(row["body_ref"])
        target_pipes = json.loads(row["target_pipes"])
        if pipe.template is None:
            raise RuntimeError(f"pipe {pipe.name} has no dumb-alert template")
        return pipe.template.format(
            source=row["source"],
            type=row["type"],
            entity=row["entity"],
            severity=row["severity"] or "unknown",
            body=body.get("text") or "",
            finding_id=row["id"],
            target_pipes=",".join(target_pipes),
        )

    async def _drain_digest(
        self,
        pipe: Pipe,
        channels: dict[str, Channel],
        known_pipes: set[str],
    ) -> None:
        drained_at = self._clock.now_iso()
        last_drain_at = self.catalog.last_pipe_drain_at(pipe.name)
        pending_rows = self.catalog.pending_pipe_items(pipe.name, limit=None)
        finding_ids = [int(row["id"]) for row in pending_rows]
        structured = self._structured_inputs(pipe, last_drain_at)
        if not pending_rows and _is_same_utc_day(last_drain_at, drained_at):
            return
        preamble = self._render_preamble(pipe, structured)
        body, llm_error = await self._render_llm_body(pipe, structured)
        if llm_error is not None:
            body = LLM_FALLBACK_FOOTER
            # The digest still ships with the structured fallback body, but
            # the synthesis paragraph is gone -- a degraded delivery worth an
            # ERROR line alongside the internal/render finding (B22).
            LOGGER.error(
                "pipe %s: llm digest render failed, using fallback body: %s",
                pipe.name,
                llm_error,
            )
            self.catalog.write_internal_finding(
                "internal/render",
                "llm_render_failed",
                pipe.name,
                llm_error,
                known_pipes,
            )
        else:
            # Recovery edge for internal/render llm_render_failed: a clean
            # render clears any open render incident for this pipe so the gate
            # re-arms. Gate-dropped to a no-op when nothing is open.
            self.catalog.write_internal_clearance(
                "internal/render",
                pipe.name,
                f"{pipe.name} digest render recovered",
                known_pipes,
            )
        # Reversed from the original (preamble, body) order: Patrick wants
        # a synthesis paragraph FIRST, then the structured item list. The
        # llm body is the synthesis; the preamble is the items. The two
        # voices used to interleave (preamble said "Open: <list>", then
        # llm re-rendered the same list as a markdown table) -- now the
        # llm writes only a short summary paragraph and the preamble owns
        # the item rendering. See chronicler prompt below.
        message = "\n\n".join(part for part in (body, preamble) if part)
        # Local-time subject, screen-reader friendly, date-only (no time).
        # `astimezone()` with no arg uses the system local TZ. Patrick's
        # box is America/New_York; if the daemon ever runs in a container
        # without TZ data the subject silently drifts to a different
        # calendar day (fell-r1 CONSIDER #2). Multiple drains on the same
        # UTC day get the same subject; the natural flow is one per day
        # so that's fine. Day-of-month formatted via direct attribute
        # access -- strftime %-d is a GNU extension that breaks on
        # macOS/BSD/Windows (fell-r1 CONSIDER #3).
        local_now = self._clock.now_local()
        subject = (
            f"Angelus Observances for "
            f"{local_now.strftime('%A %B')} "
            f"{local_now.day}, {local_now.year}"
        )
        # The push (telegram) leg rides a separate compact render, not the full
        # long-form `message`: telegram caps at 4096 chars and the digest prose
        # would split into many messages. Built once; selected per channel kind
        # below. Email (and any non-push channel) keeps the full message.
        compact = self._render_compact(subject, structured)

        any_channel_succeeded = False
        for channel_name in pipe.channels:
            channel = channels[channel_name]
            payload = compact if channel.kind == "push" else message
            # Product decision: the digest path does NOT consult
            # is_channel_unhealthy before attempting send, even though the
            # immediate path does. The digest is the consolidation/audit
            # surface (slice-3 issue-20260514-wh1k); the dispatch row IS
            # the dispatch state, and the operator must keep seeing the
            # actual outcome each cycle until recovery. Adopting the
            # immediate path's skip would silently swallow a digest cycle
            # against a known-broken channel -- exactly the gap
            # (friction-20260514-c64x) the per-channel attempt counter
            # below was added to close. The attempt-anyway shape also gives
            # an intermittently-recovered channel a natural reset path via
            # record_digest_send_success below; a skip would freeze the
            # counter and the channel could never recover without a daemon
            # restart.
            try:
                await self._send_channel(channel, payload, subject)
            except Exception as exc:
                # The daily digest is the routine-delivery contract; a failed
                # send here is the exact 2026-05-29 silent-failure shape, so
                # log ERROR in addition to the failed dispatch row and the
                # internal/dispatch finding (B22).
                LOGGER.error(
                    "pipe %s: digest dispatch over channel %s failed: %s",
                    pipe.name,
                    channel.name,
                    exc,
                )
                self.catalog.record_dispatch(
                    pipe.name,
                    channel.name,
                    finding_ids,
                    "failed",
                    error=str(exc),
                    mark_queue=False,
                )
                self.catalog.write_internal_finding(
                    "internal/dispatch",
                    "channel_unhealthy",
                    channel.name,
                    str(exc),
                    known_pipes,
                )
                # Escalate to channel_health via the digest-specific
                # per-channel counter. The (pipe, finding_id) shape on
                # pipe_queues used by the immediate path would inflate the
                # threshold N-per-cycle on the digest path (one cycle
                # carries N finding_ids); this counter is keyed only by
                # (pipe, channel) so N CONSECUTIVE failure cycles cross
                # the same MAX_RETRY_ATTEMPTS threshold the immediate path
                # uses. Without this, a digest-only channel failure mode
                # (e.g. dead SMTP route while push works) would generate
                # one internal/dispatch finding per cycle forever and
                # never register in channel_health -- the gap
                # friction-20260514-c64x + friction-20260514-5ddc named.
                self.catalog.record_digest_send_failure(
                    pipe.name, channel.name, str(exc)
                )
            else:
                any_channel_succeeded = True
                self.catalog.record_dispatch(
                    pipe.name,
                    channel.name,
                    finding_ids,
                    "sent",
                    mark_queue=False,
                )
                # Reset the per-channel digest attempt counter so an
                # intermittent channel does not gradually accumulate to
                # threshold across many mostly-succeeding cycles.
                self.catalog.record_digest_send_success(pipe.name, channel.name)
                # Recovery edge for internal/dispatch channel_unhealthy. The
                # digest path attempts even a known-unhealthy channel (see the
                # note above), so this is the primary place a channel that
                # failed N cycles clears once it sends again. Gate-dropped to a
                # no-op when nothing is open.
                self.catalog.write_internal_clearance(
                    "internal/dispatch",
                    channel.name,
                    f"{channel.name} delivery recovered",
                    known_pipes,
                )
        if any_channel_succeeded:
            self.catalog.mark_pipe_items_dispatched(pipe.name, finding_ids)
            self.catalog.mark_pipe_drained(pipe.name, drained_at)
            # Dead-man heartbeat: the digest demonstrably went out on at least
            # one channel this cycle, so ping the off-box check. A missing ping
            # is the signal "the digest never fired" -- a gap belfry (on-box,
            # liveness-only) cannot see. Best-effort and last, after the drain
            # is fully recorded, so a slow endpoint cannot affect delivery.
            await self._ping_digest_heartbeat()

    async def _ping_digest_heartbeat(self) -> None:
        """Best-effort dead-man ping after a successful digest drain.

        Inert unless ``ANGELUS_DIGEST_HEARTBEAT_URL`` is set. Never raises:
        the digest has already been delivered and recorded by the time this
        runs, so a ping failure must not turn a delivered digest into an
        error. Run in a thread so the blocking urlopen cannot stall the event
        loop. The per-operation socket timeout (not a single total bound) plus
        the capped read in ``_get_url`` keep a misbehaving endpoint from
        meaningfully delaying the drain; a fast healthcheck endpoint returns
        well under it.
        """
        url = os.environ.get(DIGEST_HEARTBEAT_URL_ENV)
        if not url:
            return
        try:
            await asyncio.to_thread(_get_url, url, DIGEST_HEARTBEAT_TIMEOUT_SEC)
            LOGGER.info("digest heartbeat pinged %s", DIGEST_HEARTBEAT_URL_ENV)
        except asyncio.CancelledError:
            # Shutdown cancelled the drain task mid-ping. asyncio cannot cancel
            # a running thread, so this await defers the cancellation until the
            # urlopen thread returns (bounded per-operation by the socket
            # timeout) and then raises CancelledError; re-raise so teardown
            # sees it. Worst case it adds up to a few sequential socket-timeouts
            # (connect + header recv + the capped read) to the daemon's
            # already-bounded drain-shutdown wait.
            raise
        except Exception as exc:
            LOGGER.warning(
                "digest heartbeat ping to %s failed: %s",
                DIGEST_HEARTBEAT_URL_ENV,
                exc,
            )

    def _render_compact(self, subject: str, structured: dict[str, Any]) -> str:
        """Render the compact push (telegram) digest.

        A heartbeat header (the email subject line, so its presence alone
        confirms the digest fired) plus one-line counts and a capped headline
        list per non-empty section. Plain text, one item per line, screen-
        reader friendly; the full item list always rides the email leg.
        """
        open_incidents = structured.get("open_incidents") or []
        new_findings = structured.get("findings_since_last_drain") or []
        closures = structured.get("recent_closures") or []
        suppressed = structured.get("suppressed_findings") or []

        # Closures are counted in the summary line but deliberately not listed
        # as their own section: resolved items are good news, and the compact
        # leg reserves its limited headline space for what still needs eyes
        # (open incidents, new findings). The email leg lists closures in full.
        lines = [
            subject,
            (
                f"{len(new_findings)} new finding(s), "
                f"{len(open_incidents)} open incident(s), "
                f"{len(closures)} closed since last digest."
            ),
        ]
        cap = DEFAULT_COMPACT_MAX_ITEMS_PER_SECTION

        def _section(title: str, items: list[dict[str, Any]], fmt) -> None:
            if not items:
                return
            lines.append("")
            lines.append(f"{title} ({len(items)}):")
            for item in items[:cap]:
                lines.append(fmt(item))
            if len(items) > cap:
                lines.append(f"+{len(items) - cap} more")

        _section(
            "Open incidents",
            open_incidents,
            lambda i: (
                f"{i.get('severity') or 'unknown'} "
                f"{i.get('type') or ''}: {i.get('entity') or ''}".rstrip()
            ),
        )
        _section(
            "New findings",
            new_findings,
            lambda f: (
                f"{f.get('severity') or 'unknown'} "
                f"{f.get('type') or ''} on {f.get('entity') or ''}".rstrip()
            ),
        )
        if suppressed:
            lines.append("")
            lines.append(f"Rate-limit overflow ({len(suppressed)}).")
        return "\n".join(lines)

    async def _send_channel(self, channel: Channel, message: str, subject: str) -> None:
        if channel.kind == "push":
            await send_push(channel, message, self.workdir)
        elif channel.kind == "email":
            await send_email(channel, subject, message, self.workdir)
        else:
            raise RuntimeError(f"unsupported channel kind: {channel.kind}")

    def _structured_inputs(self, pipe: Pipe, last_drain_at: str | None) -> dict[str, Any]:
        open_incidents = self.catalog.open_incidents()
        raw = {
            "findings_since_last_drain": self.catalog.findings_for_pipe_since(
                pipe.name, last_drain_at, exclude_types=("clearance",)
            ),
            "suppressed_findings": self.catalog.suppressed_findings_since(last_drain_at),
            "open_incidents": open_incidents,
            "recent_closures": self.catalog.clearance_findings_since(last_drain_at),
            "fixer_actions": _gather_fixer_actions(
                _fixer_log_path(self.workdir), last_drain_at
            ),
        }
        # Backstop cap (B30): bound every input before it reaches the
        # preamble templates and the chronicler prompt, so no upstream flood
        # can blow the digest up regardless of the emission gate. Applied
        # before timestamp attachment so the marker row (which carries no
        # timestamps) is the only added item.
        for name, collection in raw.items():
            raw[name] = _cap_digest_input(collection, name)
        # Add local-time strings alongside the UTC originals so both the
        # jinja templates and the chronicler prompt can use human-readable
        # timestamps without re-implementing the conversion. The UTC
        # originals stay -- they're load-bearing for downstream catalog
        # operations and for deterministic test assertions. The local
        # `*_local` siblings are display-only.
        for collection in raw.values():
            for item in collection:
                _attach_local_timestamps(item)
        return raw

    def _render_preamble(self, pipe: Pipe, structured: dict[str, Any]) -> str:
        environment = Environment(
            loader=FileSystemLoader(self.workdir / "render-templates"),
            autoescape=select_autoescape(enabled_extensions=()),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        rendered: list[str] = []
        for block in pipe.render.get("preamble", []):
            if block.get("kind") != "structured":
                continue
            template = environment.get_template(f"{block['template']}.j2")
            rendered.append(template.render(**structured).strip())
        return "\n\n".join(part for part in rendered if part)

    async def _render_llm_body(
        self, pipe: Pipe, structured: dict[str, Any]
    ) -> tuple[str | None, str | None]:
        body_config = pipe.render.get("body") or {}
        mantle = body_config.get("mantle")
        if not mantle:
            return None, "daily digest body missing mantle"
        input_names = body_config.get("inputs") or []
        inputs = {name: structured[name] for name in input_names}
        # Tight constraints on the chronicler output: the preamble (jinja
        # templates) owns the structured item rendering; the LLM owns ONLY
        # a short synthesis paragraph at the top. Previous loose prompt
        # produced markdown tables, headers, and emoji in a plain-text
        # email -- mess Patrick called out. Asking for plain text in 2-4
        # sentences forces the model to do the synthesis work (which is
        # what an LLM is good for) rather than re-render the data (which
        # the preamble already does, deterministically).
        prompt = (
            "You are writing the opening synthesis paragraph of a plain-text "
            "ops digest email. Read the structured inputs and produce a single "
            "short paragraph (2 to 4 sentences) that summarizes what changed "
            "since the last digest.\n"
            "\n"
            "Strict rules:\n"
            "- Plain text only. No markdown headers, no asterisks for bold, "
            "no bulleted lists, no tables, no emoji, no horizontal rules.\n"
            "- Do not enumerate every item. The reader sees a structured "
            "list directly below your paragraph; do not duplicate it.\n"
            "- Lead with the most severe or unusual item.\n"
            "- Use entity names (e.g. 'speakbot', 'example.com') when "
            "there are fewer than five things to mention; otherwise count "
            "and summarize by category.\n"
            "- Times in your prose should use local clock times when "
            "*_local fields are present (e.g. 'since Tue 21:07 EDT'), not "
            "the UTC originals.\n"
            "- If nothing notable happened, say so in a single sentence.\n"
            "\n"
            "Structured inputs (JSON):\n"
            + json.dumps(inputs, sort_keys=True, default=str)
        )
        # The prompt embeds json.dumps(inputs), which grows with the backlog.
        # Passed via --message (argv) it blew past the OS single-argument
        # limit (MAX_ARG_STRLEN, ~128KB) on a busy day -- exec failed with
        # E2BIG ("Argument list too long") and the digest silently degraded
        # to LLM_FALLBACK_FOOTER (2026-06-01). Stage the prompt in a file and
        # hand it to horizon via --message-file: no argv limit, and -- unlike
        # stdin -- it leaves the subprocess I/O (a plain communicate() with no
        # stdin pipe) byte-for-byte identical to the proven shutdown-reap
        # path, so it introduces no new cancel/reap race.
        #
        # The staging area is retained, not a throwaway tmp file: the prompt
        # is the load-bearing input to the morning email, so keeping the last
        # N (parallel to state/sre-reports/) makes "what did the digest
        # actually ask the chronicler for" auditable after the fact -- which
        # is exactly what was missing when the 2026-06-01 digest degraded.
        staging_dir = _digest_staging_dir(self.workdir)
        prompt_path = staging_dir / (
            f"{self._clock.now().strftime('%Y%m%dT%H%M%SZ')}-{pipe.name}.txt"
        )
        try:
            staging_dir.mkdir(parents=True, exist_ok=True)
            prompt_path.write_text(prompt, encoding="utf-8")
        except OSError as exc:
            return None, f"chronicler prompt staging failed: {exc}"
        _prune_digest_staging(staging_dir)
        # WINDOW A vs WINDOW B -- two distinct daemon-shutdown cancellation
        # races, both of which orphan the forking `horizon cast` subtree if
        # unhandled.
        #
        # A digest pipe drains from an APScheduler interval/cron job.
        # AsyncIOExecutor.shutdown() cancels that job's asyncio task on
        # daemon shutdown -- it .cancel()s the future, it does NOT await it.
        # The CancelledError lands at whichever `await` the drain is parked
        # on, and there are two of them in this render:
        #
        #   WINDOW B (rarer) -- parked at `await process.communicate()`
        #   below. `process` is already bound there, so the
        #   `except CancelledError` arm can _kill_and_reap(process). That
        #   arm, plus the daemon tracking in-flight drain tasks in
        #   self._drain_tasks and awaiting them in run()'s shutdown-finally,
        #   is what makes B's reap complete before the event loop closes.
        #
        #   WINDOW A (dominant under load) -- parked INSIDE
        #   create_subprocess_exec, before its result is assigned to
        #   `process`. The horizon leader has already forked its grandchild,
        #   but our coroutine never received the Process handle, so a naive
        #   `except CancelledError: reap(process)` would have no `process` to
        #   reap -- the handle is lost and the subtree orphans.
        #
        # Fix for A: run the spawn as its own task (`launch`) and await it
        # through asyncio.shield. shield detaches the *waiter* from
        # cancellation: when our drain task is cancelled mid-spawn, our await
        # raises CancelledError but `launch` keeps running and still produces
        # the Process. In the cancel arm we recover that handle from `launch`
        # (see _recover_cancelled_spawn) and reap the whole group before
        # re-raising, so the subtree cannot outlive the daemon. The
        # uncancelled path is behaviourally unchanged: shield(launch) awaits
        # the spawn and returns the same Process a bare
        # `await create_subprocess_exec(...)` would have, and an OSError from
        # the spawn still surfaces here.
        launch: asyncio.Future[asyncio.subprocess.Process] = asyncio.ensure_future(
            asyncio.create_subprocess_exec(
                "horizon",
                "cast",
                "--mantle",
                str(mantle),
                "--message-file",
                str(prompt_path),
                # Without --json the cast stdout has a preamble ("New strand
                # created: ...", three help lines about cast --omlet) and a
                # footer block (Omlet: / Strand: / Status: / Bearing: /
                # Duration: lines) wrapped around a "Result: ..." line. The
                # whole envelope leaks into the digest body. --json returns
                # a structured object whose `result` field is the unwrapped
                # model output -- no preamble, no footer, no Result: prefix.
                "--json",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                # Own process group so a timeout/cancel SIGKILLs the whole
                # `horizon cast` tree (it shells out and forks), not just
                # the leader -- the same hardening run_shell_source uses.
                start_new_session=True,
            )
        )
        try:
            process = await asyncio.shield(launch)
        except asyncio.CancelledError:
            # WINDOW A reap: the spawn was in flight when we were cancelled,
            # so `process` above is unbound. Recover the handle the shielded
            # spawn still produced and SIGKILL+reap the whole horizon process
            # group before re-raising, so the subtree cannot be orphaned.
            orphan = await _recover_cancelled_spawn(launch)
            if orphan is not None:
                await _kill_and_reap(orphan)
            raise
        except OSError as exc:
            return None, f"chronicler launch failed: {exc}"
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=120
            )
        except asyncio.TimeoutError:
            await _kill_and_reap(process)
            return None, "chronicler timed out after 120s"
        except asyncio.CancelledError:
            # A digest pipe drains from an APScheduler interval/cron job;
            # AsyncIOExecutor.shutdown() cancels that job task on daemon
            # shutdown. Without reaping, the `horizon` subtree (a forking
            # command) outlives the daemon -- the same orphan class as a
            # non-reaped scheduled-source timeout. Reap the group, re-raise.
            await _kill_and_reap(process)
            raise
        raw = stdout.decode("utf-8", errors="replace").strip()
        if process.returncode != 0:
            error = stderr.decode("utf-8", errors="replace").strip()
            return None, f"chronicler exited {process.returncode}: {error}"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            return None, f"chronicler --json output unparseable: {exc}"
        result = payload.get("result")
        if not isinstance(result, str):
            return None, "chronicler --json missing string `result`"
        output = result.strip()
        # The chronicler prompt explicitly says "if nothing notable
        # happened, say so in a single sentence." That can be a
        # legitimately short reply like "All quiet since last digest."
        # (29 chars) or, worse, "All quiet." (10 chars). The prior
        # threshold of 20 rejected the latter as if it were a failure
        # and showed the operator the LLM_FALLBACK_FOOTER instead of
        # the actual compliant summary -- exactly the false-failure
        # shape fell-r1 CONSIDER #7 flagged. The new floor of 5 catches
        # empty / whitespace-only / <=4-char output (which means the
        # model genuinely produced nothing useful -- "ok", "yes.", etc.)
        # but accepts the quiet-day case. A 5-char "quiet" passes; that
        # is the intentionally degenerate compliant form.
        if len(output) < 5:
            return None, "chronicler output was empty or too short"
        return output, None

    def _over_rate_limit(self, pipe: Pipe, row) -> bool:
        if not pipe.rate_limit:
            return False
        since = (self._clock.now() - timedelta(hours=1)).isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")
        per_source = _parse_hourly_limit(pipe.rate_limit.get("per_source"))
        if per_source is not None:
            source_count = self.catalog.sent_dispatch_count_for_source(row["source"], since)
            if source_count >= per_source:
                return True
        per_channel = _parse_hourly_limit(pipe.rate_limit.get("per_channel"))
        if per_channel is None:
            return False
        for channel_name in pipe.channels:
            channel_count = self.catalog.sent_dispatch_count_for_channel(
                channel_name, since
            )
            if channel_count >= per_channel:
                return True
        return False


async def _recover_cancelled_spawn(
    launch: asyncio.Future[asyncio.subprocess.Process],
) -> asyncio.subprocess.Process | None:
    """Recover the Process handle from a shielded `horizon` spawn whose
    awaiting task was cancelled mid-flight, so the caller can reap it.

    See _render_llm_body's WINDOW A note for why the spawn is shielded. By
    the time we get here the calling drain task has already taken a
    CancelledError at `await asyncio.shield(launch)`, but `launch` itself was
    never cancelled (shield detaches only the waiter), so it still runs to
    completion and yields the real Process -- we just have to wait for it.

    We can be cancelled *again* while waiting: the daemon's shutdown-finally
    cancels each in-flight drain task on top of the AsyncIOExecutor cancel
    that started this. So a re-cancel simply loops. We wait via asyncio.wait
    rather than `await launch` / `await shield(launch)` deliberately:
    asyncio.wait resolves when `launch` is done WITHOUT propagating its
    result or exception, so a spawn that failed with OSError does not surface
    here and mask the CancelledError the caller is mid-handling -- we inspect
    `launch`'s state below instead. asyncio.wait also does not cancel the
    futures it waits on when it is itself cancelled, so `launch` keeps
    running and always finishes; the loop terminates.

    create_subprocess_exec returns in milliseconds in practice, so this does
    not meaningfully delay shutdown; the daemon's bounded shutdown gather
    (_DRAIN_SHUTDOWN_TIMEOUT) is the outer ceiling if a spawn ever genuinely
    wedges.

    Returns None if the spawn itself failed (e.g. OSError) -- nothing was
    started, so there is nothing to reap.
    """
    while not launch.done():
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait({launch})
    if launch.cancelled() or launch.exception() is not None:
        return None
    return launch.result()


def _is_internal(source: str | None) -> bool:
    """True for angelus's OWN self-reported failure findings.

    internal/* findings are the system's distress signal -- the self-reports
    written by write_internal_finding (internal/dispatch, internal/render,
    internal/config, ...). Detection is the ``internal/`` source prefix and
    nothing else, so the rule stays domain-agnostic: it never names a channel
    or a finding type, and a new internal source under that prefix is covered
    for free. Two immediate-path sites must agree on this definition -- the
    rate-limit bypass in _drain_immediate (internal findings are never
    suppressed off `now`) and the channel fan in _dispatch_channels (internal
    findings dispatch to every channel) -- so it lives here once rather than
    being spelled out at each site, where the two could drift apart.
    """
    return str(source).startswith("internal/")


def _parse_hourly_limit(value: str | None) -> int | None:
    if not value:
        return None
    text = value.strip().lower()
    if not text.endswith("/hr"):
        raise ValueError(f"unsupported rate limit {value!r}")
    amount = int(text[:-3])
    if amount <= 0:
        raise ValueError(f"unsupported rate limit {value!r}")
    return amount


def _is_same_utc_day(left: str | None, right: str | None) -> bool:
    if not left or not right:
        return False
    return _parse_utc(left).date() == _parse_utc(right).date()


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _get_url(url: str, timeout: float) -> None:
    """Blocking best-effort GET for the digest dead-man ping.

    Runs in a worker thread (see PipeDrain._ping_digest_heartbeat). Restricts
    the scheme to http/https (the URL is env-sourced; reject file:// and other
    schemes outright). Reads only a small bounded prefix of the response to
    bound the read and let the connection close; a non-2xx status raises so the
    caller logs it. Network/HTTP errors propagate to the caller's handler.
    """
    if not url.startswith(("http://", "https://")):
        raise RuntimeError(f"healthcheck URL must be http(s): {url!r}")
    with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310
        response.read(64)  # capped: a healthcheck ack is tiny; don't drain a flood
        status = getattr(response, "status", 200)
    if not 200 <= int(status) < 300:
        raise RuntimeError(f"healthcheck ping returned HTTP {status}")


# Timestamp fields the catalog returns on findings + incidents. Listed
# explicitly rather than "any field ending in _at" to avoid converting
# something that isn't a UTC ISO string (e.g. a future field a future
# operator names `created_label_at` for unrelated reasons).
_LOCAL_TS_FIELDS = (
    "occurred_at",
    "created_at",
    "updated_at",
    "queued_at",
    "opened_at",
    "closed_at",
)


def _cap_digest_input(
    items: list[dict[str, Any]],
    name: str,
    max_items: int = DEFAULT_DIGEST_MAX_ITEMS_PER_INPUT,
) -> list[dict[str, Any]]:
    """Truncate a digest input list to ``max_items`` with an omission marker.

    Returns ``items`` unchanged when it is already within budget. When it is
    over, returns the first ``max_items`` followed by a single marker dict
    whose fields render cleanly in every preamble template (severity / type /
    entity / body_text) and read clearly in the chronicler JSON, so the
    operator always sees that N items were dropped rather than a silently
    short list. The marker is a plain dict, not a finding row -- nothing is
    written to the catalog.
    """
    if len(items) <= max_items:
        return items
    omitted = len(items) - max_items
    capped = items[:max_items]
    capped.append(
        {
            "severity": "info",
            "type": "omitted",
            "entity": f"[{omitted} more {name} omitted]",
            "body_text": (
                f"{omitted} additional {name} omitted to bound the digest"
            ),
        }
    )
    return capped


def _attach_local_timestamps(item: dict[str, Any]) -> None:
    """For each known UTC-ISO timestamp field, add a sibling
    `<field>_local` rendered in the system local timezone.

    Mutates `item` in place. The UTC original is kept (the templates may
    still want raw ISO for sorting/dedup, and downstream catalog code
    reads UTC). Failures parse silently to None -- a malformed timestamp
    shouldn't crash the whole digest render.
    """
    for field in _LOCAL_TS_FIELDS:
        value = item.get(field)
        if not isinstance(value, str) or not value:
            continue
        try:
            dt_utc = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            continue
        local = dt_utc.astimezone()
        # rstrip handles the rare-but-real case where the resolved
        # local TZ has no abbreviation -- %Z renders to "", leaving a
        # trailing space (fell-r2 CONSIDER on the %Z edge). Linux boxes
        # with proper tzdata always emit an abbreviation; the rstrip
        # is cheap insurance for containerized deploys.
        item[f"{field}_local"] = local.strftime("%a %Y-%m-%d %H:%M %Z").rstrip()


# ---------------------------------------------------------------------------
# Fixer-actions log gatherer
# ---------------------------------------------------------------------------

# Matches key=value pairs in fixers.log lines. Values are either Python-repr
# single-quoted ('...'), double-quoted ("..."), or bare non-whitespace tokens.
_FIXER_KV_RE = re.compile(
    r"(\w+)="
    r"(?:'((?:[^'\\]|\\.)*)'|\"((?:[^\"\\]|\\.)*)\"|(\S+))"
)


def _fixer_log_path(workdir: Path) -> Path:
    """Path to fixers.log. Respects ANGELUS_BELFRY_FIXERS_LOG_PATH override
    so tests and alternate deployments can redirect without touching state/."""
    override = os.environ.get("ANGELUS_BELFRY_FIXERS_LOG_PATH")
    if override:
        return Path(override)
    return workdir / "state" / "fixers.log"


def _digest_staging_dir(workdir: Path) -> Path:
    """Directory where each digest drain stages the chronicler prompt.

    A retained, inspectable area (parallel to state/sre-reports/) rather than
    a throwaway tmp file: the prompt is the load-bearing input to the morning
    email, so keeping the recent ones makes "what did the digest actually ask
    for" auditable after the fact. Default state/digest-staging;
    ANGELUS_DIGEST_STAGING_DIR overrides for tests and alternate deployments.
    """
    override = os.environ.get("ANGELUS_DIGEST_STAGING_DIR")
    if override:
        return Path(override)
    return workdir / "state" / DEFAULT_DIGEST_STAGING_DIRNAME


def _prune_digest_staging(
    staging_dir: Path, keep: int = DEFAULT_DIGEST_STAGING_KEEP
) -> None:
    """Keep only the most recent ``keep`` staged prompts; best-effort.

    Bounds the folder so a daily (or replayed) digest cannot grow it without
    limit. The timestamp prefix is fixed-width, so lexical name order is
    chronological. Pruning is housekeeping, not correctness -- any IO error
    is swallowed so it can never fail a render.

    ``keep`` is a budget across ALL digest pipes that share this directory,
    not per-pipe: the glob is dir-wide, so with multiple digest pipes their
    prompts prune each other and per-pipe history is shorter than ``keep``.
    Fine for the single daily digest; revisit if more digest pipes land.
    """
    if keep <= 0:
        return
    try:
        staged = sorted(
            (p for p in staging_dir.glob("*.txt") if p.is_file()),
            key=lambda p: p.name,
        )
    except OSError:
        return
    for stale in staged[:-keep]:
        with contextlib.suppress(OSError):
            stale.unlink()


def _gather_fixer_actions(
    log_path: Path, since: str | None
) -> list[dict[str, Any]]:
    """Read fixers.log and return autoremediation actions since `since`.

    Lines with a timestamp <= since are excluded. Missing or empty log
    yields an empty list without error. For spawn lines with a report_path,
    an optional report_excerpt dict (outcome/root-cause fields) is attached
    when the report file exists.
    """
    if not log_path.exists():
        return []
    try:
        text = log_path.read_text(encoding="utf-8")
    except OSError:
        return []
    actions: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        first_space = line.find(" ")
        if first_space < 0:
            continue
        ts_str = line[:first_space]
        rest = line[first_space + 1:]
        if since is not None:
            try:
                if _parse_utc(ts_str) <= _parse_utc(since):
                    continue
            except ValueError:
                continue
        entry: dict[str, Any] = {"occurred_at": ts_str}
        for m in _FIXER_KV_RE.finditer(rest):
            key = m.group(1)
            value = next(
                (v for v in (m.group(2), m.group(3), m.group(4)) if v is not None), ""
            )
            entry[key] = value
        if entry.get("action") == "spawn":
            report_path_str = entry.get("report_path")
            if report_path_str:
                report_path = Path(report_path_str)
                if report_path.exists():
                    entry["report_excerpt"] = _excerpt_sre_report(report_path)
        actions.append(entry)
    return actions


def _excerpt_sre_report(report_path: Path) -> dict[str, str]:
    """Extract outcome and root-cause values from an SRE report file.

    Returns a dict with whichever of 'outcome' and 'root-cause' are present.
    OSError or parse failures return an empty dict.
    """
    try:
        text = report_path.read_text(encoding="utf-8")
    except OSError:
        return {}
    excerpt: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        for field in ("outcome:", "root-cause:"):
            if stripped.lower().startswith(field):
                excerpt[field.rstrip(":")] = stripped[len(field):].strip()
                break
    return excerpt
