"""Pipe rendering and draining."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
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
            if self._over_rate_limit(pipe, row):
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
            for channel_name in pipe.channels:
                if self.catalog.is_channel_unhealthy(channel_name):
                    continue
                channel = channels[channel_name]
                try:
                    await self._send_channel(channel, message, subject)
                except Exception as exc:
                    exhausted = self.catalog.record_pipe_send_failure(
                        pipe.name,
                        channel.name,
                        finding_id,
                        str(exc),
                    )
                    if exhausted:
                        # Retries exhausted: the channel is now marked
                        # unhealthy and an internal/dispatch finding is
                        # written. Log at ERROR -- this is a delivery the
                        # system has given up on (B22).
                        LOGGER.error(
                            "pipe %s: dispatch of finding %s over channel %s "
                            "failed and exhausted retries; marking channel "
                            "unhealthy: %s",
                            pipe.name,
                            finding_id,
                            channel.name,
                            exc,
                        )
                        self.catalog.write_internal_finding(
                            "internal/dispatch",
                            "channel_unhealthy",
                            channel.name,
                            str(exc),
                            known_pipes,
                        )
                    else:
                        # Will retry on a later drain. WARNING, not ERROR --
                        # a single transient failure is expected to recover.
                        LOGGER.warning(
                            "pipe %s: dispatch of finding %s over channel %s "
                            "failed, will retry: %s",
                            pipe.name,
                            finding_id,
                            channel.name,
                            exc,
                        )
                else:
                    self.catalog.record_dispatch(
                        pipe.name,
                        channel.name,
                        [finding_id],
                        "sent",
                        source=row["source"],
                    )

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

        any_channel_succeeded = False
        for channel_name in pipe.channels:
            channel = channels[channel_name]
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
                await self._send_channel(channel, message, subject)
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
        if any_channel_succeeded:
            self.catalog.mark_pipe_items_dispatched(pipe.name, finding_ids)
            self.catalog.mark_pipe_drained(pipe.name, drained_at)

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
        try:
            process = await asyncio.create_subprocess_exec(
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
