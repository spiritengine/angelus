"""Pipe rendering and draining."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from angelus.channels import send_email, send_push
from angelus.lodging import Channel, Pipe
from angelus.sources.runner import _kill_and_reap
from angelus.storage import Catalog, utcnow

LLM_FALLBACK_FOOTER = "LLM digest body unavailable — see structured data above."


class PipeDrain:
    def __init__(
        self,
        catalog: Catalog,
        pipe: Pipe,
        channels: dict[str, Channel],
        workdir: Path,
        known_pipes: set[str],
    ) -> None:
        self.catalog = catalog
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
                    await send_push(channel, message, self.workdir)
                except Exception as exc:
                    exhausted = self.catalog.record_pipe_send_failure(
                        pipe.name,
                        channel.name,
                        finding_id,
                        str(exc),
                    )
                    if exhausted:
                        self.catalog.write_internal_finding(
                            "internal/dispatch",
                            "channel_unhealthy",
                            channel.name,
                            str(exc),
                            known_pipes,
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
        drained_at = utcnow()
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
            self.catalog.write_internal_finding(
                "internal/render",
                "llm_render_failed",
                pipe.name,
                llm_error,
                known_pipes,
            )
        message = "\n\n".join(part for part in (preamble, body) if part)
        subject = (
            f"Angelus daily digest {datetime.now(UTC).date().isoformat()} UTC"
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
        return {
            "findings_since_last_drain": self.catalog.findings_for_pipe_since(
                pipe.name, last_drain_at, exclude_types=("clearance",)
            ),
            "suppressed_findings": self.catalog.suppressed_findings_since(last_drain_at),
            "open_incidents": open_incidents,
            "recent_closures": self.catalog.clearance_findings_since(last_drain_at),
        }

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
        prompt = (
            "Render a concise Angelus daily digest body from the structured inputs. "
            "Do not omit urgent operational facts.\n\n"
            + json.dumps(inputs, sort_keys=True, default=str)
        )
        try:
            process = await asyncio.create_subprocess_exec(
                "horizon",
                "cast",
                "--mantle",
                str(mantle),
                "--message",
                prompt,
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
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120)
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
        output = stdout.decode("utf-8", errors="replace").strip()
        if process.returncode != 0:
            error = stderr.decode("utf-8", errors="replace").strip()
            return None, f"chronicler exited {process.returncode}: {error}"
        if len(output) < 20:
            return None, "chronicler output was empty or too short"
        return output, None

    def _over_rate_limit(self, pipe: Pipe, row) -> bool:
        if not pipe.rate_limit:
            return False
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat(
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
