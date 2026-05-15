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
        self.pipe = pipe
        self.channels = channels
        self.workdir = workdir
        self.known_pipes = known_pipes
        self.lock = asyncio.Lock()

    async def drain_once(self) -> None:
        async with self.lock:
            if self.pipe.render_kind == "digest":
                await self._drain_digest()
                return

            rows = self.catalog.pending_pipe_items(self.pipe.name)
            for row in rows:
                finding_id = int(row["id"])
                message = self._render(row)
                if self._over_rate_limit(row):
                    self.catalog.suppress_pipe_item_to(
                        finding_id,
                        self.pipe.name,
                        self.pipe.rate_limit["overflow"],
                    )
                    continue
                for channel_name in self.pipe.channels:
                    if self.catalog.is_channel_unhealthy(channel_name):
                        continue
                    channel = self.channels[channel_name]
                    try:
                        await send_push(channel, message, self.workdir)
                    except Exception as exc:
                        exhausted = self.catalog.record_pipe_send_failure(
                            self.pipe.name,
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
                                self.known_pipes,
                            )
                    else:
                        self.catalog.record_dispatch(
                            self.pipe.name,
                            channel.name,
                            [finding_id],
                            "sent",
                            source=row["source"],
                        )

    def _render(self, row) -> str:
        body = self.catalog.read_body(row["body_ref"])
        target_pipes = json.loads(row["target_pipes"])
        if self.pipe.template is None:
            raise RuntimeError(f"pipe {self.pipe.name} has no dumb-alert template")
        return self.pipe.template.format(
            source=row["source"],
            type=row["type"],
            entity=row["entity"],
            severity=row["severity"] or "unknown",
            body=body.get("text") or "",
            finding_id=row["id"],
            target_pipes=",".join(target_pipes),
        )

    async def _drain_digest(self) -> None:
        drained_at = utcnow()
        last_drain_at = self.catalog.last_pipe_drain_at(self.pipe.name)
        pending_rows = self.catalog.pending_pipe_items(self.pipe.name, limit=None)
        finding_ids = [int(row["id"]) for row in pending_rows]
        structured = self._structured_inputs(last_drain_at)
        if not pending_rows and _is_same_utc_day(last_drain_at, drained_at):
            return
        preamble = self._render_preamble(structured)
        body, llm_error = await self._render_llm_body(structured)
        if llm_error is not None:
            body = LLM_FALLBACK_FOOTER
            self.catalog.write_internal_finding(
                "internal/render",
                "llm_render_failed",
                self.pipe.name,
                llm_error,
                self.known_pipes,
            )
        message = "\n\n".join(part for part in (preamble, body) if part)
        subject = (
            f"Angelus daily digest {datetime.now(UTC).date().isoformat()} UTC"
        )

        any_channel_succeeded = False
        for channel_name in self.pipe.channels:
            channel = self.channels[channel_name]
            try:
                await self._send_channel(channel, message, subject)
            except Exception as exc:
                self.catalog.record_dispatch(
                    self.pipe.name,
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
                    self.known_pipes,
                )
            else:
                any_channel_succeeded = True
                self.catalog.record_dispatch(
                    self.pipe.name,
                    channel.name,
                    finding_ids,
                    "sent",
                    mark_queue=False,
                )
        if any_channel_succeeded:
            self.catalog.mark_pipe_items_dispatched(self.pipe.name, finding_ids)
            self.catalog.mark_pipe_drained(self.pipe.name, drained_at)

    async def _send_channel(self, channel: Channel, message: str, subject: str) -> None:
        if channel.kind == "push":
            await send_push(channel, message, self.workdir)
        elif channel.kind == "email":
            await send_email(channel, subject, message, self.workdir)
        else:
            raise RuntimeError(f"unsupported channel kind: {channel.kind}")

    def _structured_inputs(self, last_drain_at: str | None) -> dict[str, Any]:
        open_incidents = self.catalog.open_incidents()
        return {
            "findings_since_last_drain": self.catalog.findings_for_pipe_since(
                self.pipe.name, last_drain_at, exclude_types=("clearance",)
            ),
            "suppressed_findings": self.catalog.suppressed_findings_since(last_drain_at),
            "open_incidents": open_incidents,
            "recent_closures": self.catalog.clearance_findings_since(last_drain_at),
        }

    def _render_preamble(self, structured: dict[str, Any]) -> str:
        environment = Environment(
            loader=FileSystemLoader(self.workdir / "render-templates"),
            autoescape=select_autoescape(enabled_extensions=()),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        rendered: list[str] = []
        for block in self.pipe.render.get("preamble", []):
            if block.get("kind") != "structured":
                continue
            template = environment.get_template(f"{block['template']}.j2")
            rendered.append(template.render(**structured).strip())
        return "\n\n".join(part for part in rendered if part)

    async def _render_llm_body(
        self, structured: dict[str, Any]
    ) -> tuple[str | None, str | None]:
        body_config = self.pipe.render.get("body") or {}
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
            )
        except OSError as exc:
            return None, f"chronicler launch failed: {exc}"
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            return None, "chronicler timed out after 120s"
        output = stdout.decode("utf-8", errors="replace").strip()
        if process.returncode != 0:
            error = stderr.decode("utf-8", errors="replace").strip()
            return None, f"chronicler exited {process.returncode}: {error}"
        if len(output) < 20:
            return None, "chronicler output was empty or too short"
        return output, None

    def _over_rate_limit(self, row) -> bool:
        if not self.pipe.rate_limit:
            return False
        since = (datetime.now(UTC) - timedelta(hours=1)).isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")
        per_source = _parse_hourly_limit(self.pipe.rate_limit.get("per_source"))
        if per_source is not None:
            source_count = self.catalog.sent_dispatch_count_for_source(row["source"], since)
            if source_count >= per_source:
                return True
        per_channel = _parse_hourly_limit(self.pipe.rate_limit.get("per_channel"))
        if per_channel is None:
            return False
        for channel_name in self.pipe.channels:
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
