"""Pipe rendering and draining."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from angelus.channels import send_push
from angelus.lodging import Channel, Pipe
from angelus.storage import Catalog


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
            rows = self.catalog.pending_pipe_items(self.pipe.name)
            for row in rows:
                finding_id = int(row["id"])
                message = self._render(row)
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
                            self.pipe.name, channel.name, [finding_id], "sent"
                        )

    def _render(self, row) -> str:
        body = self.catalog.read_body(row["body_ref"])
        target_pipes = json.loads(row["target_pipes"])
        return self.pipe.template.format(
            source=row["source"],
            type=row["type"],
            entity=row["entity"],
            severity=row["severity"] or "unknown",
            body=body.get("text") or "",
            finding_id=row["id"],
            target_pipes=",".join(target_pipes),
        )
