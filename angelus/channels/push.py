"""Push channel wrapper."""

from __future__ import annotations

import asyncio
import os
import shlex
from pathlib import Path

from angelus.lodging import Channel
from angelus.sources.runner import _kill_and_reap

DEFAULT_TIMEOUT_SECONDS = 30.0


async def send_push(
    channel: Channel,
    message: str,
    workdir: Path,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> None:
    if os.environ.get("ANGELUS_DRY_RUN") == "1":
        with (workdir / "dispatches.log").open("a", encoding="utf-8") as handle:
            handle.write(message.replace("\n", " ") + "\n")
        return

    argv = shlex.split(channel.command) + [message]
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        # Own process group so timeout/cancel reaps the whole tree, not
        # just the leader (shared kill-on-timeout hardening).
        start_new_session=True,
    )
    try:
        _, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        await _kill_and_reap(process)
        raise RuntimeError(
            f"{channel.name} timed out after {timeout_seconds:g}s"
        ) from exc
    except asyncio.CancelledError:
        # A digest pipe-drain job sends through here and is cancelled by
        # AsyncIOExecutor.shutdown() on daemon shutdown. Reap the group so
        # the send subprocess never outlives the daemon; re-raise.
        await _kill_and_reap(process)
        raise
    if process.returncode != 0:
        error = stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"{channel.name} failed: {error}")
