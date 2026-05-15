"""Push channel wrapper."""

from __future__ import annotations

import asyncio
import os
import shlex
from pathlib import Path

from angelus.lodging import Channel

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
    )
    try:
        _, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        process.kill()
        await process.wait()
        raise RuntimeError(
            f"{channel.name} timed out after {timeout_seconds:g}s"
        ) from exc
    if process.returncode != 0:
        error = stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"{channel.name} failed: {error}")
