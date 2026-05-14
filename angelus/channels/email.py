"""Email channel wrapper for patbot-email."""

from __future__ import annotations

import asyncio
import os
import shlex
import subprocess
from pathlib import Path

from angelus.lodging import Channel


async def send_email(channel: Channel, subject: str, body: str, workdir: Path) -> None:
    to_address = _resolve_to(channel.to)
    if os.environ.get("ANGELUS_DRY_RUN") == "1":
        with (workdir / "dispatches.log").open("a", encoding="utf-8") as handle:
            handle.write(f"email:{to_address}:{subject}:{body.replace(chr(10), ' ')}\n")
        return

    argv = shlex.split(channel.command) + ["send", to_address, subject]
    result = await asyncio.to_thread(
        subprocess.run,
        argv,
        input=body.encode("utf-8"),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        error = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"{channel.name} failed: {error}")


def _resolve_to(value: str | None) -> str:
    if not value:
        raise RuntimeError("email channel missing to address")
    if value.startswith("$env:"):
        env_name = value.removeprefix("$env:")
        resolved = os.environ.get(env_name)
        if not resolved:
            raise RuntimeError(f"email channel env var is unset: {env_name}")
        return resolved
    return value
