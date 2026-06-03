"""Email channel wrapper for patbot-email."""

from __future__ import annotations

import asyncio
import os
import shlex
from pathlib import Path

from angelus.lodging import ENV_REF_PREFIX, Channel
from angelus.sources.runner import _kill_and_reap

DEFAULT_TIMEOUT_SECONDS = 30.0


async def send_email(
    channel: Channel,
    subject: str,
    body: str,
    workdir: Path,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> None:
    to_address = _resolve_to(channel.to)
    if os.environ.get("ANGELUS_DRY_RUN") == "1":
        with (workdir / "dispatches.log").open("a", encoding="utf-8") as handle:
            handle.write(f"email:{to_address}:{subject}:{body.replace(chr(10), ' ')}\n")
        return

    argv = shlex.split(channel.command) + ["send", to_address, subject]
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        # Own process group so timeout/cancel reaps the whole tree, not
        # just the leader (shared kill-on-timeout hardening).
        start_new_session=True,
    )
    try:
        _, stderr = await asyncio.wait_for(
            process.communicate(body.encode("utf-8")),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        await _kill_and_reap(process)
        raise RuntimeError(
            f"{channel.name} timed out after {timeout_seconds:g}s"
        ) from exc
    except asyncio.CancelledError:
        # Reached from a digest pipe-drain job; AsyncIOExecutor.shutdown()
        # cancels that job task on daemon shutdown. Reap the group so the
        # send subprocess never outlives the daemon; re-raise.
        await _kill_and_reap(process)
        raise
    if process.returncode != 0:
        error = stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"{channel.name} failed: {error}")


def _resolve_to(value: str | None) -> str:
    if not value:
        raise RuntimeError("email channel missing to address")
    if value.startswith(ENV_REF_PREFIX):
        env_name = value.removeprefix(ENV_REF_PREFIX)
        resolved = os.environ.get(env_name)
        if not resolved:
            raise RuntimeError(f"email channel env var is unset: {env_name}")
        return resolved
    return value
