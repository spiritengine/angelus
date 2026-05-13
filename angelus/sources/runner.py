"""Scheduled source execution."""

from __future__ import annotations

import asyncio
import shlex

from angelus.lodging import ScheduledSource


async def run_shell_source(source: ScheduledSource) -> tuple[bool, dict[str, object]]:
    argv = shlex.split(source.command)
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        error = stderr.decode("utf-8", errors="replace").strip()
        return False, {"url": source.url, "error": error, "returncode": process.returncode}

    text = stdout.decode("utf-8", errors="replace").strip()
    try:
        status_code: int | str = int(text)
    except ValueError:
        status_code = text
    return True, {"url": source.url, "status_code": status_code}
