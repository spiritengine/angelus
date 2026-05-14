"""Scheduled source execution."""

from __future__ import annotations

import asyncio
import json

from angelus.lodging import ScheduledSource


async def run_shell_source(source: ScheduledSource) -> tuple[bool, dict[str, object]]:
    process = await asyncio.create_subprocess_shell(
        source.command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        error = stderr.decode("utf-8", errors="replace").strip()
        return False, {"error": error, "returncode": process.returncode}

    text = stdout.decode("utf-8", errors="replace").strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return False, {
            "error": f"shell check stdout is not valid JSON: {exc}",
            "stdout": text,
        }
    if not isinstance(payload, dict):
        return False, {
            "error": "shell check stdout is not a JSON object",
            "stdout": text,
        }
    return True, payload
