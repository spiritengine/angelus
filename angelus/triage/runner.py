"""Triager subprocess runner."""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

from angelus.lodging import Triager


async def run_python_triager(
    triager: Triager,
    observation: dict[str, Any],
    prior_state: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(triager.handler_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    payload = json.dumps(
        {"observation": observation, "prior_state": prior_state},
        sort_keys=True,
    ).encode("utf-8")
    stdout, stderr = await process.communicate(payload)
    if stderr:
        sys.stderr.write(stderr.decode("utf-8", errors="replace"))
    if process.returncode != 0:
        raise RuntimeError(f"triager {triager.name} exited {process.returncode}")
    data = json.loads(stdout.decode("utf-8"))
    findings = data.get("findings") or []
    new_state = data.get("new_state") or {}
    if not isinstance(findings, list) or not isinstance(new_state, dict):
        raise ValueError(f"triager {triager.name} returned invalid JSON shape")
    return findings, new_state
