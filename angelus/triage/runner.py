"""Triager subprocess runner."""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

from angelus.lodging import Triager
from angelus.sources.runner import _kill_and_reap


async def run_python_triager(
    triager: Triager,
    observation: dict[str, Any],
    prior_state: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # start_new_session puts the child in its own process group so a
    # forking triager (one that exec's another tool or shells out) can
    # be reaped completely on timeout or cancellation: SIGKILL to the
    # shell alone leaves an orphaned grandchild holding the pipe
    # write-ends, blocking wait() until the grandchild itself exits.
    # See angelus/sources/runner.py:_kill_and_reap for the full mechanism.
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(triager.handler_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    payload = json.dumps(
        {
            "observation": observation,
            "prior_state": prior_state,
            "triager": {
                "name": triager.name,
                "source_ref": triager.source_ref,
                "metadata": triager.metadata,
            },
        },
        sort_keys=True,
    ).encode("utf-8")
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(payload), timeout=triager.timeout_seconds
        )
    except TimeoutError as exc:
        await _kill_and_reap(process)
        raise RuntimeError(
            f"triager {triager.name} timed out after {triager.timeout_seconds:g}s"
        ) from exc
    except asyncio.CancelledError:
        # Daemon shutdown cancels the _triage_loop task; without
        # reaping here the triager child and its process group survive
        # the daemon (same orphan a non-reaped timeout produces).
        await _kill_and_reap(process)
        raise
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
