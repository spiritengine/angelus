"""Scheduled source execution."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal

from angelus.lodging import Dependency, ScheduledSource

_MAX_DEP_DETAIL = 4000

# Hard ceiling on the post-timeout reap. With the whole process group
# killed the child tree is gone and wait() returns at once; this only
# guards the pathological case (an unkillable/zombie tree) so a cron
# probe can never hang forever -- a hung probe is the worst outcome.
_REAP_TIMEOUT = 5.0


async def run_shell_source(source: ScheduledSource) -> tuple[bool, dict[str, object]]:
    process = await asyncio.create_subprocess_shell(
        source.command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=source.timeout_seconds
        )
    except TimeoutError:
        process.kill()
        await process.wait()
        return False, {
            "error": f"shell check timed out after {source.timeout_seconds:g}s",
            "timeout_seconds": source.timeout_seconds,
        }
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


async def run_dep_check(dependency: Dependency) -> tuple[str, str]:
    """Run a dependency's check command and classify health.

    Returns (status, detail) where status is 'healthy' (exit 0) or
    'unhealthy' (non-zero exit or timeout) and detail is the trimmed
    process output. The check is a single shell command -- one mechanism
    for both URL tripwires (`curl -fsS https://...`) and local CLIs
    (`notify-pat --help`) -- run via the same create_subprocess_shell +
    wait_for + kill-on-timeout pattern run_shell_source uses, not a
    reinvented one. The probe never touches sqlite; its caller sends the
    result to the daemon over the control socket.
    """
    # start_new_session puts the check in its own process group so a
    # timeout kill takes the WHOLE tree, not just the shell. dash/sh -c
    # forks the real command as a child; SIGKILL to the shell alone
    # orphans that child, which keeps the stdout/stderr pipes open and
    # makes asyncio's process.wait() block until those pipes drain (i.e.
    # until the orphan exits on its own -- a full hang for `sleep 30`).
    # Killing the group avoids that. (run_shell_source still has this
    # latent hang on its own shell-timeout path; out of scope here,
    # flagged in the slice-5c tender.)
    process = await asyncio.create_subprocess_shell(
        dependency.check,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=dependency.timeout_seconds
        )
    except TimeoutError:
        _kill_process_group(process)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(process.wait(), _REAP_TIMEOUT)
        return "unhealthy", (
            f"check timed out after {dependency.timeout_seconds:g}s"
        )
    out = stdout.decode("utf-8", errors="replace").strip()
    err = stderr.decode("utf-8", errors="replace").strip()
    if process.returncode == 0:
        return "healthy", _trim_detail(out or "ok")
    parts = [p for p in (f"exit {process.returncode}", err or out) if p]
    return "unhealthy", _trim_detail(": ".join(parts))


def _kill_process_group(process: asyncio.subprocess.Process) -> None:
    """SIGKILL the check's whole process group; fall back to the single
    process if the group is already gone (the child raced to exit)."""
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        with contextlib.suppress(ProcessLookupError):
            process.kill()


def _trim_detail(text: str) -> str:
    if len(text) <= _MAX_DEP_DETAIL:
        return text
    return text[:_MAX_DEP_DETAIL] + "...[truncated]"
