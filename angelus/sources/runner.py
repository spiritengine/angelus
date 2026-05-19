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
# guards the pathological case (an unkillable/zombie tree) so a timed-out
# command can never hang the caller forever.
_REAP_TIMEOUT = 5.0


async def run_shell_source(source: ScheduledSource) -> tuple[bool, dict[str, object]]:
    # start_new_session + the process-group kill on timeout: identical
    # hardening to run_dep_check, and for the same reason (see
    # _kill_and_reap). This is the live daemon scheduled-fire path, so a
    # forking check command that times out would otherwise leak an
    # orphaned grandchild + its inherited fds on every fire.
    process = await asyncio.create_subprocess_shell(
        source.command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=source.timeout_seconds
        )
    except TimeoutError:
        await _kill_and_reap(process)
        return False, {
            "error": f"shell check timed out after {source.timeout_seconds:g}s",
            "timeout_seconds": source.timeout_seconds,
        }
    except asyncio.CancelledError:
        # The live scheduled-fire path: APScheduler submits _fire_source as
        # an event-loop task and AsyncIOExecutor.shutdown() cancels it on
        # daemon shutdown (it never honours wait=True -- it just .cancel()s
        # pending futures). A bare cancel here would unwind without touching
        # the child, orphaning the check subprocess AND its process group
        # (start_new_session) exactly like an un-reaped timeout would. Reap
        # the whole group, then re-raise so cancellation is never swallowed.
        await _kill_and_reap(process)
        raise
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
    # timeout kills the WHOLE tree, not just the shell (see _kill_and_reap
    # for why the shell alone is not enough).
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
        await _kill_and_reap(process)
        return "unhealthy", (
            f"check timed out after {dependency.timeout_seconds:g}s"
        )
    except asyncio.CancelledError:
        # Symmetric with run_shell_source: cancellation must reap the whole
        # process group before unwinding so a cancelled dep-check leaves no
        # orphaned child. Re-raise -- cancellation is never swallowed.
        await _kill_and_reap(process)
        raise
    out = stdout.decode("utf-8", errors="replace").strip()
    err = stderr.decode("utf-8", errors="replace").strip()
    if process.returncode == 0:
        return "healthy", _trim_detail(out or "ok")
    parts = [p for p in (f"exit {process.returncode}", err or out) if p]
    return "unhealthy", _trim_detail(": ".join(parts))


async def _kill_and_reap(process: asyncio.subprocess.Process) -> None:
    """SIGKILL a timed-out OR cancelled command's whole process group, then
    reap it within a hard ceiling.

    Invoked on the timeout AND CancelledError paths of every subprocess
    site angelus runs -- source-fire and dep-check probes here in sources/,
    push and email channel sends in channels/, and the digest LLM body
    render in pipes/. Cancellation sources differ by site: daemon shutdown
    cancels APScheduler-submitted source-fire and pipe-digest tasks
    (AsyncIOExecutor.shutdown just .cancel()s pending futures), the
    channel-send sites are cancelled when their drain task is cancelled,
    and the cron-fired dep-check probe (a CLI process invoked from
    angelus/cli.py, not the daemon) takes its cancel from operator
    interrupt -- in every case, without reaping on cancel the child and
    its process group would survive their parent, the same orphan a
    non-reaped timeout produces. Caller names are deliberately listed by
    area (sources / channels / pipes), not enumerated -- new subprocess
    sites should adopt this helper rather than grow yet another shape,
    and a per-name list rots the moment they do (the prior
    "run_shell_source / run_dep_check" enumeration was already wrong by
    the time the integration fell ran).

    Why the group and not just the shell: asyncio resolves
    `process.wait()` only when the subprocess transport is fully done --
    the OS process has exited AND the stdout/stderr pipe transports have
    seen EOF (connection_lost). For a FORKING check command `sh -c`
    forks the real work as a grandchild that inherits copies of the
    stdout/stderr pipe write-ends. SIGKILL to the shell alone exits the
    shell, but the orphaned grandchild (reparented to init) keeps those
    write-ends open, so the read-ends never see EOF, so the transport
    never completes, so `await process.wait()` blocks until the
    grandchild itself exits -- the full command runtime (~30s for
    `sleep 30`). Empirically confirmed: a plain process.kill() on a
    forking command hangs ~30s. Killing the whole process group also
    kills the grandchild, closing the write-ends; EOF arrives and wait()
    returns at the timeout. (The original friction-20260519-pcml
    diagnosis -- "process.wait() blocks until the pipes drain" -- was
    correct; this is that mechanism stated precisely.)

    A non-forking simple command like `sleep 30` is exec'd by dash
    directly: it IS the direct child, SIGKILL reaches it, EOF follows,
    no hang -- which is exactly why such a command cannot exercise or
    test this path. A forking command is required to reproduce it.

    The bounded reap only guards a pathological unkillable/zombie tree so
    the caller can never hang forever.
    """
    _kill_process_group(process)
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(process.wait(), _REAP_TIMEOUT)


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
