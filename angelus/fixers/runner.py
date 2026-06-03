"""Fixer handler subprocess runner (B11).

A fixer remediates by running its python handler as a subprocess -- the same
isolation model as triagers (see angelus/triage/runner.py): the handler gets
the matched condition as JSON on stdin and reports its outcome as JSON on
stdout. Running out-of-process means a fixer shells out to do real work
(systemctl, notify-pat, curl) and can never reach into live daemon state, so a
buggy fixer cannot corrupt the catalog or the event loop.

The guardrails (max_attempts / window / backoff) are enforced by the dispatcher
BEFORE this runs; this module is purely "invoke one handler, parse its result".
"""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

from angelus.lodging import Fixer
from angelus.sources.runner import _kill_and_reap


async def run_python_fixer(
    fixer: Fixer,
    context: dict[str, Any],
) -> dict[str, Any]:
    """Run a fixer's handler and return its parsed result dict.

    `context` is the matched condition (kind, condition_key, and the live
    incident/channel detail) passed to the handler as JSON on stdin. The
    handler must print a JSON object on stdout; the convention is::

        {"outcome": "recovered", "note": "re-ran dep check"}

    `outcome` (required, non-empty string) is recorded in the attempt ledger
    and the shared fixers.log; `note` (optional) is appended to the audit line.

    Raises RuntimeError on timeout or non-zero exit and ValueError on a missing
    or malformed result, exactly like the triager runner -- the dispatcher
    turns any of these into a recorded outcome="error" rather than letting it
    escape.
    """
    # start_new_session: own process group so a handler that forks (shells out
    # to systemctl, notify-pat) is reaped completely on timeout/cancellation.
    # See angelus/sources/runner.py:_kill_and_reap for the mechanism.
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(fixer.handler_path),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    payload = json.dumps(
        {
            "fixer": {
                "name": fixer.name,
                "condition": {
                    "kind": fixer.condition.kind,
                    "source": fixer.condition.source,
                    "incident_type": fixer.condition.incident_type,
                    "entity": fixer.condition.entity,
                    "channel": fixer.condition.channel,
                },
            },
            "condition": context,
        },
        sort_keys=True,
    ).encode("utf-8")
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(payload), timeout=fixer.handler_timeout
        )
    except TimeoutError as exc:
        await _kill_and_reap(process)
        raise RuntimeError(
            f"fixer {fixer.name} timed out after {fixer.handler_timeout:g}s"
        ) from exc
    except asyncio.CancelledError:
        # Daemon shutdown cancels the fixer loop task; reap the child so its
        # process group does not survive the daemon (same orphan a non-reaped
        # timeout would produce).
        await _kill_and_reap(process)
        raise
    if stderr:
        sys.stderr.write(stderr.decode("utf-8", errors="replace"))
    if process.returncode != 0:
        raise RuntimeError(f"fixer {fixer.name} exited {process.returncode}")
    try:
        data = json.loads(stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"fixer {fixer.name} returned non-JSON output") from exc
    if not isinstance(data, dict):
        raise ValueError(f"fixer {fixer.name} returned invalid JSON shape")
    outcome = data.get("outcome")
    if not isinstance(outcome, str) or not outcome:
        raise ValueError(
            f"fixer {fixer.name} result missing non-empty 'outcome' string"
        )
    return data


__all__ = ["run_python_fixer"]
