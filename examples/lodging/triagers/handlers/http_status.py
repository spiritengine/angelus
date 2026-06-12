#!/usr/bin/env python3
"""Generic HTTP-status dead-link triager.

Reads entity name, severity, and routing pipes from the triager metadata
the runner passes in. Emits a `down` finding on a 200 -> non-200 transition
(or on a first observation that's already non-200) and a `clearance`
finding on a non-200 -> 200 recovery.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime


def main() -> None:
    request = json.load(sys.stdin)
    observation = request.get("observation") or {}
    prior_state = request.get("prior_state") or {}
    triager = request.get("triager") or {}
    metadata = triager.get("metadata") or {}

    source_ref = str(triager.get("source_ref") or "")
    entity = str(metadata.get("entity") or observation.get("entity") or "unknown")
    severity = str(metadata.get("severity") or "high")
    target_pipe = str(metadata.get("target_pipe") or "now")
    clearance_pipe = str(metadata.get("clearance_pipe") or target_pipe)

    status = _coerce_status(observation.get("status_code"))
    last_status = _coerce_status(prior_state.get("last_status"))
    url = str(observation.get("url") or "")
    failure_detail: str | None = None

    # If the source-runner short-circuited (JSON parse failure, timeout,
    # missing binary, OS-load SIGKILL, exit code != 0 when `|| true` is
    # absent) it writes an observation tagged `type=check_failed` with
    # no status_code. The handler treats that as down semantically --
    # the entity did not produce an HTTP response -- so the operator
    # still gets paged. Without this branch, every check_failed shape
    # produces zero findings (opus fell-r3 #1). Mapping to status=0
    # collapses this with the curl-emitted "000" path so subsequent
    # state comparisons (down -> still down -> recovery) work uniformly.
    if status is None and observation.get("type") == "check_failed":
        status = 0
        failure_detail = (
            str(observation.get("error"))
            if observation.get("error") is not None
            else "source check failed"
        )

    findings = []
    if status is not None and status != 200 and (last_status is None or last_status == 200):
        findings.append(
            {
                "source": source_ref,
                "type": "down",
                "entity": entity,
                "severity": severity,
                "timestamp": _utcnow(),
                "target_pipes": [target_pipe],
                "body": {
                    "text": (
                        f"{url} {_describe_status(status)}; previous status was "
                        f"{last_status if last_status is not None else 'unknown'}."
                        + (f" Check error: {failure_detail}" if failure_detail else "")
                    )
                },
            }
        )
    elif status == 200 and last_status is not None and last_status != 200:
        findings.append(
            {
                "source": source_ref,
                "type": "clearance",
                "entity": entity,
                "severity": "info",
                "timestamp": _utcnow(),
                "target_pipes": [clearance_pipe],
                "body": {
                    "text": (
                        f"{url} returned HTTP 200; previous status was "
                        f"{_describe_status(last_status)}."
                    )
                },
            }
        )

    new_state = {"last_status": status} if status is not None else prior_state
    json.dump({"findings": findings, "new_state": new_state}, sys.stdout)
    sys.stdout.write("\n")


def _coerce_status(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _describe_status(status: int) -> str:
    """Curl emits %{http_code}=000 when the request never completes a
    response (DNS failure, TCP refused, TLS handshake error, timeout). The
    `|| true` in the watch command lets that flow through as status=0; the
    handler shouldn't pretend HTTP 0 is a real status code."""
    if status == 0:
        return "did not respond (DNS, TCP, or TLS failure)"
    return f"returned HTTP {status}"


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
