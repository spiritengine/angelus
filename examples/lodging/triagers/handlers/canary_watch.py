#!/usr/bin/env python3
"""Generic url-watch triager for synthetic canary sources.

The canaries (sources/scheduled/canary-loose-{a,b}.yaml) emit JSON
observations that carry their own source_ref and entity so this handler
can route findings without hardcoding either value. The transition rule
matches the generic http_status handler: a non-200 status_code emits a
finding only on the up->down edge (last_status None or 200), so
within-source repeats are deduped via prior_state.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime


def main() -> None:
    request = json.load(sys.stdin)
    observation = request.get("observation") or {}
    prior_state = request.get("prior_state") or {}
    source_ref = str(observation.get("source_ref") or "")
    entity = str(observation.get("entity") or "")
    if not source_ref or not entity:
        raise ValueError(
            "canary_watch observation must include source_ref and entity"
        )
    url = str(observation.get("url") or "")
    status = _coerce_status(observation.get("status_code"))
    last_status = _coerce_status(prior_state.get("last_status"))
    findings = []

    if status is not None and status != 200 and (last_status is None or last_status == 200):
        findings.append(
            {
                "source": source_ref,
                "type": "down",
                "entity": entity,
                "severity": "high",
                "timestamp": _utcnow(),
                "target_pipes": ["now", "daily"],
                "body": {
                    "text": (
                        f"{url} returned HTTP {status}; previous status was "
                        f"{last_status if last_status is not None else 'unknown'}."
                    ),
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
                "target_pipes": ["daily"],
                "body": {
                    "text": (
                        f"{url} returned HTTP 200; "
                        f"previous status was {last_status}."
                    ),
                },
            }
        )

    new_state = {"last_status": status} if status is not None else prior_state
    json.dump({"findings": findings, "new_state": new_state}, sys.stdout)
    sys.stdout.write("\n")


def _coerce_status(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
