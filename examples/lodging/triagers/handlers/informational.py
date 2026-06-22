#!/usr/bin/env python3
"""Generic informational triager: fan a batch observation out into findings.

One batch observation carries every record drained this tick; this handler emits
one finding per record. Each content finding is type `info` (declared
informational in informational.yaml, so the engine lanes it informational: a
one-shot delivery that opens no incident and renders under "Updates" in the
daily email). A store_corrupt observation -- a torn inbox, a real CONDITION --
becomes a high-severity finding routed to the urgent pipe with the digest as a
floor.

Routing is fixed to metadata.target_pipe (the daily email), cross-ref validated
at load. The intake deliberately does NOT honour a free-form per-record pipe:
that would be runtime data write_finding silently drops on a typo, the exact
silent-misroute class the lodging guards against. A second digest, if ever
wanted, is a lodged pipe + a metadata change, not per-record data.

dedup_key is source-namespaced (`<record source>:<record id>`) so two different
producers that pick the same id cannot collide and silently drop one another --
the informational lane's deliver-once gate keys on dedup_key.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime


def main() -> None:
    request = json.load(sys.stdin)
    observation = request.get("observation") or {}
    triager = request.get("triager") or {}
    metadata = triager.get("metadata") or {}

    source_ref = str(triager.get("source_ref") or observation.get("source_ref") or "")
    target_pipe = str(metadata.get("target_pipe") or "daily")
    urgent_pipe = str(metadata.get("urgent_pipe") or "now")

    obs_type = str(observation.get("type") or "")
    findings: list[dict] = []

    if obs_type == "store_corrupt":
        malformed = observation.get("malformed_lines")
        findings.append(
            {
                "source": source_ref,
                "type": "store_corrupt",
                "entity": str(observation.get("entity") or "informational-store"),
                "severity": "high",
                "timestamp": _utcnow(),
                # A real condition: jump to the urgent pipe with the digest as a
                # never-drop floor. store_corrupt is NOT an informational type,
                # so the engine lanes this a condition (it opens an incident).
                "target_pipes": _dedup([urgent_pipe, target_pipe]),
                "body": {
                    "text": (
                        f"Informational inbox has {malformed} malformed line(s). "
                        "An update may be torn; the bytes are preserved (not "
                        "deleted) in state/informational.jsonl -- inspect and "
                        "repair so the row drains."
                    ),
                    "malformed_lines": malformed,
                },
            }
        )
    elif obs_type == "batch":
        for record in observation.get("records") or []:
            if not isinstance(record, dict):
                # A malformed records shape (a scalar where a dict is expected)
                # must not crash the whole batch -- skip it defensively. The
                # shipped store only ever emits dict records, so this guards an
                # injected/corrupted observation body.
                continue
            rid = str(record.get("id") or "")
            rsource = str(record.get("source") or "unknown")
            title = str(record.get("title") or "").strip()
            if not rid or not title:
                # A malformed record should have been quarantined upstream; skip
                # defensively rather than emit a titleless finding.
                continue
            findings.append(
                {
                    "source": source_ref,
                    "type": "info",
                    # entity is the record id; dedup_key is source-namespaced so
                    # cross-producer id collisions cannot silently drop.
                    "entity": rid,
                    "dedup_key": f"{rsource}:{rid}",
                    "severity": str(record.get("severity") or "low"),
                    "timestamp": _utcnow(),
                    "target_pipes": [target_pipe],
                    "body": {
                        "title": title,
                        "body": record.get("body"),
                        "source": rsource,
                        "severity": str(record.get("severity") or "low"),
                    },
                }
            )
    # idle observations produce no finding.

    json.dump({"findings": findings, "new_state": {}}, sys.stdout)
    sys.stdout.write("\n")


def _dedup(pipes: list[str]) -> list[str]:
    seen: set[str] = set()
    return [p for p in pipes if not (p in seen or seen.add(p))]


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
