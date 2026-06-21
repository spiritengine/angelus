#!/usr/bin/env python3
"""Pitch seed triager: turn a drip observation into a seed finding.

Firing discipline (brief-20260619-opi5, Patrick's spec): HIGH RECALL, LOW
PRECISION. Pitch deliberately over-fires -- roughly one real hit per seven or
eight rejects -- because a missed seed (the Skills/ToxicSkills miss) costs a
talk and the territory, while a spurious seed costs a minute of triage. This
triager therefore does NOT add a quiet-by-default filter: every distinct seed
observation becomes a finding. Do not "tune this for quiet" -- that inverts the
whole point and reintroduces start-latency, the failure this source exists to
fix.

Dedup is structural, not by suppression. Each seed carries a stable id
(seed_store.seed_id over cannon+event), which is both the observation `state`
token and the finding `entity`. So:
  - the drip source stamps emitted_at and drains a seed exactly once, and
  - the catalog emission gate opens one incident per seed id and drops any
    repeat for the same id.
A given seed never alerts twice without any state being kept here.

Idle observations (store empty / fully drained) carry no seed_id and produce no
finding.

Routing: low/medium seeds -> the daily `seeds` pipe (metadata.target_pipe).
severity == "high" (rare, xz-scale) -> the urgent `now` pipe
(metadata.urgent_pipe) AS WELL, jumping the daily queue while keeping the daily
pipe as a never-drop floor (see _route). urgent_pipe is cross-ref validated at
load now (finding-20260619-dle0), so a typo fails the config; the floor stays as
defense-in-depth beneath that. Per-finding target_pipes is the same mechanism the
canary/http handlers use to route by severity; no engine change.
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

    source_ref = str(
        triager.get("source_ref") or observation.get("source_ref") or ""
    )
    target_pipe = str(metadata.get("target_pipe") or "seeds")
    urgent_pipe = str(metadata.get("urgent_pipe") or "now")

    seed_id = observation.get("seed_id")
    obs_type = str(observation.get("type") or "")

    findings = []
    # A torn store raises a corruption observation (no seed_id). Turn it into a
    # high-severity finding so it jumps to the urgent pipe with the daily `seeds`
    # pipe as the never-drop floor -- a possibly-lost seed is urgent. The fixed
    # entity makes the catalog emission gate dedup it to exactly one incident.
    if obs_type == "store_corrupt":
        malformed = observation.get("malformed_lines")
        findings.append(
            {
                "source": source_ref,
                "type": "store_corrupt",
                "entity": str(observation.get("entity") or "pitch-seed-store"),
                "severity": "high",
                "timestamp": _utcnow(),
                "target_pipes": _route("high", target_pipe, urgent_pipe),
                "body": {
                    "text": (
                        f"Pitch seed store has {malformed} malformed line(s). A "
                        "seed may be torn; the bytes are preserved (not deleted) "
                        "in state/pitch-seeds.jsonl -- inspect and repair them so "
                        "the row drains."
                    ),
                    "malformed_lines": malformed,
                },
            }
        )
    # Idle observations carry no seed_id; emit nothing. Everything else with a
    # seed_id becomes a finding (high recall -- no further gating).
    elif obs_type != "idle" and seed_id:
        severity = str(observation.get("severity") or "low")
        target_pipes = _route(severity, target_pipe, urgent_pipe)
        cannon = str(observation.get("cannon") or "")
        event = str(observation.get("event") or "")
        build_move = str(observation.get("build_move") or "")
        findings.append(
            {
                "source": source_ref,
                "type": "seed",
                # The seed id is the dedup key (one incident per seed).
                "entity": str(seed_id),
                "severity": severity,
                "timestamp": _utcnow(),
                "target_pipes": target_pipes,
                "body": {
                    "text": _body_text(cannon, event, build_move),
                    # Structured fields carried through so the seeds digest
                    # (preamble template + chronicler) can render the card
                    # without re-parsing the text blob.
                    "cannon": cannon,
                    "event": event,
                    "build_move": build_move,
                    "severity": severity,
                    "seed_id": str(seed_id),
                    "discovered_at": observation.get("discovered_at"),
                },
            }
        )

    # Stateless: dedup is structural (seed id + emission gate), so there is
    # nothing to carry between fires. Pass prior_state straight through.
    json.dump({"findings": findings, "new_state": prior_state}, sys.stdout)
    sys.stdout.write("\n")


def _route(severity: str, target_pipe: str, urgent_pipe: str) -> list[str]:
    """Pipes a seed finding routes to, with the daily pipe as a never-drop floor.

    A normal seed batches into the daily `target_pipe`. The rare severity=="high"
    (xz-scale) seed ADDITIONALLY routes to `urgent_pipe` ahead of it, jumping the
    daily queue. lodging/config.py now cross-ref validates urgent_pipe too
    (finding-20260619-dle0), so a typo (`urgent_pipe: nwo`) fails the config at
    load rather than being silently skipped at enqueue. The daily pipe is kept as
    a FLOOR beneath the urgent jump anyway -- defense-in-depth: were an unknown
    urgent_pipe ever to reach here, the seed would still deliver (and its now-open
    incident would not suppress a corrected re-alert -- the silent-loss failure
    this source exists to prevent). Order preserved, dups collapsed (urgent_pipe
    may equal target_pipe).
    """
    pipes = [urgent_pipe, target_pipe] if severity == "high" else [target_pipe]
    seen: set[str] = set()
    return [pipe for pipe in pipes if not (pipe in seen or seen.add(pipe))]


def _body_text(cannon: str, event: str, build_move: str) -> str:
    return f"Seed: {event}\nCannon: {cannon}\nBuild move: {build_move}"


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
