#!/usr/bin/env python3
"""Pitch seed triager: turn a drip observation into a seed finding.

Firing discipline (brief-20260619-opi5, Patrick's spec): HIGH RECALL, LOW
PRECISION. Pitch deliberately over-fires -- a missed change costs more than a
spurious one -- so this triager does NOT add a quiet-by-default filter: every
distinct seed observation becomes a finding. Do not "tune this for quiet" -- that
reintroduces the start-latency this source exists to fix.

Dedup is structural, not by suppression. Each seed carries a stable id
(seed_store.seed_id over the event), which is both the observation `state` token
and the finding `entity`. So:
  - the drip source stamps emitted_at and drains a seed exactly once, and
  - the catalog emission gate opens one incident per seed id and drops any
    repeat for the same id.
A given seed never alerts twice without any state being kept here.

Idle observations (store empty / fully drained) carry no seed_id and produce no
finding.

Routing: seeds are INFORMATIONAL -- every seed batches into the daily digest
(metadata.target_pipe), whatever its content, and opens no incident (decision
2026-06-22, finding-20260622-tf7x). A seed never jumps to `now`. urgent_pipe
carries ONLY the store_corrupt torn-store alert below -- a real CONDITION, a
possibly-lost seed. Both pipes are cross-ref validated at load
(finding-20260619-dle0).
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
    # high-severity finding so it jumps to the urgent pipe with the daily digest
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
        event = str(observation.get("event") or "")
        source = str(observation.get("source") or "")
        date = str(observation.get("date") or "")
        findings.append(
            {
                "source": source_ref,
                "type": "seed",
                # The seed id is the dedup key (one incident per seed).
                "entity": str(seed_id),
                # Seeds are informational; severity is not meaningful on this
                # lane. A fixed low keeps the finding schema consistent without
                # implying urgency -- routing is digest-only regardless.
                "severity": "low",
                "timestamp": _utcnow(),
                # DIGEST-ONLY (decision 2026-06-22, finding-20260622-tf7x): a seed
                # routes ONLY to the daily digest, never to `now`.
                "target_pipes": [target_pipe],
                "body": {
                    "text": _body_text(event, source, date),
                    # An explicit headline for the informational lane. The live
                    # email card renders body.event (updates-cards.j2 seed branch)
                    # and the compact push already headlines the event via
                    # body.text's first line -- so title is not strictly required
                    # for either. But setting it makes a seed headline its event
                    # through ANY informational renderer (the push's
                    # _informational_headline and any generic-branch card both
                    # prefer body.title), instead of depending on which branch or
                    # fallback happens to apply.
                    "title": event,
                    # Structured fields carried through so the digest cards
                    # (preamble template + chronicler) render without re-parsing
                    # the text blob.
                    "event": event,
                    "source": source,
                    "date": date,
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
    """Pipes a store_corrupt finding routes to, with the daily pipe as a floor.

    The torn-store alert (severity=="high") routes to `urgent_pipe` ahead of the
    daily `target_pipe`, jumping the queue, while the daily pipe stays a
    never-drop FLOOR beneath it. lodging/config.py cross-ref validates both pipes
    at load (finding-20260619-dle0), so a typo (`urgent_pipe: nwo`) fails the
    config rather than being silently skipped at enqueue. Order preserved, dups
    collapsed (urgent_pipe may equal target_pipe). Seeds never call this -- they
    route digest-only.
    """
    pipes = [urgent_pipe, target_pipe] if severity == "high" else [target_pipe]
    seen: set[str] = set()
    return [pipe for pipe in pipes if not (pipe in seen or seen.add(pipe))]


def _body_text(event: str, source: str, date: str) -> str:
    line = event
    if source:
        line += f"\n{source}"
    if date:
        line += f"\n{date}"
    return line


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
