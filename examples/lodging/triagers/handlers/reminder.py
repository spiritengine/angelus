#!/usr/bin/env python3
"""Reminder triager: turn a drip observation into a reminder finding.

Every distinct reminder observation becomes a finding -- there is no quiet-by-
default filter, because a reminder is an intent Patrick (or an agent) explicitly
filed; dropping it would be a missed deadline. Dedup is structural, not by
suppression: each reminder carries a RANDOM id (reminder_store.new_id), which is
both the observation `state` token and the finding `entity`. So the drip stamps
fired_at and drains a reminder exactly once, and the catalog emission gate opens
one incident per id and collapses an at-least-once re-emit. Because the id is
random (never a content hash), two genuinely-distinct reminders that share a date
and message still get distinct ids -> two findings, not one. (Contrast Pitch,
whose stable cannon+event id is deliberate cross-run dedup.)

Idle observations (nothing due) carry no reminder_id and produce no finding.

Routing: a normal reminder -> the daily `reminders` pipe (metadata.target_pipe),
which renders only new findings (no incident recurrence). A store_corrupt
observation (a possibly-lost reminder) -> the urgent `now` pipe
(metadata.urgent_pipe) AS WELL, jumping the daily queue while keeping the daily
pipe as a never-drop floor. urgent_pipe is cross-ref validated at load now
(finding-20260619-dle0), so a typo fails the config; the floor stays as
defense-in-depth -- were an unknown urgent_pipe ever to reach here it still
could not drop the alert.
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
    target_pipe = str(metadata.get("target_pipe") or "reminders")
    urgent_pipe = str(metadata.get("urgent_pipe") or "now")

    reminder_id = observation.get("reminder_id")
    obs_type = str(observation.get("type") or "")

    findings = []
    # A torn store raises a corruption observation (no reminder_id). Turn it into
    # a high-severity finding so it jumps to the urgent pipe with the daily
    # `reminders` pipe as the never-drop floor -- a possibly-lost reminder is a
    # possibly-missed deadline. The fixed entity makes the catalog emission gate
    # dedup it to one incident.
    if obs_type == "store_corrupt":
        malformed = observation.get("malformed_lines")
        findings.append(
            {
                "source": source_ref,
                "type": "store_corrupt",
                "entity": str(observation.get("entity") or "reminder-store"),
                "severity": "high",
                "timestamp": _utcnow(),
                "target_pipes": _route("high", target_pipe, urgent_pipe),
                "body": {
                    "text": (
                        f"Reminder store has {malformed} malformed line(s). A "
                        "reminder may be torn; the bytes are preserved (not "
                        "deleted) in state/reminders.jsonl -- inspect and repair "
                        "them so the row drains."
                    ),
                    "malformed_lines": malformed,
                },
            }
        )
    # Idle observations carry no reminder_id; emit nothing. Everything else with a
    # reminder_id becomes a finding (every filed reminder surfaces).
    elif obs_type != "idle" and reminder_id:
        severity = str(observation.get("severity") or "low")
        # A reminder is never an emergency (the brief's framing): it ALWAYS
        # batches into the daily `reminders` pipe, whatever its severity. Only the
        # store_corrupt alert above (a possibly-lost reminder) jumps to the urgent
        # pipe. So severity is display-only here, never a routing lever -- an agent
        # cannot page `now` just by filing a reminder with --severity high.
        target_pipes = [target_pipe]
        message = str(observation.get("message") or "")
        findings.append(
            {
                "source": source_ref,
                "type": "reminder",
                # The random reminder id is the dedup key (one incident per id).
                "entity": str(reminder_id),
                "severity": severity,
                "timestamp": _utcnow(),
                "target_pipes": target_pipes,
                "body": {
                    "text": message,
                    # Structured fields carried through so the reminders digest
                    # (preamble template + chronicler) renders the card and
                    # computes "in N days" at render time -- NOT baked into prose
                    # here, where fired_at and the digest render-time differ.
                    "message": message,
                    "fire_date": observation.get("fire_date"),
                    "deadline": observation.get("deadline"),
                    "severity": severity,
                    "reminder_id": str(reminder_id),
                    "created_by": observation.get("created_by"),
                },
            }
        )

    # Stateless: dedup is structural (random id + emission gate), so there is
    # nothing to carry between fires. Pass prior_state straight through.
    json.dump({"findings": findings, "new_state": prior_state}, sys.stdout)
    sys.stdout.write("\n")


def _route(severity: str, target_pipe: str, urgent_pipe: str) -> list[str]:
    """Pipes a finding routes to, with the daily pipe as a never-drop floor.

    A normal reminder batches into the daily `target_pipe`. A high-severity
    finding (only the store_corrupt alert today) ADDITIONALLY routes to
    `urgent_pipe` ahead of it. config.py now cross-ref validates urgent_pipe too
    (finding-20260619-dle0), so a typo (`urgent_pipe: nwo`) fails the config at
    load rather than being silently skipped at enqueue. The daily pipe is kept as
    a FLOOR beneath the urgent jump anyway -- defense-in-depth: were an unknown
    urgent_pipe ever to reach here, the alert would still deliver (and its now-open
    incident would not suppress a corrected re-alert). Order preserved, dups
    collapsed.
    """
    pipes = [urgent_pipe, target_pipe] if severity == "high" else [target_pipe]
    seen: set[str] = set()
    return [pipe for pipe in pipes if not (pipe in seen or seen.add(pipe))]


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
