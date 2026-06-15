#!/usr/bin/env python3
"""Auto-reprocess fixer handler (brief-20260612-q7de): re-budget the failing
triager's source so an exhausted transition gets another triage.

When a triager exhausts its retry ladder on a collapsed transition observation
the product finding for that transition is lost until the next state change
(brief-20260612-29he). Exhaustion is loud -- it opens an internal/triage
incident -- but the product alert is gone. The documented heal is
catalog.reprocess_source, which returns the exhausted ('triage_failed')
observation to 'ready' so the triage loop re-picks it (brief-20260607-6qsq
Stage 1). This handler issues that reprocess by shelling `angelus reprocess`,
the same control-socket op an operator would run by hand.

The daemon resolves the triager NAME the incident carries as its entity to
that triager's source and passes it as condition.source_ref; this handler
reprocesses ONLY that source, never an unrelated one. The fixer's guardrails
cap how often it may fire: a transient failure heals on the first reprocess
(the triager succeeds and the incident clears); a deterministically-broken
triager just re-exhausts and re-opens the same incident, the per-condition
budget runs down, and the registry converges to the loud steady state
(incident stays open, belfry red) instead of storming reprocess forever.

  stdin  -- JSON: {"fixer": {...}, "condition": {"kind":
            "open_internal_incident", "condition_key": ..., "incident": {...},
            "source_ref": "<source>"}}
  stdout -- JSON: {"outcome": "<non-empty>", "note": "<optional>"}
  exit 0 on success; a non-zero exit or non-JSON output is recorded by the
  daemon as outcome="error" and counts against the fixer's guardrail.
"""

from __future__ import annotations

import json
import subprocess
import sys


def main() -> int:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        payload = {}
    condition = payload.get("condition") or {}
    source_ref = condition.get("source_ref")
    if not source_ref:
        # The daemon could not resolve the incident's triager to a source (the
        # triager was hot-removed, or this fixer was bound to a non-triage
        # incident whose entity is not a triager). Nothing to reprocess; report
        # a no-op outcome the audit log keeps rather than erroring.
        print(
            json.dumps(
                {
                    "outcome": "skipped",
                    "note": "no source_ref in condition; nothing to reprocess",
                }
            )
        )
        return 0
    result = subprocess.run(
        ["angelus", "reprocess", str(source_ref)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # Surface the failure as a non-zero exit so the daemon records
        # outcome="error" and it counts against the guardrail like any other.
        sys.stderr.write(result.stderr)
        return 1
    note = result.stdout.strip() or f"reprocessed {source_ref}"
    print(json.dumps({"outcome": "reprocessed", "note": note}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
