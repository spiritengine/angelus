#!/usr/bin/env python3
"""Trivial fixer handler (B11): acknowledge the condition, remediate nothing.

This is the wiring-proof fixer the registry ships with. It reads the matched
condition as JSON on stdin and reports an "observed" outcome on stdout -- it
does NOT attempt any repair. Its job is to prove the whole path end to end:
discovery (a fixers/<name>.yaml that binds it gets picked up), condition
evaluation, the guardrail gate, handler invocation, and the fixers.log audit
line / digest fixer_actions surfacing.

It is also the template to copy for a real fixer: a real handler does its
repair by shelling out (systemctl, notify-pat, curl) and reports the outcome
the same way. The contract:

  stdin  -- JSON: {"fixer": {...}, "condition": {"kind": ..., "condition_key":
            ..., "incident"|"channel": {...}}}
  stdout -- JSON: {"outcome": "<non-empty string>", "note": "<optional>"}
  exit 0 on success; a non-zero exit or non-JSON output is recorded by the
  daemon as outcome="error" and counts against the fixer's guardrail.
"""

from __future__ import annotations

import json
import sys


def main() -> int:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        payload = {}
    condition = payload.get("condition") or {}
    key = condition.get("condition_key", "<unknown>")
    print(
        json.dumps(
            {
                "outcome": "observed",
                "note": f"observe-only fixer acknowledged {key}",
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
