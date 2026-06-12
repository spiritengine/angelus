#!/usr/bin/env python3
"""GitHub Actions latest-run-on-default-branch triager.

Reads the most recent workflow run for the entity's default branch and emits
a `down` finding on healthy→failing transitions (or on a first observation
that's already failing), and a `clearance` finding on failing→healthy
recoveries. Skip findings on null conclusions (run in progress, no runs at
all) so a half-finished CI run doesn't churn alerts.

Observation shape produced by the watch's check command:

    {"entity": "skein", "conclusion": "success" | "failure" | ... | null,
     "status": "completed" | "in_progress" | null,
     "run_started": ISO8601 | null, "sha": HEX | null, "workflow": NAME | null}

State stored across cycles: `{"last_conclusion": "ok" | "failing"}`. Absent
on the first observation. `null` conclusions never change stored state.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime

# Treat skipped and neutral runs as healthy: a workflow that was
# path-filtered out or returned a neutral conclusion isn't a brokenness
# signal. Everything else non-null non-success is failing.
_HEALTHY_CONCLUSIONS = frozenset({"success", "skipped", "neutral"})


def main() -> None:
    request = json.load(sys.stdin)
    observation = request.get("observation") or {}
    prior_state = request.get("prior_state") or {}
    triager = request.get("triager") or {}
    metadata = triager.get("metadata") or {}

    source_ref = str(triager.get("source_ref") or "")
    entity = str(metadata.get("entity") or observation.get("entity") or "unknown")
    severity = str(metadata.get("severity") or "medium")
    target_pipe = str(metadata.get("target_pipe") or "now")
    clearance_pipe = str(metadata.get("clearance_pipe") or target_pipe)

    # check_failed observations (gh CLI missing, auth failure, timeout
    # SIGKILL, JSON parse error) leave us blind: we don't know if CI is
    # green or red. Emitting `down` would be noisy on transient gh
    # outages; emitting nothing leaves a real visibility hole. The
    # compromise: skip entirely so per-repo monitoring failures don't
    # churn alerts. The skip is safe because the blind-watch hole is
    # covered one level down: the daemon counts consecutive check_failed
    # fires per source at fire time (collapse means we'd never see the
    # repeats here) and opens an internal/source alarm when a check is
    # persistently failing -- see AngelusDaemon._note_source_fire_outcome.
    # Belfry separately catches daemon-wide breakage (out-of-process,
    # see angelus README).
    if observation.get("type") == "check_failed":
        json.dump({"findings": [], "new_state": prior_state}, sys.stdout)
        sys.stdout.write("\n")
        return

    effective = _classify(observation.get("conclusion"))
    last = prior_state.get("last_conclusion")  # None | "ok" | "failing"

    findings: list[dict] = []
    new_state = dict(prior_state)

    if effective is None:
        # Run still in progress or repo has no runs yet. Don't change
        # state and don't emit -- we'll classify on the next cycle when
        # a terminal conclusion lands.
        pass
    elif effective == "failing" and last != "failing":
        findings.append(
            {
                "source": source_ref,
                "type": "down",
                "entity": entity,
                "severity": severity,
                "timestamp": _utcnow(),
                "target_pipes": [target_pipe],
                "body": {"text": _down_body(entity, observation)},
            }
        )
        new_state["last_conclusion"] = "failing"
    elif effective == "ok" and last == "failing":
        findings.append(
            {
                "source": source_ref,
                "type": "clearance",
                "entity": entity,
                "severity": "info",
                "timestamp": _utcnow(),
                "target_pipes": [clearance_pipe],
                "body": {"text": _clearance_body(entity, observation)},
            }
        )
        new_state["last_conclusion"] = "ok"
    elif effective == "ok" and last != "ok":
        # First observed healthy run; remember it so a future failing
        # run triggers a down finding rather than being suppressed.
        new_state["last_conclusion"] = "ok"

    json.dump({"findings": findings, "new_state": new_state}, sys.stdout)
    sys.stdout.write("\n")


def _classify(conclusion: object) -> str | None:
    """Return "ok", "failing", or None for unknown/in-progress.

    Defensive coerce-to-string in case jq somehow emits a non-string
    value -- the contract says it should be a string or null, but the
    handler shouldn't crash on a malformed observation.
    """
    if conclusion is None:
        return None
    value = str(conclusion).strip().lower()
    if not value:
        return None
    if value in _HEALTHY_CONCLUSIONS:
        return "ok"
    return "failing"


def _down_body(entity: str, observation: dict) -> str:
    workflow = observation.get("workflow") or "workflow"
    conclusion = observation.get("conclusion") or "unknown"
    sha = observation.get("sha") or ""
    short_sha = sha[:7] if sha else ""
    parts = [f"{entity} CI {conclusion}: {workflow}"]
    if short_sha:
        parts.append(f"@ {short_sha}")
    return " ".join(parts)


def _clearance_body(entity: str, observation: dict) -> str:
    workflow = observation.get("workflow") or "workflow"
    sha = observation.get("sha") or ""
    short_sha = sha[:7] if sha else ""
    parts = [f"{entity} CI recovered: {workflow} succeeded"]
    if short_sha:
        parts.append(f"@ {short_sha}")
    return " ".join(parts)


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
