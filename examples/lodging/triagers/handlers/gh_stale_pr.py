#!/usr/bin/env python3
"""GitHub stale-PR triager.

Emits a `stale_pr` finding the first cycle a PR crosses the staleness
threshold and a `clearance` finding when that specific PR recovers
(merged, closed, or refreshed). State stores the set of PR numbers we've
already alerted on so the daily pipe doesn't get the same PR every cycle.

Observation shape produced by the watch's check command:

    {"entity": "skein", "prs": [{"number": 7, "title": "...",
                                 "updatedAt": "2025-12-01T10:00:00Z"}, ...]}

`gh pr list --json ...` produces a bare array, but the source-runner
requires a JSON object. The watch wraps the array via jq into the shape
above; if that wrapping is ever changed the contract here breaks loudly
(KeyError) rather than silently skipping PRs.

Incident model: incidents are keyed PER PR, not per repo. The finding
entity is `"{repo}#{number}"` (e.g. "skein#7"), so the catalog's UNIQUE
(source, type, entity) on open incidents (storage/catalog.py) gives each
stale PR its own incident. Consequences:

  - A second PR going stale while the first is still open opens its own
    incident and emits its own finding — per-PR visibility is preserved
    under the B30 emission gate.
  - When a specific PR is no longer stale, we emit a `clearance` keyed to
    THAT PR's entity, closing only its incident and re-arming the gate
    for it. Partial recoveries clear the recovered PR and leave the
    still-stale PRs' incidents open (each independent).

Threshold is configurable via watch metadata `stale_days` (default 30).
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta


_DEFAULT_STALE_DAYS = 30


def main() -> None:
    request = json.load(sys.stdin)
    observation = request.get("observation") or {}
    prior_state = request.get("prior_state") or {}
    triager = request.get("triager") or {}
    metadata = triager.get("metadata") or {}

    source_ref = str(triager.get("source_ref") or "")
    entity = str(metadata.get("entity") or observation.get("entity") or "unknown")
    severity = str(metadata.get("severity") or "low")
    target_pipe = str(metadata.get("target_pipe") or "daily")
    clearance_pipe = str(metadata.get("clearance_pipe") or target_pipe)
    stale_days = _coerce_positive_int(
        metadata.get("stale_days"), default=_DEFAULT_STALE_DAYS
    )

    already_alerted = set(_coerce_pr_number_list(prior_state.get("alerted_prs")))

    # check_failed: skip, like gh_actions_status. The daily pipe doesn't
    # need monitoring-broken noise; the daemon's fire-time internal/source
    # alarm covers a persistently failing check (collapse hides the
    # repeats from this layer -- see _note_source_fire_outcome) and belfry
    # catches daemon-wide outages.
    # Pass alerted_prs through untouched so a transient gh outage doesn't
    # collapse to an empty set and trigger a spurious clearance on the
    # next successful check. Spread prior_state so any future state keys
    # survive (fell-r2 NIT #2 -- prior code narrowed pass-through to
    # alerted_prs only, which would silently erase a future last_seen_at
    # or similar extension on every check_failed cycle).
    if observation.get("type") == "check_failed":
        passthrough = dict(prior_state)
        passthrough["alerted_prs"] = sorted(already_alerted)
        json.dump({"findings": [], "new_state": passthrough}, sys.stdout)
        sys.stdout.write("\n")
        return

    prs = observation.get("prs") or []
    now = datetime.now(UTC)
    threshold = now - timedelta(days=stale_days)

    currently_stale: dict[int, dict] = {}
    for pr in prs:
        number = _coerce_pr_number(pr.get("number"))
        if number is None:
            continue
        updated_at = _parse_iso(pr.get("updatedAt"))
        if updated_at is not None and updated_at < threshold:
            currently_stale[number] = pr
            continue
        # Defensive: a PR with null/unparseable updatedAt that was
        # previously alerted stays in the stale set so a transient
        # field-shape change can't flap an alert open<->closed. gh
        # always populates updatedAt today, but the alternative is
        # a silent re-alert next cycle when the field reappears.
        if updated_at is None and number in already_alerted:
            currently_stale[number] = pr

    new_alerts = sorted(set(currently_stale) - already_alerted)
    recovered = sorted(already_alerted - set(currently_stale))
    findings: list[dict] = []
    # New stale PRs: emit one finding each, in number order for stable
    # output (tests + log readability). Each opens its own per-PR
    # incident keyed on entity "{repo}#{number}".
    for number in new_alerts:
        pr = currently_stale[number]
        findings.append(
            {
                "source": source_ref,
                "type": "stale_pr",
                "entity": _pr_entity(entity, number),
                "severity": severity,
                "timestamp": _utcnow(),
                "target_pipes": [target_pipe],
                "body": {"text": _stale_body(entity, pr, stale_days)},
            }
        )

    # Recovered PRs: each previously-alerted PR that is no longer stale
    # (merged, closed, or refreshed) gets a `clearance` keyed to its own
    # entity, closing only that PR's incident and re-arming the gate for
    # it. Emitting per recovered PR (rather than once per repo) keeps the
    # incident lifecycle per-PR-consistent: a partial recovery clears the
    # recovered PR while still-stale PRs' incidents stay open.
    #
    # Dedup_key note: catalog.write_finding synthesizes dedup_key as
    # f"{source}:{finding_type}:{entity}". With per-PR entities the
    # clearance and stale_pr keys differ only by type, both PR-scoped. The
    # shipped clearance_pipe="daily" is a digest pipe (mute-skip per
    # pipes/runner.py), so the type divergence is benign. A future operator
    # who points clearance_pipe at an immediate pipe and wants a per-PR
    # stale_pr mute to also suppress that PR's clearance should set an
    # explicit dedup_key shared across the PR's stale_pr/clearance pair.
    for number in recovered:
        findings.append(
            {
                "source": source_ref,
                "type": "clearance",
                "entity": _pr_entity(entity, number),
                "severity": "info",
                "timestamp": _utcnow(),
                "target_pipes": [clearance_pipe],
                "body": {"text": _clearance_body(entity, number)},
            }
        )

    new_state = {"alerted_prs": sorted(currently_stale)}
    json.dump({"findings": findings, "new_state": new_state}, sys.stdout)
    sys.stdout.write("\n")


def _stale_body(entity: str, pr: dict, stale_days: int) -> str:
    number = pr.get("number")
    title = (pr.get("title") or "").strip() or "(no title)"
    updated_at = pr.get("updatedAt") or "unknown"
    return (
        f"{entity} PR #{number} stale for >{stale_days}d: "
        f"{title} (last activity {updated_at})"
    )


def _clearance_body(entity: str, number: int) -> str:
    return f"{entity} PR #{number} no longer stale."


def _pr_entity(entity: str, number: int) -> str:
    """Per-PR incident key: "{repo}#{number}" (e.g. "skein#7"). Keying the
    incident on the individual PR — not the repo — gives each stale PR its
    own incident under the catalog's UNIQUE (source, type, entity) gate."""
    return f"{entity}#{number}"


def _coerce_pr_number(value: object) -> int | None:
    """gh emits integer PR numbers, but defensively coerce strings too in
    case a downstream jq filter ever wraps them."""
    if isinstance(value, bool):
        return None  # bool subclasses int; reject explicitly
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _coerce_pr_number_list(value: object) -> list[int]:
    if not isinstance(value, list):
        return []
    out: list[int] = []
    for item in value:
        number = _coerce_pr_number(item)
        if number is not None:
            out.append(number)
    return out


def _coerce_positive_int(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str):
        try:
            n = int(value)
        except ValueError:
            return default
        return n if n > 0 else default
    return default


def _parse_iso(value: object) -> datetime | None:
    """Parse the ISO8601 timestamps gh emits (e.g. "2026-05-21T15:39:18Z").

    Python 3.11+ fromisoformat handles the trailing Z, but we keep the
    explicit replace so a stray older runtime won't silently produce
    naive datetimes that compare wrong with the aware `now`.
    """
    if not isinstance(value, str) or not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
