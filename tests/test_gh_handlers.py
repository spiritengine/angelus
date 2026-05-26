"""Handler tests for the `repo` entity kind: gh_actions_status and gh_stale_pr.

Mirrors the runner contract shape (observation, prior_state, triager.metadata)
without going through the full triage pipeline, the way test_entities_watch.py
exercises http_status. End-to-end coverage that the watch YAML substitutes
correctly and the load_lodging wiring fans out per repo lives at the bottom.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from angelus.lodging import load_lodging
from angelus.triage import run_python_triager


REPO_ROOT = Path(__file__).resolve().parents[1]
CI_HANDLER = REPO_ROOT / "triagers" / "handlers" / "gh_actions_status.py"
STALE_HANDLER = REPO_ROOT / "triagers" / "handlers" / "gh_stale_pr.py"


def _invoke(handler: Path, observation: dict, prior_state: dict, metadata: dict) -> dict:
    payload = json.dumps(
        {
            "observation": observation,
            "prior_state": prior_state,
            "triager": {
                "name": "test-triager",
                "source_ref": "scheduled/test__entity",
                "metadata": metadata,
            },
        }
    )
    result = subprocess.run(
        [sys.executable, str(handler)],
        input=payload.encode("utf-8"),
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


# --- gh_actions_status: classification + transitions ------------------------


def test_ci_first_observation_failing_emits_down() -> None:
    out = _invoke(
        CI_HANDLER,
        {
            "entity": "skein",
            "conclusion": "failure",
            "status": "completed",
            "sha": "abc1234567",
            "workflow": "Tests",
        },
        {},
        {"entity": "skein", "severity": "medium", "target_pipe": "now"},
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "down"
    assert findings[0]["entity"] == "skein"
    assert findings[0]["severity"] == "medium"
    assert findings[0]["target_pipes"] == ["now"]
    # Short SHA in body text aids triage without overwhelming the alert.
    assert "abc1234" in findings[0]["body"]["text"]
    assert out["new_state"] == {"last_conclusion": "failing"}


def test_ci_ok_to_failing_emits_down() -> None:
    out = _invoke(
        CI_HANDLER,
        {"entity": "skein", "conclusion": "cancelled", "workflow": "Tests"},
        {"last_conclusion": "ok"},
        {"entity": "skein", "severity": "medium", "target_pipe": "now"},
    )
    assert out["findings"][0]["type"] == "down"
    assert "cancelled" in out["findings"][0]["body"]["text"]
    assert out["new_state"] == {"last_conclusion": "failing"}


def test_ci_failing_to_ok_emits_clearance() -> None:
    out = _invoke(
        CI_HANDLER,
        {"entity": "skein", "conclusion": "success", "workflow": "Tests"},
        {"last_conclusion": "failing"},
        {
            "entity": "skein",
            "severity": "medium",
            "target_pipe": "now",
            "clearance_pipe": "daily",
        },
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "clearance"
    assert findings[0]["severity"] == "info"
    assert findings[0]["target_pipes"] == ["daily"]
    assert out["new_state"] == {"last_conclusion": "ok"}


def test_ci_first_observation_ok_no_finding_but_records_state() -> None:
    """A first observation of a passing build should not alert, but must
    record state so a subsequent failing build flips to a down finding
    (not silently treated as "first observation, suppress")."""
    out = _invoke(
        CI_HANDLER,
        {"entity": "skein", "conclusion": "success"},
        {},
        {"entity": "skein", "severity": "medium", "target_pipe": "now"},
    )
    assert out["findings"] == []
    assert out["new_state"] == {"last_conclusion": "ok"}


def test_ci_null_conclusion_does_not_change_state() -> None:
    """Run in progress or empty array -> conclusion is null. The handler
    must NOT downgrade a previously-known state to "unknown" on transient
    null observations -- otherwise a passing repo with an in-progress run
    would lose its "ok" state and a follow-up failure would silently not
    fire."""
    out = _invoke(
        CI_HANDLER,
        {"entity": "skein", "conclusion": None, "status": "in_progress"},
        {"last_conclusion": "ok"},
        {"entity": "skein", "severity": "medium", "target_pipe": "now"},
    )
    assert out["findings"] == []
    assert out["new_state"] == {"last_conclusion": "ok"}


def test_ci_skipped_and_neutral_treated_as_healthy() -> None:
    for conclusion in ("skipped", "neutral"):
        out = _invoke(
            CI_HANDLER,
            {"entity": "skein", "conclusion": conclusion},
            {"last_conclusion": "failing"},
            {"entity": "skein", "severity": "medium", "target_pipe": "now"},
        )
        assert out["findings"][0]["type"] == "clearance", (
            f"{conclusion} should clear a failing state"
        )
        assert out["new_state"] == {"last_conclusion": "ok"}


def test_ci_failing_to_failing_no_finding() -> None:
    """An already-known-failing repo doesn't re-alert every cycle. Without
    this the urgent pipe would spam on every 30m fire while a repo stays
    red."""
    out = _invoke(
        CI_HANDLER,
        {"entity": "skein", "conclusion": "failure"},
        {"last_conclusion": "failing"},
        {"entity": "skein", "severity": "medium", "target_pipe": "now"},
    )
    assert out["findings"] == []
    assert out["new_state"] == {"last_conclusion": "failing"}


def test_ci_check_failed_observation_no_finding_no_state_change() -> None:
    """gh CLI missing / auth error / timeout -> source-runner writes a
    check_failed observation. Treating that as down would flap on every
    transient gh outage; treating it as healthy would falsely clear real
    failures. The handler must pass through prior state untouched and
    emit no finding -- belfry catches daemon-wide problems separately."""
    out = _invoke(
        CI_HANDLER,
        {"type": "check_failed", "error": "gh: command not found"},
        {"last_conclusion": "failing"},
        {"entity": "skein", "severity": "medium", "target_pipe": "now"},
    )
    assert out["findings"] == []
    assert out["new_state"] == {"last_conclusion": "failing"}


def test_ci_unknown_conclusion_treated_as_failing() -> None:
    """gh's conclusion enum can grow over time (e.g. 'action_required').
    The handler must default unknown non-null conclusions to failing
    rather than silently treating them as healthy -- the failure mode
    here is "new GH state we've never seen is silently OK", which is
    exactly the silent-broken-monitoring class the brief is fixing."""
    out = _invoke(
        CI_HANDLER,
        {"entity": "skein", "conclusion": "some_future_state"},
        {"last_conclusion": "ok"},
        {"entity": "skein", "severity": "medium", "target_pipe": "now"},
    )
    assert out["findings"][0]["type"] == "down"


# --- gh_stale_pr: per-PR alerting + dedup -----------------------------------


def _iso_days_ago(n: int) -> str:
    return (
        (datetime.now(UTC) - timedelta(days=n))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def test_stale_pr_first_cycle_emits_one_finding_per_stale_pr() -> None:
    observation = {
        "entity": "skein",
        "prs": [
            {"number": 1, "title": "fresh", "updatedAt": _iso_days_ago(2)},
            {"number": 7, "title": "stale alpha", "updatedAt": _iso_days_ago(45)},
            {"number": 9, "title": "stale beta", "updatedAt": _iso_days_ago(60)},
        ],
    }
    out = _invoke(
        STALE_HANDLER,
        observation,
        {},
        {"entity": "skein", "severity": "low", "target_pipe": "daily"},
    )
    findings = out["findings"]
    assert len(findings) == 2
    # Deterministic numeric ordering keeps the daily digest stable for
    # the chronicler LLM and for log reading.
    assert [f["body"]["text"].split("#")[1].split(" ")[0] for f in findings] == ["7", "9"]
    for f in findings:
        assert f["type"] == "stale_pr"
        assert f["severity"] == "low"
        assert f["target_pipes"] == ["daily"]
    assert out["new_state"] == {"alerted_prs": [7, 9]}


def test_stale_pr_already_alerted_not_repeated() -> None:
    """Repeat cycle with the same stale PRs: no findings (already alerted)."""
    observation = {
        "entity": "skein",
        "prs": [
            {"number": 7, "title": "stale alpha", "updatedAt": _iso_days_ago(45)},
            {"number": 9, "title": "stale beta", "updatedAt": _iso_days_ago(60)},
        ],
    }
    out = _invoke(
        STALE_HANDLER,
        observation,
        {"alerted_prs": [7, 9]},
        {"entity": "skein", "severity": "low", "target_pipe": "daily"},
    )
    assert out["findings"] == []
    assert out["new_state"] == {"alerted_prs": [7, 9]}


def test_stale_pr_closed_or_merged_emits_clearance() -> None:
    """PR no longer in the open listing -> alerted_prs drops the number AND
    a pr_recovered finding routes to the clearance pipe. The "loop closed"
    visibility matters for the daily digest, not just bookkeeping."""
    observation = {
        "entity": "skein",
        "prs": [
            {"number": 9, "title": "stale beta", "updatedAt": _iso_days_ago(60)},
        ],
    }
    out = _invoke(
        STALE_HANDLER,
        observation,
        {"alerted_prs": [7, 9]},
        {
            "entity": "skein",
            "severity": "low",
            "target_pipe": "daily",
            "clearance_pipe": "daily",
        },
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "pr_recovered"
    assert "#7" in findings[0]["body"]["text"]
    assert "merged or closed" in findings[0]["body"]["text"]
    assert out["new_state"] == {"alerted_prs": [9]}


def test_stale_pr_refreshed_activity_emits_clearance() -> None:
    """An alerted PR that gets fresh activity (updatedAt moves forward,
    no longer past the threshold) drops from alerted_prs and emits a
    recovery -- so a comment on a stale PR closes the loop without
    waiting for the PR itself to close."""
    observation = {
        "entity": "skein",
        "prs": [
            {"number": 7, "title": "got a comment", "updatedAt": _iso_days_ago(1)},
        ],
    }
    out = _invoke(
        STALE_HANDLER,
        observation,
        {"alerted_prs": [7]},
        {"entity": "skein", "severity": "low", "target_pipe": "daily"},
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "pr_recovered"
    assert "fresh activity" in findings[0]["body"]["text"]
    assert out["new_state"] == {"alerted_prs": []}


def test_stale_pr_threshold_override_via_metadata() -> None:
    """A watch may override the stale_days threshold via watch.metadata.
    Verifies that path so a future archive-tier watch with stale_days=90
    Just Works."""
    observation = {
        "entity": "skein",
        "prs": [
            {"number": 7, "title": "borderline", "updatedAt": _iso_days_ago(45)},
        ],
    }
    out = _invoke(
        STALE_HANDLER,
        observation,
        {},
        {
            "entity": "skein",
            "severity": "low",
            "target_pipe": "daily",
            "stale_days": 60,
        },
    )
    assert out["findings"] == []
    assert out["new_state"] == {"alerted_prs": []}


def test_stale_pr_check_failed_no_change() -> None:
    out = _invoke(
        STALE_HANDLER,
        {"type": "check_failed", "error": "gh: not found"},
        {"alerted_prs": [7]},
        {"entity": "skein", "severity": "low", "target_pipe": "daily"},
    )
    assert out["findings"] == []
    assert out["new_state"] == {"alerted_prs": [7]}


def test_stale_pr_empty_listing_clears_old_alerts() -> None:
    """A repo with no open PRs after previously having stale ones should
    emit pr_recovered for the now-gone PRs and empty alerted_prs."""
    out = _invoke(
        STALE_HANDLER,
        {"entity": "skein", "prs": []},
        {"alerted_prs": [7, 9]},
        {"entity": "skein", "severity": "low", "target_pipe": "daily"},
    )
    assert len(out["findings"]) == 2
    assert all(f["type"] == "pr_recovered" for f in out["findings"])
    assert out["new_state"] == {"alerted_prs": []}


# --- End-to-end: load_lodging fans out per repo + substitution works --------


def test_load_lodging_fans_out_per_repo_active() -> None:
    """The shipped watches and entities expand to one synthesized source
    per (watch, active repo). Catches the regression where a future
    entity-loader change loses the repo-kind fan-out -- the brief's whole
    point of "drop-a-file, get-discovered" rides on this property."""
    lodging = load_lodging(REPO_ROOT)
    repo_sources = sorted(
        ref for ref in lodging.sources
        if ref.startswith("scheduled/ci-failing-on-main__")
        or ref.startswith("scheduled/stale-pr__")
    )
    # One ci-failing watch + one stale-pr watch per active repo entity.
    assert len(repo_sources) >= 2, "expected at least one repo fan-out pair"
    assert any(
        "ci-failing-on-main__skein" in ref for ref in repo_sources
    ), "skein entity must be picked up by ci-failing-on-main watch"
    assert any(
        "stale-pr__skein" in ref for ref in repo_sources
    ), "skein entity must be picked up by stale-pr watch"


def test_load_lodging_substitutes_repo_attrs_into_command() -> None:
    """github + default_branch from the entity YAML must land in the
    rendered command. If substitution breaks for repo kind, the daemon
    fires `gh run list --repo {github}` literally and gh errors."""
    lodging = load_lodging(REPO_ROOT)
    source = lodging.sources["scheduled/ci-failing-on-main__skein"]
    assert "--repo spiritengine/skein" in source.command
    assert "--branch master" in source.command
    # No leftover format-brace artifacts in the rendered command.
    assert "{github}" not in source.command
    assert "{default_branch}" not in source.command


def test_load_lodging_repo_triager_metadata_has_entity_and_pipes() -> None:
    lodging = load_lodging(REPO_ROOT)
    triager = lodging.triagers["ci-failing-on-main__skein"]
    assert triager.metadata["entity"] == "skein"
    assert triager.metadata["entity_kind"] == "repo"
    assert triager.metadata["target_pipe"] == "now"
    assert triager.metadata["clearance_pipe"] == "daily"


def test_repo_handler_through_runner(tmp_path: Path) -> None:
    """End-to-end: drive a synthesized repo triager through the actual
    runner with a constructed observation. Catches contract drift
    between expand's metadata shape and the gh_actions_status handler."""
    lodging = load_lodging(REPO_ROOT)
    triager = lodging.triagers["ci-failing-on-main__skein"]
    findings, state = asyncio.run(
        run_python_triager(
            triager,
            {
                "entity": "skein",
                "conclusion": "failure",
                "status": "completed",
                "sha": "deadbeef" * 5,
                "workflow": "Tests",
            },
            {"last_conclusion": "ok"},
        )
    )
    assert state == {"last_conclusion": "failing"}
    assert len(findings) == 1
    assert findings[0]["entity"] == "skein"
    assert findings[0]["type"] == "down"
    assert findings[0]["target_pipes"] == ["now"]


def test_repo_watch_command_round_trips_through_jq_on_empty_runs() -> None:
    """Confirm the brace-escaping in the watch YAML actually produces a
    jq expression that yields a valid JSON object even when the upstream
    array is empty -- the failure mode is "we render `{entity: ...}`
    that jq parses as malformed because of leftover python braces", and
    that would silently break every repo with no CI runs.

    Uses a local `echo` to feed jq the empty array, avoiding a live gh
    call (which would flake on network problems and rate limits)."""
    lodging = load_lodging(REPO_ROOT)
    source = lodging.sources["scheduled/ci-failing-on-main__skein"]
    # Replace the gh invocation with `echo '[]'` so the jq part of the
    # pipeline is exercised against an empty array without hitting GH.
    jq_start = source.command.find("--jq")
    assert jq_start != -1
    jq_filter = source.command[jq_start + len("--jq"):].strip()
    # Strip surrounding quotes the way bash would.
    if jq_filter.startswith("'") and jq_filter.endswith("'"):
        jq_filter = jq_filter[1:-1]
    result = subprocess.run(
        ["jq", "-c", jq_filter],
        input=b"[]",
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload == {
        "entity": "skein",
        "conclusion": None,
        "status": None,
        "run_started": None,
        "sha": None,
        "workflow": None,
    }


@pytest.mark.parametrize(
    "watch_name",
    ["ci-failing-on-main", "stale-pr"],
)
def test_repo_watch_yaml_loads_without_error(watch_name: str) -> None:
    """A YAML parse error in a watch file isn't caught by importing the
    handler -- only load_lodging exercises parse_watch. Pin both files
    as parseable so a future syntax mistake fails one targeted test
    rather than every integration test."""
    lodging = load_lodging(REPO_ROOT)
    matches = [name for name in lodging.triagers if name.startswith(f"{watch_name}__")]
    assert matches, f"watch {watch_name} produced no triagers"
