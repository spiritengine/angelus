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
from angelus.storage import Catalog, init_db
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


def test_stale_pr_partial_recovery_no_finding_keeps_incident_open() -> None:
    """One PR clears (merged/closed/refreshed) while another remains
    stale. The (single, per-repo) stale_pr incident must stay open --
    emitting a clearance here would close the incident prematurely
    because the catalog close path closes ALL incidents on (source,
    entity), not by dedup_key (storage/catalog.py:_close_incident).
    State just drops the recovered PR silently; the chronicler still
    has the original stale_pr finding in `findings_since_last_drain`."""
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
    assert out["findings"] == []
    assert out["new_state"] == {"alerted_prs": [9]}


def test_stale_pr_full_recovery_emits_single_clearance() -> None:
    """When the LAST stale PR clears, emit one clearance finding (type
    must be 'clearance' so storage.catalog._close_incident actually
    closes the per-repo stale_pr incident -- fell-r1 BLOCK #1). The
    body lists which PRs cleared, so the daily digest's recent_closures
    section is informative without per-PR finding spam during partial
    recoveries."""
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
        {
            "entity": "skein",
            "severity": "low",
            "target_pipe": "daily",
            "clearance_pipe": "daily",
        },
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "clearance"
    assert findings[0]["severity"] == "info"
    assert findings[0]["target_pipes"] == ["daily"]
    assert "#7" in findings[0]["body"]["text"]
    assert "cleared" in findings[0]["body"]["text"]
    assert out["new_state"] == {"alerted_prs": []}


def test_stale_pr_full_recovery_with_new_stale_emits_only_stale_finding() -> None:
    """Old alerts cleared but a different PR is now stale: just emit
    the new stale_pr finding, no clearance. The per-repo incident
    stays open (now tracking the new stale PR). Without this guard a
    clearance would fire and close the incident the same cycle the
    new stale_pr finding reopens it -- a flapping (open, close) pair
    that confuses the catalog's incident lifecycle."""
    observation = {
        "entity": "skein",
        "prs": [
            {"number": 14, "title": "newly stale", "updatedAt": _iso_days_ago(40)},
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
    assert findings[0]["type"] == "stale_pr"
    assert "#14" in findings[0]["body"]["text"]
    assert out["new_state"] == {"alerted_prs": [14]}


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
    """A repo with no open PRs after previously having stale ones emits
    one clearance finding (the per-repo incident closes) and lists the
    PR numbers in the body so the daily digest knows what cleared."""
    out = _invoke(
        STALE_HANDLER,
        {"entity": "skein", "prs": []},
        {"alerted_prs": [7, 9]},
        {"entity": "skein", "severity": "low", "target_pipe": "daily"},
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "clearance"
    assert "#7" in findings[0]["body"]["text"]
    assert "#9" in findings[0]["body"]["text"]
    assert out["new_state"] == {"alerted_prs": []}


def test_stale_pr_null_updated_at_preserves_alerted_state() -> None:
    """Defensive: a PR with null/unparseable updatedAt that's already
    alerted stays in alerted_prs so a transient field-shape change
    doesn't flap an alert open->closed. gh always populates updatedAt
    today, but the alternative behavior (silent drop, then re-alert
    next cycle) would corrupt the per-repo incident lifecycle."""
    observation = {
        "entity": "skein",
        "prs": [
            {"number": 7, "title": "missing updated", "updatedAt": None},
        ],
    }
    out = _invoke(
        STALE_HANDLER,
        observation,
        {"alerted_prs": [7]},
        {"entity": "skein", "severity": "low", "target_pipe": "daily"},
    )
    assert out["findings"] == []
    assert out["new_state"] == {"alerted_prs": [7]}


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
    """Both pipes route to `daily` -- broken CI is "review with morning
    coffee" routing, not pants-on-fire. If a future change re-points
    target_pipe at `now` without an explicit operator decision (i.e. a
    real action lives behind the urgent path), this test forces the
    decision to surface in the diff."""
    lodging = load_lodging(REPO_ROOT)
    triager = lodging.triagers["ci-failing-on-main__skein"]
    assert triager.metadata["entity"] == "skein"
    assert triager.metadata["entity_kind"] == "repo"
    assert triager.metadata["target_pipe"] == "daily"
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
    assert findings[0]["target_pipes"] == ["daily"]


def test_every_synthesized_source_has_a_parseable_cadence() -> None:
    """A cadence string like `1d` parses through load_lodging fine (it's
    just a str) but BLOWS UP at daemon startup inside _make_trigger
    because _cadence_seconds only knows s/m/h. This is exactly how the
    initial stale-pr.yaml shipped with `cadence: 1d` -- tests passed,
    daemon would have refused to start. Pin every cadence string in the
    live lodging through _make_trigger so the next mistake fails one
    fast targeted test."""
    from angelus.daemon import _make_trigger

    lodging = load_lodging(REPO_ROOT)
    for ref, source in lodging.sources.items():
        try:
            _make_trigger(source.cadence)
        except Exception as exc:  # pragma: no cover - exercised on regression
            raise AssertionError(
                f"source {ref!r} has unschedulable cadence "
                f"{source.cadence!r}: {exc}"
            ) from exc


def _extract_jq_filter(command: str) -> str:
    """Pull the single-quoted --jq filter out of a rendered watch command."""
    jq_start = command.find("--jq")
    assert jq_start != -1, f"no --jq in command: {command!r}"
    rest = command[jq_start + len("--jq"):].strip()
    assert rest.startswith("'"), f"expected single-quoted jq filter: {rest!r}"
    end = rest.find("'", 1)
    assert end != -1, f"unterminated single-quoted jq filter: {rest!r}"
    return rest[1:end]


def test_ci_watch_command_round_trips_through_jq_on_empty_runs() -> None:
    """Confirm the brace-escaping in ci-failing-on-main.yaml produces a
    jq expression that yields a valid JSON object even when the upstream
    array is empty -- the failure mode is "we render `{entity: ...}`
    that jq parses as malformed because of leftover python braces", and
    that would silently break every repo with no CI runs.

    Uses a local jq invocation rather than a live gh call so the test
    doesn't depend on network or auth state."""
    lodging = load_lodging(REPO_ROOT)
    source = lodging.sources["scheduled/ci-failing-on-main__skein"]
    jq_filter = _extract_jq_filter(source.command)
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


def test_stale_pr_watch_command_round_trips_through_jq() -> None:
    """Parallel coverage for stale-pr.yaml. The brace-escaping mistake
    would render `{entity: "{entity}", prs: .}` with the outer braces
    missing -- and jq would parse `entity: ..., prs: .` as a syntax
    error. Feed both empty and populated arrays so the wrapping
    behavior is verified end-to-end."""
    lodging = load_lodging(REPO_ROOT)
    source = lodging.sources["scheduled/stale-pr__skein"]
    jq_filter = _extract_jq_filter(source.command)

    empty = subprocess.run(
        ["jq", "-c", jq_filter],
        input=b"[]",
        capture_output=True,
        check=True,
    )
    assert json.loads(empty.stdout) == {"entity": "skein", "prs": []}

    populated_in = json.dumps(
        [{"number": 7, "title": "demo", "updatedAt": "2026-01-01T00:00:00Z"}]
    ).encode("utf-8")
    populated = subprocess.run(
        ["jq", "-c", jq_filter],
        input=populated_in,
        capture_output=True,
        check=True,
    )
    payload = json.loads(populated.stdout)
    assert payload["entity"] == "skein"
    assert payload["prs"][0]["number"] == 7


def test_stale_pr_incident_lifecycle_through_handler_and_catalog(
    tmp_path: Path,
) -> None:
    """Pins fell-r1 BLOCK #1 by driving the handler's actual emitted
    findings through the live catalog. Reverting `\"type\": \"clearance\"`
    back to `\"type\": \"pr_recovered\"` in gh_stale_pr.py must make this
    test fail -- the round-1 docstring claimed this property but the
    earlier version of the test wrote findings to the catalog directly
    with hardcoded type='clearance', so a handler regression slipped
    past (fell-r2 NIT #1).

    Run sequence: handler emits stale_pr findings (cycle 1) -> writes
    them to catalog -> handler emits clearance (cycle 2) -> writes to
    catalog -> assert open_incidents drops to zero and
    clearance_findings_since reports the recovery."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    metadata = {
        "entity": "skein",
        "severity": "low",
        "target_pipe": "daily",
        "clearance_pipe": "daily",
    }
    try:
        # Cycle 1: two PRs cross staleness threshold. Handler emits two
        # stale_pr findings; we write each through the catalog. They
        # upsert into ONE open incident (unique source/type/entity).
        cycle1 = _invoke(
            STALE_HANDLER,
            {
                "entity": "skein",
                "prs": [
                    {
                        "number": 7,
                        "title": "stale alpha",
                        "updatedAt": _iso_days_ago(45),
                    },
                    {
                        "number": 9,
                        "title": "stale beta",
                        "updatedAt": _iso_days_ago(60),
                    },
                ],
            },
            {},
            metadata,
        )
        assert len(cycle1["findings"]) == 2
        for finding in cycle1["findings"]:
            catalog.write_finding(None, finding, known_pipes={"daily"})

        open_after_stale = [
            i for i in catalog.open_incidents() if i["entity"] == "skein"
        ]
        assert len(open_after_stale) == 1
        assert open_after_stale[0]["type"] == "stale_pr"

        # Cycle 2: both PRs recovered (no longer in listing). Handler
        # emits one clearance finding; writing it through catalog must
        # close the open incident. A regression to type='pr_recovered'
        # here would land in the else-branch of write_finding and OPEN
        # a new incident, leaving the stale_pr one open -- the test
        # would see two opens and fail.
        cycle2 = _invoke(
            STALE_HANDLER,
            {"entity": "skein", "prs": []},
            cycle1["new_state"],
            metadata,
        )
        assert len(cycle2["findings"]) == 1
        finding = cycle2["findings"][0]
        # Explicit pin: the catalog only fires _close_incident on this
        # exact type string. If the handler ever stops emitting it, the
        # assertion below catches it before the catalog write.
        assert finding["type"] == "clearance", (
            "handler must emit type='clearance' so catalog._close_incident "
            f"fires (got {finding['type']!r})"
        )
        catalog.write_finding(None, finding, known_pipes={"daily"})

        open_after_clear = [
            i for i in catalog.open_incidents() if i["entity"] == "skein"
        ]
        assert open_after_clear == [], (
            "handler's clearance finding must close the per-repo stale_pr "
            f"incident through the catalog; got still-open: {open_after_clear}"
        )

        closures = catalog.clearance_findings_since(None)
        skein_closures = [c for c in closures if c["entity"] == "skein"]
        assert len(skein_closures) == 1, (
            "clearance must appear in clearance_findings_since (feeds the "
            "chronicler's recent_closures input); a non-clearance type "
            "would silently miss this filter"
        )
    finally:
        connection.close()


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
