"""Entity + watch expansion: selectors, substitution, fan-out, conflict detection."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from angelus.lodging import load_lodging
from angelus.lodging.dynamic import (
    Entity,
    Watch,
    _substitute,
    expand,
    matches,
    parse_entity,
    parse_watch,
)
from angelus.triage import run_python_triager


REPO_ROOT = Path(__file__).resolve().parents[1]
HTTP_STATUS_HANDLER = REPO_ROOT / "examples" / "lodging" / "triagers" / "handlers" / "http_status.py"


def _entity(
    name: str,
    kind: str = "web",
    labels: tuple[str, ...] = (),
    **attrs: object,
) -> Entity:
    return Entity(name=name, kind=kind, labels=labels, attrs=dict(attrs))


# --- selector matching ------------------------------------------------------


def test_selector_kind_only_matches_by_kind() -> None:
    web = _entity("a", kind="web")
    inbox = _entity("b", kind="imap")
    assert matches({"kind": "web"}, web)
    assert not matches({"kind": "web"}, inbox)


def test_selector_labels_requires_all_present() -> None:
    e = _entity("a", labels=("important", "flask"))
    assert matches({"labels": ["important"]}, e)
    assert matches({"labels": ["important", "flask"]}, e)
    assert not matches({"labels": ["important", "missing"]}, e)


def test_selector_labels_any_matches_if_one_present() -> None:
    e = _entity("a", labels=("archive",))
    assert matches({"labels_any": ["important", "archive"]}, e)
    assert not matches({"labels_any": ["important", "active"]}, e)


def test_selector_labels_none_excludes_paused() -> None:
    paused = _entity("a", labels=("important", "paused"))
    active = _entity("b", labels=("important",))
    selector = {"labels": ["important"], "labels_none": ["paused"]}
    assert not matches(selector, paused)
    assert matches(selector, active)


def test_selector_name_exact_match() -> None:
    e = _entity("nehimpact.org")
    assert matches({"name": "nehimpact.org"}, e)
    assert not matches({"name": "iotaschool.com"}, e)


def test_selector_empty_matches_everything() -> None:
    """An empty selector falls through every check; treat as match-all so a
    `watch:` with no `selector:` block applies broadly. Documenting the
    behavior in a test so we don't accidentally invert it."""
    assert matches({}, _entity("anything", kind="web"))
    assert matches({}, _entity("anything", kind="imap", labels=("paused",)))


# --- entity + watch parsing -------------------------------------------------


def test_parse_entity_collects_attrs_minus_reserved(tmp_path: Path) -> None:
    path = tmp_path / "foo.com.yaml"
    path.write_text(
        "kind: web\nurl: https://foo.com\nlabels: [important]\nip: 1.2.3.4\n",
        encoding="utf-8",
    )
    entity = parse_entity(path)
    assert entity.name == "foo.com"
    assert entity.kind == "web"
    assert entity.labels == ("important",)
    assert entity.attrs == {"url": "https://foo.com", "ip": "1.2.3.4"}


def test_parse_watch_requires_severity_and_target_pipe(tmp_path: Path) -> None:
    handler = tmp_path / "h.py"
    handler.write_text("pass\n", encoding="utf-8")
    path = tmp_path / "w.yaml"
    path.write_text(
        "selector: {kind: web}\n"
        "check: {kind: shell, command: 'echo {entity}'}\n"
        f"handler: {{kind: python, path: {handler.name}}}\n"
        "cadence: 5m\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="severity"):
        parse_watch(tmp_path, path)


def test_parse_watch_clearance_pipe_defaults_to_target(tmp_path: Path) -> None:
    handler = tmp_path / "h.py"
    handler.write_text("pass\n", encoding="utf-8")
    path = tmp_path / "w.yaml"
    path.write_text(
        "selector: {kind: web}\n"
        "check: {kind: shell, command: 'echo {entity}'}\n"
        f"handler: {{kind: python, path: {handler.name}}}\n"
        "cadence: 5m\nseverity: high\ntarget_pipe: now\n",
        encoding="utf-8",
    )
    watch = parse_watch(tmp_path, path)
    assert watch.clearance_pipe == "now"


def test_parse_entity_rejects_reserved_keys(tmp_path: Path) -> None:
    """A stray `entity:` key in an entity YAML would silently shadow the
    computed entity name during substitution and produce a check command
    targeting the wrong host. The parser must refuse it. Same for `name`."""
    path = tmp_path / "site.com.yaml"
    path.write_text(
        "kind: web\nurl: https://site.com\nentity: hijacked.example.com\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="reserved"):
        parse_entity(path)


def test_parse_watch_rejects_non_mapping_selector(tmp_path: Path) -> None:
    """If `selector:` is a list or scalar by accident, the loader must fail
    loud instead of falling back to {} (which would match every entity)."""
    handler = tmp_path / "h.py"
    handler.write_text("pass\n", encoding="utf-8")
    path = tmp_path / "w.yaml"
    path.write_text(
        "selector: [important]\n"
        "check: {kind: shell, command: 'echo {entity}'}\n"
        f"handler: {{kind: python, path: {handler.name}}}\n"
        "cadence: 5m\nseverity: high\ntarget_pipe: now\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="selector must be a mapping"):
        parse_watch(tmp_path, path)


def test_parse_watch_rejects_non_string_clearance_pipe(tmp_path: Path) -> None:
    """A typo like `clearance_pipe: 123` (int) used to silently coerce
    to the target_pipe (sonnet fell-r3). Now it raises so an operator
    can't accidentally route clearances to the urgent pipe."""
    handler = tmp_path / "h.py"
    handler.write_text("pass\n", encoding="utf-8")
    path = tmp_path / "w.yaml"
    path.write_text(
        "selector: {kind: web}\n"
        "check: {kind: shell, command: 'echo {entity}'}\n"
        f"handler: {{kind: python, path: {handler.name}}}\n"
        "cadence: 5m\nseverity: high\ntarget_pipe: now\n"
        "clearance_pipe: 123\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="clearance_pipe must be"):
        parse_watch(tmp_path, path)


def test_parse_watch_rejects_non_mapping_metadata(tmp_path: Path) -> None:
    handler = tmp_path / "h.py"
    handler.write_text("pass\n", encoding="utf-8")
    path = tmp_path / "w.yaml"
    path.write_text(
        "selector: {kind: web}\n"
        "check: {kind: shell, command: 'echo {entity}'}\n"
        f"handler: {{kind: python, path: {handler.name}}}\n"
        "cadence: 5m\nseverity: high\ntarget_pipe: now\n"
        "metadata: not-a-dict\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="metadata must be a mapping"):
        parse_watch(tmp_path, path)


# --- expansion --------------------------------------------------------------


def _make_watch(
    tmp_path: Path,
    name: str,
    selector: dict,
    cadence: str = "5m",
    severity: str = "high",
) -> Watch:
    handler = tmp_path / f"{name}_handler.py"
    handler.write_text("pass\n", encoding="utf-8")
    return Watch(
        name=name,
        selector=selector,
        check_command="echo {entity} {url}",
        check_timeout=30.0,
        handler_path=handler,
        handler_timeout=60.0,
        cadence=cadence,
        severity=severity,
        target_pipe="now",
        clearance_pipe="daily",
    )


def test_expand_synthesizes_per_match(tmp_path: Path) -> None:
    entities = {
        "a.com": _entity("a.com", labels=("important",), url="https://a.com"),
        "b.com": _entity("b.com", labels=("archive",), url="https://b.com"),
        "c.com": _entity("c.com", labels=("important",), url="https://c.com"),
    }
    watch = _make_watch(tmp_path, "imp", {"kind": "web", "labels": ["important"]})
    sources, triagers = expand(entities, {"imp": watch})

    assert set(sources) == {
        "scheduled/imp__a.com",
        "scheduled/imp__c.com",
    }
    assert "scheduled/imp__b.com" not in sources  # b is archive, not important

    a_source = sources["scheduled/imp__a.com"]
    assert a_source.cadence == "5m"
    assert "https://a.com" in a_source.command
    assert "a.com" in a_source.command

    triager = triagers["imp__a.com"]
    assert triager.metadata["entity"] == "a.com"
    assert triager.metadata["severity"] == "high"
    assert triager.metadata["target_pipe"] == "now"
    assert triager.metadata["watch"] == "imp"


def test_expand_one_entity_two_watches_no_collision(tmp_path: Path) -> None:
    """Same entity matched by two watches → two distinct synthesized sources.
    This is the load-bearing property of the design (Patrick's "double duty")."""
    entities = {
        "nehimpact.org": _entity(
            "nehimpact.org", labels=("important", "web"), url="https://nehimpact.org"
        )
    }
    http_watch = _make_watch(tmp_path, "http", {"labels": ["important"]})
    tls_watch = _make_watch(
        tmp_path, "tls", {"labels": ["web"]}, cadence="1d", severity="low"
    )
    sources, triagers = expand(entities, {"http": http_watch, "tls": tls_watch})

    assert set(sources) == {
        "scheduled/http__nehimpact.org",
        "scheduled/tls__nehimpact.org",
    }
    assert sources["scheduled/http__nehimpact.org"].cadence == "5m"
    assert sources["scheduled/tls__nehimpact.org"].cadence == "1d"


def test_expand_missing_placeholder_raises(tmp_path: Path) -> None:
    """If the watch command references a placeholder the entity doesn't
    supply, lodging-load fails loud rather than producing a broken command."""
    entities = {"a": _entity("a")}  # no `url` attr
    watch = _make_watch(tmp_path, "w", {})  # uses {url} in command
    with pytest.raises(ValueError, match="missing placeholders"):
        expand(entities, {"w": watch})


def test_substitute_exposes_watch_metadata() -> None:
    """A check command can reference watch `metadata:` fields (e.g.
    {stale_days}) so a jq threshold can derive from the single source of
    truth the handler also reads, instead of a duplicated literal."""
    entity = _entity("skein", kind="repo", github="spiritengine/skein")
    rendered = _substitute(
        "echo {entity} {github} {stale_days}", entity, {"stale_days": 30}
    )
    assert rendered == "echo skein spiritengine/skein 30"


def test_substitute_entity_fields_win_over_metadata() -> None:
    """Precedence guard: a metadata key colliding with an entity field must
    NOT retarget the check command. The entity value always takes effect, so
    a stray `metadata: {github: ...}` can't silently point a check at the
    wrong repo."""
    entity = _entity("skein", kind="repo", github="spiritengine/skein")
    rendered = _substitute(
        "echo {github}", entity, {"github": "attacker/evil", "stale_days": 30}
    )
    assert rendered == "echo spiritengine/skein"


def _run_stale_pr_jq(command: str, prs: list[dict]) -> str:
    """Extract the jq program from a rendered stale-pr check command and run
    it against a PR array, returning the observation-collapse `state` token.
    The jq is the final `--jq '<program>'` segment; its body uses double
    quotes only, so the single-quote delimiters bound it unambiguously."""
    marker = "--jq '"
    start = command.index(marker) + len(marker)
    jq_program = command[start:].rstrip().rstrip("'")
    out = subprocess.run(
        ["jq", "-c", jq_program],
        input=json.dumps(prs),
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)["state"]


def test_stale_pr_jq_threshold_tracks_stale_days() -> None:
    """The stale-pr check jq derives its staleness cutoff from the watch's
    `stale_days` metadata, threaded through expand -> _substitute, NOT a
    hardcoded literal. A PR aged 45 days is classified stale under
    stale_days=30 (token names the PR) and fresh under stale_days=60 (token
    "clear"). This pins the single-source-of-truth fix: it FAILS if the jq
    reverts to a fixed 2592000 (30d), because then the 45d PR would read
    stale under BOTH thresholds and the stale_days=60 assertion would break.
    Goes through the real watch YAML and the real expand path so it also
    proves expand threads watch metadata into the command render."""
    watch = parse_watch(REPO_ROOT / "examples" / "lodging", REPO_ROOT / "examples" / "lodging" / "watch" / "stale-pr.yaml")
    entity = _entity(
        "skein", kind="repo", labels=("active",), github="spiritengine/skein"
    )
    updated = (datetime.now(UTC) - timedelta(days=45)).strftime("%Y-%m-%dT%H:%M:%SZ")
    prs = [{"number": 7, "title": "x", "updatedAt": updated}]

    def stale_token(stale_days: int) -> str:
        w = dataclasses.replace(watch, extra_metadata={"stale_days": stale_days})
        sources, _ = expand({"skein": entity}, {"stale-pr": w})
        command = sources["scheduled/stale-pr__skein"].command
        return _run_stale_pr_jq(command, prs)

    assert stale_token(30) == "7", "45d PR must be stale under a 30d threshold"
    assert stale_token(60) == "clear", "45d PR must be fresh under a 60d threshold"


def test_load_lodging_rejects_synth_metadata_pipe_typo(tmp_path: Path) -> None:
    """A typo in `target_pipe:` on a watch silently routes findings to a
    pipe Catalog.write_finding doesn't know about -- dispatches drop with
    no audit row. validate_cross_refs must catch this at load time so the
    daemon refuses to start instead of going dark on that watch.
    """
    (tmp_path / "entities").mkdir()
    (tmp_path / "entities" / "a.com.yaml").write_text(
        "kind: web\nurl: https://a.com\nlabels: [important]\n", encoding="utf-8"
    )
    (tmp_path / "watch").mkdir()
    handler = tmp_path / "h.py"
    handler.write_text("pass\n", encoding="utf-8")
    (tmp_path / "watch" / "imp.yaml").write_text(
        "selector: {labels: [important]}\n"
        "check: {kind: shell, command: 'echo {entity}'}\n"
        f"handler: {{kind: python, path: {handler.name}}}\n"
        "cadence: 5m\nseverity: high\ntarget_pipe: nwo\n",  # typo
        encoding="utf-8",
    )
    (tmp_path / "pipes").mkdir()
    (tmp_path / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [email]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}'\n",
        encoding="utf-8",
    )
    (tmp_path / "channels").mkdir()
    (tmp_path / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: x@example.com\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="missing pipe 'nwo'"):
        load_lodging(tmp_path)


def test_load_lodging_detects_synth_vs_handwritten_collision(tmp_path: Path) -> None:
    """If a hand-written sources/scheduled/foo.yaml exists with the same
    ref a watch would synthesize, the loader refuses to start instead of
    silently overwriting one with the other."""
    (tmp_path / "sources" / "scheduled").mkdir(parents=True)
    (tmp_path / "sources" / "scheduled" / "imp__a.com.yaml").write_text(
        "cadence: 1h\ncheck: {kind: shell, command: 'true'}\n",
        encoding="utf-8",
    )
    (tmp_path / "entities").mkdir()
    (tmp_path / "entities" / "a.com.yaml").write_text(
        "kind: web\nurl: https://a.com\nlabels: [important]\n",
        encoding="utf-8",
    )
    (tmp_path / "watch").mkdir()
    handler = tmp_path / "h.py"
    handler.write_text("pass\n", encoding="utf-8")
    (tmp_path / "watch" / "imp.yaml").write_text(
        "selector: {labels: [important]}\n"
        "check: {kind: shell, command: 'echo {entity}'}\n"
        f"handler: {{kind: python, path: {handler.name}}}\n"
        "cadence: 5m\nseverity: high\ntarget_pipe: now\n",
        encoding="utf-8",
    )
    # Minimal pipes/channels to satisfy cross-ref validation on the
    # hand-written source.
    (tmp_path / "pipes").mkdir()
    (tmp_path / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [email]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}'\n",
        encoding="utf-8",
    )
    (tmp_path / "channels").mkdir()
    (tmp_path / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: x@example.com\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="collides"):
        load_lodging(tmp_path)


# --- generic handler --------------------------------------------------------


def _invoke_http_status(observation: dict, prior_state: dict, metadata: dict) -> dict:
    """Run the http_status handler as the runner would, returning the
    parsed JSON output. Mirrors the runner's contract without going through
    the full triage pipeline."""
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
        [sys.executable, str(HTTP_STATUS_HANDLER)],
        input=payload.encode("utf-8"),
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_handler_down_finding_on_first_observation_already_down() -> None:
    out = _invoke_http_status(
        {"entity": "a.com", "url": "https://a.com", "status_code": 502},
        {},
        {"entity": "a.com", "severity": "high", "target_pipe": "now"},
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "down"
    assert findings[0]["entity"] == "a.com"
    assert findings[0]["severity"] == "high"
    assert findings[0]["target_pipes"] == ["now"]
    assert out["new_state"] == {"last_status": 502}


def test_handler_down_finding_on_200_to_500_transition() -> None:
    out = _invoke_http_status(
        {"entity": "a.com", "url": "https://a.com", "status_code": 500},
        {"last_status": 200},
        {"entity": "a.com", "severity": "medium", "target_pipe": "now"},
    )
    assert out["findings"][0]["type"] == "down"
    assert out["findings"][0]["severity"] == "medium"


def test_handler_clearance_on_500_to_200_recovery() -> None:
    out = _invoke_http_status(
        {"entity": "a.com", "url": "https://a.com", "status_code": 200},
        {"last_status": 500},
        {
            "entity": "a.com",
            "severity": "high",
            "target_pipe": "now",
            "clearance_pipe": "daily",
        },
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "clearance"
    assert findings[0]["severity"] == "info"
    assert findings[0]["target_pipes"] == ["daily"]


def test_handler_emits_down_on_connection_failure_status_zero() -> None:
    """curl emits status_code=000 on DNS/TCP/TLS failure when guarded by
    `|| true`. The handler must treat that as down (not silence). Catches
    a regression where a site dropping off the internet entirely produces
    no alert -- opus fell-r1 #6."""
    out = _invoke_http_status(
        {"entity": "a.com", "url": "https://a.com", "status_code": 0},
        {"last_status": 200},
        {"entity": "a.com", "severity": "high", "target_pipe": "now"},
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "down"
    assert "did not respond" in findings[0]["body"]["text"]


def test_handler_emits_down_on_check_failed_observation() -> None:
    """When the source-runner itself short-circuits (curl missing,
    timeout SIGKILL, JSON parse failure) the daemon writes a
    check_failed observation with no status_code. The handler must
    still emit a down finding -- otherwise any non-curl-emitted failure
    silently produces zero alerts (opus fell-r3 #1)."""
    out = _invoke_http_status(
        {
            "type": "check_failed",
            "error": "shell check timed out after 30s",
            "timeout_seconds": 30,
        },
        {"last_status": 200},
        {"entity": "a.com", "severity": "high", "target_pipe": "now"},
    )
    findings = out["findings"]
    assert len(findings) == 1
    assert findings[0]["type"] == "down"
    assert "Check error" in findings[0]["body"]["text"]
    assert "timed out" in findings[0]["body"]["text"]
    # State stores last_status=0 so a subsequent check_failed observation
    # is a no-op (already-down), not a repeated alert.
    assert out["new_state"] == {"last_status": 0}


def test_handler_does_not_repeat_alert_when_check_failed_persists() -> None:
    out = _invoke_http_status(
        {"type": "check_failed", "error": "boom"},
        {"last_status": 0},
        {"entity": "a.com", "severity": "high", "target_pipe": "now"},
    )
    assert out["findings"] == []
    assert out["new_state"] == {"last_status": 0}


def test_watch_curl_output_on_connection_failure_is_valid_json() -> None:
    """End-to-end check that the live watch command, when run against a
    guaranteed-unreachable target, produces valid JSON the source-runner
    can parse, with a status_code the handler interprets as down. Pins
    the round-2 critical bug: curl's `%{http_code}` is zero-padded ("000")
    and must be quoted in the -w format so the JSON is well-formed --
    bare `000` is invalid JSON under RFC 8259 (no leading zeros). The
    chain breaks at `run_shell_source` (JSONDecodeError) without this fix,
    so a site dropping off the internet would produce no down finding.
    Opus + sonnet fell-r2 #1.

    Uses `http://127.0.0.1:1` rather than a `.invalid` TLD so the test
    doesn't depend on resolver behavior (some hostile-DNS networks return
    NXDOMAIN-redirect synth pages for reserved TLDs -- opus fell-r3 #3).
    Port 1 is privileged and almost never bound; TCP returns RST
    immediately. Curl exits non-zero, `|| true` masks, `-w` is written.
    """
    lodging = load_lodging(REPO_ROOT / "examples" / "lodging")
    source = lodging.sources["scheduled/web-important__example-site"]
    command = source.command.replace(
        "https://example.com", "http://127.0.0.1:1"
    )
    result = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
        timeout=35,
    )
    assert result.returncode == 0, (
        f"`|| true` should mask curl's non-zero exit; got rc={result.returncode}, "
        f"stderr={result.stderr!r}"
    )
    payload = json.loads(result.stdout)  # would raise if 000 were unquoted
    assert payload["entity"] == "example-site"
    # status_code arrives as the string "000"; handler coerces to int 0.
    out = _invoke_http_status(
        payload,
        {"last_status": 200},
        {
            "entity": payload["entity"],
            "severity": "high",
            "target_pipe": "now",
        },
    )
    findings = out["findings"]
    assert len(findings) == 1, f"expected down finding, got {findings}"
    assert findings[0]["type"] == "down"
    assert "did not respond" in findings[0]["body"]["text"]


def test_handler_no_finding_on_unchanged_state() -> None:
    out = _invoke_http_status(
        {"entity": "a.com", "url": "https://a.com", "status_code": 200},
        {"last_status": 200},
        {"entity": "a.com", "severity": "high", "target_pipe": "now"},
    )
    assert out["findings"] == []
    assert out["new_state"] == {"last_status": 200}

    out = _invoke_http_status(
        {"entity": "a.com", "url": "https://a.com", "status_code": 502},
        {"last_status": 502},
        {"entity": "a.com", "severity": "high", "target_pipe": "now"},
    )
    assert out["findings"] == []


def test_handler_through_runner_with_real_triager(tmp_path: Path) -> None:
    """End-to-end: load a real lodging that exercises the expand path, then
    run the synthesized triager via the runner. Catches contract mismatches
    between expand's metadata shape and the handler's expectations."""
    lodging = load_lodging(REPO_ROOT / "examples" / "lodging")
    triager = lodging.triagers["web-important__example-site"]
    findings, state = asyncio.run(
        run_python_triager(
            triager,
            {
                "entity": "example-site",
                "url": "https://example.com",
                "status_code": 502,
            },
            {"last_status": 200},
        )
    )
    assert state == {"last_status": 502}
    assert len(findings) == 1
    assert findings[0]["entity"] == "example-site"
    assert findings[0]["severity"] == "high"
    assert findings[0]["type"] == "down"
