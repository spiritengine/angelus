"""Tests for the fixer_actions digest input.

Covers:
  (a) fixer_actions in SUPPORTED_DIGEST_INPUTS and validates in a pipe config
  (b) gatherer parses fixers.log lines within the drain window; excludes older
  (c) spawn line report pointer included; missing report file handled gracefully
  (d) empty / missing fixers.log yields empty section without error
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.lodging import Pipe, Channel, load_lodging
from angelus.lodging.config import SUPPORTED_DIGEST_INPUTS
from angelus.pipes import PipeDrain
from angelus.pipes.runner import _gather_fixer_actions, _fixer_log_path
from angelus.storage import Catalog, init_db


# ---------------------------------------------------------------------------
# (a) SUPPORTED_DIGEST_INPUTS and pipe config validation
# ---------------------------------------------------------------------------


def test_fixer_actions_in_supported_digest_inputs():
    assert "fixer_actions" in SUPPORTED_DIGEST_INPUTS


def test_fixer_actions_validates_in_pipe_config(tmp_path):
    """A daily.yaml that lists fixer_actions in body.inputs must load cleanly."""
    (tmp_path / "sources" / "scheduled").mkdir(parents=True)
    (tmp_path / "sources" / "scheduled" / "test.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n"
    )
    (tmp_path / "triagers" / "handlers").mkdir(parents=True)
    (tmp_path / "triagers" / "handlers" / "noop.py").write_text(
        "import json\nprint(json.dumps({'findings': [], 'new_state': {}}))\n"
    )
    (tmp_path / "triagers" / "noop.yaml").write_text(
        "inputs:\n  source: scheduled/test\n"
        "handler:\n  kind: python\n  path: triagers/handlers/noop.py\n"
    )
    (tmp_path / "pipes").mkdir()
    (tmp_path / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [push]\n"
        "render:\n"
        "  preamble:\n"
        "    - kind: structured\n      template: rate-limit-callout\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n"
        "    inputs:\n      - findings_since_last_drain\n      - fixer_actions\n"
    )
    (tmp_path / "channels").mkdir()
    (tmp_path / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n"
    )
    (tmp_path / "render-templates").mkdir()
    (tmp_path / "render-templates" / "rate-limit-callout.j2").write_text("")

    lodging = load_lodging(tmp_path)
    assert "daily" in lodging.pipes
    assert "fixer_actions" in lodging.pipes["daily"].render["body"]["inputs"]


def test_repo_daily_yaml_loads_with_fixer_actions():
    """The shipped pipes/daily.yaml with fixer_actions must load cleanly."""
    lodging = load_lodging(Path.cwd())
    assert "fixer_actions" in lodging.pipes["daily"].render["body"]["inputs"]


# ---------------------------------------------------------------------------
# (b) Window filtering
# ---------------------------------------------------------------------------


def _write_log(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_gather_fixer_actions_no_since_returns_all(tmp_path):
    log = tmp_path / "state" / "fixers.log"
    _write_log(log, [
        "2026-05-31T03:00:00.000Z actor=belfry action=restart reason='dead' outcome=success",
        "2026-05-31T04:00:00.000Z actor=belfry action=restart reason='dead again' outcome=success",
    ])
    actions = _gather_fixer_actions(log, None)
    assert len(actions) == 2
    assert actions[0]["action"] == "restart"
    assert actions[0]["actor"] == "belfry"
    assert actions[0]["occurred_at"] == "2026-05-31T03:00:00.000Z"


def test_gather_fixer_actions_excludes_lines_at_or_before_since(tmp_path):
    log = tmp_path / "state" / "fixers.log"
    _write_log(log, [
        "2026-05-31T01:00:00.000Z actor=belfry action=restart reason='old' outcome=success",
        # exactly at the since boundary - should be excluded (<=)
        "2026-05-31T02:00:00.000Z actor=belfry action=restart reason='boundary' outcome=success",
        "2026-05-31T03:00:00.000Z actor=belfry action=restart reason='new' outcome=success",
        "2026-05-31T04:00:00.000Z actor=sre-runner action=spawn reason='loop' outcome=completed spool_id=s1 report_path=/tmp/r.md",
    ])
    since = "2026-05-31T02:00:00.000Z"
    actions = _gather_fixer_actions(log, since)
    assert len(actions) == 2
    assert actions[0]["occurred_at"] == "2026-05-31T03:00:00.000Z"
    assert actions[1]["actor"] == "sre-runner"


def test_gather_fixer_actions_all_old_returns_empty(tmp_path):
    log = tmp_path / "state" / "fixers.log"
    _write_log(log, [
        "2026-05-30T00:00:00.000Z actor=belfry action=restart reason='yesterday' outcome=success",
    ])
    actions = _gather_fixer_actions(log, "2026-05-31T00:00:00.000Z")
    assert actions == []


def test_gather_fixer_actions_parses_reason_with_spaces(tmp_path):
    log = tmp_path / "state" / "fixers.log"
    _write_log(log, [
        "2026-05-31T10:00:00.000Z actor=belfry action=escalate reason='crash-loop: 3 restarts' outcome=blocked",
    ])
    actions = _gather_fixer_actions(log, None)
    assert len(actions) == 1
    assert actions[0]["reason"] == "crash-loop: 3 restarts"
    assert actions[0]["outcome"] == "blocked"


# ---------------------------------------------------------------------------
# (c) Spawn line report pointer and missing report handling
# ---------------------------------------------------------------------------


def test_gather_fixer_actions_spawn_includes_report_path(tmp_path):
    report = tmp_path / "state" / "sre-reports" / "2026-05-31T18_35_01Z.md"
    report.parent.mkdir(parents=True)
    report.write_text(
        "outcome: service restored\nroot-cause: OOM on startup\nconfidence: high\n",
        encoding="utf-8",
    )
    log = tmp_path / "state" / "fixers.log"
    _write_log(log, [
        f"2026-05-31T18:35:01.000Z actor=sre-runner action=spawn "
        f"reason='crash-loop: oom' outcome=completed spool_id=spool99 "
        f"report_path={report}",
    ])
    actions = _gather_fixer_actions(log, None)
    assert len(actions) == 1
    a = actions[0]
    assert a["spool_id"] == "spool99"
    assert str(report) in a["report_path"]
    assert "report_excerpt" in a
    assert a["report_excerpt"]["outcome"] == "service restored"
    assert a["report_excerpt"]["root-cause"] == "OOM on startup"


def test_gather_fixer_actions_spawn_missing_report_no_crash(tmp_path):
    log = tmp_path / "state" / "fixers.log"
    _write_log(log, [
        "2026-05-31T18:35:01.000Z actor=sre-runner action=spawn "
        "reason='crash-loop' outcome=completed spool_id=spoolX "
        "report_path=/nonexistent/path/report.md",
    ])
    actions = _gather_fixer_actions(log, None)
    assert len(actions) == 1
    assert actions[0]["action"] == "spawn"
    assert "report_excerpt" not in actions[0]


def test_gather_fixer_actions_spawn_no_report_path_field(tmp_path):
    log = tmp_path / "state" / "fixers.log"
    _write_log(log, [
        "2026-05-31T18:35:01.000Z actor=sre-runner action=spawn "
        "reason='loop' outcome=spawn-failed spool_id=",
    ])
    actions = _gather_fixer_actions(log, None)
    assert len(actions) == 1
    assert "report_excerpt" not in actions[0]


# ---------------------------------------------------------------------------
# (d) Empty / missing log
# ---------------------------------------------------------------------------


def test_gather_fixer_actions_missing_log(tmp_path):
    log = tmp_path / "state" / "fixers.log"
    assert not log.exists()
    actions = _gather_fixer_actions(log, None)
    assert actions == []


def test_gather_fixer_actions_empty_log(tmp_path):
    log = tmp_path / "state" / "fixers.log"
    log.parent.mkdir(parents=True)
    log.write_text("", encoding="utf-8")
    actions = _gather_fixer_actions(log, None)
    assert actions == []


def test_gather_fixer_actions_blank_lines_skipped(tmp_path):
    log = tmp_path / "state" / "fixers.log"
    log.parent.mkdir(parents=True)
    log.write_text("\n\n   \n", encoding="utf-8")
    actions = _gather_fixer_actions(log, None)
    assert actions == []


# ---------------------------------------------------------------------------
# Integration: fixer_actions flows into _structured_inputs and LLM payload
# ---------------------------------------------------------------------------


def _daily_pipe_with_fixer_actions() -> Pipe:
    return Pipe(
        name="daily",
        cadence="0 8 * * *",
        render_kind="digest",
        template=None,
        channels=["push"],
        render={
            "preamble": [],
            "body": {
                "kind": "llm",
                "mantle": "chronicler",
                "inputs": ["findings_since_last_drain", "fixer_actions"],
            },
        },
    )


def test_structured_inputs_contains_fixer_actions(tmp_path, monkeypatch):
    """fixer_actions appears in structured inputs; actions in the state dir are read."""
    log = tmp_path / "state" / "fixers.log"
    log.parent.mkdir(parents=True)
    log.write_text(
        "2026-05-31T03:00:00.000Z actor=belfry action=restart "
        "reason='daemon dead' outcome=success\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ANGELUS_BELFRY_FIXERS_LOG_PATH", str(log))

    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe_with_fixer_actions(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    try:
        structured = drain._structured_inputs(_daily_pipe_with_fixer_actions(), None)
    finally:
        connection.close()

    assert "fixer_actions" in structured
    assert len(structured["fixer_actions"]) == 1
    action = structured["fixer_actions"][0]
    assert action["actor"] == "belfry"
    assert action["action"] == "restart"


def test_fixer_actions_appears_in_llm_payload(tmp_path, monkeypatch):
    """End-to-end: fixer_actions data reaches the chronicler prompt."""
    log = tmp_path / "state" / "fixers.log"
    log.parent.mkdir(parents=True)
    log.write_text(
        "2026-05-31T06:00:00.000Z actor=belfry action=restart "
        "reason='daemon was absent' outcome=success\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ANGELUS_BELFRY_FIXERS_LOG_PATH", str(log))

    (tmp_path / "render-templates").mkdir(exist_ok=True)

    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe_with_fixer_actions(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "down",
            "entity": "site",
            "severity": "high",
            "target_pipes": ["daily"],
        },
        {"daily"},
    )
    captured: dict[str, str] = {}

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        return None

    async def capture_llm(self, pipe, structured):
        import json as _json
        captured["payload"] = _json.dumps(
            {k: structured[k] for k in pipe.render["body"].get("inputs", [])}
        )
        return "Belfry restarted the daemon overnight.", None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", capture_llm)

    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert "payload" in captured
    import json as _json
    payload = _json.loads(captured["payload"])
    assert "fixer_actions" in payload
    assert len(payload["fixer_actions"]) == 1
    assert payload["fixer_actions"][0]["actor"] == "belfry"
