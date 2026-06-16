"""The daily digest surfaces a behind-master deploy-staleness FYI (brief-
20260613-3spy item 1c).

When the installed pin trails master past the threshold, the digest carries a
LOW-SEVERITY, NEVER-PAGES line ("installed N commits behind master, oldest M
days"). It is digest-only by construction: provenance.deploy_staleness returns
a plain dict the runner threads into the structured inputs and renders on the
push leg; it never writes a finding, opens an incident, or flips belfry red.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

from angelus.clock import FakeClock
from angelus.lodging import Channel, Pipe
from angelus.pipes.runner import PipeDrain
from angelus.storage import Catalog, init_db

# Patch deploy_staleness on the SAME module object PipeDrain is bound to.
# test_belfry_imports_no_angelus_modules pops angelus.* from sys.modules, so a
# later `import angelus.pipes.runner` can yield a DIFFERENT module than the one
# this file's PipeDrain came from -- patching the re-import would miss. Resolve
# the module via PipeDrain.__module__ so the patch always lands where
# PipeDrain._deploy_staleness looks up the name.
_RUNNER_MOD = sys.modules[PipeDrain.__module__]


def _daily_pipe() -> Pipe:
    return Pipe(
        name="daily",
        cadence="0 7 * * *",
        render_kind="digest",
        template=None,
        channels=["push"],
        render={"preamble": [], "body": {
            "kind": "llm", "mantle": "chronicler",
            "inputs": ["findings_since_last_drain"],
        }},
    )


def _drain(tmp_path: Path, clock: FakeClock | None = None) -> tuple[PipeDrain, object]:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path, clock=clock) if clock else Catalog(
        connection, tmp_path
    )
    drain = PipeDrain(
        catalog,
        _daily_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
        clock=clock,
    )
    return drain, connection


def test_structured_inputs_carries_deploy_staleness(tmp_path, monkeypatch):
    """_structured_inputs threads the deploy_staleness dict, computed off the
    injected clock's now (so a test controls the age arithmetic)."""
    seen = {}

    def fake_staleness(now_epoch, **_):
        seen["now"] = now_epoch
        return {"commits_behind": 4, "oldest_days": 9}

    monkeypatch.setattr(_RUNNER_MOD, "deploy_staleness", fake_staleness)
    clock = FakeClock(datetime(2026, 6, 15, 12, 0, tzinfo=UTC))
    drain, connection = _drain(tmp_path, clock)
    try:
        structured = drain._structured_inputs(_daily_pipe(), None)
    finally:
        connection.close()

    assert structured["deploy_staleness"] == {"commits_behind": 4, "oldest_days": 9}
    # now threaded from the clock, not the wall clock.
    assert seen["now"] == clock.now().timestamp()


def test_structured_inputs_deploy_staleness_none_when_current(tmp_path, monkeypatch):
    """No staleness (pin current / repo unlocatable) -> the key is present and
    None, so the render path simply omits the line."""
    monkeypatch.setattr(_RUNNER_MOD, "deploy_staleness", lambda *a, **k: None)
    drain, connection = _drain(tmp_path)
    try:
        structured = drain._structured_inputs(_daily_pipe(), None)
    finally:
        connection.close()
    assert structured["deploy_staleness"] is None


def test_compact_render_includes_staleness_line(tmp_path):
    """The push (compact) leg renders the behind-master line deterministically
    when the dict is present -- the reliable receipt, independent of any lodging
    template or the LLM body."""
    drain, connection = _drain(tmp_path)
    try:
        structured = {
            "open_incidents": [],
            "findings_since_last_drain": [],
            "recent_closures": [],
            "suppressed_findings": [],
            "deploy_staleness": {"commits_behind": 4, "oldest_days": 9},
        }
        compact = drain._render_compact("Subject", structured)
    finally:
        connection.close()
    assert "Deploy: installed 4 commit(s) behind master, oldest 9 day(s)." in compact


def test_compact_render_omits_staleness_when_none(tmp_path):
    """No line when deploy_staleness is None -- the common case must not add
    noise to every digest."""
    drain, connection = _drain(tmp_path)
    try:
        structured = {
            "open_incidents": [],
            "findings_since_last_drain": [],
            "recent_closures": [],
            "suppressed_findings": [],
            "deploy_staleness": None,
        }
        compact = drain._render_compact("Subject", structured)
    finally:
        connection.close()
    assert "Deploy:" not in compact
    assert "behind master" not in compact


def test_staleness_never_opens_incident_or_finding(tmp_path, monkeypatch):
    """Building the digest inputs with staleness present must NOT write any
    finding row or open an incident -- the line can never flip belfry red."""
    monkeypatch.setattr(
        _RUNNER_MOD,
        "deploy_staleness",
        lambda *a, **k: {"commits_behind": 2, "oldest_days": 30},
    )
    drain, connection = _drain(tmp_path)
    try:
        drain._structured_inputs(_daily_pipe(), None)
        # No findings, no incidents were created as a side effect.
        findings = connection.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
        incidents = connection.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    finally:
        connection.close()
    assert findings == 0
    assert incidents == 0


def test_email_preamble_template_renders_when_present_and_empty_when_none():
    """The email long-form leg's deploy-staleness.j2 preamble renders the FYI
    when the dict is present and collapses to nothing when None -- so an absent
    staleness adds no blank block to the digest. Guards the template's jinja
    syntax (the trim_blocks footgun) too."""
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    templates = (
        Path(__file__).resolve().parent.parent
        / "examples" / "lodging" / "render-templates"
    )
    env = Environment(
        loader=FileSystemLoader(templates),
        autoescape=select_autoescape(enabled_extensions=()),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("deploy-staleness.j2")

    present = template.render(
        deploy_staleness={"commits_behind": 4, "oldest_days": 9}
    ).strip()
    assert "4 commit(s) behind master, oldest 9 day(s)" in present

    absent = template.render(deploy_staleness=None).strip()
    assert absent == ""
