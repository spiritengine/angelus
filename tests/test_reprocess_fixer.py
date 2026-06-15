"""Auto-reprocess fixer (brief-20260612-q7de).

The exhaustion-recovery wiring for the observation-collapse retry-redundancy
loss (brief-20260612-29he): a triager that exhausts its retry ladder on a
collapsed transition observation loses the product finding for that transition
until the next state change. Exhaustion opens an internal/triage incident (the
loud path), and this fixer binds to that incident to issue the documented heal
-- catalog.reprocess_source, which returns the exhausted observation to 'ready'
(brief-20260607-6qsq Stage 1) -- scoped to the failing triager's source.

These tests pin the daemon-side wiring the registry adds for this fixer:

  * the open internal/triage incident matches and fires the handler ONCE,
    carrying the failing triager's source_ref so the handler can scope the
    reprocess;
  * the source_ref resolved is the FAILING triager's, not an unrelated lodged
    triager's source;
  * the guardrail caps attempts so a deterministically-broken triager (whose
    reprocess just re-exhausts and re-opens the same incident) converges to the
    loud steady state instead of storming reprocess;
  * the fixer does not fire when no internal/triage incident is open.

The reprocess HANDLER itself (examples/lodging/fixers/handlers/reprocess.py) is
exercised directly at the bottom: given a context with a source_ref it shells
`angelus reprocess <source_ref>` and reports the outcome.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon, _fixers_log_path

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = REPO_ROOT / "examples" / "lodging"
PINNED = datetime(2026, 6, 3, 12, 0, 0, tzinfo=UTC)

TRIAGER = "site"
SOURCE = "scheduled/site"
DECOY_TRIAGER = "other"
DECOY_SOURCE = "scheduled/other"

# Records the source_ref the daemon resolved into the fixers.log note so a test
# can assert what the handler was asked to reprocess -- without a control
# socket. Stands in for the shipped reprocess.py (tested separately below).
_RECORD_HANDLER = (
    "import json, sys\n"
    "cond = json.loads(sys.stdin.read() or '{}').get('condition') or {}\n"
    "print(json.dumps({'outcome': 'reprocessed', "
    "'note': 'source=' + str(cond.get('source_ref'))}))\n"
)


def _write_triager(root: Path, name: str, source_ref: str) -> None:
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True, exist_ok=True)
    (scheduled / f"{source_ref.split('/', 1)[1]}.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    handlers = root / "triagers" / "handlers"
    handlers.mkdir(parents=True, exist_ok=True)
    (handlers / f"{name}.py").write_text(
        "import json,sys\njson.dump({'findings':[],'new_state':{}}, sys.stdout)\n",
        encoding="utf-8",
    )
    (root / "triagers" / f"{name}.yaml").write_text(
        f"inputs:\n  source: {source_ref}\n"
        f"handler:\n  kind: python\n  path: triagers/handlers/{name}.py\n",
        encoding="utf-8",
    )


def _write_base_lodging(root: Path) -> None:
    _write_triager(root, TRIAGER, SOURCE)
    _write_triager(root, DECOY_TRIAGER, DECOY_SOURCE)
    (root / "pipes").mkdir(exist_ok=True)
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir(exist_ok=True)
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )


def _write_fixer(
    root: Path,
    *,
    handler_body: str = _RECORD_HANDLER,
    max_attempts: int = 3,
    window_seconds: int = 86400,
    backoff_seconds: int = 0,
) -> None:
    fixers = root / "fixers"
    handlers = fixers / "handlers"
    handlers.mkdir(parents=True, exist_ok=True)
    (handlers / "reprocess.py").write_text(handler_body, encoding="utf-8")
    (fixers / "reprocess-triage.yaml").write_text(
        "condition:\n"
        "  kind: open_internal_incident\n"
        "  source: internal/triage\n"
        "  incident_type: triage_failed\n"
        "handler:\n  kind: python\n  path: fixers/handlers/reprocess.py\n"
        "  timeout_seconds: 30\n"
        f"guardrails:\n  max_attempts: {max_attempts}\n"
        f"  window_seconds: {window_seconds}\n"
        f"  backoff_seconds: {backoff_seconds}\n",
        encoding="utf-8",
    )


def _make_daemon(root: Path, clock: FakeClock) -> AngelusDaemon:
    daemon = AngelusDaemon(root)
    daemon.clock = clock
    daemon.catalog._clock = clock
    return daemon


def _open_triage_incident(daemon: AngelusDaemon, entity: str = TRIAGER) -> None:
    daemon.catalog.write_internal_finding(
        "internal/triage",
        "triage_failed",
        entity,
        "simulated transient triager failure",
        set(daemon.lodging.pipes),
    )


def _clear_triage_incident(daemon: AngelusDaemon, entity: str = TRIAGER) -> None:
    daemon.catalog.write_internal_clearance(
        "internal/triage", entity, f"{entity} recovered", set(daemon.lodging.pipes)
    )


def _read_fixers_log(root: Path) -> list[str]:
    path = _fixers_log_path(root)
    if not path.exists():
        return []
    return [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


# --- daemon-side wiring ------------------------------------------------------


def test_fires_once_on_open_triage_incident_with_source_ref(tmp_path) -> None:
    """An open internal/triage incident fires the fixer once, and the daemon
    resolves the incident's triager-name entity to that triager's source_ref so
    the handler knows what to reprocess."""
    _write_base_lodging(tmp_path)
    _write_fixer(tmp_path)
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        _open_triage_incident(daemon)
        asyncio.run(daemon._evaluate_fixers())

        lines = _read_fixers_log(tmp_path)
        assert len(lines) == 1, "exactly one reprocess attempt"
        assert "actor=reprocess-triage" in lines[0]
        assert "outcome=reprocessed" in lines[0]
        # The resolved source is the FAILING triager's, scoped, not the decoy.
        assert f"source={SOURCE}" in lines[0]
        assert DECOY_SOURCE not in lines[0]
    finally:
        daemon.connection.close()


def test_scopes_to_failing_triager_source(tmp_path) -> None:
    """An incident for the decoy triager resolves to the decoy's source, never
    the other triager's -- the reprocess is strictly scoped to the source whose
    triager actually exhausted."""
    _write_base_lodging(tmp_path)
    _write_fixer(tmp_path)
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        _open_triage_incident(daemon, entity=DECOY_TRIAGER)
        asyncio.run(daemon._evaluate_fixers())

        lines = _read_fixers_log(tmp_path)
        assert len(lines) == 1
        assert f"source={DECOY_SOURCE}" in lines[0]
        assert f"source={SOURCE}" not in lines[0]
    finally:
        daemon.connection.close()


def test_non_triage_incident_with_triager_name_entity_gets_no_source_ref(
    tmp_path,
) -> None:
    """The source_ref enrichment is gated on source == internal/triage. A
    DIFFERENT open_internal_incident source (internal/dep) whose entity happens
    to coincide with a lodged triager name must NOT receive source_ref -- else a
    future fixer bound to that source could reprocess an unrelated source."""
    from angelus.lodging.config import FixerCondition

    _write_base_lodging(tmp_path)
    _write_fixer(tmp_path)
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        # entity == TRIAGER ("site"), which IS a lodged triager name -- but the
        # incident is internal/dep, not internal/triage.
        daemon.catalog.write_internal_finding(
            "internal/dep",
            "dependency_unhealthy",
            TRIAGER,
            "dep is down",
            set(daemon.lodging.pipes),
        )
        matches = daemon._match_fixer_condition(
            FixerCondition(kind="open_internal_incident", source="internal/dep")
        )
        assert len(matches) == 1, "the internal/dep incident matches its condition"
        _key, context = matches[0]
        assert "source_ref" not in context, (
            "a non-triage incident must not get source_ref even when its entity "
            "coincides with a lodged triager name"
        )
    finally:
        daemon.connection.close()


def test_does_not_fire_without_triage_incident(tmp_path) -> None:
    """No internal/triage incident -> no reprocess. An unrelated open incident
    (internal/dep) does not match the source=internal/triage condition."""
    _write_base_lodging(tmp_path)
    _write_fixer(tmp_path)
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        daemon.catalog.write_internal_finding(
            "internal/dep",
            "dependency_unhealthy",
            "iotaschool.com",
            "dep is down",
            set(daemon.lodging.pipes),
        )
        asyncio.run(daemon._evaluate_fixers())
        assert _read_fixers_log(tmp_path) == []
    finally:
        daemon.connection.close()


def test_guardrail_caps_attempts_across_incident_reopen(tmp_path) -> None:
    """The convergence guarantee: a deterministically-broken triager re-exhausts
    after every reprocess and re-opens the SAME incident, but the open-incident
    identity (source/type/entity) keeps the condition_key stable across the
    close/reopen, so attempts accumulate against one budget. Once max_attempts
    is spent the fixer goes quiet with the incident still open (the loud steady
    state) -- it does not storm reprocess forever."""
    _write_base_lodging(tmp_path)
    _write_fixer(tmp_path, max_attempts=3, window_seconds=86400)
    clock = FakeClock(PINNED)
    daemon = _make_daemon(tmp_path, clock)
    try:
        # Simulate the broken-triager loop: each round the incident is open,
        # the fixer reprocesses, the triager re-exhausts and re-opens. Five
        # rounds, but only three attempts are permitted in the window.
        for _ in range(5):
            _open_triage_incident(daemon)  # no-op while already open (gate)
            asyncio.run(daemon._evaluate_fixers())
            clock.advance(timedelta(minutes=1))
            _clear_triage_incident(daemon)
            _open_triage_incident(daemon)  # re-exhaust: same identity reopens

        assert len(_read_fixers_log(tmp_path)) == 3, (
            "the cap holds across reopen -- converges to loud, not a storm"
        )

        # Past the window the budget refreshes (a slow periodic retry, not a
        # storm) -- proves the cap is the window budget, not a permanent stop.
        clock.advance(timedelta(days=2))
        asyncio.run(daemon._evaluate_fixers())
        assert len(_read_fixers_log(tmp_path)) == 4
    finally:
        daemon.connection.close()


# --- the shipped handler -----------------------------------------------------


def test_reprocess_handler_shells_angelus_scoped_to_source(tmp_path) -> None:
    """The shipped reprocess.py, given a context with a source_ref, shells
    `angelus reprocess <source_ref>` -- scoped to exactly that source. A stub
    `angelus` on PATH records the argv so the call is observable without a
    daemon/control socket."""
    handler = EXAMPLES / "fixers" / "handlers" / "reprocess.py"
    recorder = tmp_path / "argv.txt"
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    stub = stub_dir / "angelus"
    stub.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        f"open({str(recorder)!r}, 'w').write(' '.join(sys.argv[1:]))\n"
        "print('reprocess: 1 observations from %s will be re-triaged' % sys.argv[-1])\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)

    payload = {
        "fixer": {"name": "reprocess-triage"},
        "condition": {
            "kind": "open_internal_incident",
            "condition_key": "open_internal_incident:internal/triage:triage_failed:site",
            "incident": {"source": "internal/triage", "entity": TRIAGER},
            "source_ref": SOURCE,
        },
    }
    env = {"PATH": f"{stub_dir}:/usr/bin:/bin"}
    proc = subprocess.run(
        [sys.executable, str(handler)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr
    assert recorder.read_text() == f"reprocess {SOURCE}"
    out = json.loads(proc.stdout)
    assert out["outcome"] == "reprocessed"


def test_shipped_reprocess_template_parses_when_enabled() -> None:
    """The repo ships fixers/reprocess-triage-failure.yaml.disabled as a
    template; copying it enabled (against the examples lodging, where its
    handler lives) must parse, so the documented shape never silently rots."""
    from angelus.lodging import parse_fixer

    src = EXAMPLES / "fixers" / "reprocess-triage-failure.yaml.disabled"
    assert src.exists(), "shipped template missing"
    enabled = EXAMPLES / "fixers" / "reprocess-triage-failure.yaml"
    enabled.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    try:
        fixer = parse_fixer(EXAMPLES, enabled)
    finally:
        enabled.unlink()
    assert fixer.condition.kind == "open_internal_incident"
    assert fixer.condition.source == "internal/triage"
    assert fixer.condition.incident_type == "triage_failed"
    assert fixer.handler_path == EXAMPLES / "fixers" / "handlers" / "reprocess.py"
    assert fixer.max_attempts == 3
    assert fixer.window_seconds == 86400
    assert fixer.backoff_seconds == 600


def test_reprocess_handler_skips_when_no_source_ref(tmp_path) -> None:
    """No resolved source_ref (triager hot-removed, or bound to a non-triage
    incident) -> a quiet 'skipped' no-op, not an error and not a reprocess."""
    handler = EXAMPLES / "fixers" / "handlers" / "reprocess.py"
    payload = {"condition": {"kind": "open_internal_incident", "incident": {}}}
    proc = subprocess.run(
        [sys.executable, str(handler)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["outcome"] == "skipped"
