"""B11: in-daemon fixer (autoremediation) registry.

A fixer is a lodging entry under fixers/ that binds a CONDITION (matched against
live catalog state each pass) to a python HANDLER (run as a subprocess, like a
triager) under GUARDRAILS (max_attempts within a window, plus backoff). The
in-daemon fixer loop evaluates conditions, gates on the guardrails, runs the
handler, records the attempt, and appends one line to the shared fixers.log
audit trail (the same file belfry's B12 restart-fixer writes, so a fixer's
actions flow into the daily digest's fixer_actions for free).

The contract this item must hold:

  * fixers/ is a flat, .disabled-honoring, hot-reloadable lodging dir; dropping
    a fixer file wires it with no code change.
  * the parser fails loud on a bad condition kind, a condition missing its
    required matcher, a guardrail block that does not cap the blast radius, and
    a handler that is missing or not python.
  * a fixer fires on its condition: it runs the handler, records the attempt,
    and writes a fixers.log line a digest reader can parse.
  * the guardrails actually throttle: max_attempts caps firing within the
    window, and backoff spaces attempts -- a handler that errors still counts,
    so a persistently-failing fixer backs off rather than hammering.
  * giving up is quiet, not an escalation (the underlying condition stays loud
    via belfry/health; escalation-on-exhaustion is B14, not this layer).
"""

from __future__ import annotations

import asyncio
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from angelus.daemon import AngelusDaemon, _fixers_log_path
from angelus.clock import FakeClock
from angelus.fixers.runner import run_python_fixer
from angelus.lodging import Fixer, FixerCondition, parse_fixer, validate_cross_refs
from angelus.lodging.config import _load_fixers, load_lodging
from angelus.lodging.reloader import LodgingReloader, _identify
from angelus.pipes.runner import _FIXER_KV_RE

REPO_ROOT = Path(__file__).resolve().parents[1]
PINNED = datetime(2026, 6, 3, 12, 0, 0, tzinfo=UTC)


# --- lodging scaffolding -----------------------------------------------------


_OBSERVE_HANDLER = (
    "import json, sys\n"
    "payload = json.loads(sys.stdin.read() or '{}')\n"
    "key = (payload.get('condition') or {}).get('condition_key', '?')\n"
    "print(json.dumps({'outcome': 'observed', 'note': 'ack ' + key}))\n"
)

# Exits non-zero: the daemon must record this as outcome="error" and count it
# against the guardrail.
_FAILING_HANDLER = "import sys\nsys.exit(3)\n"


def _write_base_lodging(root: Path) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )


def _write_handler(root: Path, name: str, body: str) -> Path:
    handlers = root / "fixers" / "handlers"
    handlers.mkdir(parents=True, exist_ok=True)
    path = handlers / name
    path.write_text(body, encoding="utf-8")
    return path


def _write_fixer(
    root: Path,
    name: str,
    *,
    handler: str = "fixers/handlers/observe.py",
    condition: str = "kind: open_internal_incident\n  source: internal/dep\n",
    max_attempts: int = 3,
    window_seconds: int = 3600,
    backoff_seconds: int = 0,
    disabled: bool = False,
) -> Path:
    (root / "fixers").mkdir(parents=True, exist_ok=True)
    fname = f"{name}.yaml.disabled" if disabled else f"{name}.yaml"
    path = root / "fixers" / fname
    path.write_text(
        f"condition:\n  {condition}"
        f"handler:\n  kind: python\n  path: {handler}\n  timeout_seconds: 30\n"
        f"guardrails:\n  max_attempts: {max_attempts}\n"
        f"  window_seconds: {window_seconds}\n"
        f"  backoff_seconds: {backoff_seconds}\n",
        encoding="utf-8",
    )
    return path


def _make_daemon(root: Path, clock: FakeClock | None = None) -> AngelusDaemon:
    daemon = AngelusDaemon(root)
    if clock is not None:
        daemon.clock = clock
        daemon.catalog._clock = clock
    return daemon


def _open_dep_incident(daemon: AngelusDaemon, entity: str = "iotaschool.com") -> None:
    daemon.catalog.write_internal_finding(
        "internal/dep",
        "dependency_unhealthy",
        entity,
        "dependency is down",
        set(daemon.lodging.pipes),
    )


def _read_fixers_log(root: Path) -> list[str]:
    path = _fixers_log_path(root)
    if not path.exists():
        return []
    return [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


# --- parser ------------------------------------------------------------------


def test_parse_valid_open_internal_incident_fixer(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    path = _write_fixer(
        tmp_path,
        "f",
        condition=(
            "kind: open_internal_incident\n  source: internal/dep\n"
            "  incident_type: dependency_unhealthy\n  entity: iotaschool.com\n"
        ),
        backoff_seconds=300,
    )
    fixer = parse_fixer(tmp_path, path)
    assert isinstance(fixer, Fixer)
    assert fixer.name == "f"
    assert fixer.condition.kind == "open_internal_incident"
    assert fixer.condition.source == "internal/dep"
    assert fixer.condition.incident_type == "dependency_unhealthy"
    assert fixer.condition.entity == "iotaschool.com"
    assert fixer.condition.channel is None
    assert fixer.max_attempts == 3
    assert fixer.window_seconds == 3600
    assert fixer.backoff_seconds == 300
    assert fixer.handler_path == tmp_path / "fixers" / "handlers" / "observe.py"


def test_parse_valid_channel_unhealthy_fixer(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    path = _write_fixer(
        tmp_path, "f", condition="kind: channel_unhealthy\n  channel: push\n"
    )
    fixer = parse_fixer(tmp_path, path)
    assert fixer.condition.kind == "channel_unhealthy"
    assert fixer.condition.channel == "push"
    assert fixer.condition.source is None


def test_parse_rejects_unknown_condition_kind(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    path = _write_fixer(tmp_path, "f", condition="kind: daemon_dead\n")
    with pytest.raises(ValueError, match="unsupported condition.kind"):
        parse_fixer(tmp_path, path)


def test_parse_rejects_incident_kind_without_source(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    path = _write_fixer(tmp_path, "f", condition="kind: open_internal_incident\n")
    with pytest.raises(ValueError, match="requires.*source"):
        parse_fixer(tmp_path, path)


def test_parse_rejects_channel_field_on_incident_kind(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    path = _write_fixer(
        tmp_path,
        "f",
        condition="kind: open_internal_incident\n  source: internal/dep\n  channel: push\n",
    )
    with pytest.raises(ValueError, match="condition.channel is only valid"):
        parse_fixer(tmp_path, path)


def test_parse_rejects_incident_field_on_channel_kind(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    path = _write_fixer(
        tmp_path,
        "f",
        condition="kind: channel_unhealthy\n  source: internal/dep\n",
    )
    with pytest.raises(ValueError, match="only valid for"):
        parse_fixer(tmp_path, path)


def test_parse_rejects_nonpositive_max_attempts(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    path = _write_fixer(tmp_path, "f", max_attempts=0)
    with pytest.raises(ValueError, match="positive integer max_attempts"):
        parse_fixer(tmp_path, path)


def test_parse_rejects_negative_backoff(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    path = _write_fixer(tmp_path, "f", backoff_seconds=-1)
    with pytest.raises(ValueError, match="non-negative integer backoff_seconds"):
        parse_fixer(tmp_path, path)


def test_parse_rejects_missing_guardrails(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    (tmp_path / "fixers").mkdir(parents=True, exist_ok=True)
    path = tmp_path / "fixers" / "f.yaml"
    path.write_text(
        "condition:\n  kind: open_internal_incident\n  source: internal/dep\n"
        "handler:\n  kind: python\n  path: fixers/handlers/observe.py\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="expected guardrails mapping"):
        parse_fixer(tmp_path, path)


def test_parse_rejects_missing_handler_file(tmp_path) -> None:
    path = _write_fixer(tmp_path, "f", handler="fixers/handlers/nope.py")
    with pytest.raises(ValueError, match="handler path does not exist"):
        parse_fixer(tmp_path, path)


def test_parse_rejects_nonpython_handler(tmp_path) -> None:
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    (tmp_path / "fixers").mkdir(parents=True, exist_ok=True)
    path = tmp_path / "fixers" / "f.yaml"
    path.write_text(
        "condition:\n  kind: open_internal_incident\n  source: internal/dep\n"
        "handler:\n  kind: shell\n  path: fixers/handlers/observe.py\n"
        "guardrails:\n  max_attempts: 3\n  window_seconds: 3600\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="only handler.kind=python"):
        parse_fixer(tmp_path, path)


# --- discovery / loading -----------------------------------------------------


def test_load_lodging_discovers_fixer_and_skips_disabled(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(tmp_path, "live", backoff_seconds=0)
    _write_fixer(tmp_path, "inert", disabled=True)
    lodging = load_lodging(tmp_path)
    assert set(lodging.fixers) == {"live"}


def test_load_fixers_ignores_handler_py_files(tmp_path) -> None:
    # The handlers/ subdir holds *.py, which the *.yaml glob must not pick up.
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(tmp_path, "live")
    assert set(_load_fixers(tmp_path)) == {"live"}


def test_absent_fixers_dir_is_empty(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    lodging = load_lodging(tmp_path)
    assert lodging.fixers == {}


def test_shipped_template_parses_when_enabled(tmp_path) -> None:
    # The repo ships fixers/observe-internal-incident.yaml.disabled as a
    # template; copying it enabled (against the repo root, where its handler
    # lives) must parse, so the documented shape never silently rots.
    src = REPO_ROOT / "examples" / "lodging" / "fixers" / "observe-internal-incident.yaml.disabled"
    assert src.exists(), "shipped template missing"
    enabled = REPO_ROOT / "examples" / "lodging" / "fixers" / "observe-internal-incident.yaml"
    shutil.copyfile(src, enabled)
    try:
        fixer = parse_fixer(REPO_ROOT / "examples" / "lodging", enabled)
    finally:
        enabled.unlink()
    assert fixer.condition.kind == "open_internal_incident"
    assert fixer.handler_path == REPO_ROOT / "examples" / "lodging" / "fixers" / "handlers" / "observe.py"


# --- cross-ref ---------------------------------------------------------------


def test_cross_ref_rejects_channel_unhealthy_missing_channel(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(
        tmp_path, "f", condition="kind: channel_unhealthy\n  channel: nope\n"
    )
    with pytest.raises(ValueError, match="references missing channel 'nope'"):
        load_lodging(tmp_path)


def test_cross_ref_ok_for_nameless_channel_unhealthy(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(tmp_path, "f", condition="kind: channel_unhealthy\n")
    lodging = load_lodging(tmp_path)  # any-channel binding: no cross-ref
    assert validate_cross_refs(lodging) == []


# --- firing / dispatch (acceptance) -----------------------------------------


def test_fixer_fires_on_open_internal_incident(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(tmp_path, "dep-observe")
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        _open_dep_incident(daemon)
        asyncio.run(daemon._evaluate_fixers())

        key = (
            "open_internal_incident:internal/dep:dependency_unhealthy:"
            "iotaschool.com"
        )
        assert (
            daemon.catalog.fixer_attempt_count_in_window("dep-observe", key, 3600)
            == 1
        )
        lines = _read_fixers_log(tmp_path)
        assert len(lines) == 1
        assert "actor=dep-observe" in lines[0]
        assert "outcome=observed" in lines[0]
        assert key in lines[0]
    finally:
        daemon.connection.close()


def test_fixer_does_not_fire_without_matching_condition(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(tmp_path, "dep-observe")
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        # No incident open, and an unrelated source open should not match.
        daemon.catalog.write_internal_finding(
            "internal/triage", "triage_failed", "x", "boom",
            set(daemon.lodging.pipes),
        )
        asyncio.run(daemon._evaluate_fixers())
        assert _read_fixers_log(tmp_path) == []
    finally:
        daemon.connection.close()


def test_channel_unhealthy_fixer_fires(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(tmp_path, "ch", condition="kind: channel_unhealthy\n  channel: push\n")
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        daemon.catalog.mark_channel_unhealthy("push", "connection refused")
        asyncio.run(daemon._evaluate_fixers())
        lines = _read_fixers_log(tmp_path)
        assert len(lines) == 1
        assert "actor=ch" in lines[0]
        assert "channel_unhealthy:push" in lines[0]
    finally:
        daemon.connection.close()


def test_fixers_log_line_is_digest_parseable(tmp_path) -> None:
    # The fixers.log line must parse with the same key=value grammar the daily
    # digest's _gather_fixer_actions uses, so an in-daemon fixer surfaces there.
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(tmp_path, "dep-observe")
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        _open_dep_incident(daemon)
        asyncio.run(daemon._evaluate_fixers())
        line = _read_fixers_log(tmp_path)[0]
        _ts, rest = line.split(" ", 1)
        kv = {m.group(1): next(g for g in m.groups()[1:] if g is not None)
              for m in _FIXER_KV_RE.finditer(rest)}
        assert kv["actor"] == "dep-observe"
        assert kv["action"] == "fix"
        assert kv["outcome"] == "observed"
        assert "internal/dep" in kv["reason"]
    finally:
        daemon.connection.close()


# --- guardrails --------------------------------------------------------------


def test_guardrail_caps_attempts_in_window(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(tmp_path, "dep-observe", max_attempts=2, window_seconds=3600)
    clock = FakeClock(PINNED)
    daemon = _make_daemon(tmp_path, clock)
    try:
        _open_dep_incident(daemon)
        # Three passes, but max_attempts=2 within the hour -> only two fire.
        for _ in range(3):
            asyncio.run(daemon._evaluate_fixers())
            clock.advance(timedelta(minutes=1))
        assert len(_read_fixers_log(tmp_path)) == 2

        # Past the window, the budget refreshes and it fires again.
        clock.advance(timedelta(hours=2))
        asyncio.run(daemon._evaluate_fixers())
        assert len(_read_fixers_log(tmp_path)) == 3
    finally:
        daemon.connection.close()


def test_guardrail_backoff_spaces_attempts(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(
        tmp_path, "dep-observe", max_attempts=10, backoff_seconds=600
    )
    clock = FakeClock(PINNED)
    daemon = _make_daemon(tmp_path, clock)
    try:
        _open_dep_incident(daemon)
        asyncio.run(daemon._evaluate_fixers())  # fires
        clock.advance(timedelta(minutes=5))  # < 600s backoff
        asyncio.run(daemon._evaluate_fixers())  # suppressed
        assert len(_read_fixers_log(tmp_path)) == 1
        clock.advance(timedelta(minutes=6))  # now > 600s since first
        asyncio.run(daemon._evaluate_fixers())  # fires again
        assert len(_read_fixers_log(tmp_path)) == 2
    finally:
        daemon.connection.close()


def test_attempts_persist_across_daemon_restart(tmp_path) -> None:
    # The guardrail must survive a restart: a crash-looping fixer cannot earn a
    # fresh budget every time the daemon comes back. The count reads from the
    # file-backed fixer_attempts table, so a fresh daemon on the same DB sees
    # the prior attempts.
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    _write_fixer(tmp_path, "dep-observe", max_attempts=1)
    key = (
        "open_internal_incident:internal/dep:dependency_unhealthy:iotaschool.com"
    )

    daemon1 = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        _open_dep_incident(daemon1)
        asyncio.run(daemon1._evaluate_fixers())  # fires once, hits the cap
        assert (
            daemon1.catalog.fixer_attempt_count_in_window("dep-observe", key, 3600)
            == 1
        )
    finally:
        daemon1.connection.close()

    # Restart: a new daemon on the same root/DB (the open incident and the
    # attempt ledger both persist). Same pinned instant -> still in window.
    daemon2 = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        assert (
            daemon2.catalog.fixer_attempt_count_in_window("dep-observe", key, 3600)
            == 1
        )
        asyncio.run(daemon2._evaluate_fixers())  # guard blocks: no new fire
        assert len(_read_fixers_log(tmp_path)) == 1
    finally:
        daemon2.connection.close()


def _bare_fixer(handler_path: Path, timeout: float = 30.0) -> Fixer:
    return Fixer(
        name="t",
        condition=FixerCondition(kind="open_internal_incident", source="internal/dep"),
        handler_path=handler_path,
        handler_timeout=timeout,
        max_attempts=3,
        window_seconds=3600,
        backoff_seconds=0,
    )


def test_run_python_fixer_timeout_raises(tmp_path) -> None:
    handler = _write_handler(tmp_path, "slow.py", "import time\ntime.sleep(5)\n")
    fixer = _bare_fixer(handler, timeout=0.5)
    with pytest.raises(RuntimeError, match="timed out"):
        asyncio.run(run_python_fixer(fixer, {"condition_key": "k"}))


def test_run_python_fixer_non_json_raises(tmp_path) -> None:
    handler = _write_handler(tmp_path, "garbage.py", "print('not json')\n")
    fixer = _bare_fixer(handler)
    with pytest.raises(ValueError, match="non-JSON"):
        asyncio.run(run_python_fixer(fixer, {"condition_key": "k"}))


def test_run_python_fixer_missing_outcome_raises(tmp_path) -> None:
    handler = _write_handler(tmp_path, "no_outcome.py", "print('{\"note\": \"x\"}')\n")
    fixer = _bare_fixer(handler)
    with pytest.raises(ValueError, match="non-empty 'outcome'"):
        asyncio.run(run_python_fixer(fixer, {"condition_key": "k"}))


def test_handler_error_recorded_and_counts_against_guard(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "boom.py", _FAILING_HANDLER)
    _write_fixer(
        tmp_path, "dep-boom", handler="fixers/handlers/boom.py", max_attempts=1
    )
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    try:
        _open_dep_incident(daemon)
        asyncio.run(daemon._evaluate_fixers())  # errors, but counts
        asyncio.run(daemon._evaluate_fixers())  # guard now blocks
        lines = _read_fixers_log(tmp_path)
        assert len(lines) == 1
        assert "outcome=error" in lines[0]
    finally:
        daemon.connection.close()


# --- hot reload --------------------------------------------------------------


def test_hot_reload_adds_and_removes_fixer(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)
    try:
        assert daemon.lodging.fixers == {}
        path = _write_fixer(tmp_path, "dep-observe")
        reloader.event_queue.put(str(path))
        asyncio.run(reloader.process_pending_events())
        assert set(daemon.lodging.fixers) == {"dep-observe"}

        path.unlink()
        reloader.event_queue.put(str(path))
        asyncio.run(reloader.process_pending_events())
        assert daemon.lodging.fixers == {}
    finally:
        daemon.connection.close()


def test_hot_reload_rejects_bad_fixer_with_lodging_finding(tmp_path) -> None:
    _write_base_lodging(tmp_path)
    _write_handler(tmp_path, "observe.py", _OBSERVE_HANDLER)
    daemon = _make_daemon(tmp_path, FakeClock(PINNED))
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)
    try:
        bad = _write_fixer(tmp_path, "bad", condition="kind: bogus\n")
        reloader.event_queue.put(str(bad))
        asyncio.run(reloader.process_pending_events())
        # Not applied, and a load failure was recorded for the file.
        assert daemon.lodging.fixers == {}
        assert any(
            "fixers/bad.yaml" in str(p) for p in reloader.rejected
        )
        assert daemon.catalog.open_internal_incident_count() >= 1
    finally:
        daemon.connection.close()


def test_identify_maps_fixer_path(tmp_path) -> None:
    ident = _identify(tmp_path, tmp_path / "fixers" / "x.yaml")
    assert ident is not None
    assert ident.kind == "fixer"
    assert ident.key == "x"
    # A handler .py one level down is not a fixer binding.
    assert _identify(tmp_path, tmp_path / "fixers" / "handlers" / "x.py") is None
