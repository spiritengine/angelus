"""Tests for deploy/sre_runner.py.

Mock the spindle spawn and notify-pat — never spawn a real agent or send real
pages.  Tests verify guard logic, audit log output, and sentinel lifecycle.
"""

from __future__ import annotations

import fcntl
import importlib.util
import json
import os
import time
from pathlib import Path
from unittest.mock import MagicMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
SRE_RUNNER_PATH = REPO_ROOT / "deploy" / "sre_runner.py"


def _load_runner():
    spec = importlib.util.spec_from_file_location("sre_runner_under_test", SRE_RUNNER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_sentinel(state: Path, reason: str = "crash-loop: test reason") -> None:
    state.mkdir(parents=True, exist_ok=True)
    (state / "belfry-needs-sre").write_text(f"2026-05-31T00:00:00Z {reason}\n")


def _read_fixers_log(state: Path) -> list[str]:
    path = state / "fixers.log"
    if not path.exists():
        return []
    return [line for line in path.read_text().splitlines() if line.strip()]


def _report_path_from_prompt(prompt: str) -> Path | None:
    """Pull the absolute report path the runner embedded in the agent prompt.

    The runner generates a timestamped report filename inside _run, so a test
    cannot predict it; the agent learns it from the prompt, so a fake agent
    (spindle_spin side effect) does the same to simulate writing the report.
    """
    for raw in prompt.splitlines():
        line = raw.strip()
        if "sre-reports" in line and line.endswith(".md"):
            return Path(line)
    return None


_RECOVERED_REPORT = (
    "outcome: resolved\nroot-cause: test\nservice-state: recovered\n"
    "confidence: high\n"
)


def _spin_writes_report(spool_id: str = "abc12345", content: str = _RECOVERED_REPORT):
    """Fake spindle_spin that simulates the agent writing its report file.

    Mirrors the verified happy path: the spool runs and leaves a report at the
    exact path the runner passed in the prompt. `content` defaults to a
    recovered/resolved header; pass an honest-failure or malformed header to
    exercise the report-content gate.
    """
    def _spin(prompt, working_dir, tags, env=None):
        report_path = _report_path_from_prompt(prompt)
        assert report_path is not None, "report path not found in prompt"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(content)
        return spool_id
    return _spin


def _fake_spin_ok(spool_id: str = "abc12345"):
    """Return a subprocess.CompletedProcess that looks like a successful spindle spin."""
    mock = MagicMock()
    mock.returncode = 0
    mock.stdout = json.dumps({"spool_id": spool_id})
    mock.stderr = ""
    return mock


def _fake_wait_ok(spool_id: str = "abc12345"):
    """Return a subprocess.CompletedProcess that looks like a successful spindle wait."""
    mock = MagicMock()
    mock.returncode = 0
    # gather mode JSON: {spool_id: result_text}
    mock.stdout = json.dumps({spool_id: "Agent completed"})
    mock.stderr = ""
    return mock


def _fake_wait_timeout(spool_id: str = "abc12345"):
    mock = MagicMock()
    mock.returncode = 1
    mock.stdout = f"Timeout after 1800s. Spools still running: {spool_id}"
    mock.stderr = ""
    return mock


def _spools_json(spool_id: str, status: str) -> str:
    """Real `spindle spools` JSON shape (spindle/__init__.py _spools_sync): a
    dict keyed by spool_id, each value carrying status/prompt/created_at/
    session_id. `status` is the authoritative terminal field
    ('complete'/'error'/'running'/'pending')."""
    return json.dumps(
        {
            spool_id: {
                "status": status,
                "prompt": "angelus's belfry watchdog escalated...",
                "created_at": "2026-06-12T00:00:00",
                "session_id": "sess-abcd",
            }
        },
        indent=2,
    )


def _spindle_dispatch(wait_stdout: str, spools_stdout: str):
    """subprocess.run side_effect dispatching on the spindle subcommand.

    spindle_wait now issues two calls — the `spindle wait` barrier and the
    `spindle spools` typed-status query — so a single return_value no longer
    suffices. Dispatch on argv so the test does not depend on call ordering.
    """
    def _run(cmd, *args, **kwargs):
        proc = MagicMock()
        proc.returncode = 0
        proc.stderr = ""
        if cmd[:2] == ["spindle", "wait"]:
            proc.stdout = wait_stdout
        elif cmd[:2] == ["spindle", "spools"]:
            proc.stdout = spools_stdout
        else:
            proc.stdout = ""
        return proc

    return _run


# ---------------------------------------------------------------------------
# Test: no sentinel -> no spawn
# ---------------------------------------------------------------------------

def test_no_sentinel_no_spawn(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    state.mkdir()

    with patch.object(runner, "spindle_spin") as mock_spin, \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    mock_spin.assert_not_called()
    mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# Test: sentinel present, fresh incident -> spawns once; records timestamps;
#       fixers.log gets spawn line with spool_id and report path.
# ---------------------------------------------------------------------------

def test_fresh_incident_spawns_once(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "test crash-loop reason")

    # Daemon is "healthy" after the run AND the agent wrote its report, so this
    # is a verified resolution: sentinel gets cleared, no page.
    with patch.object(runner, "spindle_spin", side_effect=_spin_writes_report("abc12345")) as mock_spin, \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    mock_spin.assert_called_once()

    # spawn log should have one entry
    spawn_log_path = state / "sre-spawn-log"
    assert spawn_log_path.exists()
    entries = [l.strip() for l in spawn_log_path.read_text().splitlines() if l.strip()]
    assert len(entries) == 1
    ts = float(entries[0])
    assert time.time() - ts < 10

    # last-spawn file should have been cleared (daemon healthy -> reset)
    last_spawn = state / "sre-last-spawn-at"
    assert not last_spawn.exists()

    # sentinel should be cleared
    assert not (state / "belfry-needs-sre").exists()

    # fixers.log must contain a spawn line with spool_id and report_path
    log_lines = _read_fixers_log(state)
    spawn_lines = [l for l in log_lines if "action=spawn" in l]
    assert len(spawn_lines) == 1
    assert "spool_id=abc12345" in spawn_lines[0]
    assert "report_path=" in spawn_lines[0]

    # notify_pat should not fire (daemon is healthy, no error path taken)
    mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# Test: spawn invocation uses permission auto+shard and canonical repo dir,
#       and prompt contains the absolute report path + required-report instruction.
# ---------------------------------------------------------------------------

def test_spawn_invocation_shape(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "loop reason")

    captured_prompt = {}
    captured_working_dir = {}

    def fake_spin(prompt, working_dir, tags, env=None):
        captured_prompt["v"] = prompt
        captured_working_dir["v"] = working_dir
        return "spool99"

    with patch.object(runner, "spindle_spin", side_effect=fake_spin), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat"):
        runner._run(state)

    prompt = captured_prompt["v"]
    working_dir = captured_working_dir["v"]

    # The fixer agent must land in the ENGINE repo, never the deployment root
    # the runner was invoked against -- in a split deployment that root is a
    # YAML-only lodging repo with no code or tests (the same deployment-root/
    # code-root conflation belfry's stale-deploy check had). The repo is
    # resolved from the installed package's provenance, not __file__.
    assert working_dir == str(runner.resolve_engine_repo())
    assert working_dir != str(tmp_path)

    # prompt must contain the absolute report path under state/sre-reports/
    assert str(state / "sre-reports") in prompt

    # prompt must contain the required-report-file instruction
    assert "you MUST write your report to this exact absolute path" in prompt

    # prompt must reference the required fields
    for field in ("outcome:", "root-cause:", "actions-taken:", "service-state:", "confidence:"):
        assert field in prompt

    # Diagnose-and-tender model: the agent prepares a fix on its shard branch but
    # does NOT deploy (it can't from the sandbox, and a restart wouldn't apply it
    # under the non-editable install). The prompt must say it does not deploy and
    # must NOT instruct it to merge/deploy/restart as the apply path.
    assert "YOU DO NOT DEPLOY" in prompt
    assert "COMMIT it to your shard branch" in prompt
    assert "Do NOT merge to master" in prompt
    # No leftover "restart heals" or "run make deploy to apply" instruction.
    assert "systemctl --user restart angelus` and verify it comes" not in prompt
    assert "run `make deploy`. This is the only" not in prompt


# ---------------------------------------------------------------------------
# Test: MIN_SPAWN_INTERVAL throttle -> NO spawn when last spawn < interval ago.
# ---------------------------------------------------------------------------

def test_min_interval_throttle(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    _write_sentinel(state)

    # Write a last-spawn timestamp from 60 seconds ago
    recent_ts = time.time() - 60
    (state / "sre-last-spawn-at").write_text(str(recent_ts))

    with patch.dict(os.environ, {"ANGELUS_SRE_MIN_INTERVAL_SEC": "2700"}), \
         patch.object(runner, "spindle_spin") as mock_spin, \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    mock_spin.assert_not_called()
    mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# Test: MAX_SPAWNS cap -> NO spawn, escalation page fires, sentinel retained.
# ---------------------------------------------------------------------------

def test_relative_reports_dir_reaches_prompt_and_bind_absolute(
    tmp_path, monkeypatch
):
    """A relative ANGELUS_SRE_REPORTS_DIR must be resolved once at
    construction: the report path in the agent prompt and the sandbox bind
    must be the same ABSOLUTE directory. Unresolved, the prompt carried the
    relative path while the bind resolved against the runner's cwd -- the
    agent (sitting in a shard of the engine repo, not that cwd) would write
    the 3am incident report outside the bound directory and it would be
    silently lost. Pins the resolve at _run's construction site; every other
    test passes an absolute state path, where resolve is identity."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "loop reason")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ANGELUS_SRE_REPORTS_DIR", "rel-reports")
    monkeypatch.delenv("SPINDLE_SHARD_WRITABLE_BINDS", raising=False)

    captured = {}

    def fake_spin(prompt, working_dir, tags, env=None):
        captured["prompt"] = prompt
        captured["env"] = env
        return "spool99"

    with patch.object(runner, "spindle_spin", side_effect=fake_spin), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat"):
        runner._run(state)

    expected_dir = (tmp_path / "rel-reports").resolve()
    bind = captured["env"]["SPINDLE_SHARD_WRITABLE_BINDS"]
    assert Path(bind).is_absolute()
    assert bind == str(expected_dir)
    assert str(expected_dir) in captured["prompt"]
    assert " rel-reports/" not in captured["prompt"]


def test_max_spawns_cap_triggers_escalation(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    _write_sentinel(state, "loop reason")

    # Write 3 spawn log entries all within the 6h window (use short window for test)
    now = time.time()
    timestamps = [now - 100, now - 200, now - 300]
    (state / "sre-spawn-log").write_text("".join(f"{ts}\n" for ts in timestamps))

    # Set max to 3 and window large enough to include all entries
    with patch.dict(os.environ, {
        "ANGELUS_SRE_MAX_SPAWNS": "3",
        "ANGELUS_SRE_SPAWN_WINDOW_SEC": "21600",
    }), \
         patch.object(runner, "spindle_spin") as mock_spin, \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    mock_spin.assert_not_called()

    # notify_pat must fire (escalate-and-stop)
    mock_notify.assert_called_once()
    page_msg = mock_notify.call_args[0][0]
    assert "exhausted" in page_msg.lower() or "budget" in page_msg.lower()

    # sentinel must be retained
    assert (state / "belfry-needs-sre").exists()

    # fixers.log must have sre-exhausted line
    log_lines = _read_fixers_log(state)
    exhausted = [l for l in log_lines if "sre-exhausted" in l]
    assert len(exhausted) == 1


# ---------------------------------------------------------------------------
# Test: fail-safe — rate-guard state unreadable -> NO spawn.
# ---------------------------------------------------------------------------

def test_fail_safe_unreadable_spawn_log(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    _write_sentinel(state)

    # Make read_spawn_log raise OSError (simulates permissions error)
    with patch.object(runner, "read_spawn_log", side_effect=OSError("EIO")), \
         patch.object(runner, "spindle_spin") as mock_spin, \
         patch.object(runner, "notify_pat"):
        rc = runner._run(state)

    assert rc == 0
    mock_spin.assert_not_called()


def test_fail_safe_unreadable_last_spawn(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    _write_sentinel(state)

    # Make read_last_spawn_ts raise OSError
    with patch.object(runner, "read_last_spawn_ts", side_effect=OSError("EPERM")), \
         patch.object(runner, "spindle_spin") as mock_spin, \
         patch.object(runner, "notify_pat"):
        rc = runner._run(state)

    assert rc == 0
    mock_spin.assert_not_called()


def test_fail_safe_unwritable_spawn_log(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    _write_sentinel(state)

    # write_spawn_log returns False (simulates write failure)
    with patch.object(runner, "write_spawn_log", return_value=False), \
         patch.object(runner, "spindle_spin") as mock_spin, \
         patch.object(runner, "notify_pat"):
        rc = runner._run(state)

    assert rc == 0
    mock_spin.assert_not_called()


def test_fail_safe_write_last_spawn_ts_fails_no_spawn(tmp_path):
    """write_last_spawn_ts returning False triggers rollback and blocks spawn."""
    runner = _load_runner()
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    _write_sentinel(state)

    with patch.object(runner, "write_last_spawn_ts", return_value=False), \
         patch.object(runner, "spindle_spin") as mock_spin, \
         patch.object(runner, "notify_pat"):
        rc = runner._run(state)

    assert rc == 0
    mock_spin.assert_not_called()


def test_failed_spawn_counts_toward_both_guards(tmp_path):
    """A spindle_spin returning None (failed spawn) still persists both state files.

    The next tick must be throttled by the 45-min interval and the failed
    attempt counts toward the 6h max-spawns window.
    """
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: failed-spawn test")

    before = time.time()

    with patch.object(runner, "spindle_spin", return_value=None) as mock_spin, \
         patch.object(runner, "notify_pat"):
        rc = runner._run(state)

    after = time.time()

    mock_spin.assert_called_once()

    # sre-spawn-log must have exactly one entry within the test time range
    spawn_log_path = state / "sre-spawn-log"
    assert spawn_log_path.exists(), "sre-spawn-log must exist after a failed spawn"
    entries = [l.strip() for l in spawn_log_path.read_text().splitlines() if l.strip()]
    assert len(entries) == 1
    ts = float(entries[0])
    assert before <= ts <= after, "spawn log entry timestamp is outside test window"

    # sre-last-spawn-at must be written so the next tick is throttled
    last_spawn_path = state / "sre-last-spawn-at"
    assert last_spawn_path.exists(), "sre-last-spawn-at must exist after a failed spawn"
    last_ts = float(last_spawn_path.read_text().strip())
    assert before <= last_ts <= after, "last-spawn timestamp is outside test window"

    # rc=1 signals failed spawn (guards still applied)
    assert rc == 1


# ---------------------------------------------------------------------------
# Test: sentinel clear on healthy post-check; retained on unhealthy.
# ---------------------------------------------------------------------------

def test_sentinel_cleared_when_daemon_healthy(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "test")

    with patch.object(runner, "spindle_spin", return_value="spoolA"), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat"):
        runner._run(state)

    assert not (state / "belfry-needs-sre").exists()
    # spawn state also cleared
    assert not (state / "sre-last-spawn-at").exists()


def test_sentinel_retained_when_daemon_still_down(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "still broken")

    with patch.object(runner, "spindle_spin", return_value="spoolB"), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=False), \
         patch.object(runner, "notify_pat") as mock_notify:
        runner._run(state)

    assert (state / "belfry-needs-sre").exists()
    mock_notify.assert_called_once()
    msg = mock_notify.call_args[0][0]
    assert "still down" in msg or "unhealthy" in msg.lower() or "spoolB" in msg


# ---------------------------------------------------------------------------
# Resolution attribution: a healthy daemon alone must not credit the agent.
# A dead/erroring spool, or a spool that wrote no report, is an UNVERIFIED
# recovery — clear the sentinel (daemon is up) but log it honestly and page,
# never a clean resolved/cleared. Reserve "resolved" for spool-completed +
# report-present. (issue-20260612-hfje)
# ---------------------------------------------------------------------------

def test_errored_spool_healthy_daemon_clears_unverified_not_resolved(tmp_path):
    """Reproduces the 2026-06-12 incident: the SRE spool errored (died at a
    session limit after read-only diagnostics, no fix, no report), but a human
    independently recovered the daemon. The runner must NOT log resolved/cleared
    crediting the dead spool — it must clear the sentinel (daemon healthy) yet
    log it as unverified and page a human."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: migration 0015 not applied")

    # Errored spool, no report file ever written, daemon healthy via external recovery.
    with patch.object(runner, "spindle_spin", return_value="358328ee"), \
         patch.object(runner, "spindle_wait", return_value="errored"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0

    # Sentinel cleared (daemon is up — do not re-spawn every tick).
    assert not (state / "belfry-needs-sre").exists()
    assert not (state / "sre-last-spawn-at").exists()

    # A human is paged that recovery was unverified.
    mock_notify.assert_called_once()
    msg = mock_notify.call_args[0][0]
    assert "358328ee" in msg
    assert "verified" in msg.lower()

    log_lines = _read_fixers_log(state)
    # NO resolved line crediting the dead agent.
    assert not [l for l in log_lines if "action=resolved" in l]
    # An honest unverified line instead.
    unverified = [l for l in log_lines if "action=cleared-unverified" in l]
    assert len(unverified) == 1
    assert "spool_id=358328ee" in unverified[0]
    assert "completion_status=errored" in unverified[0]
    assert "report_written=false" in unverified[0]


def test_completed_spool_with_report_logs_resolved(tmp_path):
    """Spool completed AND the report file exists at report_path AND daemon
    healthy -> the one case we credit the agent: a clean resolved/cleared, no
    page."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: real fix")

    with patch.object(runner, "spindle_spin", side_effect=_spin_writes_report("good01")), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    assert not (state / "belfry-needs-sre").exists()
    assert not (state / "sre-last-spawn-at").exists()

    # Verified resolution does not page.
    mock_notify.assert_not_called()

    log_lines = _read_fixers_log(state)
    resolved = [l for l in log_lines if "action=resolved" in l and "outcome=cleared" in l]
    assert len(resolved) == 1
    assert "spool_id=good01" in resolved[0]
    # No unverified line on the clean path.
    assert not [l for l in log_lines if "action=cleared-unverified" in l]


def test_completed_spool_no_report_clears_unverified(tmp_path):
    """Spool reported completed but wrote NO report file, daemon healthy. The
    report is the proof the agent did its job; without it the recovery is
    unverified even though the spool didn't explicitly error."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: no report")

    # spindle_spin returns a spool id but never writes a report.
    with patch.object(runner, "spindle_spin", return_value="noreport1"), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    assert not (state / "belfry-needs-sre").exists()

    mock_notify.assert_called_once()
    msg = mock_notify.call_args[0][0]
    assert "verified" in msg.lower()

    log_lines = _read_fixers_log(state)
    assert not [l for l in log_lines if "action=resolved" in l]
    unverified = [l for l in log_lines if "action=cleared-unverified" in l]
    assert len(unverified) == 1
    assert "completion_status=completed" in unverified[0]
    assert "report_written=false" in unverified[0]


def test_errored_spool_unhealthy_daemon_retains_sentinel(tmp_path):
    """Daemon still unhealthy after an errored spool -> unchanged behavior:
    sentinel retained for the next tick, human paged."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: still down")

    with patch.object(runner, "spindle_spin", return_value="dead99"), \
         patch.object(runner, "spindle_wait", return_value="errored"), \
         patch.object(runner, "check_daemon_healthy", return_value=False), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    # Sentinel retained — daemon is still down.
    assert (state / "belfry-needs-sre").exists()
    mock_notify.assert_called_once()

    log_lines = _read_fixers_log(state)
    assert not [l for l in log_lines if "action=resolved" in l]
    assert not [l for l in log_lines if "action=cleared-unverified" in l]


def test_spindle_wait_reads_typed_status():
    """spindle_wait classifies by spindle's authoritative typed per-spool status
    (read from `spindle spools`), not by the wait result's freeform text. Maps
    'complete' -> 'completed' and 'error' -> 'errored', regardless of what the
    wait payload says."""
    runner = _load_runner()
    spool = "x1"

    # Typed status 'error' -> errored, even though the wait payload is a normal
    # gather-mode result (no "Error:" prefix).
    with patch.object(
        runner.subprocess,
        "run",
        side_effect=_spindle_dispatch(
            json.dumps({spool: "did some diagnostics"}),
            _spools_json(spool, "error"),
        ),
    ):
        assert runner.spindle_wait(spool, 10) == "errored"

    # Typed status 'complete' -> completed.
    with patch.object(
        runner.subprocess,
        "run",
        side_effect=_spindle_dispatch(
            json.dumps({spool: "Fixed migration 0015; daemon healthy."}),
            _spools_json(spool, "complete"),
        ),
    ):
        assert runner.spindle_wait(spool, 10) == "completed"


def test_spindle_wait_result_text_error_prefix_but_typed_complete_is_completed():
    """Codex's false-positive (hfje fell-r2): a SUCCESSFUL agent whose result
    text happens to begin "Error:" serializes in gather mode identically to a
    failed spool. The old text heuristic (value.startswith("Error:")) misread
    that as 'errored' -> a false cleared-unverified + page. Reading the typed
    status dissolves the ambiguity: typed 'complete' -> completed regardless of
    the result text."""
    runner = _load_runner()
    spool = "358328ee"

    with patch.object(
        runner.subprocess,
        "run",
        side_effect=_spindle_dispatch(
            json.dumps({spool: "Error: budget reached, but root cause was X — fixed."}),
            _spools_json(spool, "complete"),
        ),
    ):
        assert runner.spindle_wait(spool, 10) == "completed"


def test_spindle_wait_absent_spool_is_errored():
    """If the spool_id is missing from `spindle spools` output (e.g. spindle's
    own wait failed on an unknown id, or the spool record vanished), there is no
    typed status to vouch for completion -> conservative 'errored'."""
    runner = _load_runner()
    spool = "ghost123"

    with patch.object(
        runner.subprocess,
        "run",
        side_effect=_spindle_dispatch(
            f"Error: Unknown spool_id '{spool}'",
            _spools_json("someone-else", "complete"),
        ),
    ):
        assert runner.spindle_wait(spool, 10) == "errored"


def test_spindle_wait_non_terminal_status_is_errored():
    """A still-running/pending spool surfaced by the spools query (barrier
    returned without timeout text but the spool has not reached a terminal
    state) is treated conservatively as errored, never credited."""
    runner = _load_runner()
    spool = "x1"

    with patch.object(
        runner.subprocess,
        "run",
        side_effect=_spindle_dispatch(
            json.dumps({spool: "partial"}),
            _spools_json(spool, "running"),
        ),
    ):
        assert runner.spindle_wait(spool, 10) == "errored"


def test_typed_error_spool_with_report_clears_unverified_not_resolved(tmp_path):
    """End-to-end Step 8 through the REAL spindle_wait/query_spool_status path
    (subprocess mocked, not spindle_wait's return value): a spool that wrote its
    report and THEN died at a session limit. Its gather-mode result text even
    LOOKS successful, but spindle's typed status is 'error'. With the daemon
    recovered out-of-band and a report present on disk, this must take the
    unverified path (cleared-unverified + page), never a clean resolved
    crediting the dead spool.

    Fails against the current shard code: the old text heuristic reads the
    success-looking result text as 'completed' and the report-present check
    credits the agent — exactly the hfje incident (fixers.log: spool 358328ee
    `outcome=completed`)."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: session limit after diagnostics")

    spool = "358328ee"
    # Result TEXT looks like a clean success; only the typed status reveals the
    # failure. This is what makes the test fail on the pre-fix text heuristic.
    dispatch = _spindle_dispatch(
        json.dumps({spool: "Root cause found in migration 0015; daemon healthy."}),
        _spools_json(spool, "error"),
    )

    with patch.object(runner, "spindle_spin", side_effect=_spin_writes_report(spool)), \
         patch.object(runner.subprocess, "run", side_effect=dispatch), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    # Sentinel cleared (daemon is up) but the agent is NOT credited.
    assert not (state / "belfry-needs-sre").exists()

    # Paged: recovery unverified despite a report being present.
    mock_notify.assert_called_once()
    assert "verified" in mock_notify.call_args[0][0].lower()

    log_lines = _read_fixers_log(state)
    # NOT a resolved line, even though the report file exists on disk.
    assert not [l for l in log_lines if "action=resolved" in l]
    unverified = [l for l in log_lines if "action=cleared-unverified" in l]
    assert len(unverified) == 1
    assert f"spool_id={spool}" in unverified[0]
    assert "completion_status=errored" in unverified[0]
    # report WAS written — the distinguishing detail vs the no-report case, and
    # the proof the credit hinges on completion_status, not report presence.
    assert "report_written=true" in unverified[0]


def test_typed_complete_spool_with_report_logs_resolved(tmp_path):
    """End-to-end through the REAL typed-status path: typed status 'complete'
    plus a report on disk plus a healthy daemon -> clean resolved, agent
    credited. The success counterpart to the typed-error test above."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: recovered after fix")

    spool = "abc12345"
    dispatch = _spindle_dispatch(
        json.dumps({spool: "Fixed migration 0015; daemon healthy."}),
        _spools_json(spool, "complete"),
    )

    with patch.object(runner, "spindle_spin", side_effect=_spin_writes_report(spool)), \
         patch.object(runner.subprocess, "run", side_effect=dispatch), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    assert not (state / "belfry-needs-sre").exists()
    # Verified resolution credits the agent and does NOT page.
    mock_notify.assert_not_called()

    log_lines = _read_fixers_log(state)
    resolved = [l for l in log_lines if "action=resolved" in l]
    assert len(resolved) == 1
    assert f"spool_id={spool}" in resolved[0]
    assert not [l for l in log_lines if "action=cleared-unverified" in l]


def test_typed_complete_spool_no_report_clears_unverified(tmp_path):
    """Typed status 'complete' but NO report on disk plus a healthy daemon ->
    cleared-unverified + page. completion_status alone is not enough; the
    report-existence backstop still gates crediting the agent."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: recovered out-of-band")

    spool = "noreport1"
    dispatch = _spindle_dispatch(
        json.dumps({spool: "looked around, wrote nothing"}),
        _spools_json(spool, "complete"),
    )

    # spindle_spin returns the spool id but writes NO report.
    with patch.object(runner, "spindle_spin", return_value=spool), \
         patch.object(runner.subprocess, "run", side_effect=dispatch), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    assert not (state / "belfry-needs-sre").exists()
    mock_notify.assert_called_once()

    log_lines = _read_fixers_log(state)
    assert not [l for l in log_lines if "action=resolved" in l]
    unverified = [l for l in log_lines if "action=cleared-unverified" in l]
    assert len(unverified) == 1
    assert "completion_status=completed" in unverified[0]
    assert "report_written=false" in unverified[0]


# ---------------------------------------------------------------------------
# Report-content gate (issue-20260613-oe3x): the report's EXISTENCE is not
# proof of a fix — the SRE prompt requires a report in EVERY outcome, including
# an honest escalated-to-human / service-state: not-recovered. Credit a clean
# resolved only when the report's own header affirmatively claims recovery
# (outcome: resolved AND service-state: recovered). Any other header — an
# honest failure, a contradiction, or a missing/malformed field — must NOT
# credit and must page (fail-loud). This closes the concurrent-external-
# recovery false-attribution case hfje left as a follow-up.
# ---------------------------------------------------------------------------

def test_report_claims_recovery_parses_header():
    """Unit-level: the parser credits ONLY outcome: resolved + service-state:
    recovered, and fails closed on every other shape."""
    runner = _load_runner()

    def _check(content, tmp):
        p = tmp / "r.md"
        p.write_text(content)
        return runner.report_claims_recovery(p)

    import tempfile
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        # The one credited shape.
        assert _check(
            "outcome: resolved\nservice-state: recovered\nconfidence: high\n", tmp
        ) is True
        # Case/whitespace tolerance on the value.
        assert _check(
            "outcome:  Resolved \nservice-state:\tRECOVERED\n", tmp
        ) is True
        # Honest failure: escalated + not-recovered.
        assert _check(
            "outcome: escalated-to-human\nservice-state: not-recovered\n", tmp
        ) is False
        # Contradictions never credit.
        assert _check("outcome: resolved\nservice-state: not-recovered\n", tmp) is False
        assert _check("outcome: escalated-to-human\nservice-state: recovered\n", tmp) is False
        # Unknown service-state.
        assert _check("outcome: resolved\nservice-state: unknown\n", tmp) is False
        # Missing fields entirely.
        assert _check("root-cause: something\nconfidence: low\n", tmp) is False
        assert _check("outcome: resolved\n", tmp) is False
        # Decorated value (exact-token match) -> fail-loud, does not credit.
        assert _check(
            "outcome: resolved\nservice-state: recovered (daemon up)\n", tmp
        ) is False
        # Duplicated header: a credited-looking first occurrence followed by a
        # contradicting duplicate is ambiguous -> fail closed, must page.
        assert _check(
            "outcome: resolved\nservice-state: recovered\n"
            "outcome: escalated-to-human\nservice-state: not-recovered\n",
            tmp,
        ) is False
        # Benign exact-duplicate (values agree) is still ambiguous under the
        # fail-loud posture -> fail closed regardless of agreement.
        assert _check(
            "outcome: resolved\nservice-state: recovered\n"
            "service-state: recovered\n",
            tmp,
        ) is False
        # Empty / junk file.
        assert _check("", tmp) is False
        assert _check("totally unstructured prose with no header at all", tmp) is False

    # Unreadable file (does not exist) fails closed, never raises.
    assert runner.report_claims_recovery(Path("/nonexistent/report.md")) is False


def test_recovered_report_credits_resolved(tmp_path):
    """Completed spool + report whose header says outcome: resolved /
    service-state: recovered + healthy daemon -> the one credited case."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: genuine fix")

    with patch.object(runner, "spindle_spin", side_effect=_spin_writes_report("rec01")), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    assert not (state / "belfry-needs-sre").exists()
    mock_notify.assert_not_called()

    log_lines = _read_fixers_log(state)
    resolved = [l for l in log_lines if "action=resolved" in l and "outcome=cleared" in l]
    assert len(resolved) == 1
    assert "spool_id=rec01" in resolved[0]
    assert not [l for l in log_lines if "action=cleared-unverified" in l]


def test_honest_not_recovered_report_does_not_credit_and_pages(tmp_path):
    """The oe3x case: a live agent COMPLETES and honestly writes an
    escalated-to-human / service-state: not-recovered report, while the daemon
    recovers out-of-band. Existence-only crediting would falsely log resolved;
    the content gate must route to cleared-unverified + page instead."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: agent could not fix")

    honest = (
        "outcome: escalated-to-human\n"
        "root-cause: bad env value in angelus.env; needs a human\n"
        "actions-taken: - diagnosed, did not change config\n"
        "commits: none\n"
        "service-state: not-recovered\n"
        "confidence: high\n"
        "follow-ups: human should fix the env value and redeploy\n"
    )

    with patch.object(runner, "spindle_spin", side_effect=_spin_writes_report("honest1", honest)), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    # Sentinel cleared (daemon is up) but the agent is NOT credited.
    assert not (state / "belfry-needs-sre").exists()

    mock_notify.assert_called_once()
    assert "verified" in mock_notify.call_args[0][0].lower()

    log_lines = _read_fixers_log(state)
    # NO resolved line crediting an agent that itself said "not recovered".
    assert not [l for l in log_lines if "action=resolved" in l]
    unverified = [l for l in log_lines if "action=cleared-unverified" in l]
    assert len(unverified) == 1
    assert "spool_id=honest1" in unverified[0]
    assert "completion_status=completed" in unverified[0]
    # The report WAS written — the distinguishing detail vs the no-report case,
    # proving the credit hinges on CONTENT, not existence.
    assert "report_written=true" in unverified[0]
    assert "recovery_claimed=false" in unverified[0]


def test_malformed_report_header_does_not_credit_and_pages(tmp_path):
    """A report present on disk but with a missing/unparseable header (the agent
    wrote freeform prose, or the header drifted) must fail closed: not credited,
    cleared-unverified, paged — consistent with resolve_engine_repo's fail-loud
    posture."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: garbled report")

    garbled = "I poked around for a while and think it's probably fine now.\n"

    with patch.object(runner, "spindle_spin", side_effect=_spin_writes_report("junk1", garbled)), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    assert not (state / "belfry-needs-sre").exists()
    mock_notify.assert_called_once()
    assert "verified" in mock_notify.call_args[0][0].lower()

    log_lines = _read_fixers_log(state)
    assert not [l for l in log_lines if "action=resolved" in l]
    unverified = [l for l in log_lines if "action=cleared-unverified" in l]
    assert len(unverified) == 1
    assert "report_written=true" in unverified[0]
    assert "recovery_claimed=false" in unverified[0]


def test_typed_error_spool_unhealthy_daemon_retains_sentinel(tmp_path):
    """Daemon still unhealthy -> sentinel retained for the next tick, regardless
    of typed status. The unhealthy branch is unchanged by the status-source fix."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: still down")

    spool = "stilldown"
    dispatch = _spindle_dispatch(
        json.dumps({spool: "Error: session limit reached"}),
        _spools_json(spool, "error"),
    )

    with patch.object(runner, "spindle_spin", return_value=spool), \
         patch.object(runner.subprocess, "run", side_effect=dispatch), \
         patch.object(runner, "check_daemon_healthy", return_value=False), \
         patch.object(runner, "notify_pat"):
        rc = runner._run(state)

    assert rc == 0
    # Daemon down: sentinel must remain so the next tick re-attempts.
    assert (state / "belfry-needs-sre").exists()
    log_lines = _read_fixers_log(state)
    assert not [l for l in log_lines if "action=resolved" in l]
    assert not [l for l in log_lines if "action=cleared-unverified" in l]


# ---------------------------------------------------------------------------
# Test: lock held -> tick no-ops.
# ---------------------------------------------------------------------------

def test_lock_held_exits_cleanly(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    _write_sentinel(state, "test")

    lock_path = state / "sre-runner.lock"
    lock_path.touch()

    # Acquire the lock ourselves before calling main()
    with lock_path.open("a") as held_fh:
        fcntl.flock(held_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            with patch.object(runner, "spindle_spin") as mock_spin, \
                 patch.object(runner, "notify_pat"):
                rc = runner.main([str(tmp_path)])
        finally:
            fcntl.flock(held_fh, fcntl.LOCK_UN)

    assert rc == 0
    mock_spin.assert_not_called()


# ---------------------------------------------------------------------------
# Test: timeout -> sentinel retained, page fires.
# ---------------------------------------------------------------------------

def test_timeout_retains_sentinel(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "timeout test")

    with patch.object(runner, "spindle_spin", return_value="spoolT"), \
         patch.object(runner, "spindle_wait", return_value="timeout"), \
         patch.object(runner, "check_daemon_healthy", return_value=False), \
         patch.object(runner, "notify_pat") as mock_notify:
        runner._run(state)

    assert (state / "belfry-needs-sre").exists()
    mock_notify.assert_called_once()
    msg = mock_notify.call_args[0][0]
    assert "timed out" in msg or "timeout" in msg.lower()


# ---------------------------------------------------------------------------
# Test: fixers.log records spawn with spool_id and report path.
# ---------------------------------------------------------------------------

def test_fixers_log_spawn_line(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "log test reason")

    with patch.object(runner, "spindle_spin", return_value="spool42"), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat"):
        runner._run(state)

    log_lines = _read_fixers_log(state)
    spawn_lines = [l for l in log_lines if "action=spawn" in l]
    assert spawn_lines, "Expected at least one spawn line in fixers.log"
    line = spawn_lines[0]
    assert "actor=sre-runner" in line
    assert "spool_id=spool42" in line
    assert "report_path=" in line
    # The report_path field should reference the sre-reports dir
    assert "sre-reports" in line


# ---------------------------------------------------------------------------
# Test: SPINDLE_SHARD_WRITABLE_BINDS is set in the spawn env.
# ---------------------------------------------------------------------------

def test_spawn_env_contains_writable_binds(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "loop reason")

    captured_env = {}

    def fake_spin(prompt, working_dir, tags, env=None):
        captured_env["v"] = env
        return "spool99"

    with patch.object(runner, "spindle_spin", side_effect=fake_spin), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat"):
        runner._run(state)

    env = captured_env["v"]
    assert env is not None
    binds = env.get("SPINDLE_SHARD_WRITABLE_BINDS", "")
    expected = str((state / "sre-reports").resolve())
    assert expected in binds.split(":")


def test_spawn_env_appends_existing_writable_binds(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "loop reason")

    captured_env = {}

    def fake_spin(prompt, working_dir, tags, env=None):
        captured_env["v"] = env
        return "spool99"

    with patch.dict(os.environ, {"SPINDLE_SHARD_WRITABLE_BINDS": "/some/other/path"}), \
         patch.object(runner, "spindle_spin", side_effect=fake_spin), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat"):
        runner._run(state)

    env = captured_env["v"]
    assert env is not None
    parts = env.get("SPINDLE_SHARD_WRITABLE_BINDS", "").split(":")
    assert "/some/other/path" in parts
    expected = str((state / "sre-reports").resolve())
    assert expected in parts


def test_reports_dir_created_before_spawn(tmp_path):
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "loop reason")

    dir_existed_at_spin = {}

    def fake_spin(prompt, working_dir, tags, env=None):
        dir_existed_at_spin["v"] = (state / "sre-reports").exists()
        return "spool99"

    with patch.object(runner, "spindle_spin", side_effect=fake_spin), \
         patch.object(runner, "spindle_wait", return_value="completed"), \
         patch.object(runner, "check_daemon_healthy", return_value=True), \
         patch.object(runner, "notify_pat"):
        runner._run(state)

    assert dir_existed_at_spin["v"] is True


# ---------------------------------------------------------------------------
# Engine-repo resolution (issue-20260615-njaq): post-cutover the runner is
# copied to <lodging>/bin, so __file__/cwd no longer point at the engine repo.
# It must resolve the repo from the installed package's provenance, and refuse
# to spawn (fail loud, retain sentinel) when it cannot find a valid one.
# ---------------------------------------------------------------------------

def _make_engine_repo(path: Path) -> Path:
    """Construct the minimum that is_valid_engine_repo accepts."""
    (path / "angelus").mkdir(parents=True, exist_ok=True)
    (path / "angelus" / "__init__.py").write_text("", encoding="utf-8")
    (path / ".git").mkdir(exist_ok=True)
    (path / "Makefile").write_text("deploy:\n\t@true\n", encoding="utf-8")
    return path


def test_is_valid_engine_repo_requires_git_package_and_makefile(tmp_path):
    runner = _load_runner()
    good = _make_engine_repo(tmp_path / "good")
    assert runner.is_valid_engine_repo(good) is True

    # A YAML-only lodging root (no angelus/, no Makefile, no .git) is rejected --
    # this is exactly the codeless tree the cutover regression pointed at.
    lodging = tmp_path / "lodging"
    (lodging / "entities").mkdir(parents=True)
    assert runner.is_valid_engine_repo(lodging) is False

    # Each missing component individually fails the check.
    for missing in ("angelus", ".git", "Makefile"):
        partial = _make_engine_repo(tmp_path / f"no_{missing.strip('.')}")
        target = partial / missing
        if target.is_dir():
            __import__("shutil").rmtree(target)
        else:
            target.unlink()
        assert runner.is_valid_engine_repo(partial) is False, missing


def test_resolve_engine_repo_prefers_direct_url_provenance(tmp_path):
    """A non-editable install (prod): the PEP 610 file:// URL wins, even though
    the imported-package fallback would point at site-packages."""
    runner = _load_runner()
    repo = _make_engine_repo(tmp_path / "engine")
    with patch.object(
        runner, "_direct_url_repo", return_value=repo
    ), patch.object(
        runner, "_imported_package_repo", return_value=Path("/some/site-packages")
    ):
        assert runner.resolve_engine_repo() == repo


def test_resolve_engine_repo_falls_back_to_imported_location(tmp_path):
    """An editable/dev checkout carries no direct_url; the repo is inferred from
    where the angelus package is imported from."""
    runner = _load_runner()
    repo = _make_engine_repo(tmp_path / "engine")
    with patch.object(
        runner, "_direct_url_repo", return_value=None
    ), patch.object(runner, "_imported_package_repo", return_value=repo):
        assert runner.resolve_engine_repo() == repo


def test_resolve_engine_repo_rejects_invalid_candidates(tmp_path):
    """Both strategies yield non-repos (e.g. site-packages, the lodging root):
    resolve returns None rather than handing the agent a codeless tree."""
    runner = _load_runner()
    not_a_repo = tmp_path / "lodging"
    not_a_repo.mkdir()
    with patch.object(
        runner, "_direct_url_repo", return_value=not_a_repo
    ), patch.object(runner, "_imported_package_repo", return_value=None):
        assert runner.resolve_engine_repo() is None


def test_direct_url_repo_parses_real_provenance_and_fails_soft(tmp_path):
    """Exercise the actual direct_url.json parsing (not a mock of the strategy):
    a git+file install records `url: file://<repo>` -> the repo path; a
    git+file-prefixed scheme, a non-string url, and a malformed authority must
    all return None (never raise -- a raise would skip the caller's fail-loud
    page). This is the parse most likely to break silently on a pip change."""
    runner = _load_runner()

    class _Dist:
        def __init__(self, payload):
            self._payload = payload

        def read_text(self, name):
            assert name == "direct_url.json"
            return self._payload

    def _with(payload):
        return patch.object(
            runner.importlib.metadata, "distribution", return_value=_Dist(payload)
        )

    # Canonical git+file install: pip strips git+, records a file:// URL.
    with _with('{"url": "file:///home/p/projects/angelus", '
               '"vcs_info": {"vcs": "git", "commit_id": "abc"}}'):
        assert runner._direct_url_repo() == Path("/home/p/projects/angelus")

    # Fail-soft (return None, never raise) on each malformed shape.
    for payload in (
        '{"url": "git+file:///home/p/angelus"}',   # scheme not file -> None
        '{"url": 123}',                            # non-string url
        '{"url": "file://["}',                     # bad authority -> ValueError
        '{"no_url": "x"}',                         # missing url
        "not json at all",                         # unparseable
    ):
        with _with(payload):
            assert runner._direct_url_repo() is None, payload


def test_run_fails_loud_and_does_not_spawn_without_engine_repo(tmp_path):
    """When no valid engine repo can be resolved, the runner must NOT spawn an
    agent into a wrong/codeless tree: it pages, retains the needs-sre sentinel,
    logs a blocked outcome, and rate-limits via last-spawn WITHOUT consuming the
    escalation budget (spawn log stays empty)."""
    runner = _load_runner()
    state = tmp_path / "state"
    _write_sentinel(state, "crash-loop: real reason")

    with patch.object(runner, "resolve_engine_repo", return_value=None), \
         patch.object(runner, "spindle_spin") as mock_spin, \
         patch.object(runner, "notify_pat") as mock_notify:
        rc = runner._run(state)

    assert rc == 0
    # The agent was never spawned.
    mock_spin.assert_not_called()
    # Sentinel retained for a human; daemon is still down.
    assert (state / "belfry-needs-sre").exists()
    # A human is paged.
    mock_notify.assert_called_once()
    assert "engine repo" in mock_notify.call_args[0][0]
    # Budget untouched (this is a deploy/env fault, not an escalation attempt),
    # but the page is rate-limited via the last-spawn timestamp.
    assert not (state / "sre-spawn-log").exists() or _read_lines(
        state / "sre-spawn-log"
    ) == []
    assert (state / "sre-last-spawn-at").exists()
    # Logged honestly.
    assert any("blocked-no-engine-repo" in line for line in _read_fixers_log(state))


def _read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line for line in path.read_text().splitlines() if line.strip()]
