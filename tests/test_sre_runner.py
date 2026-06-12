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

    # Daemon is "healthy" after the run so sentinel gets cleared
    with patch.object(runner, "spindle_spin", return_value="abc12345") as mock_spin, \
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

    # The fixer agent must land in the ENGINE repo (CODE_ROOT), never the
    # deployment root the runner was invoked against -- in a split deployment
    # that root is a YAML-only lodging repo with no code or tests (the same
    # deployment-root/code-root conflation belfry's stale-deploy check had).
    assert working_dir == str(runner.CODE_ROOT)
    assert working_dir != str(tmp_path)

    # prompt must contain the absolute report path under state/sre-reports/
    assert str(state / "sre-reports") in prompt

    # prompt must contain the required-report-file instruction
    assert "you MUST write your report to this exact absolute path" in prompt

    # prompt must reference the required fields
    for field in ("outcome:", "root-cause:", "actions-taken:", "service-state:", "confidence:"):
        assert field in prompt


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
