from __future__ import annotations

import importlib.util
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BELFRY_PATH = REPO_ROOT / "belfry" / "belfry.py"


class _Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def _load_belfry():
    spec = importlib.util.spec_from_file_location("belfry_under_test", BELFRY_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_source_fire(root: Path, fired_at: datetime) -> None:
    state = root / "state"
    state.mkdir(exist_ok=True)
    connection = sqlite3.connect(state / "angelus.sqlite3")
    try:
        connection.execute(
            """
            CREATE TABLE source_fires (
                id INTEGER PRIMARY KEY,
                source_name TEXT NOT NULL,
                scheduled_at TEXT,
                fired_at TEXT NOT NULL,
                outcome TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO source_fires (source_name, scheduled_at, fired_at, outcome)
            VALUES ('scheduled/test', NULL, ?, 'ok')
            """,
            (fired_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),),
        )
        connection.commit()
    finally:
        connection.close()


def _set_urls(monkeypatch) -> None:
    monkeypatch.setenv("ANGELUS_BELFRY_SUCCESS_URL", "https://hc.example/success")
    monkeypatch.setenv("ANGELUS_BELFRY_DOWN_URL", "https://hc.example/down")
    monkeypatch.setenv("ANGELUS_EMAIL_TO", "test@example.com")


def test_belfry_imports_no_angelus_modules() -> None:
    for name in list(sys.modules):
        if name == "angelus" or name.startswith("angelus."):
            sys.modules.pop(name)

    _load_belfry()

    assert "angelus" not in sys.modules


def test_pid_dead_path_hits_down_and_escalates(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text("999999", encoding="utf-8")
    pings: list[str] = []
    calls: list[list[str]] = []

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 1
    assert pings == ["https://hc.example/down"]
    assert calls
    assert "dead: PID 999999 is not running" in " ".join(calls[0])


def test_pid_missing_path_hits_down_and_escalates(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    pings: list[str] = []
    calls: list[list[str]] = []

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 1
    assert pings == ["https://hc.example/down"]
    assert calls
    assert "missing PID file" in " ".join(calls[0])


def test_pid_permission_error_is_treated_as_alive(tmp_path, monkeypatch, capsys) -> None:
    belfry = _load_belfry()
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text("12345", encoding="utf-8")

    def deny_kill(_pid: int, _signal: int) -> None:
        raise PermissionError

    monkeypatch.setattr(belfry.os, "kill", deny_kill)

    assert belfry.pid_failure(tmp_path / "state" / "angelus.pid") is None
    assert "permission denied" in capsys.readouterr().err


def test_wedge_path_hits_down_and_escalates(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text(str(os.getpid()), encoding="utf-8")
    _write_source_fire(tmp_path, datetime.now(UTC) - timedelta(minutes=30))
    pings: list[str] = []
    calls: list[list[str]] = []

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 1
    assert pings == ["https://hc.example/down"]
    assert calls
    assert "wedged: last source fire" in " ".join(calls[0])


def test_happy_path_hits_success_without_escalation(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text(str(os.getpid()), encoding="utf-8")
    _write_source_fire(tmp_path, datetime.now(UTC))
    pings: list[str] = []
    calls: list[list[str]] = []

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]
    assert calls == []


# --- M2 slice 6: belfry independence end-to-end --------------------------
#
# The unit tests above stage a synthetic state directory; this test brings
# up a real angelus daemon subprocess, lets it write source_fires, SIGKILLs
# it, then drives a single belfry tick. The contract: belfry's pid_failure
# detects the dead PID file and escalates (down ping + notify-pat) BEFORE
# the wedge threshold can apply -- source_fires is deliberately recent so
# wedge_failure returns None and pid_failure is the discriminating axis.


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _write_daemon_lodging(root: Path) -> None:
    """Minimal lodging the daemon can load. cadence: 1s on the source so
    a source_fires row appears within ~2s and the wedge axis is taken off
    the table for the discrimination inversion (a row exists and is fresh,
    so wedge_failure returns None on its own)."""
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        "cadence: 1s\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
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
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )


def _spawn_daemon(root: Path) -> subprocess.Popen:
    """Spawn the angelus daemon as an OS subprocess so we can SIGKILL it
    later (the in-process integration harness elsewhere in this repo
    cannot be SIGKILLed without killing the test runner).

    start_new_session=True puts the daemon in its own process group, the
    same hardening pattern Risk 3 of the integration fell locked in for
    daemon-owned subprocess sites. The fresh session insulates the
    daemon's signal-handling and any of its children from the test
    runner's controlling terminal."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT) + (
        os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env else ""
    )
    env["ANGELUS_DRY_RUN"] = "1"
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from angelus.daemon import main\n"
            "from pathlib import Path\n"
            "import sys\n"
            "main(Path(sys.argv[1]))\n",
            str(root),
        ],
        env=env,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _read_pid_file(root: Path, timeout: float) -> int:
    """Wait for state/angelus.pid to appear AND contain a parseable PID."""
    pid_path = root / "state" / "angelus.pid"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return int(pid_path.read_text(encoding="utf-8").strip())
        except (FileNotFoundError, ValueError):
            time.sleep(0.05)
    raise AssertionError(f"daemon PID file never appeared at {pid_path}")


def _wait_for_source_fire(root: Path, timeout: float) -> str:
    """Wait until source_fires has at least one row and return its
    fired_at. Required for the discriminating inversion: with a fresh row
    on disk, wedge_failure returns None on its own, so pid_failure is the
    only axis that can produce the down-ping."""
    db = root / "state" / "angelus.sqlite3"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            try:
                row = conn.execute(
                    "SELECT max(fired_at) FROM source_fires"
                ).fetchone()
            finally:
                conn.close()
            if row and row[0]:
                return str(row[0])
        except sqlite3.Error:
            pass
        time.sleep(0.1)
    raise AssertionError(
        f"source_fires never gained a row within {timeout}s"
    )


def _kill_and_wait(proc: subprocess.Popen, pid: int) -> None:
    """SIGKILL the leader and confirm the kernel has reaped the pid. We
    use SIGKILL rather than SIGTERM because the daemon catches SIGTERM
    and shuts down cleanly (which unlinks the PID file as part of the
    finally clause). SIGKILL is uncatchable, leaving the PID file
    ORPHANED on disk with a now-dead PID -- the harder pid_failure path
    belfry must detect."""
    os.kill(pid, signal.SIGKILL)
    try:
        proc.wait(timeout=5.0)
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(
            f"daemon PID {pid} did not exit within 5s of SIGKILL"
        ) from exc
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and _alive(pid):
        time.sleep(0.02)
    if _alive(pid):
        raise AssertionError(f"daemon PID {pid} still alive after SIGKILL")


def test_belfry_independence_against_sigkilled_daemon(
    tmp_path, monkeypatch
) -> None:
    """Full integration: real angelus daemon subprocess -> source_fires
    populated -> SIGKILL -> single belfry tick on the orphaned state dir.
    Belfry's pid_failure must detect the dead-PID file, ping the down URL
    once, and call notify-pat with a payload identifying the dead-PID
    path -- all within the wedge threshold (default 600s).

    SIGKILL is the discriminating choice: it leaves the PID file on disk
    pointing at a dead PID (the daemon's clean shutdown handler never ran),
    which is the harder pid_failure path. The companion case -- SIGTERM
    cleanly removes the PID file, hitting the missing-PID-file arm of
    pid_failure -- is already pinned by a unit test above for the same
    arm under a synthetic state directory.

    Discrimination (verified locally on this code by inverting belfry's
    pid_failure to `return None` unconditionally): the
    `pings == ["https://hc.example/down"]` assertion below fires. With
    pid_failure short-circuited, wedge_failure runs and reads a fresh
    source_fire (we waited for one) so returns None too -- belfry sees
    clean and pings the SUCCESS URL instead of the DOWN URL.
    """
    _write_daemon_lodging(tmp_path)

    proc = _spawn_daemon(tmp_path)
    try:
        pid = _read_pid_file(tmp_path, timeout=15.0)
        assert _alive(pid), f"daemon PID {pid} was not alive on PID-file read"
        # Required for discrimination: source_fires must hold a fresh row
        # before SIGKILL, otherwise wedge_failure's "no rows" branch fires
        # under pid_failure inversion and the test can't distinguish the
        # two axes.
        fired_at = _wait_for_source_fire(tmp_path, timeout=15.0)
        _kill_and_wait(proc, pid)
    finally:
        if proc.poll() is None:
            os.kill(proc.pid, signal.SIGKILL)
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                pass

    # Confirm the orphaned state: PID file present, source_fires populated.
    assert (tmp_path / "state" / "angelus.pid").exists(), (
        "SIGKILL should leave the PID file on disk (clean-shutdown unlink "
        "only runs in the daemon's finally clause, which SIGKILL bypasses)"
    )
    assert (tmp_path / "state" / "angelus.sqlite3").exists()
    # fired_at is used: it is what makes wedge_failure return None under
    # pid_failure inversion. The mandatory-reader contract -- anything
    # written above (the wait) has a reader (this freshness assertion)
    # in the same test.
    fire_dt = datetime.fromisoformat(fired_at.replace("Z", "+00:00"))
    if fire_dt.tzinfo is None:
        fire_dt = fire_dt.replace(tzinfo=UTC)
    age = (datetime.now(UTC) - fire_dt).total_seconds()
    assert age < 60, (
        f"source_fire age {age:.1f}s exceeds wedge slack; the discrimination "
        f"inversion requires a fresh fire so wedge_failure returns None"
    )

    # Drive a single belfry tick with healthchecks.io and notify-pat both
    # mocked. notify-pat MUST NOT actually run (no real ping to Patrick's
    # phone); urlopen MUST NOT actually hit healthchecks.io.
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    pings: list[str] = []
    calls: list[list[str]] = []

    def fake_urlopen(url, timeout):  # type: ignore[no-untyped-def]
        pings.append(url)
        return _Response()

    def fake_run(args, check):  # type: ignore[no-untyped-def]
        calls.append(args)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(belfry.subprocess, "run", fake_run)

    started = time.monotonic()
    rc = belfry.main([str(tmp_path)])
    elapsed = time.monotonic() - started

    # Belfry returns 1 (DOWN, escalation succeeded).
    assert rc == 1, f"belfry exit code {rc}, expected 1 (DOWN escalation)"
    # Discriminating assertion #1 -- the down URL, not the success URL.
    # Under inverted pid_failure (always-None), this becomes the success
    # URL because wedge_failure also returns None on a fresh source_fire.
    assert pings == ["https://hc.example/down"], (
        f"expected single ping to DOWN URL; got {pings}"
    )
    # Discriminating assertion #2 -- notify-pat invoked exactly once with
    # a payload naming the dead-PID path. Under pid_failure inversion no
    # notify-pat call is made at all (the daemon looks healthy to belfry),
    # so this fails as `assert 0 == 1`.
    assert len(calls) == 1, f"notify-pat call count: {calls}"
    payload = " ".join(calls[0])
    assert f"dead: PID {pid} is not running" in payload, (
        f"notify-pat payload did not name the dead-PID failure: {payload!r}"
    )

    # The wedge threshold is the 600s default; the whole belfry tick is
    # well under it. This bound is the slice's "within an asserted window"
    # property -- belfry's response is bounded, not best-effort.
    wedge_threshold = belfry.DEFAULT_WEDGE_THRESHOLD_SEC
    assert elapsed < wedge_threshold, (
        f"belfry tick took {elapsed:.2f}s, exceeding wedge threshold "
        f"{wedge_threshold}s"
    )
    # And in practice the tick is interactive-fast (no network, mocked
    # subprocess); 5s is a generous CI ceiling.
    assert elapsed < 5.0, f"belfry tick took {elapsed:.2f}s (unexpectedly slow)"


# --- M2 slice 8: belfry liveness sentinel ---------------------------------
#
# Two units pinned here: the sentinel is touched on EVERY tick (success
# AND failure paths) and the path resolves to <root>/state/belfry-pinged-at
# by default with ANGELUS_BELFRY_SENTINEL_PATH overriding. The "every
# tick" choice is the slice's semantic decision -- liveness of belfry is
# distinct from health of angelus, so a tick that finds angelus down
# still proves belfry itself fired on schedule.


def _sentinel_mtime(path: Path) -> float:
    return path.stat().st_mtime


def test_sentinel_touched_on_success_tick(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text(
        str(os.getpid()), encoding="utf-8"
    )
    _write_source_fire(tmp_path, datetime.now(UTC))

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: subprocess.CompletedProcess(args, 0),
    )

    sentinel = tmp_path / "state" / "belfry-pinged-at"
    assert not sentinel.exists()
    before = time.time()
    assert belfry.main([str(tmp_path)]) == 0
    after = time.time()
    assert sentinel.exists(), "sentinel must be created on a successful tick"
    mtime = _sentinel_mtime(sentinel)
    # The mtime is bracketed by wall-clock before/after of the belfry tick.
    # Both bounds are necessary: a frozen mtime (e.g. write to wrong path)
    # would fail the lower bound; a future mtime would fail the upper.
    assert before - 1 <= mtime <= after + 1, (
        f"sentinel mtime {mtime} outside [{before}, {after}]"
    )


def test_sentinel_touched_on_failure_tick(tmp_path, monkeypatch) -> None:
    """Discrimination axis: 'every tick, not only success.' Belfry on a
    daemon-down tick still fires the sentinel. Under the inversion 'touch
    only on success path' (move the touch_sentinel call below the
    dead/wedge bail-out), this assertion fires."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    # Dead-PID setup: PID 999999 is not running on any sane test host.
    (tmp_path / "state" / "angelus.pid").write_text("999999", encoding="utf-8")

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: subprocess.CompletedProcess(args, 0),
    )

    sentinel = tmp_path / "state" / "belfry-pinged-at"
    before = time.time()
    rc = belfry.main([str(tmp_path)])
    after = time.time()
    assert rc == 1, "expected DOWN-escalation exit"
    assert sentinel.exists(), (
        "sentinel must be touched on the failure-tick path too -- belfry "
        "liveness is the question, not angelus health"
    )
    mtime = _sentinel_mtime(sentinel)
    assert before - 1 <= mtime <= after + 1


def test_sentinel_touch_updates_existing_mtime(tmp_path, monkeypatch) -> None:
    """A pre-existing sentinel file gets its mtime advanced. Without an
    explicit os.utime, Path.touch(exist_ok=True) on most filesystems does
    bump mtime, but the explicit utime guards against subtle differences
    on tmpfs / network filesystems and makes the contract local to the
    function."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text(
        str(os.getpid()), encoding="utf-8"
    )
    _write_source_fire(tmp_path, datetime.now(UTC))

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: subprocess.CompletedProcess(args, 0),
    )

    sentinel = tmp_path / "state" / "belfry-pinged-at"
    # Pre-create with an mtime well in the past so the after-tick mtime
    # must be visibly greater.
    sentinel.touch()
    old_time = time.time() - 3600
    os.utime(sentinel, (old_time, old_time))
    assert _sentinel_mtime(sentinel) == old_time

    assert belfry.main([str(tmp_path)]) == 0
    new_mtime = _sentinel_mtime(sentinel)
    assert new_mtime > old_time + 60, (
        f"sentinel mtime did not advance: {new_mtime} vs {old_time}"
    )


def test_sentinel_path_override_honored(tmp_path, monkeypatch) -> None:
    """ANGELUS_BELFRY_SENTINEL_PATH overrides the default. Discrimination:
    under the inversion 'ignore the env var, always use the default path,'
    the default file would be touched and the override would not -- both
    assertions below flip."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text(
        str(os.getpid()), encoding="utf-8"
    )
    _write_source_fire(tmp_path, datetime.now(UTC))

    override = tmp_path / "custom-belfry-sentinel"
    monkeypatch.setenv("ANGELUS_BELFRY_SENTINEL_PATH", str(override))

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 0
    assert override.exists(), "override path was not touched"
    assert not (tmp_path / "state" / "belfry-pinged-at").exists(), (
        "default path was touched despite override env var being set"
    )


def test_sentinel_touch_swallows_oserror(tmp_path, monkeypatch, capsys) -> None:
    """A broken sentinel filesystem must not stop belfry from running its
    angelus-health pings on this tick. Sentinel is a liveness signal, not
    a correctness gate -- failures are logged and swallowed.

    Under the inversion 'let OSError propagate from touch_sentinel,' the
    belfry tick would bail before pinging the SUCCESS URL, so the
    pings == [success_url] assertion below would fail."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text(
        str(os.getpid()), encoding="utf-8"
    )
    _write_source_fire(tmp_path, datetime.now(UTC))

    def boom(self: Path, *args, **kwargs):
        raise OSError("simulated EIO from test")

    monkeypatch.setattr(belfry.Path, "touch", boom)
    pings: list[str] = []
    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: subprocess.CompletedProcess(args, 0),
    )

    rc = belfry.main([str(tmp_path)])
    assert rc == 0
    assert pings == ["https://hc.example/success"]
    err = capsys.readouterr().err
    assert "failed to touch sentinel" in err
