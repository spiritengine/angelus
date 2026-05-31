from __future__ import annotations

import importlib.util
import os
import re
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


def _create_failure_tables(root: Path) -> None:
    """Add the dispatches and incidents tables (the schema belfry's
    failure-surface check reads) to an existing angelus.sqlite3. Mirrors
    the real columns the check selects; other columns are omitted as the
    read-only query never touches them."""
    db = root / "state" / "angelus.sqlite3"
    connection = sqlite3.connect(db)
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS dispatches (
                id INTEGER PRIMARY KEY,
                pipe TEXT NOT NULL,
                channel TEXT NOT NULL,
                finding_ids TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 1,
                last_error TEXT,
                dispatched_at TEXT,
                created_at TEXT,
                updated_at TEXT,
                source TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS incidents (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                type TEXT NOT NULL,
                entity TEXT NOT NULL,
                dedup_key TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                closed_at TEXT,
                status TEXT NOT NULL CHECK (status IN ('open', 'closed'))
            )
            """
        )
        connection.commit()
    finally:
        connection.close()


def _insert_dispatch(root: Path, status: str, channel: str = "push") -> None:
    db = root / "state" / "angelus.sqlite3"
    connection = sqlite3.connect(db)
    try:
        connection.execute(
            """
            INSERT INTO dispatches (pipe, channel, finding_ids, status, last_error)
            VALUES ('daily', ?, '[1]', ?, 'simulated transport failure')
            """,
            (channel, status),
        )
        connection.commit()
    finally:
        connection.close()


def _open_internal_incident(root: Path, source: str = "internal/dispatch") -> None:
    db = root / "state" / "angelus.sqlite3"
    connection = sqlite3.connect(db)
    try:
        connection.execute(
            """
            INSERT INTO incidents (source, type, entity, dedup_key, opened_at, status)
            VALUES (?, 'dispatch_failed', 'daily', ?, '2026-05-29T00:00:00Z', 'open')
            """,
            (source, f"{source}:dispatch_failed:daily"),
        )
        connection.commit()
    finally:
        connection.close()


def _set_urls(monkeypatch) -> None:
    monkeypatch.setenv("ANGELUS_BELFRY_SUCCESS_URL", "https://hc.example/success")
    monkeypatch.setenv("ANGELUS_BELFRY_DOWN_URL", "https://hc.example/down")


def test_belfry_imports_no_angelus_modules() -> None:
    for name in list(sys.modules):
        if name == "angelus" or name.startswith("angelus."):
            sys.modules.pop(name)

    _load_belfry()

    assert "angelus" not in sys.modules


def test_pid_dead_path_hits_down_and_escalates(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
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
        lambda args, check=False, **_: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 1
    assert pings == ["https://hc.example/down"]
    assert calls
    # notify-pat is the last subprocess call (systemctl restart precedes it)
    assert "dead: PID 999999 is not running" in " ".join(calls[-1])


def test_pid_missing_path_hits_down_and_escalates(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
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
        lambda args, check=False, **_: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 1
    assert pings == ["https://hc.example/down"]
    assert calls
    # notify-pat is the last subprocess call (systemctl restart precedes it)
    assert "missing PID file" in " ".join(calls[-1])


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
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
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
        lambda args, check=False, **_: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    assert belfry.main([str(tmp_path)]) == 1
    assert pings == ["https://hc.example/down"]
    assert calls
    # notify-pat is the last subprocess call (systemctl restart precedes it)
    assert "wedged: last source fire" in " ".join(calls[-1])


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
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]
    assert calls == []


def test_systemd_main_pid_injects_bus_env_when_absent(monkeypatch) -> None:
    # The drift check runs from cron, where XDG_RUNTIME_DIR/DBUS are unset and
    # `systemctl --user` can't reach the user bus. systemd_main_pid must inject
    # XDG_RUNTIME_DIR so the check isn't a silent no-op.
    belfry = _load_belfry()
    captured: dict = {}

    def fake_run(args, **kwargs):
        captured["args"] = args
        captured["env"] = kwargs.get("env")
        return subprocess.CompletedProcess(args, 0, stdout="2678722\n", stderr="")

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setattr(belfry.os, "getuid", lambda: 1000)
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.delenv("DBUS_SESSION_BUS_ADDRESS", raising=False)

    assert belfry.systemd_main_pid() == 2678722
    assert captured["args"][:3] == ["systemctl", "--user", "show"]
    assert captured["env"]["XDG_RUNTIME_DIR"] == "/run/user/1000"


def test_systemd_main_pid_preserves_existing_xdg(monkeypatch) -> None:
    belfry = _load_belfry()
    captured: dict = {}

    def fake_run(args, **kwargs):
        captured["env"] = kwargs.get("env")
        return subprocess.CompletedProcess(args, 0, stdout="5\n", stderr="")

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setenv("XDG_RUNTIME_DIR", "/run/user/already")

    assert belfry.systemd_main_pid() == 5
    assert captured["env"]["XDG_RUNTIME_DIR"] == "/run/user/already"


def test_systemd_main_pid_skips_xdg_when_dbus_present(monkeypatch) -> None:
    belfry = _load_belfry()
    captured: dict = {}

    def fake_run(args, **kwargs):
        captured["env"] = kwargs.get("env")
        return subprocess.CompletedProcess(args, 0, stdout="5\n", stderr="")

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.setenv("DBUS_SESSION_BUS_ADDRESS", "unix:path=/run/user/1000/bus")

    assert belfry.systemd_main_pid() == 5
    assert "XDG_RUNTIME_DIR" not in captured["env"]


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
    # Skip the post-restart recovery wait so the tick completes immediately.
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    pings: list[str] = []
    calls: list[list[str]] = []

    def fake_urlopen(url, timeout):  # type: ignore[no-untyped-def]
        pings.append(url)
        return _Response()

    def fake_run(args, check=False, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(list(args))
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
    # Discriminating assertion #2 -- notify-pat invoked with a payload naming
    # the dead-PID path. B12 adds a systemctl restart call before notify-pat,
    # so we filter to find the notify-pat call specifically.  Under
    # pid_failure inversion no notify-pat call is made at all (daemon looks
    # healthy), so this fails as `assert not notify_calls`.
    notify_calls = [c for c in calls if c and c[0] != "systemctl"]
    assert len(notify_calls) == 1, (
        f"expected exactly one notify-pat call; got {notify_calls} (all: {calls})"
    )
    payload = " ".join(notify_calls[0])
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
    # Drift axis off the table for the sentinel/wedge/happy paths -- these
    # tests isolate other axes; B17 drift has dedicated coverage below.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

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
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
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
        lambda args, check=False, **_: subprocess.CompletedProcess(args, 0),
    )
    # Drift axis off the table for the sentinel/wedge/happy paths -- these
    # tests isolate other axes; B17 drift has dedicated coverage below.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

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
    # Drift axis off the table for the sentinel/wedge/happy paths -- these
    # tests isolate other axes; B17 drift has dedicated coverage below.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

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
    # Drift axis off the table for the sentinel/wedge/happy paths -- these
    # tests isolate other axes; B17 drift has dedicated coverage below.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

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
    # Drift axis off the table for the sentinel/wedge/happy paths -- these
    # tests isolate other axes; B17 drift has dedicated coverage below.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    rc = belfry.main([str(tmp_path)])
    assert rc == 0
    assert pings == ["https://hc.example/success"]
    err = capsys.readouterr().err
    assert "failed to touch sentinel" in err


# --- B1 slice: belfry surfaces the daemon's self-reported failures --------
#
# A live, non-wedged daemon can still be failing its actual job: dispatches
# landing in status='failed', or internal/* incidents left open. Before B1
# belfry stayed green through exactly that (the 2026-05-29 incident). These
# tests pin the third, generic failure-surfacing axis: failed dispatches
# are edge-triggered off a last-seen-id watermark, open internal incidents
# are level-triggered off current state, and neither special-cases any
# channel.


def _alive_daemon_state(root: Path) -> None:
    """A daemon that pid_failure and wedge_failure both call healthy: live
    PID, a fresh source_fire, plus the dispatches/incidents tables the
    failure-surface check reads. Isolates the failure-surface axis as the
    only one that can move the result."""
    (root / "state").mkdir(exist_ok=True)
    (root / "state" / "angelus.pid").write_text(str(os.getpid()), encoding="utf-8")
    _write_source_fire(root, datetime.now(UTC))
    _create_failure_tables(root)


def _record_mocks(belfry, monkeypatch):
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
    # Take the B17 drift axis off the table: these tests isolate the
    # failure-surface axis, so fail the drift check open (None = "cannot
    # determine, no drift"). The drift axis has its own dedicated tests.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)
    return pings, calls


def test_failed_dispatch_since_last_tick_pings_down(tmp_path, monkeypatch) -> None:
    """A failed dispatch recorded after belfry's last tick drives DOWN, and
    re-firing belfry without a NEW failure does not re-alert (the watermark
    is edge-triggered, not level-triggered)."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_daemon_state(tmp_path)
    pings, calls = _record_mocks(belfry, monkeypatch)

    # Tick 1: clean -> SUCCESS, and establishes the watermark.
    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]

    # A failed dispatch lands after the bookmark.
    _insert_dispatch(tmp_path, status="failed")

    # Tick 2: the new failure surfaces -> DOWN, reason names it.
    assert belfry.main([str(tmp_path)]) == 1
    assert pings[-1] == "https://hc.example/down"
    payload = " ".join(calls[-1])
    assert "failed dispatch(es) since last tick" in payload
    assert "daily/push" in payload

    # Tick 3: no NEW failure -> back to SUCCESS (edge-triggered, the same
    # failed row is not re-alerted).
    assert belfry.main([str(tmp_path)]) == 0
    assert pings[-1] == "https://hc.example/success"


def test_no_failures_pings_success(tmp_path, monkeypatch) -> None:
    """With no failed dispatches and no open internal incidents, the new
    check is inert and belfry pings SUCCESS."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_daemon_state(tmp_path)
    # A SUCCEEDED dispatch must not be mistaken for a failure.
    _insert_dispatch(tmp_path, status="sent")
    pings, calls = _record_mocks(belfry, monkeypatch)

    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]
    assert calls == []


def test_open_internal_incident_pings_down_every_tick(tmp_path, monkeypatch) -> None:
    """An open internal/* incident is level-triggered: belfry stays red on
    each tick until it closes, even with no failed dispatch present and on
    the very first tick (no watermark needed for this axis)."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_daemon_state(tmp_path)
    _open_internal_incident(tmp_path, source="internal/dispatch")
    pings, calls = _record_mocks(belfry, monkeypatch)

    assert belfry.main([str(tmp_path)]) == 1
    assert pings[-1] == "https://hc.example/down"
    payload = " ".join(calls[-1])
    assert "open internal finding(s)" in payload
    assert "internal/dispatch" in payload

    # Still open on the next tick -> still DOWN (not silenced by a bookmark).
    assert belfry.main([str(tmp_path)]) == 1
    assert pings[-1] == "https://hc.example/down"


def test_closed_internal_incident_does_not_ping_down(tmp_path, monkeypatch) -> None:
    """A closed internal incident must not keep belfry red -- only OPEN
    internal incidents surface."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_daemon_state(tmp_path)
    _open_internal_incident(tmp_path, source="internal/dispatch")
    db = tmp_path / "state" / "angelus.sqlite3"
    conn = sqlite3.connect(db)
    try:
        conn.execute("UPDATE incidents SET status = 'closed'")
        conn.commit()
    finally:
        conn.close()
    pings, _calls = _record_mocks(belfry, monkeypatch)

    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]


def test_first_run_does_not_replay_failed_history(tmp_path, monkeypatch) -> None:
    """On belfry's first tick (no watermark file yet) a pre-existing failed
    dispatch is NOT replayed as DOWN -- the first tick establishes the
    bookmark. Otherwise a fresh belfry against a populated db would flood
    on every historical failure."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_daemon_state(tmp_path)
    _insert_dispatch(tmp_path, status="failed")  # predates belfry's first tick
    assert not (tmp_path / "state" / "belfry-failcheck-at").exists()
    pings, _calls = _record_mocks(belfry, monkeypatch)

    # First tick: establish watermark, do not replay the old failure.
    assert belfry.main([str(tmp_path)]) == 0
    assert pings[-1] == "https://hc.example/success"
    assert (tmp_path / "state" / "belfry-failcheck-at").exists()

    # Second tick: still nothing new -> SUCCESS.
    assert belfry.main([str(tmp_path)]) == 0
    assert pings[-1] == "https://hc.example/success"


def test_failcheck_path_override_honored(tmp_path, monkeypatch) -> None:
    """ANGELUS_BELFRY_FAILCHECK_PATH overrides the default watermark
    location, mirroring the sentinel override pattern."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_daemon_state(tmp_path)
    override = tmp_path / "custom-failcheck"
    monkeypatch.setenv("ANGELUS_BELFRY_FAILCHECK_PATH", str(override))
    pings, _calls = _record_mocks(belfry, monkeypatch)

    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]
    assert override.exists(), "override watermark path was not written"
    assert not (tmp_path / "state" / "belfry-failcheck-at").exists(), (
        "default watermark path written despite override env var being set"
    )


def test_failure_surface_fails_open_on_missing_tables(tmp_path, monkeypatch) -> None:
    """If the dispatches/incidents tables are absent (schema-incomplete or
    pre-migration db), failure_surface swallows the sqlite error and
    returns None rather than manufacturing a false DOWN. The pid/wedge
    axes remain the liveness backstop."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text(str(os.getpid()), encoding="utf-8")
    _write_source_fire(tmp_path, datetime.now(UTC))  # source_fires only, no dispatches
    pings, calls = _record_mocks(belfry, monkeypatch)

    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]
    assert calls == []


# --- B17 slice: drift detector -------------------------------------------
#
# A daemon can be alive (pid healthy, sources firing, no failed dispatches)
# and STILL be the wrong instance: hand-launched outside its systemd unit,
# detached from EnvironmentFile and Restart supervision. That is the exact
# 2026-05-29 incident -- the process looked alive while running off-unit and
# silently lost ANGELUS_EMAIL_TO. belfry is the out-of-band place to assert
# "the live daemon IS the systemd-managed instance." systemd_main_pid() is
# the seam: the tests below drive it (and the real subprocess plumbing) so
# the check never depends on whatever unit happens to run on the test host.


def _alive_for_drift(root: Path) -> None:
    """Daemon state that pid_failure and wedge_failure both call healthy:
    a live PID (this test process) and a fresh source_fire. Leaves the
    failure-surface tables absent so that axis fails open -- the drift axis
    is the only thing the stubbed systemd_main_pid can move."""
    (root / "state").mkdir(exist_ok=True)
    (root / "state" / "angelus.pid").write_text(
        str(os.getpid()), encoding="utf-8"
    )
    _write_source_fire(root, datetime.now(UTC))


def test_drift_pids_match_no_drift(tmp_path, monkeypatch) -> None:
    """When systemd's MainPID equals the live daemon pid, the daemon IS the
    managed instance: no drift, belfry pings SUCCESS."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_for_drift(tmp_path)
    pings, calls = _record_mocks(belfry, monkeypatch)
    # Override the _record_mocks fail-open default: systemd reports the same
    # pid the pid file holds.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: os.getpid())

    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]
    assert calls == []


def test_drift_pids_differ_pings_down(tmp_path, monkeypatch) -> None:
    """A live daemon whose pid does not match systemd's MainPID is an
    instance running outside its unit -> DOWN, reason names the drift and
    both pids."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_for_drift(tmp_path)
    pings, calls = _record_mocks(belfry, monkeypatch)
    other_pid = os.getpid() + 1
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: other_pid)

    assert belfry.main([str(tmp_path)]) == 1
    assert pings[-1] == "https://hc.example/down"
    payload = " ".join(calls[-1])
    assert "drift" in payload
    assert str(os.getpid()) in payload
    assert str(other_pid) in payload


def test_drift_unit_inactive_with_live_pid_pings_down(
    tmp_path, monkeypatch
) -> None:
    """systemd reports MainPID 0 (unit inactive) while a daemon pid is alive
    -> a daemon is running with no supervising unit at all -> DOWN."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_for_drift(tmp_path)
    pings, calls = _record_mocks(belfry, monkeypatch)
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: 0)

    assert belfry.main([str(tmp_path)]) == 1
    assert pings[-1] == "https://hc.example/down"
    payload = " ".join(calls[-1])
    assert "drift" in payload
    assert "inactive" in payload


def test_drift_fails_open_when_systemctl_unavailable(
    tmp_path, monkeypatch
) -> None:
    """If systemd's MainPID cannot be determined (systemd_main_pid -> None),
    belfry must not manufacture a false DOWN -- it pings SUCCESS. This is the
    documented fail-open contract for an uninterrogatable systemd."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_for_drift(tmp_path)
    pings, calls = _record_mocks(belfry, monkeypatch)
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]
    assert calls == []


def test_drift_failure_missing_pid_file_returns_none(tmp_path) -> None:
    """drift_failure with no pid file returns None -- pid_failure owns the
    missing/dead-pid case, so drift has nothing to compare and fails open."""
    belfry = _load_belfry()
    assert belfry.drift_failure(tmp_path / "state" / "angelus.pid") is None


def test_systemd_main_pid_parses_value(monkeypatch) -> None:
    """systemd_main_pid shells out to systemctl and returns the integer
    MainPID on a clean exit."""
    belfry = _load_belfry()
    captured: list[list[str]] = []

    def fake_run(args, check, capture_output, text, timeout, env=None):
        captured.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="12345\n", stderr="")

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    assert belfry.systemd_main_pid() == 12345
    # The invocation is the dependency-free systemctl --user query.
    assert captured[0][:5] == [
        "systemctl",
        "--user",
        "show",
        "-p",
        "MainPID",
    ]
    assert captured[0][-1] == "angelus"


def test_systemd_main_pid_unit_override(monkeypatch) -> None:
    """ANGELUS_SYSTEMD_UNIT names the unit belfry interrogates."""
    belfry = _load_belfry()
    monkeypatch.setenv("ANGELUS_SYSTEMD_UNIT", "angelus-staging")
    captured: list[list[str]] = []

    def fake_run(args, check, capture_output, text, timeout, env=None):
        captured.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="7\n", stderr="")

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    assert belfry.systemd_main_pid() == 7
    assert captured[0][-1] == "angelus-staging"


def test_systemd_main_pid_fails_open_when_systemctl_missing(
    monkeypatch, capsys
) -> None:
    """No systemctl binary (FileNotFoundError) -> None (fail open), logged."""
    belfry = _load_belfry()

    def boom(args, check, capture_output, text, timeout, env=None):
        raise FileNotFoundError("systemctl")

    monkeypatch.setattr(belfry.subprocess, "run", boom)
    assert belfry.systemd_main_pid() is None
    assert "drift check cannot run systemctl" in capsys.readouterr().err


def test_systemd_main_pid_fails_open_on_nonzero_exit(monkeypatch) -> None:
    """A non-zero systemctl exit (no user bus, unknown unit) -> None."""
    belfry = _load_belfry()

    def fake_run(args, check, capture_output, text, timeout, env=None):
        return subprocess.CompletedProcess(
            args, 1, stdout="", stderr="Failed to connect to bus"
        )

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    assert belfry.systemd_main_pid() is None


def test_systemd_main_pid_fails_open_on_unparseable(monkeypatch) -> None:
    """Unparseable MainPID output -> None (fail open), never a crash."""
    belfry = _load_belfry()

    def fake_run(args, check, capture_output, text, timeout, env=None):
        return subprocess.CompletedProcess(
            args, 0, stdout="not-a-number\n", stderr=""
        )

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    assert belfry.systemd_main_pid() is None


def test_systemd_main_pid_fails_open_on_timeout(monkeypatch) -> None:
    """A systemctl timeout -> None (fail open)."""
    belfry = _load_belfry()

    def boom(args, check, capture_output, text, timeout, env=None):
        raise subprocess.TimeoutExpired(cmd=args, timeout=timeout)

    monkeypatch.setattr(belfry.subprocess, "run", boom)
    assert belfry.systemd_main_pid() is None


# --- B22 slice: belfry log lines are timestamped -------------------------
#
# Every line belfry writes to belfry.log (stdout/stderr under the cron
# redirect) is prefixed with an ISO8601 UTC timestamp via plain strftime
# (belfry stays dependency-free -- no angelus clock seam). Before B22 a
# belfry.log line had no time anchor, so a postmortem could not place a
# ping in the incident timeline.


_TS_LINE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z) (?P<msg>.+)$")


def _assert_all_lines_timestamped(text: str) -> int:
    """Assert every non-blank line starts with a parseable ISO8601 UTC
    stamp followed by a non-empty message. Returns the line count so the
    caller can assert lines were actually emitted."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    for line in lines:
        match = _TS_LINE.match(line)
        assert match, f"log line not timestamped: {line!r}"
        # The stamp is a real timestamp, not just digits in the right shape.
        datetime.strptime(match.group("ts"), "%Y-%m-%dT%H:%M:%SZ")
        assert match.group("msg").strip(), f"empty message after stamp: {line!r}"
    return len(lines)


def test_log_lines_timestamped_on_down_tick(tmp_path, monkeypatch, capsys) -> None:
    """A DOWN tick emits the ping line and the 'DOWN: <reason>' line, both
    timestamped. Dead-PID is the simplest DOWN trigger."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text("999999", encoding="utf-8")
    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check=False, **_: subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 1
    captured = capsys.readouterr()
    combined = captured.out + "\n" + captured.err
    count = _assert_all_lines_timestamped(combined)
    assert count >= 1
    # The reason line is present and carries the dead-PID detail after its
    # timestamp -- proving the prefix did not eat the message.
    assert "angelus belfry: DOWN: dead: PID 999999" in captured.err


def test_log_lines_timestamped_on_success_tick(
    tmp_path, monkeypatch, capsys
) -> None:
    """A clean tick's stdout (the 'ok' and ping lines) is timestamped too --
    not only the failure path."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_for_drift(tmp_path)
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
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    assert belfry.main([str(tmp_path)]) == 0
    captured = capsys.readouterr()
    count = _assert_all_lines_timestamped(captured.out)
    assert count >= 1
    assert "angelus belfry: ok" in captured.out


# --- B8 slice: belfry notify() goes over push, not email -----------------
#
# belfry's alerting must not share a transport with the daemon -- email
# silently breaking (2026-05-29 incident) is exactly the failure mode
# belfry must detect. notify() shells out to notify-pat (push) instead of
# patbot-email. ANGELUS_EMAIL_TO is irrelevant to belfry's own alert path.


def test_notify_push_argv_shape(monkeypatch) -> None:
    """notify() calls [command, message] -- the notify-pat interface, not the
    old patbot-email shape [command, 'send', to, subject, '--body', body]."""
    belfry = _load_belfry()
    captured: list[list[str]] = []

    def fake_run(args, check):
        captured.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.delenv("ANGELUS_EMAIL_TO", raising=False)

    assert belfry.notify("daemon dead") is True
    assert len(captured) == 1
    assert captured[0] == ["notify-pat", "angelus belfry alert: daemon dead"]


def test_notify_default_command_is_notify_pat(monkeypatch) -> None:
    """Without ANGELUS_BELFRY_NOTIFY_COMMAND set, the command is notify-pat."""
    belfry = _load_belfry()
    captured: list[list[str]] = []

    def fake_run(args, check):
        captured.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.delenv("ANGELUS_BELFRY_NOTIFY_COMMAND", raising=False)

    belfry.notify("test reason")
    assert captured[0][0] == "notify-pat"


def test_notify_command_override(monkeypatch) -> None:
    """ANGELUS_BELFRY_NOTIFY_COMMAND replaces the default."""
    belfry = _load_belfry()
    captured: list[list[str]] = []

    def fake_run(args, check):
        captured.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setenv("ANGELUS_BELFRY_NOTIFY_COMMAND", "/usr/local/bin/custom-push")

    belfry.notify("test reason")
    assert captured[0][0] == "/usr/local/bin/custom-push"


def test_notify_does_not_require_email_to(monkeypatch) -> None:
    """notify() must not skip or fail when ANGELUS_EMAIL_TO is unset."""
    belfry = _load_belfry()
    captured: list[list[str]] = []

    def fake_run(args, check):
        captured.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.delenv("ANGELUS_EMAIL_TO", raising=False)

    result = belfry.notify("daemon dead")
    assert result is True
    assert len(captured) == 1


def test_notify_oserror_logs_and_returns_false(monkeypatch, capsys) -> None:
    """If the push command fails to start, notify() logs via log_err and
    returns False -- same error-handling shape as before B8."""
    belfry = _load_belfry()

    def boom(args, check):
        raise OSError("no such file")

    monkeypatch.setattr(belfry.subprocess, "run", boom)

    assert belfry.notify("test") is False
    err = capsys.readouterr().err
    assert "failed to start" in err


def test_notify_nonzero_exit_logs_and_returns_false(monkeypatch, capsys) -> None:
    """A non-zero exit from the push command is logged and returns False."""
    belfry = _load_belfry()

    def fake_run(args, check):
        return subprocess.CompletedProcess(args, 1)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)

    assert belfry.notify("test") is False
    err = capsys.readouterr().err
    assert "exited 1" in err


# --- B12 slice: belfry restart-fixer ----------------------------------------
#
# When the daemon is ABSENT (pid_failure or wedge_failure), belfry attempts a
# loop-guarded restart via `systemctl --user restart`.  The key invariants:
#   - restart fires on absence, NOT on failure_surface or drift (daemon is UP).
#   - the loop guard (at most N restarts per rolling window) prevents crash-loop
#     restart-looping; on exceed it writes a needs-sre sentinel and pages.
#   - a successful restart is still alert-worthy: DOWN ping + notify() fire.
#   - every attempt is recorded in the fixers audit log.
#
# The "guard stops the loop" test (test_loop_guard_blocks_after_n_restarts) is
# the safety-critical one: it proves a crash-loop hits N and STOPS, not just
# that a single restart works.
#
# Note: a real end-to-end crash-loop live test requires the fault-injection path
# (B28, not yet landed).  The live guarantee here rests on these unit tests
# mocking systemctl.


def _absence_restart_mock(monkeypatch, belfry, tmp_path):
    """Shared setup: dead PID, mocked subprocess.run and urlopen, no sleep.

    Returns (pings, all_calls) where all_calls captures every subprocess.run
    invocation; callers filter by args[0] to separate systemctl from notify-pat.
    """
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    (tmp_path / "state").mkdir(exist_ok=True)
    (tmp_path / "state" / "angelus.pid").write_text("999999", encoding="utf-8")
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)
    pings: list[str] = []
    all_calls: list[list[str]] = []

    def fake_run(args, check=False, **kwargs):
        all_calls.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    return pings, all_calls


def test_restart_fires_on_pid_dead(tmp_path, monkeypatch) -> None:
    """Restart is attempted when pid_failure fires (daemon dead: absent)."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    pings, all_calls = _absence_restart_mock(monkeypatch, belfry, tmp_path)

    belfry.main([str(tmp_path)])

    restart_calls = [c for c in all_calls if c[:3] == ["systemctl", "--user", "restart"]]
    assert len(restart_calls) == 1, f"expected one restart; got {all_calls}"
    assert restart_calls[0][-1] == belfry.systemd_unit()


def test_restart_fires_on_wedge(tmp_path, monkeypatch) -> None:
    """Restart is attempted when wedge_failure fires (daemon alive but wedged: absent)."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    (tmp_path / "state").mkdir()
    # Daemon alive (live pid) but stale source_fire → wedge.
    (tmp_path / "state" / "angelus.pid").write_text(
        str(os.getpid()), encoding="utf-8"
    )
    _write_source_fire(tmp_path, datetime.now(UTC) - timedelta(minutes=30))
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    all_calls: list[list[str]] = []

    def fake_run(args, check=False, **kwargs):
        all_calls.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setattr(
        belfry.urllib.request, "urlopen",
        lambda url, timeout: _Response(),
    )

    belfry.main([str(tmp_path)])

    restart_calls = [c for c in all_calls if c[:3] == ["systemctl", "--user", "restart"]]
    assert len(restart_calls) == 1, (
        f"expected one restart on wedge; got {all_calls}"
    )


def test_no_restart_on_failure_surface_only(tmp_path, monkeypatch) -> None:
    """failure_surface alone (daemon UP, self-reporting errors) must NOT trigger
    a restart.  Restarting a live self-reporting daemon masks the root cause."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    # Alive daemon with dispatches table so failure_surface can fire.
    _alive_daemon_state(tmp_path)
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    all_calls: list[list[str]] = []

    def fake_run(args, check=False, **kwargs):
        all_calls.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    pings: list[str] = []
    monkeypatch.setattr(
        belfry.urllib.request, "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )

    # Tick 1: establishes the failcheck watermark (no failure_surface yet).
    belfry.main([str(tmp_path)])
    _insert_dispatch(tmp_path, status="failed")  # new failure since last tick

    all_calls.clear()
    pings.clear()
    # Tick 2: failure_surface fires → DOWN, but no restart.
    belfry.main([str(tmp_path)])

    restart_calls = [c for c in all_calls if c[:3] == ["systemctl", "--user", "restart"]]
    assert restart_calls == [], (
        f"restart must NOT fire on failure_surface-only; got {restart_calls}"
    )
    assert pings[-1] == "https://hc.example/down"


def test_no_restart_on_drift_only(tmp_path, monkeypatch) -> None:
    """drift_failure alone (daemon alive but mis-launched) must NOT trigger a
    restart.  That is alert-only; mis-launch is a human/SRE fix."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    _alive_for_drift(tmp_path)
    # Drift: live pid != systemd MainPID.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: os.getpid() + 9999)

    all_calls: list[list[str]] = []

    def fake_run(args, check=False, **kwargs):
        all_calls.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setattr(
        belfry.urllib.request, "urlopen",
        lambda url, timeout: _Response(),
    )

    rc = belfry.main([str(tmp_path)])
    assert rc in (1, 2)

    restart_calls = [c for c in all_calls if c[:3] == ["systemctl", "--user", "restart"]]
    assert restart_calls == [], (
        f"restart must NOT fire on drift-only; got {restart_calls}"
    )


def test_restart_daemon_success_path(monkeypatch, capsys) -> None:
    """restart_daemon() returns True and logs when systemctl exits 0."""
    belfry = _load_belfry()
    captured: dict = {}

    def fake_run(args, check=False, **kwargs):
        captured["args"] = list(args)
        captured["env"] = kwargs.get("env")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)

    assert belfry.restart_daemon() is True
    assert captured["args"][:3] == ["systemctl", "--user", "restart"]
    assert captured["env"] is not None  # _user_bus_env was threaded through
    assert "restart" in capsys.readouterr().out


def test_restart_daemon_failure_oserror(monkeypatch, capsys) -> None:
    """restart_daemon() returns False and logs on OSError (e.g. no systemctl binary)."""
    belfry = _load_belfry()

    def boom(args, check=False, **kwargs):
        raise OSError("no systemctl")

    monkeypatch.setattr(belfry.subprocess, "run", boom)

    assert belfry.restart_daemon() is False
    assert "restart failed" in capsys.readouterr().err


def test_restart_daemon_failure_nonzero(monkeypatch, capsys) -> None:
    """restart_daemon() returns False and logs on nonzero systemctl exit."""
    belfry = _load_belfry()

    def fake_run(args, check=False, **kwargs):
        return subprocess.CompletedProcess(args, 1, stdout="", stderr="unit not found")

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)

    assert belfry.restart_daemon() is False
    assert "restart systemctl failed" in capsys.readouterr().err


def test_loop_guard_blocks_after_n_restarts(tmp_path, monkeypatch) -> None:
    """THE key loop-guard test.

    Simulates N+1 consecutive absence ticks with systemctl mocked to 'succeed'
    but the daemon staying dead (verify_recovery always returns False because
    PID 999999 is still dead in the pid file).

    Ticks 1 through N each attempt a restart (systemctl is called).
    Tick N+1 is BLOCKED by the loop guard: no systemctl restart fires, the
    needs-sre sentinel is written, and a loud page (notify-pat) fires.

    This is the proof that a crash-loop hits N and STOPS, not merely that a
    single restart works.
    """
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    monkeypatch.setenv("ANGELUS_BELFRY_MAX_RESTARTS", "3")
    monkeypatch.setenv("ANGELUS_BELFRY_RESTART_WINDOW_SEC", "1800")
    (tmp_path / "state").mkdir()
    # Dead PID that stays dead no matter how many times systemctl 'restarts' it.
    (tmp_path / "state" / "angelus.pid").write_text("999999", encoding="utf-8")
    # Dead process short-circuits before drift_failure; but stub it anyway.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    systemctl_restart_calls: list[list[str]] = []
    notify_calls: list[list[str]] = []

    def fake_run(args, check=False, **kwargs):
        a = list(args)
        if a[:3] == ["systemctl", "--user", "restart"]:
            systemctl_restart_calls.append(a)
        else:
            notify_calls.append(a)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: _Response(),
    )

    # Ticks 1, 2, 3: each must attempt exactly one restart.
    for tick in range(1, 4):
        rc = belfry.main([str(tmp_path)])
        assert rc in (1, 2), f"tick {tick}: expected DOWN (got {rc})"
        assert len(systemctl_restart_calls) == tick, (
            f"tick {tick}: expected {tick} restart(s); got "
            f"{len(systemctl_restart_calls)}"
        )

    restarts_before_tick4 = len(systemctl_restart_calls)

    # Tick 4: loop guard must block the restart.
    rc = belfry.main([str(tmp_path)])
    assert rc in (1, 2), "tick 4: expected DOWN"

    # No new systemctl restart call — the guard stopped it.
    assert len(systemctl_restart_calls) == restarts_before_tick4, (
        f"tick 4: loop guard FAILED to block restart; "
        f"systemctl calls: {systemctl_restart_calls}"
    )

    # Loud page still fires on loop-exceed.
    assert notify_calls, "tick 4: notify-pat must fire even when loop guard blocks"
    escalation_payload = " ".join(notify_calls[-1])
    assert "crash-loop" in escalation_payload, (
        f"tick 4: escalation message missing 'crash-loop': {escalation_payload!r}"
    )

    # needs-sre sentinel written with crash-loop content.
    nsre = belfry.needs_sre_path(tmp_path / "state")
    assert nsre.exists(), "needs-sre sentinel not written on loop-exceed"
    assert "crash-loop" in nsre.read_text(encoding="utf-8")

    # Audit log: 3 restart entries + 1 escalate entry.
    flog = belfry.fixers_log_path(tmp_path / "state")
    assert flog.exists(), "fixers.log not written"
    lines = [l for l in flog.read_text(encoding="utf-8").splitlines() if l.strip()]
    restart_lines = [l for l in lines if "action=restart" in l]
    escalate_lines = [l for l in lines if "action=escalate" in l]
    assert len(restart_lines) == 3, (
        f"expected 3 restart log entries; got {restart_lines}"
    )
    assert len(escalate_lines) == 1, (
        f"expected 1 escalate entry; got {escalate_lines}"
    )


def test_loop_guard_window_pruning(tmp_path, monkeypatch) -> None:
    """Restart timestamps older than the window do not count toward N.

    Pre-populate the restart log with N entries all outside the rolling window.
    The next tick must attempt a restart (not be blocked), proving old entries
    are pruned before the count check.
    """
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    monkeypatch.setenv("ANGELUS_BELFRY_MAX_RESTARTS", "3")
    monkeypatch.setenv("ANGELUS_BELFRY_RESTART_WINDOW_SEC", "1800")
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text("999999", encoding="utf-8")
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    # Pre-populate with 3 timestamps all outside the 1800s window (2 hours ago).
    rlog = belfry.restart_log_path(tmp_path / "state")
    old_ts = time.time() - 7200
    belfry.write_restart_log(rlog, [old_ts, old_ts - 10, old_ts - 20])

    systemctl_calls: list[list[str]] = []

    def fake_run(args, check=False, **kwargs):
        systemctl_calls.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setattr(
        belfry.urllib.request, "urlopen",
        lambda url, timeout: _Response(),
    )

    belfry.main([str(tmp_path)])

    restart_calls = [
        c for c in systemctl_calls if c[:3] == ["systemctl", "--user", "restart"]
    ]
    assert len(restart_calls) == 1, (
        f"expected restart despite pre-populated old timestamps; "
        f"got {systemctl_calls}"
    )


def test_audit_log_restart_appends_line(tmp_path, monkeypatch) -> None:
    """A restart attempt appends one structured line to state/fixers.log."""
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    pings, all_calls = _absence_restart_mock(monkeypatch, belfry, tmp_path)

    belfry.main([str(tmp_path)])

    flog = belfry.fixers_log_path(tmp_path / "state")
    assert flog.exists(), "fixers.log was not created"
    text = flog.read_text(encoding="utf-8")
    lines = [l for l in text.splitlines() if l.strip()]
    assert lines, "fixers.log is empty"
    line = lines[0]
    # Shape: "ISO8601 actor=belfry action=restart reason=... outcome=..."
    assert "actor=belfry" in line
    assert "action=restart" in line
    assert "outcome=" in line
    # Timestamp is parseable.
    ts_part = line.split()[0]
    datetime.strptime(ts_part, "%Y-%m-%dT%H:%M:%SZ")


def test_outbound_fires_on_restart(tmp_path, monkeypatch) -> None:
    """A successful auto-restart still fires the DOWN ping and notify().

    A real problem occurred (daemon was absent); the next clean tick (daemon
    healthy again) pings SUCCESS as normal.  Belfry must never go silent on
    an absence even if the restart appeared to succeed.
    """
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    pings, all_calls = _absence_restart_mock(monkeypatch, belfry, tmp_path)

    rc = belfry.main([str(tmp_path)])

    # DOWN path fired.
    assert rc in (1, 2)
    assert "https://hc.example/down" in pings, f"DOWN ping missing; pings={pings}"

    # notify-pat called with the restart outcome in the message.
    notify_calls = [c for c in all_calls if c[:3] != ["systemctl", "--user", "restart"]]
    assert len(notify_calls) == 1, (
        f"expected one notify-pat call; got {notify_calls}"
    )
    payload = " ".join(notify_calls[0])
    assert "auto-restart" in payload, (
        f"notify-pat payload missing 'auto-restart': {payload!r}"
    )


def test_restart_withheld_when_log_persist_fails(
    tmp_path, monkeypatch, capsys
) -> None:
    """If write_restart_log fails (disk/permissions error), the restart is NOT
    attempted.  The guard must err toward NOT restarting when its own state
    cannot be durably recorded — an un-guarded restart loop is the failure
    mode the guard exists to prevent.

    A DOWN ping and notify() still fire: the daemon is genuinely absent,
    that is a real alert regardless of the guard's internal state.
    """
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text("999999", encoding="utf-8")
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    # write_restart_log returns False — simulates disk-full / permission error.
    monkeypatch.setattr(belfry, "write_restart_log", lambda path, ts: False)

    restart_attempted: list[bool] = []
    monkeypatch.setattr(
        belfry,
        "restart_daemon",
        lambda: restart_attempted.append(True) or True,
    )

    pings: list[str] = []
    notify_calls: list[list[str]] = []

    def fake_run(args, check=False, **kwargs):
        notify_calls.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )

    rc = belfry.main([str(tmp_path)])

    # Still alerts DOWN (daemon is genuinely absent).
    assert rc in (1, 2), f"expected DOWN; got {rc}"
    assert "https://hc.example/down" in pings, f"DOWN ping missing; pings={pings}"

    # restart_daemon must NOT have been called when guard cannot persist.
    assert not restart_attempted, (
        "restart_daemon must NOT be called when write_restart_log fails"
    )

    # The reason text mentions the withheld restart so notify-pat carries it.
    all_payloads = " ".join(" ".join(c) for c in notify_calls)
    assert "withheld" in all_payloads, (
        f"notify payload must mention 'withheld': {all_payloads!r}"
    )


def test_no_restart_on_wedge_and_drift(tmp_path, monkeypatch) -> None:
    """When the daemon is BOTH wedged (absence reason) AND drifted, the
    restart must be withheld.  Drift means the daemon is alive but running
    outside its systemd unit — `systemctl restart` would start a SECOND
    instance alongside the mis-launched one, violating the single-writer
    invariant.  Drift is always alert-only, never auto-restarted.

    Discrimination: with drift suppression removed (just call
    _autoremediate_absence unconditionally), the restart_calls list below
    gains one entry and the assertion fails.
    """
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    (tmp_path / "state").mkdir()
    # Daemon alive (live pid) but stale source_fire → wedge (absence reason).
    (tmp_path / "state" / "angelus.pid").write_text(
        str(os.getpid()), encoding="utf-8"
    )
    _write_source_fire(tmp_path, datetime.now(UTC) - timedelta(minutes=30))
    # Also drifted: live pid != systemd MainPID (other reason).
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: os.getpid() + 9999)

    restart_calls: list[list[str]] = []
    notify_calls: list[list[str]] = []

    def fake_run(args, check=False, **kwargs):
        a = list(args)
        if a[:3] == ["systemctl", "--user", "restart"]:
            restart_calls.append(a)
        else:
            notify_calls.append(a)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    pings: list[str] = []
    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )

    rc = belfry.main([str(tmp_path)])

    # Still alerts DOWN.
    assert rc in (1, 2), f"expected DOWN; got {rc}"
    assert "https://hc.example/down" in pings, f"DOWN ping missing; pings={pings}"

    # No restart (drift suppressed it).
    assert restart_calls == [], (
        f"restart must NOT fire when drift is also present; got {restart_calls}"
    )

    # Reason text carries the drift-suppression explanation.
    all_payloads = " ".join(" ".join(c) for c in notify_calls)
    assert "drift" in all_payloads, (
        f"notify payload must mention 'drift': {all_payloads!r}"
    )
    assert "withheld" in all_payloads, (
        f"notify payload must mention 'withheld': {all_payloads!r}"
    )


def test_failed_restart_counts_toward_guard(tmp_path, monkeypatch) -> None:
    """A FAILED systemctl restart still counts toward the loop guard.

    The guard records the attempt BEFORE calling restart_daemon(), so even a
    restart that systemctl rejects accumulates toward N.  This pins that
    invariant: if only successful restarts counted, a daemon systemctl cannot
    restart would never accumulate toward the limit, defeating the guard
    entirely for that failure class.

    Setup: restart_daemon() always returns False (systemctl nonzero).  After
    N ticks (all failed restarts), tick N+1 must be BLOCKED by the guard with
    the needs-sre sentinel written.
    """
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    monkeypatch.setenv("ANGELUS_BELFRY_MAX_RESTARTS", "3")
    monkeypatch.setenv("ANGELUS_BELFRY_RESTART_WINDOW_SEC", "1800")
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text("999999", encoding="utf-8")
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)

    # restart_daemon always fails — systemctl returns nonzero / unit not found.
    monkeypatch.setattr(belfry, "restart_daemon", lambda: False)

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: _Response(),
    )
    notify_calls: list[list[str]] = []

    def fake_run(args, check=False, **kwargs):
        notify_calls.append(list(args))
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)

    # Ticks 1, 2, 3: all attempt restart (which fails), all record a timestamp.
    rlog_path = belfry.restart_log_path(tmp_path / "state")
    for tick in range(1, 4):
        rc = belfry.main([str(tmp_path)])
        assert rc in (1, 2), f"tick {tick}: expected DOWN (got {rc})"
        # Verify N timestamps accumulated so far (all within window).
        timestamps = belfry.read_restart_log(rlog_path)
        in_window = [ts for ts in timestamps if ts >= time.time() - 1800]
        assert len(in_window) == tick, (
            f"tick {tick}: expected {tick} in-window timestamp(s); "
            f"got {len(in_window)}"
        )

    # Tick 4: loop guard must block despite all prior restarts having failed.
    rc = belfry.main([str(tmp_path)])
    assert rc in (1, 2), "tick 4: expected DOWN"

    # needs-sre sentinel must be written — guard fired.
    nsre = belfry.needs_sre_path(tmp_path / "state")
    assert nsre.exists(), "needs-sre sentinel not written when guard blocks on failed restarts"
    assert "crash-loop" in nsre.read_text(encoding="utf-8")

    # Audit log: 3 restart entries (failed outcome) + 1 escalate entry.
    flog = belfry.fixers_log_path(tmp_path / "state")
    assert flog.exists(), "fixers.log not written"
    lines = [ln for ln in flog.read_text(encoding="utf-8").splitlines() if ln.strip()]
    restart_lines = [ln for ln in lines if "action=restart" in ln]
    escalate_lines = [ln for ln in lines if "action=escalate" in ln]
    assert len(restart_lines) == 3, (
        f"expected 3 restart log entries (failed outcome); got {restart_lines}"
    )
    assert len(escalate_lines) == 1, (
        f"expected 1 escalate entry; got {escalate_lines}"
    )
