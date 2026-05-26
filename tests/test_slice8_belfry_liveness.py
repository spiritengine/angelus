"""M2 slice 8: belfry liveness via sentinel file.

Two test families:

* Daemon-side unit tests pin _belfry_status (and the _op_health belfry
  field that surfaces it) for the three sentinel states: missing
  (never-pinged), present-and-fresh, present-and-stale. Plus an
  env-var override pinned for both the path and the stale threshold.
* End-to-end integration test drives belfry (the cron entry point) on
  a tmp root and then constructs an AngelusDaemon and calls the live
  _op_health, asserting the sentinel file belfry wrote is what the
  daemon reads. This is the mandatory-reader contract: belfry-writer
  and daemon-reader in the same slice, no dead-config trap.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from angelus.daemon import (
    DEFAULT_BELFRY_STALE_AFTER_SEC,
    AngelusDaemon,
    _belfry_sentinel_path,
    _belfry_stale_after_seconds,
    _belfry_status,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
BELFRY_PATH = REPO_ROOT / "belfry" / "belfry.py"


def _load_belfry():
    spec = importlib.util.spec_from_file_location("belfry_under_test", BELFRY_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_lodging(root: Path) -> None:
    """Minimal lodging the daemon can construct around."""
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
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )


def _set_sentinel_mtime(path: Path, when: datetime) -> None:
    epoch = when.timestamp()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    os.utime(path, (epoch, epoch))


# --- _belfry_sentinel_path / _belfry_stale_after_seconds helpers ----------


def test_sentinel_path_default(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ANGELUS_BELFRY_SENTINEL_PATH", raising=False)
    assert _belfry_sentinel_path(tmp_path) == tmp_path / "state" / "belfry-pinged-at"


def test_sentinel_path_override(tmp_path, monkeypatch) -> None:
    override = tmp_path / "custom-sentinel"
    monkeypatch.setenv("ANGELUS_BELFRY_SENTINEL_PATH", str(override))
    assert _belfry_sentinel_path(tmp_path) == override


def test_stale_threshold_default(monkeypatch) -> None:
    monkeypatch.delenv("ANGELUS_BELFRY_STALE_AFTER_SEC", raising=False)
    assert _belfry_stale_after_seconds() == DEFAULT_BELFRY_STALE_AFTER_SEC


def test_stale_threshold_override(monkeypatch) -> None:
    monkeypatch.setenv("ANGELUS_BELFRY_STALE_AFTER_SEC", "60")
    assert _belfry_stale_after_seconds() == 60


def test_stale_threshold_falls_back_on_invalid(monkeypatch) -> None:
    """Non-integer override falls back. Discrimination: under the
    inversion 'crash on bad env,' _op_health would raise and the daemon
    would surface an internal error -- this assertion catches the
    fallback path explicitly."""
    monkeypatch.setenv("ANGELUS_BELFRY_STALE_AFTER_SEC", "not-a-number")
    assert _belfry_stale_after_seconds() == DEFAULT_BELFRY_STALE_AFTER_SEC


def test_stale_threshold_falls_back_on_non_positive(monkeypatch) -> None:
    monkeypatch.setenv("ANGELUS_BELFRY_STALE_AFTER_SEC", "0")
    assert _belfry_stale_after_seconds() == DEFAULT_BELFRY_STALE_AFTER_SEC


# --- _belfry_status: the three sentinel states ----------------------------


def test_belfry_status_missing_sentinel(tmp_path, monkeypatch) -> None:
    """File missing -> never-pinged shape. Discrimination axis: under
    the inversion 'hardcode None' (the pre-slice-8 daemon), this
    assertion's shape mismatch fires."""
    monkeypatch.delenv("ANGELUS_BELFRY_SENTINEL_PATH", raising=False)
    (tmp_path / "state").mkdir()
    status = _belfry_status(tmp_path)
    assert status == {"last_pinged_at": None, "stale": True}


def test_belfry_status_fresh_sentinel(tmp_path, monkeypatch) -> None:
    """File mtime recent -> not stale. Discrimination axis: under the
    inversion 'always set stale=True regardless of mtime,' this
    assertion's stale=False fires."""
    monkeypatch.delenv("ANGELUS_BELFRY_SENTINEL_PATH", raising=False)
    monkeypatch.delenv("ANGELUS_BELFRY_STALE_AFTER_SEC", raising=False)
    (tmp_path / "state").mkdir()
    sentinel = tmp_path / "state" / "belfry-pinged-at"
    fresh = datetime.now(UTC) - timedelta(seconds=5)
    _set_sentinel_mtime(sentinel, fresh)

    status = _belfry_status(tmp_path)
    assert status["stale"] is False
    assert status["last_pinged_at"] is not None
    pinged_dt = datetime.fromisoformat(
        status["last_pinged_at"].replace("Z", "+00:00")
    )
    # Allow 2s slack for filesystem timestamp granularity / clock skew.
    assert abs((pinged_dt - fresh).total_seconds()) < 2


def test_belfry_status_stale_sentinel(tmp_path, monkeypatch) -> None:
    """File mtime older than threshold -> stale. Discrimination axis:
    under the inversion 'compute age but never flip stale=True' or
    'use wrong cadence (e.g. divide threshold by 60),' this assertion
    fires."""
    monkeypatch.delenv("ANGELUS_BELFRY_SENTINEL_PATH", raising=False)
    # Tight threshold so we don't sleep for 20 minutes in the test.
    monkeypatch.setenv("ANGELUS_BELFRY_STALE_AFTER_SEC", "10")
    (tmp_path / "state").mkdir()
    sentinel = tmp_path / "state" / "belfry-pinged-at"
    stale_when = datetime.now(UTC) - timedelta(seconds=60)
    _set_sentinel_mtime(sentinel, stale_when)

    status = _belfry_status(tmp_path)
    assert status["stale"] is True
    assert status["last_pinged_at"] is not None


def test_belfry_status_override_path_honored(tmp_path, monkeypatch) -> None:
    """ANGELUS_BELFRY_SENTINEL_PATH override is read by both sides.
    Discrimination axis: under the inversion 'ignore env var on the
    daemon side,' the default path is missing so status would still
    be never-pinged -- this fresh assertion's stale=False catches that."""
    override = tmp_path / "other-place" / "sentinel"
    monkeypatch.setenv("ANGELUS_BELFRY_SENTINEL_PATH", str(override))
    monkeypatch.delenv("ANGELUS_BELFRY_STALE_AFTER_SEC", raising=False)
    fresh = datetime.now(UTC) - timedelta(seconds=2)
    _set_sentinel_mtime(override, fresh)

    status = _belfry_status(tmp_path)
    assert status["stale"] is False
    assert status["last_pinged_at"] is not None


# --- end-to-end: _op_health on a daemon, after a real belfry tick ---------


def test_op_health_surfaces_belfry_after_real_belfry_tick(
    tmp_path, monkeypatch
) -> None:
    """Mandatory-reader contract: belfry writes the sentinel, the daemon
    reads it via _op_health, both sides in the same slice.

    Discrimination axes (all three landed in the worktree, observed, reverted):

    * Inversion 1 -- remove the touch from belfry.main() (delete the
      touch_sentinel call). Sentinel never exists, _op_health surfaces
      {"last_pinged_at": None, "stale": True}. The
      last_pinged_at-is-not-None assertion below fires.
    * Inversion 2 -- hardcode _op_health's belfry field back to None.
      The result["belfry"] == dict shape assertion fails (got NoneType).
    * Inversion 3 -- set ANGELUS_BELFRY_STALE_AFTER_SEC to 0 (or a
      negative; the fallback intervenes but the test specifically asserts
      stale=False on a freshly-touched sentinel under default threshold,
      so any code path that flips stale=True under a fresh mtime fires).
    """
    _write_lodging(tmp_path)
    monkeypatch.delenv("ANGELUS_BELFRY_SENTINEL_PATH", raising=False)
    monkeypatch.delenv("ANGELUS_BELFRY_STALE_AFTER_SEC", raising=False)
    monkeypatch.setenv("ANGELUS_BELFRY_SUCCESS_URL", "https://hc.example/success")
    monkeypatch.setenv("ANGELUS_BELFRY_DOWN_URL", "https://hc.example/down")
    monkeypatch.setenv("ANGELUS_EMAIL_TO", "test@example.com")
    # Belfry takes the DOWN path (no live PID file). The sentinel-touch
    # contract is "every tick, success OR failure," so the failure path
    # exercises exactly the same touch the test cares about, without
    # needing a hand-rolled source_fires table that would collide with
    # init_db's migrations when the AngelusDaemon constructs below.
    (tmp_path / "state").mkdir()

    belfry = _load_belfry()
    pings: list[str] = []
    calls: list[list[str]] = []
    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _no_op_response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    sentinel = tmp_path / "state" / "belfry-pinged-at"
    assert not sentinel.exists()

    before = time.time()
    rc = belfry.main([str(tmp_path)])
    after = time.time()
    assert rc == 1, "belfry should escalate (DOWN path) without a PID file"
    assert sentinel.exists(), (
        "belfry MUST touch the sentinel on every tick, including the "
        "down-escalation path -- slice-8 semantics"
    )

    # Read the live _op_health (full daemon construction + control op
    # call). This is the path angelus health goes through end-to-end --
    # we deliberately do NOT short-circuit to _belfry_status because we
    # want the dict shape returned by the control op surface itself.
    async def driver():
        daemon = AngelusDaemon(tmp_path)
        try:
            response = await daemon._op_health({})
        finally:
            daemon.connection.close()
        return response

    result = asyncio.run(driver())

    assert isinstance(result["belfry"], dict), (
        "slice 8 contract: belfry field is a dict, never bare None. "
        "Hardcoded-None inversion fires this assertion."
    )
    belfry_field = result["belfry"]
    assert belfry_field["last_pinged_at"] is not None, (
        "belfry just wrote the sentinel; last_pinged_at must be populated"
    )
    assert belfry_field["stale"] is False, (
        f"freshly-touched sentinel should not be stale: {belfry_field}"
    )
    # Round-trip the mtime through iso8601 and confirm it matches the
    # wall-clock window of the belfry tick.
    pinged_dt = datetime.fromisoformat(
        belfry_field["last_pinged_at"].replace("Z", "+00:00")
    )
    pinged_epoch = pinged_dt.timestamp()
    assert before - 1 <= pinged_epoch <= after + 1, (
        f"belfry-reported mtime {pinged_epoch} outside wall-clock "
        f"[{before}, {after}]"
    )


class _no_op_response:
    """Minimal context-manager response double for urllib.urlopen monkeypatch."""

    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def test_op_health_stale_sentinel_after_passing_threshold(
    tmp_path, monkeypatch
) -> None:
    """A sentinel mtime older than the configured threshold surfaces as
    stale=True via the full _op_health path. Discrimination axis: this
    catches a threshold-misapplied regression (e.g. comparing age to
    threshold * 1000 instead of threshold seconds) that the unit
    _belfry_status test would catch too, but here through the full op
    response shape so the contract holds at the integration surface."""
    _write_lodging(tmp_path)
    monkeypatch.delenv("ANGELUS_BELFRY_SENTINEL_PATH", raising=False)
    monkeypatch.setenv("ANGELUS_BELFRY_STALE_AFTER_SEC", "5")

    (tmp_path / "state").mkdir()
    sentinel = tmp_path / "state" / "belfry-pinged-at"
    stale_when = datetime.now(UTC) - timedelta(seconds=60)
    _set_sentinel_mtime(sentinel, stale_when)

    async def driver():
        daemon = AngelusDaemon(tmp_path)
        try:
            return await daemon._op_health({})
        finally:
            daemon.connection.close()

    result = asyncio.run(driver())
    belfry_field = result["belfry"]
    assert belfry_field["stale"] is True
    assert belfry_field["last_pinged_at"] is not None
