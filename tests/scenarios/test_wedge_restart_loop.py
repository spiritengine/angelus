"""S2 wedge_restart_loop -- 2026-06-07, brief-20260612-p3ct (the migration incident).

A change-detection migration shipped to a live deploy and left watch_state
EMPTY -- the table was recreated but the per-source heartbeat rows had not been
repopulated yet. The daemon restarted into that empty table and could not fire
all its sources fast enough to refill it before belfry's 5-minute wedge check
ran. belfry read "watch_state has no rows", called it a wedge, and restarted the
daemon -- which came up to the same empty table and got restarted again. Two of
three restarts burned before an SRE caught it. The fell that reviewed the fix
failed to model the real shape: it tested a FRESH db, not migration-on-live-prod
with an emptied table under a just-restarted daemon.

The fix was two-sided: Fix A fires every source once at boot to repopulate
watch_state; Fix B gives belfry a startup grace so a young daemon with an empty
table is NOT treated as wedged. This scenario proves both, in order, and proves
the fixture discriminates with a counterfactual:

  (a) young daemon + empty watch_state  -> wedge suppressed, NO restart;
  (b) the boot fire populates watch_state -> wedge green on its own merits;
  (c) OLD daemon + empty watch_state -> wedge fires, restart attempted, and
      repeated ticks hit the loop-guard cap: restarts stop, the needs-sre
      sentinel is written, the escalation page fires.

Mutation-verified: reverting Fix B -- the young-process startup grace in
``wedge_failure`` (delete the ``within_startup_grace`` suppression branch so the
wedge reports unconditionally) -- turns phase (a) red: a young daemon with an
empty table is reported wedged and a restart is dispatched, the exact restart
loop. Confirmed red with that edit; restored after.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
from datetime import UTC, datetime

import pytest

from angelus.sim import SimHarness


class _Resp:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return None


def _mock_belfry_io(belfry, monkeypatch):
    """Record every subprocess.run and urlopen; return (systemctl, notify, pings).
    systemctl restart calls and notify-pat calls are split by argv[:3]."""
    systemctl: list[list[str]] = []
    notify: list[list[str]] = []
    pings: list[str] = []

    def fake_run(args, check=False, **kwargs):
        a = list(args)
        if a[:3] == ["systemctl", "--user", "restart"]:
            systemctl.append(a)
        else:
            notify.append(a)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(belfry.subprocess, "run", fake_run)
    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Resp(),
    )
    monkeypatch.setenv("ANGELUS_BELFRY_SUCCESS_URL", "https://hc.example/success")
    monkeypatch.setenv("ANGELUS_BELFRY_DOWN_URL", "https://hc.example/down")
    # Drift axis off the table: systemd reports the live pid, so no drift.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: os.getpid())
    # Stale-deploy axis off the table: a 1970 process start (the "old daemon"
    # below) would otherwise read every recent commit as newer-than-running.
    monkeypatch.setattr(belfry, "last_code_commit_epoch", lambda _root: None)
    return systemctl, notify, pings


@pytest.mark.scenario
def test_wedge_restart_loop(tmp_path, monkeypatch, reference_lodging, belfry, belfry_paths):
    # Several sources, no per-pipe delivery SLA (this scenario is about the
    # wedge/restart axis, not delivery). START at real-now so the boot fire in
    # phase (b) stamps a wall-fresh heartbeat the wall-clock wedge reads green.
    sources = (
        ("scheduled/watch-a", "1h"),
        ("scheduled/watch-b", "1h"),
        ("scheduled/watch-c", "1h"),
    )
    start = datetime.now(UTC)

    # ---- (a) young daemon + EMPTY watch_state -> wedge suppressed, no restart.
    root_young = tmp_path / "young"
    reference_lodging(root_young, sources=sources, daily_max_interval=None)
    young = belfry_paths(root_young)

    with SimHarness(root_young, start) as sim:
        # No fire: watch_state has the migrated-but-empty shape.
        assert sim.daemon.catalog.connection.execute(
            "SELECT COUNT(*) AS n FROM watch_state"
        ).fetchone()["n"] == 0, "phase (a) requires an empty watch_state"

        young.write_pid(os.getpid())
        # A genuinely young daemon process (just restarted into the empty table).
        monkeypatch.setattr(belfry, "process_start_epoch", lambda _pid: datetime.now(UTC).timestamp())

        # Fix B: the grace suppresses the wedge for the young daemon.
        assert belfry.wedge_failure(young.db_path, young.pid_file) is None, (
            "a young daemon with an empty watch_state must NOT be wedged"
        )

        # And end to end: belfry does not dispatch a restart.
        systemctl_a, _notify_a, _pings_a = _mock_belfry_io(belfry, monkeypatch)
        rc = belfry.main([str(root_young)])
        assert systemctl_a == [], f"phase (a) must dispatch no restart; got {systemctl_a}"
        assert rc == 0, "phase (a): a young empty-table daemon is healthy (SUCCESS)"

        # ---- (b) the boot fire populates watch_state -> wedge green on merits.
        async def _boot_fire() -> None:
            for source_ref, _cadence in sources:
                await sim.fire_source(source_ref)

        asyncio.run(_boot_fire())
        assert sim.daemon.catalog.connection.execute(
            "SELECT COUNT(*) AS n FROM watch_state"
        ).fetchone()["n"] == len(sources), "the boot fire must populate watch_state"

        # No pid_file -> grace opted out: the wedge is green purely because the
        # heartbeat is fresh, not because of the startup grace.
        assert belfry.wedge_failure(young.db_path) is None, (
            "a populated, fresh watch_state clears the wedge on its own merits"
        )

    # ---- (c) COUNTERFACTUAL: OLD daemon + empty watch_state -> the restart loop.
    # A separate fresh root keeps watch_state empty (phase (b) populated the
    # other one). This is the discriminator: same empty table, but an OLD
    # process, so the grace does NOT apply and the genuine wedge fires.
    root_old = tmp_path / "old"
    reference_lodging(root_old, sources=sources, daily_max_interval=None)
    old = belfry_paths(root_old)

    monkeypatch.setenv("ANGELUS_BELFRY_RECOVER_WAIT_SEC", "0")
    monkeypatch.setenv("ANGELUS_BELFRY_MAX_RESTARTS", "3")
    monkeypatch.setenv("ANGELUS_BELFRY_RESTART_WINDOW_SEC", "1800")

    with SimHarness(root_old, start) as sim_old:
        assert sim_old.daemon.catalog.connection.execute(
            "SELECT COUNT(*) AS n FROM watch_state"
        ).fetchone()["n"] == 0, "phase (c) requires an empty watch_state"

        old.write_pid(os.getpid())
        # Daemon alive but up far longer than the grace: a genuine wedge.
        monkeypatch.setattr(belfry, "process_start_epoch", lambda _pid: 1_000.0)
        # Grace does NOT save an old daemon: the wedge is real.
        assert belfry.wedge_failure(old.db_path, old.pid_file) is not None

        systemctl, notify, _pings = _mock_belfry_io(belfry, monkeypatch)
        # Re-apply the old process age (the helper above did not touch it, but
        # it re-stubbed systemd_main_pid/last_code_commit_epoch).
        monkeypatch.setattr(belfry, "process_start_epoch", lambda _pid: 1_000.0)

        # Ticks 1..3 each attempt exactly one restart (the daemon stays wedged
        # because nothing repopulates the empty table).
        for tick in range(1, 4):
            rc = belfry.main([str(root_old)])
            assert rc in (1, 2), f"tick {tick}: expected DOWN"
            assert len(systemctl) == tick, (
                f"tick {tick}: expected {tick} restart(s); got {len(systemctl)}"
            )

        restarts_before = len(systemctl)
        # Tick 4: the loop guard blocks -- no new restart, escalation instead.
        rc = belfry.main([str(root_old)])
        assert rc in (1, 2), "tick 4: expected DOWN"
        assert len(systemctl) == restarts_before, (
            f"tick 4: loop guard FAILED to block restart; got {systemctl}"
        )

        # The escalation page fires even though the restart was withheld.
        assert notify, "tick 4: the escalation page must fire when the guard blocks"
        assert "crash-loop" in " ".join(notify[-1]), notify[-1]

        # The needs-sre sentinel is written so an off-box SRE runner picks it up.
        nsre = belfry.needs_sre_path(old.state_dir)
        assert nsre.exists() and "crash-loop" in nsre.read_text(encoding="utf-8")
