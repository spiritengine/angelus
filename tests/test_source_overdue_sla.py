"""Per-source overdue detection (0014): belfry must alarm when ANY single
source stops being checked, not only when the daemon stops checking everything.

belfry's wedge check reads a GLOBAL max(last_checked_at) -- it fires only when
the daemon stops checking ALL sources, so one healthy source masks any stale
subset. This is the input-side mirror of the per-pipe delivery SLA (B2): the
daemon persists each interval-cadence source's check window into source_sla,
and belfry -- the out-of-band, pure-stdlib layer -- reads it read-only and pings
DOWN (alert-only, never a restart) when one source's last_checked_at heartbeat
lapses past its window. The discriminating shape: one source silent while the
other ~25 keep max() fresh and belfry stays green (the iotaschool gap).
"""

from __future__ import annotations

import importlib.util
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from angelus.clock import FakeClock
from angelus.daemon import SOURCE_SLA_SLACK_FLOOR_SECONDS
from angelus.storage import Catalog, init_db

_REPO_ROOT = Path(__file__).resolve().parent.parent
_BELFRY_PATH = _REPO_ROOT / "belfry" / "belfry.py"
PINNED = datetime(2026, 6, 7, 12, 0, 0, tzinfo=UTC)


def _load_belfry():
    spec = importlib.util.spec_from_file_location("belfry_source_sla", _BELFRY_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _catalog(tmp_path: Path, clock: FakeClock) -> Catalog:
    connection = init_db(tmp_path / "angelus.sqlite3")
    return Catalog(connection, tmp_path, clock=clock)


def _stamp(when: datetime) -> str:
    return when.isoformat(timespec="milliseconds").replace("+00:00", "Z")


# --- catalog: sync_source_sla ------------------------------------------------


def _sla_rows(catalog: Catalog) -> dict[str, tuple]:
    return {
        r["source_ref"]: (r["max_interval_seconds"], r["tracking_since"])
        for r in catalog.connection.execute(
            "SELECT source_ref, max_interval_seconds, tracking_since FROM source_sla"
        )
    }


def test_sync_source_sla_inserts_and_keeps_tracking_since(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)

    catalog.sync_source_sla({"scheduled/a": 28800})
    first = _sla_rows(catalog)
    assert first["scheduled/a"] == (28800, _stamp(PINNED))

    # A later sync with a changed window updates the window but NOT
    # tracking_since (so a stall spanning restarts is still measured from the
    # original baseline).
    clock.advance(timedelta(hours=10))
    catalog.sync_source_sla({"scheduled/a": 43200})
    second = _sla_rows(catalog)
    assert second["scheduled/a"][0] == 43200
    assert second["scheduled/a"][1] == first["scheduled/a"][1]


def test_sync_source_sla_removes_declassified_source(tmp_path) -> None:
    catalog = _catalog(tmp_path, FakeClock(PINNED))
    catalog.sync_source_sla({"scheduled/a": 28800, "scheduled/b": 86400})
    assert set(_sla_rows(catalog)) == {"scheduled/a", "scheduled/b"}

    # b drops out (removed, or reclassified to a crontab cadence) -> its row is
    # removed so no stale row keeps belfry red.
    catalog.sync_source_sla({"scheduled/a": 28800})
    assert set(_sla_rows(catalog)) == {"scheduled/a"}

    # empty -> table cleared.
    catalog.sync_source_sla({})
    assert _sla_rows(catalog) == {}


# --- belfry: source_overdue_failure ------------------------------------------


def _seed(
    tmp_path: Path,
    clock: FakeClock,
    *,
    sla: dict[str, int] | None = None,
    checks: dict[str, datetime] | None = None,
) -> Path:
    """Build a real angelus.sqlite3 with source_sla rows + watch_state
    heartbeats. `checks` maps source_ref -> last_checked_at time (a fired
    source); a source in `sla` but absent from `checks` has never fired."""
    catalog = _catalog(tmp_path, clock)
    if sla:
        catalog.sync_source_sla(sla)
    for source_ref, checked_at in (checks or {}).items():
        clock.set(checked_at)
        catalog.record_watch_check(source_ref, "200", "ok", None)
    catalog.connection.close()
    return tmp_path / "angelus.sqlite3"


def test_single_stale_source_pings_down_while_others_fresh(tmp_path) -> None:
    """THE discriminating test (the gap today): one source's last_checked_at is
    stale past its window while every other source is fresh, so the global
    max(last_checked_at) wedge check stays green -- but per-source detection
    names the one stale source. Fails today (no per-source check) and fails if
    the change is reverted."""
    belfry = _load_belfry()
    db = _seed(
        tmp_path,
        FakeClock(PINNED),
        sla={"scheduled/stale": 8 * 3600, "scheduled/fresh": 8 * 3600},
        checks={
            # fresh checked 1h ago -> keeps the global max() fresh.
            "scheduled/fresh": PINNED - timedelta(hours=1),
            # stale last checked 10h ago, window 8h -> overdue.
            "scheduled/stale": PINNED - timedelta(hours=10),
        },
    )
    reason = belfry.source_overdue_failure(db, now=PINNED)
    assert reason is not None
    assert "scheduled/stale overdue" in reason
    assert "last checked 10" in reason
    # The fresh source must NOT be named.
    assert "scheduled/fresh" not in reason


def test_slow_source_within_window_no_alarm(tmp_path) -> None:
    """A legitimately slow source within its per-source window does not alarm,
    even though a faster sibling exists -- proving the window is per-source
    cadence, not a single global threshold (a global threshold tuned to the
    fast source would false-alarm this one)."""
    belfry = _load_belfry()
    db = _seed(
        tmp_path,
        FakeClock(PINNED),
        sla={"scheduled/daily": 48 * 3600, "scheduled/fast": 600},
        checks={
            # daily (window 48h) checked 20h ago -> well inside its window.
            "scheduled/daily": PINNED - timedelta(hours=20),
            "scheduled/fast": PINNED - timedelta(minutes=2),
        },
    )
    assert belfry.source_overdue_failure(db, now=PINNED) is None


def test_never_fired_within_tracking_since_grace_ok(tmp_path) -> None:
    belfry = _load_belfry()
    # SLA registered 2h before "now", never fired, window 8h -> still in grace.
    clock = FakeClock(PINNED - timedelta(hours=2))
    db = _seed(tmp_path, clock, sla={"scheduled/new": 8 * 3600}, checks=None)
    assert belfry.source_overdue_failure(db, now=PINNED) is None


def test_never_fired_past_grace_alarms(tmp_path) -> None:
    belfry = _load_belfry()
    # Registered 10h before "now", never fired, window 8h -> overdue from
    # tracking_since.
    clock = FakeClock(PINNED - timedelta(hours=10))
    db = _seed(tmp_path, clock, sla={"scheduled/new": 8 * 3600}, checks=None)
    reason = belfry.source_overdue_failure(db, now=PINNED)
    assert reason is not None
    assert "scheduled/new overdue" in reason
    assert "never checked" in reason


def test_uses_real_last_check_even_older_than_tracking_since(tmp_path) -> None:
    """When a source HAS fired, the baseline is its real last_checked_at -- not
    tracking_since -- even if that check predates tracking_since (the SLA was
    enabled on an already-running source). The stricter `checked or
    tracking_since` first-non-null choice, mirroring sla_failure; pin it so a
    future maintainer can't 'simplify' to max() and grant extra grace."""
    belfry = _load_belfry()
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)
    # A check 40h before now...
    clock.set(PINNED - timedelta(hours=40))
    catalog.record_watch_check("scheduled/a", "200", "ok", None)
    # ...then the SLA registered only 2h before now (tracking_since recent).
    clock.set(PINNED - timedelta(hours=2))
    catalog.sync_source_sla({"scheduled/a": 8 * 3600})
    catalog.connection.close()
    db = tmp_path / "angelus.sqlite3"

    reason = belfry.source_overdue_failure(db, now=PINNED)
    assert reason is not None
    assert "last checked 40" in reason


def test_young_daemon_suppresses_overdue(tmp_path, monkeypatch) -> None:
    """A young daemon (process within startup grace) with a not-yet-fired
    source must NOT alarm: it is still establishing its first heartbeats. Same
    grace gate wedge_failure applies, via the pid_file."""
    belfry = _load_belfry()
    # Registered 10h ago, never fired, window 8h -> overdue by tracking_since...
    clock = FakeClock(PINNED - timedelta(hours=10))
    db = _seed(tmp_path, clock, sla={"scheduled/new": 8 * 3600}, checks=None)
    pid_file = tmp_path / "angelus.pid"
    pid_file.write_text(str(os.getpid()), encoding="utf-8")
    # ...but the process is young: start epoch is "now", inside the 180s grace.
    monkeypatch.setattr(belfry, "process_start_epoch", lambda _pid: PINNED.timestamp())
    monkeypatch.setattr(belfry.time, "time", lambda: PINNED.timestamp())

    assert belfry.source_overdue_failure(db, pid_file, now=PINNED) is None

    # And once the process ages past the grace, the same overdue surfaces (the
    # never-fired-but-OLD case is still caught via tracking_since).
    monkeypatch.setattr(
        belfry, "process_start_epoch", lambda _pid: PINNED.timestamp() - 10_000
    )
    reason = belfry.source_overdue_failure(db, pid_file, now=PINNED)
    assert reason is not None
    assert "scheduled/new overdue" in reason


def test_fails_open_without_table(tmp_path) -> None:
    """A db with no source_sla table (predates 0014) must not DOWN."""
    belfry = _load_belfry()
    db = tmp_path / "angelus.sqlite3"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE watch_state (source_ref TEXT PRIMARY KEY)")
    connection.commit()
    connection.close()
    assert belfry.source_overdue_failure(db, now=PINNED) is None


def test_no_rows_is_ok(tmp_path) -> None:
    belfry = _load_belfry()
    db = _seed(tmp_path, FakeClock(PINNED), sla={}, checks=None)
    assert belfry.source_overdue_failure(db, now=PINNED) is None


# --- daemon: startup + hot reload persist the SLA ----------------------------


def _write_min_lodging(root: Path) -> None:
    """A minimal lodging the daemon can construct around (sources + a pipe +
    a channel)."""
    (root / "sources" / "scheduled").mkdir(parents=True)
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


def test_daemon_startup_syncs_source_sla_with_slack_policy(tmp_path) -> None:
    """_sync_source_sla persists cadence + max(cadence, floor) per interval
    source. 4h -> 4h + 4h = 8h; 30s -> 30s + floor."""
    from angelus.daemon import AngelusDaemon

    _write_min_lodging(tmp_path)
    (tmp_path / "sources" / "scheduled" / "loose.yaml").write_text(
        "cadence: 4h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (tmp_path / "sources" / "scheduled" / "fast.yaml").write_text(
        "cadence: 30s\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )

    daemon = AngelusDaemon(tmp_path)
    try:
        daemon._sync_source_sla()
        rows = _sla_rows(daemon.catalog)
        assert rows["scheduled/loose"][0] == 4 * 3600 + max(4 * 3600, SOURCE_SLA_SLACK_FLOOR_SECONDS)
        assert rows["scheduled/fast"][0] == 30 + max(30, SOURCE_SLA_SLACK_FLOOR_SECONDS)
    finally:
        daemon.connection.close()


def test_daemon_skips_crontab_source_without_crashing(tmp_path, caplog) -> None:
    """A crontab-cadence source must not crash _sync_source_sla and must not be
    silently dropped: it is skipped (left to the global wedge backstop) and
    named in a WARNING so the gap is visible. The interval sibling is still
    tracked."""
    import logging

    from angelus.daemon import AngelusDaemon

    _write_min_lodging(tmp_path)
    (tmp_path / "sources" / "scheduled" / "interval.yaml").write_text(
        "cadence: 4h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (tmp_path / "sources" / "scheduled" / "daily-cron.yaml").write_text(
        "cadence: '0 7 * * *'\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )

    daemon = AngelusDaemon(tmp_path)
    try:
        with caplog.at_level(logging.WARNING, logger="angelus.daemon"):
            daemon._sync_source_sla()  # must not raise on the crontab source
        rows = _sla_rows(daemon.catalog)
        assert set(rows) == {"scheduled/interval"}
        assert "scheduled/daily-cron" not in rows
        # The skip is visible, not silent: a warning names the crontab source.
        warnings = "\n".join(
            r.message for r in caplog.records if r.levelno >= logging.WARNING
        )
        assert "scheduled/daily-cron" in warnings
    finally:
        daemon.connection.close()


def test_hot_reload_resyncs_source_sla(tmp_path) -> None:
    """A hot-added source becomes monitored without a restart, mirroring the
    pipe-SLA re-sync apply_lodging already does."""
    import asyncio

    from angelus.daemon import AngelusDaemon
    from angelus.lodging.reloader import LodgingReloader

    _write_min_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)
    try:
        daemon._sync_source_sla()
        assert _sla_rows(daemon.catalog) == {}

        new_source = tmp_path / "sources" / "scheduled" / "added.yaml"
        new_source.write_text(
            "cadence: 4h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
            encoding="utf-8",
        )
        reloader.event_queue.put(str(new_source))
        asyncio.run(reloader.process_pending_events())

        rows = _sla_rows(daemon.catalog)
        assert "scheduled/added" in rows
    finally:
        daemon.connection.close()


# --- main(): OTHER/alert-only classification ---------------------------------


def _alive(root: Path) -> None:
    """Daemon state pid_failure and wedge_failure both call healthy: a live PID
    (this test process) and a fresh global heartbeat."""
    (root / "state").mkdir(exist_ok=True)
    (root / "state" / "angelus.pid").write_text(str(os.getpid()), encoding="utf-8")


def _seed_state_db(root: Path, clock: FakeClock, **kwargs) -> None:
    db = _seed(root / "state", clock, **kwargs)
    assert db.exists()


def test_source_overdue_pings_down_alert_only(tmp_path, monkeypatch) -> None:
    """A live daemon with one stale source pings DOWN naming it, and does NOT
    trigger a restart -- per-source overdue is an alert-only OTHER reason (a
    single stale source is not a dead daemon; restart is the wrong tool and
    could auto-deploy)."""
    belfry = _load_belfry()
    monkeypatch.setenv("ANGELUS_BELFRY_SUCCESS_URL", "https://hc.example/success")
    monkeypatch.setenv("ANGELUS_BELFRY_DOWN_URL", "https://hc.example/down")
    (tmp_path / "state").mkdir()
    _alive(tmp_path)
    # One stale source (10h), one fresh (keeps the wedge global max() green).
    _seed_state_db(
        tmp_path,
        FakeClock(PINNED),
        sla={"scheduled/stale": 8 * 3600, "scheduled/fresh": 8 * 3600},
        checks={
            # fresh checked 2 min ago -> keeps the GLOBAL max(last_checked_at)
            # within the wedge threshold, so wedge_failure stays green and
            # source_overdue is the only thing that can move the result. This is
            # exactly the masking the per-source check exists to defeat.
            "scheduled/fresh": PINNED - timedelta(minutes=2),
            "scheduled/stale": PINNED - timedelta(hours=10),
        },
    )

    pings: list[str] = []
    calls: list[list[str]] = []
    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _FakeResponse(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check=False, **_: calls.append(args)
        or _completed(args),
    )
    # Take drift/stale-deploy off the table so source_overdue is the only OTHER
    # reason moving the result.
    monkeypatch.setattr(belfry, "systemd_main_pid", lambda: None)
    monkeypatch.setattr(belfry, "last_code_commit_epoch", lambda _root: None)
    # belfry tick time is "now"; freeze it so the seeded ages hold.
    monkeypatch.setattr(belfry, "datetime", _FrozenDatetime)
    # The process is old (not in startup grace) so the overdue is not suppressed.
    monkeypatch.setattr(belfry, "process_start_epoch", lambda _pid: 1_000.0)
    # Guard: any restart attempt fails the test loudly.
    monkeypatch.setattr(
        belfry,
        "_autoremediate_absence",
        lambda *a, **k: pytest.fail("source-overdue must not restart"),
    )

    assert belfry.main([str(tmp_path)]) == 1
    assert pings[-1] == "https://hc.example/down"
    payload = " ".join(calls[-1])
    assert "scheduled/stale overdue" in payload
    assert "scheduled/fresh" not in payload


class _FakeResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def _completed(args):
    import subprocess

    return subprocess.CompletedProcess(args, 0)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return PINNED if tz is None else PINNED.astimezone(tz)
