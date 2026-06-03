"""B2: assert each pipe actually delivers on its cadence -- the contract.

A pipe declares an expected max interval between successful deliveries
(`max_interval: 27h`). The daemon persists it to the pipe_sla table at startup;
belfry -- the out-of-band, pure-stdlib layer -- reads it read-only and pings
DOWN when a pipe's last SUCCESSFUL dispatch lapses past the window. This is the
on-box, all-pipes generalization of the off-box digest dead-man and catches the
exact 2026-05-29 shape: nothing errored, the daily pipe just silently stopped
delivering.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from angelus.clock import FakeClock
from angelus.lodging.config import parse_pipe
from angelus.storage import Catalog, init_db

_REPO_ROOT = Path(__file__).resolve().parent.parent
_BELFRY_PATH = _REPO_ROOT / "belfry" / "belfry.py"
PINNED = datetime(2026, 6, 3, 12, 0, 0, tzinfo=UTC)


def _load_belfry():
    spec = importlib.util.spec_from_file_location("belfry_b2", _BELFRY_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _catalog(tmp_path: Path, clock: FakeClock) -> Catalog:
    connection = init_db(tmp_path / "angelus.sqlite3")
    return Catalog(connection, tmp_path, clock=clock)


# --- lodging: max_interval parsing -------------------------------------------


def _write_pipe(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


_DUMB = "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n"


def test_max_interval_parses_to_seconds(tmp_path) -> None:
    pipe = parse_pipe(
        _write_pipe(
            tmp_path / "daily.yaml",
            f"cadence: '0 7 * * *'\nchannels: [push]\nmax_interval: 27h\n{_DUMB}",
        )
    )
    assert pipe.max_interval_seconds == 27 * 3600


def test_max_interval_absent_is_none(tmp_path) -> None:
    pipe = parse_pipe(
        _write_pipe(
            tmp_path / "now.yaml",
            f"cadence: immediate\nchannels: [push]\n{_DUMB}",
        )
    )
    assert pipe.max_interval_seconds is None


def test_max_interval_supports_days(tmp_path) -> None:
    pipe = parse_pipe(
        _write_pipe(
            tmp_path / "weekly.yaml",
            f"cadence: '0 7 * * 1'\nchannels: [push]\nmax_interval: 8d\n{_DUMB}",
        )
    )
    assert pipe.max_interval_seconds == 8 * 86400


@pytest.mark.parametrize("bad", ["27", "27x", "0h", "-3h", "abc"])
def test_max_interval_bad_value_fails_loud(tmp_path, bad) -> None:
    with pytest.raises(ValueError, match="max_interval"):
        parse_pipe(
            _write_pipe(
                tmp_path / "p.yaml",
                f"cadence: '0 7 * * *'\nchannels: [push]\nmax_interval: '{bad}'\n{_DUMB}",
            )
        )


# --- catalog: sync_pipe_sla --------------------------------------------------


def _sla_rows(catalog: Catalog) -> dict[str, tuple]:
    return {
        r["pipe_name"]: (r["max_interval_seconds"], r["tracking_since"])
        for r in catalog.connection.execute(
            "SELECT pipe_name, max_interval_seconds, tracking_since FROM pipe_sla"
        )
    }


def test_sync_pipe_sla_inserts_and_keeps_tracking_since(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)

    catalog.sync_pipe_sla({"daily": 97200})
    first = _sla_rows(catalog)
    assert first["daily"] == (97200, PINNED.isoformat(timespec="milliseconds").replace("+00:00", "Z"))

    # A later sync with a changed interval updates the interval but NOT
    # tracking_since (so a stall spanning restarts is still measured from the
    # original baseline).
    clock.advance(timedelta(hours=10))
    catalog.sync_pipe_sla({"daily": 100000})
    second = _sla_rows(catalog)
    assert second["daily"][0] == 100000
    assert second["daily"][1] == first["daily"][1]  # tracking_since unchanged


def test_sync_pipe_sla_removes_declassified_pipe(tmp_path) -> None:
    catalog = _catalog(tmp_path, FakeClock(PINNED))
    catalog.sync_pipe_sla({"daily": 97200, "weekly": 700000})
    assert set(_sla_rows(catalog)) == {"daily", "weekly"}

    # weekly drops its SLA -> its row is removed (no stale row keeping belfry red).
    catalog.sync_pipe_sla({"daily": 97200})
    assert set(_sla_rows(catalog)) == {"daily"}

    # empty -> table cleared.
    catalog.sync_pipe_sla({})
    assert _sla_rows(catalog) == {}


# --- belfry: sla_failure -----------------------------------------------------


def _seed(tmp_path: Path, clock: FakeClock, *, sla=None, sent_at=None) -> Path:
    """Build a real angelus.sqlite3 with pipe_sla + a daily 'sent' dispatch."""
    catalog = _catalog(tmp_path, clock)
    if sla:
        catalog.sync_pipe_sla(sla)
    if sent_at is not None:
        clock.set(sent_at)
        catalog.record_dispatch("daily", "push", [1], "sent", mark_queue=False)
    catalog.connection.close()
    return tmp_path / "angelus.sqlite3"


def test_sla_overdue_when_last_delivery_lapsed(tmp_path) -> None:
    belfry = _load_belfry()
    # daily delivered 30h before "now", SLA is 27h -> overdue.
    db = _seed(
        tmp_path,
        FakeClock(PINNED),
        sla={"daily": 27 * 3600},
        sent_at=PINNED - timedelta(hours=30),
    )
    reason = belfry.sla_failure(db, now=PINNED)
    assert reason is not None
    assert "daily overdue" in reason
    assert "max 27h" in reason


def test_sla_ok_within_window(tmp_path) -> None:
    belfry = _load_belfry()
    # daily delivered 2h ago, SLA 27h -> not overdue.
    db = _seed(
        tmp_path,
        FakeClock(PINNED),
        sla={"daily": 27 * 3600},
        sent_at=PINNED - timedelta(hours=2),
    )
    assert belfry.sla_failure(db, now=PINNED) is None


def test_sla_never_delivered_uses_tracking_since(tmp_path) -> None:
    belfry = _load_belfry()
    clock = FakeClock(PINNED - timedelta(hours=30))
    # SLA registered 30h before "now", never delivered, window 27h -> overdue.
    db = _seed(tmp_path, clock, sla={"daily": 27 * 3600}, sent_at=None)
    reason = belfry.sla_failure(db, now=PINNED)
    assert reason is not None
    assert "no successful delivery" in reason


def test_sla_never_delivered_within_grace_is_ok(tmp_path) -> None:
    belfry = _load_belfry()
    clock = FakeClock(PINNED - timedelta(hours=2))
    # Registered 2h before "now", never delivered, window 27h -> still in grace.
    db = _seed(tmp_path, clock, sla={"daily": 27 * 3600}, sent_at=None)
    assert belfry.sla_failure(db, now=PINNED) is None


def test_sla_fails_open_without_table(tmp_path) -> None:
    belfry = _load_belfry()
    # A db with no pipe_sla table (predates the migration) must not DOWN.
    db = tmp_path / "angelus.sqlite3"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE dispatches (id INTEGER PRIMARY KEY)")
    connection.commit()
    connection.close()
    assert belfry.sla_failure(db, now=PINNED) is None


def test_sla_no_rows_is_ok(tmp_path) -> None:
    belfry = _load_belfry()
    db = _seed(tmp_path, FakeClock(PINNED), sla={}, sent_at=None)
    assert belfry.sla_failure(db, now=PINNED) is None


# --- daemon: startup persists the SLA ----------------------------------------


def test_daemon_startup_syncs_pipe_sla(tmp_path) -> None:
    from angelus.daemon import AngelusDaemon

    (tmp_path / "pipes").mkdir()
    _write_pipe(
        tmp_path / "pipes" / "now.yaml",
        f"cadence: immediate\nchannels: [push]\n{_DUMB}",
    )
    _write_pipe(
        tmp_path / "pipes" / "daily.yaml",
        "cadence: '0 7 * * *'\nchannels: [push]\nmax_interval: 27h\n"
        "render:\n  preamble:\n    - kind: structured\n      template: rate-limit-callout\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n"
        "    inputs:\n      - findings_since_last_drain\n",
    )
    (tmp_path / "channels").mkdir()
    _write_pipe(tmp_path / "channels" / "push.yaml", "kind: push\ncommand: 'true'\n")
    (tmp_path / "render-templates").mkdir()
    _write_pipe(
        tmp_path / "render-templates" / "rate-limit-callout.j2", "Suppressed:\n"
    )

    daemon = AngelusDaemon(tmp_path)
    try:
        daemon._sync_pipe_sla()
        rows = _sla_rows(daemon.catalog)
        # only daily declares max_interval; the immediate now pipe opts out.
        assert set(rows) == {"daily"}
        assert rows["daily"][0] == 27 * 3600
    finally:
        daemon.connection.close()


def test_hot_reload_resyncs_sla(tmp_path) -> None:
    """A hot-changed max_interval takes effect without a restart, mirroring the
    dep_health prune apply_lodging already does."""
    import asyncio

    from angelus.daemon import AngelusDaemon
    from angelus.lodging.reloader import LodgingReloader

    (tmp_path / "pipes").mkdir()
    daily = tmp_path / "pipes" / "daily.yaml"
    _write_pipe(
        tmp_path / "pipes" / "now.yaml",
        f"cadence: immediate\nchannels: [push]\n{_DUMB}",
    )
    _write_pipe(
        daily,
        f"cadence: '0 7 * * *'\nchannels: [push]\nmax_interval: 27h\n{_DUMB}",
    )
    (tmp_path / "channels").mkdir()
    _write_pipe(tmp_path / "channels" / "push.yaml", "kind: push\ncommand: 'true'\n")

    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)
    try:
        daemon._sync_pipe_sla()
        assert _sla_rows(daemon.catalog)["daily"][0] == 27 * 3600

        _write_pipe(
            daily,
            f"cadence: '0 7 * * *'\nchannels: [push]\nmax_interval: 30h\n{_DUMB}",
        )
        reloader.event_queue.put(str(daily))
        asyncio.run(reloader.process_pending_events())

        assert _sla_rows(daemon.catalog)["daily"][0] == 30 * 3600
    finally:
        daemon.connection.close()
