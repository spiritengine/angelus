"""B5: `angelus health` answers "is it WORKING", not just "is it running".

The health surface gains a delivery section: last successful send per pipe
(every configured pipe, 'never' if none), a recent-window failed-dispatch
count, and the count of angelus's own open internal incidents. Plain text,
one item per line (screen-reader friendly). This is the gap the 2026-05-29
incident hid -- the daemon was alive and "healthy" while delivery was dead.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon, _delivery_surface
from angelus.storage import Catalog, init_db

PINNED = datetime(2026, 6, 3, 12, 0, 0, tzinfo=UTC)


def _catalog(tmp_path: Path, clock: FakeClock | None = None) -> Catalog:
    connection = init_db(tmp_path / "angelus.sqlite3")
    return Catalog(connection, tmp_path, clock=clock or FakeClock(PINNED))


# --- catalog: last successful dispatch per pipe ------------------------------


def test_last_successful_dispatch_only_counts_sent(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)

    # now: an early sent, then a later sent -> the later one wins.
    catalog.record_dispatch("now", "push", [1], "sent", mark_queue=False)
    clock.advance(timedelta(hours=1))
    catalog.record_dispatch("now", "push", [2], "sent", mark_queue=False)
    later = clock.now_iso()

    # daily: only a failed and a muted dispatch -> no successful send.
    catalog.record_dispatch("daily", "email", [3], "failed", mark_queue=False)
    catalog.record_dispatch("daily", "(muted)", [4], "muted", mark_queue=False)

    result = catalog.last_successful_dispatch_per_pipe()
    assert result == {"now": later}
    assert "daily" not in result  # never successfully delivered


# --- catalog: failed-dispatch count over a recent window ---------------------


def test_failed_dispatch_count_respects_window(tmp_path) -> None:
    clock = FakeClock(PINNED)
    catalog = _catalog(tmp_path, clock)

    # One failure 30h ago (outside a 24h window)...
    clock.set(PINNED - timedelta(hours=30))
    catalog.record_dispatch("daily", "email", [1], "failed", mark_queue=False)
    # ...and one 1h ago (inside it).
    clock.set(PINNED - timedelta(hours=1))
    catalog.record_dispatch("daily", "email", [2], "failed", mark_queue=False)

    clock.set(PINNED)
    assert catalog.failed_dispatch_count(window_hours=24) == 1
    assert catalog.failed_dispatch_count(window_hours=48) == 2


def test_failed_dispatch_count_ignores_sent(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    catalog.record_dispatch("now", "push", [1], "sent", mark_queue=False)
    assert catalog.failed_dispatch_count() == 0


# --- catalog: open internal incident count -----------------------------------


def test_open_internal_incident_count(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    assert catalog.open_internal_incident_count() == 0
    catalog.write_internal_finding(
        "internal/dispatch", "channel_unhealthy", "email", "boom", {"now"}
    )
    assert catalog.open_internal_incident_count() == 1


# --- delivery surface: never-delivered pipes are still listed ----------------


def test_delivery_surface_lists_never_delivered_pipe(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    catalog.record_dispatch("now", "push", [1], "sent", mark_queue=False)

    surface = _delivery_surface(catalog, ["now", "daily"])
    assert surface["last_successful_send"]["now"] is not None
    assert surface["last_successful_send"]["daily"] is None  # 'never'
    assert surface["failed_dispatches"] == {"window_hours": 24, "count": 0}
    assert surface["open_internal_incidents"] == 0


# --- daemon: _op_health carries the delivery section -------------------------


def _write_lodging(root: Path) -> None:
    (root / "pipes").mkdir(parents=True)
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "pipes" / "daily.yaml").write_text(
        "cadence: '0 7 * * *'\nchannels: [push]\n"
        "render:\n"
        "  preamble:\n"
        "    - kind: structured\n      template: rate-limit-callout\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n"
        "    inputs:\n      - findings_since_last_drain\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )
    (root / "render-templates").mkdir()
    (root / "render-templates" / "rate-limit-callout.j2").write_text(
        "Suppressed:\n", encoding="utf-8"
    )


def test_op_health_includes_delivery(tmp_path) -> None:
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        daemon.catalog.record_dispatch("now", "push", [1], "sent", mark_queue=False)
        result = asyncio.run(daemon._op_health({}))
        delivery = result["delivery"]
        assert set(delivery["last_successful_send"]) == {"now", "daily"}
        assert delivery["last_successful_send"]["now"] is not None
        assert delivery["last_successful_send"]["daily"] is None
        assert delivery["failed_dispatches"]["window_hours"] == 24
        assert delivery["open_internal_incidents"] == 0
    finally:
        daemon.connection.close()


# --- render: plain text, one item per line, 'never' for unsent ---------------


def test_render_delivery_is_plain_and_one_per_line(capsys) -> None:
    from angelus.cli import _render_delivery

    _render_delivery(
        {
            "last_successful_send": {"now": "2026-06-03T12:00:00.000Z", "daily": None},
            "failed_dispatches": {"window_hours": 24, "count": 2},
            "open_internal_incidents": 1,
        }
    )
    out = capsys.readouterr().out
    lines = out.splitlines()
    assert "delivery:" in lines
    assert "    now: 2026-06-03T12:00:00.000Z" in lines
    assert "    daily: never" in lines
    assert "  failed dispatches (last 24h): 2" in lines
    assert "  open internal incidents: 1" in lines
    # Screen-reader friendly: no tables/columns (no pipe glyphs, no tabs).
    assert "|" not in out
    assert "\t" not in out


def test_render_delivery_partial_dict_does_not_emit_none_window(capsys) -> None:
    """A truthy-but-partial delivery dict (e.g. an old/hand-built shape missing
    failed_dispatches) must not render '(last Noneh)'. The window falls back to
    the default rather than printing None."""
    from angelus.cli import _render_delivery

    _render_delivery({"last_successful_send": {"now": None}})
    out = capsys.readouterr().out
    assert "Noneh" not in out
    assert "(last 24h)" in out
