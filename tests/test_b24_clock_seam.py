"""B24 clock-seam: a fake clock controls every timestamp/window read.

These tests pin a FakeClock and assert that catalog timestamps, expiry
windows, retry timers, and the runner's rendered digest date all observe the
injected instant rather than the wall clock.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import angelus.pipes.runner as pipe_runner
from angelus.clock import Clock, FakeClock
from angelus.lodging import Channel, Pipe
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db

PINNED = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)


def test_fake_clock_set_and_advance() -> None:
    clock = FakeClock(PINNED)
    assert clock.now() == PINNED
    assert clock.now_iso() == "2026-01-15T12:00:00.000Z"

    clock.advance(timedelta(hours=25))
    assert clock.now_iso() == "2026-01-16T13:00:00.000Z"

    clock.set(datetime(2030, 6, 1, tzinfo=UTC))
    assert clock.now_iso() == "2030-06-01T00:00:00.000Z"

    # A naive instant is interpreted as UTC.
    naive = FakeClock(datetime(2026, 1, 15, 12, 0, 0))
    assert naive.now_iso() == "2026-01-15T12:00:00.000Z"


def test_catalog_timestamp_uses_injected_clock(tmp_path: Path) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    clock = FakeClock(PINNED)
    catalog = Catalog(connection, tmp_path, clock=clock)
    try:
        catalog.record_source_fire("scheduled/test", None, "ok")
        first = connection.execute(
            "SELECT fired_at FROM source_fires ORDER BY id DESC LIMIT 1"
        ).fetchone()["fired_at"]
        assert first == "2026-01-15T12:00:00.000Z"

        # Advancing the clock moves the next stamped row -- nothing reads the
        # wall clock.
        clock.advance(timedelta(days=2, hours=3))
        catalog.record_source_fire("scheduled/test", None, "ok")
        second = connection.execute(
            "SELECT fired_at FROM source_fires ORDER BY id DESC LIMIT 1"
        ).fetchone()["fired_at"]
        assert second == "2026-01-17T15:00:00.000Z"
    finally:
        connection.close()


def test_mute_window_observes_injected_clock(tmp_path: Path) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    clock = FakeClock(PINNED)
    catalog = Catalog(connection, tmp_path, clock=clock)
    try:
        expires_at = catalog.add_mute("down:example", duration_seconds=3600, comment=None)
        assert expires_at == "2026-01-15T13:00:00.000Z"

        # Still inside the window at the pinned instant.
        assert catalog.is_muted("down:example") is True

        # Advance just shy of expiry: still muted.
        clock.advance(timedelta(minutes=59))
        assert catalog.is_muted("down:example") is True

        # Advance past expiry: the window closes purely because the clock
        # moved, with no GC sweep.
        clock.advance(timedelta(minutes=2))
        assert catalog.is_muted("down:example") is False
    finally:
        connection.close()


def test_retry_timer_window_uses_injected_clock(tmp_path: Path) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    clock = FakeClock(PINNED)
    catalog = Catalog(connection, tmp_path, clock=clock)
    try:
        finding_id = catalog.write_finding(
            None,
            {
                "source": "scheduled/test",
                "type": "down",
                "entity": "site",
                "severity": "high",
                "target_pipes": ["daily"],
            },
            {"daily"},
        )
        exhausted = catalog.record_pipe_send_failure("daily", "push", finding_id, "boom")
        assert exhausted is False

        row = connection.execute(
            "SELECT next_attempt_at FROM pipe_queues WHERE finding_id = ? AND pipe = ?",
            (finding_id, "daily"),
        ).fetchone()
        # First retry delay is 1 minute past the injected instant.
        assert row["next_attempt_at"] == "2026-01-15T12:01:00.000Z"
    finally:
        connection.close()


def _daily_email_pipe() -> Pipe:
    return Pipe(
        name="daily",
        cadence="0 8 * * *",
        render_kind="digest",
        template=None,
        channels=["email"],
        render={
            "preamble": [{"kind": "structured", "template": "incident-status"}],
            "body": {"kind": "llm", "mantle": "chronicler", "inputs": []},
        },
    )


def _write_templates(root: Path) -> None:
    (root / "render-templates").mkdir()
    (root / "render-templates" / "incident-status.j2").write_text(
        "Incidents:\n{% for incident in open_incidents %}{{ incident.entity }}\n{% endfor %}",
        encoding="utf-8",
    )


def test_digest_subject_date_comes_from_injected_clock(tmp_path: Path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    clock = FakeClock(PINNED)
    catalog = Catalog(connection, tmp_path, clock=clock)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_email_pipe(),
        {
            "email": Channel(
                name="email",
                kind="email",
                command="/bin/true",
                to="person@example.com",
            )
        },
        tmp_path,
        {"daily"},
        clock=clock,
    )
    catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "down",
            "entity": "site",
            "severity": "high",
            "target_pipes": ["daily"],
        },
        {"daily"},
    )

    captured: list[str] = []

    async def fake_email(_channel, subject: str, _message: str, _workdir: Path) -> None:
        captured.append(subject)

    async def fake_llm(self, _pipe, _structured):
        return "synthesis paragraph", None

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert captured, "digest did not dispatch"
    # The subject renders in local time off the injected clock. Derive the
    # expectation from the same clock so the assertion is TZ-agnostic; the
    # pinned January date proves it is the fake instant, not today's wall
    # clock (the suite runs in May 2026).
    local = clock.now_local()
    expected = f"{local.strftime('%A %B')} {local.day}, {local.year}"
    assert expected in captured[0]
    assert "January" in captured[0]


def test_catalog_defaults_to_real_clock(tmp_path: Path) -> None:
    """No injected clock -> real wall clock, so stamps land near now."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    try:
        catalog.record_source_fire("scheduled/test", None, "ok")
        stamped = connection.execute(
            "SELECT fired_at FROM source_fires ORDER BY id DESC LIMIT 1"
        ).fetchone()["fired_at"]
        parsed = datetime.fromisoformat(stamped.replace("Z", "+00:00"))
        assert abs((Clock().now() - parsed).total_seconds()) < 60
    finally:
        connection.close()
