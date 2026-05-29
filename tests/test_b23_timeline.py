"""B23 event-timeline: `angelus timeline` reconstructs the ordered story for
a time window (fires, observations, findings, dispatches including failures)
interleaved by timestamp, plain text, one event per line.

The load-bearing assertion is chronological order across tables: the rows are
seeded out of insertion order and across all four tables, and the rendered
lines must come back sorted by timestamp -- the fire -> dispatch-failure
sequence that today's incident postmortem depends on.
"""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from angelus.cli import main
from angelus.storage import init_db


def _seed(root: Path) -> None:
    """Seed a DB with interleaved events inserted out of chronological order
    so the test exercises the sort, not insertion order."""
    state = root / "state"
    state.mkdir()
    connection = init_db(state / "angelus.sqlite3")
    # Insert deliberately scrambled: a late dispatch first, an early fire last.
    connection.execute(
        "INSERT INTO source_fires (source_name, scheduled_at, fired_at, outcome) "
        "VALUES (?, ?, ?, ?)",
        ("scheduled/daily-drain", None, "2026-05-29T12:00:00.000Z", "ok"),
    )
    connection.execute(
        "INSERT INTO dispatches (pipe, channel, finding_ids, status, attempts, "
        "last_error, dispatched_at) VALUES (?, ?, ?, ?, 1, ?, ?)",
        (
            "daily",
            "email",
            "[1]",
            "failed",
            "email channel env var is unset: ANGELUS_EMAIL_TO",
            "2026-05-29T12:00:06.986Z",
        ),
    )
    connection.execute(
        "INSERT INTO observations (source, status, created_at) VALUES (?, ?, ?)",
        ("scheduled/daily-drain", "ready", "2026-05-29T12:00:02.000Z"),
    )
    connection.execute(
        "INSERT INTO findings (observation_id, source, type, entity, dedup_key, "
        "target_pipes, status, severity, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            1,
            "internal/dispatch",
            "channel_unhealthy",
            "email",
            "internal/dispatch:channel_unhealthy:email",
            "now",
            "ready",
            "high",
            "2026-05-29T12:00:06.989Z",
        ),
    )
    # An event well outside the queried window: must not appear.
    connection.execute(
        "INSERT INTO source_fires (source_name, scheduled_at, fired_at, outcome) "
        "VALUES (?, ?, ?, ?)",
        ("scheduled/yesterday", None, "2026-05-28T09:00:00.000Z", "ok"),
    )
    connection.commit()
    connection.close()


def test_timeline_renders_events_in_chronological_order(tmp_path) -> None:
    _seed(tmp_path)

    result = CliRunner().invoke(
        main,
        [
            "timeline",
            "--since",
            "2026-05-29T11:59:00Z",
            "--until",
            "2026-05-29T12:01:00Z",
            "--root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    # Drop the two-line header (window + count).
    event_lines = lines[2:]
    timestamps = [line.split(" ", 1)[0] for line in event_lines]
    assert timestamps == sorted(timestamps), event_lines
    assert timestamps == [
        "2026-05-29T12:00:00.000Z",  # fire
        "2026-05-29T12:00:02.000Z",  # observation
        "2026-05-29T12:00:06.986Z",  # dispatch failure
        "2026-05-29T12:00:06.989Z",  # finding raised by the failure
    ]
    # The four kinds and the failure detail all render on their own line.
    assert "fire scheduled/daily-drain ok" in event_lines[0]
    assert "observation scheduled/daily-drain (ready)" in event_lines[1]
    assert "dispatch daily/email failed: email channel env var is unset" in (
        event_lines[2]
    )
    assert "finding internal/dispatch channel_unhealthy email" in event_lines[3]
    # Out-of-window event is excluded.
    assert "yesterday" not in result.output


def test_timeline_window_shorthand_excludes_older_events(tmp_path) -> None:
    """--window looks back from --until; events before the window are dropped."""
    _seed(tmp_path)

    result = CliRunner().invoke(
        main,
        [
            "timeline",
            "--until",
            "2026-05-29T12:01:00Z",
            "--window",
            "2m",
            "--root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    # Window start is 11:59:00; the 12:00:00 fire is in, yesterday is out.
    assert "scheduled/daily-drain" in result.output
    assert "yesterday" not in result.output


def test_timeline_rejects_since_and_window_together(tmp_path) -> None:
    _seed(tmp_path)

    result = CliRunner().invoke(
        main,
        [
            "timeline",
            "--since",
            "2026-05-29T11:00:00Z",
            "--window",
            "2h",
            "--root",
            str(tmp_path),
        ],
    )

    assert result.exit_code != 0
    assert "not both" in result.output


def test_timeline_empty_window_reports_none(tmp_path) -> None:
    _seed(tmp_path)

    result = CliRunner().invoke(
        main,
        [
            "timeline",
            "--since",
            "2020-01-01T00:00:00Z",
            "--until",
            "2020-01-02T00:00:00Z",
            "--root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "events: 0" in result.output
    assert "none" in result.output
