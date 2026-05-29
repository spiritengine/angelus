"""Injectable clock seam.

Every "now" read in angelus -- lifecycle ages, since-last-drain windows,
retry timers, dispatch timestamps, the rendered digest date -- goes through a
Clock so tests and a sim mode can pin time instead of faking cadence by
side effects. The daemon constructs the real ``Clock`` once and threads it
into the catalog and pipe runner; everything else reads time off the catalog.

This module is the ONLY place in angelus/ that calls ``datetime.now`` /
``utcnow`` against the wall clock. Keep it that way: route new time reads
through an injected clock.

``FakeClock`` is the test/sim counterpart -- a fixed instant you can ``set``
or ``advance``.
"""

from __future__ import annotations

from datetime import UTC, datetime, tzinfo


def _iso(dt: datetime) -> str:
    """Fixed-width ISO8601 UTC string (``...Z``), matching the format the
    catalog has always written so existing rows stay comparable as text."""
    return (
        dt.astimezone(UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class Clock:
    """Real wall clock. The default everywhere outside tests/sim."""

    def now(self) -> datetime:
        """Timezone-aware current instant in UTC."""
        return datetime.now(UTC)

    def now_local(self, tz: tzinfo | None = None) -> datetime:
        """Current instant in the system local timezone (or ``tz`` if given).

        Used for the human-facing digest subject / startup TZ log line, which
        deliberately render in the operator's local calendar day.
        """
        return self.now().astimezone(tz)

    def now_iso(self) -> str:
        """Current UTC instant as the catalog's fixed-width ISO8601 string."""
        return _iso(self.now())


class FakeClock(Clock):
    """Test/sim clock pinned to an arbitrary instant.

    >>> from datetime import timedelta
    >>> c = FakeClock(datetime(2026, 5, 29, 12, 0, tzinfo=UTC))
    >>> c.now_iso()
    '2026-05-29T12:00:00.000Z'
    >>> c.advance(timedelta(hours=25))
    >>> c.now().isoformat()
    '2026-05-30T13:00:00+00:00'

    A naive instant is interpreted as UTC.
    """

    def __init__(self, instant: datetime) -> None:
        self.set(instant)

    def set(self, instant: datetime) -> None:
        if instant.tzinfo is None:
            instant = instant.replace(tzinfo=UTC)
        self._instant = instant

    def advance(self, delta) -> None:
        self._instant = self._instant + delta

    def now(self) -> datetime:
        return self._instant.astimezone(UTC)

    def now_local(self, tz: tzinfo | None = None) -> datetime:
        return self._instant.astimezone(tz)


# Shared real-clock singleton. Code or tests that genuinely do not take an
# injected clock (e.g. the catalog's default argument) read off this so the
# wall-clock call still lives in this module.
SYSTEM_CLOCK = Clock()


def utcnow() -> str:
    """Real-wall-clock UTC ISO string.

    Convenience for the few call sites that have not been threaded a clock.
    Prefer an injected ``Clock`` (``catalog._clock.now_iso()``) for anything
    whose timing a test or sim needs to control.
    """
    return SYSTEM_CLOCK.now_iso()
