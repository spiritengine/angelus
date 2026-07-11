"""Crontab day-of-week semantics: angelus honors crontab (0=Sunday), not
APScheduler's native 0=Monday.

APScheduler's CronTrigger.from_crontab forwards a numeric day-of-week straight
into its own field (0=Mon..6=Sun), so a "* * 0" Sunday cron silently fires on
MONDAY. angelus advertises crontab syntax, so _make_trigger translates the
day-of-week field to APScheduler-correct weekday names first. This regression
(freakzone-capture recorded Monday's radio, not Sunday's Freak Zone) is the
motivating case.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from angelus.daemon import (
    _make_trigger,
    _translate_crontab_dow_field,
    _crontab_max_gap_seconds,
)

TZ = ZoneInfo("America/New_York")
# 2026-07-11 is a Saturday; the next Sunday is 2026-07-12, the next Monday
# 2026-07-13 -- the dates that made the off-by-one visible.
NOW = datetime(2026, 7, 11, 6, 0, tzinfo=TZ)


def _next_day(cadence: str) -> str:
    fire = _make_trigger(cadence).get_next_fire_time(None, NOW)
    return fire.strftime("%a %Y-%m-%d %H:%M")


# --------------------------------------------------------------------------
# field translation
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field, expected",
    [
        ("0", "sun"),                                   # crontab 0 = Sunday
        ("6", "sat"),
        ("7", "sun"),                                   # 7 is also Sunday
        ("1-5", "mon,tue,wed,thu,fri"),                 # weekdays
        ("0-6", "sun,mon,tue,wed,thu,fri,sat"),         # every day
        ("1-7", "sun,mon,tue,wed,thu,fri,sat"),         # every day (7 as range end)
        ("0,3", "sun,wed"),
        ("*/2", "sun,tue,thu,sat"),                     # step from Sunday
        ("0/2", "sun,tue,thu,sat"),                     # N/step == N..Sat/step
        ("2/2", "tue,thu,sat"),
        ("*", "*"),                                     # unchanged
        ("sun", "sun"),                                 # a single name
        ("mon-fri", "mon,tue,wed,thu,fri"),             # a name range -> expanded
        ("sun-thu", "sun,mon,tue,wed,thu"),             # Sunday-initial name range
        ("0,sun", "sun"),                               # mixed number+name (deduped)
        ("sun,0", "sun"),
        ("1,mon", "mon"),
    ],
)
def test_dow_field_translation(field: str, expected: str) -> None:
    assert _translate_crontab_dow_field(field) == expected


# Malformed / wrapping fields must fail loud at load, never silently mis-schedule.
# "mon-sun"/"sat-sun" wrap around Sunday (invalid ascending crontab ranges); write
# them as a list ("sat,sun") or numeric "1-7" instead.
@pytest.mark.parametrize(
    "bad", ["8", "0-2/0", "9-10", "5-1", "mon-sun", "sat-sun", "fnord"]
)
def test_dow_field_rejects_malformed(bad: str) -> None:
    # A bad day-of-week must fail loud at load, never silently mis-schedule.
    with pytest.raises(ValueError):
        _translate_crontab_dow_field(bad)


# --------------------------------------------------------------------------
# end-to-end: the trigger fires on the crontab day, not APScheduler's
# --------------------------------------------------------------------------


def test_sunday_cron_fires_sunday_not_monday() -> None:
    # The freakzone-capture regression: "* * 0" must land on Sunday 2026-07-12.
    assert _next_day("20 14 * * 0") == "Sun 2026-07-12 14:20"
    assert _next_day("35 18 * * 0") == "Sun 2026-07-12 18:35"


def test_weekday_range_fires_mon_to_fri() -> None:
    # "1-5" is Mon-Fri in crontab; the next weekday after Sat 07-11 is Mon 07-13.
    assert _next_day("0 9 * * 1-5") == "Mon 2026-07-13 09:00"


def test_saturday_and_names_agree() -> None:
    # crontab "6" and the name "sat" must resolve to the same day.
    assert _next_day("0 9 * * 6") == _next_day("0 9 * * sat") == "Sat 2026-07-11 09:00"


def test_daily_and_star_dow_unchanged() -> None:
    # A dow of '*' (every day) is untouched -- the daily digest keeps firing daily.
    assert _next_day("0 7 * * *") == "Sat 2026-07-11 07:00"


def test_interval_cadence_unaffected() -> None:
    # Non-crontab cadences never touch the dow translation.
    trigger = _make_trigger("10m")
    assert trigger.__class__.__name__ == "IntervalTrigger"


# --------------------------------------------------------------------------
# the SLA max-gap walk uses the same translated trigger (must stay consistent)
# --------------------------------------------------------------------------


def test_sla_gap_uses_translated_dow() -> None:
    # _crontab_max_gap_seconds builds the trigger via _make_trigger, so a weekly
    # Sunday cron yields a 7-day max gap -- and it must not raise on a dow cron.
    gap = _crontab_max_gap_seconds("20 14 * * 0")
    assert gap == pytest.approx(7 * 86400, abs=1)
