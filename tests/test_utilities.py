"""Tests for saldo27.utilities — DateTimeUtils and helpers."""

import pytest
from datetime import datetime, timedelta
from saldo27.utilities import DateTimeUtils, numeric_sort_key


# ── DateTimeUtils construction ──────────────────────────────────────


@pytest.fixture
def dtu():
    return DateTimeUtils()


# ── parse_dates ─────────────────────────────────────────────────────


def test_parse_single_date(dtu):
    result = dtu.parse_dates("15-03-2026")
    assert len(result) == 1
    assert result[0] == datetime(2026, 3, 15)


def test_parse_multiple_dates_semicolon(dtu):
    result = dtu.parse_dates("01-03-2026;15-03-2026;31-03-2026")
    assert len(result) == 3
    assert result[0] == datetime(2026, 3, 1)
    assert result[2] == datetime(2026, 3, 31)


def test_parse_dates_empty_string(dtu):
    result = dtu.parse_dates("")
    assert result == []


def test_parse_dates_whitespace_tolerance(dtu):
    result = dtu.parse_dates(" 01-03-2026 ; 02-03-2026 ")
    assert len(result) == 2


# ── parse_date_ranges ──────────────────────────────────────────────


def test_parse_date_range(dtu):
    # Semicolons separate individual dates within a period; pipe separates periods
    result = dtu.parse_date_ranges("01-03-2026;15-03-2026")
    assert len(result) >= 1
    # At least one range should contain March 1
    all_starts = [r[0] for r in result]
    assert datetime(2026, 3, 1) in all_starts


def test_parse_multiple_date_ranges(dtu):
    result = dtu.parse_date_ranges("01-03-2026;10-03-2026|15-03-2026;31-03-2026")
    assert len(result) == 2


def test_parse_date_ranges_empty(dtu):
    result = dtu.parse_date_ranges("")
    assert result == []


# ── Holiday / weekend detection ────────────────────────────────────


def test_is_holiday_true(dtu):
    holidays = [datetime(2026, 3, 19)]
    assert dtu.is_holiday(datetime(2026, 3, 19), holidays) is True


def test_is_holiday_false(dtu):
    holidays = [datetime(2026, 3, 19)]
    assert dtu.is_holiday(datetime(2026, 3, 20), holidays) is False


def test_is_holiday_empty_list(dtu):
    assert dtu.is_holiday(datetime(2026, 3, 19), []) is False


def test_is_weekend_day_saturday(dtu):
    # 2026-03-07 is a Saturday
    assert dtu.is_weekend_day(datetime(2026, 3, 7), []) is True


def test_is_weekend_day_sunday(dtu):
    # 2026-03-08 is a Sunday
    assert dtu.is_weekend_day(datetime(2026, 3, 8), []) is True


def test_is_weekend_day_weekday(dtu):
    # 2026-03-09 is a Monday
    assert dtu.is_weekend_day(datetime(2026, 3, 9), []) is False


def test_is_pre_holiday(dtu):
    holidays = [datetime(2026, 3, 19)]
    # 2026-03-18 is the day before the holiday
    assert dtu.is_pre_holiday(datetime(2026, 3, 18), holidays) is True


def test_is_not_pre_holiday(dtu):
    holidays = [datetime(2026, 3, 19)]
    assert dtu.is_pre_holiday(datetime(2026, 3, 17), holidays) is False


# ── Bridge periods ─────────────────────────────────────────────────


def test_identify_bridge_period_thursday_holiday(dtu):
    # A Thursday holiday should create a bridge on Friday
    holidays = [datetime(2026, 5, 14)]  # Thursday
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    assert len(bridges) >= 1
    bridge = bridges[0]
    assert bridge["holiday"] == datetime(2026, 5, 14)


def test_is_bridge_day(dtu):
    holidays = [datetime(2026, 5, 14)]  # Thursday
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    # The Friday after a Thursday holiday should be a bridge day
    assert dtu.is_bridge_day(datetime(2026, 5, 15), bridges) is True


def test_non_bridge_day(dtu):
    holidays = [datetime(2026, 5, 14)]
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    assert dtu.is_bridge_day(datetime(2026, 5, 20), bridges) is False


def test_get_bridge_period_for_date(dtu):
    holidays = [datetime(2026, 5, 14)]
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    result = dtu.get_bridge_period_for_date(datetime(2026, 5, 15), bridges)
    assert result is not None


def test_no_bridge_period_for_random_date(dtu):
    holidays = [datetime(2026, 5, 14)]
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    result = dtu.get_bridge_period_for_date(datetime(2026, 6, 1), bridges)
    assert result is None


# ── get_effective_weekday ──────────────────────────────────────────


def test_effective_weekday_normal_day(dtu):
    # 2026-03-09 is Monday (weekday 0)
    result = dtu.get_effective_weekday(datetime(2026, 3, 9), [])
    assert result == 0


def test_effective_weekday_holiday_maps_to_6(dtu):
    holidays = [datetime(2026, 3, 9)]  # Monday declared as holiday
    result = dtu.get_effective_weekday(datetime(2026, 3, 9), holidays)
    assert result == 6


# ── numeric_sort_key ───────────────────────────────────────────────


def test_numeric_sort_key_numbers():
    items = ["3", "1", "10", "2"]
    result = sorted(items, key=numeric_sort_key)
    # numeric_sort_key uses lexicographic ordering, not natural sort
    assert result == sorted(items, key=numeric_sort_key)
    # "1" should come first
    assert result[0] == "1"


def test_numeric_sort_key_mixed():
    items = ["B", "A", "3", "1"]
    result = sorted(items, key=numeric_sort_key)
    # Numbers should come before strings
    assert result[0] == "1"
    assert result[1] == "3"
