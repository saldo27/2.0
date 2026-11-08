"""Tests for saldo27.utilities — DateTimeUtils and helpers."""

from datetime import datetime, timedelta

import pytest

from saldo27.constraint_checker import candidate_spacing_rejection
from saldo27.utilities import (
    DateTimeUtils,
    apportion_largest_remainder,
    format_gap_periods_text,
    get_effective_min_gap,
    normalize_gap_periods,
    numeric_sort_key,
    parse_gap_periods_text,
)


def test_apportion_largest_remainder_keeps_the_total_and_favours_largest_fractions():
    # exact 1.6 / 1.6 / 1.8: rounding each share in sequence would give 2/2/1
    assert apportion_largest_remainder(5, [8, 8, 9]) == [2, 1, 2]


def test_apportion_largest_remainder_breaks_ties_by_index():
    assert apportion_largest_remainder(2, [1, 1, 1]) == [1, 1, 0]


def test_apportion_largest_remainder_without_weight_or_total_gives_zeros():
    assert apportion_largest_remainder(4, [0, 0]) == [0, 0]
    assert apportion_largest_remainder(0, [1, 2]) == [0, 0]


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
    # Thursday holiday: Wednesday prefestivo through Sunday
    holidays = [datetime(2026, 5, 14)]  # Thursday
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    assert len(bridges) >= 1
    bridge = bridges[0]
    assert bridge["holiday"] == datetime(2026, 5, 14)
    assert bridge["start_date"] == datetime(2026, 5, 13)  # Wednesday prefestivo
    assert bridge["end_date"] == datetime(2026, 5, 17)


def test_friday_holiday_includes_thursday_prefestivo(dtu):
    holidays = [datetime(2026, 5, 15)]  # Friday
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    assert len(bridges) == 1
    assert bridges[0]["start_date"] == datetime(2026, 5, 14)  # Thursday prefestivo
    assert bridges[0]["end_date"] == datetime(2026, 5, 17)
    assert dtu.is_bridge_day(datetime(2026, 5, 14), bridges) is True


def test_monday_holiday_prolongs_weekend_from_saturday(dtu):
    holidays = [datetime(2026, 5, 4)]  # Monday
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    assert len(bridges) == 1
    assert bridges[0]["start_date"] == datetime(2026, 5, 2)  # Saturday
    assert bridges[0]["end_date"] == datetime(2026, 5, 4)
    assert dtu.is_bridge_day(datetime(2026, 5, 1), bridges) is False  # Friday out


def test_tuesday_holiday_prolongs_weekend_from_saturday(dtu):
    holidays = [datetime(2026, 5, 5)]  # Tuesday
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    assert len(bridges) == 1
    assert bridges[0]["start_date"] == datetime(2026, 5, 2)  # Saturday
    assert bridges[0]["end_date"] == datetime(2026, 5, 5)


def test_reyes_midweek_cluster_is_not_a_bridge(dtu):
    """5-7 Jan 2027 (Tue-Thu) sit mid-week with Reyes on Wednesday: no puente."""
    holidays = [
        datetime(2027, 1, 5),
        datetime(2027, 1, 6),
        datetime(2027, 1, 7),
    ]
    bridges = dtu.identify_bridge_periods(holidays, 2027)
    assert bridges == []
    for day in (
        datetime(2027, 1, 4),
        datetime(2027, 1, 5),
        datetime(2027, 1, 7),
        datetime(2027, 1, 8),
        datetime(2027, 1, 9),
        datetime(2027, 1, 10),
    ):
        assert dtu.is_bridge_day(day, bridges) is False


def test_is_bridge_day(dtu):
    holidays = [datetime(2026, 5, 14)]  # Thursday
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    assert dtu.is_bridge_day(datetime(2026, 5, 13), bridges) is True  # prefestivo
    assert dtu.is_bridge_day(datetime(2026, 5, 15), bridges) is True  # Friday


def test_non_bridge_day(dtu):
    holidays = [datetime(2026, 5, 14)]
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    assert dtu.is_bridge_day(datetime(2026, 5, 20), bridges) is False


def test_get_bridge_period_for_date(dtu):
    holidays = [datetime(2026, 5, 14)]
    bridges = dtu.identify_bridge_periods(holidays, 2026)
    result = dtu.get_bridge_period_for_date(datetime(2026, 5, 15), bridges)
    assert result is not None


def test_bridge_periods_include_holidays_after_the_start_year(dtu):
    # 19-03-2027 is a Friday holiday; Thursday 18th is its prefestivo.
    holidays = [datetime(2026, 12, 25), datetime(2027, 3, 19)]
    bridges = dtu.identify_bridge_periods_for_span(holidays, datetime(2026, 12, 23), datetime(2027, 6, 30))
    assert dtu.is_bridge_day(datetime(2027, 3, 18), bridges) is True
    assert dtu.is_bridge_day(datetime(2027, 3, 19), bridges) is True


def test_nav26_bridge_days_exclude_reyes_false_bridges(dtu):
    holidays = [
        datetime(2026, 12, 24),
        datetime(2026, 12, 25),
        datetime(2026, 12, 31),
        datetime(2027, 1, 1),
        datetime(2027, 1, 5),
        datetime(2027, 1, 6),
        datetime(2027, 1, 7),
        datetime(2027, 3, 19),
        datetime(2027, 3, 25),
        datetime(2027, 3, 26),
        datetime(2027, 5, 1),
        datetime(2027, 6, 9),
    ]
    bridges = dtu.identify_bridge_periods_for_span(holidays, datetime(2026, 12, 23), datetime(2027, 6, 30))
    expected = {
        # Navidad (23 = prefestivo del 24 jueves)
        datetime(2026, 12, 23),
        datetime(2026, 12, 24),
        datetime(2026, 12, 25),
        datetime(2026, 12, 26),
        datetime(2026, 12, 27),
        # Año Nuevo (30 = prefestivo del 31 jueves)
        datetime(2026, 12, 30),
        datetime(2026, 12, 31),
        datetime(2027, 1, 1),
        datetime(2027, 1, 2),
        datetime(2027, 1, 3),
        # San José (18 = prefestivo del 19 viernes)
        datetime(2027, 3, 18),
        datetime(2027, 3, 19),
        datetime(2027, 3, 20),
        datetime(2027, 3, 21),
        # Semana Santa (24 = prefestivo del 25 jueves)
        datetime(2027, 3, 24),
        datetime(2027, 3, 25),
        datetime(2027, 3, 26),
        datetime(2027, 3, 27),
        datetime(2027, 3, 28),
    }
    got = set()
    day = datetime(2026, 12, 23)
    while day <= datetime(2027, 6, 30):
        if dtu.is_bridge_day(day, bridges):
            got.add(day)
        day += timedelta(days=1)
    assert got == expected


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


# ── Effective gap ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("work_percentage", "auto", "expected"),
    [(100, True, 4), (60, True, 4), (59, True, 5), (50, False, 5), (100, False, 4)],
)
def test_effective_gap_adds_one_below_sixty_percent(work_percentage, auto, expected):
    worker = {"work_percentage": work_percentage, "auto_calculate_shifts": auto}
    assert get_effective_min_gap(worker, 4) == expected


def test_gap_four_allows_monday_friday_but_not_monday_thursday():
    monday = datetime(2026, 3, 2)
    kwargs = {"min_gap": 4, "date_is_mandatory": False, "block_gap2_weekend_pairs": False}
    assert candidate_spacing_rejection(datetime(2026, 3, 6), [monday], **kwargs) is None
    rejection = candidate_spacing_rejection(datetime(2026, 3, 5), [monday], **kwargs)
    assert rejection is not None and rejection.reason == "gap"


def test_gap_periods_use_the_stricter_date_and_keep_the_part_time_extra_day():
    periods = [
        {"start_date": "2026-03-01", "end_date": "2026-03-10", "gap": 2},
        {"start_date": datetime(2026, 3, 25), "end_date": datetime(2026, 3, 20), "gap": 5},
    ]
    normalized = normalize_gap_periods(periods)
    assert normalized[1]["start_date"] == datetime(2026, 3, 20)
    assert normalized[1]["end_date"] == datetime(2026, 3, 25)

    full_time = {"work_percentage": 100}
    inside = datetime(2026, 3, 2)
    also_inside = datetime(2026, 3, 5)
    outside = datetime(2026, 3, 16)
    assert get_effective_min_gap(full_time, 4, date=inside, other_date=also_inside, gap_periods=normalized) == 2
    assert (
        get_effective_min_gap(full_time, 4, date=outside, other_date=datetime(2026, 3, 19), gap_periods=normalized) == 4
    )
    assert get_effective_min_gap(full_time, 4, date=inside, other_date=outside, gap_periods=normalized) == 4

    part_time = {"work_percentage": 50}
    assert get_effective_min_gap(part_time, 4, date=inside, other_date=also_inside, gap_periods=normalized) == 3

    # A later overlapping period replaces the earlier one.
    overlapping = [
        *normalized,
        {"start_date": datetime(2026, 3, 1), "end_date": datetime(2026, 3, 10), "gap": 6},
    ]
    assert get_effective_min_gap(full_time, 4, date=inside, other_date=also_inside, gap_periods=overlapping) == 6


def test_parse_gap_periods_text_round_trips_and_reports_bad_lines():
    text = "01-08-2026 / 31-08-2026: 2\n15-12-2026: 1\nno-es-una-fecha: 3\n01-01-2026 / 02-01-2026: 9"
    periods, warnings = parse_gap_periods_text(text)
    assert len(periods) == 2
    assert periods[0]["gap"] == 2
    assert periods[1]["start_date"] == datetime(2026, 12, 15)
    assert len(warnings) == 2
    assert parse_gap_periods_text(format_gap_periods_text(periods))[0] == periods


def test_gap_for_other_applies_each_neighbours_threshold():
    monday = datetime(2026, 3, 2)
    thursday = datetime(2026, 3, 5)

    def lookup(other):
        return 4 if other == monday else 2

    rejection = candidate_spacing_rejection(
        thursday,
        [monday],
        min_gap=2,
        date_is_mandatory=False,
        block_gap2_weekend_pairs=False,
        gap_for_other=lookup,
    )
    assert rejection is not None
    assert rejection.reason == "gap"
    assert rejection.min_gap == 4
    assert (
        candidate_spacing_rejection(
            thursday,
            [monday],
            min_gap=4,
            date_is_mandatory=False,
            block_gap2_weekend_pairs=False,
            gap_for_other=lambda _other: 2,
        )
        is None
    )
