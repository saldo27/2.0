"""Tests for saldo27.tolerance_policy — the shared shift-count tolerance contract."""

from datetime import datetime

import pytest

from saldo27 import tolerance_policy
from saldo27.utilities import DateTimeUtils

AUTO = {"id": "A", "auto_calculate_shifts": True, "work_percentage": 100}
MANUAL = {"id": "M", "auto_calculate_shifts": False, "work_percentage": 100}


def test_full_time_bounds_strict_and_relaxed():
    assert tolerance_policy.shift_bounds(AUTO, 30, relaxed=False) == (27, 33)
    assert tolerance_policy.shift_bounds(AUTO, 30, relaxed=True) == (27, 33)
    assert tolerance_policy.shift_bounds(AUTO, 20, relaxed=False) == (18, 22)
    assert tolerance_policy.shift_bounds(AUTO, 20, relaxed=True) == (18, 22)
    assert tolerance_policy.shift_bounds(AUTO, 40, relaxed=True) == (35, 45)


def test_bounds_absorb_float_noise():
    # 30 * 0.9 == 27.000000000000004 would otherwise ceil to 28
    assert tolerance_policy.shift_bounds(AUTO, 30, relaxed=False)[0] == 27


def test_manual_worker_is_exact():
    assert tolerance_policy.tolerance_fraction(MANUAL) == 0.0
    assert tolerance_policy.shift_bounds(MANUAL, 6) == (6, 6)
    assert tolerance_policy.balance_band(MANUAL, 6) == 0


def test_part_time_fraction_scales_with_floor():
    half = {**AUTO, "work_percentage": 50}
    tiny = {**AUTO, "work_percentage": 20}
    assert tolerance_policy.tolerance_fraction(half, relaxed=True) == pytest.approx(0.065)
    assert tolerance_policy.tolerance_fraction(tiny, relaxed=True) == tolerance_policy.MIN_PART_TIME_TOLERANCE


def test_non_positive_target_has_no_room():
    assert tolerance_policy.shift_bounds(AUTO, 0) == (0, 0)
    assert tolerance_policy.balance_band(AUTO, 0) == 0


def test_balance_band_is_one_when_hard_limit_allows_it():
    assert tolerance_policy.balance_band(AUTO, 14) == 1


def test_balance_band_shrinks_to_zero_for_small_targets():
    # 7 * 1.13 = 7.91 → the hard limit already forbids an 8th shift
    assert tolerance_policy.balance_band(AUTO, 7) == 0


def test_balance_band_never_exceeds_requested_band():
    assert tolerance_policy.balance_band(AUTO, 40, band=1) == 1
    assert tolerance_policy.balance_band(AUTO, 40, band=3) == 3
    assert tolerance_policy.balance_band(AUTO, 40, band=9) == 5


def test_monthly_ceiling_manual_exact_auto_plus_margin():
    assert tolerance_policy.monthly_ceiling(MANUAL, 3) == 3
    assert tolerance_policy.monthly_ceiling(AUTO, 3) == 3 + tolerance_policy.MONTHLY_MARGIN


def test_monthly_cap_gives_part_time_auto_workers_the_same_margin():
    part_time = {**AUTO, "work_percentage": 50, "monthly_targets": {"2026-03": 2}}

    assert tolerance_policy.monthly_cap(part_time, 2026, 3) == 3


def test_monthly_cap_is_none_without_month_target_or_for_cadence():
    assert tolerance_policy.monthly_cap({**AUTO, "monthly_targets": {}}, 2026, 3) is None
    cadence = {**MANUAL, "has_cadence": True, "monthly_targets": {"2026-03": 0}}
    assert tolerance_policy.monthly_cap(cadence, 2026, 3) is None


def test_exceeds_monthly_cap_ignores_mandatory_shifts():
    manual = {**MANUAL, "monthly_targets": {"2026-03": 2}}
    mandatory = {datetime(2026, 3, 2), datetime(2026, 3, 9)}
    held = [*mandatory, datetime(2026, 3, 16)]

    assert tolerance_policy.exceeds_monthly_cap(manual, datetime(2026, 3, 23), held, mandatory) is False
    held.append(datetime(2026, 3, 23))
    assert tolerance_policy.exceeds_monthly_cap(manual, datetime(2026, 3, 30), held, mandatory) is True
    assert tolerance_policy.exceeds_monthly_cap(manual, datetime(2026, 3, 9), held, mandatory) is False


def test_exceeds_monthly_cap_accepts_assignments_that_already_hold_the_date():
    auto = {**AUTO, "monthly_targets": {"2026-03": 1}}
    day = datetime(2026, 3, 10)

    assert tolerance_policy.exceeds_monthly_cap(auto, day, [datetime(2026, 3, 1), day], set()) is False


def test_monthly_pace_spreads_the_target_up_to_the_last_available_day():
    # GABI: 3/mes, available until the 20th → 3 by the 20th, not ceil(3*20/31) = 2
    assert tolerance_policy.monthly_pace_cap(3, 20, 20) == 3
    assert tolerance_policy.monthly_pace_cap(3, 20, 31) == 2
    assert tolerance_policy.monthly_pace_cap(3, 1, 31) == 1
    assert tolerance_policy.monthly_pace_cap(0, 15, 31) == 0


def test_last_available_day_considers_work_periods_and_days_off():
    utils = DateTimeUtils()

    assert utils.last_available_day_in_month("", "", 2026, 2) == 28
    assert utils.last_available_day_in_month("01-03-2026 - 20-03-2026", "", 2026, 3) == 20
    assert utils.last_available_day_in_month("", "22-03-2026 - 05-04-2026", 2026, 3) == 21
    assert utils.last_available_day_in_month("01-04-2026 - 30-04-2026", "", 2026, 3) == 0
