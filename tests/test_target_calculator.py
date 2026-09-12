from datetime import datetime

from saldo27.scheduler import Scheduler
from saldo27.target_calculator import TargetCalculator


def _build_scheduler(workers_data, start_date, end_date, num_shifts=1):
    return Scheduler(
        {
            "start_date": start_date,
            "end_date": end_date,
            "num_shifts": num_shifts,
            "workers_data": workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )


def test_manual_worker_monthly_targets_are_not_skewed_by_days_in_month(sample_workers_data):
    """A manual worker with a fixed guardias/mes of 3 spanning two full
    months (March=31 days, April=30 days) must get exactly 3 shifts in each
    month. Proportionally splitting the raw *total* target by available days
    per month previously produced e.g. 4 in March and 2 in April instead of
    3/3, since the days-in-month ratio (31/61 vs 30/61) skews rounding away
    from the exact per-month quota the manual worker was configured with."""
    workers = [dict(w) for w in sample_workers_data]
    workers[0]["auto_calculate_shifts"] = False
    workers[0]["target_shifts"] = 3
    workers[0]["work_periods"] = ""
    workers[0]["mandatory_days"] = ""
    workers[0]["days_off"] = ""

    scheduler = _build_scheduler(
        workers,
        datetime(2026, 3, 1),
        datetime(2026, 4, 30),
    )

    assert TargetCalculator(scheduler).calculate() is True

    worker = next(w for w in scheduler.workers_data if w["id"] == "DOC001")
    assert worker["monthly_targets"]["2026-03"] == 3
    assert worker["monthly_targets"]["2026-04"] == 3


def test_manual_worker_monthly_targets_prorated_for_vacation_days_off(sample_workers_data):
    """A manual worker with guardias/mes=4 who is on vacation (days_off) for
    the first half of June (1-15) must get their June target prorated down
    to ~half (2), while July/August (fully available) keep the full 4/mes."""
    workers = [dict(w) for w in sample_workers_data]
    workers[0]["auto_calculate_shifts"] = False
    workers[0]["target_shifts"] = 4
    workers[0]["work_periods"] = ""
    workers[0]["mandatory_days"] = ""
    workers[0]["days_off"] = "01-06-2026 - 15-06-2026"

    scheduler = _build_scheduler(
        workers,
        datetime(2026, 6, 1),
        datetime(2026, 8, 31),
    )

    assert TargetCalculator(scheduler).calculate() is True

    worker = next(w for w in scheduler.workers_data if w["id"] == "DOC001")
    # June has 30 days; 15 available (16-30) → 4 * 15/30 = 2
    assert worker["monthly_targets"]["2026-06"] == 2
    assert worker["monthly_targets"]["2026-07"] == 4
    assert worker["monthly_targets"]["2026-08"] == 4


def test_manual_worker_monthly_targets_prorated_for_out_of_work_period(sample_workers_data):
    """Being outside the configured work_period for part of a month must
    prorate the guardias/mes target the same way vacation (days_off) does."""
    workers = [dict(w) for w in sample_workers_data]
    workers[0]["auto_calculate_shifts"] = False
    workers[0]["target_shifts"] = 4
    workers[0]["work_periods"] = "16-06-2026 - 31-08-2026"
    workers[0]["mandatory_days"] = ""
    workers[0]["days_off"] = ""

    scheduler = _build_scheduler(
        workers,
        datetime(2026, 6, 1),
        datetime(2026, 8, 31),
    )

    assert TargetCalculator(scheduler).calculate() is True

    worker = next(w for w in scheduler.workers_data if w["id"] == "DOC001")
    assert worker["monthly_targets"]["2026-06"] == 2
    assert worker["monthly_targets"]["2026-07"] == 4
    assert worker["monthly_targets"]["2026-08"] == 4


def test_auto_worker_monthly_targets_prorated_for_vacation_days_off(sample_workers_data):
    """Automatic-shift workers (auto_calculate_shifts=True) must have the
    SAME target/available-days proportionality applied per month as manual
    workers: a worker on vacation (days_off) for part of a month must get
    that month's share of their target reduced accordingly, not split by
    raw days-in-month alone."""
    workers = [dict(w) for w in sample_workers_data][:1]
    workers[0]["auto_calculate_shifts"] = True
    workers[0]["work_periods"] = ""
    workers[0]["mandatory_days"] = ""
    workers[0]["days_off"] = "01-06-2026 - 15-06-2026"

    scheduler = _build_scheduler(
        workers,
        datetime(2026, 6, 1),
        datetime(2026, 7, 31),
    )

    calculator = TargetCalculator(scheduler)
    # Isolate the per-month distribution logic under test: override the
    # already-computed overall total (set by Scheduler.__init__) with a
    # known value, independent of other workers' relative weights.
    worker = next(w for w in scheduler.workers_data if w["id"] == "DOC001")
    worker["target_shifts"] = 6
    assert calculator._calculate_monthly_targets() is True

    # June: 30 days, 15 available (16-30) after removing vacation.
    # July: 31 days, all available. Total available = 46.
    # 6 * 15/46 ≈ 1.96 → 2 ; 6 * 31/46 ≈ 4.04 → 4. Sums to 6 (the total).
    # Without days_off-awareness this would incorrectly yield 3/3 (based on
    # raw days-in-month 30/31 instead of the vacation-adjusted 15/31).
    assert worker["monthly_targets"]["2026-06"] == 2
    assert worker["monthly_targets"]["2026-07"] == 4
    assert worker["monthly_targets"]["2026-06"] + worker["monthly_targets"]["2026-07"] == 6


def test_manual_worker_monthly_targets_honour_guardias_mes_over_three_months(sample_workers_data):
    """Same scenario across three consecutive full months of differing
    lengths (31/30/31 days) must still yield exactly 3/3/3, not skewed
    towards the longer months."""
    workers = [dict(w) for w in sample_workers_data]
    workers[0]["auto_calculate_shifts"] = False
    workers[0]["target_shifts"] = 3
    workers[0]["work_periods"] = ""
    workers[0]["mandatory_days"] = ""
    workers[0]["days_off"] = ""

    scheduler = _build_scheduler(
        workers,
        datetime(2026, 3, 1),
        datetime(2026, 5, 31),
    )

    assert TargetCalculator(scheduler).calculate() is True

    worker = next(w for w in scheduler.workers_data if w["id"] == "DOC001")
    assert worker["monthly_targets"]["2026-03"] == 3
    assert worker["monthly_targets"]["2026-04"] == 3
    assert worker["monthly_targets"]["2026-05"] == 3
