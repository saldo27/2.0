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
