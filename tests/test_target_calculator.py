import logging
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


def _plain_worker(worker_id, **fields):
    worker = {
        "id": worker_id,
        "name": worker_id,
        "work_percentage": 100,
        "work_periods": "",
        "days_off": "",
        "mandatory_days": "",
        "incompatible_with": [],
        "auto_calculate_shifts": True,
    }
    worker.update(fields)
    return worker


def _march_days(*days):
    return ";".join(f"{d:02d}-03-2026" for d in days)


def test_auto_targets_reshare_slots_taken_by_mandatory_excess():
    """31 slots, three equal workers (share ≈ 10.3). A worker with 15 mandatory
    days holds 15 slots, so the other two share the remaining 16 instead of
    keeping their 10-11 each and over-subscribing the month."""
    workers = [
        _plain_worker("A", mandatory_days=_march_days(*range(1, 30, 2))),
        _plain_worker("B"),
        _plain_worker("C"),
    ]
    scheduler = _build_scheduler(workers, datetime(2026, 3, 1), datetime(2026, 3, 31))

    a, b, c = scheduler.workers_data
    assert (a["_raw_target"], a["_mandatory_count"], a["target_shifts"]) == (15, 15, 0)
    assert b["_raw_target"] == c["_raw_target"] == 8
    assert sum(w["_raw_target"] for w in scheduler.workers_data) == 31


def test_manual_mandatory_beyond_guardias_mes_is_reserved_from_the_auto_pool():
    workers = [
        _plain_worker("M", auto_calculate_shifts=False, target_shifts=1, mandatory_days=_march_days(1, 5, 9, 13, 17)),
        _plain_worker("A"),
    ]
    scheduler = _build_scheduler(workers, datetime(2026, 3, 1), datetime(2026, 3, 31))

    auto = scheduler.workers_data[1]
    assert auto["_raw_target"] == 26


def test_auto_monthly_targets_discount_the_mandatory_days_of_each_month():
    """Objective 10 (6 non-mandatory + 4 mandatory, all in March) over March and
    April: March's share (≈5.1) is mostly covered by its mandatory days, so the
    non-mandatory budget goes 1/5 rather than 3/3."""
    workers = [_plain_worker("A", mandatory_days=_march_days(2, 9, 16, 23))]
    scheduler = _build_scheduler(workers, datetime(2026, 3, 1), datetime(2026, 4, 30))
    worker = scheduler.workers_data[0]
    worker["target_shifts"] = 6

    assert TargetCalculator(scheduler)._calculate_monthly_targets() is True

    assert worker["monthly_targets"] == {"2026-03": 1, "2026-04": 5}


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


def test_mandatory_days_within_days_off_logs_warning(sample_workers_data, caplog):
    """A mandatory_days date that falls inside the worker's own days_off
    period is a configuration conflict (the worker is marked both
    unavailable and mandatorily on-shift the same day) and must be
    surfaced as a warning rather than silently accepted."""
    workers = [dict(w) for w in sample_workers_data]
    workers[0]["auto_calculate_shifts"] = False
    workers[0]["target_shifts"] = 4
    workers[0]["work_periods"] = ""
    workers[0]["mandatory_days"] = "10-06-2026"
    workers[0]["days_off"] = "01-06-2026 - 15-06-2026"

    scheduler = _build_scheduler(
        workers,
        datetime(2026, 6, 1),
        datetime(2026, 7, 31),
    )

    with caplog.at_level(logging.WARNING, logger="root"):
        assert TargetCalculator(scheduler).calculate() is True

    assert any(
        "DOC001" in record.message and "days_off" in record.message and "10-06-2026" in record.message
        for record in caplog.records
    )


def test_mandatory_days_outside_work_periods_logs_warning(sample_workers_data, caplog):
    """A mandatory_days date that falls outside all configured
    work_periods is a configuration conflict and must be surfaced as a
    warning rather than silently accepted."""
    workers = [dict(w) for w in sample_workers_data]
    workers[0]["auto_calculate_shifts"] = False
    workers[0]["target_shifts"] = 4
    workers[0]["work_periods"] = "16-06-2026 - 31-08-2026"
    workers[0]["mandatory_days"] = "10-06-2026"
    workers[0]["days_off"] = ""

    scheduler = _build_scheduler(
        workers,
        datetime(2026, 6, 1),
        datetime(2026, 8, 31),
    )

    with caplog.at_level(logging.WARNING, logger="root"):
        assert TargetCalculator(scheduler).calculate() is True

    assert any(
        "DOC001" in record.message and "work_periods" in record.message and "10-06-2026" in record.message
        for record in caplog.records
    )


def test_mandatory_days_within_availability_logs_no_warning(sample_workers_data, caplog):
    """A mandatory_days date inside the worker's normal availability must
    NOT trigger a conflict warning."""
    workers = [dict(w) for w in sample_workers_data]
    workers[0]["auto_calculate_shifts"] = False
    workers[0]["target_shifts"] = 4
    workers[0]["work_periods"] = ""
    workers[0]["mandatory_days"] = "20-06-2026"
    workers[0]["days_off"] = ""

    scheduler = _build_scheduler(
        workers,
        datetime(2026, 6, 1),
        datetime(2026, 6, 30),
    )

    with caplog.at_level(logging.WARNING, logger="root"):
        assert TargetCalculator(scheduler).calculate() is True

    assert not any("configuration conflict" in record.message for record in caplog.records)
