"""Tests for OptimizationMetrics score-component deadzones and exemptions."""

from datetime import datetime

from saldo27.optimization_metrics import OptimizationMetrics
from saldo27.scheduler import Scheduler


def _build_scheduler(workers_data, num_shifts=2):
    return Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 31),
            "num_shifts": num_shifts,
            "workers_data": workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 1,
            "max_consecutive_weekends": 5,
        }
    )


def test_workload_balance_score_no_penalty_within_10_percent():
    """Deviations of <= 10% from target_shifts must not penalize the score."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
        {
            "id": "B",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers)
    # TargetCalculator recalculates target_shifts based on availability; force
    # the fixed values under test after construction.
    scheduler.workers_data[0]["target_shifts"] = 10
    scheduler.workers_data[1]["target_shifts"] = 10
    metrics = OptimizationMetrics(scheduler)

    # A is exactly 10% below target (9/10); B is exactly on target.
    scheduler.worker_assignments["A"] = {datetime(2026, 3, i) for i in range(1, 10)}
    scheduler.worker_assignments["B"] = {datetime(2026, 3, i) for i in range(10, 20)}

    assert metrics._calculate_workload_balance_score() == 100.0


def test_workload_balance_score_penalizes_beyond_10_percent():
    """Deviations exceeding the 10% deadzone must still reduce the score."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers)
    scheduler.workers_data[0]["target_shifts"] = 10
    metrics = OptimizationMetrics(scheduler)

    # 30% deviation (7/10) -> 20% excess beyond the 10% deadzone.
    scheduler.worker_assignments["A"] = {datetime(2026, 3, i) for i in range(1, 8)}

    assert metrics._calculate_workload_balance_score() < 100.0


def test_workload_balance_score_excludes_mandatory_shifts():
    """target_shifts excludes mandatory shifts: 4 free + 2 mandatory with target 4 is on target."""
    workers = [
        {
            "id": "A",
            "target_shifts": 4,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers)
    scheduler.workers_data[0]["target_shifts"] = 4
    scheduler.workers_data[0]["mandatory_days"] = "05-03-2026;10-03-2026"
    metrics = OptimizationMetrics(scheduler)

    free_dates = {datetime(2026, 3, d) for d in (1, 15, 20, 25)}
    scheduler.worker_assignments["A"] = free_dates | {datetime(2026, 3, 5), datetime(2026, 3, 10)}

    assert metrics._calculate_workload_balance_score() == 100.0


def _weekend_worker_scheduler():
    """March 2026 has 13 weekend days of 31 (fixed posts), so an objective of 31 shifts
    means 13 weekend shifts."""
    scheduler = _build_scheduler(
        [
            {
                "id": "A",
                "target_shifts": 10,
                "work_percentage": 100,
                "work_periods": "",
                "mandatory_days": "",
                "days_off": "",
            },
        ]
    )
    scheduler.workers_data[0]["_raw_target"] = 31
    weekend_dates = sorted(d for d in scheduler.schedule if scheduler.date_utils.is_weekend_day(d, set()))
    assert len(weekend_dates) == 13
    return scheduler, weekend_dates


def test_weekend_balance_score_counts_mandatory_weekends_as_assigned():
    """Mandatory weekends count in the actual weekend total like any other weekend."""
    scheduler, weekend_dates = _weekend_worker_scheduler()
    scheduler.worker_assignments["A"] = set(weekend_dates)
    scheduler.workers_data[0]["mandatory_days"] = ";".join(d.strftime("%d-%m-%Y") for d in weekend_dates[:3])

    assert OptimizationMetrics(scheduler)._calculate_weekend_balance_score() == 100.0


def test_weekend_balance_score_no_penalty_within_15_percent():
    """12 weekends against an objective of 13 (7.7%) must not penalize."""
    scheduler, weekend_dates = _weekend_worker_scheduler()
    scheduler.worker_assignments["A"] = set(weekend_dates[:12])

    assert OptimizationMetrics(scheduler)._calculate_weekend_balance_score() == 100.0


def test_weekend_balance_score_penalizes_beyond_15_percent():
    """9 weekends against an objective of 13 (31%) is beyond the deadzone."""
    scheduler, weekend_dates = _weekend_worker_scheduler()
    scheduler.worker_assignments["A"] = set(weekend_dates[:9])

    assert OptimizationMetrics(scheduler)._calculate_weekend_balance_score() < 100.0


def test_post_rotation_score_no_last_post_worker_treated_as_no_deviation():
    """Workers with no_last_post must be exempt from post-rotation penalties."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
            "no_last_post": True,
        },
    ]
    scheduler = _build_scheduler(workers, num_shifts=3)
    metrics = OptimizationMetrics(scheduler)

    # A is always assigned to post 0 and never to post 2 (last post) - a
    # structurally "unbalanced" post distribution that must not be penalized.
    dates = [datetime(2026, 3, i) for i in range(1, 11)]
    scheduler.worker_assignments["A"] = set(dates)
    for d in dates:
        scheduler.schedule[d][0] = "A"

    assert metrics._calculate_post_rotation_score() == 100.0


def test_post_rotation_score_only_last_post_worker_treated_as_no_deviation():
    """Workers with only_last_post must be exempt from post-rotation penalties."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
            "only_last_post": True,
        },
    ]
    scheduler = _build_scheduler(workers, num_shifts=3)
    metrics = OptimizationMetrics(scheduler)

    dates = [datetime(2026, 3, i) for i in range(1, 11)]
    scheduler.worker_assignments["A"] = set(dates)
    for d in dates:
        scheduler.schedule[d][2] = "A"

    assert metrics._calculate_post_rotation_score() == 100.0


def test_post_rotation_score_no_penalty_within_20_percent():
    """Regular workers (no post restriction) tolerate <= 20% post-distribution deviation."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    metrics = OptimizationMetrics(scheduler)

    # 10 shifts split 5.5/4.5 avg -> use 6/4 split: expected=5 per post,
    # mard = (|6-5| + |4-5|) / 2 / 5 = 0.2 (exactly 20%, at the deadzone boundary).
    dates = [datetime(2026, 3, i) for i in range(1, 11)]
    scheduler.worker_assignments["A"] = set(dates)
    for i, d in enumerate(dates):
        post = 0 if i < 6 else 1
        scheduler.schedule[d][post] = "A"

    assert metrics._calculate_post_rotation_score() == 100.0


def test_post_rotation_score_penalizes_beyond_20_percent():
    """Regular workers must still be penalized once deviation exceeds 20%."""
    workers = [
        {
            "id": "A",
            "target_shifts": 10,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
        },
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    metrics = OptimizationMetrics(scheduler)

    # 8/2 split: expected=5 per post, mard = (|8-5| + |2-5|) / 2 / 5 = 0.6 (60%).
    dates = [datetime(2026, 3, i) for i in range(1, 11)]
    scheduler.worker_assignments["A"] = set(dates)
    for i, d in enumerate(dates):
        post = 0 if i < 8 else 1
        scheduler.schedule[d][post] = "A"

    assert metrics._calculate_post_rotation_score() < 100.0
