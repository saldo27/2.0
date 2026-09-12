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


def test_weekend_balance_score_no_penalty_within_15_percent():
    """Deviations of <= 15% from the proportional weekend target must not penalize."""
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
    metrics = OptimizationMetrics(scheduler)

    weekend_dates = [d for d in scheduler.schedule if scheduler.date_utils.is_weekend_day(d, set())]
    assert len(weekend_dates) >= 13

    # Worker A is assigned ONLY weekend days, so the overall weekend_ratio
    # computed from the schedule is exactly 1.0 (ratio == actual/total for a
    # single worker). Setting target_shifts to actual/1.15 makes the expected
    # weekend count exactly actual/1.15, i.e. a deviation of exactly 15%
    # (the deadzone boundary), which must still score 100.
    actual_weekend_count = 13
    scheduler.worker_assignments["A"] = set(weekend_dates[:actual_weekend_count])
    scheduler.workers_data[0]["target_shifts"] = actual_weekend_count / 1.15

    assert metrics._calculate_weekend_balance_score() == 100.0


def test_weekend_balance_score_penalizes_beyond_15_percent():
    """Weekend deviations exceeding the 15% deadzone must still reduce the score."""
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
    metrics = OptimizationMetrics(scheduler)

    weekend_dates = [d for d in scheduler.schedule if scheduler.date_utils.is_weekend_day(d, set())]
    actual_weekend_count = 13
    scheduler.worker_assignments["A"] = set(weekend_dates[:actual_weekend_count])
    # 30% deviation (well beyond the 15% deadzone).
    scheduler.workers_data[0]["target_shifts"] = actual_weekend_count / 1.30

    assert metrics._calculate_weekend_balance_score() < 100.0


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
