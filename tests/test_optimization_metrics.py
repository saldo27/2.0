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


def _worker(worker_id: str, **overrides):
    worker = {
        "id": worker_id,
        "name": worker_id,
        "target_shifts": 10,
        "_raw_target": 10,
        "work_percentage": 100,
        "work_periods": "",
        "mandatory_days": "",
        "days_off": "",
        "incompatible_with": [],
        "auto_calculate_shifts": True,
        "has_cadence": False,
        "no_last_post": False,
        "only_last_post": False,
    }
    worker.update(overrides)
    return worker


def test_workload_score_uses_clique_scaled_target():
    """Incompat clique capacity caps the scoring target so structural shortfall is neutral."""
    from saldo27.schedule_builder import ScheduleBuilder
    from saldo27.structural_score import StructuralScoreBaselines

    workers = [
        _worker("A", incompatible_with=["B", "C"], target_shifts=20, _raw_target=20),
        _worker("B", incompatible_with=["A", "C"], target_shifts=20, _raw_target=20),
        _worker("C", incompatible_with=["A", "B"], target_shifts=20, _raw_target=20),
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    for w in scheduler.workers_data:
        w["target_shifts"] = 20
        w["_raw_target"] = 20

    baselines = StructuralScoreBaselines(scheduler)
    # 10-day March slice in _build_scheduler is full month — capacity large.
    # Force a short horizon by using only early March days for capacity via end_date...
    # With full March (~31 days) and targets 20*3=60 > 31, scaled < 20.
    scaled = {w["id"]: baselines.scoring_shift_target(w) for w in scheduler.workers_data}
    assert all(v < 20 for v in scaled.values())

    days = list(scheduler._get_date_range(scheduler.start_date, scheduler.end_date))
    cursor = 0
    packed = {}
    for wid, target in scaled.items():
        n = round(target)
        packed[wid] = set(days[cursor : cursor + n])
        cursor += n
    scheduler.worker_assignments = packed

    assert OptimizationMetrics(scheduler)._calculate_workload_balance_score() == 100.0


def test_post_rotation_spillover_rosell_from_no_last_not_penalized():
    """A free worker taking more Rosell because peers are no_last must not be penalised."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [
        _worker("FREE", target_shifts=8, _raw_target=8),
        _worker("NL1", no_last_post=True, target_shifts=8, _raw_target=8),
        _worker("NL2", no_last_post=True, target_shifts=8, _raw_target=8),
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    for w in scheduler.workers_data:
        w["target_shifts"] = 8
        w["_raw_target"] = 8

    # FREE always on last post; NL1/NL2 on first — structural spillover pattern.
    days = [datetime(2026, 3, i) for i in range(1, 9)]
    for i, day in enumerate(days):
        nl = "NL1" if i % 2 == 0 else "NL2"
        scheduler.schedule[day] = [nl, "FREE"]
    scheduler._synchronize_tracking_data()

    assert OptimizationMetrics(scheduler)._calculate_post_rotation_score() == 100.0


def test_fill_rate_structural_empty_counts_as_filled():
    """A hole nobody can take (all no_last on Rosell) must not reduce fill score."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [
        _worker("A", no_last_post=True),
        _worker("B", no_last_post=True),
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    day = datetime(2026, 3, 1)
    # Post 0 filled; last post empty and unfillable (both no_last).
    scheduler.schedule[day] = ["A", None]
    scheduler.worker_assignments = {"A": {day}, "B": set()}
    # B also assigned elsewhere so not available? Actually B could take post 0 only.
    # Last post: both A (already on day) and B (no_last) cannot → structural.

    metrics = OptimizationMetrics(scheduler)
    # Only score the one day for clarity: clear other days' empties by removing them.
    only = {day: scheduler.schedule[day]}
    scheduler.schedule = only
    assert metrics._calculate_fill_rate_score() == 100.0


def test_fill_rate_fillable_empty_still_penalizes():
    """An empty slot a free worker could take must still reduce fill score."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [
        _worker("A"),
        _worker("B"),
    ]
    scheduler = _build_scheduler(workers, num_shifts=2)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    day = datetime(2026, 3, 1)
    scheduler.schedule = {day: ["A", None]}
    scheduler.worker_assignments = {"A": {day}, "B": set()}

    assert OptimizationMetrics(scheduler)._calculate_fill_rate_score() < 100.0
