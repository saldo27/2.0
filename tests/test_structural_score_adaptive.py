"""Adaptive overall_quality uses structural baselines (same contract as OptMetrics)."""

from datetime import datetime

from saldo27.adaptive_iterations import AdaptiveIterationManager
from saldo27.optimization_metrics import OptimizationMetrics
from saldo27.schedule_builder import ScheduleBuilder
from saldo27.scheduler import Scheduler
from saldo27.structural_score import StructuralScoreBaselines, maximal_incompat_cliques


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


def _scheduler(workers, *, start=datetime(2026, 3, 1), end=datetime(2026, 3, 10), num_shifts=2):
    scheduler = Scheduler(
        {
            "start_date": start,
            "end_date": end,
            "num_shifts": num_shifts,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 1,
            "max_consecutive_weekends": 5,
        }
    )
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    return scheduler


def test_maximal_incompat_cliques_detects_complete_group():
    workers = [
        _worker("A", incompatible_with=["B", "C"]),
        _worker("B", incompatible_with=["A", "C"]),
        _worker("C", incompatible_with=["A", "B"]),
    ]
    cliques = maximal_incompat_cliques(workers)
    assert any(clique == frozenset({"A", "B", "C"}) for clique in cliques)


def test_adaptive_assignment_balance_respects_different_targets():
    """Two workers meeting different targets must not look unbalanced."""
    workers = [
        _worker("LOW", target_shifts=4, _raw_target=4),
        _worker("HIGH", target_shifts=8, _raw_target=8),
    ]
    scheduler = _scheduler(workers, end=datetime(2026, 3, 20))
    for w in scheduler.workers_data:
        if w["id"] == "LOW":
            w["target_shifts"] = 4
            w["_raw_target"] = 4
        else:
            w["target_shifts"] = 8
            w["_raw_target"] = 8

    low_days = [datetime(2026, 3, d) for d in (1, 5, 9, 13)]
    high_days = [datetime(2026, 3, d) for d in (2, 4, 6, 8, 10, 12, 14, 16)]
    for day in scheduler._get_date_range(scheduler.start_date, scheduler.end_date):
        scheduler.schedule[day] = [None, None]
    for day in low_days:
        scheduler.schedule[day][0] = "LOW"
    for day in high_days:
        scheduler.schedule[day][0] = "HIGH"
    scheduler._synchronize_tracking_data()

    adaptive = AdaptiveIterationManager(scheduler)
    metrics = adaptive.calculate_quality_metrics(scheduler)
    assert metrics["assignment_balance"] == 100.0
    assert metrics["target_satisfaction"] == 100.0


def test_adaptive_overall_quality_neutral_on_clique_shortfall():
    """Clique under nominal target but on scaled target should keep high overall_quality."""
    workers = [
        _worker("A", incompatible_with=["B", "C"], no_last_post=True, target_shifts=20, _raw_target=20),
        _worker("B", incompatible_with=["A", "C"], no_last_post=True, target_shifts=20, _raw_target=20),
        _worker("C", incompatible_with=["A", "B"], no_last_post=True, target_shifts=20, _raw_target=20),
        _worker("FREE", target_shifts=3, _raw_target=3),
    ]
    # Three weekdays: clique capacity 3 vs nominal 60 → scaled target 1 each.
    scheduler = _scheduler(workers, start=datetime(2026, 3, 2), end=datetime(2026, 3, 4), num_shifts=2)
    for w in scheduler.workers_data:
        if w["id"] == "FREE":
            w["target_shifts"] = 3
            w["_raw_target"] = 3
        else:
            w["target_shifts"] = 20
            w["_raw_target"] = 20

    baselines = StructuralScoreBaselines(scheduler)
    scaled = {w["id"]: baselines.scoring_shift_target(w) for w in scheduler.workers_data}
    assert scaled["A"] == 1.0

    days = list(scheduler._get_date_range(scheduler.start_date, scheduler.end_date))
    assert len(days) == 3
    for day, wid in zip(days, ("A", "B", "C"), strict=True):
        scheduler.schedule[day] = [wid, "FREE"]
    scheduler._synchronize_tracking_data()

    adaptive = AdaptiveIterationManager(scheduler)
    metrics = adaptive.calculate_quality_metrics(scheduler)
    assert metrics["coverage_percentage"] == 100.0
    assert metrics["assignment_balance"] == 100.0
    assert metrics["weekend_balance"] == 100.0
    assert metrics["overall_quality"] >= 95.0

    opt = OptimizationMetrics(scheduler)
    assert opt._calculate_workload_balance_score() == 100.0
