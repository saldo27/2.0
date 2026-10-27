"""Unit tests for ScheduleBuilder workload balance and tolerance enforcement."""

from __future__ import annotations

from datetime import datetime

from saldo27.advanced_distribution_engine import AdvancedDistributionEngine
from saldo27.schedule_builder import ScheduleBuilder
from saldo27.scheduler import Scheduler


def _worker(worker_id: str, mandatory_days: str = "") -> dict:
    return {
        "id": worker_id,
        "name": worker_id,
        "target_shifts": 6,
        "_raw_target": 6,
        "work_percentage": 100,
        "work_periods": "",
        "days_off": "",
        "mandatory_days": mandatory_days,
        "incompatible_with": [],
        "is_incompatible_all": False,
        "auto_calculate_shifts": True,
    }


def _scheduler_with(assignments: dict[str, list[int]], workers: list[dict], targets: dict[str, int]) -> Scheduler:
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 31),
            "num_shifts": 1,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 1,
            "max_consecutive_weekends": 5,
        }
    )
    # Init replaces the targets with period-wide figures; pin the scenario targets.
    for worker in scheduler.workers_data:
        worker["target_shifts"] = targets[worker["id"]]
        worker["_raw_target"] = targets[worker["id"]]
        worker["monthly_targets"] = {"2026-03": targets[worker["id"]]}
    for day in scheduler.schedule:
        scheduler.schedule[day] = [None]
    for worker_id, days in assignments.items():
        for day in days:
            scheduler.schedule[datetime(2026, 3, day)] = [worker_id]
    scheduler._synchronize_tracking_data()
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    return scheduler


def _counts(scheduler: Scheduler) -> dict[str, int]:
    return {wid: len(days) for wid, days in scheduler.worker_assignments.items() if days}


def test_balance_workloads_does_not_treat_mandatory_shifts_as_excess():
    """6 free + 2 mandatory against a free target of 6 is on target, so A gives nothing away."""
    workers = [_worker("A", mandatory_days="02-03-2026;05-03-2026"), _worker("B")]
    scheduler = _scheduler_with(
        {"A": [2, 5, 8, 11, 14, 17, 20, 23], "B": [25, 27, 29, 31]},
        workers,
        {"A": 6, "B": 6},
    )

    scheduler.schedule_builder._balance_workloads()

    assert _counts(scheduler) == {"A": 8, "B": 4}


def test_balance_workloads_moves_shifts_until_both_are_inside_the_band():
    workers = [_worker("A"), _worker("B")]
    # Targets of 8 give a ±1 band; A is +2, B is -2 and far from A's days.
    scheduler = _scheduler_with(
        {"A": list(range(1, 11)), "B": [17, 19, 21, 23, 25, 27]},
        workers,
        {"A": 8, "B": 8},
    )

    scheduler.schedule_builder._balance_workloads()

    counts = _counts(scheduler)
    assert counts["A"] + counts["B"] == 16
    assert abs(counts["A"] - 8) <= 1
    assert abs(counts["B"] - 8) <= 1


def _manual_zero_scheduler() -> Scheduler:
    manual = {**_worker("M", mandatory_days="10-03-2026"), "auto_calculate_shifts": False}
    scheduler = _scheduler_with({}, [manual, _worker("B")], {"M": 0, "B": 6})
    scheduler.workers_data[0]["monthly_targets"] = {"2026-03": 0}
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    return scheduler


def test_tolerance_blocks_free_shift_for_manual_worker_with_zero_target():
    builder = _manual_zero_scheduler().schedule_builder

    assert builder._would_violate_tolerance("M", datetime(2026, 3, 3), allow_relaxation=True) is True
    assert builder._would_violate_tolerance("M", datetime(2026, 3, 10), allow_relaxation=True) is False


def test_every_monthly_check_gives_a_part_time_worker_the_same_ceiling():
    """Target 4 in March → ceiling 5 non-mandatory shifts in every engine, mandatory not counted."""
    part_time = {**_worker("P", mandatory_days="01-03-2026"), "work_percentage": 50}
    scheduler = _scheduler_with({"P": [1, 4, 8, 12, 16, 20]}, [part_time, _worker("B")], {"P": 4, "B": 6})
    builder = scheduler.schedule_builder
    held = scheduler.worker_assignments["P"]

    sixth_free = datetime(2026, 3, 28)
    simulated = {**scheduler.worker_assignments, "P": held | {sixth_free}}
    assert builder._would_exceed_monthly_limit("P", sixth_free) is True
    assert builder._check_constraints_on_simulated("P", sixth_free, 0, scheduler.schedule, simulated) is False
    assert builder._calculate_monthly_target_score(part_time, sixth_free, 0) == float("-inf")

    scheduler.schedule[datetime(2026, 3, 20)] = [None]
    scheduler._synchronize_tracking_data()
    simulated = {**scheduler.worker_assignments, "P": scheduler.worker_assignments["P"] | {sixth_free}}
    assert builder._would_exceed_monthly_limit("P", sixth_free) is False
    assert builder._check_constraints_on_simulated("P", sixth_free, 0, scheduler.schedule, simulated) is True


def test_advanced_fill_keeps_manual_worker_at_exact_monthly_target():
    scheduler = _manual_zero_scheduler()
    engine = AdvancedDistributionEngine(scheduler, scheduler.schedule_builder)

    assert engine._try_assign_with_validation("M", datetime(2026, 3, 3), 0) is False
    assert engine._try_assign_with_validation("B", datetime(2026, 3, 3), 0) is True


def test_manual_deficit_takes_auto_who_is_over_period_but_not_over_month():
    """SALVA case: the only valid day is full, the occupant is at monthly target
    but over the period target. The repair must displace them."""
    manual = {
        **_worker("M"),
        "auto_calculate_shifts": False,
        "work_periods": "03-03-2026 - 03-03-2026",
    }
    auto = _worker("A")
    scheduler = _scheduler_with({"A": [3, 10]}, [manual, auto], {"M": 1, "A": 1})
    for worker in scheduler.workers_data:
        if worker["id"] == "A":
            worker["monthly_targets"] = {"2026-03": 2}
        else:
            worker["monthly_targets"] = {"2026-03": 1}
    builder = scheduler.schedule_builder

    assert builder._monthly_fill_deficit("M", scheduler.workers_data[0], (2026, 3), strict_donor=False) is True
    assert scheduler.schedule[datetime(2026, 3, 3)] == ["M"]
    assert datetime(2026, 3, 3) not in scheduler.worker_assignments["A"]
    assert len(scheduler.worker_assignments["M"]) == 1


def test_manual_deficit_does_not_take_from_another_manual_or_cadence():
    other_manual = {**_worker("N"), "auto_calculate_shifts": False}
    cadence = {**_worker("C"), "has_cadence": True, "mandatory_days": "03-03-2026"}
    short = {
        **_worker("M"),
        "auto_calculate_shifts": False,
        "work_periods": "03-03-2026 - 03-03-2026",
    }
    scheduler = _scheduler_with({"N": [3]}, [short, other_manual, cadence], {"M": 1, "N": 1, "C": 0})
    builder = scheduler.schedule_builder

    assert builder._monthly_fill_deficit("M", scheduler.workers_data[0], (2026, 3), strict_donor=False) is False
    assert scheduler.schedule[datetime(2026, 3, 3)] == ["N"]


def test_auto_deficit_still_requires_a_monthly_over_donor():
    under = {**_worker("U"), "work_periods": "03-03-2026 - 03-03-2026"}
    donor = _worker("D")
    scheduler = _scheduler_with({"D": [3, 10]}, [under, donor], {"U": 2, "D": 2})
    for worker in scheduler.workers_data:
        worker["monthly_targets"] = {"2026-03": 2}
    builder = scheduler.schedule_builder

    assert builder._monthly_fill_deficit("U", scheduler.workers_data[0], (2026, 3), strict_donor=False) is False
    assert scheduler.schedule[datetime(2026, 3, 3)] == ["D"]
