"""Unit tests for StrictBalanceOptimizer manual-target enforcement."""

from __future__ import annotations

from datetime import datetime

from saldo27.schedule_builder import ScheduleBuilder
from saldo27.scheduler import Scheduler
from saldo27.strict_balance_optimizer import StrictBalanceOptimizer


def _make_scheduler(workers_data, start=datetime(2026, 3, 1), end=datetime(2026, 3, 31)):
    return Scheduler(
        {
            "start_date": start,
            "end_date": end,
            "num_shifts": 2,
            "workers_data": workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )


def _workers(manual_target=8, auto_target=8):
    return [
        {
            "id": "A",
            "name": "Manual Worker",
            "target_shifts": manual_target,
            "_raw_target": manual_target,
            "work_percentage": 100,
            "work_periods": "",
            "days_off": "",
            "mandatory_days": "",
            "incompatible_with": [],
            "is_incompatible_all": False,
            "auto_calculate_shifts": False,
        },
        {
            "id": "B",
            "name": "Auto Worker",
            "target_shifts": auto_target,
            "_raw_target": auto_target,
            "work_percentage": 100,
            "work_periods": "",
            "days_off": "",
            "mandatory_days": "",
            "incompatible_with": [],
            "is_incompatible_all": False,
            "auto_calculate_shifts": True,
        },
    ]


def test_manual_worker_off_by_one_flagged_despite_default_tolerance():
    """
    A manual worker (auto_calculate_shifts=False) with a deviation of exactly
    1 must still be flagged as imbalanced, even though the default tolerance
    (±1) would normally leave a deviation of 1 untouched for auto-calculated
    workers.  Manual targets must be matched EXACTLY.
    """
    workers = _workers(manual_target=8, auto_target=8)
    scheduler = _make_scheduler(workers)
    # Scheduler.__init__ recalculates target_shifts from availability; pin the
    # values explicitly afterward so this test is independent of that logic.
    for w in scheduler.workers_data:
        w["target_shifts"] = 8

    dates_a = [datetime(2026, 3, d) for d in [1, 4, 7, 10, 13, 16, 19]]  # 7 shifts, 1 short of target 8
    scheduler.schedule = {d: ["A", None] for d in dates_a}
    scheduler.worker_assignments["A"] = set(dates_a)
    scheduler.worker_assignments["B"] = set()

    sbo = StrictBalanceOptimizer(scheduler, scheduler.schedule_builder)
    _overloaded, underloaded = sbo._get_imbalanced_workers(tolerance=1)

    assert any(wid == "A" for wid, _ in underloaded), (
        "Manual worker with deviation -1 must be flagged as underloaded (tolerance must be 0 for manual workers)"
    )


def test_auto_worker_off_by_one_not_flagged_with_default_tolerance():
    """Sanity check: an auto-calculated worker with deviation 1 is still
    within the default ±1 tolerance and must NOT be flagged."""
    workers = _workers(manual_target=8, auto_target=8)
    scheduler = _make_scheduler(workers)
    # Scheduler.__init__ recalculates target_shifts from availability; pin the
    # values explicitly afterward so this test is independent of that logic.
    for w in scheduler.workers_data:
        w["target_shifts"] = 8

    dates_b = [datetime(2026, 3, d) for d in [1, 4, 7, 10, 13, 16, 19]]  # 7 shifts, 1 short of target 8
    scheduler.schedule = {d: [None, "B"] for d in dates_b}
    scheduler.worker_assignments["A"] = set()
    scheduler.worker_assignments["B"] = set(dates_b)

    sbo = StrictBalanceOptimizer(scheduler, scheduler.schedule_builder)
    _overloaded, underloaded = sbo._get_imbalanced_workers(tolerance=1)

    assert not any(wid == "B" for wid, _ in underloaded), (
        "Auto-calculated worker with deviation -1 must stay within the default ±1 tolerance"
    )


def test_relaxed_swap_never_removes_config_mandatory_shift():
    """
    _try_relaxed_swap must never remove a shift from a config-mandatory date
    (worker "A" has 2026-03-04 listed in mandatory_days), even though that
    date is NOT in the runtime _locked_mandatory set. Regression test for a
    bug where this strategy only checked is_locked_mandatory and bypassed
    _can_modify_assignment (which also checks _is_mandatory / manual floor).
    """
    workers = _workers(manual_target=8, auto_target=8)
    workers[0]["mandatory_days"] = "04-03-2026"
    scheduler = _make_scheduler(workers)
    for w in scheduler.workers_data:
        w["target_shifts"] = 8

    mandatory_date = datetime(2026, 3, 4)
    dates_a = [datetime(2026, 3, d) for d in [1, 4, 7, 10, 13, 16, 19, 22, 25]]  # 9 shifts, 1 over target 8
    scheduler.schedule = {d: ["A", None] for d in dates_a}
    scheduler.worker_assignments["A"] = set(dates_a)
    scheduler.worker_assignments["B"] = set()
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    sbo = StrictBalanceOptimizer(scheduler, scheduler.schedule_builder)
    overloaded, underloaded = sbo._get_imbalanced_workers(tolerance=1)
    sbo._try_relaxed_swap(overloaded, underloaded, tolerance=1)

    assert mandatory_date in scheduler.worker_assignments["A"], (
        "_try_relaxed_swap must not remove a shift from a config-mandatory date"
    )
    assert scheduler.schedule[mandatory_date][0] == "A"


def test_relaxed_swap_never_drops_manual_worker_below_floor():
    """
    _try_relaxed_swap must not remove a shift from a manual worker (A) whose
    current shift count is already at or below their monthly target, even if
    A is globally reported as "overloaded" for the whole scheduling period.
    """
    workers = _workers(manual_target=8, auto_target=8)
    scheduler = _make_scheduler(workers)
    for w in scheduler.workers_data:
        w["target_shifts"] = 8

    dates_a = [datetime(2026, 3, d) for d in [1, 4, 7, 10, 13, 16, 19, 22]]  # exactly at target 8
    scheduler.schedule = {d: ["A", None] for d in dates_a}
    scheduler.worker_assignments["A"] = set(dates_a)
    scheduler.worker_assignments["B"] = set()
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    sbo = StrictBalanceOptimizer(scheduler, scheduler.schedule_builder)
    over_count_before = len(scheduler.worker_assignments["A"])
    sbo._try_relaxed_swap([("A", 8)], [("B", -8)], tolerance=1)

    assert len(scheduler.worker_assignments["A"]) == over_count_before, (
        "_try_relaxed_swap must not drop a manual worker below their monthly floor"
    )
