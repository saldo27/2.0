"""Unit tests for FinalAdjustmentEngine swap-aware constraint helpers."""

from __future__ import annotations

from datetime import datetime

from saldo27.final_adjustment_engine import FinalAdjustmentEngine
from saldo27.schedule_builder import ScheduleBuilder
from saldo27.scheduler import Scheduler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _simple_workers():
    return [
        {
            "id": "A",
            "name": "Worker A",
            "target_shifts": 8,
            "_raw_target": 8,
            "work_percentage": 100,
            "work_periods": "",
            "days_off": "",
            "mandatory_days": "",
            "incompatible_with": [],
            "is_incompatible_all": False,
            "auto_calculate_shifts": True,
        },
        {
            "id": "B",
            "name": "Worker B",
            "target_shifts": 8,
            "_raw_target": 8,
            "work_percentage": 100,
            "work_periods": "",
            "days_off": "",
            "mandatory_days": "",
            "incompatible_with": [],
            "is_incompatible_all": False,
            "auto_calculate_shifts": True,
        },
    ]


def _build_engine(scheduler):
    """Build a FinalAdjustmentEngine without a schedule_builder (no-builder mode)."""
    return FinalAdjustmentEngine(scheduler)


def _make_stub_builder(scheduler):
    """
    Build a minimal ScheduleBuilder stub that has enough state to exercise
    _can_take_in_swap without running the full builder initialization.
    """
    sb = ScheduleBuilder.__new__(ScheduleBuilder)
    sb.scheduler = scheduler
    sb.workers_data = scheduler.workers_data
    sb.worker_assignments = scheduler.worker_assignments
    sb.date_utils = scheduler.date_utils
    sb.gap_between_shifts = scheduler.gap_between_shifts
    sb._locked_mandatory = set()
    sb.num_shifts = 2
    sb._check_incompatibility_with_list = lambda wid, others: True
    sb._is_mandatory = lambda wid, date: False
    # Delegate unavailability check to the real ScheduleBuilder method
    sb._is_worker_unavailable = lambda wid, d: ScheduleBuilder._is_worker_unavailable(sb, wid, d)
    return sb


# ---------------------------------------------------------------------------
# _can_swap_away
# ---------------------------------------------------------------------------

def test_can_swap_away_regular_shift_allowed():
    """A normal, non-mandatory shift can always be swapped away."""
    scheduler = _make_scheduler(_simple_workers())
    engine = _build_engine(scheduler)
    # No schedule_builder means no mandatory checks → always True
    assert engine._can_swap_away("A", datetime(2026, 3, 5))


def test_can_swap_away_with_locked_mandatory_blocked():
    """A shift marked as locked-mandatory cannot be swapped away."""
    scheduler = _make_scheduler(_simple_workers())

    locked_date = datetime(2026, 3, 5)
    other_date = datetime(2026, 3, 6)

    class _StubBuilder:
        _locked_mandatory: frozenset = frozenset({("A", locked_date)})

        def _is_mandatory(self, wid, date):
            return False

    engine = _build_engine(scheduler)
    engine.schedule_builder = _StubBuilder()

    assert not engine._can_swap_away("A", locked_date)
    assert engine._can_swap_away("A", other_date)  # unlocked date is fine


def test_can_swap_away_with_config_mandatory_blocked():
    """A config-mandatory date cannot be swapped away."""
    scheduler = _make_scheduler(_simple_workers())

    mandatory_date = datetime(2026, 3, 10)
    other_date = datetime(2026, 3, 11)

    class _StubBuilder:
        _locked_mandatory: frozenset = frozenset()

        def _is_mandatory(self, wid, date):
            return wid == "A" and date == mandatory_date

    engine = _build_engine(scheduler)
    engine.schedule_builder = _StubBuilder()

    assert not engine._can_swap_away("A", mandatory_date)
    assert engine._can_swap_away("A", other_date)


# ---------------------------------------------------------------------------
# _can_take_in_swap
# ---------------------------------------------------------------------------

def test_can_take_in_swap_no_builder_always_true():
    """Without a schedule_builder every swap is allowed."""
    scheduler = _make_scheduler(_simple_workers())
    engine = _build_engine(scheduler)

    result = engine._can_take_in_swap("B", datetime(2026, 3, 7), 0, datetime(2026, 3, 2))
    assert result is True


def test_can_take_in_swap_respects_gap_with_simulated_removal():
    """
    Gap check must use the SIMULATED assignment set (removing date_lose).

    Scenario:
      - Worker B is assigned on 2026-03-01 and 2026-03-05.
      - gap_between_shifts = 4, so the hard floor is 4 days.
      - We want B to gain 2026-03-09 while losing 2026-03-05.
      - With date_lose=2026-03-05 removed, the nearest assignment is 2026-03-01,
        so gap to 2026-03-09 = 8 days → should be allowed.
    """
    workers = _simple_workers()
    scheduler = _make_scheduler(workers)

    date_lose = datetime(2026, 3, 5)
    date_gain = datetime(2026, 3, 9)
    date_far = datetime(2026, 3, 1)

    scheduler.worker_assignments["B"] = {date_far, date_lose}
    scheduler.schedule[date_far] = ["A", "B"]
    scheduler.schedule[date_lose] = ["A", "B"]
    scheduler.schedule[date_gain] = ["A", None]

    engine = _build_engine(scheduler)
    engine.schedule_builder = _make_stub_builder(scheduler)

    result = engine._can_take_in_swap("B", date_gain, 1, date_lose)
    assert result is True


def test_can_take_in_swap_blocked_by_availability():
    """A worker on days-off cannot receive a shift even in a swap context."""
    workers = _simple_workers()
    workers[1]["days_off"] = "14-03-2026"
    scheduler = _make_scheduler(workers)

    engine = _build_engine(scheduler)
    engine.schedule_builder = _make_stub_builder(scheduler)

    blocked_date = datetime(2026, 3, 14)
    result = engine._can_take_in_swap("B", blocked_date, 0, datetime(2026, 3, 7))
    assert result is False


def test_can_take_in_swap_does_not_check_target_tolerance():
    """
    Workers already at their target shift count must NOT be blocked when
    taking a shift as part of a swap (they are simultaneously losing one).

    The old _can_take_shift used _calculate_worker_score which returned -inf
    for workers at/above tolerance.  The new _can_take_in_swap must NOT apply
    that gate.
    """
    workers = _simple_workers()
    scheduler = _make_scheduler(workers)

    dates_for_b = [datetime(2026, 3, i) for i in [2, 6, 10, 14, 18, 22, 26, 30]]
    scheduler.worker_assignments["B"] = set(dates_for_b)
    for d in dates_for_b:
        scheduler.schedule.setdefault(d, [None, None])
        scheduler.schedule[d][1] = "B"

    engine = _build_engine(scheduler)

    date_gain = datetime(2026, 3, 5)
    date_lose = datetime(2026, 3, 2)
    scheduler.schedule[date_gain] = ["A", None]

    # Without schedule_builder, the method returns True regardless of target count.
    result = engine._can_take_in_swap("B", date_gain, 1, date_lose)
    assert result is True
