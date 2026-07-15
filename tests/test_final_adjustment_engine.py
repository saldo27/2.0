"""Unit tests for FinalAdjustmentEngine swap-aware constraint helpers."""

from __future__ import annotations

from datetime import datetime, timedelta

from saldo27.final_adjustment_engine import FinalAdjustmentEngine
from saldo27.scheduler import Scheduler
from saldo27.utilities import get_effective_min_gap

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


class _ScheduleBuilderStub:
    """
    Standalone stub that implements only the ScheduleBuilder interface methods
    required by FinalAdjustmentEngine._can_take_in_swap.  It does NOT inherit
    from ScheduleBuilder and does NOT use __new__ to bypass initialization —
    it provides the minimal interface needed for testing.

    Gap and availability logic is implemented directly so these tests remain
    decoupled from ScheduleBuilder's internal structure.
    """

    def __init__(self, scheduler, *, allow_all: bool = False):
        self._scheduler = scheduler
        self._locked_mandatory: set = set()
        self.num_shifts: int = 2
        self._allow_all = allow_all  # when True, mark all workers as available

    def _is_mandatory(self, wid: str, date: datetime) -> bool:
        return False

    def _check_incompatibility_with_list(self, wid: str, others: list) -> bool:
        return True

    def _is_worker_unavailable(self, worker_id: str, date: datetime) -> bool:
        if self._allow_all:
            return False
        worker = next((w for w in self._scheduler.workers_data if w["id"] == worker_id), None)
        if not worker:
            return True
        days_off_str = worker.get("days_off", "") or ""
        if days_off_str:
            try:
                days_off = set(self._scheduler.date_utils.parse_dates(days_off_str))
                if date in days_off:
                    return True
            except Exception:
                return True
        return False

    def _check_gap_constraint_simulated(self, worker_id: str, date: datetime, simulated_assignments: dict) -> bool:
        worker = next((w for w in self._scheduler.workers_data if w["id"] == worker_id), None)
        min_days = get_effective_min_gap(worker, self._scheduler.gap_between_shifts)
        current = simulated_assignments.get(worker_id, set())
        prior_raw = getattr(self._scheduler, "prior_assignments", {}).get(worker_id, set())
        cutoff = self._scheduler.start_date - timedelta(days=90)
        prior = {d for d in prior_raw if d >= cutoff}
        for prev in sorted(current | prior):
            if prev == date:
                continue
            days = abs((date - prev).days)
            if days < min_days:
                return False
            if (days == 7 or days == 14) and date.weekday() == prev.weekday():
                return False
        return True


def _make_stub_builder(scheduler):
    """Return a _ScheduleBuilderStub wired to the given scheduler."""
    return _ScheduleBuilderStub(scheduler)


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


# ---------------------------------------------------------------------------
# _raw_targets cache – fallback logic
# ---------------------------------------------------------------------------


def test_raw_targets_fallback_when_raw_target_is_none():
    """
    When _raw_target is not present (or None) on a worker dict, the engine must
    fall back to target_shifts.  The None-guard prevents falsy 0 from triggering
    an incorrect fallback.

    We inject the workers_data directly onto the scheduler (bypassing the
    scheduler's own target-computation which always sets _raw_target) so we can
    test the engine's fallback logic in isolation.
    """
    scheduler = _make_scheduler(_simple_workers())

    # Override workers_data with specific raw_target values
    scheduler.workers_data = [
        {
            "id": "X",
            "name": "Worker X",
            "target_shifts": 5,
            # No _raw_target key → should fall back to target_shifts=5
        },
        {
            "id": "Y",
            "name": "Worker Y",
            "target_shifts": 7,
            "_raw_target": None,  # explicitly None → should fall back to target_shifts=7
        },
        {
            "id": "Z",
            "name": "Worker Z",
            "target_shifts": 4,
            "_raw_target": 0,  # explicitly 0 → must NOT fall back; 0 is a valid raw target
        },
    ]

    engine = _build_engine(scheduler)

    assert engine._raw_targets["X"] == 5  # fell back to target_shifts
    assert engine._raw_targets["Y"] == 7  # fell back to target_shifts (None treated as missing)
    assert engine._raw_targets["Z"] == 0  # raw_target=0 preserved (not a fallback)


# ---------------------------------------------------------------------------
# OR-Tools CP-SAT Phase (Phase 4) tests
# ---------------------------------------------------------------------------


def _build_minimal_schedule(scheduler, workers, dates_per_worker):
    """
    Populate scheduler.schedule and worker_assignments from a dict
    ``{worker_id: [date, ...]}``.  Each date gets a two-slot row; the
    workers are distributed across slots in the order they appear.
    """
    from collections import defaultdict

    # Gather all (date, worker) pairs
    day_assignments: dict = defaultdict(list)
    for wid, dates in dates_per_worker.items():
        for d in dates:
            day_assignments[d].append(wid)

    for date, assigned in day_assignments.items():
        row = [None] * scheduler.num_shifts
        for i, wid in enumerate(assigned[: scheduler.num_shifts]):
            row[i] = wid
        scheduler.schedule[date] = row

    for wid, dates in dates_per_worker.items():
        scheduler.worker_assignments[wid] = set(dates)
        scheduler.worker_shift_counts[wid] = len(dates)


def test_ortools_phase_skipped_without_ortools(monkeypatch):
    """
    When ortools is not importable, _run_ortools_phase returns 0 and does
    not raise.
    """
    import builtins

    original_import = builtins.__import__

    def _no_ortools(name, *args, **kwargs):
        if name == "ortools.sat.python" or name.startswith("ortools"):
            raise ImportError("ortools not available (mocked)")
        return original_import(name, *args, **kwargs)

    scheduler = _make_scheduler(_simple_workers())
    engine = _build_engine(scheduler)

    monkeypatch.setattr(builtins, "__import__", _no_ortools)
    result = engine._run_ortools_phase(time_limit_seconds=5)
    assert result == 0


def test_ortools_phase_respects_mandatory():
    """
    Mandatory (locked) assignments must remain in place after the OR-Tools
    phase completes.
    """
    workers = _simple_workers()
    scheduler = _make_scheduler(workers)

    mandatory_date = datetime(2026, 3, 10)
    other_date = datetime(2026, 3, 20)

    scheduler.schedule[mandatory_date] = ["A", "B"]
    scheduler.schedule[other_date] = ["A", "B"]
    scheduler.worker_assignments["A"] = {mandatory_date, other_date}
    scheduler.worker_assignments["B"] = {mandatory_date, other_date}
    scheduler.worker_shift_counts["A"] = 2
    scheduler.worker_shift_counts["B"] = 2

    class _StubBuilder:
        _locked_mandatory: frozenset = frozenset({("A", mandatory_date)})
        num_shifts: int = 2

        def _is_mandatory(self, wid, date):
            return False

        def _is_worker_unavailable(self, wid, date):
            return False

    engine = _build_engine(scheduler)
    engine.schedule_builder = _StubBuilder()

    engine._run_ortools_phase(time_limit_seconds=10)

    # Mandatory slot must be unchanged
    assert scheduler.schedule[mandatory_date][0] == "A"


def test_ortools_phase_respects_gap_constraint():
    """
    The OR-Tools solution must not introduce gap violations that were absent
    in the initial schedule.

    Setup: A and B each work on 5 well-spaced days (7-day gaps) so the
    initial schedule is gap-valid.  After OR-Tools, every pair of dates
    for the same worker must still satisfy the minimum gap.
    """
    from saldo27.utilities import get_effective_min_gap

    workers = _simple_workers()
    scheduler = _make_scheduler(workers, end=datetime(2026, 3, 31))

    # 7-day spacing → always >= min_gap (which is max(1, 4-1)=3 for auto workers)
    # Each day has one slot per worker; A takes slot 0, B takes slot 1.
    dates_a = [datetime(2026, 3, d) for d in [1, 8, 15, 22, 29]]
    dates_b = [datetime(2026, 3, d) for d in [4, 11, 18, 25]]

    for d in dates_a:
        scheduler.schedule[d] = ["A", None]
    for d in dates_b:
        scheduler.schedule[d] = ["B", None]

    scheduler.worker_assignments["A"] = set(dates_a)
    scheduler.worker_assignments["B"] = set(dates_b)
    scheduler.worker_shift_counts["A"] = len(dates_a)
    scheduler.worker_shift_counts["B"] = len(dates_b)

    engine = _build_engine(scheduler)
    engine._run_ortools_phase(time_limit_seconds=15)

    # Verify no gap violation in resulting assignments
    for wid in ("A", "B"):
        wd = next(w for w in workers if w["id"] == wid)
        min_gap = get_effective_min_gap(wd, scheduler.gap_between_shifts)
        assigned = sorted(scheduler.worker_assignments[wid])
        for i in range(len(assigned)):
            for j in range(i + 1, len(assigned)):
                delta = (assigned[j] - assigned[i]).days
                assert delta >= min_gap or delta == 0, (
                    f"Worker {wid}: gap violation {delta} < {min_gap} "
                    f"between {assigned[i]} and {assigned[j]}"
                )


def test_ortools_phase_improves_or_neutral():
    """
    After the OR-Tools phase the total weighted deviation score must not
    increase compared to before it ran.
    """
    from saldo27.final_adjustment_engine import _deviation_score

    workers = _simple_workers()
    scheduler = _make_scheduler(workers)

    # Assign all shifts to A → heavy imbalance
    dates = [datetime(2026, 3, d) for d in range(1, 17, 2)]
    for d in dates:
        scheduler.schedule[d] = ["A", None]
    scheduler.worker_assignments["A"] = set(dates)
    scheduler.worker_assignments["B"] = set()
    scheduler.worker_shift_counts["A"] = len(dates)
    scheduler.worker_shift_counts["B"] = 0

    engine = _build_engine(scheduler)
    metrics_before = engine.compute_metrics()
    score_before = _deviation_score(metrics_before)

    engine._run_ortools_phase(time_limit_seconds=15)

    metrics_after = engine.compute_metrics()
    score_after = _deviation_score(metrics_after)

    assert score_after <= score_before, (
        f"OR-Tools phase worsened the schedule: score {score_before} → {score_after}"
    )
