"""Cadence workers must only receive shifts on their cadence dates."""

from datetime import datetime

import pytest

from saldo27.schedule_builder import ScheduleBuilder
from saldo27.scheduler import Scheduler


def _cadence_worker(**overrides):
    worker = {
        "id": "CAD001",
        "name": "CAD001",
        "target_shifts": 0,
        "_raw_target": 2,
        "work_percentage": 100,
        "work_periods": "",
        "mandatory_days": "01-03-2026;07-03-2026",
        "days_off": "",
        "incompatible_with": [],
        "is_incompatible_all": False,
        "auto_calculate_shifts": True,
        "has_cadence": True,
        "cadence_days": 6,
        "cadence_start_date": "01-03-2026",
    }
    worker.update(overrides)
    return worker


def _other_worker():
    return {
        "id": "DOC001",
        "name": "DOC001",
        "target_shifts": 4,
        "_raw_target": 4,
        "work_percentage": 100,
        "work_periods": "",
        "mandatory_days": "",
        "days_off": "",
        "incompatible_with": [],
        "is_incompatible_all": False,
        "auto_calculate_shifts": True,
        "has_cadence": False,
    }


def _make_scheduler(workers):
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 14),
            "num_shifts": 2,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": 3,
        }
    )
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    return scheduler


def test_builder_blocks_assignment_outside_cadence():
    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)
    builder = scheduler.schedule_builder

    cadence_date = datetime(2026, 3, 1)
    outside_date = datetime(2026, 3, 2)

    assert builder._is_worker_unavailable("CAD001", cadence_date) is False
    assert builder._is_worker_unavailable("CAD001", outside_date) is True
    assert builder._can_assign_worker("CAD001", outside_date, 0) is False
    assert builder._check_hard_constraints("CAD001", outside_date, 0) is False


def test_constraint_checker_blocks_assignment_outside_cadence():
    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)

    cadence_date = datetime(2026, 3, 7)
    outside_date = datetime(2026, 3, 8)

    assert scheduler.constraint_checker._is_worker_unavailable("CAD001", cadence_date) is False
    assert scheduler.constraint_checker._is_worker_unavailable("CAD001", outside_date) is True


def test_two_consecutive_rosell_shifts_are_a_violation():
    """Two Rosell shifts in a row in the worker's own sequence are forbidden.
    ELENA on 17-03-2027 and 29-03-2027 is that case: the 13-03 shift is not Rosell,
    so 17-03 and 29-03 are consecutive in her sequence.
    """
    workers = [_other_worker(), _cadence_worker(id="ELENA", name="ELENA", has_cadence=False, mandatory_days="")]
    scheduler = _make_scheduler(workers)
    scheduler.num_shifts = 4
    first = datetime(2027, 3, 17)
    second = datetime(2027, 3, 29)
    between = datetime(2027, 3, 13)
    scheduler.schedule[between] = ["MARIA", "VALLE", "ELENA", "LAURA"]
    scheduler.schedule[first] = ["JESUS", "KIKO", "MAR", "ELENA"]
    scheduler.schedule[second] = ["MANUEL", "LOLA", "CHEMA", "ELENA"]
    scheduler.worker_assignments["ELENA"] = {between, first, second}

    violations = scheduler.constraint_checker.check_schedule_violations(scheduler.worker_assignments)
    consecutive = [v for v in violations if v["type"] == "consecutive_last_post" and v["worker_id"] == "ELENA"]
    assert len(consecutive) == 1
    assert consecutive[0]["run_length"] == 2
    assert consecutive[0]["date1"] == first
    assert consecutive[0]["date2"] == second
    assert scheduler.constraint_checker._would_exceed_consecutive_last_post("ELENA", second, True) is True


def test_three_consecutive_rosell_shifts_are_a_violation():
    workers = [_other_worker()]
    scheduler = _make_scheduler(workers)
    scheduler.num_shifts = 4
    dates = [datetime(2027, 3, 5), datetime(2027, 3, 17), datetime(2027, 3, 29)]
    for day in dates:
        scheduler.schedule[day] = ["A", "B", "C", "DOC001"]
    scheduler.worker_assignments["DOC001"] = set(dates)

    violations = scheduler.constraint_checker.check_schedule_violations(scheduler.worker_assignments)
    consecutive = [v for v in violations if v["type"] == "consecutive_last_post"]
    assert len(consecutive) == 1
    assert consecutive[0]["run_length"] == 3
    assert consecutive[0]["date1"] == dates[0]
    assert consecutive[0]["date2"] == dates[2]


def test_check_schedule_violations_reports_outside_cadence():
    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)

    cadence_date = datetime(2026, 3, 1)
    outside_date = datetime(2026, 3, 3)
    scheduler.schedule[cadence_date][0] = "CAD001"
    scheduler.schedule[outside_date][0] = "CAD001"
    scheduler.worker_assignments["CAD001"] = {cadence_date, outside_date}

    violations = scheduler.constraint_checker.check_schedule_violations(scheduler.worker_assignments)
    outside = [v for v in violations if v["type"] == "outside_cadence"]
    assert len(outside) == 1
    assert outside[0]["worker_id"] == "CAD001"
    assert outside[0]["date"] == outside_date


def test_fix_removes_outside_cadence_assignment():
    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)

    cadence_date = datetime(2026, 3, 1)
    outside_date = datetime(2026, 3, 4)
    scheduler.schedule[cadence_date][0] = "CAD001"
    scheduler.schedule[outside_date][0] = "CAD001"
    scheduler.worker_assignments["CAD001"] = {cadence_date, outside_date}

    fixes = scheduler._fix_constraint_violations()
    assert fixes >= 1
    assert scheduler.schedule[outside_date][0] is None
    assert outside_date not in scheduler.worker_assignments["CAD001"]
    assert scheduler.schedule[cadence_date][0] == "CAD001"


def _place_cadence(scheduler, posts: dict[datetime, int]) -> None:
    for day in scheduler.schedule:
        scheduler.schedule[day] = [None, None]
    for day, post in posts.items():
        scheduler.schedule[day][post] = "CAD001"
    scheduler._synchronize_tracking_data()


def test_cadence_skips_days_off_and_keeps_its_rhythm():
    """Every 4 days from 01/01 with vacation 10/01-20/01: 01, 05, 09, 21, 25, 29."""
    workers = [
        _cadence_worker(cadence_days=4, cadence_start_date="01-01-2026", days_off="10-01-2026 - 20-01-2026"),
        _other_worker(),
    ]
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 1, 1),
            "end_date": datetime(2026, 1, 31),
            "num_shifts": 2,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": 3,
        }
    )
    cadence = next(w for w in scheduler.workers_data if w["id"] == "CAD001")

    assert cadence["mandatory_days"] == "01-01-2026;05-01-2026;09-01-2026;21-01-2026;25-01-2026;29-01-2026"


def test_cadence_skips_dates_outside_work_periods():
    workers = [_cadence_worker(work_periods="05-03-2026 - 14-03-2026"), _other_worker()]
    scheduler = _make_scheduler(workers)
    cadence = next(w for w in scheduler.workers_data if w["id"] == "CAD001")

    assert cadence["mandatory_days"] == "07-03-2026;13-03-2026"
    assert scheduler.schedule_builder._is_worker_unavailable("CAD001", datetime(2026, 3, 1)) is True


def test_cadence_date_ignores_weekend_limit():
    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)
    scheduler.max_consecutive_weekends = 0

    assert scheduler.constraint_checker._is_worker_unavailable("CAD001", datetime(2026, 3, 7)) is False


def test_builder_never_moves_cadence_shifts_but_allows_same_day_post_changes():
    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)
    builder = scheduler.schedule_builder
    day = datetime(2026, 3, 7)

    for operation in ("balance_workloads", "swap"):
        assert builder._can_modify_assignment("CAD001", day, operation) is False
    for operation in ("adjust_last_post", "strict_last_post_B"):
        assert builder._can_modify_assignment("CAD001", day, operation) is True


def test_mandatory_placement_puts_cadence_first_and_never_relocates_it():
    workers = [_cadence_worker(only_last_post=True), _other_worker()]
    scheduler = _make_scheduler(workers)
    builder = scheduler.schedule_builder
    for day in scheduler.schedule:
        scheduler.schedule[day] = [None, None]
    scheduler._synchronize_tracking_data()

    builder._assign_mandatory_guards()

    for day in (datetime(2026, 3, 1), datetime(2026, 3, 7), datetime(2026, 3, 13)):
        assert scheduler.schedule[day][-1] == "CAD001"
        assert builder.is_locked_mandatory("CAD001", day)


def test_violation_check_skips_spacing_and_rosell_for_cadence():
    workers = [_cadence_worker(cadence_days=1), _other_worker()]
    scheduler = _make_scheduler(workers)
    scheduler.gap_between_shifts = 3
    _place_cadence(scheduler, {datetime(2026, 3, d): 1 for d in (1, 2, 3)})

    violations = scheduler.constraint_checker.check_schedule_violations(scheduler.worker_assignments)

    assert [v for v in violations if v.get("worker_id") == "CAD001"] == []


def test_final_adjustment_protects_every_cadence_shift():
    from saldo27.final_adjustment_engine import FinalAdjustmentEngine

    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)
    engine = FinalAdjustmentEngine(scheduler)

    assert engine._is_protected_assignment("CAD001", datetime(2026, 3, 7)) is True
    assert engine._is_protected_assignment("DOC001", datetime(2026, 3, 7)) is False


def test_cadence_objectives_are_the_cadence_itself():
    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)
    _place_cadence(scheduler, {datetime(2026, 3, 1): 1, datetime(2026, 3, 7): 0, datetime(2026, 3, 13): 1})
    cadence = next(w for w in scheduler.workers_data if w["id"] == "CAD001")

    # 01-03 (Sun) and 07-03 (Sat) are weekend days; 13-03 (Fri) too.
    assert scheduler.get_weekend_objective(cadence) == 3
    assert scheduler.get_last_post_objective(cadence) == 2


def test_cadence_keeps_its_bridge_days_and_others_share_the_rest():
    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)
    _place_cadence(scheduler, {datetime(2026, 3, 7): 0})
    for day in (6, 7, 8):
        scheduler.schedule[datetime(2026, 3, day)][1] = "DOC001"
    scheduler.bridge_periods = [
        {
            "id": "bridge_test",
            "start_date": datetime(2026, 3, 6),
            "end_date": datetime(2026, 3, 8),
            "holiday": datetime(2026, 3, 6),
            "type": "friday",
        }
    ]

    assert scheduler.get_bridge_objective_for_worker("CAD001") == 1
    assert scheduler.get_bridge_objective_for_worker("DOC001") == 3


def test_violation_check_still_applies_spacing_to_other_workers():
    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)
    scheduler.gap_between_shifts = 3
    dates = [datetime(2026, 3, 2), datetime(2026, 3, 3)]
    for day in dates:
        scheduler.schedule[day] = ["DOC001", None]
    scheduler.worker_assignments["DOC001"] = set(dates)

    violations = scheduler.constraint_checker.check_schedule_violations(scheduler.worker_assignments)

    assert any(v.get("worker_id") == "DOC001" for v in violations)


@pytest.fixture
def real_time_scheduler():
    from saldo27.real_time_engine import RealTimeEngine

    workers = [_cadence_worker(), _other_worker()]
    scheduler = _make_scheduler(workers)
    _place_cadence(scheduler, {datetime(2026, 3, 1): 0, datetime(2026, 3, 7): 0, datetime(2026, 3, 13): 0})
    scheduler.schedule[datetime(2026, 3, 3)][0] = "DOC001"
    scheduler._synchronize_tracking_data()
    return scheduler, RealTimeEngine(scheduler)


def test_real_time_unassign_of_cadence_shift_is_blocked(real_time_scheduler):
    scheduler, engine = real_time_scheduler

    result = engine.unassign_worker_real_time(datetime(2026, 3, 7), 0)

    assert result.success is False
    assert scheduler.schedule[datetime(2026, 3, 7)][0] == "CAD001"


def test_real_time_assign_over_cadence_shift_is_blocked_even_with_force(real_time_scheduler):
    scheduler, engine = real_time_scheduler

    result = engine.assign_worker_real_time("DOC001", datetime(2026, 3, 7), 0, force=True)

    assert result.success is False
    assert scheduler.schedule[datetime(2026, 3, 7)][0] == "CAD001"


def test_real_time_assign_outside_cadence_is_blocked_even_with_force(real_time_scheduler):
    scheduler, engine = real_time_scheduler

    result = engine.assign_worker_real_time("CAD001", datetime(2026, 3, 4), 0, force=True)

    assert result.success is False
    assert scheduler.schedule[datetime(2026, 3, 4)][0] is None


def test_real_time_swap_to_another_day_is_blocked(real_time_scheduler):
    scheduler, engine = real_time_scheduler

    result = engine.swap_workers_real_time(datetime(2026, 3, 7), 0, datetime(2026, 3, 3), 0, force=True)

    assert result.success is False
    assert scheduler.schedule[datetime(2026, 3, 7)][0] == "CAD001"
    assert scheduler.schedule[datetime(2026, 3, 3)][0] == "DOC001"


def test_real_time_swap_within_the_same_day_is_allowed(real_time_scheduler):
    scheduler, engine = real_time_scheduler
    day = datetime(2026, 3, 7)

    result = engine.swap_workers_real_time(day, 0, day, 1, force=True)

    assert result.success is True
    assert scheduler.schedule[day] == [None, "CAD001"]
