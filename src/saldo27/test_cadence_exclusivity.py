"""Cadence workers must only receive shifts on their cadence dates."""

from datetime import datetime

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
