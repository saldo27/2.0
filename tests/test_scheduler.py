import json
from datetime import datetime
from math import isclose
from typing import ClassVar

from saldo27.scheduler import Scheduler


def _build_scheduler(sample_workers_data):
    return Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 3),
            "num_shifts": 4,
            "workers_data": sample_workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )


def test_update_tracking_data_removal_keeps_post_when_worker_still_has_same_post(sample_workers_data):
    scheduler = _build_scheduler(sample_workers_data)
    first_date = datetime(2026, 3, 1)
    second_date = datetime(2026, 3, 2)

    scheduler.schedule[first_date][0] = "DOC001"
    scheduler.schedule[second_date][0] = "DOC001"
    scheduler._synchronize_tracking_data()

    scheduler.schedule[first_date][0] = None
    scheduler._update_tracking_data("DOC001", first_date, 0, removing=True)

    assert scheduler.worker_assignments["DOC001"] == {second_date}
    assert scheduler.worker_posts["DOC001"] == {0}


def test_update_tracking_data_removal_drops_post_when_last_assignment_is_removed(sample_workers_data):
    scheduler = _build_scheduler(sample_workers_data)
    assignment_date = datetime(2026, 3, 1)

    scheduler.schedule[assignment_date][0] = "DOC001"
    scheduler._synchronize_tracking_data()

    scheduler.schedule[assignment_date][0] = None
    scheduler._update_tracking_data("DOC001", assignment_date, 0, removing=True)

    assert scheduler.worker_assignments["DOC001"] == set()
    assert scheduler.worker_posts["DOC001"] == set()


def test_validate_and_fix_final_schedule_uses_canonical_weekly_pattern_violation(sample_workers_data):
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 10),
            "num_shifts": 4,
            "workers_data": sample_workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    first_date = datetime(2026, 3, 2)
    second_date = datetime(2026, 3, 9)

    scheduler.schedule[first_date][0] = "DOC001"
    scheduler.schedule[second_date][0] = "DOC001"
    scheduler._synchronize_tracking_data()

    fixes_made = scheduler.validate_and_fix_final_schedule()

    assert fixes_made == 1
    assert scheduler.schedule[second_date][0] is None
    assert scheduler.worker_assignments["DOC001"] == {first_date}


def test_fix_constraint_violations_preserves_budgeted_weekly_pattern_exception(sample_workers_data):
    """A weekly_pattern violation whose 7/14 budget is already spent (0) is an
    intentional, sanctioned exception (see schedule_builder._violations_714_budget)
    and must be left untouched by the final validation/fix pass, instead of being
    undone as if it were an accidental bug."""
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 10),
            "num_shifts": 4,
            "workers_data": sample_workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    first_date = datetime(2026, 3, 2)
    second_date = datetime(2026, 3, 9)

    scheduler.schedule[first_date][0] = "DOC001"
    scheduler.schedule[second_date][0] = "DOC001"
    scheduler._synchronize_tracking_data()

    class _StubBuilder:
        _violations_714_budget: ClassVar[dict[str, int]] = {"DOC001": 0}

    scheduler.schedule_builder = _StubBuilder()

    fixes_made = scheduler.validate_and_fix_final_schedule()

    assert fixes_made == 0
    assert scheduler.schedule[first_date][0] == "DOC001"
    assert scheduler.schedule[second_date][0] == "DOC001"
    assert scheduler.worker_assignments["DOC001"] == {first_date, second_date}


def test_finalization_phase_runs_final_validation_and_fix(sample_workers_data):
    """The finalization phase must invoke the (previously dead) final
    validation/fix safety net so genuine constraint violations left over from
    earlier repair passes get caught and repaired."""
    from saldo27.schedule_builder import ScheduleBuilder
    from saldo27.scheduler_core import SchedulerCore

    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 10),
            "num_shifts": 4,
            "workers_data": sample_workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    first_date = datetime(2026, 3, 2)
    second_date = datetime(2026, 3, 9)

    scheduler.schedule[first_date][0] = "DOC001"
    scheduler.schedule[second_date][0] = "DOC001"
    scheduler._synchronize_tracking_data()
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    scheduler.best_schedule_data = {
        "schedule": scheduler.schedule,
        "worker_assignments": scheduler.worker_assignments,
        "worker_shift_counts": scheduler.worker_shift_counts,
        "worker_weekend_counts": scheduler.worker_weekend_counts,
        "worker_posts": scheduler.worker_posts,
        "last_assignment_date": scheduler.last_assignment_date,
        "consecutive_shifts": scheduler.consecutive_shifts,
        "score": scheduler.calculate_score(),
    }

    core = SchedulerCore(scheduler)
    assert core._finalization_phase() is True

    assert scheduler.schedule[second_date][0] is None
    assert scheduler.worker_assignments["DOC001"] == {first_date}


def test_calculate_score_returns_filled_percentage(sample_workers_data):
    scheduler = _build_scheduler(sample_workers_data)
    first_date = datetime(2026, 3, 1)

    scheduler.schedule[first_date][0] = "DOC001"

    assert isclose(scheduler.calculate_score(), 100 / 12)


def test_export_schedule_json_serializes_schedule_and_assignments(sample_workers_data, tmp_path):
    scheduler = _build_scheduler(sample_workers_data)
    first_date = datetime(2026, 3, 1)
    output_file = tmp_path / "schedule.json"

    scheduler.schedule[first_date][0] = "DOC001"
    scheduler._synchronize_tracking_data()

    exported_path = scheduler.export_schedule_json(str(output_file))
    exported = json.loads(output_file.read_text(encoding="utf-8"))

    assert exported_path == str(output_file)
    assert exported["schedule"]["2026-03-01"][0] == "DOC001"
    assert exported["worker_assignments"]["DOC001"] == ["2026-03-01"]
