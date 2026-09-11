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


def test_mandatory_assignment_places_only_last_post_worker_in_last_post(sample_workers_data):
    """A worker with only_last_post=True and a mandatory_days assignment must be
    placed directly in the LAST post slot, never in an earlier post, even
    though earlier posts remain free that day."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [dict(w) for w in sample_workers_data]
    workers[0]["mandatory_days"] = "01-03-2026"
    workers[0]["only_last_post"] = True

    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 10),
            "num_shifts": 4,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    mandatory_date = datetime(2026, 3, 1)
    scheduler.schedule_builder._assign_mandatory_guards()

    # Must be in the LAST post (index num_shifts - 1), all earlier posts free.
    assert scheduler.schedule[mandatory_date][3] == "DOC001"
    assert scheduler.schedule[mandatory_date][:3] == [None, None, None]
    assert ("DOC001", mandatory_date) in scheduler.schedule_builder.get_locked_mandatory()


def test_mandatory_assignment_never_places_no_last_post_worker_in_last_post(sample_workers_data):
    """A worker with no_last_post=True and a mandatory_days assignment must
    never be placed in the last post slot, even if it's the only free one
    tried first in iteration order."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [dict(w) for w in sample_workers_data]
    workers[0]["mandatory_days"] = "01-03-2026"
    workers[0]["no_last_post"] = True

    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 10),
            "num_shifts": 4,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    mandatory_date = datetime(2026, 3, 1)
    scheduler.schedule_builder._assign_mandatory_guards()

    assert "DOC001" in scheduler.schedule[mandatory_date][:3]
    assert scheduler.schedule[mandatory_date][3] is None


def test_fix_constraint_violations_always_fixes_non_mandatory_weekly_pattern(sample_workers_data):
    """HARD INVARIANT: a weekly_pattern (7/14-day same-weekday) violation must
    always be fixed by the final validation/fix pass unless BOTH colliding dates
    are mandatory_days for that worker. A spent/absent _violations_714_budget is
    NOT a sanctioned reason to leave the violation in place — that budget only
    controls whether generation-time relaxation passes are allowed to *create*
    such a violation (they never do, see schedule_builder._violations_714_budget),
    it must never suppress the final repair."""
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

        def is_mandatory(self, worker_id, date):
            return False

        def _can_modify_assignment(self, worker_id, date, reason, enforce_monthly_target_floor=True):
            return True

        def get_locked_mandatory(self):
            return set()

    scheduler.schedule_builder = _StubBuilder()

    fixes_made = scheduler.validate_and_fix_final_schedule()

    assert fixes_made == 1
    assert scheduler.schedule[first_date][0] == "DOC001"
    assert scheduler.schedule[second_date][0] is None
    assert scheduler.worker_assignments["DOC001"] == {first_date}


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


def test_finalization_phase_restores_manual_monthly_target_after_violation_fix(sample_workers_data):
    """A worker with a fixed number of shifts/month (auto_calculate_shifts=False)
    must end up with exactly that number of shifts for the month, even if the
    final constraint-violation repair pass had to unassign one of them (e.g. to
    fix a 7/14-day pattern violation). The finalization phase must re-run the
    manual monthly-target enforcement AFTER the validation/fix pass so the
    obligatory monthly count is restored, using a different, non-conflicting
    date instead of the one that caused the violation."""
    from saldo27.schedule_builder import ScheduleBuilder
    from saldo27.scheduler_core import SchedulerCore

    workers = [dict(w) for w in sample_workers_data]
    workers[0]["auto_calculate_shifts"] = False
    workers[0]["target_shifts"] = 2
    workers[0]["_original_target_shifts"] = 2
    # The fixture's work_periods ("01-03-2026;31-03-2026") is two single-day
    # ranges (semicolon separates ranges, not a start/end pair), which would
    # make TargetCalculator treat this worker as available only 2 of 31 days.
    # Clear it so the worker is available the whole test period, matching the
    # scenario under test (a full month, fixed monthly quota of 2).
    workers[0]["work_periods"] = ""

    # Use a full month period so TargetCalculator's proportional-months calculation
    # (based on work_periods availability) lands on the full fixed monthly quota
    # of 2, instead of being scaled down by a short test period.
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 31),
            "num_shifts": 4,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    first_date = datetime(2026, 3, 2)
    second_date = datetime(2026, 3, 9)  # 7 days later, same weekday -> weekly_pattern violation

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

    # The violation must be gone (weekly_pattern is fixed)...
    remaining = scheduler._check_schedule_constraints()
    assert not any(v["type"] == "weekly_pattern" for v in remaining)

    # ...but the worker's fixed monthly shift count must still be satisfied,
    # using a replacement date instead of simply losing the shift.
    assert len(scheduler.worker_assignments["DOC001"]) == 2


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
