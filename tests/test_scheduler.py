import json
from datetime import datetime
from math import isclose
from typing import Any, cast

import pytest

from saldo27.schedule_builder import ScheduleBuilder
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


def test_bridge_shift_in_the_second_year_is_counted(sample_workers_data):
    """A Friday holiday in the year after the schedule starts is a bridge day.
    A shift on that Friday counts as an assigned bridge shift."""
    workers = [dict(sample_workers_data[0])]
    workers[0]["id"] = "LUIS H"
    workers[0]["name"] = "LUIS H"
    workers[0]["work_periods"] = ""
    friday_holiday = datetime(2027, 3, 19)
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 12, 23),
            "end_date": datetime(2027, 6, 30),
            "num_shifts": 4,
            "workers_data": workers,
            "holidays": [friday_holiday],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    scheduler.schedule[friday_holiday] = ["LUIS H", None, None, None]

    assert friday_holiday.weekday() == 4
    assert scheduler.date_utils.is_bridge_day(friday_holiday, scheduler.bridge_periods) is True
    assert scheduler.count_bridges_for_worker("LUIS H") == 1


def test_bridge_objective_uses_cadence_shift_count_not_the_zero_budget():
    """A cadence worker's target_shifts stays 0. The bridge objective must follow
    _raw_target (the cadence length), which can be higher than a 100% worker."""
    workers = [
        {
            "id": "CAD",
            "name": "CAD",
            "target_shifts": 0,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
            "incompatible_with": [],
            "auto_calculate_shifts": False,
            "has_cadence": False,
        },
        {
            "id": "FULL",
            "name": "FULL",
            "target_shifts": 20,
            "work_percentage": 100,
            "work_periods": "",
            "mandatory_days": "",
            "days_off": "",
            "incompatible_with": [],
            "auto_calculate_shifts": True,
            "has_cadence": False,
        },
    ]
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 8),
            "num_shifts": 1,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    for worker in scheduler.workers_data:
        if worker["id"] == "CAD":
            worker["target_shifts"] = 0
            worker["_raw_target"] = 30
        else:
            worker["target_shifts"] = 20
            worker["_raw_target"] = 20

    scheduler.bridge_periods = [
        {
            "id": "bridge_test",
            "start_date": datetime(2026, 3, 6),
            "end_date": datetime(2026, 3, 8),
            "holiday": datetime(2026, 3, 6),
            "type": "friday",
        }
    ]
    for day in (6, 7, 8):
        scheduler.schedule[datetime(2026, 3, day)] = ["FULL"]

    cadence_objective = scheduler.get_bridge_objective_for_worker("CAD")
    full_objective = scheduler.get_bridge_objective_for_worker("FULL")

    assert cadence_objective == 3 * 30 / 50
    assert full_objective == 3 * 20 / 50
    assert cadence_objective > full_objective


def _variable_shifts_scheduler(workers):
    """4 posts per day, except 10-12 March with 3 posts."""
    return Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 15),
            "num_shifts": 4,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [
                {"start_date": datetime(2026, 3, 10), "end_date": datetime(2026, 3, 12), "shifts": 3},
            ],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": 3,
        }
    )


def test_weekend_objective_is_proportional_to_weekend_slots(sample_workers_data):
    """1-15 March, 4 posts, 3 posts on 13-15 March (Fri-Sun): 25 of 57 slots are weekend."""
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 15),
            "num_shifts": 4,
            "workers_data": [dict(w) for w in sample_workers_data],
            "holidays": [],
            "variable_shifts": [
                {"start_date": datetime(2026, 3, 13), "end_date": datetime(2026, 3, 15), "shifts": 3},
            ],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": 3,
        }
    )

    assert scheduler._weekend_slot_ratio() == pytest.approx(25 / 57)
    assert scheduler.get_weekend_objective({"_raw_target": 57}) == pytest.approx(25)


def test_last_post_is_the_last_slot_of_each_day(sample_workers_data):
    scheduler = _variable_shifts_scheduler([dict(w) for w in sample_workers_data])

    assert scheduler.is_last_post(datetime(2026, 3, 9), 3)
    assert scheduler.last_post_index(datetime(2026, 3, 10)) == 2
    assert scheduler.is_last_post(datetime(2026, 3, 10), 2)
    assert not scheduler.is_last_post(datetime(2026, 3, 9), 2)


def test_last_post_objective_shares_rosell_days_left_after_only_last_post(sample_workers_data):
    """10 Rosell days; C (only_last_post) covers 4, so A and B share the other 6 by objective."""
    workers = [dict(w) for w in sample_workers_data]
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 10),
            "num_shifts": 2,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": 3,
        }
    )
    a, b, c, d = scheduler.workers_data
    for worker, objective in ((a, 8), (b, 4), (c, 4), (d, 4)):
        worker["_raw_target"] = objective
    c["only_last_post"] = True
    d["no_last_post"] = True

    assert scheduler.get_last_post_objective(a) == 4.0
    assert scheduler.get_last_post_objective(b) == 2.0
    assert scheduler.get_last_post_objective(c) == 4.0
    assert scheduler.get_last_post_objective(d) == 0.0


def test_post_restrictions_follow_the_last_slot_on_short_days(sample_workers_data):
    """On a 3-post day the last post is index 2, for both no_last_post and only_last_post."""
    from saldo27.schedule_builder import ScheduleBuilder

    workers = [dict(w) for w in sample_workers_data]
    workers[0]["only_last_post"] = True
    workers[1]["no_last_post"] = True
    for w in workers[:2]:
        w["work_periods"] = ""
    scheduler = _variable_shifts_scheduler(workers)
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    builder = scheduler.schedule_builder
    short_day = datetime(2026, 3, 11)

    assert builder._check_hard_constraints("DOC001", short_day, 2) is True
    assert builder._check_hard_constraints("DOC001", short_day, 1) is False
    assert builder._check_hard_constraints("DOC002", short_day, 2) is False
    assert builder._check_hard_constraints("DOC002", short_day, 1) is True


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


def test_mandatory_assignment_reserves_non_last_posts_for_restricted_workers(sample_workers_data):
    """Unrestricted mandatory workers must not consume every non-last post
    before a no_last_post worker is placed. With 4 posts and 4 mandatory
    workers (2 of them no_last_post), everyone fits and the last post goes
    to someone who is allowed to take it."""
    from saldo27.schedule_builder import ScheduleBuilder

    def _worker(worker_id, *, no_last_post=False, mandatory="01-03-2026"):
        base = dict(sample_workers_data[0])
        base.update(
            {
                "id": worker_id,
                "name": worker_id,
                "mandatory_days": mandatory,
                "no_last_post": no_last_post,
                "only_last_post": False,
                "days_off": "",
                "work_periods": "",
            }
        )
        return base

    # Same shape as 25-12-2026: two unrestricted workers listed before two
    # no_last_post workers, plus a fifth worker who is free that day.
    workers = [
        _worker("JULIA"),
        _worker("KIKO", no_last_post=True),
        _worker("BEA"),
        _worker("VALLE", no_last_post=True),
        _worker("JOSE", mandatory="10-03-2026"),
    ]
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
    scheduler.schedule_builder._assign_mandatory_guards()

    day = datetime(2026, 3, 1)
    assigned = scheduler.schedule[day]
    assert set(assigned) == {"JULIA", "KIKO", "BEA", "VALLE"}
    assert assigned[3] not in {"KIKO", "VALLE"}
    assert "JOSE" not in assigned


def test_cadence_worker_rotates_through_last_post(sample_workers_data):
    """A cadence worker with no_last_post=False must not stay in slot 0.
    Successive cadence dates rotate across every post, including the last one."""
    from saldo27.schedule_builder import ScheduleBuilder

    base = dict(sample_workers_data[0])
    base.update(
        {
            "id": "MARINA",
            "name": "MARINA",
            "has_cadence": True,
            "no_last_post": False,
            "only_last_post": False,
            "mandatory_days": "01-03-2026;07-03-2026;13-03-2026;19-03-2026",
            "days_off": "",
            "work_periods": "",
            "auto_calculate_shifts": False,
        }
    )
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 20),
            "num_shifts": 4,
            "workers_data": [base],
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    scheduler.schedule_builder._assign_mandatory_guards()

    posts = [scheduler.schedule[datetime(2026, 3, day)].index("MARINA") for day in (1, 7, 13, 19)]
    assert posts == [0, 1, 2, 3]


def test_mandatory_worker_rotates_posts_and_respects_last_post_flags(sample_workers_data):
    """Mandatory dates lock the day, not the slot. Successive mandatory days
    rotate through the posts the worker is allowed to take."""
    from saldo27.schedule_builder import ScheduleBuilder

    def _posts_for(**flags):
        base = dict(sample_workers_data[0])
        base.update(
            {
                "id": "DOCX",
                "name": "DOCX",
                "has_cadence": False,
                "no_last_post": False,
                "only_last_post": False,
                "mandatory_days": "01-03-2026;07-03-2026;13-03-2026;19-03-2026",
                "days_off": "",
                "work_periods": "",
                "auto_calculate_shifts": True,
            }
        )
        base.update(flags)
        scheduler = Scheduler(
            {
                "start_date": datetime(2026, 3, 1),
                "end_date": datetime(2026, 3, 20),
                "num_shifts": 4,
                "workers_data": [base],
                "holidays": [],
                "variable_shifts": [],
                "gap_between_shifts": 4,
                "max_consecutive_weekends": 3,
            }
        )
        scheduler.schedule_builder = ScheduleBuilder(scheduler)
        scheduler.schedule_builder._assign_mandatory_guards()
        return [scheduler.schedule[datetime(2026, 3, day)].index("DOCX") for day in (1, 7, 13, 19)]

    assert _posts_for() == [0, 1, 2, 3]
    assert _posts_for(no_last_post=True) == [0, 1, 2, 0]
    assert _posts_for(only_last_post=True) == [3, 3, 3, 3]


def test_cadence_worker_can_swap_last_post_on_the_same_day(sample_workers_data):
    """Mandatory dates stay fixed. A same-day last-post swap is allowed for
    every mandatory worker. Removing the date is not."""
    from saldo27.schedule_builder import ScheduleBuilder

    cadence = dict(sample_workers_data[0])
    cadence.update(
        {
            "id": "MARINA",
            "name": "MARINA",
            "has_cadence": True,
            "no_last_post": False,
            "only_last_post": False,
            "mandatory_days": "01-03-2026",
            "days_off": "",
            "work_periods": "",
            "auto_calculate_shifts": False,
        }
    )
    mandatory = dict(sample_workers_data[1])
    mandatory.update(
        {
            "mandatory_days": "02-03-2026",
            "has_cadence": False,
            "days_off": "",
            "work_periods": "",
        }
    )
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 10),
            "num_shifts": 4,
            "workers_data": [cadence, mandatory],
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    builder = ScheduleBuilder(scheduler)
    scheduler.schedule_builder = builder
    cadence_day = datetime(2026, 3, 1)
    other_day = datetime(2026, 3, 2)
    builder._locked_mandatory.add(("MARINA", cadence_day))
    builder._locked_mandatory.add(("DOC002", other_day))

    assert builder._can_modify_assignment("MARINA", cadence_day, "adjust_last_post") is True
    assert builder._can_modify_assignment("MARINA", cadence_day, "strict_last_post_B") is True
    assert builder._can_modify_assignment("MARINA", cadence_day, "fix_consecutive_last_post") is False
    assert builder._can_modify_assignment("MARINA", cadence_day, "balance_workloads") is False
    assert builder._can_modify_assignment("DOC002", other_day, "adjust_last_post") is True
    assert builder._can_modify_assignment("DOC002", other_day, "balance_workloads") is False


def test_fix_constraint_violations_always_fixes_non_mandatory_weekly_pattern(sample_workers_data):
    """HARD INVARIANT: a weekly_pattern (7/14-day same-weekday) violation must
    always be fixed by the final validation/fix pass unless BOTH colliding dates
    are mandatory_days for that worker."""
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
        def is_mandatory(self, worker_id, date):
            return False

        def _can_modify_assignment(self, worker_id, date, reason, enforce_monthly_target_floor=True):
            return True

        def get_locked_mandatory(self):
            return set()

    scheduler.schedule_builder = cast("Any", _StubBuilder())

    fixes_made = scheduler.validate_and_fix_final_schedule()

    assert fixes_made == 1
    assert scheduler.schedule[first_date][0] == "DOC001"
    assert scheduler.schedule[second_date][0] is None
    assert scheduler.worker_assignments["DOC001"] == {first_date}


def test_friday_monday_blocked_regardless_of_gap(sample_workers_data):
    """Friday-Monday is removed even when the worker's minimum gap is below 3."""
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 15),
            "num_shifts": 2,
            "workers_data": sample_workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": 3,
        }
    )
    friday = datetime(2026, 3, 6)
    monday = datetime(2026, 3, 9)
    scheduler.schedule[friday][0] = "DOC001"
    scheduler._synchronize_tracking_data()

    assert scheduler.constraint_checker._check_gap_constraint("DOC001", monday) is False

    scheduler.schedule[monday][0] = "DOC001"
    scheduler._synchronize_tracking_data()
    fixes_made = scheduler.validate_and_fix_final_schedule()

    assert fixes_made == 1
    assert scheduler.schedule[friday][0] == "DOC001"
    assert scheduler.schedule[monday][0] is None


def test_mandatory_shift_may_land_on_friday_monday_and_714(sample_workers_data):
    """A mandatory date may be placed on a Friday-Monday or 7/14 interval."""
    workers = [dict(w) for w in sample_workers_data]
    workers[0]["mandatory_days"] = "09-03-2026"
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 15),
            "num_shifts": 2,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": 3,
        }
    )
    friday = datetime(2026, 3, 6)
    monday = datetime(2026, 3, 9)
    earlier_monday = datetime(2026, 3, 2)
    scheduler.schedule[friday][0] = "DOC001"
    scheduler.schedule[earlier_monday][0] = "DOC001"
    scheduler._synchronize_tracking_data()

    # Monday is mandatory, so both the Friday-Monday pair and the 7-day pair are allowed.
    assert scheduler.constraint_checker._check_gap_constraint("DOC001", monday) is True


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
            "enable_hole_fill": False,
        }
    )
    first_date = datetime(2026, 3, 2)
    second_date = datetime(2026, 3, 9)

    scheduler.schedule[first_date][0] = "DOC001"
    scheduler.schedule[second_date][0] = "DOC001"
    scheduler._synchronize_tracking_data()
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    assert scheduler.schedule_builder is not None
    scheduler.schedule_builder.best_schedule_data = {
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
            "enable_hole_fill": False,
        }
    )
    first_date = datetime(2026, 3, 2)
    second_date = datetime(2026, 3, 9)  # 7 days later, same weekday -> weekly_pattern violation

    scheduler.schedule[first_date][0] = "DOC001"
    scheduler.schedule[second_date][0] = "DOC001"
    scheduler._synchronize_tracking_data()
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    assert scheduler.schedule_builder is not None
    scheduler.schedule_builder.best_schedule_data = {
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


def test_pass1_scores_each_worker_once_per_empty_slot(sample_workers_data):
    """Pass 1 scores a worker once for a slot. Later relaxation steps reuse that score."""
    scheduler = _build_scheduler(sample_workers_data)
    builder = ScheduleBuilder(scheduler)
    scheduler.schedule_builder = builder
    calls: list[int] = []

    def _score(worker, date, post, relaxation_level=0):
        calls.append(relaxation_level)
        return float("-inf")

    builder._calculate_worker_score = _score  # ty: ignore[invalid-assignment]
    builder._try_fill_empty_shifts()

    empty_slots = sum(slot is None for posts in scheduler.schedule.values() for slot in posts)
    assert len(calls) == empty_slots * len(sample_workers_data)
    assert set(calls) == {1}
