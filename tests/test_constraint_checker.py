"""The spacing decision is one function. Callers only supply dates and the gap."""

from datetime import datetime

from saldo27.constraint_checker import candidate_spacing_rejection
from saldo27.schedule_builder import ScheduleBuilder
from saldo27.scheduler import Scheduler


def test_spacing_rejection_covers_gap_patterns_and_mandatory_exemption():
    friday = datetime(2026, 3, 6)
    monday = datetime(2026, 3, 9)
    thursday = datetime(2026, 3, 5)
    saturday = datetime(2026, 3, 7)
    next_friday = datetime(2026, 3, 13)

    friday_monday = candidate_spacing_rejection(
        monday, [friday], min_gap=1, date_is_mandatory=False, block_gap2_weekend_pairs=False
    )
    assert friday_monday is not None
    assert friday_monday.reason == "friday_monday"
    assert (
        candidate_spacing_rejection(monday, [friday], min_gap=1, date_is_mandatory=True, block_gap2_weekend_pairs=False)
        is None
    )

    pattern_714 = candidate_spacing_rejection(
        next_friday, [friday], min_gap=1, date_is_mandatory=False, block_gap2_weekend_pairs=False
    )
    assert pattern_714 is not None
    assert pattern_714.reason == "pattern_714"
    assert (
        candidate_spacing_rejection(
            next_friday, [friday], min_gap=1, date_is_mandatory=True, block_gap2_weekend_pairs=False
        )
        is None
    )

    gap = candidate_spacing_rejection(
        saturday, [thursday], min_gap=3, date_is_mandatory=True, block_gap2_weekend_pairs=True
    )
    assert gap is not None
    assert gap.reason == "gap"

    bridge = candidate_spacing_rejection(
        saturday, [thursday], min_gap=2, date_is_mandatory=True, block_gap2_weekend_pairs=True
    )
    assert bridge is not None
    assert bridge.reason == "gap2_weekend"
    assert (
        candidate_spacing_rejection(
            saturday, [thursday], min_gap=2, date_is_mandatory=False, block_gap2_weekend_pairs=False
        )
        is None
    )


def _gap_period_scheduler(workers, periods):
    return Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 31),
            "num_shifts": 2,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "gap_periods": periods,
            "max_consecutive_weekends": 3,
        }
    )


def test_gap_periods_relax_inside_the_interval_and_keep_the_stricter_side(sample_workers_data):
    """Default gap 4. 1–10 March allows 2 days. A pair that leaves the interval still needs 4."""
    scheduler = _gap_period_scheduler(
        sample_workers_data,
        [{"start_date": datetime(2026, 3, 1), "end_date": datetime(2026, 3, 10), "gap": 2}],
    )
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    monday = datetime(2026, 3, 2)
    thursday = datetime(2026, 3, 5)
    later_monday = datetime(2026, 3, 16)
    later_thursday = datetime(2026, 3, 19)
    bridge_saturday = datetime(2026, 3, 7)

    def _place(date):
        for day_slots in scheduler.schedule.values():
            for index, worker_id in enumerate(day_slots):
                if worker_id == "DOC001":
                    day_slots[index] = None
        scheduler.schedule[date][0] = "DOC001"
        scheduler._synchronize_tracking_data()

    _place(monday)
    assert scheduler.constraint_checker._check_gap_constraint("DOC001", thursday) is True
    assert scheduler.schedule_builder._spacing_rejection("DOC001", thursday) is None

    # Thu–Sat is exactly 2 days and crosses the weekend, so the gap-2 bridge rule still applies.
    _place(thursday)
    bridge = scheduler.schedule_builder._spacing_rejection("DOC001", bridge_saturday)
    assert bridge is not None and bridge.reason == "gap2_weekend"

    _place(later_monday)
    assert scheduler.constraint_checker._check_gap_constraint("DOC001", later_thursday) is False

    # One date inside the loose interval and one outside: the outside date's gap of 4 wins.
    _place(datetime(2026, 3, 10))
    assert scheduler.constraint_checker._check_gap_constraint("DOC001", datetime(2026, 3, 13)) is False


def test_builder_and_checker_share_the_friday_monday_decision(sample_workers_data):
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
    scheduler.schedule_builder = ScheduleBuilder(scheduler)

    worker = next(w for w in sample_workers_data if w["id"] == "DOC001")
    assert scheduler.constraint_checker._check_gap_constraint("DOC001", monday) is False
    assert scheduler.schedule_builder._check_gap_constraint_simulated("DOC001", monday, {"DOC001": {friday}}) is False
    assert scheduler.schedule_builder._check_gap_constraints(worker, monday, 0) is False
    assert scheduler.schedule_builder._spacing_rejection("DOC001", monday) is not None


def test_incompatibility_is_checked_in_both_directions(sample_workers_data):
    """DOC003 lists DOC001 as incompatible; DOC001 lists nobody. Neither may join the other."""
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
    date = datetime(2026, 3, 5)
    checker = scheduler.constraint_checker

    scheduler.schedule[date][0] = "DOC003"
    assert checker._check_incompatibility("DOC001", date) is False
    assert checker._check_incompatibility("DOC002", date) is True

    scheduler.schedule[date][0] = "DOC001"
    assert checker._check_incompatibility("DOC003", date) is False


def _weekend_scheduler(workers, *, max_consecutive=2, proportional=False, tolerance=1):
    return Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 31),
            "num_shifts": 2,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": max_consecutive,
            "enable_proportional_weekends": proportional,
            "weekend_tolerance": tolerance,
        }
    )


def test_consecutive_weekend_run_counts_weeks_on_both_sides(sample_workers_data):
    """Filling the gap between two weekends makes a run of three."""
    checker = _weekend_scheduler(sample_workers_data).constraint_checker
    assigned = {datetime(2026, 3, 6), datetime(2026, 3, 20)}

    assert checker._would_exceed_weekend_limit("DOC001", datetime(2026, 3, 14), assigned_dates=assigned) is True
    assert checker._would_exceed_weekend_limit("DOC001", datetime(2026, 3, 27), assigned_dates=assigned) is False


def test_consecutive_weekend_run_continues_from_prior_period(sample_workers_data):
    scheduler = _weekend_scheduler(sample_workers_data)
    checker = scheduler.constraint_checker
    march_6 = datetime(2026, 3, 6)

    assert checker._would_exceed_weekend_limit("DOC001", march_6, assigned_dates=set()) is False
    scheduler.prior_assignments = {"DOC001": {datetime(2026, 2, 20), datetime(2026, 2, 27)}}
    assert checker._would_exceed_weekend_limit("DOC001", march_6, assigned_dates=set()) is True


def test_two_weekend_shifts_in_the_same_week_are_allowed(sample_workers_data):
    checker = _weekend_scheduler(sample_workers_data).constraint_checker

    assert (
        checker._would_exceed_weekend_limit("DOC001", datetime(2026, 3, 8), assigned_dates={datetime(2026, 3, 6)})
        is False
    )


def test_proportional_cap_uses_objective_plus_tolerance(sample_workers_data):
    assigned = {datetime(2026, 3, 6), datetime(2026, 3, 13)}
    march_27 = datetime(2026, 3, 27)

    strict = _weekend_scheduler(sample_workers_data, max_consecutive=3, proportional=True, tolerance=0)
    strict.get_weekend_objective = lambda worker: 2.0
    assert strict.constraint_checker._would_exceed_weekend_limit("DOC001", march_27, assigned_dates=assigned) is True

    loose = _weekend_scheduler(sample_workers_data, max_consecutive=3, proportional=True, tolerance=1)
    loose.get_weekend_objective = lambda worker: 2.0
    assert loose.constraint_checker._would_exceed_weekend_limit("DOC001", march_27, assigned_dates=assigned) is False


def test_proportional_cap_is_off_when_disabled(sample_workers_data):
    scheduler = _weekend_scheduler(sample_workers_data, max_consecutive=3, proportional=False, tolerance=0)
    scheduler.get_weekend_objective = lambda worker: 2.0
    assigned = {datetime(2026, 3, 6), datetime(2026, 3, 13)}

    assert (
        scheduler.constraint_checker._would_exceed_weekend_limit(
            "DOC001", datetime(2026, 3, 27), assigned_dates=assigned
        )
        is False
    )
