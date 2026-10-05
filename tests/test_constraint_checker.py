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
