"""HoleFillEngine fills leftover empty slots without moving protected dates."""

from datetime import datetime

from saldo27.hole_fill_engine import HoleFillEngine
from saldo27.infrastructure import optional_engines
from saldo27.schedule_builder import ScheduleBuilder
from saldo27.scheduler import Scheduler


def _worker(worker_id: str, **overrides) -> dict:
    worker = {
        "id": worker_id,
        "name": worker_id,
        "target_shifts": 4,
        "_raw_target": 4,
        "work_percentage": 100,
        "work_periods": "",
        "days_off": "",
        "mandatory_days": "",
        "incompatible_with": [],
        "is_incompatible_all": False,
        "auto_calculate_shifts": True,
        "has_cadence": False,
        "monthly_targets": {"2026-03": 4},
    }
    worker.update(overrides)
    return worker


def _scheduler(workers: list[dict], posts: int = 1) -> Scheduler:
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 8),
            "num_shifts": posts,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": 3,
        }
    )
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    for worker in scheduler.workers_data:
        source = next(w for w in workers if w["id"] == worker["id"])
        worker["target_shifts"] = source["target_shifts"]
        worker["_raw_target"] = source.get("_raw_target", source["target_shifts"])
        worker["monthly_targets"] = source.get("monthly_targets", {"2026-03": source["target_shifts"]})
        worker["auto_calculate_shifts"] = source.get("auto_calculate_shifts", True)
        worker["has_cadence"] = source.get("has_cadence", False)
        worker["mandatory_days"] = source.get("mandatory_days", "")
    return scheduler


def _place(scheduler: Scheduler, layout: dict[datetime, list]) -> None:
    for day, posts in layout.items():
        scheduler.schedule[day] = list(posts)
    scheduler._synchronize_tracking_data()


def test_fills_an_empty_slot_when_an_auto_worker_has_room():
    scheduler = _scheduler([_worker("A"), _worker("B")])
    _place(
        scheduler,
        {
            datetime(2026, 3, 1): ["A"],
            datetime(2026, 3, 2): ["B"],
            datetime(2026, 3, 4): ["A"],
            datetime(2026, 3, 5): ["B"],
            datetime(2026, 3, 8): [None],
        },
    )

    filled = HoleFillEngine(scheduler).fill(time_limit_seconds=10)

    assert filled >= 1
    assert scheduler.schedule[datetime(2026, 3, 8)][0] in {"A", "B"}


def test_does_not_move_a_cadence_worker_off_its_day():
    cadence = _worker(
        "CAD",
        has_cadence=True,
        auto_calculate_shifts=False,
        target_shifts=0,
        _raw_target=1,
        mandatory_days="01-03-2026",
        monthly_targets={"2026-03": 0},
    )
    scheduler = _scheduler([cadence, _worker("B")])
    _place(
        scheduler,
        {
            datetime(2026, 3, 1): ["CAD"],
            datetime(2026, 3, 2): [None],
            datetime(2026, 3, 5): ["B"],
        },
    )

    HoleFillEngine(scheduler).fill(time_limit_seconds=10)

    assert scheduler.schedule[datetime(2026, 3, 1)][0] == "CAD"
    assert scheduler.schedule[datetime(2026, 3, 2)][0] != "CAD"


def test_does_not_add_a_free_shift_to_a_manual_already_at_target():
    manual = _worker("M", auto_calculate_shifts=False, target_shifts=2, _raw_target=2, monthly_targets={"2026-03": 2})
    scheduler = _scheduler([manual, _worker("B")])
    _place(
        scheduler,
        {
            datetime(2026, 3, 1): ["M"],
            datetime(2026, 3, 4): ["M"],
            datetime(2026, 3, 2): ["B"],
            datetime(2026, 3, 8): [None],
        },
    )

    HoleFillEngine(scheduler).fill(time_limit_seconds=10)

    assert scheduler.schedule[datetime(2026, 3, 8)][0] != "M"
    assert len(scheduler.worker_assignments["M"]) == 2


def test_skips_when_disabled_in_config():
    scheduler = _scheduler([_worker("A")])
    _place(scheduler, {datetime(2026, 3, 1): [None]})
    scheduler.config["enable_hole_fill"] = False

    assert HoleFillEngine(scheduler).fill() == 0
    assert scheduler.schedule[datetime(2026, 3, 1)][0] is None


def test_skips_when_ortools_is_missing():
    scheduler = _scheduler([_worker("A")])
    _place(scheduler, {datetime(2026, 3, 1): [None]})
    previous = optional_engines._ortools_available
    optional_engines._ortools_available = False
    try:
        assert HoleFillEngine(scheduler).fill() == 0
        assert scheduler.schedule[datetime(2026, 3, 1)][0] is None
    finally:
        optional_engines._ortools_available = previous


def test_does_not_fill_a_slot_that_violates_gap():
    scheduler = _scheduler([_worker("A", target_shifts=3, monthly_targets={"2026-03": 3})])
    _place(
        scheduler,
        {
            datetime(2026, 3, 1): ["A"],
            datetime(2026, 3, 2): [None],
        },
    )

    HoleFillEngine(scheduler).fill(time_limit_seconds=10)

    assert scheduler.schedule[datetime(2026, 3, 2)][0] is None


def test_fills_a_mostly_empty_calendar_without_aborting():
    """Regression: a wide empty grid used to abort CP-SAT natively on Windows."""
    scheduler = _scheduler([_worker("A"), _worker("B")], posts=4)

    filled = HoleFillEngine(scheduler).fill(time_limit_seconds=5)

    occupied = sum(1 for posts in scheduler.schedule.values() for w in posts if w is not None)
    assert filled >= 0
    assert occupied == filled


def test_does_not_empty_occupied_slots_without_a_coverage_gain():
    scheduler = _scheduler([_worker("A"), _worker("B")])
    original = {
        datetime(2026, 3, 1): ["A"],
        datetime(2026, 3, 2): ["B"],
        datetime(2026, 3, 4): ["A"],
        datetime(2026, 3, 5): ["B"],
    }
    _place(scheduler, original)
    empty_before = sum(1 for posts in scheduler.schedule.values() for w in posts if w is None)

    HoleFillEngine(scheduler).fill(time_limit_seconds=10)

    for day, _posts in original.items():
        assert scheduler.schedule[day][0] is not None
    empty_after = sum(1 for posts in scheduler.schedule.values() for w in posts if w is None)
    assert empty_after <= empty_before
