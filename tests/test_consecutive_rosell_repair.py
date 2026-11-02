"""Repair of consecutive Rosell (last-post) violations must preserve coverage."""

from datetime import datetime

from saldo27.schedule_builder import ScheduleBuilder
from saldo27.scheduler import Scheduler


def _worker(worker_id: str, **overrides):
    worker = {
        "id": worker_id,
        "name": worker_id,
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
        "no_last_post": False,
        "only_last_post": False,
    }
    worker.update(overrides)
    return worker


def _make_scheduler(workers, *, start=datetime(2027, 3, 1), end=datetime(2027, 3, 31)):
    scheduler = Scheduler(
        {
            "start_date": start,
            "end_date": end,
            "num_shifts": 4,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 2,
            "max_consecutive_weekends": 3,
        }
    )
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    return scheduler


def test_consecutive_rosell_repair_prefers_same_day_swap():
    """When a partner can take Rosell, repair swaps instead of leaving a hole."""
    workers = [
        _worker("ELENA"),
        _worker("MARIA"),
        _worker("VALLE"),
        _worker("LAURA"),
        _worker("JESUS"),
        _worker("KIKO"),
        _worker("MAR"),
        _worker("DOC001"),
    ]
    scheduler = _make_scheduler(workers)
    first = datetime(2027, 3, 17)
    second = datetime(2027, 3, 29)
    between = datetime(2027, 3, 13)

    scheduler.schedule[between] = ["MARIA", "VALLE", "LAURA", "DOC001"]
    scheduler.schedule[first] = ["JESUS", "KIKO", "MAR", "ELENA"]
    scheduler.schedule[second] = ["MARIA", "VALLE", "LAURA", "ELENA"]
    scheduler._tracking_state.synchronize()

    violations = [
        {
            "type": "consecutive_last_post",
            "worker_id": "ELENA",
            "date1": first,
            "date2": second,
            "run_length": 2,
        }
    ]
    fixes = scheduler._fix_constraint_violations(violations)
    assert fixes == 1
    assert scheduler.schedule[second][3] != "ELENA"
    assert None not in scheduler.schedule[second]
    assert "ELENA" in scheduler.schedule[second]
    assert second in scheduler.worker_assignments["ELENA"]


def test_consecutive_rosell_repair_refills_when_swap_impossible():
    """If nobody on the day can take Rosell, unassign then refill from outside."""
    workers = [
        _worker("ELENA"),
        _worker("NO1", no_last_post=True),
        _worker("NO2", no_last_post=True),
        _worker("NO3", no_last_post=True),
        _worker("FILLER"),
        _worker("OTHER"),
    ]
    scheduler = _make_scheduler(workers)
    first = datetime(2027, 3, 10)
    second = datetime(2027, 3, 20)

    # Pre-seed enough free days so FILLER is available on second.
    scheduler.schedule[first] = ["OTHER", "NO1", "NO2", "ELENA"]
    scheduler.schedule[second] = ["NO1", "NO2", "NO3", "ELENA"]
    for day in (datetime(2027, 3, 1), datetime(2027, 3, 5), datetime(2027, 3, 15), datetime(2027, 3, 25)):
        scheduler.schedule[day] = ["OTHER", "NO1", "NO2", "NO3"]
    scheduler._tracking_state.synchronize()

    # FILLER has no assignments yet and can take Rosell on second.
    assert second not in scheduler.worker_assignments.get("FILLER", set())

    violations = [
        {
            "type": "consecutive_last_post",
            "worker_id": "ELENA",
            "date1": first,
            "date2": second,
            "run_length": 2,
        }
    ]
    fixes = scheduler._fix_constraint_violations(violations)
    assert fixes == 1
    assert "ELENA" not in scheduler.schedule[second]
    assert scheduler.schedule[second][3] is not None
    assert second not in scheduler.worker_assignments["ELENA"]
    assert second in scheduler.worker_assignments[scheduler.schedule[second][3]]
