from datetime import datetime

from saldo27.domain.schedule_state import ScheduleState
from saldo27.scheduler import Scheduler


def _build_scheduler(sample_workers_data):
    return Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 3),
            "num_shifts": 2,
            "workers_data": sample_workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )


def test_schedule_state_snapshot_and_restore(sample_workers_data):
    scheduler = _build_scheduler(sample_workers_data)
    date = datetime(2026, 3, 1)
    scheduler.schedule[date][0] = "DOC001"
    scheduler._synchronize_tracking_data()

    state = ScheduleState.from_scheduler(scheduler)

    scheduler.schedule[date][0] = None
    scheduler._synchronize_tracking_data()

    state.apply_to_scheduler(scheduler)

    assert scheduler.schedule[date][0] == "DOC001"
    assert date in scheduler.worker_assignments["DOC001"]


def test_schedule_state_metrics(sample_workers_data):
    scheduler = _build_scheduler(sample_workers_data)
    date = datetime(2026, 3, 1)
    scheduler.schedule[date][0] = "DOC001"

    state = ScheduleState.from_scheduler(scheduler)
    metrics = state.to_metrics_dict()

    assert metrics["total_slots"] == 6
    assert metrics["filled_slots"] == 1
    assert metrics["empty_slots"] == 5
