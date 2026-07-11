from datetime import datetime

from saldo27.scheduler import Scheduler
from saldo27.scheduler_core import SchedulerCore


def test_sanitize_restored_attempt_state_removes_non_mandatory_prefill():
    workers_data = [
        {
            "id": "DOC001",
            "name": "DOC001",
            "target_shifts": 3,
            "work_percentage": 100,
            "mandatory_days": "01-03-2026",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
        {
            "id": "DOC002",
            "name": "DOC002",
            "target_shifts": 3,
            "work_percentage": 100,
            "mandatory_days": "",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
        {
            "id": "DOC003",
            "name": "DOC003",
            "target_shifts": 3,
            "work_percentage": 100,
            "mandatory_days": "",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
        {
            "id": "DOC004",
            "name": "DOC004",
            "target_shifts": 3,
            "work_percentage": 100,
            "mandatory_days": "",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
    ]
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 3),
            "num_shifts": 4,
            "workers_data": workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    scheduler_core = SchedulerCore(scheduler)

    assert scheduler_core._initialize_schedule_phase() is True
    assert scheduler_core._assign_mandatory_phase() is True

    assert len(scheduler.schedule_builder._locked_mandatory) == 1

    contaminated_date = datetime(2026, 3, 2)
    scheduler.schedule[contaminated_date][0] = "DOC002"
    scheduler._synchronize_tracking_data()

    stats = scheduler_core._sanitize_restored_attempt_state()

    assert scheduler.schedule[contaminated_date][0] is None
    assert scheduler.worker_assignments["DOC002"] == set()
    assert scheduler.worker_shift_counts["DOC002"] == 0

    assert stats == {
        "total_slots": 12,
        "protected_slots": 1,
        "stray_prefilled_slots": 1,
        "empty_slots": 11,
    }
