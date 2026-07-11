from datetime import datetime

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
