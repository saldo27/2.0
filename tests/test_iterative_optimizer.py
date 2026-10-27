"""Tests for IterativeOptimizer._can_worker_take_shift."""

from datetime import datetime, timedelta

import pytest

from saldo27.iterative_optimizer import IterativeOptimizer
from saldo27.schedule_builder import ScheduleBuilder
from saldo27.scheduler import Scheduler


@pytest.fixture
def single_worker_scheduler():
    """March 2026, 2 posts per day, one auto worker 'A' with fixed targets."""
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 31),
            "num_shifts": 2,
            "workers_data": [
                {
                    "id": "A",
                    "target_shifts": 2,
                    "work_percentage": 100,
                    "work_periods": "",
                    "mandatory_days": "",
                    "days_off": "",
                },
            ],
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 3,
            "max_consecutive_weekends": 3,
        }
    )
    worker = scheduler.workers_data[0]
    worker["target_shifts"] = 4
    worker["_raw_target"] = 4
    worker["monthly_targets"] = {}
    return scheduler


def _optimizer_for(scheduler):
    scheduler.schedule_builder = ScheduleBuilder(scheduler)
    optimizer = IterativeOptimizer()
    optimizer.scheduler = scheduler
    return optimizer


def _empty_march_schedule():
    return {datetime(2026, 3, 1) + timedelta(days=i): [None, None] for i in range(31)}


def test_can_take_shift_excludes_semicolon_separated_mandatory_days(single_worker_scheduler):
    """mandatory_days uses ';' separators; those shifts must not count against target_shifts."""
    worker = single_worker_scheduler.workers_data[0]
    worker["mandatory_days"] = "03-03-2026;11-03-2026"
    worker["target_shifts"] = 2
    optimizer = _optimizer_for(single_worker_scheduler)

    schedule = _empty_march_schedule()
    schedule[datetime(2026, 3, 3)][0] = "A"
    schedule[datetime(2026, 3, 11)][0] = "A"

    # 2 mandatory + 0 free assigned; a third (free) shift on Thursday 19 is within target_shifts=2.
    assert optimizer._can_worker_take_shift(
        "A", datetime(2026, 3, 19), "Post_0", schedule, single_worker_scheduler.workers_data
    )


def test_can_take_shift_blocks_second_consecutive_last_post(single_worker_scheduler):
    optimizer = _optimizer_for(single_worker_scheduler)
    schedule = _empty_march_schedule()
    schedule[datetime(2026, 3, 3)][1] = "A"
    workers = single_worker_scheduler.workers_data

    assert not optimizer._can_worker_take_shift("A", datetime(2026, 3, 19), "Post_1", schedule, workers)
    assert optimizer._can_worker_take_shift("A", datetime(2026, 3, 19), "Post_0", schedule, workers)


def test_can_take_shift_allows_consecutive_last_post_for_only_last_post(single_worker_scheduler):
    single_worker_scheduler.workers_data[0]["only_last_post"] = True
    optimizer = _optimizer_for(single_worker_scheduler)
    schedule = _empty_march_schedule()
    schedule[datetime(2026, 3, 3)][1] = "A"
    workers = single_worker_scheduler.workers_data

    assert optimizer._can_worker_take_shift("A", datetime(2026, 3, 19), "Post_1", schedule, workers)
    assert not optimizer._can_worker_take_shift("A", datetime(2026, 3, 19), "Post_0", schedule, workers)
