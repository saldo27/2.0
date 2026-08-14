import types
from datetime import datetime

import pytest

from saldo27.application.pipeline import CoreMethodPhase, OptimizationPipeline
from saldo27.domain.schedule_state import ScheduleState


@pytest.fixture
def dummy_core():
    scheduler = types.SimpleNamespace(
        schedule={datetime(2026, 3, 1): [None]},
        worker_assignments={"W1": set()},
        worker_shift_counts={"W1": 0},
        worker_weekend_counts={"W1": 0},
        worker_posts={"W1": set()},
    )
    core = types.SimpleNamespace(scheduler=scheduler)

    def ok(c):
        c.scheduler.schedule[datetime(2026, 3, 1)][0] = "W1"
        c.scheduler.worker_assignments["W1"].add(datetime(2026, 3, 1))
        c.scheduler.worker_shift_counts["W1"] = 1
        return True

    def fail(c):
        return False

    core.ok = ok
    core.fail = fail
    return core


def test_pipeline_stops_on_first_failure(dummy_core):
    pipeline = OptimizationPipeline(
        [
            CoreMethodPhase("ok", lambda c: c.ok(c)),
            CoreMethodPhase("fail", lambda c: c.fail(c)),
            CoreMethodPhase("after", lambda c: c.ok(c)),
        ]
    )
    initial = ScheduleState.from_scheduler(dummy_core.scheduler)

    success, _, trace = pipeline.run(dummy_core, initial)

    assert success is False
    assert [item.name for item in trace] == ["ok", "fail"]


def test_pipeline_collects_phase_metrics(dummy_core):
    pipeline = OptimizationPipeline([CoreMethodPhase("ok", lambda c: c.ok(c))])

    success, state, trace = pipeline.run(dummy_core, ScheduleState.from_scheduler(dummy_core.scheduler))

    assert success is True
    assert state.to_metrics_dict()["filled_slots"] == 1
    assert trace[0].metrics["filled_slots"] == 1
