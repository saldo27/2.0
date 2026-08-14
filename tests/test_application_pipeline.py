from datetime import datetime

from saldo27.application.pipeline import CoreMethodPhase, OptimizationPipeline
from saldo27.domain.schedule_state import ScheduleState


class _DummyCore:
    def __init__(self):
        self.scheduler = type("SchedulerStub", (), {})()
        self.scheduler.schedule = {datetime(2026, 3, 1): [None]}
        self.scheduler.worker_assignments = {"W1": set()}
        self.scheduler.worker_shift_counts = {"W1": 0}
        self.scheduler.worker_weekend_counts = {"W1": 0}
        self.scheduler.worker_posts = {"W1": set()}

    def ok(self):
        self.scheduler.schedule[datetime(2026, 3, 1)][0] = "W1"
        self.scheduler.worker_assignments["W1"].add(datetime(2026, 3, 1))
        self.scheduler.worker_shift_counts["W1"] = 1
        return True

    def fail(self):
        return False


def test_pipeline_stops_on_first_failure():
    core = _DummyCore()
    pipeline = OptimizationPipeline(
        [
            CoreMethodPhase("ok", lambda c: c.ok()),
            CoreMethodPhase("fail", lambda c: c.fail()),
            CoreMethodPhase("after", lambda c: c.ok()),
        ]
    )
    initial = ScheduleState.from_scheduler(core.scheduler)

    success, _, trace = pipeline.run(core, initial)

    assert success is False
    assert [item.name for item in trace] == ["ok", "fail"]


def test_pipeline_collects_phase_metrics():
    core = _DummyCore()
    pipeline = OptimizationPipeline([CoreMethodPhase("ok", lambda c: c.ok())])

    success, state, trace = pipeline.run(core, ScheduleState.from_scheduler(core.scheduler))

    assert success is True
    assert state.to_metrics_dict()["filled_slots"] == 1
    assert trace[0].metrics["filled_slots"] == 1
