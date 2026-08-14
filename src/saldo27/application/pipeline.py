from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from saldo27.domain.schedule_state import ScheduleState


class PipelinePhase(Protocol):
    name: str

    def run(self, core: object, state: ScheduleState) -> tuple[bool, ScheduleState]: ...


PhaseRunner = Callable[[object], bool]


@dataclass(frozen=True)
class CoreMethodPhase:
    name: str
    runner: PhaseRunner

    def run(self, core: object, state: ScheduleState) -> tuple[bool, ScheduleState]:
        success = self.runner(core)
        if not success:
            return False, state
        scheduler = core.scheduler
        return True, ScheduleState.from_scheduler(scheduler)


@dataclass(frozen=True)
class PhaseTrace:
    name: str
    success: bool
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    metrics: dict[str, float | int]


@dataclass
class OptimizationPipeline:
    phases: list[PipelinePhase]

    def run(self, core: object, initial_state: ScheduleState) -> tuple[bool, ScheduleState, list[PhaseTrace]]:
        current_state = initial_state
        trace: list[PhaseTrace] = []

        for phase in self.phases:
            started_at = datetime.now()
            success, current_state = phase.run(core, current_state)
            finished_at = datetime.now()

            trace.append(
                PhaseTrace(
                    name=phase.name,
                    success=success,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=(finished_at - started_at).total_seconds(),
                    metrics=current_state.to_metrics_dict(),
                )
            )

            if not success:
                return False, current_state, trace

        return True, current_state, trace
