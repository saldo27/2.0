from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from saldo27.application.contracts import GenerationProgressEvent, SchedulerCoreProtocol
from saldo27.domain.schedule_state import ScheduleState
from saldo27.scheduler_config import SchedulerConfig

KNOWN_PHASE_NAMES: frozenset[str] = SchedulerConfig.VALID_PIPELINE_PHASES


def validate_phase_names(names: list[str]) -> list[str]:
    """Raise :exc:`ValueError` if *names* contains any unknown phase name.

    Returns *names* unchanged on success so the function can be used inline::

        phases = validate_phase_names(config.get("pipeline_phases", default_order))
    """
    unknown = [n for n in names if n not in KNOWN_PHASE_NAMES]
    if unknown:
        raise ValueError(f"Unknown pipeline phases: {unknown}. Valid phase names are: {sorted(KNOWN_PHASE_NAMES)}")
    return names


# ---------------------------------------------------------------------------
# Pipeline abstractions
# ---------------------------------------------------------------------------

PhaseRunner = Callable[[SchedulerCoreProtocol], bool]


class PipelinePhase(Protocol):
    name: str

    def run(self, core: SchedulerCoreProtocol, state: ScheduleState) -> tuple[bool, ScheduleState]: ...


@dataclass(frozen=True)
class CoreMethodPhase:
    name: str
    runner: PhaseRunner

    def run(self, core: SchedulerCoreProtocol, state: ScheduleState) -> tuple[bool, ScheduleState]:
        success = self.runner(core)
        if not success:
            return False, state
        scheduler = core.scheduler  # type: ignore[attr-defined]
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

    def run(
        self,
        core: SchedulerCoreProtocol,
        initial_state: ScheduleState,
    ) -> tuple[bool, ScheduleState, list[PhaseTrace]]:
        current_state = initial_state
        trace: list[PhaseTrace] = []
        report_progress = getattr(core, "report_phase_progress", None)

        for phase in self.phases:
            started_at = datetime.now()
            if callable(report_progress):
                report_progress(
                    GenerationProgressEvent(
                        phase=phase.name,
                        stage="started",
                        timestamp=started_at,
                    )
                )
            success, current_state = phase.run(core, current_state)
            finished_at = datetime.now()

            metrics = current_state.to_metrics_dict()
            trace.append(
                PhaseTrace(
                    name=phase.name,
                    success=success,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=(finished_at - started_at).total_seconds(),
                    metrics=metrics,
                )
            )
            if callable(report_progress):
                report_progress(
                    GenerationProgressEvent(
                        phase=phase.name,
                        stage="completed",
                        timestamp=finished_at,
                        success=success,
                        coverage=float(metrics.get("coverage", 0.0)),
                        empty_slots=int(metrics.get("empty_slots", 0)),
                    )
                )

            if not success:
                return False, current_state, trace

        return True, current_state, trace
