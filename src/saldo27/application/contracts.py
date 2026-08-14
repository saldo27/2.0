from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from datetime import datetime

    from saldo27.domain.schedule_state import ScheduleState


class BuildEngine(Protocol):
    def build(self, state: ScheduleState) -> ScheduleState: ...


class OptimizeEngine(Protocol):
    def optimize(self, state: ScheduleState) -> ScheduleState: ...


class ValidateEngine(Protocol):
    def validate(self, state: ScheduleState) -> bool: ...


class FinalizeEngine(Protocol):
    def finalize(self, state: ScheduleState) -> ScheduleState: ...


class SchedulerCoreProtocol(Protocol):
    """Typed contract for the scheduler core consumed by the optimization pipeline.

    Each phase runner receives an object that satisfies this protocol so that
    both callers and the pipeline infrastructure can be type-checked without
    creating a circular import between ``scheduler_core`` and ``pipeline``.
    """

    def _initialize_schedule_phase(self) -> bool: ...

    def _assign_mandatory_phase(self) -> bool: ...

    def _run_distribution_and_optimization_phase(
        self,
        max_improvement_loops: int,
        max_complete_attempts: int,
    ) -> bool: ...

    def _finalization_phase(self) -> bool: ...

    def report_phase_progress(self, event: GenerationProgressEvent) -> None: ...


@dataclass(frozen=True)
class GenerationProgressEvent:
    phase: str
    stage: str
    timestamp: datetime
    success: bool | None = None
    coverage: float | None = None
    empty_slots: int | None = None
    message: str | None = None


class ProgressCallback(Protocol):
    def __call__(self, event: GenerationProgressEvent) -> None: ...
