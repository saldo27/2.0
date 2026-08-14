from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from saldo27.domain.schedule_state import ScheduleState


class BuildEngine(Protocol):
    def build(self, state: ScheduleState) -> ScheduleState: ...


class OptimizeEngine(Protocol):
    def optimize(self, state: ScheduleState) -> ScheduleState: ...


class ValidateEngine(Protocol):
    def validate(self, state: ScheduleState) -> bool: ...


class FinalizeEngine(Protocol):
    def finalize(self, state: ScheduleState) -> ScheduleState: ...


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
