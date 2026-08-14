from __future__ import annotations

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
