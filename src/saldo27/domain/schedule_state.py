from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(frozen=True)
class ScheduleState:
    """Immutable snapshot exchanged between pipeline phases."""

    schedule: tuple[tuple[datetime, tuple[str | None, ...]], ...]
    worker_assignments: tuple[tuple[str, tuple[datetime, ...]], ...]
    worker_shift_counts: tuple[tuple[str, int], ...]
    worker_weekend_counts: tuple[tuple[str, int], ...]
    worker_posts: tuple[tuple[str, tuple[int, ...]], ...]
    locked_mandatory: tuple[Any, ...] = ()

    @classmethod
    def from_scheduler(cls, scheduler: Any, include_locked: bool = True) -> ScheduleState:
        locked_mandatory: tuple[Any, ...] = ()
        if include_locked and hasattr(scheduler, "get_locked_mandatory"):
            raw_locked = scheduler.get_locked_mandatory()
            locked_mandatory = tuple(sorted(raw_locked, key=repr))
        elif include_locked and hasattr(scheduler, "schedule_builder") and scheduler.schedule_builder:
            raw_locked = getattr(scheduler.schedule_builder, "_locked_mandatory", set())
            locked_mandatory = tuple(sorted(raw_locked, key=repr))

        return cls(
            schedule=tuple(
                (date, tuple(shifts)) for date, shifts in sorted(scheduler.schedule.items(), key=lambda item: item[0])
            ),
            worker_assignments=tuple(
                (worker_id, tuple(sorted(dates)))
                for worker_id, dates in sorted(scheduler.worker_assignments.items(), key=lambda item: item[0])
            ),
            worker_shift_counts=tuple(sorted(scheduler.worker_shift_counts.items(), key=lambda item: item[0])),
            worker_weekend_counts=tuple(sorted(scheduler.worker_weekend_counts.items(), key=lambda item: item[0])),
            worker_posts=tuple(
                (worker_id, tuple(sorted(posts)))
                for worker_id, posts in sorted(scheduler.worker_posts.items(), key=lambda item: item[0])
            ),
            locked_mandatory=locked_mandatory,
        )

    def apply_to_scheduler(self, scheduler: Any) -> None:
        scheduler.schedule = {date: list(shifts) for date, shifts in self.schedule}
        scheduler.worker_assignments = {worker_id: set(dates) for worker_id, dates in self.worker_assignments}
        scheduler.worker_shift_counts = dict(self.worker_shift_counts)
        scheduler.worker_weekend_counts = dict(self.worker_weekend_counts)
        scheduler.worker_posts = {worker_id: set(posts) for worker_id, posts in self.worker_posts}

        if hasattr(scheduler, "set_locked_mandatory"):
            scheduler.set_locked_mandatory(set(self.locked_mandatory))
        elif hasattr(scheduler, "schedule_builder") and scheduler.schedule_builder is not None:
            scheduler.schedule_builder.set_locked_mandatory(set(self.locked_mandatory))

    def to_metrics_dict(self) -> dict[str, float | int]:
        total_slots = 0
        filled_slots = 0
        for _, shifts in self.schedule:
            total_slots += len(shifts)
            filled_slots += sum(1 for worker in shifts if worker is not None)
        empty_slots = total_slots - filled_slots
        coverage = (filled_slots / total_slots) * 100 if total_slots else 0.0
        return {
            "total_slots": total_slots,
            "filled_slots": filled_slots,
            "empty_slots": empty_slots,
            "coverage": coverage,
        }
