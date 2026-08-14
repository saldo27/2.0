"""
EngineStateMixin — shared save/restore state logic for optimisation engines.

Both ``StrictBalanceOptimizer`` and ``FinalAdjustmentEngine`` contain identical
``_save_state`` / ``_restore_state`` implementations that capture a full
scheduler-state snapshot (schedule, assignments and all tracking counters) for
local rollback.  This mixin centralises those implementations in a single place.

Concrete classes must expose the following attributes:
  * ``self.schedule``            – aliased to ``scheduler.schedule``
  * ``self.worker_assignments``  – aliased to ``scheduler.worker_assignments``
  * ``self.scheduler``           – the ``Scheduler`` instance
"""

from __future__ import annotations

from typing import Any


class EngineStateMixin:
    """Mixin that provides ``_save_state`` / ``_restore_state`` for optimisation engines."""

    # Concrete classes supply these; declared here only for type-checker awareness.
    schedule: dict
    worker_assignments: dict
    scheduler: Any

    def _save_state(self) -> dict:
        """Capture a full state snapshot for local rollback."""
        return {
            "schedule": {k: v[:] for k, v in self.schedule.items()},
            "assignments": {k: set(v) for k, v in self.worker_assignments.items()},
            "shift_counts": dict(self.scheduler.worker_shift_counts),
            "weekdays": {k: dict(v) for k, v in self.scheduler.worker_weekdays.items()},
            "weekends": {k: list(v) for k, v in self.scheduler.worker_weekends.items()},
            "weekend_counts": dict(self.scheduler.worker_weekend_counts),
            "bridge_counts": {k: set(v) for k, v in self.scheduler.worker_bridge_counts.items()},
        }

    def _restore_state(self, state: dict) -> None:
        """Restore a previously saved state snapshot."""
        self.schedule.clear()
        self.schedule.update({k: v[:] for k, v in state["schedule"].items()})

        self.worker_assignments.clear()
        self.worker_assignments.update({k: set(v) for k, v in state["assignments"].items()})

        self.scheduler.worker_shift_counts.clear()
        self.scheduler.worker_shift_counts.update(state["shift_counts"])

        self.scheduler.worker_weekdays.clear()
        self.scheduler.worker_weekdays.update({k: dict(v) for k, v in state["weekdays"].items()})

        self.scheduler.worker_weekends.clear()
        self.scheduler.worker_weekends.update({k: list(v) for k, v in state["weekends"].items()})

        self.scheduler.worker_weekend_counts.clear()
        self.scheduler.worker_weekend_counts.update(state["weekend_counts"])

        self.scheduler.worker_bridge_counts.clear()
        self.scheduler.worker_bridge_counts.update({k: set(v) for k, v in state["bridge_counts"].items()})
