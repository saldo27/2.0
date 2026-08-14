from __future__ import annotations

import logging
from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from saldo27.scheduler import Scheduler


class SchedulerValidationService:
    """Owns final validation/reconciliation flow for Scheduler."""

    def __init__(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    def validate_and_fix_final_schedule(self) -> int:
        scheduler = self.scheduler
        logging.info("Running final schedule validation...")
        violations = scheduler._check_schedule_constraints()
        violation_counts = Counter(violation["type"] for violation in violations)
        fixes_made = scheduler._fix_constraint_violations(violations)
        other_issues = 1 if scheduler._reconcile_schedule_tracking() else 0
        remaining_violations = scheduler._check_schedule_constraints()
        logging.info(
            "Final validation complete: Found %s incompatibility issues, %s gap/pattern issues, "
            "%s other issues. Made %s fixes. Remaining violations: %s.",
            violation_counts.get("incompatibility", 0),
            sum(violation_counts.get(kind, 0) for kind in ("min_rest_days", "friday_monday_pattern", "weekly_pattern")),
            other_issues,
            fixes_made,
            len(remaining_violations),
        )
        return fixes_made

    def run_final_validation_and_fix(self) -> bool:
        scheduler = self.scheduler
        try:
            scheduler._reconcile_schedule_tracking()
            fixes_made = self.validate_and_fix_final_schedule()
            if fixes_made > 0:
                logging.info(f"Validation fixed {fixes_made} issues")
            return True
        except Exception as exc:
            logging.error(f"Validation error: {exc!s}", exc_info=True)
            return False
