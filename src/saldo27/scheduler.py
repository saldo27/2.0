from __future__ import annotations

# Imports
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

from saldo27.exceptions import SchedulerError
from saldo27.scheduler_config import SchedulerConfig, setup_logging
from saldo27.scheduler_initializer import SchedulerInitializer
from saldo27.scheduler_reporting import SchedulerReportingService
from saldo27.scheduler_tracking import SchedulerTrackingState
from saldo27.scheduler_validation import SchedulerValidationService
from saldo27.utilities import DateTimeUtils

if TYPE_CHECKING:
    from collections.abc import Callable

    from saldo27.application.contracts import GenerationProgressEvent

# Initialize logging using the configuration module
setup_logging()

# NOTE: SchedulerError is imported from exceptions.py — do NOT redefine it here
# to avoid class identity mismatches with other modules that import from exceptions.py.


class Scheduler:
    """Main Scheduler class that coordinates all scheduling operations"""

    def __init__(self, config: dict[str, Any]):
        """Initialize the scheduler with configuration"""
        logging.info("Scheduler initialized")

        # Cancellation flag — set to True from the UI to abort generation
        self._cancelled: bool = False
        self._progress_callback: Callable[[GenerationProgressEvent], None] | None = None
        self._phase_trace: list[Any] = []

        # Initialize cache for performance optimization
        self._cache: dict[str, Any] = {}
        self._cache_enabled = config.get("cache_enabled", SchedulerConfig.CACHE_ENABLED)

        try:
            # Initialize date_utils FIRST, before calling any method that might need it
            self.date_utils = DateTimeUtils()
            self._tracking_state = SchedulerTrackingState(self)
            self._initializer = SchedulerInitializer(self)
            self._validation_service = SchedulerValidationService(self)
            self._reporting_service = SchedulerReportingService(self)
            self._initializer.initialize(config)

        except SchedulerError:
            raise
        except Exception as e:
            logging.error(f"Initialization error: {e!s}", exc_info=True)
            raise SchedulerError(f"Failed to initialize scheduler: {e!s}")

    # ------------------------------------------------------------------
    # Init sub-phases (called only from __init__)
    # ------------------------------------------------------------------

    def request_cancellation(self) -> None:
        self._cancelled = True

    def clear_cancellation(self) -> None:
        self._cancelled = False

    def is_cancellation_requested(self) -> bool:
        return self._cancelled

    def set_progress_callback(self, callback: Callable[[GenerationProgressEvent], None] | None) -> None:
        self._progress_callback = callback

    def emit_progress_event(self, event: GenerationProgressEvent) -> None:
        if self._progress_callback is not None:
            self._progress_callback(event)

    @property
    def phase_trace(self) -> list[Any]:
        return list(self._phase_trace)

    def set_phase_trace(self, trace: list[Any]) -> None:
        self._phase_trace = list(trace)


    def get_locked_mandatory(self) -> set[Any]:
        if hasattr(self, "schedule_builder") and self.schedule_builder is not None:
            return self.schedule_builder.get_locked_mandatory()
        return set()

    def set_locked_mandatory(self, locked: set[Any] | tuple[Any, ...]) -> None:
        if hasattr(self, "schedule_builder") and self.schedule_builder is not None:
            self.schedule_builder.set_locked_mandatory(locked)

    @staticmethod
    def _normalize_worker_ids(workers_data: list[dict[str, Any]]) -> None:
        """Normalize worker IDs to ``str`` at the single data-ingestion point.

        Worker IDs may arrive as ``int`` or ``str`` depending on the source
        (UI forms, JSON imports, etc.). Normalizing them here — once, at
        ingestion — avoids the need for defensive ``worker == worker_id or
        str(worker) == str(worker_id)`` comparisons scattered across the
        codebase.
        """
        for worker in workers_data:
            if "id" in worker and worker["id"] is not None:
                worker["id"] = str(worker["id"])
            incompatible_with = worker.get("incompatible_with")
            if incompatible_with:
                worker["incompatible_with"] = [str(w_id) for w_id in incompatible_with]


    def load_prior_schedule_data(self, json_source) -> dict[str, Any]:
        """
        Load a previously-exported schedule JSON and seed prior-period stats so
        that cross-period constraints (gap, consecutive weekends, proportional
        weekend count) are respected when generating the new schedule.

        Parameters
        ----------
        json_source : file-like / str path / dict
            The prior-period schedule JSON (exported via export_schedule_json).

        Returns
        -------
        dict with keys "error" (str or None) and "summary" (per-worker stats).
        """
        from saldo27.prior_schedule_handler import (
            apply_prior_period_balance,
            load_prior_schedule,
            summarize_prior_schedule,
            validate_target_capacity,
        )

        holidays_set = set(self.holidays)
        prior_data = load_prior_schedule(
            json_source,
            new_period_start=self.start_date,
            new_period_holidays=holidays_set,
        )

        if prior_data["error"]:
            return {"error": prior_data["error"], "summary": {}}

        self.prior_assignments = prior_data["prior_assignments"]
        self.prior_shift_counts = prior_data["prior_shift_counts"]
        self.prior_weekend_counts = prior_data["prior_weekends"]
        self.prior_target_shifts = prior_data.get("prior_target_shifts", {})
        self.prior_last_date = prior_data["prior_last_date"]

        # Adjust new-period targets to compensate for prior-period over/under-delivery
        apply_prior_period_balance(
            self.workers_data,
            self.prior_shift_counts,
            self.prior_target_shifts,
            self._base_target_shifts,
        )

        # Validate that adjusted targets don't exceed available capacity
        validate_target_capacity(self.workers_data, self.schedule, self._base_target_shifts)

        logging.info(
            f"Prior schedule loaded: {len(self.prior_assignments)} workers, "
            f"period {prior_data['prior_period_start']} → {prior_data['prior_period_end']}"
        )

        return {"error": None, "summary": summarize_prior_schedule(prior_data)}

    def clear_prior_schedule_data(self) -> None:
        """Remove any loaded prior-schedule data (resets to zero-history mode)."""
        self.prior_assignments = {}
        self.prior_shift_counts = {}
        self.prior_weekend_counts = {}
        self.prior_target_shifts = {}
        self.prior_last_date = {}
        # Restore worker targets to the original pre-adjustment values
        for worker in self.workers_data:
            base = self._base_target_shifts.get(worker["id"])
            if base is not None:
                worker["target_shifts"] = base
        logging.info("Prior schedule data cleared; target_shifts restored to base values.")

    def _get_effective_assignments(self, worker_id: str) -> set:
        """Return merged prior + current period dates for cross-period constraint checks."""
        from saldo27.prior_schedule_handler import get_effective_assignments

        return get_effective_assignments(worker_id, self.worker_assignments, self.prior_assignments, self.start_date)


    def _get_prior_weekend_count(self, worker_id: str) -> int:
        """Return just the prior-period weekend count."""
        from saldo27.prior_schedule_handler import get_prior_weekend_count

        return get_prior_weekend_count(worker_id, self.prior_weekend_counts)

    def _get_cache_key(self, method_name: str, *args) -> str:
        """Generate a cache key for method results"""
        return f"{method_name}:{hash(str(args))}"

    def _get_cached_result(self, cache_key: str) -> Any | None:
        """Get cached result if caching is enabled"""
        if self._cache_enabled:
            return self._cache.get(cache_key)
        return None

    def _set_cached_result(self, cache_key: str, result: Any) -> None:
        """Set cached result if caching is enabled"""
        if self._cache_enabled:
            self._cache[cache_key] = result

    def _clear_cache(self) -> None:
        """Clear the cache"""
        self._cache.clear()

    def _initialize_schedule_with_variable_shifts(self):
        self._tracking_state.initialize_schedule_with_variable_shifts()


    def _ensure_data_integrity(self):
        return self._tracking_state.ensure_data_integrity()

    def _synchronize_tracking_data(self) -> bool:
        return self._tracking_state.synchronize()

    def _validate_data_synchronization(self) -> tuple[bool, dict[str, Any]]:
        return self._tracking_state.validate_synchronization()

    def _repair_data_synchronization(self, validation_report: dict[str, Any] | None = None) -> bool:
        return self._tracking_state.repair_synchronization(validation_report)

    def _ensure_data_synchronization(self) -> bool:
        return self._tracking_state.ensure_synchronization()

    def _reconcile_schedule_tracking(self):
        return self._tracking_state.reconcile()

    def _get_worker_assigned_to_post(self, date: datetime, post: int) -> str | None:
        """Return the worker currently assigned to a post for a given date."""
        assignments = self.schedule.get(date)

        if isinstance(assignments, list):
            if 0 <= post < len(assignments):
                return assignments[post]
            return None

        if isinstance(assignments, dict):
            for key in (post, str(post), post + 1, str(post + 1)):
                if key in assignments:
                    return assignments[key]

        return None

    def _update_tracking_data(self, worker_id, date, post, removing=False):
        self._tracking_state.update_assignment(worker_id, date, post, removing=removing)

    def _validate_assignment_consistency(self, worker_id: str, date: datetime, removing: bool = False) -> bool:
        return self._tracking_state.validate_assignment_consistency(worker_id, date, removing=removing)

    # ========================================
    # 3. TARGET AND CALCULATION METHODS
    # ========================================
    def _calculate_target_shifts(self) -> bool:
        """Delegate target calculation to TargetCalculator."""
        from saldo27.target_calculator import TargetCalculator

        return TargetCalculator(self).calculate()


    def _get_shifts_for_date(self, date):
        """Determine the number of shifts for a specific date based on variable_shifts."""
        # Normalize to date-only if datetime
        check_date = date.date() if hasattr(date, "date") else date
        for cfg in self.variable_shifts:
            start = cfg.get("start_date")
            end = cfg.get("end_date")
            shifts = cfg.get("shifts")
            # Normalize
            sd = start.date() if hasattr(start, "date") else start
            ed = end.date() if hasattr(end, "date") else end
            if sd <= check_date <= ed:
                return shifts
        # Fallback to default
        return self.num_shifts

    def count_bridges_for_worker(self, worker_id: str) -> int:
        """
        Count the number of bridge SHIFTS assigned to a worker.
        (Counts individual shifts in bridge days, not bridge periods)

        Args:
            worker_id: ID of the worker

        Returns:
            Number of shifts in bridge days assigned to the worker
        """
        bridge_shift_count = 0

        # Get all bridge dates
        bridge_dates = set()
        for bridge_period in self.bridge_periods:
            for date in self._get_dates_in_bridge(bridge_period):
                bridge_dates.add(date)

        # Count how many shifts this worker has on bridge dates
        for date, shifts in self.schedule.items():
            if date in bridge_dates:
                # Count how many of the shifts on this date are assigned to this worker
                bridge_shift_count += shifts.count(worker_id)

        return bridge_shift_count

    def get_bridge_objective_for_worker(self, worker_id: str) -> float:
        """
        Calculate the objective (target) number of bridge shifts for a worker
        proportional to their share of total target shifts.

        Manual workers have a fixed number of shifts/month (not locked slots), so
        using work_percentage as FTE is incorrect for them.  Using target_shifts
        as the weight gives the right proportion for both manual and auto workers.

        Formula: total_bridge_shifts * (worker_target_shifts / sum_all_target_shifts)

        Args:
            worker_id: ID of the worker

        Returns:
            Target number of bridge shifts (float)
        """
        # Find worker data
        worker = next((w for w in self.workers_data if w["id"] == worker_id), None)
        if not worker:
            return 0.0

        worker_target = worker.get("target_shifts", 0)

        # Total target shifts across all workers
        total_target = sum(w.get("target_shifts", 0) for w in self.workers_data)

        if total_target == 0:
            return 0.0

        # Calculate total bridge shifts (number of filled shifts on bridge days)
        total_bridge_shifts = 0
        bridge_dates = set()
        for bridge_period in self.bridge_periods:
            for date in self._get_dates_in_bridge(bridge_period):
                if date in self.schedule:
                    bridge_dates.add(date)

        for date in bridge_dates:
            if date in self.schedule:
                # Count only filled (non-None) slots to avoid inflating targets
                total_bridge_shifts += sum(1 for w in self.schedule[date] if w is not None)

        # Objective = proportional to worker's share of total target shifts
        return total_bridge_shifts * worker_target / total_target

    def _get_dates_in_bridge(self, bridge_period: dict) -> list[datetime]:
        """
        Get all dates that are part of a bridge period.

        Args:
            bridge_period: Bridge period dictionary with 'start_date' and 'end_date'

        Returns:
            List of datetime objects for all dates in the bridge period
        """
        dates = []
        current = bridge_period["start_date"]
        end = bridge_period["end_date"]

        while current <= end:
            dates.append(current)
            current += timedelta(days=1)

        return dates

    # ========================================
    # 4. ASSIGNMENT AND CONSTRAINT CHECKING
    # ========================================


    def _check_schedule_constraints(self):
        """Check the current schedule for constraint violations.

        Delegates to :meth:`ConstraintChecker.check_schedule_violations` which is
        the single source of truth for gap/pattern/incompatibility logic.

        Returns a list of violation dicts.
        """
        try:
            violations = self.constraint_checker.check_schedule_violations(self.worker_assignments)

            # Log summary of violations
            if violations:
                logging.warning(f"Found {len(violations)} constraint violations in schedule")
                for i, v in enumerate(violations[:5]):  # Log first 5 violations
                    if v["type"] == "min_rest_days":
                        logging.warning(
                            f"Violation {i + 1}: Worker {v['worker_id']} has only {v['days_between']} days between shifts on {v['date1']} and {v['date2']} (min required: {v['min_required']})"
                        )
                    elif v["type"] == "friday_monday_pattern":
                        logging.warning(
                            f"Violation {i + 1}: Worker {v['worker_id']} has Friday-Monday assignment on {v['date1']} and {v['date2']}"
                        )
                    elif v["type"] == "weekly_pattern":
                        logging.warning(
                            f"Violation {i + 1}: Worker {v['worker_id']} has shifts exactly {v['days_between']} days apart on {v['date1']} and {v['date2']}"
                        )
                    elif v["type"] == "gap2_weekend":
                        logging.warning(
                            f"Violation {i + 1}: Worker {v['worker_id']} has a gap-2 weekend pair on {v['date1']} and {v['date2']}"
                        )
                    elif v["type"] == "incompatibility":
                        logging.warning(
                            f"Violation {i + 1}: Incompatible workers {v['worker_id']} and {v['incompatible_id']} are both assigned on {v['date']}"
                        )
                    elif v["type"] == "consecutive_last_post":
                        logging.warning(
                            f"Violation {i + 1}: Worker {v['worker_id']} has {v['run_length']} consecutive last-post shifts "
                            f"between {v['date1']} and {v['date2']}"
                        )

                if len(violations) > 5:
                    logging.warning(f"...and {len(violations) - 5} more violations")

            return violations
        except Exception as e:
            logging.error(f"Error checking schedule constraints: {e!s}", exc_info=True)
            return []

    def _fix_constraint_violations(self, violations: list[dict[str, Any]] | None = None) -> int:
        """
        Try to fix constraint violations in the current schedule.
        Returns the number of fixes made.
        """
        try:
            violations = self._check_schedule_constraints() if violations is None else violations
            if not violations:
                return 0

            logging.info(f"Attempting to fix {len(violations)} constraint violations")
            fixes_made = 0
            schedule_builder = self.schedule_builder if hasattr(self, "schedule_builder") else None
            from saldo27.constraint_checker import SPACING_VIOLATION_TYPES

            # Fix each violation
            for violation in violations:
                if violation["type"] in SPACING_VIOLATION_TYPES:
                    # Fix by unassigning one of the shifts
                    worker_id = violation["worker_id"]
                    date1 = violation["date1"]
                    date2 = violation["date2"]

                    # HARD INVARIANT: a weekly_pattern (7/14-day same-weekday) violation may
                    # ONLY be left in place when BOTH colliding dates are mandatory_days for
                    # that worker (an unavoidable configuration conflict). There is no other
                    # sanctioned exception — any other 7/14 violation (e.g. one created as a
                    # last-resort relaxation while filling a deficit/empty slot) must always
                    # be undone here, never silently tolerated.

                    # CRITICAL: Check if either date is mandatory
                    date1_is_mandatory = schedule_builder.is_mandatory(worker_id, date1) if schedule_builder else False
                    date2_is_mandatory = schedule_builder.is_mandatory(worker_id, date2) if schedule_builder else False

                    # If both are mandatory, we cannot fix this - it's a configuration error
                    if date1_is_mandatory and date2_is_mandatory:
                        logging.error(
                            f"Cannot fix violation: Worker {worker_id} has mandatory assignments on both {date1} and {date2} which violate constraints. This is a configuration error."
                        )
                        continue

                    # Decide which date to unassign - prioritize keeping mandatory assignments
                    if date2_is_mandatory:
                        date_to_unassign = date1
                    elif date1_is_mandatory:
                        date_to_unassign = date2
                    else:
                        # Neither is mandatory - prefer to unassign the later date
                        date_to_unassign = date2

                    # Find the shift number for this worker on this date
                    shift_num = None
                    if date_to_unassign in self.schedule:
                        for i, worker in enumerate(self.schedule[date_to_unassign]):
                            if worker == worker_id:
                                shift_num = i
                                break

                    if shift_num is not None:
                        # CRITICAL: Verify we can modify this assignment (never remove mandatory)
                        if schedule_builder and not schedule_builder._can_modify_assignment(
                            worker_id,
                            date_to_unassign,
                            "fix_constraint_rest",
                            enforce_monthly_target_floor=False,
                        ):
                            logging.warning(
                                f"🔒 BLOCKED: Cannot unassign MANDATORY {worker_id} from {date_to_unassign.strftime('%Y-%m-%d')}"
                            )
                            continue

                        # Unassign this worker
                        self.schedule[date_to_unassign][shift_num] = None
                        self.worker_assignments[worker_id].remove(date_to_unassign)
                        self._update_tracking_data(worker_id, date_to_unassign, shift_num, removing=True)
                        violation_type = {
                            "min_rest_days": "rest period",
                            "friday_monday_pattern": "Friday-Monday pattern",
                            "weekly_pattern": "weekly pattern",
                        }[violation["type"]]
                        logging.info(
                            f"Fixed {violation_type} violation: Unassigned worker {worker_id} from {date_to_unassign}"
                        )
                        fixes_made += 1

                elif violation["type"] == "incompatibility":
                    # Fix incompatibility by unassigning one of the workers
                    worker_id = violation["worker_id"]
                    incompatible_id = violation["incompatible_id"]
                    date = violation["date"]

                    # CRITICAL: Check if either worker has a mandatory assignment for this date
                    worker_is_mandatory = schedule_builder.is_mandatory(worker_id, date) if schedule_builder else False
                    incompatible_is_mandatory = (
                        schedule_builder.is_mandatory(incompatible_id, date) if schedule_builder else False
                    )

                    # If both are mandatory, we cannot fix this - it's a configuration error
                    if worker_is_mandatory and incompatible_is_mandatory:
                        logging.error(
                            f"Cannot fix incompatibility: Both workers {worker_id} and {incompatible_id} have mandatory assignments on {date} but are incompatible. This is a configuration error."
                        )
                        continue

                    # Decide which worker to unassign - prioritize keeping mandatory assignments
                    if worker_is_mandatory:
                        worker_to_unassign = incompatible_id
                    elif incompatible_is_mandatory:
                        worker_to_unassign = worker_id
                    else:
                        # Neither is mandatory - prefer the one with more assignments
                        w1_assignments = len(self.worker_assignments.get(worker_id, set()))
                        w2_assignments = len(self.worker_assignments.get(incompatible_id, set()))
                        worker_to_unassign = worker_id if w1_assignments >= w2_assignments else incompatible_id

                    # Find the shift number for this worker on this date
                    shift_num = None
                    if date in self.schedule:
                        for i, worker in enumerate(self.schedule[date]):
                            if worker == worker_to_unassign:
                                shift_num = i
                                break

                    if shift_num is not None:
                        # CRITICAL: Verify we can modify this assignment (never remove mandatory)
                        if schedule_builder and not schedule_builder._can_modify_assignment(
                            worker_to_unassign,
                            date,
                            "fix_constraint_incompat",
                            enforce_monthly_target_floor=False,
                        ):
                            logging.warning(
                                f"🔒 BLOCKED: Cannot unassign MANDATORY {worker_to_unassign} from {date.strftime('%Y-%m-%d')}"
                            )
                            continue

                        # Unassign this worker
                        self.schedule[date][shift_num] = None
                        self.worker_assignments[worker_to_unassign].remove(date)
                        self._update_tracking_data(worker_to_unassign, date, shift_num, removing=True)
                        logging.info(
                            f"Fixed incompatibility violation: Unassigned worker {worker_to_unassign} from {date}"
                        )
                        fixes_made += 1

                elif violation["type"] == "consecutive_last_post":
                    # Fix by unassigning the worker from one of the shifts in the
                    # over-length last-post run (prefer the most recent date in
                    # the run so earlier, already-settled shifts are undisturbed).
                    worker_id = violation["worker_id"]
                    date1 = violation["date1"]
                    date2 = violation["date2"]

                    candidates_to_unassign = [d for d in (date2, date1) if d is not None]
                    fixed = False
                    for date_to_unassign in candidates_to_unassign:
                        if schedule_builder and not schedule_builder._can_modify_assignment(
                            worker_id,
                            date_to_unassign,
                            "fix_consecutive_last_post",
                            enforce_monthly_target_floor=False,
                        ):
                            continue

                        shift_num = None
                        if date_to_unassign in self.schedule:
                            for i, worker in enumerate(self.schedule[date_to_unassign]):
                                if worker == worker_id:
                                    shift_num = i
                                    break

                        if shift_num is not None:
                            self.schedule[date_to_unassign][shift_num] = None
                            self.worker_assignments[worker_id].discard(date_to_unassign)
                            self._update_tracking_data(worker_id, date_to_unassign, shift_num, removing=True)
                            logging.info(
                                f"Fixed consecutive last-post violation: Unassigned worker {worker_id} from {date_to_unassign}"
                            )
                            fixes_made += 1
                            fixed = True
                            break

                    if not fixed:
                        logging.warning(
                            f"🔒 BLOCKED: Cannot fix consecutive last-post violation for MANDATORY worker {worker_id} "
                            f"between {date1} and {date2}"
                        )

            # Check if we fixed all violations
            remaining_violations = self._check_schedule_constraints()
            if remaining_violations:
                logging.warning(f"After fixing attempts, {len(remaining_violations)} violations still remain")
                return fixes_made
            else:
                logging.info(f"Successfully fixed all {fixes_made} constraint violations")
                return fixes_made

        except Exception as e:
            logging.error(f"Error fixing constraint violations: {e!s}", exc_info=True)
            return 0

    # ========================================
    # 5. SCHEDULE GENERATION AND OPTIMIZATION
    # ========================================
    def generate_schedule(self, max_improvement_loops: int = 70) -> bool:
        """
        Generate a schedule using the orchestrated workflow.

        Args:
            max_improvement_loops: Maximum number of improvement iterations

        Returns:
            bool: True if schedule generation was successful
        """
        from saldo27.scheduler_core import SchedulerCore

        # Create scheduler core for orchestration
        scheduler_core = SchedulerCore(self)
        self._scheduler_core = scheduler_core

        # Read max_complete_attempts from config (default 1 for backwards compatibility)
        max_complete_attempts = self.config.get("max_complete_attempts", 1)

        # Use orchestrated workflow
        return scheduler_core.orchestrate_schedule_generation(max_improvement_loops, max_complete_attempts)

    def _get_date_range(self, start_date, end_date):
        """Delegate to DateTimeUtils.get_date_range (canonical implementation)."""
        return self.date_utils.get_date_range(start_date, end_date)

    def calculate_score(self, schedule_to_score=None, assignments_to_score=None):
        return self._reporting_service.calculate_score(schedule_to_score, assignments_to_score)


    def validate_and_fix_final_schedule(self):
        return self._validation_service.validate_and_fix_final_schedule()

    def _run_final_validation_and_fix(self):
        return self._validation_service.run_final_validation_and_fix()

    # ========================================
    # 9. REPORTING AND EXPORT
    # ========================================
    def export_schedule(self, format="txt"):
        return self._reporting_service.export_schedule(output_format=format)

    def export_schedule_json(self, filename=None):
        return self._reporting_service.export_schedule_json(filename=filename)

    def generate_worker_report(self, worker_id, save_to_file=False):
        return self._reporting_service.generate_worker_report(worker_id, save_to_file=save_to_file)

    def generate_all_worker_reports(self, output_directory=None):
        return self._reporting_service.generate_all_worker_reports(output_directory=output_directory)

    def log_schedule_summary(self, title="Schedule Summary"):
        self._reporting_service.log_schedule_summary(title=title)

    # ========================================
    # 10. UTILITY METHODS
    # ========================================


    def is_real_time_enabled(self) -> bool:
        """Return True if the real-time engine is active."""
        return self.real_time_engine is not None


    def assign_worker_real_time(
        self, worker_id: str, shift_date: datetime, post_index: int, user_id: str | None = None, validate: bool = True
    ) -> dict[str, Any]:
        """Assign worker to shift with real-time validation; returns plain dict."""
        if not self.is_real_time_enabled():
            return self._RT_DISABLED
        assert self.real_time_engine is not None
        return self.real_time_engine.assign_worker_dict(worker_id, shift_date, post_index, user_id, validate)

    def unassign_worker_real_time(
        self, shift_date: datetime, post_index: int, user_id: str | None = None
    ) -> dict[str, Any]:
        """Unassign worker from shift with real-time feedback; returns plain dict."""
        if not self.is_real_time_enabled():
            return self._RT_DISABLED
        assert self.real_time_engine is not None
        return self.real_time_engine.unassign_worker_dict(shift_date, post_index, user_id)

    def swap_workers_real_time(
        self,
        shift_date1: datetime,
        post_index1: int,
        shift_date2: datetime,
        post_index2: int,
        user_id: str | None = None,
        validate: bool = True,
    ) -> dict[str, Any]:
        """Swap workers between two shifts with real-time validation; returns plain dict."""
        if not self.is_real_time_enabled():
            return self._RT_DISABLED
        assert self.real_time_engine is not None
        return self.real_time_engine.swap_workers_dict(
            shift_date1, post_index1, shift_date2, post_index2, user_id, validate
        )

    def validate_schedule_real_time(self, quick_check: bool = False) -> dict[str, Any]:
        """Perform real-time schedule validation; returns plain dict."""
        if not self.is_real_time_enabled():
            return self._RT_DISABLED
        assert self.real_time_engine is not None
        return self.real_time_engine.validate_schedule_dict(quick_check)

    def undo_last_change(self, user_id: str | None = None) -> dict[str, Any]:
        """Undo the last schedule change; returns plain dict."""
        if not self.is_real_time_enabled():
            return self._RT_DISABLED
        assert self.real_time_engine is not None
        return self.real_time_engine.undo_dict(user_id)

    def redo_last_change(self, user_id: str | None = None) -> dict[str, Any]:
        """Redo the last undone change; returns plain dict."""
        if not self.is_real_time_enabled():
            return self._RT_DISABLED
        assert self.real_time_engine is not None
        return self.real_time_engine.redo_dict(user_id)

    def get_real_time_analytics(self) -> dict[str, Any]:
        """Return real-time analytics dict from the engine."""
        if not self.is_real_time_enabled():
            return {"error": "Real-time features not enabled"}
        assert self.real_time_engine is not None
        return self.real_time_engine.get_real_time_analytics()

    def get_change_history(self, limit: int = 20, user_id: str | None = None) -> dict[str, Any]:
        """Return recent schedule changes as a plain dict."""
        if not self.is_real_time_enabled():
            return {"error": "Real-time features not enabled"}
        assert self.real_time_engine is not None
        return self.real_time_engine.change_history_dict(limit, user_id)

    # Predictive Analytics Integration Methods
    _PA_DISABLED: ClassVar[dict[str, Any]] = {
        "success": False,
        "message": "Predictive analytics not enabled",
        "error": "PREDICTIVE_ANALYTICS_DISABLED",
    }

    def is_predictive_analytics_enabled(self) -> bool:
        """Return True if the predictive analytics engine is active."""
        return self.predictive_analytics is not None

    def generate_demand_forecasts(self, forecast_days: int = 30) -> dict[str, Any]:
        """Generate demand forecasts; returns plain dict."""
        if not self.is_predictive_analytics_enabled():
            return self._PA_DISABLED
        assert self.predictive_analytics is not None
        return self.predictive_analytics.generate_demand_forecasts_dict(forecast_days)

    def get_predictive_insights(self) -> dict[str, Any]:
        """Return comprehensive predictive insights dict."""
        if not self.is_predictive_analytics_enabled():
            return self._PA_DISABLED
        assert self.predictive_analytics is not None
        return self.predictive_analytics.get_predictive_insights_dict()

    def run_predictive_optimization(self) -> dict[str, Any]:
        """Run predictive optimization analysis; returns plain dict."""
        if not self.is_predictive_analytics_enabled() or not self.predictive_optimizer:
            return {
                "success": False,
                "message": "Predictive optimization not available",
                "error": "PREDICTIVE_OPTIMIZER_DISABLED",
            }
        assert self.predictive_optimizer is not None
        return self.predictive_optimizer.run_predictive_optimization_dict()


    def get_optimization_suggestions(self) -> list[str]:
        """Return optimization suggestions from predictive analytics."""
        if not self.is_predictive_analytics_enabled():
            return ["Predictive analytics not enabled - enable for optimization suggestions"]
        assert self.predictive_analytics is not None
        return self.predictive_analytics.get_optimization_suggestions_list()

    def get_analytics_summary(self) -> dict[str, Any]:
        """Return summary of predictive analytics status and capabilities."""
        if not self.is_predictive_analytics_enabled():
            return {"enabled": False, "message": "Predictive analytics not enabled"}
        assert self.predictive_analytics is not None
        try:
            return self.predictive_analytics.get_analytics_summary()
        except Exception as e:
            logging.error(f"Error getting analytics summary: {e}")
            return {"enabled": True, "error": str(e), "message": "Error getting analytics summary"}

