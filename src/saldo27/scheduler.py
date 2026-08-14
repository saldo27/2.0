from __future__ import annotations

# Imports
import logging
import math
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

from saldo27.exceptions import SchedulerError
from saldo27.scheduler_initializer import SchedulerInitializer
from saldo27.scheduler_config import SchedulerConfig, setup_logging
from saldo27.scheduler_reporting import SchedulerReportingService
from saldo27.scheduler_tracking import SchedulerTrackingState
from saldo27.scheduler_validation import SchedulerValidationService
from saldo27.utilities import DateTimeUtils, get_effective_min_gap

if TYPE_CHECKING:
    from collections.abc import Callable

    from saldo27.application.contracts import GenerationProgressEvent
    from saldo27.domain.schedule_state import ScheduleState

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

    def snapshot_state(self, *, include_locked: bool = True) -> ScheduleState:
        from saldo27.domain.schedule_state import ScheduleState

        return ScheduleState.from_scheduler(self, include_locked=include_locked)

    def restore_state(self, state: ScheduleState) -> None:
        state.apply_to_scheduler(self)

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

    def _init_config(self, config: dict[str, Any]) -> None:
        self._initializer.apply_config(config)

    def _init_incompatibilities(self) -> None:
        self._initializer.initialize_incompatibilities()

    def _init_tracking_state(self) -> None:
        self._tracking_state.initialize()

    def _init_modules(self, config: dict[str, Any]) -> None:
        self._initializer.initialize_modules(config)

    def _init_targets_and_prior(self) -> None:
        self._initializer.initialize_targets_and_prior()

    # =========================================================================
    # PRIOR SCHEDULE INTEGRATION
    # =========================================================================

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

    def _get_effective_weekend_count(self, worker_id: str) -> int:
        """Return prior-period weekend count + current-period weekend count."""
        from saldo27.prior_schedule_handler import get_effective_weekend_count

        return get_effective_weekend_count(worker_id, self.prior_weekend_counts, self.worker_weekend_counts)

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

    def _validate_config(self, config: dict[str, Any]) -> None:
        self._initializer.validate_config(config)

    def _log_initialization(self):
        self._initializer.log_initialization()

    def _prepare_worker_data(self):
        """
        Prepare worker data before schedule generation:
        - Set empty work periods to the full schedule period
        - Handle other default values
        """
        logging.info("Preparing worker data...")

        for worker in self.workers_data:
            # Handle empty work periods - default to full schedule period
            if "work_periods" not in worker or not worker["work_periods"].strip():
                start_str = self.start_date.strftime("%d-%m-%Y")
                end_str = self.end_date.strftime("%d-%m-%Y")
                worker["work_periods"] = f"{start_str} - {end_str}"
                logging.info(f"Worker {worker['id']}: Empty work period set to full schedule period")

    # ========================================
    # 2. DATA STRUCTURE MANAGEMENT
    # ========================================
    def _initialize_schedule_with_variable_shifts(self):
        self._tracking_state.initialize_schedule_with_variable_shifts()

    def _reset_schedule(self):
        self._tracking_state.reset()

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

    def _calculate_manual_targets(self, manual_workers: list) -> int:
        """Delegate manual-target calculation to TargetCalculator (used by legacy callers)."""
        from saldo27.target_calculator import TargetCalculator

        return TargetCalculator(self)._calculate_manual_targets(manual_workers)

    def _calculate_monthly_targets(self) -> bool:
        """Delegate monthly-target calculation to TargetCalculator (used by legacy callers)."""
        from saldo27.target_calculator import TargetCalculator

        return TargetCalculator(self)._calculate_monthly_targets()

    def _get_schedule_months(self) -> dict:
        """Delegate schedule-months lookup to TargetCalculator (used by legacy callers)."""
        from saldo27.target_calculator import TargetCalculator

        return TargetCalculator(self)._get_schedule_months()

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
    def _is_allowed_assignment(self, worker_id: str, date: datetime, shift_num: int) -> bool:
        """
        Optimized constraint checking with caching for better performance.

        Args:
            worker_id: ID of the worker
            date: Date for the assignment
            shift_num: Shift number (often unused but kept for compatibility)

        Returns:
            bool: True if assignment is allowed, False otherwise
        """
        # Check cache first for repeated constraint checks
        cache_key = self._get_cache_key("_is_allowed_assignment", worker_id, date, shift_num)
        cached_result = self._get_cached_result(cache_key)
        if cached_result is not None:
            return cached_result

        try:
            worker = next((w for w in self.workers_data if w["id"] == worker_id), None)
            if not worker:
                logging.warning(f"_is_allowed_assignment: Worker {worker_id} not found in workers_data.")
                result = False
                self._set_cached_result(cache_key, result)
                return result

            # Check if worker is already assigned on this date (any post)
            if date in self.schedule and worker_id in self.schedule.get(date, []):
                logging.debug(
                    f"_is_allowed_assignment: Worker {worker_id} already assigned on {date.strftime('%Y-%m-%d')}"
                )
                result = False
                self._set_cached_result(cache_key, result)
                return result

            worker_assignments_set = self.worker_assignments.get(worker_id, set())
            if not isinstance(worker_assignments_set, set):
                worker_assignments_set = set()

            # Get work percentage once for efficiency
            work_percentage = worker.get("work_percentage", 100)
            min_days_required_between = get_effective_min_gap(worker, self.gap_between_shifts)

            # Optimized constraint checking loop
            for assigned_date in worker_assignments_set:
                if assigned_date == date:
                    continue

                days_difference = abs((date - assigned_date).days)

                # 1. Basic minimum gap check
                if days_difference < min_days_required_between:
                    logging.debug(
                        f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails gap with {assigned_date.strftime('%Y-%m-%d')} ({days_difference} < {min_days_required_between})"
                    )
                    result = False
                    self._set_cached_result(cache_key, result)
                    return result

                # 2. Special case for Friday-Monday — only if effective gap > 3
                if min_days_required_between > 3 and days_difference == 3:
                    assigned_weekday = assigned_date.weekday()
                    date_weekday = date.weekday()
                    if (assigned_weekday == 4 and date_weekday == 0) or (assigned_weekday == 0 and date_weekday == 4):
                        logging.debug(
                            f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails Fri-Mon rule with {assigned_date.strftime('%Y-%m-%d')}"
                        )
                        result = False
                        self._set_cached_result(cache_key, result)
                        return result

                # 3. Reject 7- or 14-day same-weekday patterns
                if self._is_weekly_pattern(days_difference) and date.weekday() == assigned_date.weekday():
                    logging.debug(
                        f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails 7/14 day pattern with {assigned_date.strftime('%Y-%m-%d')}"
                    )
                    result = False
                    self._set_cached_result(cache_key, result)
                    return result

            # 4. Optimized incompatibility check
            if date in self.schedule:
                assigned_on_date_others = [
                    w_id for w_id in self.schedule[date] if w_id is not None and w_id != worker_id
                ]
                worker_incompat_list = worker.get("incompatible_with", [])

                # Quick check if any incompatible workers are assigned
                if worker_incompat_list and any(
                    str(other_id) in worker_incompat_list for other_id in assigned_on_date_others
                ):
                    logging.debug(
                        f"_is_allowed_assignment: Worker {worker_id} incompatible with assigned workers on {date.strftime('%Y-%m-%d')}"
                    )
                    result = False
                    self._set_cached_result(cache_key, result)
                    return result

            result = True
            self._set_cached_result(cache_key, result)
            return result

        except Exception as e:
            logging.error(
                f"Error in Scheduler._is_allowed_assignment for worker {worker_id} on {date}: {e!s}", exc_info=True
            )
            return False

    def _assign_workers_simple(self):
        """
        Simple method to directly assign workers to shifts based on targets and ensuring
        all constraints are properly respected:
        - Special Friday-Monday constraint
        - 7/14 day pattern avoidance
        - Worker incompatibility checking
        """
        logging.info("Using simplified assignment method to ensure schedule population")

        # 1. Get all dates that need to be scheduled
        all_dates = sorted(list(self.schedule.keys()))
        if not all_dates:
            all_dates = self._get_date_range(self.start_date, self.end_date)

        # 2. Prepare worker assignments based on target shifts
        worker_assignment_counts = {w["id"]: 0 for w in self.workers_data}
        worker_targets = {w["id"]: w.get("target_shifts", 1) for w in self.workers_data}

        # Sort workers by targets (highest first) to prioritize those who need more shifts
        workers_by_priority = sorted(self.workers_data, key=lambda w: worker_targets.get(w["id"], 0), reverse=True)

        # 3. Go through each date and assign workers
        for date in all_dates:
            # For each shift on this date
            for post in range(self.num_shifts):
                # If the shift is already assigned, skip it
                if date in self.schedule and len(self.schedule[date]) > post and self.schedule[date][post] is not None:
                    continue

                # Find the best worker for this shift
                best_worker = None

                # Get currently assigned workers for this date
                currently_assigned = []
                if date in self.schedule:
                    currently_assigned = [w for w in self.schedule[date] if w is not None]

                # Try each worker in priority order
                for worker in workers_by_priority:
                    worker_id = worker["id"]

                    # Skip if worker is already assigned to this date
                    if worker_id in currently_assigned:
                        continue

                    # Skip if worker has reached their target
                    if worker_assignment_counts[worker_id] >= worker_targets[worker_id]:
                        continue

                    # Initialize too_close flag
                    too_close = False

                    # Inside the loop where we check minimum gap
                    for assigned_date in self.worker_assignments.get(worker_id, set()):
                        days_difference = abs((date - assigned_date).days)

                        # Minimum gap (calendar days) per worker type
                        min_days_between = get_effective_min_gap(worker, self.gap_between_shifts)
                        if days_difference < min_days_between:
                            too_close = True
                            break

                        # Special case: Friday-Monday — only block if effective gap > 3
                        if days_difference == 3 and min_days_between > 3:
                            if (date.weekday() == 0 and assigned_date.weekday() == 4) or (
                                date.weekday() == 4 and assigned_date.weekday() == 0
                            ):
                                too_close = True
                                break

                        # Check for weekly-pattern (7 or 14 days, same weekday)
                        if self._is_weekly_pattern(days_difference) and date.weekday() == assigned_date.weekday():
                            too_close = True

                    if too_close:
                        continue

                    # Check for worker incompatibilities
                    incompatible_with = worker.get("incompatible_with", [])
                    if incompatible_with:
                        has_conflict = False
                        for incompatible_id in incompatible_with:
                            if incompatible_id in currently_assigned:
                                has_conflict = True
                                break

                        if has_conflict:
                            continue

                    # CRITICAL: no_last_post workers cannot be assigned to the last post
                    if post == self.num_shifts - 1 and worker.get("no_last_post", False):
                        continue

                    # This worker is a good candidate
                    # CRITICAL: Final check - verify tolerance before assigning
                    if hasattr(self, "schedule_builder") and self.schedule_builder:
                        if self.schedule_builder._would_violate_tolerance(worker_id, date, allow_relaxation=True):
                            logging.debug(
                                f"Simple assignment: {worker_id} rejected for {date.strftime('%Y-%m-%d')} - tolerance violation"
                            )
                            continue  # Try next worker

                    best_worker = worker
                    break

                # If we found a suitable worker, assign them
                if best_worker:
                    worker_id = best_worker["id"]

                    # Make sure the schedule list exists and has the right size
                    if date not in self.schedule:
                        self.schedule[date] = []

                    while len(self.schedule[date]) <= post:
                        self.schedule[date].append(None)

                    # CRITICAL: Verify slot is not protected by mandatory
                    if self.schedule[date][post] is not None:
                        existing = self.schedule[date][post]
                        if hasattr(self, "schedule_builder"):
                            if self.schedule_builder.is_locked_mandatory(
                                existing, date
                            ) or self.schedule_builder.is_mandatory(existing, date):
                                logging.warning(
                                    f"🔒 BLOCKED: Cannot overwrite MANDATORY {existing} on {date.strftime('%Y-%m-%d')} post {post}"
                                )
                                continue

                    # Assign the worker
                    self.schedule[date][post] = worker_id

                    # Update tracking data
                    self._update_tracking_data(worker_id, date, post)

                    # Update the assignment count
                    worker_assignment_counts[worker_id] += 1

                    # Update currently_assigned for this date
                    currently_assigned.append(worker_id)

                    # Log the assignment
                    logging.info(f"Assigned worker {worker_id} to {date.strftime('%d-%m-%Y')}, post {post}")
                else:
                    # No suitable worker found, leave unassigned
                    if date not in self.schedule:
                        self.schedule[date] = []

                    while len(self.schedule[date]) <= post:
                        self.schedule[date].append(None)

                    logging.debug(f"No suitable worker found for {date.strftime('%d-%m-%Y')}, post {post}")

        # 4. Return the number of assignments made
        total_assigned = sum(worker_assignment_counts.values())
        total_shifts = len(all_dates) * self.num_shifts
        logging.info(
            f"Simple assignment complete: {total_assigned}/{total_shifts} shifts assigned ({total_assigned / total_shifts * 100:.1f}%)"
        )

        return total_assigned > 0

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
                    elif v["type"] == "incompatibility":
                        logging.warning(
                            f"Violation {i + 1}: Incompatible workers {v['worker_id']} and {v['incompatible_id']} are both assigned on {v['date']}"
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

            # Fix each violation
            for violation in violations:
                if violation["type"] in {"min_rest_days", "friday_monday_pattern", "weekly_pattern"}:
                    # Fix by unassigning one of the shifts
                    worker_id = violation["worker_id"]
                    date1 = violation["date1"]
                    date2 = violation["date2"]

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
                            worker_id, date_to_unassign, "fix_constraint_rest"
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
                            worker_to_unassign, date, "fix_constraint_incompat"
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

    def _cleanup_schedule(self):
        """
        Clean up the schedule before validation

        - Ensure all dates have proper shift lists
        - Remove any empty shifts at the end of lists
        - Sort schedule by date
        """
        logging.info("Cleaning up schedule...")

        # Ensure each date matches its variable-shifts count
        for date in self._get_date_range(self.start_date, self.end_date):
            expected = self._get_shifts_for_date(date)
            if date not in self.schedule:
                self.schedule[date] = [None] * expected
            else:
                actual = len(self.schedule[date])
                if actual < expected:
                    self.schedule[date].extend([None] * (expected - actual))
                elif actual > expected:
                    self.schedule[date] = self.schedule[date][:expected]
        # Create a sorted version of the schedule
        sorted_schedule = {}
        for date in sorted(self.schedule.keys()):
            sorted_schedule[date] = self.schedule[date]

        self.schedule = sorted_schedule

        logging.info("Schedule cleanup complete")
        return True

    # ========================================
    # 6. SCORING AND EVALUATION
    # ========================================
    def calculate_score(self, schedule_to_score=None, assignments_to_score=None):
        return self._reporting_service.calculate_score(schedule_to_score, assignments_to_score)

    def _calculate_coverage(self):
        return self._reporting_service.calculate_coverage()

    def _calculate_post_rotation(self):
        return self._reporting_service.calculate_post_rotation()

    def _calculate_post_rotation_coverage(self):
        return self._reporting_service.calculate_post_rotation_coverage()

    # ========================================
    # 7. BACKUP AND RESTORE OPERATIONS
    # ========================================
    def _save_global_best(self):
        """
        Save the scheduler-level global best schedule snapshot.

        Named distinctly from ``ScheduleBuilder._save_current_as_best``, which is
        the snapshot used during iterative construction/optimization and is the
        authoritative one consulted by ``SchedulerCore``.  This method captures a
        coarser, scheduler-level copy for optional external use (e.g. after the
        full generation pipeline completes).
        """
        try:
            logging.debug("Saving scheduler-level global best schedule...")

            # Create a deep copy of the current schedule
            best_schedule = {}
            for date, shifts in self.schedule.items():
                best_schedule[date] = shifts.copy()

            # Save all tracking data
            self.global_best_schedule_data = {
                "schedule": best_schedule,
                "worker_assignments": {
                    w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()
                },
                "worker_posts": {w_id: posts.copy() for w_id, posts in self.worker_posts.items()},
                "worker_weekdays": {w_id: counts.copy() for w_id, counts in self.worker_weekdays.items()},
                "worker_weekends": {w_id: dates.copy() for w_id, dates in self.worker_weekends.items()},
                "worker_shift_counts": self.worker_shift_counts.copy()
                if hasattr(self, "worker_shift_counts")
                else None,
                "worker_weekend_counts": self.worker_weekend_counts.copy()
                if hasattr(self, "worker_weekend_counts")
                else None,
                "score": self.calculate_score(),
            }

            logging.debug(f"Saved global best schedule with score: {self.global_best_schedule_data['score']}")
            return True
        except Exception as e:
            logging.error(f"Error saving global best schedule: {e!s}", exc_info=True)
            return False

    def _backup_best_schedule(self):
        """Save a backup of the current best schedule"""
        try:
            # Create deep copies of all structures
            self.backup_schedule = {}
            for date, shifts in self.schedule.items():
                self.backup_schedule[date] = shifts.copy() if shifts else []

            self.backup_worker_assignments = {}
            for worker_id, assignments in self.worker_assignments.items():
                self.backup_worker_assignments[worker_id] = assignments.copy()

            # Include other backup structures if needed
            self.backup_worker_posts = {worker_id: posts.copy() for worker_id, posts in self.worker_posts.items()}

            self.backup_worker_weekdays = {
                worker_id: weekdays.copy() for worker_id, weekdays in self.worker_weekdays.items()
            }

            self.backup_worker_weekends = {
                worker_id: weekends.copy() for worker_id, weekends in self.worker_weekends.items()
            }

            # Only backup constraint_skips if it exists to avoid errors
            if hasattr(self, "constraint_skips"):
                self.backup_constraint_skips = {}
                for worker_id, skips in self.constraint_skips.items():
                    self.backup_constraint_skips[worker_id] = {}
                    for skip_type, skip_values in skips.items():
                        if skip_values is not None:
                            self.backup_constraint_skips[worker_id][skip_type] = skip_values.copy()

            filled_shifts = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
            logging.info(f"Backed up current schedule in scheduler with {filled_shifts} filled shifts")
            return True
        except Exception as e:
            logging.error(f"Error in scheduler backup: {e!s}", exc_info=True)
            return False

    def _restore_best_schedule(self):
        """Restore the backed up schedule"""
        try:
            if not hasattr(self, "backup_schedule"):
                logging.warning("No scheduler backup available to restore")
                return False

            # Restore from our backups
            self.schedule = {}
            for date, shifts in self.backup_schedule.items():
                self.schedule[date] = shifts.copy() if shifts else []

            self.worker_assignments = {}
            for worker_id, assignments in self.backup_worker_assignments.items():
                self.worker_assignments[worker_id] = assignments.copy()

            # Restore other structures if they exist
            if hasattr(self, "backup_worker_posts"):
                self.worker_posts = {worker_id: posts.copy() for worker_id, posts in self.backup_worker_posts.items()}

            if hasattr(self, "backup_worker_weekdays"):
                self.worker_weekdays = {
                    worker_id: weekdays.copy() for worker_id, weekdays in self.backup_worker_weekdays.items()
                }

            if hasattr(self, "backup_worker_weekends"):
                self.worker_weekends = {
                    worker_id: weekends.copy() for worker_id, weekends in self.backup_worker_weekends.items()
                }

            # Only restore constraint_skips if backup exists
            if hasattr(self, "backup_constraint_skips"):
                self.constraint_skips = {}
                for worker_id, skips in self.backup_constraint_skips.items():
                    self.constraint_skips[worker_id] = {}
                    for skip_type, skip_values in skips.items():
                        if skip_values is not None:
                            self.constraint_skips[worker_id][skip_type] = skip_values.copy()

            filled_shifts = sum(1 for shifts in self.schedule.values() for worker in shifts if worker is not None)
            logging.info(f"Restored schedule in scheduler with {filled_shifts} filled shifts")
            return True
        except Exception as e:
            logging.error(f"Error in scheduler restore: {e!s}", exc_info=True)
            return False

    # ========================================
    # 8. VALIDATION AND VERIFICATION
    # ========================================
    def validate_and_fix_final_schedule(self):
        return self._validation_service.validate_and_fix_final_schedule()

    def _run_final_validation_and_fix(self):
        return self._validation_service.run_final_validation_and_fix()

    # ========================================
    # 9. REPORTING AND EXPORT
    # ========================================
    def export_schedule(self, format="txt"):
        return self._reporting_service.export_schedule(format=format)

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
    def _is_weekly_pattern(self, days_difference):
        """Return True if this is a 7- or 14-day same-weekday pattern."""
        return days_difference in (7, 14)

    def _redistribute_excess_shifts(self, excess_shifts, excluded_worker_id, mandatory_shifts_by_worker):
        """Helper method to redistribute excess shifts from one worker to others, respecting mandatory assignments"""
        eligible_workers = [w for w in self.workers_data if w["id"] != excluded_worker_id]

        if not eligible_workers:
            return

        # Sort by work percentage (give more to workers with higher percentage)
        eligible_workers.sort(key=lambda w: float(w.get("work_percentage", 100)), reverse=True)

        # Distribute excess shifts
        for i in range(excess_shifts):
            worker = eligible_workers[i % len(eligible_workers)]
            worker["target_shifts"] += 1
            logging.info(f"Redistributed 1 shift to worker {worker['id']}")

    # ========================================
    # 11. REAL-TIME OPERATIONS
    # ========================================
    _RT_DISABLED: ClassVar[dict[str, Any]] = {
        "success": False,
        "message": "Real-time features not enabled",
        "error": "REAL_TIME_DISABLED",
    }

    def is_real_time_enabled(self) -> bool:
        """Return True if the real-time engine is active."""
        return self.real_time_engine is not None

    def enable_real_time_features(self) -> bool:
        """Activate real-time features; returns True on success."""
        if not self.is_real_time_enabled():
            logging.warning("Cannot enable real-time features: real-time engine not initialized")
            return False
        assert self.real_time_engine is not None
        return self.real_time_engine.enable_features()

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

    def collect_historical_data(self) -> dict[str, Any]:
        """Collect current schedule data for historical analysis; returns plain dict."""
        if not self.is_predictive_analytics_enabled():
            return self._PA_DISABLED
        assert self.predictive_analytics is not None
        return self.predictive_analytics.collect_historical_data_dict()

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

    def apply_predictive_adjustments(self, optimization_result: dict[str, Any]) -> dict[str, Any]:
        """Apply parameter adjustments recommended by predictive optimization; returns plain dict."""
        if not self.is_predictive_analytics_enabled() or not self.predictive_optimizer:
            return {
                "success": False,
                "message": "Predictive optimization not available",
                "error": "PREDICTIVE_OPTIMIZER_DISABLED",
            }
        assert self.predictive_optimizer is not None
        return self.predictive_optimizer.apply_predictive_adjustments_dict(optimization_result)
