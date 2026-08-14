from __future__ import annotations

# Imports
import json
import logging
import math
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

from saldo27.constraint_checker import ConstraintChecker
from saldo27.data_manager import DataManager
from saldo27.exceptions import ConfigurationError, SchedulerError
from saldo27.infrastructure.optional_engines import load_optional_engines
from saldo27.scheduler_config import SchedulerConfig, setup_logging
from saldo27.statistics_calculator import StatisticsCalculator
from saldo27.utilities import DateTimeUtils, get_effective_min_gap
from saldo27.worker_eligibility import WorkerEligibilityTracker

if TYPE_CHECKING:
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

            # Then validate the configuration
            self._validate_config(config)

            # Phase 1: Extract config into instance attributes
            self._init_config(config)

            # Phase 2: Build incompatibility matrix
            self._init_incompatibilities()

            # Phase 3: Initialize all tracking dictionaries
            self._init_tracking_state()

            # Phase 4: Initialize helper modules / engines
            self._init_modules(config)

            # Phase 5: Calculate targets and prior-schedule containers
            self._init_targets_and_prior()

            self._log_initialization()

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
            return set(getattr(self.schedule_builder, "_locked_mandatory", set()))
        return set()

    def set_locked_mandatory(self, locked: set[Any] | tuple[Any, ...]) -> None:
        if hasattr(self, "schedule_builder") and self.schedule_builder is not None:
            self.schedule_builder._locked_mandatory = set(locked)

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
        """Phase 1: Extract configuration into instance attributes."""
        self.config = config
        self.start_date = config["start_date"]
        self.end_date = config["end_date"]
        self.num_shifts = config["num_shifts"]
        self.variable_shifts = config.get("variable_shifts", [])
        self.workers_data = config["workers_data"]
        self._normalize_worker_ids(self.workers_data)
        self.holidays = config.get("holidays", [])
        self.enable_proportional_weekends = config.get("enable_proportional_weekends", True)
        self.weekend_tolerance = config.get("weekend_tolerance", 1)
        self.bridge_tolerance = config.get("bridge_tolerance", 0.5)

        # Bridge periods
        year = self.start_date.year
        self.bridge_periods = self.date_utils.identify_bridge_periods(self.holidays, year)
        self.worker_bridge_counts = {w["id"]: set() for w in self.workers_data}

        # Configurable parameters with defaults
        default_config = SchedulerConfig.get_default_config()
        self.gap_between_shifts = config.get("gap_between_shifts", default_config["gap_between_shifts"])
        self.max_consecutive_weekends = config.get(
            "max_consecutive_weekends", default_config["max_consecutive_weekends"]
        )

        # Sort variable shifts for efficient lookup
        self.variable_shifts.sort(key=lambda x: x["start_date"])

        # Current time and user
        self.current_datetime = self.date_utils.get_spain_time()
        self.current_user = "saldo27"

    def _init_incompatibilities(self) -> None:
        """Phase 2: Build incompatibility lists from worker flags."""
        incompatible_worker_ids = {worker["id"] for worker in self.workers_data if worker.get("is_incompatible", False)}
        logging.debug(f"Identified incompatible worker IDs (from is_incompatible flag): {incompatible_worker_ids}")

        for worker in self.workers_data:
            worker_id = worker["id"]
            if "incompatible_with" not in worker or not worker["incompatible_with"]:
                worker["incompatible_with"] = []
                if worker.get("is_incompatible", False):
                    worker["incompatible_with"] = list(incompatible_worker_ids - {worker_id})
            else:
                logging.debug(
                    f"Worker {worker_id} has predefined incompatible_with list: {worker['incompatible_with']}"
                )
            logging.debug(f"Worker {worker_id} final incompatible_with list: {worker['incompatible_with']}")

    def _init_tracking_state(self) -> None:
        """Phase 3: Initialize all tracking dictionaries and schedule structure."""
        self.schedule = {}
        self.schedule_builder: Any = None  # Set by SchedulerCore during generation
        self.worker_assignments = {w["id"]: set() for w in self.workers_data}
        self.worker_posts = {w["id"]: set() for w in self.workers_data}
        self.worker_weekdays = {w["id"]: {i: 0 for i in range(7)} for w in self.workers_data}
        self.worker_weekends = {w["id"]: [] for w in self.workers_data}

        self._initialize_schedule_with_variable_shifts()

        self.worker_shift_counts = {w["id"]: 0 for w in self.workers_data}
        self.worker_weekend_counts = {w["id"]: 0 for w in self.workers_data}
        self.worker_post_counts = {w["id"]: {p: 0 for p in range(self.num_shifts)} for w in self.workers_data}
        self.worker_weekday_counts = {w["id"]: {d: 0 for d in range(7)} for w in self.workers_data}
        self.worker_holiday_counts = {w["id"]: 0 for w in self.workers_data}
        self.last_assignment_date = {w["id"]: None for w in self.workers_data}
        self.consecutive_shifts = {w["id"]: 0 for w in self.workers_data}

        for worker in self.workers_data:
            if "target_shifts" not in worker:
                worker["target_shifts"] = 0

        # Max shifts per worker
        total_days = (self.end_date - self.start_date).days + 1
        total_shifts_possible = total_days * self.num_shifts
        num_workers = len(self.workers_data)
        self.max_shifts_per_worker = (
            (total_shifts_possible // num_workers) + 2 if num_workers > 0 else total_shifts_possible
        )

        # Constraint skips tracking
        self.constraint_skips = {
            w["id"]: {"gap": [], "incompatibility": [], "reduced_gap": []} for w in self.workers_data
        }

    def _init_modules(self, config: dict[str, Any]) -> None:
        """Phase 4: Initialize helper modules and optional engines."""
        self.stats = StatisticsCalculator(self)
        self.constraint_checker = ConstraintChecker(self)
        self.data_manager = DataManager(self)
        self.eligibility_tracker = WorkerEligibilityTracker(
            self.workers_data,
            self.holidays,
            self.gap_between_shifts,
            self.max_consecutive_weekends,
            start_date=self.start_date,
            end_date=self.end_date,
            date_utils=self.date_utils,
            scheduler=self,
        )

        # Optional engines loaded through infrastructure adapter
        loaded_optional = load_optional_engines(self, config)

        # Predictive optimizer depends on predictive_analytics when present.
        self.predictive_optimizer = None
        self.predictive_analytics = loaded_optional.get("predictive_analytics")
        if self.predictive_analytics is not None:
            try:
                from saldo27.predictive_optimizer import PredictiveOptimizer

                self.predictive_optimizer = PredictiveOptimizer(self, self.predictive_analytics)
                predictive_config = config.get("predictive_analytics_config", {})
                if predictive_config.get("auto_collect_data", True):
                    self.predictive_analytics.auto_collect_data_if_enabled()
            except Exception as e:
                logging.error(f"Error initializing predictive optimizer: {e}")

    def _init_targets_and_prior(self) -> None:
        """Phase 5: Calculate shift targets and initialize prior-schedule containers."""
        self._calculate_target_shifts()

        self._base_target_shifts: dict[str, float] = {
            w["id"]: float(w.get("target_shifts", 0)) for w in self.workers_data
        }

        # Prior-schedule data (populated lazily via load_prior_schedule_data())
        self.prior_assignments: dict[str, set] = {}
        self.prior_shift_counts: dict[str, int] = {}
        self.prior_weekend_counts: dict[str, int] = {}
        self.prior_target_shifts: dict[str, float] = {}
        self.prior_last_date: dict[str, Any | None] = {}

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
        """
        Validate configuration parameters using the enhanced configuration validator.

        Args:
            config: Dictionary containing schedule configuration

        Raises:
            ConfigurationError: If configuration is invalid
        """
        # Use the enhanced configuration validation
        is_valid, error_message = SchedulerConfig.validate_config(config)
        if not is_valid:
            raise ConfigurationError(error_message)

        # Additional validation specific to scheduler needs
        # Validate date range
        if not isinstance(config["start_date"], datetime) or not isinstance(config["end_date"], datetime):
            raise ConfigurationError("Start date and end date must be datetime objects")

        if config["start_date"] > config["end_date"]:
            raise ConfigurationError("Start date must be before end date")

        # Validate workers data
        if not config["workers_data"] or not isinstance(config["workers_data"], list):
            raise ConfigurationError("workers_data must be a non-empty list")

        # Validate each worker's data
        for worker in config["workers_data"]:
            if not isinstance(worker, dict):
                raise ConfigurationError("Each worker must be a dictionary")

            if "id" not in worker:
                raise ConfigurationError("Each worker must have an 'id' field")

            # Validate work percentage if present
            if "work_percentage" in worker:
                try:
                    work_percentage = (
                        float(str(worker["work_percentage"]).strip()) if worker["work_percentage"] else 100
                    )
                    # Si es 0 o vacío, usar 100% por defecto
                    if work_percentage == 0:
                        worker["work_percentage"] = 100
                        work_percentage = 100
                    elif work_percentage < 0 or work_percentage > 100:
                        raise ConfigurationError(
                            f"Invalid work percentage for worker {worker['id']}: {work_percentage}"
                        )
                except (ValueError, TypeError):
                    # Si hay error de conversión, usar 100% por defecto
                    worker["work_percentage"] = 100

            # Validate date formats in work_periods if present
            if worker.get("work_periods"):
                try:
                    self.date_utils.parse_date_ranges(worker["work_periods"])
                except ValueError as e:
                    raise ConfigurationError(f"Invalid work_periods format for worker {worker['id']}: {e!s}")

            # Validate date formats in mandatory_days if present
            if worker.get("mandatory_days"):
                try:
                    self.date_utils.parse_dates(worker["mandatory_days"])
                except ValueError as e:
                    raise ConfigurationError(f"Invalid mandatory_days format for worker {worker['id']}: {e!s}")

            # Validate date formats in days_off if present
            if worker.get("days_off"):
                try:
                    self.date_utils.parse_date_ranges(worker["days_off"])
                except ValueError as e:
                    raise ConfigurationError(f"Invalid days_off format for worker {worker['id']}: {e!s}")

        # Validate holidays if present
        if "holidays" in config:
            if not isinstance(config["holidays"], list):
                raise ConfigurationError("holidays must be a list")

            for holiday in config["holidays"]:
                if not isinstance(holiday, datetime):
                    raise ConfigurationError("Each holiday must be a datetime object")

    def _log_initialization(self):
        """Log initialization parameters"""
        logging.info("Scheduler initialized with:")
        logging.info(f"Start date: {self.start_date}")
        logging.info(f"End date: {self.end_date}")
        logging.info(f"Number of shifts: {self.num_shifts}")
        logging.info(f"Number of workers: {len(self.workers_data)}")
        logging.info(f"Holidays: {[h.strftime('%d-%m-%Y') for h in self.holidays]}")
        logging.info(f"Gap between shifts: {self.gap_between_shifts}")
        logging.info(f"Max consecutive weekend/holiday shifts: {self.max_consecutive_weekends}")
        logging.info(f"Current datetime (Spain): {self.current_datetime}")
        logging.info(f"Current user: {self.current_user}")

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
        # Initialize loop variables
        current_date = self.start_date
        dates_initialized = 0
        variable_dates = 0
        # Build a lookup for fast matching of variable ranges
        var_cfgs = [(cfg["start_date"], cfg["end_date"], cfg["shifts"]) for cfg in self.variable_shifts]
        while current_date <= self.end_date:
            # Determine how many shifts this date should have
            shifts_for_date = self.num_shifts
            for start, end, cnt in var_cfgs:
                if start <= current_date <= end:
                    shifts_for_date = cnt
                    logging.info(
                        f"Variable shifts applied for {current_date}: {cnt} shifts (default is {self.num_shifts})"
                    )
                    variable_dates += 1
                    break
            # Initialize the schedule entry for this date
            self.schedule[current_date] = [None] * shifts_for_date
            dates_initialized += 1

            # Move to next date
            current_date += timedelta(days=1)

    def _reset_schedule(self):
        """Reset all schedule data"""
        self.schedule = {}
        self.worker_assignments = {w["id"]: set() for w in self.workers_data}
        self.worker_posts = {w["id"]: set() for w in self.workers_data}
        self.worker_weekdays = {w["id"]: {i: 0 for i in range(7)} for w in self.workers_data}
        self.worker_weekends = {w["id"]: [] for w in self.workers_data}
        self.constraint_skips = {
            w["id"]: {"gap": [], "incompatibility": [], "reduced_gap": []} for w in self.workers_data
        }

    def _ensure_data_integrity(self):
        """
        Ensure all data structures are consistent before schedule generation
        """
        logging.info("Ensuring data integrity...")

        # Ensure all workers have proper data structures
        for worker in self.workers_data:
            worker_id = worker["id"]

            # Ensure worker assignments tracking
            if worker_id not in self.worker_assignments:
                self.worker_assignments[worker_id] = set()

            # Ensure worker posts tracking
            if worker_id not in self.worker_posts:
                self.worker_posts[worker_id] = set()

            # Ensure weekday tracking
            if worker_id not in self.worker_weekdays:
                self.worker_weekdays[worker_id] = {i: 0 for i in range(7)}

            # Ensure weekend tracking
            if worker_id not in self.worker_weekends:
                self.worker_weekends[worker_id] = []

        # Ensure schedule dictionary entries match variable shifts configuration
        for current_date in self._get_date_range(self.start_date, self.end_date):
            expected = self._get_shifts_for_date(current_date)
            if current_date not in self.schedule:
                self.schedule[current_date] = [None] * expected
            else:
                # Pad or trim to expected length
                actual = len(self.schedule[current_date])
                if actual < expected:
                    self.schedule[current_date].extend([None] * (expected - actual))
                elif actual > expected:
                    self.schedule[current_date] = self.schedule[current_date][:expected]

        logging.info("Data integrity check completed")
        return True

    def _synchronize_tracking_data(self) -> bool:
        """
        Optimized tracking data synchronization with minimal allocations.
        Called by the ScheduleBuilder to maintain data integrity.
        """
        try:
            logging.info("Synchronizing tracking data structures...")

            # Clear cache when data changes
            self._clear_cache()

            # Reset existing tracking data efficiently
            for worker_id in (w["id"] for w in self.workers_data):
                self.worker_assignments[worker_id].clear()

                # CRITICAL FIX: Ensure worker_posts is a set, not dict
                if not isinstance(self.worker_posts[worker_id], set):
                    logging.warning(
                        f"Correcting worker_posts[{worker_id}] from {type(self.worker_posts[worker_id])} to set"
                    )
                    self.worker_posts[worker_id] = set()
                else:
                    self.worker_posts[worker_id].clear()

                # Reset weekend lists
                self.worker_weekends[worker_id].clear()
                # Reset weekday counts
                for day in range(7):
                    self.worker_weekdays[worker_id][day] = 0
                # Reset counts
                self.worker_shift_counts[worker_id] = 0
                self.worker_weekend_counts[worker_id] = 0

            # Rebuild tracking data from the current schedule efficiently
            for date, shifts in self.schedule.items():
                weekday = date.weekday()
                is_weekend_or_holiday = (
                    weekday >= 4 or date in self.holidays or (date + timedelta(days=1)) in self.holidays
                )

                for post_idx, worker_id in enumerate(shifts):
                    if worker_id is not None:
                        # Update worker assignments
                        self.worker_assignments[worker_id].add(date)

                        # Update posts worked
                        self.worker_posts[worker_id].add(post_idx)

                        # Update weekday counts
                        self.worker_weekdays[worker_id][weekday] += 1

                        # Update weekends/holidays efficiently
                        if is_weekend_or_holiday:
                            self.worker_weekends[worker_id].append(date)
                            self.worker_weekend_counts[worker_id] += 1

                        # Update shift counts
                        self.worker_shift_counts[worker_id] += 1

            # Sort weekend dates for consistency (batch operation)
            for worker_id in self.worker_weekends:
                if self.worker_weekends[worker_id]:  # Only sort if not empty
                    self.worker_weekends[worker_id].sort()

            # Seed worker_shift_counts with prior-period counts so that
            # ordering strategies naturally de-prioritize workers who already
            # accumulated many shifts in the previous schedule period.
            if self.prior_shift_counts:
                for wid, prior_count in self.prior_shift_counts.items():
                    if wid in self.worker_shift_counts:
                        self.worker_shift_counts[wid] += prior_count

            logging.info("Tracking data synchronization complete.")
            return True
        except Exception as e:
            logging.error(f"Error synchronizing tracking data: {e!s}", exc_info=True)
            return False

    def _validate_data_synchronization(self) -> tuple[bool, dict[str, Any]]:
        """
        Validate that worker_assignments and schedule are perfectly synchronized.

        Returns:
            Tuple[bool, Dict]: (is_synchronized, validation_report)
        """
        logging.debug("Validating data synchronization between worker_assignments and schedule...")

        try:
            validation_report = {
                "is_synchronized": True,
                "discrepancies": [],
                "summary": {
                    "total_workers": len(self.workers_data),
                    "workers_with_issues": 0,
                    "total_assignments_schedule": 0,
                    "total_assignments_tracking": 0,
                    "missing_from_tracking": 0,
                    "extra_in_tracking": 0,
                },
            }

            # Build assignments from schedule for comparison
            schedule_assignments = {}
            for worker in self.workers_data:
                schedule_assignments[worker["id"]] = set()

            for date, shifts in self.schedule.items():
                validation_report["summary"]["total_assignments_schedule"] += len([s for s in shifts if s is not None])
                for shift_idx, worker_id in enumerate(shifts):
                    if worker_id is not None:
                        if worker_id not in schedule_assignments:
                            schedule_assignments[worker_id] = set()
                        schedule_assignments[worker_id].add(date)

            # Count total assignments in tracking
            for worker_id, assignments in self.worker_assignments.items():
                validation_report["summary"]["total_assignments_tracking"] += len(assignments)

            # Compare each worker's assignments
            for worker_id in set(list(self.worker_assignments.keys()) + list(schedule_assignments.keys())):
                tracking_assignments = self.worker_assignments.get(worker_id, set())
                schedule_worker_assignments = schedule_assignments.get(worker_id, set())

                missing_from_tracking = schedule_worker_assignments - tracking_assignments
                extra_in_tracking = tracking_assignments - schedule_worker_assignments

                if missing_from_tracking or extra_in_tracking:
                    validation_report["is_synchronized"] = False
                    validation_report["summary"]["workers_with_issues"] += 1
                    validation_report["summary"]["missing_from_tracking"] += len(missing_from_tracking)
                    validation_report["summary"]["extra_in_tracking"] += len(extra_in_tracking)

                    discrepancy = {
                        "worker_id": worker_id,
                        "missing_from_tracking": sorted([d.strftime("%Y-%m-%d") for d in missing_from_tracking]),
                        "extra_in_tracking": sorted([d.strftime("%Y-%m-%d") for d in extra_in_tracking]),
                        "tracking_count": len(tracking_assignments),
                        "schedule_count": len(schedule_worker_assignments),
                    }
                    validation_report["discrepancies"].append(discrepancy)

            # Log summary
            if validation_report["is_synchronized"]:
                logging.debug(
                    "✓ Data synchronization validation passed: worker_assignments and schedule are synchronized"
                )
            else:
                logging.warning(
                    f"✗ Data synchronization issues detected: {len(validation_report['discrepancies'])} workers affected"
                )
                for discrepancy in validation_report["discrepancies"][:3]:  # Log first 3 issues
                    worker_id = discrepancy["worker_id"]
                    logging.warning(
                        f"  Worker {worker_id}: {len(discrepancy['missing_from_tracking'])} missing, {len(discrepancy['extra_in_tracking'])} extra"
                    )

            return validation_report["is_synchronized"], validation_report

        except Exception as e:
            logging.error(f"Error validating data synchronization: {e!s}", exc_info=True)
            return False, {"error": str(e), "is_synchronized": False}

    def _repair_data_synchronization(self, validation_report: dict[str, Any] | None = None) -> bool:
        """
        Repair synchronization issues between worker_assignments and schedule.
        Uses schedule as the source of truth.

        Args:
            validation_report: Optional validation report from _validate_data_synchronization

        Returns:
            bool: True if repair was successful
        """
        logging.info("Repairing data synchronization issues...")

        try:
            # Get current validation report if not provided
            if validation_report is None:
                is_synchronized, validation_report = self._validate_data_synchronization()
                if is_synchronized:
                    logging.info("No repair needed: data is already synchronized")
                    return True

            # Build correct worker_assignments from schedule (schedule is source of truth)
            corrected_assignments = {}
            for worker in self.workers_data:
                corrected_assignments[worker["id"]] = set()

            for date, shifts in self.schedule.items():
                for shift_idx, worker_id in enumerate(shifts):
                    if worker_id is not None:
                        if worker_id not in corrected_assignments:
                            corrected_assignments[worker_id] = set()
                        corrected_assignments[worker_id].add(date)

            # Update worker_assignments in place to preserve shared references
            self.worker_assignments.clear()
            self.worker_assignments.update(corrected_assignments)

            # Verify the repair
            is_synchronized_after, validation_after = self._validate_data_synchronization()

            if is_synchronized_after:
                total_fixes = validation_report.get("summary", {}).get(
                    "missing_from_tracking", 0
                ) + validation_report.get("summary", {}).get("extra_in_tracking", 0)
                logging.info(f"✓ Data synchronization repair successful: Fixed {total_fixes} inconsistencies")
                return True
            else:
                logging.error("✗ Data synchronization repair failed: Issues still persist")
                return False

        except Exception as e:
            logging.error(f"Error repairing data synchronization: {e!s}", exc_info=True)
            return False

    def _ensure_data_synchronization(self) -> bool:
        """
        Ensure data synchronization by validating and repairing if necessary.

        Returns:
            bool: True if data is synchronized after this call
        """
        try:
            is_synchronized, validation_report = self._validate_data_synchronization()

            if not is_synchronized:
                logging.warning("Data synchronization issues detected, attempting repair...")
                return self._repair_data_synchronization(validation_report)

            return True

        except Exception as e:
            logging.error(f"Error ensuring data synchronization: {e!s}", exc_info=True)
            return False

    def _reconcile_schedule_tracking(self):
        """
        Reconciles worker_assignments tracking with the actual schedule
        to fix any inconsistencies before validation.
        """
        logging.info("Reconciling worker assignments tracking with schedule...")

        try:
            # Use the new synchronization validation and repair methods
            is_synchronized = self._ensure_data_synchronization()

            if is_synchronized:
                logging.info("Reconciliation complete: Data structures are synchronized")
                return True
            else:
                logging.error("Reconciliation failed: Unable to synchronize data structures")
                return False

        except Exception as e:
            logging.error(f"Error reconciling schedule tracking: {e!s}", exc_info=True)
            return False

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
        """
        Update all relevant tracking data structures when a worker is assigned or unassigned.
        This includes worker_assignments, worker_posts, worker_weekdays, and worker_weekends.
        It also calls the eligibility tracker if it exists.

        Enhanced to ensure data synchronization between schedule and worker_assignments.
        """
        try:
            # Ensure basic data structures exist for the worker
            if worker_id not in self.worker_assignments:
                self.worker_assignments[worker_id] = set()

            # Robust check and initialization for self.worker_posts[worker_id]
            # Ensures it's a set even if worker_id was already a key but with a wrong type (e.g., dict)
            if worker_id not in self.worker_posts or not isinstance(self.worker_posts.get(worker_id), set):
                logging.warning(
                    f"Re-initializing self.worker_posts[{worker_id}] as a set due to incorrect type."
                )  # Optional: log this
                self.worker_posts[worker_id] = set()

            if worker_id not in self.worker_weekdays:
                self.worker_weekdays[worker_id] = {i: 0 for i in range(7)}
            if worker_id not in self.worker_weekends:
                self.worker_weekends[worker_id] = []  # List of weekend/holiday dates worked

            if removing:
                # Remove from worker assignments
                if date in self.worker_assignments.get(worker_id, set()):
                    self.worker_assignments[worker_id].remove(date)

                # Update shift count
                if self.worker_shift_counts.get(worker_id, 0) > 0:
                    self.worker_shift_counts[worker_id] -= 1

                # Clean up worker_posts if the worker no longer works at this post position
                # anywhere else in the schedule (schedule[date][post] is already None at this point).
                if post in self.worker_posts.get(worker_id, set()):
                    still_works_at_post = any(
                        self._get_worker_assigned_to_post(d, post) == worker_id
                        for d in self.worker_assignments.get(worker_id, set())
                    )
                    if not still_works_at_post:
                        self.worker_posts[worker_id].discard(post)

                # Update weekday counts
                weekday = date.weekday()
                # Ensure weekday key exists before decrementing, though init above should handle it.
                if weekday in self.worker_weekdays.get(worker_id, {}):  # Defensive access
                    if self.worker_weekdays[worker_id][weekday] > 0:
                        self.worker_weekdays[worker_id][weekday] -= 1
                else:
                    logging.warning(
                        f"Weekday {weekday} not found in self.worker_weekdays for worker {worker_id} during removal."
                    )

                # Update weekend tracking
                is_special_day = self.date_utils.is_weekend_day(date, self.holidays)

                if is_special_day:
                    current_weekends = self.worker_weekends.get(worker_id)  # Use .get for safety
                    if current_weekends is not None and date in current_weekends:
                        current_weekends.remove(date)
                    # Update weekend count
                    if self.worker_weekend_counts.get(worker_id, 0) > 0:
                        self.worker_weekend_counts[worker_id] -= 1

                # Update bridge tracking
                bridge_period = self.date_utils.get_bridge_period_for_date(date, self.bridge_periods)
                if bridge_period:
                    # Check if worker has ANY other assignments in this bridge period
                    has_other_assignment = False
                    for check_date in self._get_dates_in_bridge(bridge_period):
                        if check_date != date and check_date in self.worker_assignments.get(worker_id, set()):
                            has_other_assignment = True
                            break

                    # Only remove bridge count if worker has NO other assignments in this bridge period
                    if not has_other_assignment:
                        if worker_id in self.worker_bridge_counts:
                            self.worker_bridge_counts[worker_id].discard(bridge_period["id"])

            else:  # Adding assignment
                self.worker_assignments[worker_id].add(date)
                self.worker_posts[worker_id].add(post)  # This should now work
                # Update shift count
                self.worker_shift_counts[worker_id] = self.worker_shift_counts.get(worker_id, 0) + 1
                # Update bridge tracking
                bridge_period = self.date_utils.get_bridge_period_for_date(date, self.bridge_periods)
                if bridge_period:
                    if worker_id not in self.worker_bridge_counts:
                        self.worker_bridge_counts[worker_id] = set()
                    self.worker_bridge_counts[worker_id].add(bridge_period["id"])

                weekday = date.weekday()
                self.worker_weekdays[worker_id][weekday] = self.worker_weekdays[worker_id].get(weekday, 0) + 1

                is_special_day = self.date_utils.is_weekend_day(date, self.holidays)

                if is_special_day:
                    current_weekends = self.worker_weekends.setdefault(worker_id, [])  # Ensures list exists
                    if date not in current_weekends:
                        current_weekends.append(date)
                        current_weekends.sort()
                    # Update weekend count
                    self.worker_weekend_counts[worker_id] = self.worker_weekend_counts.get(worker_id, 0) + 1

            # Update eligibility tracker if it exists and is configured
            if hasattr(self, "eligibility_tracker") and self.eligibility_tracker:
                if removing:
                    self.eligibility_tracker.remove_worker_assignment(worker_id, date)
                else:
                    self.eligibility_tracker.update_worker_status(worker_id, date)

            # ENHANCED: Validate synchronization after update
            # This is a critical addition to catch synchronization issues immediately
            if hasattr(self, "_validate_assignment_consistency"):
                if not self._validate_assignment_consistency(worker_id, date, removing):
                    logging.error(
                        f"SYNC ERROR: Data synchronization issue detected after {'removing' if removing else 'adding'} worker {worker_id} on {date.strftime('%Y-%m-%d')}"
                    )
                    # Attempt automatic repair
                    if hasattr(self, "_ensure_data_synchronization"):
                        self._ensure_data_synchronization()

            logging.debug(
                f"{'Removed' if removing else 'Added'} assignment and updated tracking for worker {worker_id} on {date.strftime('%Y-%m-%d')}, post {post}"
            )

        except Exception as e:
            logging.error(
                f"Error in _update_tracking_data for worker {worker_id}, date {date}, post {post}, removing={removing}: {e!s}",
                exc_info=True,
            )
            raise

    def _validate_assignment_consistency(self, worker_id: str, date: datetime, removing: bool = False) -> bool:
        """
        Validate that a specific assignment is consistent between schedule and worker_assignments.

        Args:
            worker_id: ID of the worker
            date: Date of the assignment
            removing: Whether this is checking after a removal operation

        Returns:
            bool: True if consistent, False if inconsistent
        """
        try:
            # Check if worker is in schedule for this date
            is_in_schedule = date in self.schedule and worker_id in self.schedule.get(date, [])

            # Check if worker is in tracking for this date
            is_in_tracking = worker_id in self.worker_assignments and date in self.worker_assignments.get(
                worker_id, set()
            )

            if removing:
                # After removal, worker should not be in either structure
                if is_in_schedule or is_in_tracking:
                    logging.debug(
                        f"Inconsistency after removal: worker {worker_id} still found in {'schedule' if is_in_schedule else 'tracking'} for {date.strftime('%Y-%m-%d')}"
                    )
                    return False
            else:
                # After addition, worker should be in both structures
                if is_in_schedule != is_in_tracking:
                    logging.debug(
                        f"Inconsistency after addition: worker {worker_id} found in {'schedule' if is_in_schedule else 'tracking'} but not {'tracking' if is_in_schedule else 'schedule'} for {date.strftime('%Y-%m-%d')}"
                    )
                    return False

            return True

        except Exception as e:
            logging.error(f"Error validating assignment consistency: {e!s}", exc_info=True)
            return False

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
                            if (
                                existing,
                                date,
                            ) in self.schedule_builder._locked_mandatory or self.schedule_builder._is_mandatory(
                                existing, date
                            ):
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

    def _fix_constraint_violations(self):
        """
        Try to fix constraint violations in the current schedule.
        Returns True if fixed, False if couldn't fix all.
        """
        try:
            violations = self._check_schedule_constraints()
            if not violations:
                return True

            logging.info(f"Attempting to fix {len(violations)} constraint violations")
            fixes_made = 0

            # Fix each violation
            for violation in violations:
                if violation["type"] == "min_rest_days" or violation["type"] == "weekly_pattern":
                    # Fix by unassigning one of the shifts
                    worker_id = violation["worker_id"]
                    date1 = violation["date1"]
                    date2 = violation["date2"]

                    # CRITICAL: Check if either date is mandatory
                    date1_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date1)
                    date2_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date2)

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
                        if hasattr(self, "schedule_builder"):
                            if not self.schedule_builder._can_modify_assignment(
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
                        violation_type = "rest period" if violation["type"] == "min_rest_days" else "weekly pattern"
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
                    worker_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date)
                    incompatible_is_mandatory = self.schedule_builder._is_mandatory(incompatible_id, date)

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
                        if hasattr(self, "schedule_builder"):
                            if not self.schedule_builder._can_modify_assignment(
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
                return False
            else:
                logging.info(f"Successfully fixed all {fixes_made} constraint violations")
                return True

        except Exception as e:
            logging.error(f"Error fixing constraint violations: {e!s}", exc_info=True)
            return False

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
        """
        Calculate the score of the given schedule.

        This is the canonical scoring implementation, used directly by
        SchedulerCore and indirectly by ScheduleBuilder.calculate_score(),
        which delegates here for consistency. Currently scores based on the
        percentage of filled shifts; constraint-violation penalties are not
        yet applied (see commented-out example below).
        """
        logging.debug("Scheduler.calculate_score called")
        score = 0

        current_schedule = schedule_to_score if schedule_to_score is not None else self.schedule
        current_assignments = assignments_to_score if assignments_to_score is not None else self.worker_assignments

        if not current_schedule:
            return float("-inf")  # Or 0, depending on how you want to score empty schedules

        filled_shifts = 0
        total_possible_shifts = 0

        for date, shifts in current_schedule.items():
            total_possible_shifts += len(shifts)
            for worker_id in shifts:
                if worker_id is not None:
                    filled_shifts += 1

        # Basic score: percentage of filled shifts
        if total_possible_shifts > 0:
            score = (filled_shifts / total_possible_shifts) * 100
        else:
            score = 0  # Or float('-inf') if an empty schedule structure is invalid

        # Add penalties for constraint violations (conceptual)
        # if hasattr(self, 'constraint_checker'):
        #     violations = self.constraint_checker.check_all_constraints(current_schedule, current_assignments)
        #     score -= len(violations) * 10 # Example penalty

        # Add bonuses for desired properties (e.g., balanced workload, good post rotation)

        logging.debug(f"Calculated score (placeholder): {score}")
        return score

    def _calculate_coverage(self):
        """Calculate the percentage of shifts that are filled in the schedule."""
        try:
            total_shifts = sum(
                self._get_shifts_for_date(d) for d in self._get_date_range(self.start_date, self.end_date)
            )

            # Count filled shifts (where worker is not None)
            filled_shifts = 0
            for date, shifts in self.schedule.items():
                for worker in shifts:
                    if worker is not None:
                        filled_shifts += 1

            # Debug logs to see what's happening
            logging.info(f"Coverage calculation: {filled_shifts} filled out of {total_shifts} total shifts")
            logging.debug(f"Schedule contains {len(self.schedule)} dates with shifts")

            # Output some sample of the schedule to debug
            sample_size = min(3, len(self.schedule))
            if sample_size > 0:
                sample_dates = list(self.schedule.keys())[:sample_size]
                for date in sample_dates:
                    logging.debug(f"Sample date {date.strftime('%d-%m-%Y')}: {self.schedule[date]}")

            # Calculate percentage
            if total_shifts > 0:
                return (filled_shifts / total_shifts) * 100
            return 0
        except Exception as e:
            logging.error(f"Error calculating coverage: {e!s}", exc_info=True)
            return 0

    def _calculate_post_rotation(self):
        """
        Calculate post rotation metrics.

        Returns:
            dict: Dictionary with post rotation metrics
        """
        try:
            # Get the post rotation data using the existing method
            rotation_data = self._calculate_post_rotation_coverage()

            # If it's already a dictionary with the required keys, use it directly
            if isinstance(rotation_data, dict) and "uniformity" in rotation_data and "avg_worker" in rotation_data:
                return rotation_data

            # Otherwise, create a dictionary with the required structure
            # Use the value from rotation_data if it's a scalar, or fallback to a default
            overall_score = rotation_data if isinstance(rotation_data, (int, float)) else 40.0

            return {
                "overall_score": overall_score,
                "uniformity": 0.0,  # Default value
                "avg_worker": 100.0,  # Default value
            }
        except Exception as e:
            logging.error(f"Error in calculating post rotation: {e!s}")
            # Return a default dictionary with all required keys
            return {"overall_score": 40.0, "uniformity": 0.0, "avg_worker": 100.0}

    def _calculate_post_rotation_coverage(self):
        """
        Calculate post rotation coverage metrics

        Evaluates how well posts are distributed across workers

        Returns:
            dict: Dictionary containing post rotation metrics
        """
        logging.info("Calculating post rotation coverage...")

        # Initialize metrics
        metrics: dict[str, Any] = {"overall_score": 0, "worker_scores": {}, "post_distribution": {}}

        # Count assignments per post
        post_counts = {post: 0 for post in range(self.num_shifts)}
        total_assignments = 0

        for shifts in self.schedule.values():
            for post, worker in enumerate(shifts):
                if worker is not None:
                    post_counts[post] = post_counts.get(post, 0) + 1
                    total_assignments += 1

        # Calculate post distribution stats
        if total_assignments > 0:
            expected_per_post = total_assignments / self.num_shifts
            post_deviation = 0

            for post, count in post_counts.items():
                metrics["post_distribution"][post] = {
                    "count": count,
                    "percentage": (count / total_assignments * 100) if total_assignments > 0 else 0,
                }
                post_deviation += abs(count - expected_per_post)

            # Calculate overall post distribution uniformity (100% = perfect distribution)
            post_uniformity = max(0, 100 - (post_deviation / total_assignments * 100))
        else:
            post_uniformity = 0

        # Calculate individual worker post rotation scores
        worker_scores = {}
        overall_worker_deviation = 0

        for worker in self.workers_data:
            worker_id = worker["id"]
            worker_assignments = len(self.worker_assignments.get(worker_id, []))

            # Skip workers with no or very few assignments
            if worker_assignments < 2:
                worker_scores[worker_id] = 100  # Perfect score for workers with minimal assignments
                continue

            # Get post counts for this worker
            worker_post_counts = {post: 0 for post in range(self.num_shifts)}

            for date, shifts in self.schedule.items():
                for post, assigned_worker in enumerate(shifts):
                    if assigned_worker == worker_id:
                        worker_post_counts[post] = worker_post_counts.get(post, 0) + 1

            # Calculate deviation from ideal distribution
            expected_per_post_for_worker = worker_assignments / self.num_shifts
            worker_deviation = 0

            for post, count in worker_post_counts.items():
                worker_deviation += abs(count - expected_per_post_for_worker)

            # Calculate worker's post rotation score (100% = perfect distribution)
            if worker_assignments > 0:
                worker_score = max(0, 100 - (worker_deviation / worker_assignments * 100))
                normalized_worker_deviation = worker_deviation / worker_assignments
            else:
                worker_score = 100
                normalized_worker_deviation = 0

            worker_scores[worker_id] = worker_score
            overall_worker_deviation += normalized_worker_deviation

        # Calculate overall worker post rotation score
        if len(self.workers_data) > 0:
            avg_worker_score = sum(worker_scores.values()) / len(worker_scores)
        else:
            avg_worker_score = 0

        # Combine post distribution and worker rotation scores
        # Weigh post distribution more heavily (60%) than individual worker scores (40%)
        metrics["overall_score"] = (post_uniformity * 0.6) + (avg_worker_score * 0.4)
        metrics["post_uniformity"] = post_uniformity
        metrics["avg_worker_score"] = avg_worker_score
        metrics["worker_scores"] = worker_scores

        logging.info(f"Post rotation overall score: {metrics['overall_score']:.2f}%")
        logging.info(f"Post uniformity: {post_uniformity:.2f}%, Avg worker score: {avg_worker_score:.2f}%")

        return metrics

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
        """
        Final validator that scans the entire schedule and fixes any constraint violations.
        Returns the number of fixes made.
        """
        logging.info("Running final schedule validation...")

        # Count issues
        incompatibility_issues = 0
        gap_issues = 0
        other_issues = 0
        fixes_made = 0

        # 1. Check for incompatibilities
        for date in sorted(self.schedule.keys()):
            workers_assigned = [w for w in self.schedule.get(date, []) if w is not None]  # Use .get for safety

            # Use indices to safely modify the list while iterating conceptually
            indices_to_check = list(range(len(workers_assigned)))
            processed_pairs = set()  # Avoid redundant checks/fixes if multiple pairs exist

            for i in indices_to_check:
                if i >= len(workers_assigned):
                    continue  # List size might change
                worker1_id = workers_assigned[i]
                if worker1_id is None:
                    continue  # Slot might have been cleared by a previous fix

                for j in range(i + 1, len(workers_assigned)):
                    if j >= len(workers_assigned):
                        continue  # List size might change
                    worker2_id = workers_assigned[j]
                    if worker2_id is None:
                        continue  # Slot might have been cleared

                    pair = tuple(sorted((worker1_id, worker2_id)))
                    if pair in processed_pairs:
                        continue  # Already handled this pair

                    # Check if workers are incompatible using the schedule_builder method
                    if self.schedule_builder._are_workers_incompatible(worker1_id, worker2_id):
                        incompatibility_issues += 1  # Count issue regardless of fix success
                        processed_pairs.add(pair)  # Mark pair as processed
                        logging.warning(
                            f"VALIDATION: Found incompatible workers {worker1_id} and {worker2_id} on {date}"
                        )

                        # CRITICAL: Check if either worker has a mandatory assignment for this date
                        worker1_is_mandatory = self.schedule_builder._is_mandatory(worker1_id, date)
                        worker2_is_mandatory = self.schedule_builder._is_mandatory(worker2_id, date)

                        # If both are mandatory, we have a configuration error - log it but don't remove
                        if worker1_is_mandatory and worker2_is_mandatory:
                            logging.error(
                                f"VALIDATION ERROR: Both workers {worker1_id} and {worker2_id} have mandatory assignments on {date} but are incompatible. This is a configuration error that must be fixed in worker data."
                            )
                            continue  # Skip this fix - cannot remove mandatory assignments

                        # If one is mandatory, remove the other one
                        if worker1_is_mandatory:
                            worker_to_remove = worker2_id
                            logging.info(
                                f"VALIDATION: Worker {worker1_id} has mandatory assignment, will remove {worker2_id}"
                            )
                        elif worker2_is_mandatory:
                            worker_to_remove = worker1_id
                            logging.info(
                                f"VALIDATION: Worker {worker2_id} has mandatory assignment, will remove {worker1_id}"
                            )
                        else:
                            # Neither is mandatory - remove one with more assignments
                            w1_count = len(self.worker_assignments.get(worker1_id, set()))
                            w2_count = len(self.worker_assignments.get(worker2_id, set()))
                            worker_to_remove = worker1_id if w1_count >= w2_count else worker2_id
                        try:
                            # Find the post index IN THE ORIGINAL schedule[date] list
                            post_to_remove = self.schedule[date].index(worker_to_remove)

                            # CRITICAL: Final check - verify we can remove this worker
                            if hasattr(self, "schedule_builder"):
                                if not self.schedule_builder._can_modify_assignment(
                                    worker_to_remove, date, "validation_fix_incompat"
                                ):
                                    logging.error(
                                        f"🔒 BLOCKED: Cannot remove MANDATORY {worker_to_remove} from {date.strftime('%Y-%m-%d')} - This is a configuration error!"
                                    )
                                    continue

                            # Remove the worker from schedule
                            self.schedule[date][post_to_remove] = None

                            # Remove from assignments tracking
                            if worker_to_remove in self.worker_assignments:
                                self.worker_assignments[worker_to_remove].discard(date)  # Use discard

                            # --- ADDED: Update Tracking Data ---
                            self._update_tracking_data(worker_to_remove, date, post_to_remove, removing=True)
                            # --- END ADDED ---

                            fixes_made += 1
                            logging.warning(
                                f"VALIDATION: Removed worker {worker_to_remove} from {date} Post {post_to_remove} to fix incompatibility"
                            )

                            # Update the local workers_assigned list for subsequent checks on the same date
                            if worker_to_remove == worker1_id:
                                workers_assigned[i] = None  # Mark as None in local list
                            else:
                                workers_assigned[j] = None  # Mark as None in local list

                        except ValueError:
                            logging.error(
                                f"VALIDATION FIX ERROR: Worker {worker_to_remove} not found in schedule for {date} during fix."
                            )
                        except Exception as e:
                            logging.error(
                                f"VALIDATION FIX ERROR: Unexpected error removing {worker_to_remove} from {date}: {e}"
                            )

        # 2. Check for minimum gap violations (Ensure this also calls _update_tracking_data)
        for worker_id in list(self.worker_assignments.keys()):  # Iterate over copy of keys
            assignments = sorted(list(self.worker_assignments.get(worker_id, set())))  # Use .get

            indices_to_remove_gap = []  # Store (date, post) to remove after checking all pairs

            for i in range(len(assignments) - 1):
                date1 = assignments[i]
                date2 = assignments[i + 1]
                days_between = (date2 - date1).days

                worker_data = next((w for w in self.workers_data if w["id"] == worker_id), None)
                min_days_between = get_effective_min_gap(worker_data, self.gap_between_shifts)

                if days_between < min_days_between:
                    gap_issues += 1
                    logging.warning(
                        f"VALIDATION: Found gap violation for worker {worker_id}: only {days_between} days between {date1} and {date2}, minimum required: {min_days_between}"
                    )

                    # CRITICAL: Check if either date is a mandatory assignment
                    date1_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date1)
                    date2_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date2)

                    # If both are mandatory, this is a configuration error - log it but don't remove
                    if date1_is_mandatory and date2_is_mandatory:
                        logging.error(
                            f"VALIDATION ERROR: Worker {worker_id} has mandatory assignments on {date1} and {date2} but they violate minimum gap requirement. This is a configuration error that must be fixed in worker data."
                        )
                        continue  # Skip this fix - cannot remove mandatory assignments

                    # If date1 is mandatory, try to remove date2
                    # If date2 is mandatory, try to remove date1
                    # If neither is mandatory, remove the later one (date2)
                    if date2_is_mandatory:
                        date_to_remove = date1
                        logging.info(
                            f"VALIDATION: {date2} is mandatory for worker {worker_id}, will try to remove {date1}"
                        )
                    elif date1_is_mandatory:
                        date_to_remove = date2
                        logging.info(
                            f"VALIDATION: {date1} is mandatory for worker {worker_id}, will try to remove {date2}"
                        )
                    else:
                        date_to_remove = date2  # Default: remove later assignment

                    # Mark the assignment for removal
                    try:
                        # Find post index for date_to_remove
                        if date_to_remove in self.schedule and worker_id in self.schedule[date_to_remove]:
                            post_to_remove_gap = self.schedule[date_to_remove].index(worker_id)
                            indices_to_remove_gap.append((date_to_remove, post_to_remove_gap))
                        else:
                            logging.error(
                                f"VALIDATION FIX ERROR (GAP): Worker {worker_id} assignment for {date_to_remove} not found in schedule."
                            )
                    except ValueError:
                        logging.error(
                            f"VALIDATION FIX ERROR (GAP): Worker {worker_id} not found in schedule list for {date_to_remove}."
                        )

            # Now perform removals for gap violations
            for date_rem, post_rem in indices_to_remove_gap:
                if (
                    date_rem in self.schedule
                    and len(self.schedule[date_rem]) > post_rem
                    and self.schedule[date_rem][post_rem] == worker_id
                ):
                    # CRITICAL: Final verification - never remove mandatory
                    if hasattr(self, "schedule_builder"):
                        if not self.schedule_builder._can_modify_assignment(worker_id, date_rem, "validation_fix_gap"):
                            logging.error(
                                f"🔒 BLOCKED: Cannot remove MANDATORY {worker_id} from {date_rem.strftime('%Y-%m-%d')} - Gap violation cannot be fixed!"
                            )
                            continue

                    self.schedule[date_rem][post_rem] = None
                    self.worker_assignments[worker_id].discard(date_rem)
                    # --- ADDED: Update Tracking Data ---
                    self._update_tracking_data(worker_id, date_rem, post_rem, removing=True)
                    # --- END ADDED ---
                    fixes_made += 1
                    logging.warning(
                        f"VALIDATION: Removed worker {worker_id} from {date_rem} Post {post_rem} to fix gap violation"
                    )
                else:
                    logging.warning(
                        f"VALIDATION FIX SKIP (GAP): State changed, worker {worker_id} no longer at {date_rem} Post {post_rem}."
                    )

        # 3. Run the reconcile method to ensure data consistency
        if self._reconcile_schedule_tracking():
            other_issues += 1

        logging.info(
            f"Final validation complete: Found {incompatibility_issues} incompatibility issues, {gap_issues} gap issues, and {other_issues} other issues. Made {fixes_made} fixes."
        )
        return fixes_made

    def _run_final_validation_and_fix(self):
        """
        Reconcile tracking data, then apply `validate_and_fix_final_schedule`.

        Named distinctly from `DataManager._validate_final_schedule`, which performs
        the detailed per-worker constraint checks and returns error/warning lists.
        Returns True if the process completed without exceptions.
        """
        try:
            # Attempt to reconcile tracking first
            self._reconcile_schedule_tracking()

            # Run the enhanced validation
            fixes_made = self.validate_and_fix_final_schedule()

            if fixes_made > 0:
                logging.info(f"Validation fixed {fixes_made} issues")

            return True
        except Exception as e:
            logging.error(f"Validation error: {e!s}", exc_info=True)
            return False

    # ========================================
    # 9. REPORTING AND EXPORT
    # ========================================
    def export_schedule(self, format="txt"):
        """
        Export the schedule in the specified format

        Args:
            format: Output format ('txt' currently supported)
        Returns:
            str: Name of the generated file
        """
        timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
        filename = f"schedule_{timestamp}.{format}"

        if format == "txt":
            with open(filename, "w", encoding="utf-8") as f:
                # Write header
                f.write("=" * 60 + "\n")
                f.write("HORARIO GENERADO\n")
                f.write("=" * 60 + "\n")
                f.write(f"Fecha de generación: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                f.write(f"Período: {self.start_date.strftime('%d/%m/%Y')} - {self.end_date.strftime('%d/%m/%Y')}\n")
                f.write(f"Trabajadores: {len(self.workers_data)}\n")
                f.write(f"Turnos por día: {self.num_shifts}\n\n")

                # Write schedule body
                current_date = self.start_date
                while current_date <= self.end_date:
                    if current_date in self.schedule:
                        day_name = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"][
                            current_date.weekday()
                        ]
                        f.write(f"{day_name} {current_date.strftime('%d/%m/%Y')}\n")

                        for post_idx, worker_id in enumerate(self.schedule[current_date]):
                            if worker_id:
                                worker_name = next(
                                    (w["name"] for w in self.workers_data if w["id"] == worker_id), worker_id
                                )
                                f.write(f"  Turno {post_idx + 1}: {worker_name} ({worker_id})\n")
                            else:
                                f.write(f"  Turno {post_idx + 1}: [VACANTE]\n")
                        f.write("\n")
                    current_date += timedelta(days=1)

                # Write summary
                f.write("\n" + "=" * 60 + "\n")
                f.write("RESUMEN\n")
                f.write("=" * 60 + "\n")
                for worker in self.workers_data:
                    worker_id = worker["id"]
                    shift_count = len(self.worker_assignments.get(worker_id, []))
                    weekend_count = len(
                        [
                            d
                            for d in self.worker_assignments.get(worker_id, [])
                            if self.date_utils.is_weekend_day(d, self.holidays)
                        ]
                    )
                    f.write(f"{worker['name']} ({worker_id}): {shift_count} turnos, {weekend_count} fines de semana\n")

        logging.info(f"Schedule exported to {filename}")
        return filename

    def export_schedule_json(self, filename=None):
        """
        Export the complete schedule to JSON format

        Args:
            filename: Output filename (optional, auto-generated if not provided)
        Returns:
            str: Name of the generated file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"schedule_complete_{timestamp}.json"

        # Convert schedule to serializable format
        schedule_serializable = {}
        for date, workers in self.schedule.items():
            date_str = date.strftime("%Y-%m-%d")
            schedule_serializable[date_str] = workers

        # Convert worker_assignments to serializable format
        worker_assignments_serializable = {}
        for worker_id, dates in self.worker_assignments.items():
            worker_assignments_serializable[worker_id] = [d.strftime("%Y-%m-%d") for d in sorted(dates)]

        # Build complete data structure
        data = {
            "metadata": {
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "period_start": self.start_date.strftime("%Y-%m-%d"),
                "period_end": self.end_date.strftime("%Y-%m-%d"),
                "total_days": (self.end_date - self.start_date).days + 1,
                "num_shifts_per_day": self.num_shifts,
                "total_workers": len(self.workers_data),
            },
            "schedule": schedule_serializable,
            "worker_assignments": worker_assignments_serializable,
            "workers_data": self.workers_data,
            "config": {
                "start_date": self.start_date.strftime("%Y-%m-%d"),
                "end_date": self.end_date.strftime("%Y-%m-%d"),
                "num_shifts": self.num_shifts,
                "gap_between_shifts": self.gap_between_shifts,
                "max_consecutive_weekends": self.max_consecutive_weekends,
            },
        }

        # Save to file
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logging.info(f"Complete schedule exported to JSON: {filename}")
        return filename

    def generate_worker_report(self, worker_id, save_to_file=False):
        """
        Generate a worker report and optionally save it to a file

        Args:
            worker_id: ID of the worker to generate report for
            save_to_file: Whether to save report to a file (default: False)
        Returns:
            str: The report text
        """
        try:
            report = self.stats.generate_worker_report(worker_id)

            # Optionally save to file
            if save_to_file:
                filename = f"worker_{worker_id}_report.txt"
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(report)
                logging.info(f"Worker report saved to {filename}")

            return report

        except Exception as e:
            logging.error(f"Error generating worker report: {e!s}")
            return f"Error generating report: {e!s}"

    def generate_all_worker_reports(self, output_directory=None):
        """
        Generate reports for all workers

        Args:
            output_directory: Directory to save reports (default: current directory)
        Returns:
            int: Number of reports generated
        """
        count = 0
        for worker in self.workers_data:
            worker_id = worker["id"]
            try:
                report = self.stats.generate_worker_report(worker_id)

                # Create filename
                filename = f"worker_{worker_id}_report.txt"
                if output_directory:
                    import os

                    os.makedirs(output_directory, exist_ok=True)
                    filename = os.path.join(output_directory, filename)

                # Save to file
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(report)

                count += 1
                logging.info(f"Generated report for worker {worker_id}")

            except Exception as e:
                logging.error(f"Failed to generate report for worker {worker_id}: {e!s}")

        logging.info(f"Generated {count} worker reports")
        return count

    def log_schedule_summary(self, title="Schedule Summary"):
        """Helper method to log key statistics about the current schedule state."""
        logging.info(f"--- {title} ---")
        try:
            total_shifts_assigned = sum(len(assignments) for assignments in self.worker_assignments.values())
            logging.info(f"Total shifts assigned: {total_shifts_assigned}")

            empty_shifts = 0
            total_slots = 0
            for date, posts in self.schedule.items():
                total_slots += len(posts)
                empty_shifts += posts.count(None)
            logging.info(f"Total slots: {total_slots}, Empty slots: {empty_shifts}")

            logging.info("Shift Counts per Worker:")
            for worker_id, count in sorted(self.worker_shift_counts.items()):
                logging.info(f"  Worker {worker_id}: {count} shifts")

            logging.info("Weekend Shifts per Worker:")
            for worker_id, count in sorted(self.worker_weekend_counts.items()):
                logging.info(f"  Worker {worker_id}: {count} weekend shifts")

            logging.info("Post Assignments per Worker:")
            for worker_id in sorted(self.worker_posts.keys()):
                posts_set = self.worker_posts[worker_id]
                if posts_set:  # Only log if worker has assignments
                    # Convert set to sorted list for display
                    posts_list = sorted(list(posts_set))

                    # Count how many times each post was worked
                    post_counts = {}
                    for date, shifts in self.schedule.items():
                        for post_idx, assigned_worker in enumerate(shifts):
                            if assigned_worker == worker_id:
                                post_counts[post_idx] = post_counts.get(post_idx, 0) + 1

                    # Display both the posts worked and their counts
                    post_details = []
                    for post in posts_list:
                        count = post_counts.get(post, 0)
                        post_details.append(f"P{post}({count})")

                    logging.info(f"  Worker {worker_id}: {', '.join(post_details)}")

            # Add more stats as needed (e.g., consecutive shifts, score)
            current_score = self.schedule_builder.calculate_score(
                self.schedule, self.worker_assignments
            )  # Assuming calculate_score uses current state
            logging.info(f"Current Schedule Score: {current_score}")

        except Exception as e:
            logging.error(f"Error generating schedule summary: {e}")
        logging.info(f"--- End {title} ---")

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
