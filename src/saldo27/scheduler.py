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
from saldo27.utilities import DateTimeUtils, is_fixed_cadence, worker_flag_enabled

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from saldo27.application.contracts import GenerationProgressEvent
    from saldo27.constraint_checker import ConstraintChecker
    from saldo27.data_manager import DataManager
    from saldo27.schedule_builder import ScheduleBuilder
    from saldo27.scheduler_core import SchedulerCore
    from saldo27.statistics_calculator import StatisticsCalculator

# Initialize logging using the configuration module
setup_logging()

# NOTE: SchedulerError is imported from exceptions.py — do NOT redefine it here
# to avoid class identity mismatches with other modules that import from exceptions.py.


class Scheduler:
    """Main Scheduler class that coordinates all scheduling operations"""

    # Services (set in __init__)
    date_utils: DateTimeUtils
    _tracking_state: SchedulerTrackingState
    _initializer: SchedulerInitializer
    _validation_service: SchedulerValidationService
    _reporting_service: SchedulerReportingService
    _cancelled: bool
    _progress_callback: Callable[[GenerationProgressEvent], None] | None
    _phase_trace: list[Any]
    _cache: dict[str, Any]
    _cache_enabled: bool
    _scheduler_core: SchedulerCore | None
    _weekend_slot_ratio_cache: float | None
    _availability_slot_cache: dict[tuple[Any, ...], tuple[int, int, int, int]]
    _weekend_objectives_cache: tuple[tuple[Any, ...], dict[str, float]] | None

    # Configuration (set by SchedulerInitializer.apply_config)
    config: dict[str, Any]
    start_date: datetime
    end_date: datetime
    num_shifts: int
    variable_shifts: list[dict[str, Any]]
    workers_data: list[dict[str, Any]]
    holidays: list[datetime]
    enable_proportional_weekends: bool
    weekend_tolerance: int | float
    bridge_tolerance: float
    bridge_periods: list[dict[str, Any]]
    worker_bridge_counts: dict[str, set[datetime]]
    gap_between_shifts: int
    gap_periods: list[dict[str, Any]]
    max_consecutive_weekends: int
    current_datetime: datetime
    current_user: str

    # Core modules (set by SchedulerInitializer.initialize_modules)
    stats: StatisticsCalculator
    constraint_checker: ConstraintChecker
    data_manager: DataManager

    # Optional engines (None when disabled)
    real_time_engine: Any | None
    predictive_analytics: Any | None
    predictive_optimizer: Any | None

    # Mutable schedule state (set by SchedulerTrackingState.initialize)
    schedule: dict[datetime, list[str | None]]
    schedule_builder: ScheduleBuilder | None
    worker_assignments: dict[str, set[datetime]]
    worker_posts: dict[str, set[int]]
    worker_weekdays: dict[str, dict[int, int]]
    worker_weekends: dict[str, list[datetime]]
    worker_shift_counts: dict[str, int]
    worker_weekend_counts: dict[str, int]
    worker_post_counts: dict[str, dict[int, int]]
    worker_weekday_counts: dict[str, dict[int, int]]
    worker_holiday_counts: dict[str, int]
    last_assignment_date: dict[str, datetime | None]
    consecutive_shifts: dict[str, int]
    max_shifts_per_worker: int
    constraint_skips: dict[str, dict[str, list[Any]]]

    # Prior-period constraints and base targets
    _base_target_shifts: dict[str, float]
    prior_assignments: dict[str, set[datetime]]
    prior_shift_counts: dict[str, int]
    prior_weekend_counts: dict[str, int]
    prior_target_shifts: dict[str, Any]
    prior_last_date: dict[str, datetime | None]

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
        self._scheduler_core = None

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

    def last_post_index(self, date) -> int:
        """Index of the last post (Rosell) on ``date``: the last slot of that day."""
        day_slots = self.schedule.get(date)
        return (len(day_slots) if day_slots else self._get_shifts_for_date(date)) - 1

    def is_last_post(self, date, post: int) -> bool:
        """True when ``post`` is the last slot of ``date`` (variable_shifts aware)."""
        return post == self.last_post_index(date)

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
        Calculate the objective (target) number of bridge shifts for a worker.

        The weight is the worker's full shift objective (``_raw_target``, which
        includes mandatory shifts) times the share of their own available slots
        that fall on bridge days, so a worker on leave during a bridge is not
        handed a bridge deficit. A fixed-cadence worker's objective is the
        bridge days of their cadence; the other workers share what is left.

        Formula: remaining_bridge_shifts * (worker_weight / sum_non_cadence_weights)

        Args:
            worker_id: ID of the worker

        Returns:
            Target number of bridge shifts (float)
        """
        worker = next((w for w in self.workers_data if w["id"] == worker_id), None)
        if not worker:
            return 0.0

        # Fixed-cadence workers keep exactly their cadence bridge days; the rest
        # share the remaining bridge shifts.
        if is_fixed_cadence(worker):
            bridge_dates = self._scheduled_bridge_dates()
            return float(sum(1 for d in self._fixed_cadence_dates(worker) if d in bridge_dates))

        objectives = {w["id"]: self._shift_objective(w) for w in self.workers_data if not is_fixed_cadence(w)}
        return self.bridge_objectives_for(objectives).get(worker_id, 0.0)

    def bridge_objectives_for(self, objectives: Mapping[str, float]) -> dict[str, float]:
        """Bridge objectives for the non-cadence workers in ``objectives`` (worker id → shift objective).

        The filled bridge shifts left after the cadence workers' bridge days are
        shared by shift objective × the share of the worker's own available slots
        that fall on bridge days.
        """
        return self._filled_day_objectives_for(objectives, self._scheduled_bridge_dates(), share_index=1)

    def _filled_day_objectives_for(
        self, objectives: Mapping[str, float], dates: set[datetime], share_index: int, filled_only: bool = True
    ) -> dict[str, float]:
        """Share the filled slots on ``dates`` (minus cadence workers' own) among ``objectives``.

        Weight = shift objective × the worker's ``_availability_shares()[share_index]``.
        With ``filled_only=False`` every slot counts, which is what a schedule still
        being built will end up sharing.
        """
        shares = self._availability_shares()
        weights = {wid: objective * shares[wid][share_index] for wid, objective in objectives.items() if wid in shares}
        total_weight = sum(weights.values())
        if total_weight <= 0:
            return {wid: 0.0 for wid in weights}

        # Count only filled (non-None) slots to avoid inflating targets
        total_shifts = sum(
            sum(1 for w in self.schedule[d] if w is not None) if filled_only else len(self.schedule[d]) for d in dates
        )
        total_shifts -= sum(
            sum(1 for d in self._fixed_cadence_dates(w) if d in dates) for w in self.workers_data if is_fixed_cadence(w)
        )
        return {wid: max(0, total_shifts) * weight / total_weight for wid, weight in weights.items()}

    def scheduled_holiday_dates(self) -> set[datetime]:
        """Configured holidays (festivos) inside the period."""
        return {date for date in self.holidays or () if date in self.schedule}

    def count_holidays_for_worker(self, worker_id: str) -> int:
        """Holiday (festivo) shifts assigned to ``worker_id``."""
        holidays = self.scheduled_holiday_dates()
        return sum(1 for d in self.worker_assignments.get(worker_id, ()) if d in holidays)

    def get_holiday_objective(self, worker: dict, filled_only: bool = True) -> float:
        """Objective holiday (festivo) shifts for ``worker``.

        Holidays already count as weekend days; this separate objective keeps them
        from piling up on the same workers inside an otherwise balanced weekend
        count. Same sharing rule as bridges (see ``holiday_objectives_for``).
        """
        holidays = self.scheduled_holiday_dates()
        if is_fixed_cadence(worker):
            return float(sum(1 for d in self._fixed_cadence_dates(worker) if d in holidays))
        objectives = {w["id"]: self._shift_objective(w) for w in self.workers_data if not is_fixed_cadence(w)}
        return self.holiday_objectives_for(objectives, filled_only).get(str(worker.get("id")), 0.0)

    def holiday_objectives_for(self, objectives: Mapping[str, float], filled_only: bool = True) -> dict[str, float]:
        """Holiday objectives for the non-cadence workers in ``objectives`` (worker id → shift objective).

        The filled holiday shifts left after the cadence workers' holidays are
        shared by shift objective × the share of the worker's own available slots
        that fall on holidays.
        """
        return self._filled_day_objectives_for(
            objectives, self.scheduled_holiday_dates(), share_index=2, filled_only=filled_only
        )

    def _scheduled_bridge_dates(self) -> set[datetime]:
        return {
            date
            for bridge_period in self.bridge_periods
            for date in self._get_dates_in_bridge(bridge_period)
            if date in self.schedule
        }

    def get_weekend_objective(self, worker: dict) -> float:
        """Objective weekend shifts for ``worker`` (Fri-Sun, holidays and pre-holidays).

        See ``weekend_objectives_for``. A worker outside ``workers_data`` gets their
        shift objective times the period weekend ratio.

        A fixed-cadence worker's objective is the weekend days of their cadence, so
        weekend balancing never sees them as over or under target."""
        if is_fixed_cadence(worker):
            return float(
                sum(1 for d in self._fixed_cadence_dates(worker) if self.date_utils.is_weekend_day(d, self.holidays))
            )
        objectives = self._balanced_weekend_objectives()
        worker_id = worker.get("id")
        if worker_id not in objectives:
            return self._shift_objective(worker) * self._weekend_slot_ratio()
        return objectives[worker_id]

    def weekend_objectives_for(self, objectives: Mapping[str, float]) -> dict[str, float]:
        """Weekend objectives for the non-cadence workers in ``objectives`` (worker id → shift objective).

        Each shift objective is weighted by the share of the worker's *own*
        available slots that fall on weekend days, then rescaled so together they
        still cover objectives × period weekend ratio. Someone on leave every
        weekend therefore gets a lower objective and the others absorb it, instead
        of carrying an unreachable deficit. With uniform availability this equals
        objective × period weekend ratio.
        """
        shares = self._availability_shares()
        weights = {wid: objective * shares[wid][0] for wid, objective in objectives.items() if wid in shares}
        total_weight = sum(weights.values())
        if total_weight <= 0:
            return {wid: 0.0 for wid in weights}
        nominal = sum(objectives[wid] for wid in weights) * self._weekend_slot_ratio()
        return {wid: weight * nominal / total_weight for wid, weight in weights.items()}

    def _balanced_weekend_objectives(self) -> dict[str, float]:
        """``weekend_objectives_for`` on the workers' own shift objectives.

        Called from the per-candidate scoring loops, so it is cached on the worker
        fields it depends on.
        """
        fingerprint = tuple(
            (
                w.get("id"),
                w.get("_raw_target"),
                w.get("target_shifts"),
                w.get("work_periods"),
                w.get("days_off"),
                bool(w.get("has_cadence")),
            )
            for w in self.workers_data
        )
        cached = getattr(self, "_weekend_objectives_cache", None)
        if cached is not None and cached[0] == fingerprint:
            return cached[1]
        objectives = {w["id"]: self._shift_objective(w) for w in self.workers_data if not is_fixed_cadence(w)}
        result = self.weekend_objectives_for(objectives)
        self._weekend_objectives_cache = (fingerprint, result)
        return result

    def _availability_shares(self) -> dict[str, tuple[float, float, float]]:
        """(weekend, bridge, holiday) share of each non-cadence worker's available slots.

        Availability follows ``work_periods`` and ``days_off``, the same definition
        used for shift targets. A worker with no available slot gets the period-wide
        shares so they never weigh as zero.
        """
        shares: dict[str, tuple[float, float, float]] = {}
        for w in self.workers_data:
            if is_fixed_cadence(w):
                continue
            available, weekend, bridge, holiday = self._availability_slot_counts(w)
            if available <= 0:
                available, weekend, bridge, holiday = self._availability_slot_counts({})
            shares[w["id"]] = (
                (weekend / available, bridge / available, holiday / available) if available else (0.0, 0.0, 0.0)
            )
        return shares

    def _availability_slot_counts(self, worker: dict) -> tuple[int, int, int, int]:
        """(available, weekend, bridge, holiday) slots over the period for ``worker``'s availability."""
        bridges = tuple((bp["start_date"], bp["end_date"]) for bp in (self.bridge_periods or ()))
        key = (worker.get("work_periods") or "", worker.get("days_off") or "", bridges)
        cache = getattr(self, "_availability_slot_cache", None)
        if cache is None:
            cache = self._availability_slot_cache = {}
        if key in cache:
            return cache[key]

        from saldo27.target_calculator import TargetCalculator

        work_ranges, off_ranges = TargetCalculator(self)._worker_availability_ranges(worker)
        holidays = set(self.holidays or ())
        available = weekend = bridge = holiday = 0
        for day in self._get_date_range(self.start_date, self.end_date):
            if not any(rs <= day <= re for rs, re in work_ranges) or any(rs <= day <= re for rs, re in off_ranges):
                continue
            slots = self._get_shifts_for_date(day)
            available += slots
            if self.date_utils.is_weekend_day(day, self.holidays):
                weekend += slots
            if self.date_utils.is_bridge_day(day, self.bridge_periods):
                bridge += slots
            if day in holidays:
                holiday += slots
        cache[key] = (available, weekend, bridge, holiday)
        return cache[key]

    def _fixed_cadence_dates(self, worker: dict) -> list[datetime]:
        try:
            dates = self.date_utils.parse_dates(worker.get("mandatory_days", ""))
        except (TypeError, ValueError):
            return []
        return [d for d in dates if self.start_date <= d <= self.end_date]

    def _weekend_slot_ratio(self) -> float:
        ratio = getattr(self, "_weekend_slot_ratio_cache", None)  # period and holidays are fixed after init
        if ratio is None:
            ratio = self.weekend_slot_ratio_for(self._get_date_range(self.start_date, self.end_date))
            self._weekend_slot_ratio_cache = ratio
        return ratio

    def weekend_slot_ratio_for(self, dates) -> float:
        """Share of the slots on ``dates`` that fall on weekend/holiday days."""
        total_slots = weekend_slots = 0
        for d in dates:
            n = self._get_shifts_for_date(d)
            total_slots += n
            if self.date_utils.is_weekend_day(d, self.holidays):
                weekend_slots += n
        return weekend_slots / total_slots if total_slots else 0.0

    def get_last_post_objective(self, worker: dict) -> float:
        """Objective number of last-post (Rosell) shifts for ``worker``.

        ``no_last_post`` workers get 0 and ``only_last_post`` workers get their
        whole shift objective (100%). Every other worker shares the Rosell days
        left after the ``only_last_post`` workers, proportional to their
        objective. A fixed-cadence worker is outside Rosell balancing: their
        objective is the Rosell shifts they currently hold.
        """
        if worker_flag_enabled(worker, "no_last_post"):
            return 0.0
        if is_fixed_cadence(worker) and not worker_flag_enabled(worker, "only_last_post"):
            return float(self._current_last_post_count(worker["id"]))
        objective = self._shift_objective(worker)
        if worker_flag_enabled(worker, "only_last_post"):
            return float(objective)

        eligible_total = sum(
            self._shift_objective(w)
            for w in self.workers_data
            if not worker_flag_enabled(w, "no_last_post")
            and not worker_flag_enabled(w, "only_last_post")
            and not is_fixed_cadence(w)
        )
        if eligible_total <= 0:
            return 0.0
        rosell_days = sum(
            1 for d in self._get_date_range(self.start_date, self.end_date) if self._get_shifts_for_date(d) > 0
        )
        reserved = sum(
            self._shift_objective(w)
            for w in self.workers_data
            if worker_flag_enabled(w, "only_last_post") and not worker_flag_enabled(w, "no_last_post")
        )
        reserved += sum(
            self._current_last_post_count(w["id"])
            for w in self.workers_data
            if is_fixed_cadence(w)
            and not worker_flag_enabled(w, "no_last_post")
            and not worker_flag_enabled(w, "only_last_post")
        )
        return max(0, rosell_days - reserved) * objective / eligible_total

    def _current_last_post_count(self, worker_id: str) -> int:
        return sum(1 for posts in self.schedule.values() if posts and posts[-1] == worker_id)

    @staticmethod
    def _shift_objective(worker: dict) -> int:
        """Full shift count this worker should work, including mandatory and cadence.

        ``target_shifts`` excludes those fixed shifts. A cadence worker keeps
        ``target_shifts`` at 0 and stores the cadence length in ``_raw_target``.
        """
        raw = worker.get("_raw_target")
        if raw is not None:
            try:
                return int(raw)
            except (TypeError, ValueError):
                pass
        try:
            return int(worker.get("target_shifts", 0) or 0)
        except (TypeError, ValueError):
            return 0

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

    def _config_forced_assignment(self, worker_id: str, date: datetime) -> bool:
        """True when ``date`` is a mandatory day or a cadence date for ``worker_id``."""
        builder = self.schedule_builder if hasattr(self, "schedule_builder") else None
        is_mandatory = getattr(builder, "is_mandatory", None)
        if callable(is_mandatory) and bool(is_mandatory(worker_id, date)):
            return True
        from saldo27.constraint_checker import config_forced_dates

        worker = next((item for item in self.workers_data if item["id"] == worker_id), None)
        return date in config_forced_dates(worker, self.date_utils)

    def _worker_cfg(self, worker_id: str) -> dict[str, Any] | None:
        return next((item for item in self.workers_data if item["id"] == worker_id), None)

    def _record_post_move(self, worker_id: str | None, old_post: int, new_post: int) -> None:
        """Point ``worker_posts`` at the post the worker holds after a same-day swap."""
        if not worker_id:
            return
        posts = self.worker_posts.get(worker_id)
        if not isinstance(posts, set):
            posts = set()
            self.worker_posts[worker_id] = posts
        posts.add(new_post)
        if old_post == new_post or old_post not in posts:
            return
        for check_date in self.worker_assignments.get(worker_id, ()):
            if self._get_worker_assigned_to_post(check_date, old_post) == worker_id:
                return
        posts.discard(old_post)

    def _swap_posts_same_day(self, date: datetime, post_a: int, post_b: int) -> None:
        """Swap two workers on the same date and keep post tracking consistent."""
        slots = self.schedule[date]
        worker_a = slots[post_a]
        worker_b = slots[post_b]
        slots[post_a], slots[post_b] = worker_b, worker_a
        self._record_post_move(worker_a, post_a, post_b)
        self._record_post_move(worker_b, post_b, post_a)

    def _can_move_worker_to_last_post(self, worker_id: str, date: datetime) -> bool:
        """Whether ``worker_id`` may take Rosell on ``date`` without new hard violations."""
        cfg = self._worker_cfg(worker_id)
        if cfg is None:
            return False
        if cfg.get("no_last_post", False):
            return False
        checker = getattr(self, "constraint_checker", None)
        if checker is not None and checker._would_exceed_consecutive_last_post(worker_id, date, True):
            return False
        return True

    def _try_swap_off_last_post(self, worker_id: str, date: datetime, last_post: int) -> bool:
        """Move ``worker_id`` off Rosell via a same-day post swap. Returns True on success."""
        slots = self.schedule.get(date)
        if not slots or slots[last_post] != worker_id:
            return False
        holder_cfg = self._worker_cfg(worker_id)
        if holder_cfg and holder_cfg.get("only_last_post", False):
            return False

        for other_post, partner_id in enumerate(slots):
            if other_post == last_post or partner_id is None or partner_id == worker_id:
                continue
            partner_cfg = self._worker_cfg(partner_id)
            if partner_cfg and partner_cfg.get("only_last_post", False):
                continue
            if not self._can_move_worker_to_last_post(partner_id, date):
                continue
            self._swap_posts_same_day(date, last_post, other_post)
            logging.info(
                f"Fixed consecutive last-post: swapped {worker_id} off Rosell with "
                f"{partner_id} on {date.strftime('%Y-%m-%d')} (posts {last_post + 1}↔{other_post + 1})"
            )
            return True
        return False

    def _try_refill_empty_post(self, date: datetime, post: int, exclude_worker_id: str | None = None) -> bool:
        """Fill an empty post using schedule-builder scoring. Returns True if filled."""
        slots = self.schedule.get(date)
        if slots is None or slots[post] is not None:
            return False

        builder = getattr(self, "schedule_builder", None)
        if builder is None or not hasattr(builder, "_calculate_worker_score"):
            return False

        ranked: list[tuple[float, str]] = []
        for worker_data in self.workers_data:
            candidate_id = worker_data["id"]
            if candidate_id == exclude_worker_id:
                continue
            if date in self.worker_assignments.get(candidate_id, ()):
                continue
            score = builder._calculate_worker_score(worker_data, date, post, relaxation_level=1)
            if score == float("-inf"):
                continue
            ranked.append((score, candidate_id))

        if not ranked:
            # Last attempt: slightly more permissive scoring before leaving a hole.
            for worker_data in self.workers_data:
                candidate_id = worker_data["id"]
                if candidate_id == exclude_worker_id:
                    continue
                if date in self.worker_assignments.get(candidate_id, ()):
                    continue
                score = builder._calculate_worker_score(worker_data, date, post, relaxation_level=2)
                if score == float("-inf"):
                    continue
                ranked.append((score, candidate_id))

        if not ranked:
            return False

        ranked.sort(key=lambda item: item[0], reverse=True)
        best_id = ranked[0][1]
        slots[post] = best_id
        self._update_tracking_data(best_id, date, post, removing=False)
        logging.info(
            f"Refilled empty post {post + 1} on {date.strftime('%Y-%m-%d')} with {best_id} "
            f"after consecutive-Rosell repair"
        )
        return True

    def _repair_consecutive_last_post_on_date(self, worker_id: str, date: datetime) -> bool:
        """Repair a consecutive-Rosell hit on ``date`` without leaving a hole when possible.

        Order: same-day post swap → unassign+refill → unassign only (last resort).
        """
        slots = self.schedule.get(date)
        if not slots:
            return False
        last_post = len(slots) - 1
        if slots[last_post] != worker_id:
            # Worker may sit on a non-last post for this date in the run window; nothing to do.
            return False

        if self._try_swap_off_last_post(worker_id, date, last_post):
            return True

        # Swap failed — unassign then try to refill so coverage does not drop.
        slots[last_post] = None
        self.worker_assignments[worker_id].discard(date)
        self._update_tracking_data(worker_id, date, last_post, removing=True)
        refilled = self._try_refill_empty_post(date, last_post, exclude_worker_id=worker_id)
        if refilled:
            logging.info(
                f"Fixed consecutive last-post violation: replaced {worker_id} on {date.strftime('%Y-%m-%d')} Rosell"
            )
        else:
            logging.warning(
                f"Fixed consecutive last-post by unassigning {worker_id} from "
                f"{date.strftime('%Y-%m-%d')} but could not refill the empty Rosell slot"
            )
        return True

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

                    # Cadence, or both dates mandatory: leave the clash and do not score it.
                    # A single mandatory date keeps its day; the other day is removed.
                    worker = next((item for item in self.workers_data if item["id"] == worker_id), None)
                    forced1 = self._config_forced_assignment(worker_id, date1)
                    forced2 = self._config_forced_assignment(worker_id, date2)
                    if is_fixed_cadence(worker) or (forced1 and forced2):
                        logging.info(
                            f"Leaving {violation['type']} for {worker_id} between {date1} and {date2}: "
                            "cadence or two mandatory dates; not repaired and not scored."
                        )
                        continue

                    if forced2:
                        date_to_unassign = date1
                    else:
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
                    # Prefer a same-day post swap so coverage stays intact. Only
                    # unassign as a last resort, and then refill the emptied slot.
                    worker_id = violation["worker_id"]
                    date1 = violation["date1"]
                    date2 = violation["date2"]

                    assigned_in_run = [
                        day
                        for day in self.worker_assignments.get(worker_id, ())
                        if date1 is not None and date2 is not None and date1 <= day <= date2
                    ]
                    worker = next((item for item in self.workers_data if item["id"] == worker_id), None)
                    forced_in_run = sum(self._config_forced_assignment(worker_id, day) for day in assigned_in_run)
                    if is_fixed_cadence(worker) or forced_in_run >= 2:
                        logging.info(
                            f"Leaving consecutive last-post for {worker_id} between {date1} and {date2}: "
                            "cadence or two mandatory dates; not repaired and not scored."
                        )
                        continue

                    candidates = [d for d in (date2, date1) if d is not None]
                    fixed = False
                    for date_to_fix in candidates:
                        if schedule_builder and not schedule_builder._can_modify_assignment(
                            worker_id,
                            date_to_fix,
                            "fix_consecutive_last_post",
                            enforce_monthly_target_floor=False,
                        ):
                            continue
                        if self._repair_consecutive_last_post_on_date(worker_id, date_to_fix):
                            fixes_made += 1
                            fixed = True
                            break

                    if not fixed:
                        logging.warning(
                            f"🔒 BLOCKED: Cannot fix consecutive last-post violation for MANDATORY worker {worker_id} "
                            f"between {date1} and {date2}"
                        )

                elif violation["type"] == "outside_cadence":
                    # Cadence workers must only keep their cadence dates: drop any
                    # extra assignment outside the pattern (never mandatory).
                    worker_id = violation["worker_id"]
                    date_to_unassign = violation["date"]

                    shift_num = None
                    if date_to_unassign in self.schedule:
                        for i, worker in enumerate(self.schedule[date_to_unassign]):
                            if worker == worker_id:
                                shift_num = i
                                break

                    if shift_num is not None:
                        if schedule_builder and not schedule_builder._can_modify_assignment(
                            worker_id,
                            date_to_unassign,
                            "fix_outside_cadence",
                            enforce_monthly_target_floor=False,
                        ):
                            logging.warning(
                                f"🔒 BLOCKED: Cannot unassign {worker_id} from {date_to_unassign} "
                                f"(unexpected lock on outside-cadence date)"
                            )
                            continue

                        self.schedule[date_to_unassign][shift_num] = None
                        self.worker_assignments[worker_id].discard(date_to_unassign)
                        self._update_tracking_data(worker_id, date_to_unassign, shift_num, removing=True)
                        logging.info(
                            f"Fixed outside-cadence violation: Unassigned worker {worker_id} from {date_to_unassign}"
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

    _RT_DISABLED: ClassVar[dict[str, Any]] = {
        "success": False,
        "message": "Real-time features not enabled",
        "error": "REAL_TIME_DISABLED",
    }

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
