from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from saldo27.constraint_checker import ConstraintChecker
from saldo27.data_manager import DataManager
from saldo27.exceptions import ConfigurationError
from saldo27.infrastructure.optional_engines import load_optional_engines
from saldo27.scheduler_config import SchedulerConfig
from saldo27.statistics_calculator import StatisticsCalculator
from saldo27.worker_eligibility import WorkerEligibilityTracker

if TYPE_CHECKING:
    from saldo27.scheduler import Scheduler


class SchedulerInitializer:
    """Owns scheduler configuration and bootstrapping responsibilities."""

    def __init__(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    def initialize(self, config: dict[str, Any]) -> None:
        self.validate_config(config)
        self.apply_config(config)
        self.initialize_incompatibilities()
        self.initialize_tracking_state()
        self.initialize_modules(config)
        self.initialize_targets_and_prior()
        self.log_initialization()

    def validate_config(self, config: dict[str, Any]) -> None:
        scheduler = self.scheduler
        is_valid, error_message = SchedulerConfig.validate_config(config)
        if not is_valid:
            raise ConfigurationError(error_message)

        if not isinstance(config["start_date"], datetime) or not isinstance(config["end_date"], datetime):
            raise ConfigurationError("Start date and end date must be datetime objects")

        if config["start_date"] > config["end_date"]:
            raise ConfigurationError("Start date must be before end date")

        if not config["workers_data"] or not isinstance(config["workers_data"], list):
            raise ConfigurationError("workers_data must be a non-empty list")

        for worker in config["workers_data"]:
            if not isinstance(worker, dict):
                raise ConfigurationError("Each worker must be a dictionary")

            if "id" not in worker:
                raise ConfigurationError("Each worker must have an 'id' field")

            if "work_percentage" in worker:
                try:
                    work_percentage = float(str(worker["work_percentage"]).strip()) if worker["work_percentage"] else 100
                    if work_percentage == 0:
                        worker["work_percentage"] = 100
                    elif work_percentage < 0 or work_percentage > 100:
                        raise ConfigurationError(
                            f"Invalid work percentage for worker {worker['id']}: {work_percentage}"
                        )
                except (ValueError, TypeError):
                    worker["work_percentage"] = 100

            if worker.get("work_periods"):
                try:
                    scheduler.date_utils.parse_date_ranges(worker["work_periods"])
                except ValueError as exc:
                    raise ConfigurationError(f"Invalid work_periods format for worker {worker['id']}: {exc!s}")

            if worker.get("mandatory_days"):
                try:
                    scheduler.date_utils.parse_dates(worker["mandatory_days"])
                except ValueError as exc:
                    raise ConfigurationError(f"Invalid mandatory_days format for worker {worker['id']}: {exc!s}")

            if worker.get("days_off"):
                try:
                    scheduler.date_utils.parse_date_ranges(worker["days_off"])
                except ValueError as exc:
                    raise ConfigurationError(f"Invalid days_off format for worker {worker['id']}: {exc!s}")

        if "holidays" in config:
            if not isinstance(config["holidays"], list):
                raise ConfigurationError("holidays must be a list")

            for holiday in config["holidays"]:
                if not isinstance(holiday, datetime):
                    raise ConfigurationError("Each holiday must be a datetime object")

    def apply_config(self, config: dict[str, Any]) -> None:
        scheduler = self.scheduler
        scheduler.config = config
        scheduler.start_date = config["start_date"]
        scheduler.end_date = config["end_date"]
        scheduler.num_shifts = config["num_shifts"]
        scheduler.variable_shifts = config.get("variable_shifts", [])
        scheduler.workers_data = config["workers_data"]
        scheduler._normalize_worker_ids(scheduler.workers_data)
        scheduler.holidays = config.get("holidays", [])
        scheduler.enable_proportional_weekends = config.get("enable_proportional_weekends", True)
        scheduler.weekend_tolerance = config.get("weekend_tolerance", 1)
        scheduler.bridge_tolerance = config.get("bridge_tolerance", 0.5)

        year = scheduler.start_date.year
        scheduler.bridge_periods = scheduler.date_utils.identify_bridge_periods(scheduler.holidays, year)
        scheduler.worker_bridge_counts = {worker["id"]: set() for worker in scheduler.workers_data}

        default_config = SchedulerConfig.get_default_config()
        scheduler.gap_between_shifts = config.get("gap_between_shifts", default_config["gap_between_shifts"])
        scheduler.max_consecutive_weekends = config.get(
            "max_consecutive_weekends", default_config["max_consecutive_weekends"]
        )

        scheduler.variable_shifts.sort(key=lambda item: item["start_date"])
        scheduler.current_datetime = scheduler.date_utils.get_spain_time()
        scheduler.current_user = "saldo27"

    def initialize_incompatibilities(self) -> None:
        scheduler = self.scheduler
        incompatible_worker_ids = {
            worker["id"] for worker in scheduler.workers_data if worker.get("is_incompatible", False)
        }
        logging.debug(f"Identified incompatible worker IDs (from is_incompatible flag): {incompatible_worker_ids}")

        for worker in scheduler.workers_data:
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

    def initialize_tracking_state(self) -> None:
        self.scheduler._tracking_state.initialize()

    def initialize_modules(self, config: dict[str, Any]) -> None:
        scheduler = self.scheduler
        scheduler.stats = StatisticsCalculator(scheduler)
        scheduler.constraint_checker = ConstraintChecker(scheduler)
        scheduler.data_manager = DataManager(scheduler)
        scheduler.eligibility_tracker = WorkerEligibilityTracker(
            scheduler.workers_data,
            scheduler.holidays,
            scheduler.gap_between_shifts,
            scheduler.max_consecutive_weekends,
            start_date=scheduler.start_date,
            end_date=scheduler.end_date,
            date_utils=scheduler.date_utils,
            scheduler=scheduler,
        )

        loaded_optional = load_optional_engines(scheduler, config)

        scheduler.predictive_optimizer = None
        scheduler.predictive_analytics = loaded_optional.get("predictive_analytics")
        if scheduler.predictive_analytics is not None:
            try:
                from saldo27.predictive_optimizer import PredictiveOptimizer

                scheduler.predictive_optimizer = PredictiveOptimizer(scheduler, scheduler.predictive_analytics)
                predictive_config = config.get("predictive_analytics_config", {})
                if predictive_config.get("auto_collect_data", True):
                    scheduler.predictive_analytics.auto_collect_data_if_enabled()
            except Exception as exc:
                logging.error(f"Error initializing predictive optimizer: {exc}")

    def initialize_targets_and_prior(self) -> None:
        scheduler = self.scheduler
        scheduler._calculate_target_shifts()
        scheduler._base_target_shifts = {
            worker["id"]: float(worker.get("target_shifts", 0)) for worker in scheduler.workers_data
        }
        scheduler.prior_assignments = {}
        scheduler.prior_shift_counts = {}
        scheduler.prior_weekend_counts = {}
        scheduler.prior_target_shifts = {}
        scheduler.prior_last_date = {}

    def log_initialization(self) -> None:
        scheduler = self.scheduler
        logging.info("Scheduler initialized with:")
        logging.info(f"Start date: {scheduler.start_date}")
        logging.info(f"End date: {scheduler.end_date}")
        logging.info(f"Number of shifts: {scheduler.num_shifts}")
        logging.info(f"Number of workers: {len(scheduler.workers_data)}")
        logging.info(f"Holidays: {[holiday.strftime('%d-%m-%Y') for holiday in scheduler.holidays]}")
        logging.info(f"Gap between shifts: {scheduler.gap_between_shifts}")
        logging.info(f"Max consecutive weekend/holiday shifts: {scheduler.max_consecutive_weekends}")
        logging.info(f"Current datetime (Spain): {scheduler.current_datetime}")
        logging.info(f"Current user: {scheduler.current_user}")
