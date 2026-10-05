# Imports
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from saldo27.bridge_manager import BridgeManager

if TYPE_CHECKING:
    from datetime import datetime

    from saldo27.scheduler import Scheduler


class DataManager:
    """Enhanced data management and tracking with performance optimizations"""

    def __init__(self, scheduler: Scheduler):
        """
        Initialize the data manager with caching support

        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler

        # Note: schedule and worker_assignments are exposed as read-only properties
        # (see below) so they always delegate to self.scheduler and never go stale
        # after a deepcopy-reset cycle.
        self.worker_posts = scheduler.worker_posts
        self.worker_weekdays = scheduler.worker_weekdays
        self.worker_weekends = scheduler.worker_weekends
        self.num_shifts = scheduler.num_shifts
        self.workers_data = scheduler.workers_data
        self.holidays = scheduler.holidays

        # Performance optimization caches
        self._holiday_set: set[datetime] = set(self.holidays)
        self._worker_cache: dict[str, dict[str, Any]] = {}

        # Flag to track if data integrity has been verified
        self.data_integrity_verified = False

        # Initialize monthly targets structure
        self.monthly_targets = {}

        # Initialize bridge manager
        self.bridge_manager = BridgeManager()

        self._build_worker_cache()

        logging.info("Enhanced DataManager initialized with caching and bridge management")

    @property
    def schedule(self) -> dict:
        """Always delegates to the current scheduler.schedule (never stale)."""
        return self.scheduler.schedule

    @property
    def worker_assignments(self) -> dict:
        """Always delegates to the current scheduler.worker_assignments (never stale)."""
        return self.scheduler.worker_assignments

    def _build_worker_cache(self) -> None:
        """Build worker cache for faster lookups"""
        for worker in self.workers_data:
            worker_id = worker["id"]
            self._worker_cache[worker_id] = {
                "data": worker,
                "target_shifts": worker.get("target_shifts", 0),
                "work_percentage": worker.get("work_percentage", 100),
            }

    def ensure_data_integrity(self):
        """Check and fix data integrity between scheduler data structures"""
        if self.data_integrity_verified:
            return

        # Verify worker assignments match schedule
        self._verify_assignment_consistency()

        # Mark data as verified
        self.data_integrity_verified = True

    def _get_effective_weekday(self, date):
        """
        Get the effective weekday for a date.
        Holidays are treated as weekends (specifically Sunday).

        Args:
            date: Date to check

        Returns:
            int: Weekday index (0-6, where 0=Monday, 6=Sunday)
        """
        # If date is a holiday, treat it as Sunday (6)
        if date in self.scheduler.holidays:
            return 6

        # Otherwise return the actual weekday
        return self.scheduler.date_utils.get_effective_weekday(date, self.scheduler.holidays)

    def _is_weekend_day(self, date):
        """
        Check if a date is a weekend day or holiday

        Args:
            date: Date to check

        Returns:
            bool: True if weekend or holiday, False otherwise
        """
        return self.scheduler.date_utils.is_weekend_day(date, self.scheduler.holidays)

    def _get_weekend_start(self, date):
        """
        Get the first day of the weekend containing this date

        Args:
            date: Date within the weekend

        Returns:
            datetime: First day of the weekend
        """
        return self.scheduler.date_utils.get_weekend_start(date, self.scheduler.holidays)

    def _is_holiday(self, date):
        """
        Check if a date is a holiday

        Args:
            date: Date to check

        Returns:
            bool: True if the date is a holiday, False otherwise
        """
        return self.scheduler.date_utils.is_holiday(date, self.scheduler.holidays)

    def _ensure_data_integrity(self):
        """
        Ensure all data structures are consistent before schedule operations.

        Delegates to the canonical implementation in Scheduler, which correctly
        pads each schedule entry with `[None] * expected_shifts` instead of an
        empty list, so post-index lookups stay valid.
        """
        return self.scheduler._ensure_data_integrity()

    def mark_data_dirty(self):
        """Mark that data integrity needs to be verified again"""
        self.data_integrity_verified = False

    def _verify_assignment_consistency(self):
        """
        Verify that worker_assignments and schedule are consistent with each other
        and fix any inconsistencies found.

        When a ScheduleBuilder is active (i.e. during schedule generation), delegate
        to its implementation, which additionally protects mandatory assignments from
        being removed. Otherwise (e.g. right after loading data, before generation
        starts) fall back to this generic reconciliation.
        """
        # scheduler.schedule_builder is None until SchedulerCore starts generation.
        # Use getattr so lightweight test schedulers without the attribute still work.
        schedule_builder = getattr(self.scheduler, "schedule_builder", None)
        if schedule_builder is not None:
            return schedule_builder._verify_assignment_consistency()

        # Ensure data consistency before proceeding
        self._ensure_data_integrity()
        # Check each worker's assignments
        for worker_id, dates in self.worker_assignments.items():
            dates_to_remove = []
            for date in list(dates):  # Create a copy to avoid modification during iteration
                # Check if date exists in schedule
                if date not in self.schedule:
                    dates_to_remove.append(date)
                    continue

                # Check if worker is actually in the schedule for this date
                if worker_id not in self.schedule[date]:
                    dates_to_remove.append(date)
                    continue

            # Remove inconsistent assignments
            for date in dates_to_remove:
                self.worker_assignments[worker_id].discard(date)
                logging.warning(f"Fixed inconsistency: Removed date {date} from worker {worker_id}'s assignments")

        # Check schedule for workers not in worker_assignments
        for date, workers in self.schedule.items():
            for post, worker_id in enumerate(workers):
                if worker_id is not None and date not in self.worker_assignments.get(worker_id, set()):
                    # Add missing assignment
                    self.worker_assignments[worker_id].add(date)
                    logging.warning(f"Fixed inconsistency: Added date {date} to worker {worker_id}'s assignments")

    def _update_worker_stats(self, worker_id, date, removing=False):
        """
        Update worker statistics for assignment or removal

        Args:
            worker_id: The worker's ID
            date: Date of assignment
            removing: Boolean indicating if this is a removal operation
        """
        effective_weekday = self._get_effective_weekday(date)

        if removing:
            # Decrease weekday count
            self.worker_weekdays[worker_id][effective_weekday] = max(
                0, self.worker_weekdays[worker_id][effective_weekday] - 1
            )

            # Remove weekend if applicable
            if self._is_weekend_day(date):
                weekend_start = self._get_weekend_start(date)
                if weekend_start in self.worker_weekends[worker_id]:
                    self.worker_weekends[worker_id].remove(weekend_start)
        else:
            # Increase weekday count
            self.worker_weekdays[worker_id][effective_weekday] += 1

            # Add weekend if applicable
            if self._is_weekend_day(date):
                weekend_start = self._get_weekend_start(date)
                if weekend_start not in self.worker_weekends[worker_id]:
                    self.worker_weekends[worker_id].append(weekend_start)

    def clear_caches(self) -> None:
        """Clear all caches when data changes"""
        self._worker_cache.clear()
        self._build_worker_cache()
        logging.debug("DataManager caches cleared and rebuilt")
