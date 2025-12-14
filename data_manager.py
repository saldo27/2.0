# Imports
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Set, Optional, Tuple, Any, TYPE_CHECKING
from exceptions import SchedulerError

if TYPE_CHECKING:
    from scheduler import Scheduler

class DataManager:
    """Enhanced data management and tracking with performance optimizations"""
    
    def __init__(self, scheduler: 'Scheduler'):
        """
        Initialize the data manager with caching support
        
        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler
        
        # Store references to frequently accessed attributes
        self.schedule = scheduler.schedule
        self.worker_assignments = scheduler.worker_assignments
        self.worker_posts = scheduler.worker_posts
        self.worker_weekdays = scheduler.worker_weekdays
        self.worker_weekends = scheduler.worker_weekends
        self.num_shifts = scheduler.num_shifts
        self.workers_data = scheduler.workers_data
        self.holidays = scheduler.holidays
        
        # Performance optimization caches
        self._holiday_set: Set[datetime] = set(self.holidays)
        self._worker_cache: Dict[str, Dict[str, Any]] = {}
        
        # Flag to track if data integrity has been verified
        self.data_integrity_verified = False
        
        # Initialize monthly targets structure
        self.monthly_targets = {}
        
        self._build_worker_cache()
        
        logging.info("Enhanced DataManager initialized with caching")
    
    def _build_worker_cache(self) -> None:
        """Build worker cache for faster lookups"""
        for worker in self.workers_data:
            worker_id = worker['id']
            self._worker_cache[worker_id] = {
                'data': worker,
                'target_shifts': worker.get('target_shifts', 0),
                'work_percentage': worker.get('work_percentage', 100)
            }
        
    def ensure_data_integrity(self):
        """Check and fix data integrity between scheduler data structures"""
        if self.data_integrity_verified:
            return
        
        # Verify worker assignments match schedule
        self.verify_assignment_consistency()
        
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

    def _get_post_counts(self, worker_id):
        """
        Get the count of assignments for each post for a specific worker
    
        Args:
            worker_id: ID of the worker
        
        Returns:
            dict: Dictionary with post numbers as keys and counts as values
        """
        post_counts = {post: 0 for post in range(self.num_shifts)}
    
        for date, shifts in self.schedule.items():
            for post, assigned_worker in enumerate(shifts):
                if assigned_worker == worker_id:
                    post_counts[post] += 1
                
        return post_counts

    def _are_workers_incompatible(self, worker1_id, worker2_id):
        """
        Check if two workers are incompatible with each other
    
        Args:
            worker1_id: ID of first worker
            worker2_id: ID of second worker
        
        Returns:
            bool: True if workers are incompatible, False otherwise
        """
        # Find the worker data for each worker
        worker1 = next((w for w in self.workers_data if w['id'] == worker1_id), None)
        worker2 = next((w for w in self.workers_data if w['id'] == worker2_id), None)
    
        if not worker1 or not worker2:
            return False
    
        # Check if either worker has the other in their incompatibility list
        incompatible_with_1 = worker1.get('incompatible_with', [])
        incompatible_with_2 = worker2.get('incompatible_with', [])
    
        return worker2_id in incompatible_with_1 or worker1_id in incompatible_with_2

    def _get_monthly_distribution(self, worker_id):
        """
        Get monthly distribution of shifts for a worker
    
        Args:
            worker_id: Worker ID to check
        
        Returns:
            dict: Dictionary with month keys and shift counts
        """
        monthly_shifts = {}
    
        for date in self.worker_assignments.get(worker_id, []):
            month_key = f"{date.year}-{date.month:02d}"
            monthly_shifts[month_key] = monthly_shifts.get(month_key, 0) + 1
        
        return monthly_shifts

    def _is_holiday(self, date):
        """
        Check if a date is a holiday
    
        Args:
            date: Date to check
        
        Returns:
            bool: True if the date is a holiday, False otherwise
        """
        return self.scheduler.date_utils.is_holiday(date, self.scheduler.holidays)

    def _is_pre_holiday(self, date):
        """
        Check if a date is the day before a holiday
    
        Args:
            date: Date to check
        
        Returns:
            bool: True if the next day is a holiday, False otherwise
        """
        return self.scheduler.date_utils.is_pre_holiday(date, self.scheduler.holidays)

    def _is_authorized_incompatibility(self, date, worker1_id, worker2_id):
        """
        Check if two incompatible workers are authorized to work together on a specific date
    
        Args:
            date: The date to check
            worker1_id: First worker ID
            worker2_id: Second worker ID
        
        Returns:
            bool: True if the incompatibility is authorized, False otherwise
        """
        # Check if this incompatibility is recorded as deliberately skipped
        date_str = date.strftime('%Y-%m-%d')
        for skip in self.scheduler.constraint_skips.get(worker1_id, {}).get('incompatibility', []):
            if isinstance(skip, tuple) and skip[0] == date_str:
                if worker2_id in skip[1]:
                    return True
                
        return False

    def _analyze_gaps(self, worker_id):
        """
        Analyze the gaps between shifts for a worker
    
        Args:
            worker_id: Worker ID to analyze
        
        Returns:
            dict: Gap analysis statistics
        """
        assignments = sorted(list(self.worker_assignments.get(worker_id, [])))
    
        if len(assignments) <= 1:
            return {
                "min_gap": None,
                "max_gap": None,
                "avg_gap": None,
                "gaps": []
            }
    
        gaps = []
        for i in range(len(assignments) - 1):
            gap = (assignments[i+1] - assignments[i]).days
            gaps.append(gap)
    
        return {
            "min_gap": min(gaps) if gaps else None,
            "max_gap": max(gaps) if gaps else None,
            "avg_gap": sum(gaps) / len(gaps) if gaps else None,
            "gaps": gaps
        }

    def _get_schedule_months(self):
        """
        Calculate number of days in each month of the schedule period
    
        Returns:
            dict: Dictionary with month keys and their available days count
        """
        month_days = {}
        current = self.scheduler.start_date
        while current <= self.scheduler.end_date:
            month_key = f"{current.year}-{current.month:02d}"
    
            # Calculate available days for this month
            month_start = max(
                current.replace(day=1),
                self.scheduler.start_date
            )
            month_end = min(
                (current.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1),
                self.scheduler.end_date
            )
    
            days_in_month = (month_end - month_start).days + 1
            month_days[month_key] = days_in_month
    
            # Move to first day of next month
            current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)

        return month_days

    def _ensure_data_integrity(self):
        """
        Ensure all data structures are consistent before schedule operations
        """
        # Ensure all workers have proper data structures
        for worker in self.workers_data:
            worker_id = worker['id']
    
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
    
        # Ensure schedule dictionary is initialized
        for current_date in self._get_date_range(self.scheduler.start_date, self.scheduler.end_date):
            if current_date not in self.schedule:
                self.schedule[current_date] = []
    
        return True

    def _get_date_range(self, start_date, end_date):
        """
        Get list of dates between start_date and end_date (inclusive)
    
        Args:
            start_date: Start date
            end_date: End date
        
        Returns:
            list: List of dates in range
        """
        date_range = []
        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date)
            current_date += timedelta(days=1)
        return date_range
        
    def mark_data_dirty(self):
        """Mark that data integrity needs to be verified again"""
        self.data_integrity_verified = False
    
    def _verify_assignment_consistency(self):   
        """
        Verify that worker_assignments and schedule are consistent with each other
        and fix any inconsistencies found
        """
       
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
                    
    def _record_constraint_skip(self, worker_id, date, constraint_type, other_worker_id=None):
        """
        Record when a constraint is skipped for tracking purposes
        
        Args:
            worker_id: The worker's ID
            date: Date of the constraint skip
            constraint_type: Type of constraint ('gap', 'incompatibility', 'reduced_gap')
            other_worker_id: Optional ID of other worker involved (for incompatibility)
        """
        date_str = date.strftime('%Y-%m-%d')
        if constraint_type == 'incompatibility' and other_worker_id:
            self.constraint_skips[worker_id][constraint_type].append(
                (date_str, (worker_id, other_worker_id))
            )
        else:
            self.constraint_skips[worker_id][constraint_type].append(date_str)
        
        logging.warning(
            f"Constraint skip recorded - Type: {constraint_type}, "
            f"Worker: {worker_id}, Date: {date_str}"
        )
        
    def _cleanup_schedule(self):
        """
        Clean up schedule while preserving partial assignments
        """
        logging.info("Starting schedule cleanup...")
        incomplete_days = []
        
        current_date = self.start_date
        while current_date <= self.end_date:
            if current_date not in self.schedule:
                incomplete_days.append(current_date)
            else:
                # Keep partial assignments instead of removing them
                assigned_shifts = len(self.schedule[current_date])
                if assigned_shifts > 0 and assigned_shifts < self.num_shifts:
                    logging.info(f"Keeping {assigned_shifts} shifts for {current_date}")
                elif assigned_shifts == 0:
                    incomplete_days.append(current_date)
            
            current_date += timedelta(days=1)

        # Only remove days with zero assignments
        for date in incomplete_days:
            if date in self.schedule:
                logging.info(f"Removing empty day {date}")
                del self.schedule[date]

        logging.info(f"Schedule cleanup complete. Removed {len(incomplete_days)} empty days.")
        # Ensure data consistency before proceeding
        self._ensure_data_integrity()
          
    def remove_worker_assignment(self, worker_id, date):
        """
        Remove a worker's assignment from a given date and update all tracking data
        
        CRITICAL: This method should NEVER remove mandatory_days assignments.
        If called for a mandatory assignment, it will be blocked and return False.
    
        Args:
            worker_id: The worker's ID
            date: Date of the assignment to remove
            
        Returns:
            bool: True if removed successfully, False if blocked (e.g., mandatory)
        """
        try:
            # CRITICAL: Check if this is a mandatory assignment - NEVER remove mandatory_days
            # We need to check the worker's mandatory_days configuration
            worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if worker_data:
                mandatory_str = worker_data.get('mandatory_days', '')
                if mandatory_str.strip():
                    try:
                        from utilities import DateTimeUtils
                        date_utils = DateTimeUtils()
                        mandatory_dates = date_utils.parse_dates(mandatory_str)
                        
                        # Check if this date is mandatory
                        for mandatory_date in mandatory_dates:
                            if mandatory_date.date() == date.date():
                                logging.error(f"BLOCKED removal attempt: {date.strftime('%Y-%m-%d')} is a MANDATORY assignment for worker {worker_id}")
                                return False  # Cannot remove mandatory assignments
                    except Exception as e:
                        logging.error(f"Error checking mandatory_days for worker {worker_id}: {e}")
                        # Fail-safe: if we can't parse mandatory_days, assume it might be mandatory
                        return False
            
            # Check if the worker is actually assigned to this date
            if worker_id not in self.schedule.get(date, []):
                logging.warning(f"Worker {worker_id} is not assigned to {date.strftime('%Y-%m-%d')}")
                return False
            
            # Find the post the worker is assigned to
            post = self.schedule[date].index(worker_id)
        
            # Remove worker from schedule
            self.schedule[date][post] = None
        
            # Remove date from worker assignments
            self.worker_assignments[worker_id].discard(date)
        
            # Update tracking data for the removed assignment
            self._update_worker_stats(worker_id, date, removing=True)
        
            # If the worker has post assignments for this post, remove it if it's the last one
            post_counts = self._get_post_counts(worker_id)
            if post in post_counts and post_counts[post] == 0:
                self.worker_posts[worker_id].discard(post)
            
            logging.info(f"Removed worker {worker_id} assignment from {date.strftime('%Y-%m-%d')}, post {post}")
            return True
        
        except Exception as e:
            logging.error(f"Error removing worker assignment: {str(e)}")
            return False

    def _remove_day_assignments(self, date):
        """
        Remove all assignments for a specific day and update statistics
        
        Args:
            date: Date to remove assignments from
        """
        if date not in self.schedule:
            return
    
        for worker_id in self.schedule[date]:
            # Remove from worker assignments
            self.worker_assignments[worker_id].discard(date)
        
            # Update tracking data
            self._update_worker_stats(worker_id, date, removing=True)
        
        # Remove the day from schedule
        del self.schedule[date]
        
    def _find_incomplete_days(self):
        """
        Find days with incomplete shift assignments
    
        Returns:
            list: Dates where not all shifts are assigned
        """
        incomplete_days = []
        for date in self.schedule.keys():
            if len(self.schedule[date]) < self.num_shifts:
                incomplete_days.append(date)
        return incomplete_days
        
    def _calculate_monthly_targets(self):
        """Calculate target shifts per month for each worker"""
        try:
            self.monthly_targets = {}
        
            # Get available days per month
            month_days = self._get_schedule_months()
            logging.debug(f"Available days per month: {month_days}")
        
            for worker in self.workers_data:
                worker_id = worker['id']
                self.monthly_targets[worker_id] = {}
            
                # Get worker's total target shifts
                total_target = worker.get('target_shifts', 0)
                logging.debug(f"Worker {worker_id} total target: {total_target}")
            
                # Calculate monthly proportion
                total_days = sum(month_days.values())
                for month, days in month_days.items():
                    month_target = round((days / total_days) * total_target)
                    self.monthly_targets[worker_id][month] = month_target
                    logging.debug(f"Worker {worker_id}, Month {month}: {month_target} shifts")
                
        except Exception as e:
            logging.error(f"Error calculating monthly targets: {str(e)}", exc_info=True)
            raise
        
    def get_worker_schedule(self, worker_id):
        """
        Get detailed schedule for a specific worker
        
        Args:
            worker_id: The worker's ID to get schedule for
        Returns:
            dict: Detailed schedule information for the worker
        """
        worker = next(w for w in self.workers_data if w['id'] == worker_id)
        assignments = sorted(list(self.worker_assignments[worker_id]))
        
        schedule_info = {
            'worker_id': worker_id,
            'work_percentage': worker.get('work_percentage', 100),
            'total_shifts': len(assignments),
            'target_shifts': worker.get('target_shifts', 0),
            'assignments': [
                {
                    'date': date.strftime('%Y-%m-%d'),
                    'weekday': date.strftime('%A'),
                    'post': self.schedule[date].index(worker_id) + 1,
                    'is_weekend': self._is_weekend_day(date),
                    'is_holiday': self._is_holiday(date),
                    'is_pre_holiday': self._is_pre_holiday(date)
                }
                for date in assignments
            ],
            'distribution': {
                'monthly': self._get_monthly_distribution(worker_id),
                'weekday': self.worker_weekdays[worker_id],
                'posts': self._get_post_counts(worker_id)
            },
            'constraint_skips': self.constraint_skips[worker_id],
            'gaps_analysis': self._analyze_gaps(worker_id)
        }
        
        return schedule_info
    
    def get_assigned_workers(self, date):
        """
        Return a list of worker IDs that are scheduled on a given date.
        """
        return self.schedule.get(date, [])
    
    def _validate_final_schedule(self):
        """Modified validation to allow for unfilled shifts"""
        errors = []
        warnings = []

        logging.info("Starting final schedule validation...")

        # Check each date in schedule
        for date in sorted(self.schedule.keys()):
            assigned_workers = [w for w in self.schedule[date] if w is not None]
            
            # Check worker incompatibilities
            for i, worker_id in enumerate(assigned_workers):
                for other_id in assigned_workers[i+1:]:
                    if self._are_workers_incompatible(worker_id, other_id):
                        errors.append(
                            f"Incompatible workers {worker_id} and {other_id} "
                            f"on {date.strftime('%Y-%m-%d')}"
                        )

            # Check understaffing (now a warning instead of error)
            filled_shifts = len([w for w in self.schedule[date] if w is not None])
            if filled_shifts < self.num_shifts:
                warnings.append(
                    f"Understaffed on {date.strftime('%Y-%m-%d')}: "
                    f"{filled_shifts} of {self.num_shifts} shifts filled"
                )

        # Check worker-specific constraints
        for worker in self.workers_data:
            worker_id = worker['id']
            assignments = sorted([
                date for date, workers in self.schedule.items()
                if worker_id in workers
            ])
            
            # Check weekend constraints
            for date in assignments:
                if self._is_weekend_day(date):
                    window_start = date - timedelta(days=10)
                    window_end = date + timedelta(days=10)
                    
                    weekend_count = sum(
                        1 for d in assignments
                        if window_start <= d <= window_end and self._is_weekend_day(d)
                    )
                    
                    if weekend_count > 3:
                        errors.append(
                            f"Worker {worker_id} has {weekend_count} weekend/holiday "
                            f"shifts in a 3-week period around {date.strftime('%Y-%m-%d')}"
                        )

            # Validate post rotation
            self._validate_post_rotation(worker_id, warnings)

        # Log all warnings
        for warning in warnings:
            logging.warning(warning)

        # Handle errors
        if errors:
            error_msg = "Schedule validation failed:\n" + "\n".join(errors)
            logging.error(error_msg)
            raise SchedulerError(error_msg)

        logging.info("Schedule validation completed successfully")
        
    def _validate_daily_assignments(self, date, errors, warnings):
        """
        Validate assignments for a specific date
        
        Args:
            date: Date to validate
            errors: List to collect critical errors
            warnings: List to collect non-critical issues
        """
        assigned_workers = self.schedule[date]
        
        # Check staffing levels
        if len(assigned_workers) < self.num_shifts:
            warnings.append(
                f"Understaffed on {date.strftime('%Y-%m-%d')}: "
                f"{len(assigned_workers)} of {self.num_shifts} shifts filled"
            )
        
        # Check worker incompatibilities
        for i, worker_id in enumerate(assigned_workers):
            worker = next(w for w in self.workers_data if w['id'] == worker_id)
            
            for other_id in assigned_workers[i+1:]:
                other_worker = next(w for w in self.workers_data if w['id'] == other_id)
                
                if (worker.get('is_incompatible', False) and 
                    other_worker.get('is_incompatible', False)):
                    if not self._is_authorized_incompatibility(date, worker_id, other_id):
                        errors.append(
                            f"Unauthorized incompatible workers {worker_id} and {other_id} "
                            f"on {date.strftime('%Y-%m-%d')}"
                        )

    def _validate_worker_constraints(self, worker_id, errors, warnings):
        """
        Validate all constraints for a specific worker
        
        Args:
            worker_id: Worker's ID to validate
            errors: List to collect critical errors
            warnings: List to collect non-critical issues
        """
        # Validate post rotation
        self._validate_post_rotation(worker_id, warnings)
        
        # Validate monthly distribution
        self._validate_monthly_distribution(worker_id, warnings)
        
        # Validate weekday distribution
        self._validate_weekday_distribution(worker_id, warnings)
        
        # Validate consecutive weekends
        self._validate_consecutive_weekends(worker_id, errors)
        
        # Validate shift targets
        self._validate_shift_targets(worker_id, warnings)

    def _validate_post_rotation(self, worker_id, warnings):
        """Validate post rotation balance for a worker"""
        post_counts = self._get_post_counts(worker_id)
        total_assignments = sum(post_counts.values())
        last_post = self.num_shifts - 1
        target_last_post = total_assignments * (1 / self.num_shifts)
        actual_last_post = post_counts.get(last_post, 0)
        
        # Allow for some deviation
        allowed_deviation = 1
        
        if abs(actual_last_post - target_last_post) > allowed_deviation:
            warnings.append(
                f"Worker {worker_id} post rotation imbalance: {post_counts}. "
                f"Last post assignments: {actual_last_post}, Target: {target_last_post:.2f}, "
                f"Allowed deviation: ±{allowed_deviation:.2f}"
            )
                                                                      
    def _validate_monthly_distribution(self, worker_id, warnings):
        """Validate monthly shift distribution for a worker"""
        monthly_shifts = self._get_monthly_distribution(worker_id)
        
        if monthly_shifts:
            max_monthly = max(monthly_shifts.values())
            min_monthly = min(monthly_shifts.values())
        
            if max_monthly - min_monthly > 1:
                warnings.append(
                    f"Worker {worker_id} monthly shift imbalance: "
                    f"Max={max_monthly}, Min={min_monthly}\n"
                    f"Distribution: {monthly_shifts}"
                )

    def _validate_weekday_distribution(self, worker_id, warnings):
        """Validate weekday distribution for a worker"""
        weekday_counts = self.worker_weekdays[worker_id]
        max_weekday_diff = max(weekday_counts.values()) - min(weekday_counts.values())
        if max_weekday_diff > 2:
            warnings.append(
                f"Worker {worker_id} weekday imbalance: {weekday_counts}"
            )

    def _validate_consecutive_weekends(self, worker_id: str, errors: List[str]) -> None:
        """Optimized weekend validation with caching"""
        try:
            assignments = sorted(list(self.worker_assignments[worker_id]))
            if not assignments:
                return

            # Pre-calculate weekend/holiday status for all assignments
            weekend_assignments = [
                date for date in assignments
                if (date.weekday() >= 4 or 
                    date in self._holiday_set or 
                    (date + timedelta(days=1)) in self._holiday_set)
            ]
            
            if len(weekend_assignments) <= 3:
                return  # Can't violate with 3 or fewer weekend assignments

            # Check 3-week windows efficiently
            for i, date in enumerate(weekend_assignments):
                # Define 3-week window
                window_start = date - timedelta(days=10)
                window_end = date + timedelta(days=10)
                
                # Count weekend assignments in window
                weekend_count = sum(
                    1 for d in weekend_assignments[max(0, i-5):i+6]  # Optimize by checking nearby dates only
                    if window_start <= d <= window_end
                )
                
                if weekend_count > 3:
                    errors.append(
                        f"Worker {worker_id} has {weekend_count} weekend/holiday "
                        f"shifts in a 3-week period around {date.strftime('%Y-%m-%d')}"
                    )
                    return

        except Exception as e:
            logging.error(f"Error validating consecutive weekends: {str(e)}")
            errors.append(f"Error validating weekends for worker {worker_id}: {str(e)}")

    def _validate_shift_targets(self, worker_id: str, warnings: List[str]) -> None:
        """Optimized shift target validation using cache"""
        worker_cache = self._worker_cache.get(worker_id)
        if not worker_cache:
            logging.warning(f"Worker {worker_id} not found in cache")
            return
            
        assignments = self.worker_assignments[worker_id]
        target_shifts = worker_cache['target_shifts']
        
        shift_difference = abs(len(assignments) - target_shifts)
        if shift_difference > 1:  # Changed from 2 to 1
            warnings.append(
                f"Worker {worker_id} has {len(assignments)} shifts "
                f"(target: {target_shifts})"
            )
    
    def clear_caches(self) -> None:
        """Clear all caches when data changes"""
        self._worker_cache.clear()
        self._build_worker_cache()
        logging.debug("DataManager caches cleared and rebuilt")

    def verify_schedule_integrity(self):
        """
        Verify schedule integrity and constraints
    
        Returns:
            tuple: (bool, dict) - (is_valid, results)
                is_valid: True if schedule passes all validations
                results: Dictionary containing validation results and metrics
        """
        try:
            # Run comprehensive validation
            self.data_manager.validate_final_schedule()
        
            # Additional verification steps...
        
            return True, {
                'stats': self.stats.gather_statistics(),
                'metrics': self.stats.get_schedule_metrics(),
                'coverage': self.stats.calculate_coverage(),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'validator': self.current_user
            }    
        
        except SchedulerError as e:
            logging.error(f"Schedule validation failed: {str(e)}")
            return False, str(e)
    
