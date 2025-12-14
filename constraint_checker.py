# Imports
from datetime import datetime, timedelta
import logging
from typing import Dict, Set, Optional, Tuple, Any, TYPE_CHECKING
from exceptions import SchedulerError

if TYPE_CHECKING:
    from scheduler import Scheduler

class ConstraintChecker:
    """Enhanced constraint checking logic with performance optimizations"""
    
    def __init__(self, scheduler: 'Scheduler'):
        """
        Initialize the constraint checker with caching support
    
        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler
    
        # Store references to frequently accessed attributes
        self.workers_data = scheduler.workers_data
        self.schedule = scheduler.schedule
        self.worker_assignments = scheduler.worker_assignments
        self.holidays = scheduler.holidays
        self.num_shifts = scheduler.num_shifts
        self.date_utils = scheduler.date_utils
        self.gap_between_shifts = scheduler.gap_between_shifts
        self.max_consecutive_weekends = scheduler.max_consecutive_weekends
        self.max_shifts_per_worker = scheduler.max_shifts_per_worker
        
        # Performance optimization caches
        self._incompatibility_cache: Dict[Tuple[str, str], bool] = {}
        self._worker_lookup_cache: Dict[str, Dict[str, Any]] = {}
        self._holiday_set: Set[datetime] = set(self.holidays)
        
        # Build worker lookup cache
        self._build_worker_cache()
        
        logging.info("Enhanced ConstraintChecker initialized with caching")
    
    def _build_worker_cache(self) -> None:
        """Build a worker lookup cache for faster access"""
        for worker in self.workers_data:
            worker_id = worker['id']
            self._worker_lookup_cache[worker_id] = {
                'data': worker,
                'incompatible_with': set(worker.get('incompatible_with', [])),
                'work_percentage': worker.get('work_percentage', 100)
            }
    
    def _get_worker_data(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get worker data from cache"""
        cached_worker = self._worker_lookup_cache.get(worker_id)
        return cached_worker['data'] if cached_worker else None
    
    def _are_workers_incompatible(self, worker1_id: str, worker2_id: str) -> bool:
        """
        Optimized incompatibility check with caching
        """
        if worker1_id == worker2_id:
            return False
        
        # Create cache key (order-independent)
        cache_key = tuple(sorted([worker1_id, worker2_id]))
        
        # Check cache first
        if cache_key in self._incompatibility_cache:
            return self._incompatibility_cache[cache_key]
        
        try:
            worker1_cache = self._worker_lookup_cache.get(worker1_id)
            worker2_cache = self._worker_lookup_cache.get(worker2_id)

            if not worker1_cache or not worker2_cache:
                logging.warning(f"Could not find worker data for {worker1_id} or {worker2_id} during incompatibility check.")
                result = False
            else:
                # Check incompatibility using cached sets
                result = (worker2_id in worker1_cache['incompatible_with'] or 
                         worker1_id in worker2_cache['incompatible_with'])
                
                if result:
                    logging.debug(f"Workers {worker1_id} and {worker2_id} are incompatible.")
            
            # Cache the result
            self._incompatibility_cache[cache_key] = result
            return result

        except Exception as e:
            logging.error(f"Error checking worker incompatibility between {worker1_id} and {worker2_id}: {str(e)}")
            result = False
            self._incompatibility_cache[cache_key] = result
            return result
 

    def _check_incompatibility(self, worker_id: str, date: datetime) -> bool:
        """Optimized incompatibility check with early termination"""
        try:
            # Use the schedule reference from self.scheduler
            if date not in self.scheduler.schedule:
                return True # No one assigned, compatible

            # Get the list of workers already assigned (filter out None values)
            assigned_workers = [w_id for w_id in self.scheduler.schedule.get(date, []) if w_id is not None]
            
            if not assigned_workers:
                return True # No workers assigned
            
            # Get worker's incompatible list from cache
            worker_cache = self._worker_lookup_cache.get(worker_id)
            if not worker_cache:
                logging.warning(f"Worker {worker_id} not found in cache during incompatibility check")
                return False
            
            incompatible_with = worker_cache['incompatible_with']
            
            # Quick check: if no incompatibilities defined, return True
            if not incompatible_with:
                return True
            
            # Check if any assigned worker is in the incompatible list
            for assigned_id in assigned_workers:
                if assigned_id == worker_id:
                    continue # Skip self
                if assigned_id in incompatible_with:
                    logging.debug(f"Incompatibility Violation: {worker_id} cannot work with {assigned_id} on {date}")
                    return False

            return True # No incompatibilities found

        except Exception as e:
            logging.error(f"Error checking incompatibility for worker {worker_id} on {date}: {str(e)}")
            return False # Fail safe - assume incompatible on error
    
    def clear_caches(self) -> None:
        """Clear all caches (call when worker data changes)"""
        self._incompatibility_cache.clear()
        self._worker_lookup_cache.clear()
        self._build_worker_cache()
        logging.debug("ConstraintChecker caches cleared and rebuilt")


    def _check_gap_constraint(self, worker_id, date): # Removed min_gap parameter
        """Check minimum gap between assignments, Friday-Monday, and 7/14 day patterns."""
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker: return False # Should not happen
        work_percentage = worker.get('work_percentage', 100)

        # Determine base minimum days required *between* shifts
        # if gap_between_shifts = 1, then 2 days must be between assignments.
        # if gap_between_shifts = 3, then 4 days must be between assignments.
        # So, days_between must be >= self.scheduler.gap_between_shifts + 1
        min_required_days_between = self.scheduler.gap_between_shifts + 1
    
        # Part-time workers might need a larger gap
        if work_percentage < 70: # Using a common threshold from ScheduleBuilder
            min_required_days_between = max(min_required_days_between, self.scheduler.gap_between_shifts + 2) # e.g. at least +1 more day

        assignments = sorted(list(self.scheduler.worker_assignments.get(worker_id, []))) # Use scheduler's live assignments

        for prev_date in assignments:
            if prev_date == date: continue # Should not happen if checking before assignment
            days_between = abs((date - prev_date).days)

            # Basic gap check
            if days_between < min_required_days_between:
                logging.debug(f"Constraint Check: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails basic gap with {prev_date.strftime('%Y-%m-%d')} ({days_between} < {min_required_days_between})")
                return False
    
            # Friday-Monday rule: typically only if base gap is small (e.g., allows for 3-day difference)
            # This rule means a worker doing Fri cannot do Mon, creating a 3-day diff.
            # If min_required_days_between is already > 3, this rule is implicitly covered.
            if self.scheduler.gap_between_shifts <= 1: # Only apply if basic gap could allow a 3-day span
                if days_between == 3:
                    if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                        (date.weekday() == 4 and prev_date.weekday() == 0)):
                        logging.debug(f"Constraint Check: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails Fri-Mon rule with {prev_date.strftime('%Y-%m-%d')}")
                        return False
        
            # Prevent same day of week in consecutive weeks (7 or 14 day pattern)
            # CRITICAL: This constraint applies to ALL days (weekdays AND weekends)
            # NO exceptions allowed - this is a HARD constraint
            if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                logging.debug(f"Constraint Check: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails 7/14 day pattern with {prev_date.strftime('%Y-%m-%d')}")
                return False
        
        return True
    
    def _would_exceed_weekend_limit(self, worker_id, date):
        """
        Enhanced check if assigning this date would exceed weekend/holiday constraints:
        1. Max consecutive weekend/holiday constraint (adjusted for part-time)
        2. Proportional weekend count with +/- 1 tolerance based on work percentage and time worked
        """ 
        try:
            # Check if the date is a weekend or holiday
            is_weekend_or_holiday = (date.weekday() >= 4 or  # Fri, Sat, Sun
                                    date in self.scheduler.holidays or
                                    (date + timedelta(days=1)) in self.scheduler.holidays)
            if not is_weekend_or_holiday:
                return False  # Not a weekend/holiday, no need to check

            # Get worker data
            worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker_data:
                return True  # Worker not found
    
            # Get work percentage
            work_percentage = float(worker_data.get('work_percentage', 100))
    
            # Get work periods or default to full schedule period
            work_periods = []
            if 'work_periods' in worker_data and worker_data['work_periods'].strip():
                work_periods = self.date_utils.parse_date_ranges(worker_data['work_periods'])
                # Check if current date is within any work period
                if not any(start <= date <= end for start, end in work_periods):
                    return False  # Date is outside work periods
            else:
                # Default to full schedule if no work periods specified
                work_periods = [(self.scheduler.start_date, self.scheduler.end_date)]

            # PART 1: CHECK MAX CONSECUTIVE WEEKENDS/HOLIDAYS (keep existing logic)
            base_max_consecutive = self.scheduler.max_consecutive_weekends
    
            # Adjust max consecutive for part-time workers (<70%)
            if work_percentage < 70:
                adjusted_max_consecutive = max(1, int(base_max_consecutive * work_percentage / 100))
            else:
                adjusted_max_consecutive = base_max_consecutive
    
            # Get all weekend/holiday assignments including the prospective date
            current_assignments = self.scheduler.worker_assignments.get(worker_id, set())
            weekend_dates = []
            for d in current_assignments:
                if (d.weekday() >= 4 or 
                    d in self.scheduler.holidays or
                    (d + timedelta(days=1)) in self.scheduler.holidays):
                    weekend_dates.append(d)
    
            # Add the date being checked if it's not already included
            if date not in weekend_dates:
                weekend_dates.append(date)
    
            weekend_dates.sort()
    
            # Check consecutive weekend/holiday constraint
            if weekend_dates:
                consecutive_groups = []
                current_group = []
        
                for i, weekend_date in enumerate(weekend_dates):
                    if not current_group:
                        current_group = [weekend_date]
                    else:
                        prev_date = current_group[-1]
                        days_between = (weekend_date - prev_date).days
                
                        # Consecutive weekends typically have 5-10 days between them
                        if 5 <= days_between <= 10:
                            current_group.append(weekend_date)
                        else:
                            consecutive_groups.append(current_group)
                            current_group = [weekend_date]
        
                if current_group:
                    consecutive_groups.append(current_group)
        
                # Check if any group exceeds the limit
                max_consecutive = max(len(group) for group in consecutive_groups) if consecutive_groups else 0
                if max_consecutive > adjusted_max_consecutive:
                    return True  # Would exceed consecutive limit

            # PART 2: ENHANCED PROPORTIONAL WEEKEND DISTRIBUTION CHECK
            # Calculate all weekend days in the full schedule period
            all_schedule_days = []
            current_date = self.scheduler.start_date
            while current_date <= self.scheduler.end_date:
                all_schedule_days.append(current_date)
                current_date += timedelta(days=1)
        
            total_weekend_days = sum(1 for d in all_schedule_days 
                                   if (d.weekday() >= 4 or 
                                       d in self.scheduler.holidays or
                                       (d + timedelta(days=1)) in self.scheduler.holidays))
        
            # Calculate worker's available weekend days within their work periods
            worker_weekend_days = 0
            for start_period, end_period in work_periods:
                period_current = start_period
                while period_current <= end_period:
                    if (period_current.weekday() >= 4 or 
                        period_current in self.scheduler.holidays or
                        (period_current + timedelta(days=1)) in self.scheduler.holidays):
                        worker_weekend_days += 1
                    period_current += timedelta(days=1)
        
            if total_weekend_days == 0 or worker_weekend_days == 0:
                return False  # No weekends to distribute
        
            # Calculate proportional target with +/- 1 tolerance
            base_proportion = worker_weekend_days / total_weekend_days
            work_factor = work_percentage / 100
        
            # Calculate target weekend assignments
            raw_target = total_weekend_days * base_proportion * work_factor
            target_weekend_count = round(raw_target)
        
            # Apply +/- 1 tolerance as specified in requirements
            min_target = max(0, target_weekend_count - 1)
            max_target = target_weekend_count + 1
        
            # Count current weekend assignments (including the prospective one)
            current_weekend_count = len(weekend_dates)
        
            # Check if assignment would exceed the tolerance limit
            if current_weekend_count > max_target:
                logging.debug(f"Worker {worker_id}: weekend assignment would exceed proportional limit "
                             f"({current_weekend_count} > {max_target}). Target: {target_weekend_count}, "
                             f"work %: {work_percentage}%, proportion: {base_proportion:.2f}")
                return True
        
            logging.debug(f"Worker {worker_id}: weekend assignment within limits "
                         f"({current_weekend_count} <= {max_target}). Target: {target_weekend_count}")
            return False
        
        except Exception as e:
            logging.error(f"Error checking weekend limit for worker {worker_id}: {e}")
            return True  # Conservative: reject on error
            
    def _is_worker_unavailable(self, worker_id, date):
        """
        Check if worker is unavailable on a specific date
        """
        try:
            worker = next(w for w in self.workers_data if w['id'] == worker_id)
        
            # Check days off
            if worker.get('days_off'):
                off_periods = self.date_utils.parse_date_ranges(worker['days_off'])
                if any(start <= date <= end for start, end in off_periods):
                    logging.debug(f"Worker {worker_id} is off on {date}")
                    return True

            # Check work periods
            if worker.get('work_periods'):
                work_periods = self.date_utils.parse_date_ranges(worker['work_periods'])
                if not any(start <= date <= end for start, end in work_periods):
                    logging.debug(f"Worker {worker_id} is not in work period on {date}")
                    return True

            # Check if worker is already assigned for this date
            if date in self.worker_assignments.get(worker_id, []):
                logging.debug(f"Worker {worker_id} is already assigned on {date}")
                return True

            # Check weekend constraints (replacing the custom weekend check)
            # Only check if this is a weekend day or holiday to improve performance
            is_special_day_for_unavailability_check = (date.weekday() >= 4 or
                                                        date in self.holidays or
                                                        (date + timedelta(days=1)) in self.holidays)
            if is_special_day_for_unavailability_check:
                if self._would_exceed_weekend_limit(worker_id, date): # This now calls the consistently defined limit
                    logging.debug(f"Worker {worker_id} would exceed weekend limit if assigned on {date}")
                    return True

            return False

        except Exception as e:
            logging.error(f"Error checking worker {worker_id} availability: {str(e)}")
            return True  # Default to unavailable in case of error
        
    def _can_assign_worker(self, worker_id, date, post):
        """
        Check if a worker can be assigned to a shift
        """
        try:
            # Log all constraint checks
            logging.debug(f"\nChecking worker {worker_id} for {date}, post {post}")

            # 1. First check - Incompatibility
            if not self._check_incompatibility(worker_id, date):
                logging.debug(f"- Failed: Worker {worker_id} is incompatible with assigned workers")
                return False

            # 2. Check max shifts
            if len(self.worker_assignments.get(worker_id, [])) >= self.max_shifts_per_worker:
                logging.debug(f"- Failed: Max shifts reached ({self.max_shifts_per_worker})")
                return False

            # 3. Check availability
            if self._is_worker_unavailable(worker_id, date):
                logging.debug(f"- Failed: Worker unavailable")
                return False

            # 4. Check gap constraints (including 7/14 day pattern)
            if not self._check_gap_constraint(worker_id, date): # This method includes the 7/14 day check
                logging.debug(f"- Failed: Gap or 7/14 day pattern constraint for worker {worker_id} on {date}")
                return False

            # 6. CRITICAL: Check weekend limit - NEVER RELAX THIS
            if self._would_exceed_weekend_limit(worker_id, date):
                logging.debug(f"- Failed: Would exceed weekend limit")
                return False
        
            return True

        except Exception as e:
            logging.error(f"Error in _can_assign_worker for worker {worker_id}: {str(e)}", exc_info=True)
            return False
              
    def _check_constraints(self, worker_id, date, skip_constraints=False, try_part_time=False): # try_part_time seems unused
        """
        Unified constraint checking with data synchronization validation.
        Returns: (bool, str) - (passed, reason_if_failed)
        """
        try:
            # ENHANCED: Ensure data synchronization before constraint checking
            if hasattr(self.scheduler, '_ensure_data_synchronization'):
                if not self.scheduler._ensure_data_synchronization():
                    logging.warning(f"Data synchronization issues detected before constraint check for worker {worker_id} on {date.strftime('%Y-%m-%d')}")
            
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                return False, "worker_not_found"
            # work_percentage = float(worker.get('work_percentage', 100)) # Not used directly in this version

            # Basic availability checks (never skipped)
            # self.scheduler.worker_assignments refers to the main worker assignment tracking
            if date in self.scheduler.worker_assignments.get(worker_id, []): # Check against current assignments
                return False, "already_assigned_this_day" 

            if self._is_worker_unavailable(worker_id, date): # This checks days_off, work_periods
                # _is_worker_unavailable already logs
                return False, "unavailable_generic" 

            if not skip_constraints:
                # Incompatibility constraints (with workers already in self.scheduler.schedule for that date)
                # Assuming self.scheduler.constraint_checker is this instance or has the method
                if not self._check_incompatibility(worker_id, date): # Check against self.scheduler.schedule
                    # _check_incompatibility logs
                    return False, "incompatibility"

                # Gap constraints (including 7/14 day pattern, Fri-Mon)
                # This uses self.scheduler.worker_assignments for its checks
                if not self._check_gap_constraint(worker_id, date):
                    # _check_gap_constraint logs
                    return False, "gap_or_pattern_constraint"
            
                # Weekend constraints (Max consecutive weekends/special days)
                # This uses self.scheduler.worker_assignments
                if self._would_exceed_weekend_limit(worker_id, date):
                    # _would_exceed_weekend_limit logs
                    return False, "weekend_limit_exceeded"

            return True, "passed_all_checks"
        except Exception as e:
            logging.error(f"Error checking constraints for worker {worker_id} on {date}: {str(e)}", exc_info=True)
            return False, f"error_in_check_constraints: {str(e)}"

    def _check_day_compatibility(self, worker_id, date):
        """Check if worker is compatible with all workers already assigned to this date"""
        if date not in self.schedule:
            return True
        
        for assigned_worker in self.schedule[date]:
            if assigned_worker is not None and self._are_workers_incompatible(worker_id, assigned_worker):
                logging.debug(f"Worker {worker_id} is incompatible with assigned worker {assigned_worker}")
                return False
        return True

    def _check_weekday_balance(self, worker_id, date_to_assign, current_assignments_for_worker):
        """
        Check if assigning date_to_assign to worker_id would maintain weekday balance (+/- 1).
        Uses a hypothetical count.

        Args:
            worker_id: The ID of the worker.
            date_to_assign: The datetime.date object of the shift being considered.
            current_assignments_for_worker: A set of datetime.date objects representing
                                             the worker's current assignments.
        Returns:
            bool: True if assignment maintains balance, False otherwise.
        """
        try:
            # 1. Calculate hypothetical weekday counts
            hypothetical_weekday_counts = {day: 0 for day in range(7)}

            # Count existing assignments
            for assigned_date in current_assignments_for_worker:
                hypothetical_weekday_counts[assigned_date.weekday()] += 1
            
            # Add the new assignment
            hypothetical_weekday_counts[date_to_assign.weekday()] += 1

            # 2. Calculate spread
            min_count = min(hypothetical_weekday_counts.values())
            max_count = max(hypothetical_weekday_counts.values())
            spread = max_count - min_count

            # 3. Check balance: spread > 1 means the difference is 2 or more, violating +/-1
            # Example: counts {Mon:1, Tue:1, Wed:3} -> min=1, max=3, spread=2. (Violates +/-1)
            # Example: counts {Mon:1, Tue:2, Wed:2} -> min=1, max=2, spread=1. (OK for +/-1)
            if spread > 1: # This means the difference is 2 or more.
                logging.debug(f"Constraint Check (Weekday Balance): Worker {worker_id} for date {date_to_assign.strftime('%Y-%m-%d')}. "
                              f"Hypothetical counts: {hypothetical_weekday_counts}, Spread: {spread}. VIOLATES +/-1 rule.")
                return False

            logging.debug(f"Constraint Check (Weekday Balance): Worker {worker_id} for date {date_to_assign.strftime('%Y-%m-%d')}. "
                          f"Hypothetical counts: {hypothetical_weekday_counts}, Spread: {spread}. OK for +/-1 rule.")
            return True

        except Exception as e:
            logging.error(f"Error in ConstraintChecker._check_weekday_balance for worker {worker_id}, date {date_to_assign}: {str(e)}", exc_info=True)
            return False # Safer to return False on error
 
    def is_weekend_day(self, date):
        """Check if a date is a weekend day or holiday or day before holiday."""
        try:
            return self.scheduler.date_utils.is_weekend_day(date, self.scheduler.holidays)
        except Exception as e:
            logging.error(f"Error checking if date is weekend: {str(e)}")
            return False
