from datetime import datetime, timedelta
import logging

class WorkerEligibilityTracker:
    """Helper class to track and manage worker eligibility for assignments"""
    
    def __init__(self, workers_data, holidays, gap_between_shifts=1, max_consecutive_weekends=2, 
                 start_date=None, end_date=None, date_utils=None, scheduler=None):
        """
        Initialize the worker eligibility tracker
    
        Args:
            workers_data: List of worker dictionaries
            holidays: List of holiday dates
            gap_between_shifts: Minimum days between shifts (default: 1)
            max_consecutive_weekends: Maximum consecutive weekend/holiday shifts allowed (default: 2)
            start_date: Schedule start date (optional)
            end_date: Schedule end date (optional)
            date_utils: DateTimeUtils instance for date parsing (optional)
            scheduler: Reference to the scheduler object (optional)
        """
        self.workers_data = workers_data
        self.holidays = holidays
        self.gap_between_shifts = gap_between_shifts
        self.max_consecutive_weekends = max_consecutive_weekends
        self.last_worked_date = {w['id']: None for w in workers_data}
        self.total_assignments = {w['id']: 0 for w in workers_data}
        self.recent_weekends = {w['id']: [] for w in workers_data}
        
        # Store references that might be needed for weekend calculations
        self.start_date = start_date
        self.end_date = end_date
        self.date_utils = date_utils
        self.scheduler = scheduler
    
    def update_worker_status(self, worker_id, date):
        """
        Update tracking data when a worker is assigned
    
        Args:
            worker_id: ID of the worker
            date: Date of the assignment
        """
        self.last_worked_date[worker_id] = date
        self.total_assignments[worker_id] += 1
    
        if self._is_weekend_day(date):
            # Only add if not already in the list (prevent duplicates)
            if date not in self.recent_weekends[worker_id]:
                self.recent_weekends[worker_id].append(date)
            
            # Keep only recent weekends (last 21 days)
            cutoff_date = date - timedelta(days=21)
            self.recent_weekends[worker_id] = [
                d for d in self.recent_weekends[worker_id]
                if d > cutoff_date
            ]
        
            # Ensure the list is sorted for consistent window calculations
            self.recent_weekends[worker_id].sort()
    
    
    def get_eligible_workers(self, date, assigned_workers):
        """
        Get list of workers eligible for assignment on given date
        
        Args:
            date: Date to check eligibility for
            assigned_workers: List of workers already assigned to this date
        Returns:
            list: List of eligible workers
        """
        eligible_workers = []
        
        for worker in self.workers_data:
            worker_id = worker['id']
            
            # Quick checks first (most likely to fail)
            if not self._check_basic_eligibility(worker_id, date, assigned_workers):
                continue
                
            # More expensive checks
            if not self._check_weekend_constraints(worker_id, date):
                continue
                
            eligible_workers.append(worker)
            
        return eligible_workers
    
    def _check_basic_eligibility(self, worker_id, date, assigned_workers):
        # Check if already assigned that day
        if worker_id in assigned_workers:
            return False
    
        # Check minimum gap based on the configurable parameter
        last_worked = self.last_worked_date[worker_id]
        if last_worked:
            days_between = (date - last_worked).days
    
            # Basic minimum gap check - need configured gap days off
            min_days_between = self.gap_between_shifts + 1  # +1 because we need days_between > gap
            if days_between < min_days_between:
                return False
        
            # Special case for Friday-Monday 
            if days_between == 3 and (
                 (date.weekday() == 0 and last_worked.weekday() == 4) or
                 (date.weekday() == 4 and last_worked.weekday() == 0)
            ):
                return False
    
        return True
    
    def _get_date_range(self, start, end):
        """Get range of dates between start and end (inclusive)"""
        # First try to use scheduler's method
        if hasattr(self, 'scheduler') and hasattr(self.scheduler, '_get_date_range'):
            return self.scheduler._get_date_range(start, end)
            
        # Fallback implementation
        date_range = []
        current = start
        while current <= end:
            date_range.append(current)
            current += timedelta(days=1)
        return date_range
    
    def _check_weekend_constraints(self, worker_id, date):
        """
        Check weekend-related constraints - ensuring max consecutive weekend/holiday shifts
        Adjusted to account for both work percentage (<70%) and work periods.

        Args:
            worker_id: ID of the worker to check
            date: Date to check
        Returns:
            bool: True if worker can be assigned to this weekend date
        """
        # If not a weekend day or holiday, no constraint applies
        if not self._is_weekend_day(date):
            return True

        # Find worker data to get work percentage and work periods
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker:
            return False
        
        # Check if date is within worker's work periods
        work_periods = []
        if 'work_periods' in worker and worker.get('work_periods', '').strip():
            try:
                # Try different ways to parse work periods
                if self.date_utils:
                    work_periods = self.date_utils.parse_date_ranges(worker['work_periods'])
                elif hasattr(self, 'scheduler') and hasattr(self.scheduler, 'date_utils'):
                    work_periods = self.scheduler.date_utils.parse_date_ranges(worker['work_periods'])
                else:
                    logging.warning(f"Cannot parse work periods for worker {worker_id}, no date_utils available")
                
                # Check if date is in any work period
                if work_periods and not any(start <= date <= end for start, end in work_periods):
                    logging.debug(f"Worker {worker_id} not in work period on {date}")
                    return False
            except Exception as e:
                logging.error(f"Error checking work periods for worker {worker_id}: {str(e)}")
        
        # Get work percentage and adjust max consecutive weekends
        work_percentage = float(worker.get('work_percentage', 100))
        
        # Adjust max consecutive weekends for part-time workers (<70%)
        if work_percentage < 70:
            adjusted_max_weekends = max(1, int(self.max_consecutive_weekends * work_percentage / 100))
        else:
            adjusted_max_weekends = self.max_consecutive_weekends

        # Create a temporary list including existing weekend days and the new one
        all_weekend_dates = self.recent_weekends[worker_id].copy()

        # Avoid double counting if date is already in the list
        if date not in all_weekend_dates:
            all_weekend_dates.append(date)
        
        all_weekend_dates.sort()

        # Check for consecutive weekends using a window approach
        consecutive_groups = []
        current_group = []
        
        for weekend_date in all_weekend_dates:
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
        
        # Find the longest consecutive sequence
        max_consecutive = 0
        if consecutive_groups:
            max_consecutive = max(len(group) for group in consecutive_groups)
        
        # Check against adjusted max consecutive limit
        if max_consecutive > adjusted_max_weekends:
            logging.debug(f"Worker {worker_id} would exceed consecutive weekend limit: {max_consecutive} > {adjusted_max_weekends}")
            return False
        
        # Get schedule period for proportional weekend count check
        start_date = self.start_date
        end_date = self.end_date
        
        # Try to get dates from scheduler if not set directly
        if (not start_date or not end_date) and hasattr(self, 'scheduler'):
            start_date = getattr(self.scheduler, 'start_date', None)
            end_date = getattr(self.scheduler, 'end_date', None)
        
        # Only perform proportional check if we have schedule period
        if start_date and end_date:
            try:
                # Get all days in the schedule
                all_days = self._get_date_range(start_date, end_date)
                total_weekend_days = sum(1 for d in all_days if self._is_weekend_day(d))
                
                # Calculate weekend days within worker's work periods
                worker_weekend_days = 0
                if work_periods:
                    for start, end in work_periods:
                        period_days = self._get_date_range(start, end)
                        worker_weekend_days += sum(1 for d in period_days if self._is_weekend_day(d))
                else:
                    worker_weekend_days = total_weekend_days  # All weekends if no work periods
                
                # Calculate target weekend count
                proportion_of_schedule = worker_weekend_days / total_weekend_days if total_weekend_days > 0 else 0
                work_percentage_factor = work_percentage / 100
                target_weekend_count = round(total_weekend_days * proportion_of_schedule * work_percentage_factor * 0.9)
                
                # Ensure at least 1 if they have any work periods with weekends
                if worker_weekend_days > 0:
                    target_weekend_count = max(1, target_weekend_count)
                
                logging.debug(f"Worker {worker_id}: work %={work_percentage}%, schedule proportion={proportion_of_schedule:.2f}, "
                            f"target weekend count={target_weekend_count}, current={len(all_weekend_dates)}")
                
                # Check if assignment would exceed target
                if len(all_weekend_dates) > target_weekend_count:
                    logging.debug(f"Worker {worker_id} would exceed proportional weekend count: {len(all_weekend_dates)} > {target_weekend_count}")
                    return False
                    
            except Exception as e:
                logging.error(f"Error calculating proportional weekend count for worker {worker_id}: {str(e)}")

        return True  # All constraints satisfied
    
    def _is_weekend_day(self, date):
        """
        Check if date is a weekend day or holiday
        
        Args:
            date: Date to check
        Returns:
            bool: True if date is a weekend day or holiday
        """
        return self.scheduler.date_utils.is_weekend_day(date, self.scheduler.holidays)

    def calculate_proportional_weekend_target(self, worker_id, work_percentage, work_periods):
        """
        Calculate target weekend shifts based on work percentage and time worked
        with better proportional distribution
        """
        # Get all weekend days in schedule period
        all_days = self._get_date_range(self.start_date, self.end_date)
        total_weekend_days = sum(1 for d in all_days if self._is_weekend_day(d))
    
        # Calculate worker's available weekend days
        if work_periods:
            worker_weekend_days = 0
            for start, end in work_periods:
                period_days = self._get_date_range(start, end)
                worker_weekend_days += sum(1 for d in period_days if self._is_weekend_day(d))
        else:
            worker_weekend_days = total_weekend_days
    
        # Improved proportional calculation
        base_proportion = worker_weekend_days / total_weekend_days if total_weekend_days > 0 else 0
        work_factor = work_percentage / 100
    
        # Target with tolerance range (+/- 1 as specified)
        raw_target = total_weekend_days * base_proportion * work_factor
        target_weekend_count = round(raw_target)
    
        # Apply tolerance (+/- 1)
        min_target = max(0, target_weekend_count - 1)
        max_target = target_weekend_count + 1
    
        return min_target, target_weekend_count, max_target

    def validate_weekend_assignment_with_tolerance(self, worker_id, date, force_assign=False):
        """
        Validate weekend assignment with improved tolerance and proportional checking
        """
        # Check consecutive weekend limit
        if self._would_exceed_consecutive_weekend_limit(worker_id, date):
            if not force_assign:
                return False, "Would exceed consecutive weekend limit"
    
        # Check proportional limit with tolerance
        current_weekend_count = len([d for d in self.worker_assignments.get(worker_id, set()) 
                                    if self._is_weekend_day(d)])
    
        min_target, target, max_target = self.calculate_proportional_weekend_target(
            worker_id, 
            self._get_worker_percentage(worker_id),
            self._get_worker_periods(worker_id)
        )
    
        # Allow assignment if within tolerance range
        if current_weekend_count < max_target:
            return True, "Assignment within proportional limits"
        elif force_assign:
            return True, "Forced assignment despite exceeding limits"
        else:
            return False, f"Would exceed proportional limit ({current_weekend_count+1} > {max_target})"

    def _update_tracking_data(self, worker_id, date, post):
        """
        Update all tracking data structures after assignment
    
        Args:
            worker_id: ID of the worker being assigned
            date: Date of assignment
            post: Post number being assigned (0-indexed)
        """
        try:
            # Ensure data structures exist for this worker
            if worker_id not in self.scheduler.worker_assignments:
                self.scheduler.worker_assignments[worker_id] = set()
        
            if worker_id not in self.scheduler.worker_posts:
                self.scheduler.worker_posts[worker_id] = set()
            
            if worker_id not in self.scheduler.worker_weekdays:
                self.scheduler.worker_weekdays[worker_id] = {i: 0 for i in range(7)}
            
            if worker_id not in self.scheduler.worker_weekends:
                self.scheduler.worker_weekends[worker_id] = []
        
            # Update worker assignments set
            self.scheduler.worker_assignments[worker_id].add(date)
        
            # Update post tracking
            self.scheduler.worker_posts[worker_id].add(post)
        
            # Update weekday counts
            weekday = date.weekday()
            self.scheduler.worker_weekdays[worker_id][weekday] += 1
        
            # Update weekend tracking if applicable
            is_weekend = self.scheduler.date_utils.is_weekend_day(date, self.scheduler.holidays)
            if is_weekend:
                # Check if we already have this date in the weekends list
                if date not in self.scheduler.worker_weekends[worker_id]:
                    self.scheduler.worker_weekends[worker_id].append(date)
                
                # Ensure the weekend list is sorted by date
                self.scheduler.worker_weekends[worker_id].sort()
        
            # Update the worker eligibility tracker if it exists
            if hasattr(self.scheduler, 'eligibility_tracker'):
                self.scheduler.eligibility_tracker.update_worker_status(worker_id, date)
            
            # Mark data as needing verification since we've modified it
            self.mark_data_dirty()
        
            # Log the update
            logging.debug(f"Updated tracking data for {worker_id} on {date.strftime('%d-%m-%Y')}, post {post}")
        
        except Exception as e:
            logging.error(f"Error in _update_tracking_data for worker {worker_id}: {str(e)}", exc_info=True)
        
    def remove_worker_assignment(self, worker_id, date):
        """
        Remove tracking data when a worker's assignment is removed
    
        Args:
            worker_id: ID of the worker
            date: Date of the assignment being removed
        """
        # Update last worked date if needed
        if self.last_worked_date[worker_id] == date:
            # Since we don't track all assignments here, we can't determine the 
            # next most recent assignment. Just set to None to indicate no recent assignment
            self.last_worked_date[worker_id] = None
    
        # Decrement total assignments
        self.total_assignments[worker_id] = max(0, self.total_assignments[worker_id] - 1)
    
        # Remove from weekend tracking if applicable
        if self._is_weekend_day(date) and date in self.recent_weekends[worker_id]:
            self.recent_weekends[worker_id].remove(date)

    def _remove_tracking_data(self, worker_id, date, post):
        """
        Remove tracking data when a worker is unassigned from a shift
    
        Args:
            worker_id: ID of the worker being unassigned
            date: Date of assignment
            post: Post number being unassigned (0-indexed)
        """
        try:
            # Remove from worker assignments
            if worker_id in self.scheduler.worker_assignments:
                if date in self.scheduler.worker_assignments[worker_id]:
                    self.scheduler.worker_assignments[worker_id].remove(date)
        
            # We cannot directly remove from posts since worker_posts doesn't track which post
            # was assigned on which date. We'll need to recalculate this from the schedule.
        
            # Update weekday counts
            if worker_id in self.scheduler.worker_weekdays:
                weekday = date.weekday()
                if self.scheduler.worker_weekdays[worker_id][weekday] > 0:
                    self.scheduler.worker_weekdays[worker_id][weekday] -= 1
        
            # Update weekend tracking
            is_weekend = self.scheduler.date_utils.is_weekend_day(date, self.scheduler.holidays)
            if is_weekend and worker_id in self.scheduler.worker_weekends:
                if date in self.scheduler.worker_weekends[worker_id]:
                    self.scheduler.worker_weekends[worker_id].remove(date)
        
            # Update the worker eligibility tracker if it exists
            if hasattr(self.scheduler, 'eligibility_tracker'):
                self.scheduler.eligibility_tracker.remove_worker_assignment(worker_id, date)
            
            # Mark data as needing verification since we've modified it
            self.mark_data_dirty()
        
            # Log the update
            logging.debug(f"Removed tracking data for {worker_id} from {date.strftime('%d-%m-%Y')}, post {post}")
        
        except Exception as e:
            logging.error(f"Error in _remove_tracking_data for worker {worker_id}: {str(e)}", exc_info=True)

    def rebuild_worker_posts(self):
        """
        Rebuild the worker_posts tracking structure from the schedule
        Should be called after making multiple changes to the schedule
        """
        # Initialize empty post sets for all workers
        self.scheduler.worker_posts = {w['id']: set() for w in self.scheduler.workers_data}
    
        # Iterate through the schedule and rebuild post assignments
        for date, workers in self.scheduler.schedule.items():
            for post, worker_id in enumerate(workers):
                if worker_id is not None:
                    self.scheduler.worker_posts[worker_id].add(post)
    
        logging.debug("Rebuilt worker post assignments")
