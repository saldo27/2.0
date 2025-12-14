# Imports
from datetime import datetime, timedelta
import logging
import random
import copy
import json
from typing import Dict, List, Set, Optional, Tuple, Any

from scheduler_config import setup_logging, SchedulerConfig
from constraint_checker import ConstraintChecker
from schedule_builder import ScheduleBuilder
from data_manager import DataManager
from utilities import DateTimeUtils
from statistics import StatisticsCalculator
from exceptions import SchedulerError
from worker_eligibility import WorkerEligibilityTracker

# Initialize logging using the configuration module
setup_logging()

class SchedulerError(Exception):
    """Custom exception for Scheduler errors"""
    pass

class Scheduler:
    """Main Scheduler class that coordinates all scheduling operations"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the scheduler with configuration"""
        logging.info("Scheduler initialized")
        
        # Initialize cache for performance optimization
        self._cache: Dict[str, Any] = {}
        self._cache_enabled = config.get('cache_enabled', SchedulerConfig.CACHE_ENABLED)
        
        try:
            # Initialize date_utils FIRST, before calling any method that might need it
            self.date_utils = DateTimeUtils()
    
            # Then validate the configuration
            self._validate_config(config)
    
            # Basic setup from config
            self.config = config
            self.start_date = config['start_date']
            self.end_date = config['end_date']
            self.num_shifts = config['num_shifts']
            self.variable_shifts = config.get('variable_shifts', [])
            self.workers_data = config['workers_data']
            self.holidays = config.get('holidays', [])
            self.enable_proportional_weekends = config.get('enable_proportional_weekends', True)
            self.weekend_tolerance = config.get('weekend_tolerance', 1)  # +/- 1 as specified

            # --- START: Build incompatibility lists ---
            incompatible_worker_ids = {
                worker['id'] for worker in self.workers_data if worker.get('is_incompatible', False)
            }
            logging.debug(f"Identified incompatible worker IDs (from is_incompatible flag): {incompatible_worker_ids}")

            for worker in self.workers_data:
                worker_id = worker['id']
                # Check if incompatible_with is already defined
                if 'incompatible_with' not in worker or not worker['incompatible_with']:
                    # Initialize the list if not already set
                    worker['incompatible_with'] = []
                    if worker.get('is_incompatible', False):
                        # If this worker is incompatible, add all *other* incompatible workers to its list
                        worker['incompatible_with'] = list(incompatible_worker_ids - {worker_id}) # Exclude self
                else:
                    # Respect existing incompatible_with list
                    logging.debug(f"Worker {worker_id} has predefined incompatible_with list: {worker['incompatible_with']}")
                logging.debug(f"Worker {worker_id} final incompatible_with list: {worker['incompatible_with']}")
            # --- END: Build incompatibility lists ---
    
            # Get the new configurable parameters with defaults from config
            default_config = SchedulerConfig.get_default_config()
            self.gap_between_shifts = config.get('gap_between_shifts', default_config['gap_between_shifts'])
            self.max_consecutive_weekends = config.get('max_consecutive_weekends', default_config['max_consecutive_weekends'])

            # Initialize tracking dictionaries
            self.schedule = {}
            self.worker_assignments = {w['id']: set() for w in self.workers_data}
            self.worker_posts = {w['id']: set() for w in self.workers_data} # CORRECTED: Initialize as a set
            self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
            self.worker_weekends = {w['id']: [] for w in self.workers_data} # This is a list of dates, which is fine
            # Initialize the schedule structure with the appropriate number of shifts for each date
            self._initialize_schedule_with_variable_shifts() 
            # Initialize tracking data structures (These seem to be for overall counts, distinct from worker_posts which tracks *which* posts)
            self.worker_shift_counts = {w['id']: 0 for w in self.workers_data}
            self.worker_weekend_counts = {w['id']: 0 for w in self.workers_data} 
            self.worker_post_counts = {w['id']: {p: 0 for p in range(self.num_shifts)} for w in self.workers_data} # This is for counting how many times each post is worked by a worker, distinct from self.worker_posts
            self.worker_weekday_counts = {w['id']: {d: 0 for d in range(7)} for w in self.workers_data}
            self.worker_holiday_counts = {w['id']: 0 for w in self.workers_data}
            self.last_assignment_date = {w['id']: None for w in self.workers_data} # Corrected attribute name
            # Initialize consecutive_shifts
            self.consecutive_shifts = {w['id']: 0 for w in self.workers_data} # <<< --- ADD THIS LINE
                      
            # Initialize worker targets
            for worker in self.workers_data:
                if 'target_shifts' not in worker:
                    worker['target_shifts'] = 0

            # Set current time and user
            # self.date_utils = DateTimeUtils() # Already initialized above
            self.current_datetime = self.date_utils.get_spain_time()
            self.current_user = 'saldo27'
        
            # Add max_shifts_per_worker calculation
            total_days = (self.end_date - self.start_date).days + 1
            total_shifts_possible = total_days * self.num_shifts # Renamed for clarity
            num_workers = len(self.workers_data)
            # Ensure num_workers is not zero to prevent DivisionByZeroError
            self.max_shifts_per_worker = (total_shifts_possible // num_workers) + 2 if num_workers > 0 else total_shifts_possible 

            # Track constraint skips
            self.constraint_skips = {
                w['id']: {
                    'gap': [],
                    'incompatibility': [],
                    'reduced_gap': []  # For part-time workers
                } for w in self.workers_data
            }
        
            # Initialize helper modules
            self.stats = StatisticsCalculator(self)
            self.constraint_checker = ConstraintChecker(self)  
            self.data_manager = DataManager(self)
            # self.schedule_builder will be initialized later in generate_schedule
            self.eligibility_tracker = WorkerEligibilityTracker(
                self.workers_data,
                self.holidays,
                self.gap_between_shifts,
                self.max_consecutive_weekends,
                start_date=self.start_date,  # Pass start_date
                end_date=self.end_date,      # Pass end_date
                date_utils=self.date_utils,  # Pass date_utils
                scheduler=self              # Pass reference to scheduler
            )

            # Initialize real-time engine (optional, only if real-time features are enabled)
            self.real_time_engine = None
            if config.get('enable_real_time', False):
                try:
                    from real_time_engine import RealTimeEngine
                    self.real_time_engine = RealTimeEngine(self)
                    logging.info("Real-time engine initialized")
                except ImportError:
                    logging.warning("Real-time engine not available - real-time features disabled")

            # Initialize predictive analytics engine (optional, only if enabled)
            self.predictive_analytics = None
            self.predictive_optimizer = None
            if config.get('enable_predictive_analytics', True):
                try:
                    from predictive_analytics import PredictiveAnalyticsEngine
                    from predictive_optimizer import PredictiveOptimizer
                    
                    predictive_config = config.get('predictive_analytics_config', {})
                    self.predictive_analytics = PredictiveAnalyticsEngine(self, predictive_config)
                    self.predictive_optimizer = PredictiveOptimizer(self, self.predictive_analytics)
                    logging.info("Predictive analytics engine initialized")
                    
                    # Auto-collect data if enabled
                    if predictive_config.get('auto_collect_data', True):
                        self.predictive_analytics.auto_collect_data_if_enabled()
                        
                except ImportError as e:
                    logging.warning(f"Predictive analytics not available - predictive features disabled: {e}")
                except Exception as e:
                    logging.error(f"Error initializing predictive analytics: {e}")

            # Sort the variable shifts by start date for efficient lookup
            self.variable_shifts.sort(key=lambda x: x['start_date'])
    
            # Calculate targets before proceeding
            self._calculate_target_shifts()

            self._log_initialization()

            # The ScheduleBuilder is now initialized within the generate_schedule method
            # after the scheduler's own state for the run is fully prepared.
            # self.schedule_builder = ScheduleBuilder(self) # This line is moved to generate_schedule

        except Exception as e:
            logging.error(f"Initialization error: {str(e)}", exc_info=True) # Added exc_info=True
            raise SchedulerError(f"Failed to initialize scheduler: {str(e)}")

    def _get_cache_key(self, method_name: str, *args) -> str:
        """Generate a cache key for method results"""
        return f"{method_name}:{hash(str(args))}"
    
    def _get_cached_result(self, cache_key: str) -> Optional[Any]:
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
        
        
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration parameters using the enhanced configuration validator.
    
        Args:
            config: Dictionary containing schedule configuration
        
        Raises:
            SchedulerError: If configuration is invalid
        """
        # Use the enhanced configuration validation
        is_valid, error_message = SchedulerConfig.validate_config(config)
        if not is_valid:
            raise SchedulerError(error_message)

        # Additional validation specific to scheduler needs
        # Validate date range
        if not isinstance(config['start_date'], datetime) or not isinstance(config['end_date'], datetime):
            raise SchedulerError("Start date and end date must be datetime objects")
        
        if config['start_date'] > config['end_date']:
            raise SchedulerError("Start date must be before end date")

        # Validate workers data
        if not config['workers_data'] or not isinstance(config['workers_data'], list):
            raise SchedulerError("workers_data must be a non-empty list")
            
        # Validate each worker's data
        for worker in config['workers_data']:
            if not isinstance(worker, dict):
                raise SchedulerError("Each worker must be a dictionary")
            
            if 'id' not in worker:
                raise SchedulerError("Each worker must have an 'id' field")
            
            # Validate work percentage if present
            if 'work_percentage' in worker:
                try:
                    work_percentage = float(str(worker['work_percentage']).strip()) if worker['work_percentage'] else 100
                    # Si es 0 o vacío, usar 100% por defecto
                    if work_percentage == 0:
                        worker['work_percentage'] = 100
                        work_percentage = 100
                    elif work_percentage < 0 or work_percentage > 100:
                        raise SchedulerError(f"Invalid work percentage for worker {worker['id']}: {work_percentage}")
                except (ValueError, TypeError):
                    # Si hay error de conversión, usar 100% por defecto
                    worker['work_percentage'] = 100

            # Validate date formats in mandatory_days if present
            if 'mandatory_days' in worker:
                try:
                    self.date_utils.parse_dates(worker['mandatory_days'])
                except ValueError as e:
                    raise SchedulerError(f"Invalid mandatory_days format for worker {worker['id']}: {str(e)}")

            # Validate date formats in days_off if present
            if 'days_off' in worker:
                try:
                    self.date_utils.parse_date_ranges(worker['days_off'])
                except ValueError as e:
                    raise SchedulerError(f"Invalid days_off format for worker {worker['id']}: {str(e)}")

        # Validate holidays if present
        if 'holidays' in config:
            if not isinstance(config['holidays'], list):
                raise SchedulerError("holidays must be a list")
            
            for holiday in config['holidays']:
                if not isinstance(holiday, datetime):
                    raise SchedulerError("Each holiday must be a datetime object")
                
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
            if 'work_periods' not in worker or not worker['work_periods'].strip():
                start_str = self.start_date.strftime('%d-%m-%Y')
                end_str = self.end_date.strftime('%d-%m-%Y')
                worker['work_periods'] = f"{start_str} - {end_str}"
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
        var_cfgs = [
            (cfg['start_date'], cfg['end_date'], cfg['shifts'])
            for cfg in self.variable_shifts
        ]
        while current_date <= self.end_date:
            # Determine how many shifts this date should have
            shifts_for_date = self.num_shifts
            for start, end, cnt in var_cfgs:
                if start <= current_date <= end:
                    shifts_for_date = cnt
                    logging.info(f"Variable shifts applied for {current_date}: {cnt} shifts (default is {self.num_shifts})")
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
        self.worker_assignments = {w['id']: set() for w in self.workers_data}
        self.worker_posts = {w['id']: set() for w in self.workers_data}
        self.worker_weekdays = {w['id']: {i: 0 for i in range(7)} for w in self.workers_data}
        self.worker_weekends = {w['id']: [] for w in self.workers_data}
        self.constraint_skips = {
            w['id']: {'gap': [], 'incompatibility': [], 'reduced_gap': []}
            for w in self.workers_data
        }
        
    def _ensure_data_integrity(self):
        """
        Ensure all data structures are consistent before schedule generation
        """
        logging.info("Ensuring data integrity...")
    
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
            for worker_id in (w['id'] for w in self.workers_data):
                self.worker_assignments[worker_id].clear()
                
                # CRITICAL FIX: Ensure worker_posts is a set, not dict
                if not isinstance(self.worker_posts[worker_id], set):
                    logging.warning(f"Correcting worker_posts[{worker_id}] from {type(self.worker_posts[worker_id])} to set")
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
                is_weekend_or_holiday = (weekday >= 4 or 
                                      date in self.holidays or 
                                      (date + timedelta(days=1)) in self.holidays)
                
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
        
            logging.info("Tracking data synchronization complete.")
            return True
        except Exception as e:
            logging.error(f"Error synchronizing tracking data: {str(e)}", exc_info=True)
            return False
        
    def _validate_data_synchronization(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate that worker_assignments and schedule are perfectly synchronized.
        
        Returns:
            Tuple[bool, Dict]: (is_synchronized, validation_report)
        """
        logging.debug("Validating data synchronization between worker_assignments and schedule...")
        
        try:
            validation_report = {
                'is_synchronized': True,
                'discrepancies': [],
                'summary': {
                    'total_workers': len(self.workers_data),
                    'workers_with_issues': 0,
                    'total_assignments_schedule': 0,
                    'total_assignments_tracking': 0,
                    'missing_from_tracking': 0,
                    'extra_in_tracking': 0
                }
            }
            
            # Build assignments from schedule for comparison
            schedule_assignments = {}
            for worker in self.workers_data:
                schedule_assignments[worker['id']] = set()
            
            for date, shifts in self.schedule.items():
                validation_report['summary']['total_assignments_schedule'] += len([s for s in shifts if s is not None])
                for shift_idx, worker_id in enumerate(shifts):
                    if worker_id is not None:
                        if worker_id not in schedule_assignments:
                            schedule_assignments[worker_id] = set()
                        schedule_assignments[worker_id].add(date)
            
            # Count total assignments in tracking
            for worker_id, assignments in self.worker_assignments.items():
                validation_report['summary']['total_assignments_tracking'] += len(assignments)
            
            # Compare each worker's assignments
            for worker_id in set(list(self.worker_assignments.keys()) + list(schedule_assignments.keys())):
                tracking_assignments = self.worker_assignments.get(worker_id, set())
                schedule_worker_assignments = schedule_assignments.get(worker_id, set())
                
                missing_from_tracking = schedule_worker_assignments - tracking_assignments
                extra_in_tracking = tracking_assignments - schedule_worker_assignments
                
                if missing_from_tracking or extra_in_tracking:
                    validation_report['is_synchronized'] = False
                    validation_report['summary']['workers_with_issues'] += 1
                    validation_report['summary']['missing_from_tracking'] += len(missing_from_tracking)
                    validation_report['summary']['extra_in_tracking'] += len(extra_in_tracking)
                    
                    discrepancy = {
                        'worker_id': worker_id,
                        'missing_from_tracking': sorted([d.strftime('%Y-%m-%d') for d in missing_from_tracking]),
                        'extra_in_tracking': sorted([d.strftime('%Y-%m-%d') for d in extra_in_tracking]),
                        'tracking_count': len(tracking_assignments),
                        'schedule_count': len(schedule_worker_assignments)
                    }
                    validation_report['discrepancies'].append(discrepancy)
            
            # Log summary
            if validation_report['is_synchronized']:
                logging.debug("✓ Data synchronization validation passed: worker_assignments and schedule are synchronized")
            else:
                logging.warning(f"✗ Data synchronization issues detected: {len(validation_report['discrepancies'])} workers affected")
                for discrepancy in validation_report['discrepancies'][:3]:  # Log first 3 issues
                    worker_id = discrepancy['worker_id']
                    logging.warning(f"  Worker {worker_id}: {len(discrepancy['missing_from_tracking'])} missing, {len(discrepancy['extra_in_tracking'])} extra")
            
            return validation_report['is_synchronized'], validation_report
            
        except Exception as e:
            logging.error(f"Error validating data synchronization: {str(e)}", exc_info=True)
            return False, {'error': str(e), 'is_synchronized': False}
    
    def _repair_data_synchronization(self, validation_report: Dict[str, Any] = None) -> bool:
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
                corrected_assignments[worker['id']] = set()
            
            for date, shifts in self.schedule.items():
                for shift_idx, worker_id in enumerate(shifts):
                    if worker_id is not None:
                        if worker_id not in corrected_assignments:
                            corrected_assignments[worker_id] = set()
                        corrected_assignments[worker_id].add(date)
            
            # Replace worker_assignments with corrected version
            self.worker_assignments = corrected_assignments
            
            # Verify the repair
            is_synchronized_after, validation_after = self._validate_data_synchronization()
            
            if is_synchronized_after:
                total_fixes = validation_report.get('summary', {}).get('missing_from_tracking', 0) + \
                             validation_report.get('summary', {}).get('extra_in_tracking', 0)
                logging.info(f"✓ Data synchronization repair successful: Fixed {total_fixes} inconsistencies")
                return True
            else:
                logging.error("✗ Data synchronization repair failed: Issues still persist")
                return False
                
        except Exception as e:
            logging.error(f"Error repairing data synchronization: {str(e)}", exc_info=True)
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
            logging.error(f"Error ensuring data synchronization: {str(e)}", exc_info=True)
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
            logging.error(f"Error reconciling schedule tracking: {str(e)}", exc_info=True)
            return False
        
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
                logging.warning(f"Re-initializing self.worker_posts[{worker_id}] as a set due to incorrect type.") # Optional: log this
                self.worker_posts[worker_id] = set() 
            
            if worker_id not in self.worker_weekdays: 
                self.worker_weekdays[worker_id] = {i: 0 for i in range(7)}
            if worker_id not in self.worker_weekends: 
                self.worker_weekends[worker_id] = [] # List of weekend/holiday dates worked

            if removing:
                # Remove from worker assignments
                if date in self.worker_assignments.get(worker_id, set()):
                    self.worker_assignments[worker_id].remove(date)
                
                # Note: Removing from self.worker_posts for a specific post is not done here
                # as self.worker_posts[worker_id] is a set of all posts worked.
                # If a worker no longer works ANY instance of a post, that post would remain
                # in their set unless explicitly managed or self.worker_posts is rebuilt.

                # Update weekday counts
                weekday = date.weekday()
                # Ensure weekday key exists before decrementing, though init above should handle it.
                if weekday in self.worker_weekdays.get(worker_id, {}): # Defensive access
                    if self.worker_weekdays[worker_id][weekday] > 0:
                        self.worker_weekdays[worker_id][weekday] -= 1
                else:
                    logging.warning(f"Weekday {weekday} not found in self.worker_weekdays for worker {worker_id} during removal.")


                # Update weekend tracking
                is_special_day = (date.weekday() >= 4 or
                                  date in self.holidays or
                                  (date + timedelta(days=1)) in self.holidays)
                
                if is_special_day:
                    current_weekends = self.worker_weekends.get(worker_id) # Use .get for safety
                    if current_weekends is not None and date in current_weekends:
                        current_weekends.remove(date)
                        # current_weekends.sort() # Re-sort if removal changes order and order matters

            else: # Adding assignment
                self.worker_assignments[worker_id].add(date)
                self.worker_posts[worker_id].add(post) # This should now work
                
                weekday = date.weekday()
                self.worker_weekdays[worker_id][weekday] = self.worker_weekdays[worker_id].get(weekday, 0) + 1


                is_special_day = (date.weekday() >= 4 or
                                  date in self.holidays or
                                  (date + timedelta(days=1)) in self.holidays)

                if is_special_day:
                    current_weekends = self.worker_weekends.setdefault(worker_id, []) # Ensures list exists
                    if date not in current_weekends:
                        current_weekends.append(date)
                        current_weekends.sort()

            # Update eligibility tracker if it exists and is configured
            if hasattr(self, 'eligibility_tracker') and self.eligibility_tracker:
                if removing:
                    self.eligibility_tracker.remove_worker_assignment(worker_id, date)
                else:
                    self.eligibility_tracker.update_worker_status(worker_id, date)
            
            # ENHANCED: Validate synchronization after update
            # This is a critical addition to catch synchronization issues immediately
            if hasattr(self, '_validate_assignment_consistency'):
                if not self._validate_assignment_consistency(worker_id, date, removing):
                    logging.error(f"SYNC ERROR: Data synchronization issue detected after {'removing' if removing else 'adding'} worker {worker_id} on {date.strftime('%Y-%m-%d')}")
                    # Attempt automatic repair
                    if hasattr(self, '_ensure_data_synchronization'):
                        self._ensure_data_synchronization()

            logging.debug(f"{'Removed' if removing else 'Added'} assignment and updated tracking for worker {worker_id} on {date.strftime('%Y-%m-%d')}, post {post}")

        except Exception as e:
            logging.error(f"Error in _update_tracking_data for worker {worker_id}, date {date}, post {post}, removing={removing}: {str(e)}", exc_info=True)
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
            is_in_schedule = (date in self.schedule and 
                             worker_id in self.schedule.get(date, []))
            
            # Check if worker is in tracking for this date
            is_in_tracking = (worker_id in self.worker_assignments and 
                             date in self.worker_assignments.get(worker_id, set()))
            
            if removing:
                # After removal, worker should not be in either structure
                if is_in_schedule or is_in_tracking:
                    logging.debug(f"Inconsistency after removal: worker {worker_id} still found in {'schedule' if is_in_schedule else 'tracking'} for {date.strftime('%Y-%m-%d')}")
                    return False
            else:
                # After addition, worker should be in both structures
                if is_in_schedule != is_in_tracking:
                    logging.debug(f"Inconsistency after addition: worker {worker_id} found in {'schedule' if is_in_schedule else 'tracking'} but not {'tracking' if is_in_schedule else 'schedule'} for {date.strftime('%Y-%m-%d')}")
                    return False
            
            return True
            
        except Exception as e:
            logging.error(f"Error validating assignment consistency: {str(e)}", exc_info=True)
            return False
        
    # ========================================
    # 3. TARGET AND CALCULATION METHODS
    # ========================================
    def _calculate_target_shifts(self):
        """
        Recalculate each worker's target_shifts by:
          1) Counting slots they can work (based on work_periods & days_off)
          2) Weighting those slots by their work_percentage
          3) Allocating all schedule slots proportionally (largest‐remainder rounding)
        """
        try:
            logging.info("Calculating target shifts based on availability and percentage")

            # 1) Total open slots in the schedule (variable shifts considered)
            total_slots = sum(len(slots) for slots in self.schedule.values())
            if total_slots <= 0:
                logging.warning("No slots in schedule; skipping allocation")
                return False

            # 2) Compute available_slots per worker
            available_slots = {}
            for w in self.workers_data:
                wid = w['id']
                wp = w.get('work_periods','').strip()
                dp = w.get('days_off','').strip()
                work_ranges = (self.date_utils.parse_date_ranges(wp) 
                               if wp else [(self.start_date, self.end_date)])
                off_ranges  = (self.date_utils.parse_date_ranges(dp) 
                               if dp else [])
                count = 0
                for date, slots in self.schedule.items():
                    in_work = any(s <= date <= e for s, e in work_ranges)
                    in_off  = any(s <= date <= e for s, e in off_ranges)
                    if in_work and not in_off:
                        count += len(slots)
                available_slots[wid] = count
                logging.debug(f"Worker {wid}: available_slots={count}")

            # 3) Build weight = available_slots * (work_percentage/100)
            weights = []
            for w in self.workers_data:
                wid = w['id']
                pct = 1.0
                try:
                    pct = float(str(w.get('work_percentage',100)).strip())/100.0
                except Exception:
                    logging.warning(f"Worker {wid} invalid work_percentage; defaulting to 100%")
                pct = max(0.0, pct)
                weights.append(available_slots.get(wid,0) * pct)

            total_weight = sum(weights) or 1.0

            # 4) Compute exact fractional targets
            exact_targets = [wgt/total_weight*total_slots for wgt in weights]

            # 5) Largest-remainder rounding
            floors = [int(x) for x in exact_targets]
            remainder = int(total_slots - sum(floors))
            fracs = sorted(enumerate(exact_targets),
                           key=lambda ix: exact_targets[ix[0]] - floors[ix[0]],
                           reverse=True)
            targets = floors[:]
            for idx, _ in fracs[:remainder]:
                targets[idx] += 1

            # 6) Assign and log (subtract out mandatory days so they're not extra)
            for i, w in enumerate(self.workers_data):
                raw_target = targets[i]
                mand_count = 0
                mand_str = w.get('mandatory_days', '').strip()
                if mand_str:
                    try:
                        mand_dates = self.date_utils.parse_dates(mand_str)
                        mand_count = sum(1 for d in mand_dates
                                         if self.start_date <= d <= self.end_date)
                    except Exception as e:
                        logging.error(f"Failed to parse mandatory_days for {w['id']}: {e}")
                adjusted = max(0, raw_target - mand_count)
                w['target_shifts'] = adjusted
                logging.info(
                    f"Worker {w['id']}: target_shifts={raw_target} → {adjusted}"
                    f"{' (−'+str(mand_count)+' mandatory)' if mand_count else ''}"
                )

            for i,w in enumerate(self.workers_data):
                raw = targets[i]
                mand_count = 0
                if w.get('mandatory_days','').strip():
                    mand_count = sum(1 for d in self.date_utils.parse_dates(w['mandatory_days'])
                                     if self.start_date <= d <= self.end_date)
            w['_raw_target']      = raw
            w['_mandatory_count'] = mand_count
            w['target_shifts']    = max(0, raw - mand_count)
            
            return True
        
        except Exception as e:
            logging.error(f"Error in target calculation: {e}", exc_info=True)
            return False
        
    def _adjust_for_mandatory(self):
        """
        Mandatory days are not extra shifts: reduce each worker's
        target_shifts by the number of mandatories in range.
        """
        for w in self.workers_data:
            mand_list = []
            try:
                mand_list = self.date_utils.parse_dates(w.get('mandatory_days',''))
            except Exception:
                pass

            mand_count = sum(1 for d in mand_list
                             if self.start_date <= d <= self.end_date)
            # never go below zero
            new_target = max(0, w.get('target_shifts',0) - mand_count)
            logging.info(f"[Worker {w['id']}] target_shifts {w['target_shifts']} → {new_target} after mandatory")
            w['target_shifts'] = new_target

    def _calculate_monthly_targets(self):
        """
        Calculate monthly target shifts for each worker based on their overall targets
        """
        logging.info("Calculating monthly target distribution...")
    
        # Calculate available days per month
        month_days = self._get_schedule_months()
        total_days = (self.end_date - self.start_date).days + 1
    
        # Initialize monthly targets for each worker
        for worker in self.workers_data:
            worker_id = worker['id']
            overall_target = worker.get('target_shifts', 0)
        
            # Initialize or reset monthly targets
            if 'monthly_targets' not in worker:
                worker['monthly_targets'] = {}
            
            # Distribute target shifts proportionally by month
            remaining_target = overall_target
            for month_key, days_in_month in month_days.items():
                # Calculate proportion of shifts for this month
                month_proportion = days_in_month / total_days
                month_target = round(overall_target * month_proportion)
            
                # Ensure we don't exceed overall target
                month_target = min(month_target, remaining_target)
                worker['monthly_targets'][month_key] = month_target
                remaining_target -= month_target
            
                logging.debug(f"Worker {worker_id}: {month_key} → {month_target} shifts")
        
            # Handle any remaining shifts due to rounding
            if remaining_target > 0:
                # Distribute remaining shifts to months with most days first
                sorted_months = sorted(month_days.items(), key=lambda x: x[1], reverse=True)
                for month_key, _ in sorted_months:
                    if remaining_target <= 0:
                        break
                    worker['monthly_targets'][month_key] += 1
                    remaining_target -= 1
                    logging.debug(f"Worker {worker_id}: Added +1 to {month_key} for rounding")
    
        # Log the results
        logging.info("Monthly targets calculated")
        return True
        
    def _get_schedule_months(self):
        """
        Calculate number of months in schedule period considering partial months
    
        Returns:
            dict: Dictionary with month keys and their available days count
        """
        month_days = {}
        current = self.start_date
        while current <= self.end_date:
            month_key = f"{current.year}-{current.month:02d}"
        
            # Calculate available days for this month
            month_start = max(
                current.replace(day=1),
                self.start_date
            )
            month_end = min(
                (current.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1),
                self.end_date
            )
        
            days_in_month = (month_end - month_start).days + 1
            month_days[month_key] = days_in_month
        
            # Move to first day of next month
            current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
    
        return month_days

    def _get_shifts_for_date(self, date):
        """Determine the number of shifts for a specific date based on variable_shifts."""
        logging.debug(f"Checking variable shifts for date: {date}")
        # Normalize to date-only if datetime
        check_date = date.date() if hasattr(date, 'date') else date
        for cfg in self.variable_shifts:
            start = cfg.get('start_date')
            end   = cfg.get('end_date')
            shifts = cfg.get('shifts')
            # Normalize
            sd = start.date() if hasattr(start, 'date') else start
            ed = end.date() if hasattr(end, 'date') else end
            if sd <= check_date <= ed:
                logging.debug(f"Variable shifts: {shifts} for {date}")
                return shifts
        # Fallback to default
        logging.debug(f"No variable shift override for {date}, default={self.num_shifts}")
        return self.num_shifts
    
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
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                logging.warning(f"_is_allowed_assignment: Worker {worker_id} not found in workers_data.")
                result = False
                self._set_cached_result(cache_key, result)
                return result
        
            # Check if worker is already assigned on this date (any post)
            if date in self.schedule and worker_id in self.schedule.get(date, []):
                logging.debug(f"_is_allowed_assignment: Worker {worker_id} already assigned on {date.strftime('%Y-%m-%d')}")
                result = False
                self._set_cached_result(cache_key, result)
                return result
    
            worker_assignments_set = self.worker_assignments.get(worker_id, set())
            if not isinstance(worker_assignments_set, set):
                worker_assignments_set = set()

            # Get work percentage once for efficiency
            work_percentage = worker.get('work_percentage', 100)
            min_days_required_between = self.gap_between_shifts + 1
            if work_percentage < 70:  # Part-time threshold
                 min_days_required_between = max(min_days_required_between, self.gap_between_shifts + 2)

            # Optimized constraint checking loop
            for assigned_date in worker_assignments_set:
                if assigned_date == date: 
                    continue

                days_difference = abs((date - assigned_date).days)
    
                # 1. Basic minimum gap check
                if days_difference < min_days_required_between:
                    logging.debug(f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails gap with {assigned_date.strftime('%Y-%m-%d')} ({days_difference} < {min_days_required_between})")
                    result = False
                    self._set_cached_result(cache_key, result)
                    return result
        
                # 2. Special case for Friday-Monday (if base gap allows 3-day span)
                if self.gap_between_shifts <= 1 and days_difference == 3:
                    assigned_weekday = assigned_date.weekday()
                    date_weekday = date.weekday()
                    if ((assigned_weekday == 4 and date_weekday == 0) or 
                        (assigned_weekday == 0 and date_weekday == 4)):
                        logging.debug(f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails Fri-Mon rule with {assigned_date.strftime('%Y-%m-%d')}")
                        result = False
                        self._set_cached_result(cache_key, result)
                        return result
            
                # 3. Reject 7- or 14-day same-weekday patterns
                if self._is_weekly_pattern(days_difference) and date.weekday() == assigned_date.weekday():
                    logging.debug(f"_is_allowed_assignment: Worker {worker_id} on {date.strftime('%Y-%m-%d')} fails 7/14 day pattern with {assigned_date.strftime('%Y-%m-%d')}")
                    result = False
                    self._set_cached_result(cache_key, result)
                    return result

            # 4. Optimized incompatibility check
            if date in self.schedule:
                assigned_on_date_others = [w_id for w_id in self.schedule[date] if w_id is not None and w_id != worker_id]
                worker_incompat_list = worker.get('incompatible_with', [])
                
                # Quick check if any incompatible workers are assigned
                if worker_incompat_list and any(str(other_id) in worker_incompat_list for other_id in assigned_on_date_others):
                    logging.debug(f"_is_allowed_assignment: Worker {worker_id} incompatible with assigned workers on {date.strftime('%Y-%m-%d')}")
                    result = False
                    self._set_cached_result(cache_key, result)
                    return result
    
            result = True
            self._set_cached_result(cache_key, result)
            return result
            
        except Exception as e:
            logging.error(f"Error in Scheduler._is_allowed_assignment for worker {worker_id} on {date}: {str(e)}", exc_info=True)
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
        worker_assignment_counts = {w['id']: 0 for w in self.workers_data}
        worker_targets = {w['id']: w.get('target_shifts', 1) for w in self.workers_data}
    
        # Sort workers by targets (highest first) to prioritize those who need more shifts
        workers_by_priority = sorted(
            self.workers_data, 
            key=lambda w: worker_targets.get(w['id'], 0),
            reverse=True
        )    
    
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
                    worker_id = worker['id']

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
    
                        # We need at least gap_between_shifts days off, so (gap+1)+ days between assignments
                        min_days_between = self.gap_between_shifts + 1
                        if days_difference < min_days_between:
                            too_close = True
                            break
    
                        # Special case: Friday-Monday (needs 3 days off, so 4+ days between)
                        if days_difference == 3:
                            if ((date.weekday() == 0 and assigned_date.weekday() == 4) or 
                                (date.weekday() == 4 and assigned_date.weekday() == 0)):
                                too_close = True
                                break
    
                        # Check for weekly-pattern (7 or 14 days, same weekday)
                        if self._is_weekly_pattern(days_difference) and date.weekday() == assigned_date.weekday():
                            too_close = True
    
                    if too_close:
                        continue
                    
                    # Check for worker incompatibilities
                    incompatible_with = worker.get('incompatible_with', [])
                    if incompatible_with:
                        has_conflict = False
                        for incompatible_id in incompatible_with:
                            if incompatible_id in currently_assigned:
                                has_conflict = True
                                break

                        if has_conflict:
                            continue
                
                    # This worker is a good candidate
                    best_worker = worker
                    break
            
                # If we found a suitable worker, assign them
                if best_worker:
                    worker_id = best_worker['id']
            
                    # Make sure the schedule list exists and has the right size
                    if date not in self.schedule:
                        self.schedule[date] = []
                
                    while len(self.schedule[date]) <= post:
                        self.schedule[date].append(None)
                    
                    # CRITICAL: Verify slot is not protected by mandatory
                    if self.schedule[date][post] is not None:
                        existing = self.schedule[date][post]
                        if hasattr(self, 'schedule_builder'):
                            if ((existing, date) in self.schedule_builder._locked_mandatory or 
                                self.schedule_builder._is_mandatory(existing, date)):
                                logging.warning(f"🔒 BLOCKED: Cannot overwrite MANDATORY {existing} on {date.strftime('%Y-%m-%d')} post {post}")
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
        logging.info(f"Simple assignment complete: {total_assigned}/{total_shifts} shifts assigned ({total_assigned/total_shifts*100:.1f}%)")
    
        return total_assigned > 0
        
    def _check_schedule_constraints(self):
        """
        Check the current schedule for constraint violations.
        Returns a list of violations found.
        """
        violations = []
        
        try:
            # Check for minimum rest days violations, Friday-Monday patterns, and weekly patterns
            for worker in self.workers_data:
                worker_id = worker['id']
                if worker_id not in self.worker_assignments:
                    continue
            
                # Sort the worker's assignments by date
                assigned_dates = sorted(list(self.worker_assignments[worker_id]))
            
                # Check all pairs of dates for violations
                for i, date1 in enumerate(assigned_dates):
                    for j, date2 in enumerate(assigned_dates):
                        if i >= j:  # Skip same date or already checked pairs
                            continue
                    
                        days_between = abs((date2 - date1).days)
                    
                        # When checking for insufficient rest periods
                        if 0 < days_between < self.gap_between_shifts + 1:
                            violations.append({
                                'type': 'min_rest_days',
                                'worker_id': worker_id,
                                'date1': date1,
                                'date2': date2,
                                'days_between': days_between,
                                'min_required': self.gap_between_shifts
                            })
                    
                        # Check for Friday-Monday assignments (special case requiring 3 days)
                        if days_between == 3:
                            if ((date1.weekday() == 4 and date2.weekday() == 0) or 
                                (date1.weekday() == 0 and date2.weekday() == 4)):
                                violations.append({
                                    'type': 'friday_monday_pattern',
                                    'worker_id': worker_id,
                                    'date1': date1,
                                    'date2': date2,
                                    'days_between': days_between
                                })
                    
                        # Check for 7 or 14 day patterns
                        if (days_between == 7 or days_between == 14) and date1.weekday() == date2.weekday(): # CORRECTED LOGIC + WEEKDAY CHECK
                            violations.append({
                                'type': 'weekly_pattern',        # Ensure this and following lines are indented correctly
                                'worker_id': worker_id,
                                'date1': date1,
                                'date2': date2,
                                'days_between': days_between
                            })
        
            # Check for incompatibility violations
            for date in self.schedule.keys():
                workers_assigned = [w for w in self.schedule.get(date, []) if w is not None]
            
                # Check each worker against others for incompatibility
                for worker_id in workers_assigned:
                    worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
                    if not worker:
                        continue
                    
                    incompatible_with = worker.get('incompatible_with', [])
                    for incompatible_id in incompatible_with:
                        if incompatible_id in workers_assigned:
                            violations.append({
                                'type': 'incompatibility',
                                'worker_id': worker_id,
                                'incompatible_id': incompatible_id,
                                'date': date
                            })
        
            # Log summary of violations
            if violations:
                logging.warning(f"Found {len(violations)} constraint violations in schedule")
                for i, v in enumerate(violations[:5]):  # Log first 5 violations
                    if v['type'] == 'min_rest_days':
                        logging.warning(f"Violation {i+1}: Worker {v['worker_id']} has only {v['days_between']} days between shifts on {v['date1']} and {v['date2']} (min required: {v['min_required']})")
                    elif v['type'] == 'friday_monday_pattern':
                        logging.warning(f"Violation {i+1}: Worker {v['worker_id']} has Friday-Monday assignment on {v['date1']} and {v['date2']}")
                    elif v['type'] == 'weekly_pattern':
                        logging.warning(f"Violation {i+1}: Worker {v['worker_id']} has shifts exactly {v['days_between']} days apart on {v['date1']} and {v['date2']}")
                    elif v['type'] == 'incompatibility':
                        logging.warning(f"Violation {i+1}: Incompatible workers {v['worker_id']} and {v['incompatible_id']} are both assigned on {v['date']}")
                
                if len(violations) > 5:
                    logging.warning(f"...and {len(violations) - 5} more violations")
            
            return violations
        except Exception as e:
            logging.error(f"Error checking schedule constraints: {str(e)}", exc_info=True)
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
                if violation['type'] == 'min_rest_days' or violation['type'] == 'weekly_pattern':
                    # Fix by unassigning one of the shifts
                    worker_id = violation['worker_id']
                    date1 = violation['date1']
                    date2 = violation['date2']
                
                    # CRITICAL: Check if either date is mandatory
                    date1_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date1)
                    date2_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date2)
                    
                    # If both are mandatory, we cannot fix this - it's a configuration error
                    if date1_is_mandatory and date2_is_mandatory:
                        logging.error(f"Cannot fix violation: Worker {worker_id} has mandatory assignments on both {date1} and {date2} which violate constraints. This is a configuration error.")
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
                        if hasattr(self, 'schedule_builder'):
                            if not self.schedule_builder._can_modify_assignment(worker_id, date_to_unassign, "fix_constraint_rest"):
                                logging.warning(f"🔒 BLOCKED: Cannot unassign MANDATORY {worker_id} from {date_to_unassign.strftime('%Y-%m-%d')}")
                                continue
                        
                        # Unassign this worker
                        self.schedule[date_to_unassign][shift_num] = None
                        self.worker_assignments[worker_id].remove(date_to_unassign)
                        violation_type = "rest period" if violation['type'] == 'min_rest_days' else "weekly pattern"
                        logging.info(f"Fixed {violation_type} violation: Unassigned worker {worker_id} from {date_to_unassign}")
                        fixes_made += 1
                
                elif violation['type'] == 'incompatibility':
                    # Fix incompatibility by unassigning one of the workers
                    worker_id = violation['worker_id']
                    incompatible_id = violation['incompatible_id']
                    date = violation['date']
                
                    # CRITICAL: Check if either worker has a mandatory assignment for this date
                    worker_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date)
                    incompatible_is_mandatory = self.schedule_builder._is_mandatory(incompatible_id, date)
                    
                    # If both are mandatory, we cannot fix this - it's a configuration error
                    if worker_is_mandatory and incompatible_is_mandatory:
                        logging.error(f"Cannot fix incompatibility: Both workers {worker_id} and {incompatible_id} have mandatory assignments on {date} but are incompatible. This is a configuration error.")
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
                        if hasattr(self, 'schedule_builder'):
                            if not self.schedule_builder._can_modify_assignment(worker_to_unassign, date, "fix_constraint_incompat"):
                                logging.warning(f"🔒 BLOCKED: Cannot unassign MANDATORY {worker_to_unassign} from {date.strftime('%Y-%m-%d')}")
                                continue
                        
                        # Unassign this worker
                        self.schedule[date][shift_num] = None
                        self.worker_assignments[worker_to_unassign].remove(date)
                        logging.info(f"Fixed incompatibility violation: Unassigned worker {worker_to_unassign} from {date}")
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
            logging.error(f"Error fixing constraint violations: {str(e)}", exc_info=True)
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
        from scheduler_core import SchedulerCore
        
        # Create scheduler core for orchestration
        scheduler_core = SchedulerCore(self)
        
        # Use orchestrated workflow
        return scheduler_core.orchestrate_schedule_generation(max_improvement_loops)
   
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
        This is a placeholder and should be implemented with actual scoring logic.
        """
        logging.debug("Scheduler.calculate_score called (placeholder)")
        # Placeholder scoring logic:
        # For example, count filled shifts, penalize constraint violations, etc.
        score = 0
        
        current_schedule = schedule_to_score if schedule_to_score is not None else self.schedule
        current_assignments = assignments_to_score if assignments_to_score is not None else self.worker_assignments

        if not current_schedule:
            return float('-inf') # Or 0, depending on how you want to score empty schedules

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
            score = 0 # Or float('-inf') if an empty schedule structure is invalid

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
            total_shifts = (self.end_date - self.start_date).days + 1  # One shift per day
            total_shifts *= self.num_shifts  # Multiply by number of shifts per day
        
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
            logging.error(f"Error calculating coverage: {str(e)}", exc_info=True)
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
            if isinstance(rotation_data, dict) and 'uniformity' in rotation_data and 'avg_worker' in rotation_data:
                return rotation_data
            
            # Otherwise, create a dictionary with the required structure
            # Use the value from rotation_data if it's a scalar, or fallback to a default
            overall_score = rotation_data if isinstance(rotation_data, (int, float)) else 40.0
        
            return {
                'overall_score': overall_score,
                'uniformity': 0.0,  # Default value
                'avg_worker': 100.0  # Default value
            }
        except Exception as e:
            logging.error(f"Error in calculating post rotation: {str(e)}")
            # Return a default dictionary with all required keys
            return {
                'overall_score': 40.0,
                'uniformity': 0.0,
                'avg_worker': 100.0
            }
        
    def _calculate_post_rotation_coverage(self):
        """
        Calculate post rotation coverage metrics
    
        Evaluates how well posts are distributed across workers
    
        Returns:
            dict: Dictionary containing post rotation metrics
        """
        logging.info("Calculating post rotation coverage...")
    
        # Initialize metrics
        metrics = {
            'overall_score': 0,
            'worker_scores': {},
            'post_distribution': {}
        }
    
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
                metrics['post_distribution'][post] = {
                    'count': count,
                    'percentage': (count / total_assignments * 100) if total_assignments > 0 else 0
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
            worker_id = worker['id']
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
        metrics['overall_score'] = (post_uniformity * 0.6) + (avg_worker_score * 0.4)
        metrics['post_uniformity'] = post_uniformity
        metrics['avg_worker_score'] = avg_worker_score
        metrics['worker_scores'] = worker_scores
    
        logging.info(f"Post rotation overall score: {metrics['overall_score']:.2f}%")
        logging.info(f"Post uniformity: {post_uniformity:.2f}%, Avg worker score: {avg_worker_score:.2f}%")
    
        return metrics
        
    # ========================================
    # 7. BACKUP AND RESTORE OPERATIONS
    # ========================================
    def _save_current_as_best(self):
        """
        Save the current schedule as the best schedule found so far.
        """
        try:
            logging.debug("Saving current schedule as best...")
        
            # Create a deep copy of the current schedule
            best_schedule = {}
            for date, shifts in self.schedule.items():
                best_schedule[date] = shifts.copy()
            
            # Save all tracking data
            self.best_schedule_data = {
                'schedule': best_schedule,
                'worker_assignments': {w_id: assignments.copy() for w_id, assignments in self.worker_assignments.items()},
                'worker_posts': {w_id: posts.copy() for w_id, posts in self.worker_posts.items()},
                'worker_weekdays': {w_id: counts.copy() for w_id, counts in self.worker_weekdays.items()},
                'worker_weekends': {w_id: dates.copy() for w_id, dates in self.worker_weekends.items()},
                'worker_shift_counts': self.worker_shift_counts.copy() if hasattr(self, 'worker_shift_counts') else None,
                'worker_weekend_counts': self.worker_weekend_counts.copy() if hasattr(self, 'worker_weekend_counts') else None,
                'score': self.calculate_score()
            }
        
            logging.debug(f"Saved best schedule with score: {self.best_schedule_data['score']}")
            return True
        except Exception as e:
            logging.error(f"Error saving best schedule: {str(e)}", exc_info=True)
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
            self.backup_worker_posts = {
                worker_id: posts.copy() for worker_id, posts in self.worker_posts.items()
            }
        
            self.backup_worker_weekdays = {
                worker_id: weekdays.copy() for worker_id, weekdays in self.worker_weekdays.items()
            }
        
            self.backup_worker_weekends = {
                worker_id: weekends.copy() for worker_id, weekends in self.worker_weekends.items()
            }    
        
            # Only backup constraint_skips if it exists to avoid errors
            if hasattr(self, 'constraint_skips'):
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
            logging.error(f"Error in scheduler backup: {str(e)}", exc_info=True)
            return False

    def _restore_best_schedule(self):
        """Restore the backed up schedule"""
        try:
            if not hasattr(self, 'backup_schedule'):
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
            if hasattr(self, 'backup_worker_posts'):
                self.worker_posts = {
                    worker_id: posts.copy() for worker_id, posts in self.backup_worker_posts.items()
                }
            
            if hasattr(self, 'backup_worker_weekdays'):
                self.worker_weekdays = {
                    worker_id: weekdays.copy() for worker_id, weekdays in self.backup_worker_weekdays.items()
                }
            
            if hasattr(self, 'backup_worker_weekends'):
                self.worker_weekends = {
                    worker_id: weekends.copy() for worker_id, weekends in self.backup_worker_weekends.items()
                }
            
            # Only restore constraint_skips if backup exists
            if hasattr(self, 'backup_constraint_skips'):
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
            logging.error(f"Error in scheduler restore: {str(e)}", exc_info=True)
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
            workers_assigned = [w for w in self.schedule.get(date, []) if w is not None] # Use .get for safety

            # Use indices to safely modify the list while iterating conceptually
            indices_to_check = list(range(len(workers_assigned)))
            processed_pairs = set() # Avoid redundant checks/fixes if multiple pairs exist

            for i in indices_to_check:
                 if i >= len(workers_assigned): continue # List size might change
                 worker1_id = workers_assigned[i]
                 if worker1_id is None: continue # Slot might have been cleared by a previous fix

                 for j in range(i + 1, len(workers_assigned)):
                      if j >= len(workers_assigned): continue # List size might change
                      worker2_id = workers_assigned[j]
                      if worker2_id is None: continue # Slot might have been cleared

                      pair = tuple(sorted((worker1_id, worker2_id)))
                      if pair in processed_pairs: continue # Already handled this pair

                      # Check if workers are incompatible using the schedule_builder method
                      if self.schedule_builder._are_workers_incompatible(worker1_id, worker2_id):
                          incompatibility_issues += 1 # Count issue regardless of fix success
                          processed_pairs.add(pair) # Mark pair as processed
                          logging.warning(f"VALIDATION: Found incompatible workers {worker1_id} and {worker2_id} on {date}")

                          # CRITICAL: Check if either worker has a mandatory assignment for this date
                          worker1_is_mandatory = self.schedule_builder._is_mandatory(worker1_id, date)
                          worker2_is_mandatory = self.schedule_builder._is_mandatory(worker2_id, date)
                          
                          # If both are mandatory, we have a configuration error - log it but don't remove
                          if worker1_is_mandatory and worker2_is_mandatory:
                              logging.error(f"VALIDATION ERROR: Both workers {worker1_id} and {worker2_id} have mandatory assignments on {date} but are incompatible. This is a configuration error that must be fixed in worker data.")
                              continue  # Skip this fix - cannot remove mandatory assignments
                          
                          # If one is mandatory, remove the other one
                          if worker1_is_mandatory:
                              worker_to_remove = worker2_id
                              logging.info(f"VALIDATION: Worker {worker1_id} has mandatory assignment, will remove {worker2_id}")
                          elif worker2_is_mandatory:
                              worker_to_remove = worker1_id
                              logging.info(f"VALIDATION: Worker {worker2_id} has mandatory assignment, will remove {worker1_id}")
                          else:
                              # Neither is mandatory - remove one with more assignments
                              w1_count = len(self.worker_assignments.get(worker1_id, set()))
                              w2_count = len(self.worker_assignments.get(worker2_id, set()))
                              worker_to_remove = worker1_id if w1_count >= w2_count else worker2_id
                          try:
                              # Find the post index IN THE ORIGINAL schedule[date] list
                              post_to_remove = self.schedule[date].index(worker_to_remove)

                              # CRITICAL: Final check - verify we can remove this worker
                              if hasattr(self, 'schedule_builder'):
                                  if not self.schedule_builder._can_modify_assignment(worker_to_remove, date, "validation_fix_incompat"):
                                      logging.error(f"🔒 BLOCKED: Cannot remove MANDATORY {worker_to_remove} from {date.strftime('%Y-%m-%d')} - This is a configuration error!")
                                      continue
                              
                              # Remove the worker from schedule
                              self.schedule[date][post_to_remove] = None

                              # Remove from assignments tracking
                              if worker_to_remove in self.worker_assignments:
                                  self.worker_assignments[worker_to_remove].discard(date) # Use discard

                              # --- ADDED: Update Tracking Data ---
                              self._update_tracking_data(worker_to_remove, date, post_to_remove, removing=True)
                              # --- END ADDED ---

                              fixes_made += 1
                              logging.warning(f"VALIDATION: Removed worker {worker_to_remove} from {date} Post {post_to_remove} to fix incompatibility")

                              # Update the local workers_assigned list for subsequent checks on the same date
                              if worker_to_remove == worker1_id:
                                   workers_assigned[i] = None # Mark as None in local list
                              else:
                                   workers_assigned[j] = None # Mark as None in local list

                          except ValueError:
                               logging.error(f"VALIDATION FIX ERROR: Worker {worker_to_remove} not found in schedule for {date} during fix.")
                          except Exception as e:
                               logging.error(f"VALIDATION FIX ERROR: Unexpected error removing {worker_to_remove} from {date}: {e}")

        # 2. Check for minimum gap violations (Ensure this also calls _update_tracking_data)
        for worker_id in list(self.worker_assignments.keys()): # Iterate over copy of keys
            assignments = sorted(list(self.worker_assignments.get(worker_id, set()))) # Use .get

            indices_to_remove_gap = [] # Store (date, post) to remove after checking all pairs

            for i in range(len(assignments) - 1):
                date1 = assignments[i]
                date2 = assignments[i+1]
                days_between = (date2 - date1).days

                min_days_between = self.gap_between_shifts + 1
                # Add specific part-time logic if needed here, e.g., based on worker data

                if days_between < min_days_between:
                    gap_issues += 1
                    logging.warning(f"VALIDATION: Found gap violation for worker {worker_id}: only {days_between} days between {date1} and {date2}, minimum required: {min_days_between}")

                    # CRITICAL: Check if either date is a mandatory assignment
                    date1_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date1)
                    date2_is_mandatory = self.schedule_builder._is_mandatory(worker_id, date2)
                    
                    # If both are mandatory, this is a configuration error - log it but don't remove
                    if date1_is_mandatory and date2_is_mandatory:
                        logging.error(f"VALIDATION ERROR: Worker {worker_id} has mandatory assignments on {date1} and {date2} but they violate minimum gap requirement. This is a configuration error that must be fixed in worker data.")
                        continue  # Skip this fix - cannot remove mandatory assignments
                    
                    # If date1 is mandatory, try to remove date2
                    # If date2 is mandatory, try to remove date1
                    # If neither is mandatory, remove the later one (date2)
                    if date2_is_mandatory:
                        date_to_remove = date1
                        logging.info(f"VALIDATION: {date2} is mandatory for worker {worker_id}, will try to remove {date1}")
                    elif date1_is_mandatory:
                        date_to_remove = date2
                        logging.info(f"VALIDATION: {date1} is mandatory for worker {worker_id}, will try to remove {date2}")
                    else:
                        date_to_remove = date2  # Default: remove later assignment

                    # Mark the assignment for removal
                    try:
                         # Find post index for date_to_remove
                         if date_to_remove in self.schedule and worker_id in self.schedule[date_to_remove]:
                              post_to_remove_gap = self.schedule[date_to_remove].index(worker_id)
                              indices_to_remove_gap.append((date_to_remove, post_to_remove_gap))
                         else:
                              logging.error(f"VALIDATION FIX ERROR (GAP): Worker {worker_id} assignment for {date_to_remove} not found in schedule.")
                    except ValueError:
                         logging.error(f"VALIDATION FIX ERROR (GAP): Worker {worker_id} not found in schedule list for {date_to_remove}.")

            # Now perform removals for gap violations
            for date_rem, post_rem in indices_to_remove_gap:
                 if date_rem in self.schedule and len(self.schedule[date_rem]) > post_rem and self.schedule[date_rem][post_rem] == worker_id:
                      # CRITICAL: Final verification - never remove mandatory
                      if hasattr(self, 'schedule_builder'):
                          if not self.schedule_builder._can_modify_assignment(worker_id, date_rem, "validation_fix_gap"):
                              logging.error(f"🔒 BLOCKED: Cannot remove MANDATORY {worker_id} from {date_rem.strftime('%Y-%m-%d')} - Gap violation cannot be fixed!")
                              continue
                      
                      self.schedule[date_rem][post_rem] = None
                      self.worker_assignments[worker_id].discard(date_rem)
                      # --- ADDED: Update Tracking Data ---
                      self._update_tracking_data(worker_id, date_rem, post_rem, removing=True)
                      # --- END ADDED ---
                      fixes_made += 1
                      logging.warning(f"VALIDATION: Removed worker {worker_id} from {date_rem} Post {post_rem} to fix gap violation")
                 else:
                      logging.warning(f"VALIDATION FIX SKIP (GAP): State changed, worker {worker_id} no longer at {date_rem} Post {post_rem}.")

    
        # 3. Run the reconcile method to ensure data consistency
        if self._reconcile_schedule_tracking():
            other_issues += 1
    
        logging.info(f"Final validation complete: Found {incompatibility_issues} incompatibility issues, {gap_issues} gap issues, and {other_issues} other issues. Made {fixes_made} fixes.")
        return fixes_made

    def _validate_final_schedule(self):
        """
        Validate the final schedule before returning it.
        Returns True if valid, False if issues found.
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
            logging.error(f"Validation error: {str(e)}", exc_info=True)
            return False
        
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
            self._validate_final_schedule()
            
            # Gather statistics and metrics
            stats = self.gather_statistics()
            metrics = self.get_schedule_metrics()
            
            # Calculate coverage
            coverage = self._calculate_coverage()
            if coverage < 95:  # Less than 95% coverage is considered problematic
                logging.warning(f"Low schedule coverage: {coverage:.1f}%")
            
            # Check worker assignment balance
            for worker_id, worker_stats in stats['workers'].items():
                if abs(worker_stats['total_shifts'] - worker_stats['target_shifts']) > 2:
                    logging.warning(
                        f"Worker {worker_id} has significant deviation from target shifts: "
                        f"Actual={worker_stats['total_shifts']}, "
                        f"Target={worker_stats['target_shifts']}"
                    )
            
            return True, {
                'stats': stats,
                'metrics': metrics,
                'coverage': coverage,
                'timestamp': datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
                'validator': self.current_user
            }
            
        except SchedulerError as e:
            logging.error(f"Schedule validation failed: {str(e)}")
            return False, str(e)
        
    # ========================================
    # 9. REPORTING AND EXPORT
    # ========================================
    def export_schedule(self, format='txt'):
        """
        Export the schedule in the specified format
        
        Args:
            format: Output format ('txt' currently supported)
        Returns:
            str: Name of the generated file
        """
        timestamp = datetime.now().strftime('%d%m%Y_%H%M%S')
        filename = f'schedule_{timestamp}.{format}'
        
        if format == 'txt':
            with open(filename, 'w', encoding='utf-8') as f:
                # Write header
                f.write("="*60 + "\n")
                f.write("HORARIO GENERADO\n")
                f.write("="*60 + "\n")
                f.write(f"Fecha de generación: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                f.write(f"Período: {self.start_date.strftime('%d/%m/%Y')} - {self.end_date.strftime('%d/%m/%Y')}\n")
                f.write(f"Trabajadores: {len(self.workers_data)}\n")
                f.write(f"Turnos por día: {self.num_shifts}\n\n")
                
                # Write schedule body
                current_date = self.start_date
                while current_date <= self.end_date:
                    if current_date in self.schedule:
                        day_name = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo'][current_date.weekday()]
                        f.write(f"{day_name} {current_date.strftime('%d/%m/%Y')}\n")
                        
                        for post_idx, worker_id in enumerate(self.schedule[current_date]):
                            if worker_id:
                                worker_name = next((w['name'] for w in self.workers_data if w['id'] == worker_id), worker_id)
                                f.write(f"  Turno {post_idx + 1}: {worker_name} ({worker_id})\n")
                            else:
                                f.write(f"  Turno {post_idx + 1}: [VACANTE]\n")
                        f.write("\n")
                    current_date += timedelta(days=1)
                
                # Write summary
                f.write("\n" + "="*60 + "\n")
                f.write("RESUMEN\n")
                f.write("="*60 + "\n")
                for worker in self.workers_data:
                    worker_id = worker['id']
                    shift_count = len(self.worker_assignments.get(worker_id, []))
                    weekend_count = len([d for d in self.worker_assignments.get(worker_id, []) if d.weekday() >= 4])
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
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'schedule_complete_{timestamp}.json'
        
        # Convert schedule to serializable format
        schedule_serializable = {}
        for date, workers in self.schedule.items():
            date_str = date.strftime('%Y-%m-%d')
            schedule_serializable[date_str] = workers
        
        # Convert worker_assignments to serializable format
        worker_assignments_serializable = {}
        for worker_id, dates in self.worker_assignments.items():
            worker_assignments_serializable[worker_id] = [d.strftime('%Y-%m-%d') for d in sorted(dates)]
        
        # Build complete data structure
        data = {
            'metadata': {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'period_start': self.start_date.strftime('%Y-%m-%d'),
                'period_end': self.end_date.strftime('%Y-%m-%d'),
                'total_days': (self.end_date - self.start_date).days + 1,
                'num_shifts_per_day': self.num_shifts,
                'total_workers': len(self.workers_data)
            },
            'schedule': schedule_serializable,
            'worker_assignments': worker_assignments_serializable,
            'workers_data': self.workers_data,
            'config': {
                'start_date': self.start_date.strftime('%Y-%m-%d'),
                'end_date': self.end_date.strftime('%Y-%m-%d'),
                'num_shifts': self.num_shifts,
                'gap_between_shifts': self.gap_between_shifts,
                'max_consecutive_weekends': self.max_consecutive_weekends
            }
        }
        
        # Save to file
        with open(filename, 'w', encoding='utf-8') as f:
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
                filename = f'worker_{worker_id}_report.txt'
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(report)
                logging.info(f"Worker report saved to {filename}")
            
            return report
        
        except Exception as e:
            logging.error(f"Error generating worker report: {str(e)}")
            return f"Error generating report: {str(e)}"

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
            worker_id = worker['id']
            try:
                report = self.stats.generate_worker_report(worker_id)
            
                # Create filename
                filename = f'worker_{worker_id}_report.txt'
                if output_directory:
                    import os
                    os.makedirs(output_directory, exist_ok=True)
                    filename = os.path.join(output_directory, filename)
                
                # Save to file
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(report)
                
                count += 1
                logging.info(f"Generated report for worker {worker_id}")
            
            except Exception as e:
                logging.error(f"Failed to generate report for worker {worker_id}: {str(e)}")
            
        logging.info(f"Generated {count} worker reports")
        return count
        
    def log_schedule_summary(self, title="Schedule Summary"):
        """ Helper method to log key statistics about the current schedule state. """
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
                if posts_set: # Only log if worker has assignments
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
            current_score = self.schedule_builder.calculate_score(self.schedule, self.worker_assignments) # Assuming calculate_score uses current state
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
        eligible_workers = [w for w in self.workers_data if w['id'] != excluded_worker_id]
    
        if not eligible_workers:
            return
    
        # Sort by work percentage (give more to workers with higher percentage)
        eligible_workers.sort(key=lambda w: float(w.get('work_percentage', 100)), reverse=True)
    
        # Distribute excess shifts
        for i in range(excess_shifts):
            worker = eligible_workers[i % len(eligible_workers)]
            worker['target_shifts'] += 1
            logging.info(f"Redistributed 1 shift to worker {worker['id']}")

    # ========================================
    # 11. REAL-TIME OPERATIONS
    # ========================================
    def is_real_time_enabled(self) -> bool:
        """Check if real-time features are enabled"""
        return self.real_time_engine is not None
    
    def enable_real_time_features(self) -> bool:
        """
        Enable and fully activate real-time features after schedule generation.
        This method ensures all real-time components are properly connected and ready for use.
        
        Returns:
            bool: True if real-time features were successfully enabled, False otherwise
        """
        if not self.is_real_time_enabled():
            logging.warning("Cannot enable real-time features: real-time engine not initialized")
            return False
        
        try:
            # Ensure the real-time engine has access to the current schedule data
            # The incremental updater should be able to access current schedule state
            if hasattr(self.real_time_engine, 'incremental_updater'):
                logging.debug("Real-time incremental updater is ready with current schedule")
            
            # Verify the live validator has access to current constraints and worker data
            if hasattr(self.real_time_engine, 'live_validator'):
                logging.debug("Real-time live validator is ready with current constraints")
            
            # Initialize change tracking for the current schedule state if available
            if hasattr(self.real_time_engine, 'change_tracker'):
                # Record initial state as baseline for change tracking
                logging.debug("Real-time change tracker is ready for operation")
            
            # Publish event to notify that real-time features are fully active
            if hasattr(self.real_time_engine, 'event_bus'):
                from event_bus import EventType, ScheduleEvent
                event = ScheduleEvent(
                    event_type=EventType.REAL_TIME_ACTIVATED,
                    data={'message': 'Real-time features fully activated', 'user_id': self.current_user}
                )
                self.real_time_engine.event_bus.publish(event)
                logging.debug("Real-time activation event published")
            
            logging.info("Real-time features successfully enabled and activated for smart swapping")
            return True
            
        except Exception as e:
            logging.error(f"Error enabling real-time features: {e}", exc_info=True)
            return False
    
    def assign_worker_real_time(self, worker_id: str, shift_date: datetime, post_index: int,
                               user_id: Optional[str] = None, validate: bool = True) -> Dict[str, Any]:
        """
        Assign worker to shift with real-time validation and feedback
        
        Args:
            worker_id: ID of worker to assign
            shift_date: Date of the shift
            post_index: Post index (0-based)
            user_id: ID of user making the change
            validate: Whether to perform validation
            
        Returns:
            Dictionary with operation result and feedback
        """
        if not self.is_real_time_enabled():
            return {
                'success': False,
                'message': 'Real-time features not enabled',
                'error': 'REAL_TIME_DISABLED'
            }
        
        try:
            result = self.real_time_engine.assign_worker_real_time(
                worker_id, shift_date, post_index, user_id, validate
            )
            
            return {
                'success': result.success,
                'message': result.message,
                'operation_id': result.operation_id,
                'validation_results': [v.__dict__ for v in result.validation_results],
                'conflicts': [c.__dict__ for c in result.conflicts],
                'suggestions': result.suggestions
            }
            
        except Exception as e:
            logging.error(f"Error in real-time assignment: {e}")
            return {
                'success': False,
                'message': f'Assignment failed: {str(e)}',
                'error': 'OPERATION_FAILED'
            }
    
    def unassign_worker_real_time(self, shift_date: datetime, post_index: int, 
                                 user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Unassign worker from shift with real-time feedback
        
        Args:
            shift_date: Date of the shift
            post_index: Post index (0-based)
            user_id: ID of user making the change
            
        Returns:
            Dictionary with operation result and feedback
        """
        if not self.is_real_time_enabled():
            return {
                'success': False,
                'message': 'Real-time features not enabled',
                'error': 'REAL_TIME_DISABLED'
            }
        
        try:
            result = self.real_time_engine.unassign_worker_real_time(
                shift_date, post_index, user_id
            )
            
            return {
                'success': result.success,
                'message': result.message,
                'operation_id': result.operation_id,
                'suggestions': result.suggestions
            }
            
        except Exception as e:
            logging.error(f"Error in real-time unassignment: {e}")
            return {
                'success': False,
                'message': f'Unassignment failed: {str(e)}',
                'error': 'OPERATION_FAILED'
            }
    
    def swap_workers_real_time(self, shift_date1: datetime, post_index1: int,
                              shift_date2: datetime, post_index2: int,
                              user_id: Optional[str] = None, validate: bool = True) -> Dict[str, Any]:
        """
        Swap workers between shifts with real-time validation
        
        Args:
            shift_date1: Date of first shift
            post_index1: Post index of first shift
            shift_date2: Date of second shift
            post_index2: Post index of second shift
            user_id: ID of user making the change
            validate: Whether to perform validation
            
        Returns:
            Dictionary with operation result and feedback
        """
        if not self.is_real_time_enabled():
            return {
                'success': False,
                'message': 'Real-time features not enabled',
                'error': 'REAL_TIME_DISABLED'
            }
        
        try:
            result = self.real_time_engine.swap_workers_real_time(
                shift_date1, post_index1, shift_date2, post_index2, user_id, validate
            )
            
            return {
                'success': result.success,
                'message': result.message,
                'operation_id': result.operation_id,
                'validation_results': [v.__dict__ for v in result.validation_results],
                'conflicts': [c.__dict__ for c in result.conflicts]
            }
            
        except Exception as e:
            logging.error(f"Error in real-time swap: {e}")
            return {
                'success': False,
                'message': f'Swap failed: {str(e)}',
                'error': 'OPERATION_FAILED'
            }
    
    def validate_schedule_real_time(self, quick_check: bool = False) -> Dict[str, Any]:
        """
        Perform real-time schedule validation
        
        Args:
            quick_check: If True, perform only essential validations
            
        Returns:
            Dictionary with validation results
        """
        if not self.is_real_time_enabled():
            return {
                'success': False,
                'message': 'Real-time features not enabled',
                'error': 'REAL_TIME_DISABLED'
            }
        
        try:
            result = self.real_time_engine.validate_schedule_real_time(quick_check)
            
            return {
                'success': result.success,
                'message': result.message,
                'operation_id': result.operation_id,
                'validation_results': [v.__dict__ for v in result.validation_results],
                'conflicts': [c.__dict__ for c in result.conflicts]
            }
            
        except Exception as e:
            logging.error(f"Error in real-time validation: {e}")
            return {
                'success': False,
                'message': f'Validation failed: {str(e)}',
                'error': 'OPERATION_FAILED'
            }
    
    def undo_last_change(self, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Undo the last schedule change
        
        Args:
            user_id: ID of user performing the undo
            
        Returns:
            Dictionary with undo result
        """
        if not self.is_real_time_enabled():
            return {
                'success': False,
                'message': 'Real-time features not enabled',
                'error': 'REAL_TIME_DISABLED'
            }
        
        try:
            result = self.real_time_engine.undo_last_change(user_id)
            
            return {
                'success': result.success,
                'message': result.message,
                'operation_id': result.operation_id
            }
            
        except Exception as e:
            logging.error(f"Error in undo operation: {e}")
            return {
                'success': False,
                'message': f'Undo failed: {str(e)}',
                'error': 'OPERATION_FAILED'
            }
    
    def redo_last_change(self, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Redo the last undone change
        
        Args:
            user_id: ID of user performing the redo
            
        Returns:
            Dictionary with redo result
        """
        if not self.is_real_time_enabled():
            return {
                'success': False,
                'message': 'Real-time features not enabled',
                'error': 'REAL_TIME_DISABLED'
            }
        
        try:
            result = self.real_time_engine.redo_last_change(user_id)
            
            return {
                'success': result.success,
                'message': result.message,
                'operation_id': result.operation_id
            }
            
        except Exception as e:
            logging.error(f"Error in redo operation: {e}")
            return {
                'success': False,
                'message': f'Redo failed: {str(e)}',
                'error': 'OPERATION_FAILED'
            }
    
    def get_real_time_analytics(self) -> Dict[str, Any]:
        """
        Get real-time analytics and metrics
        
        Returns:
            Dictionary with analytics data
        """
        if not self.is_real_time_enabled():
            return {
                'error': 'Real-time features not enabled'
            }
        
        try:
            return self.real_time_engine.get_real_time_analytics()
        except Exception as e:
            logging.error(f"Error getting real-time analytics: {e}")
            return {
                'error': f'Analytics failed: {str(e)}'
            }
    
    def get_change_history(self, limit: int = 20, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get recent schedule changes
        
        Args:
            limit: Maximum number of changes to return
            user_id: Filter by user ID
            
        Returns:
            Dictionary with change history
        """
        if not self.is_real_time_enabled():
            return {
                'error': 'Real-time features not enabled'
            }
        
        try:
            changes = self.real_time_engine.change_tracker.get_change_history(
                limit=limit, user_id=user_id
            )
            
            return {
                'changes': [change.to_dict() for change in changes],
                'total_count': len(changes),
                'can_undo': self.real_time_engine.change_tracker.can_undo(),
                'can_redo': self.real_time_engine.change_tracker.can_redo()
            }
            
        except Exception as e:
            logging.error(f"Error getting change history: {e}")
            return {
                'error': f'History retrieval failed: {str(e)}'
            }
    
    # Predictive Analytics Integration Methods
    
    def is_predictive_analytics_enabled(self) -> bool:
        """Check if predictive analytics features are enabled"""
        return self.predictive_analytics is not None
    
    def generate_demand_forecasts(self, forecast_days: int = 30) -> Dict[str, Any]:
        """
        Generate demand forecasts using predictive analytics
        
        Args:
            forecast_days: Number of days to forecast ahead
            
        Returns:
            Dictionary containing forecast results and recommendations
        """
        if not self.is_predictive_analytics_enabled():
            return {
                'success': False,
                'message': 'Predictive analytics not enabled',
                'error': 'PREDICTIVE_ANALYTICS_DISABLED'
            }
        
        try:
            result = self.predictive_analytics.generate_demand_forecasts(forecast_days)
            
            # Auto-collect current data after generating forecasts for future improvements
            if self.predictive_analytics.config.get('auto_collect_data', True):
                self.predictive_analytics.auto_collect_data_if_enabled()
            
            return {
                'success': True,
                'forecasts': result.get('forecasts'),
                'status': result.get('status'),
                'message': f'Forecasts generated for {forecast_days} days'
            }
            
        except Exception as e:
            logging.error(f"Error generating demand forecasts: {e}")
            return {
                'success': False,
                'message': f'Forecast generation failed: {str(e)}',
                'error': 'FORECAST_FAILED'
            }
    
    def get_predictive_insights(self) -> Dict[str, Any]:
        """
        Get comprehensive predictive insights and recommendations
        
        Returns:
            Dictionary containing insights, recommendations, and analytics
        """
        if not self.is_predictive_analytics_enabled():
            return {
                'success': False,
                'message': 'Predictive analytics not enabled',
                'error': 'PREDICTIVE_ANALYTICS_DISABLED'
            }
        
        try:
            result = self.predictive_analytics.get_predictive_insights()
            
            return {
                'success': True,
                'insights': result.get('insights'),
                'status': result.get('status'),
                'message': 'Predictive insights generated successfully'
            }
            
        except Exception as e:
            logging.error(f"Error getting predictive insights: {e}")
            return {
                'success': False,
                'message': f'Insights generation failed: {str(e)}',
                'error': 'INSIGHTS_FAILED'
            }
    
    def run_predictive_optimization(self) -> Dict[str, Any]:
        """
        Run predictive optimization analysis
        
        Returns:
            Dictionary containing optimization results and recommendations
        """
        if not self.is_predictive_analytics_enabled() or not self.predictive_optimizer:
            return {
                'success': False,
                'message': 'Predictive optimization not available',
                'error': 'PREDICTIVE_OPTIMIZER_DISABLED'
            }
        
        try:
            # Get latest forecasts if available
            forecast_data = None
            if self.predictive_analytics.latest_forecasts:
                forecast_data = self.predictive_analytics.latest_forecasts
            
            result = self.predictive_optimizer.predict_and_optimize(forecast_data)
            
            return {
                'success': True,
                'optimization_results': result,
                'message': 'Predictive optimization completed successfully'
            }
            
        except Exception as e:
            logging.error(f"Error in predictive optimization: {e}")
            return {
                'success': False,
                'message': f'Predictive optimization failed: {str(e)}',
                'error': 'OPTIMIZATION_FAILED'
            }
    
    def collect_historical_data(self) -> Dict[str, Any]:
        """
        Collect and store current schedule data for historical analysis
        
        Returns:
            Dictionary containing collection results
        """
        if not self.is_predictive_analytics_enabled():
            return {
                'success': False,
                'message': 'Predictive analytics not enabled',
                'error': 'PREDICTIVE_ANALYTICS_DISABLED'
            }
        
        try:
            result = self.predictive_analytics.collect_and_store_current_data()
            
            return {
                'success': result.get('status') == 'success',
                'message': result.get('message', 'Data collection completed'),
                'data_summary': result.get('data_summary')
            }
            
        except Exception as e:
            logging.error(f"Error collecting historical data: {e}")
            return {
                'success': False,
                'message': f'Data collection failed: {str(e)}',
                'error': 'DATA_COLLECTION_FAILED'
            }
    
    def get_optimization_suggestions(self) -> List[str]:
        """
        Get optimization suggestions based on predictive analytics
        
        Returns:
            List of optimization suggestions
        """
        if not self.is_predictive_analytics_enabled():
            return ["Predictive analytics not enabled - enable for optimization suggestions"]
        
        try:
            return self.predictive_analytics.get_optimization_suggestions()
        except Exception as e:
            logging.error(f"Error getting optimization suggestions: {e}")
            return [f"Error getting suggestions: {str(e)}"]
    
    def get_analytics_summary(self) -> Dict[str, Any]:
        """
        Get summary of predictive analytics status and capabilities
        
        Returns:
            Dictionary with analytics summary
        """
        if not self.is_predictive_analytics_enabled():
            return {
                'enabled': False,
                'message': 'Predictive analytics not enabled'
            }
        
        try:
            return self.predictive_analytics.get_analytics_summary()
        except Exception as e:
            logging.error(f"Error getting analytics summary: {e}")
            return {
                'enabled': True,
                'error': str(e),
                'message': 'Error getting analytics summary'
            }
    
    def apply_predictive_adjustments(self, optimization_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply parameter adjustments recommended by predictive optimization
        
        Args:
            optimization_result: Result from predictive optimization
            
        Returns:
            Dictionary with application results
        """
        if not self.is_predictive_analytics_enabled() or not self.predictive_optimizer:
            return {
                'success': False,
                'message': 'Predictive optimization not available',
                'error': 'PREDICTIVE_OPTIMIZER_DISABLED'
            }
        
        try:
            parameter_adjustments = optimization_result.get('parameter_adjustments', {})
            if not parameter_adjustments:
                return {
                    'success': False,
                    'message': 'No parameter adjustments found in optimization result',
                    'error': 'NO_ADJUSTMENTS'
                }
            
            result = self.predictive_optimizer.apply_recommended_adjustments(parameter_adjustments)
            
            return {
                'success': result.get('status') == 'success',
                'application_results': result.get('results'),
                'message': 'Parameter adjustments applied successfully' if result.get('status') == 'success' 
                          else f"Application failed: {result.get('message', 'Unknown error')}"
            }
            
        except Exception as e:
            logging.error(f"Error applying predictive adjustments: {e}")
            return {
                'success': False,
                'message': f'Adjustment application failed: {str(e)}',
                'error': 'ADJUSTMENT_FAILED'
            }


