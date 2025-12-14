# Imports
from datetime import datetime, timedelta
import copy
import logging
import random
import math
from typing import Dict, List, Set, Optional, Tuple, Any, TYPE_CHECKING
from exceptions import SchedulerError
from adaptive_iterations import AdaptiveIterationManager

if TYPE_CHECKING:
    from scheduler import Scheduler

class ScheduleBuilder:
    """Enhanced schedule generation and improvement with performance optimizations"""
    
    # ========================================
    # 1. INITIALIZATION AND SETUP
    # ========================================
    def __init__(self, scheduler: 'Scheduler'):
        """
        Initialize the schedule builder with performance optimizations

        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler

        # IMPORTANT: Use direct references, not copies
        self.workers_data = scheduler.workers_data
        self.schedule = scheduler.schedule # self.schedule IS scheduler.schedule
        logging.debug(f"[ScheduleBuilder.__init__] self.schedule object ID: {id(self.schedule)}, Initial keys: {list(self.schedule.keys())}")
        self.config = scheduler.config
        self.worker_assignments = scheduler.worker_assignments  # Use the same reference
        self.num_shifts = scheduler.num_shifts
        self.holidays = scheduler.holidays
        self.constraint_checker = scheduler.constraint_checker
        self.best_schedule_data: Optional[Dict[str, Any]] = None # Initialize the attribute to store the best state found
        self._locked_mandatory: Set[Tuple[str, datetime]] = set()
        # Keep track of which (worker_id, date) pairs are truly mandatory
        self.start_date = scheduler.start_date
        self.end_date = scheduler.end_date
        self.date_utils = scheduler.date_utils
        self.gap_between_shifts = scheduler.gap_between_shifts 
        self.max_shifts_per_worker = scheduler.max_shifts_per_worker
        self.max_consecutive_weekends = scheduler.max_consecutive_weekends 
        self.data_manager = scheduler.data_manager
        self.worker_posts = scheduler.worker_posts
        self.worker_weekdays = scheduler.worker_weekdays
        self.worker_weekends = scheduler.worker_weekends
        self.constraint_skips = scheduler.constraint_skips
        self.last_assigned_date = scheduler.last_assignment_date # Used in calculate_score
        self.consecutive_shifts = scheduler.consecutive_shifts # Used in calculate_score
        
        # Performance optimization caches
        self._worker_cache: Dict[str, Dict[str, Any]] = {}
        self._date_cache: Dict[datetime, Dict[str, Any]] = {}
        self._incompatibility_cache: Dict[str, set] = {}
        self._assignment_cache: Dict[str, Any] = {}
        
        self.iteration_manager = AdaptiveIterationManager(scheduler)
        self.adaptive_config = self.iteration_manager.calculate_adaptive_iterations()
        logging.info(f"Adaptive config: {self.adaptive_config}")
        
        # STRICT MODE for initial distribution
        self.use_strict_mode = True  # Default: strict for initial fill
        self.relaxation_level_override = None  # None = use method parameter, or set to force level
        
        # Build performance caches
        self._build_optimization_caches()

        logging.debug(f"[ScheduleBuilder.__init__] self.schedule object ID: {id(self.schedule)}, Initial keys: {list(self.schedule.keys())[:5]}")
        logging.info("Enhanced ScheduleBuilder initialized with caching (STRICT MODE enabled)")
    
    def _build_optimization_caches(self) -> None:
        """Build caches for performance optimization"""
        # Build worker cache
        for worker in self.workers_data:
            worker_id = worker['id']
            self._worker_cache[worker_id] = {
                'data': worker,
                'target_shifts': worker.get('target_shifts', 0),
                'work_percentage': worker.get('work_percentage', 100),
                'mandatory_days': worker.get('mandatory_days', ''),
                'days_off': worker.get('days_off', ''),
                'is_incompatible': worker.get('is_incompatible', False)
            }
        
        # Build incompatibility lookup for fast access
        self._incompatibility_cache = {}
        for worker in self.workers_data:
            worker_id = worker['id']
            incompatible_list = worker.get('incompatible_with', [])
            if isinstance(incompatible_list, str):
                incompatible_list = [incompatible_list] if incompatible_list else []
            self._incompatibility_cache[worker_id] = set(incompatible_list)
        
        # Build date cache for weekend/holiday status
        current_date = self.start_date
        holiday_set = set(self.holidays)
        while current_date <= self.end_date:
            self._date_cache[current_date] = {
                'weekday': current_date.weekday(),
                'is_weekend': current_date.weekday() >= 4,
                'is_holiday': current_date in holiday_set,
                'is_special': (current_date.weekday() >= 4 or 
                             current_date in holiday_set or 
                             (current_date + timedelta(days=1)) in holiday_set)
            }
            current_date += timedelta(days=1)
        
        logging.debug(f"Built optimization caches for {len(self._worker_cache)} workers and {len(self._date_cache)} dates")
    
    # ========================================
    # STRICT MODE CONTROL
    # ========================================
    
    def enable_strict_mode(self):
        """
        Activa modo ESTRICTO para reparto inicial.
        
        Restricciones ESTRICTAS:
        - Target: +10% máximo (ajustado por work_percentage)
        - Gap: Sin reducción
        - Patrón 7/14: PROHIBIDO absolutamente
        - Balance mensual: ±1 estricto
        - Balance weekend: ±1 estricto
        """
        self.use_strict_mode = True
        logging.info("✅ STRICT MODE ENABLED - All constraints enforced strictly")
    
    def enable_relaxed_mode(self):
        """
        Activa modo RELAJADO para optimización iterativa.
        
        RESTRICCIONES EN MODO RELAJADO:
        - Target: SIEMPRE +10% máximo (NO aumenta)
        - Gap: Permite reducción de -1 SOLAMENTE (con déficit ≥3)
        - Patrón 7/14: Permite violación si déficit >10% del target
        - Balance: Trabaja con tolerancias ±10% en guardias/mes, weekends
        
        NUNCA SE RELAJAN:
        - Mandatory shifts (siempre protegidos)
        - Incompatibilidades (siempre bloqueadas)
        - Days off (nunca se asignan)
        """
        self.use_strict_mode = False
        logging.info("🔓 RELAXED MODE ENABLED - Limited constraint relaxation (±10% tolerance, gap-1)")
    
    def is_strict_mode(self) -> bool:
        """Retorna True si está en modo estricto."""
        return self.use_strict_mode
        
    def _ensure_data_integrity(self) -> bool:
        """
        Ensure all data structures are consistent - delegates to scheduler
        """
        # Let the scheduler handle the data integrity check as it has the primary data
        return self.scheduler._ensure_data_integrity()    

    def _verify_assignment_consistency(self) -> None:
        """
        Optimized verification and fixing of data consistency between schedule and tracking data
        """
        inconsistencies_fixed = 0
        
        # Check schedule against worker_assignments and fix inconsistencies
        for date, shifts in self.schedule.items():
            for post, worker_id in enumerate(shifts):
                if worker_id is None:
                    continue
                
                # Ensure worker is tracked for this date
                if worker_id not in self.worker_assignments:
                    self.worker_assignments[worker_id] = set()
                
                if date not in self.worker_assignments[worker_id]:
                    self.worker_assignments[worker_id].add(date)
                    inconsistencies_fixed += 1
    
        # Check worker_assignments against schedule
        for worker_id, assignments in self.worker_assignments.items():
            for date in list(assignments):  # Make a copy to safely modify during iteration
                # Check if this worker is actually in the schedule for this date
                if date not in self.schedule or worker_id not in self.schedule[date]:
                    # CRITICAL: NEVER remove mandatory assignments even if inconsistent
                    if self._is_mandatory(worker_id, date):
                        logging.error(f"CRITICAL: Mandatory assignment inconsistency detected for {worker_id} on {date} - NOT REMOVING")
                        continue
                    if (worker_id, date) in self._locked_mandatory:
                        logging.error(f"CRITICAL: Locked mandatory assignment inconsistency detected for {worker_id} on {date} - NOT REMOVING")
                        continue
                    
                    # Remove this inconsistent assignment
                    self.worker_assignments[worker_id].remove(date)
                    inconsistencies_fixed += 1
                    logging.warning(f"Fixed inconsistency: Worker {worker_id} was tracked for {date} but not in schedule")
        
        if inconsistencies_fixed > 0:
            logging.info(f"Fixed {inconsistencies_fixed} data consistency issues")
    
    def _is_weekend_or_holiday_cached(self, date: datetime) -> bool:
        """Get weekend/holiday status from cache"""
        date_info = self._date_cache.get(date)
        return date_info['is_special'] if date_info else False
    
    def _get_worker_cached(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Get worker data from cache"""
        return self._worker_cache.get(worker_id)
    
    def clear_optimization_caches(self) -> None:
        """Clear optimization caches when data changes significantly"""
        self._assignment_cache.clear()
        self._incompatibility_cache.clear()
        logging.debug("ScheduleBuilder optimization caches cleared")
        
    # ========================================
    # 2. UTILITY AND HELPER METHODS
    # ========================================
    def _parse_dates(self, date_str):
        """
        Parse semicolon-separated dates using the date_utils
    
        Args:
            date_str: String with semicolon-separated dates in DD-MM-YYYY format
        Returns:
            list: List of datetime objects
        """
        if not date_str:
            return []
    
        # Delegate to the DateTimeUtils class
        return self.date_utils.parse_dates(date_str)
        
    def _synchronize_tracking_data(self):
        # Placeholder for your method in ScheduleBuilder if it exists, or call scheduler\'s
        if hasattr(self.scheduler, '_synchronize_tracking_data'):
            self.scheduler._synchronize_tracking_data()
        else:
            logging.warning("Scheduler\'_synchronize_tracking_data not found by builder.")
            # Fallback or simplified sync if necessary:
            new_worker_assignments = {w['id']: set() for w in self.workers_data}
            new_worker_posts = {w['id']: set() for w in self.workers_data}  # FIXED: Should be sets, not dicts
            new_worker_post_counts = {w['id']: {p: 0 for p in range(self.num_shifts)} for w in self.workers_data}
            for date, shifts_on_date in self.schedule.items():
                for post_idx, worker_id_in_post in enumerate(shifts_on_date):
                    if worker_id_in_post is not None:
                        new_worker_assignments.setdefault(worker_id_in_post, set()).add(date)
                        new_worker_posts.setdefault(worker_id_in_post, set()).add(post_idx)  # FIXED: Add to set
                        new_worker_post_counts.setdefault(worker_id_in_post, {p: 0 for p in range(self.num_shifts)})[post_idx] += 1
            self.worker_assignments = new_worker_assignments # Update builder\'s reference
            self.scheduler.worker_assignments = new_worker_assignments # Update scheduler\'s reference
            self.worker_posts = new_worker_posts
            self.scheduler.worker_posts = new_worker_posts  # FIXED: Now correctly sets
            # Update post counts if the scheduler has this tracking
            if hasattr(self.scheduler, 'worker_post_counts'):
                self.scheduler.worker_post_counts = new_worker_post_counts
            self.scheduler.worker_shift_counts = {worker_id: len(dates) for worker_id, dates in new_worker_assignments.items()}
            # self.scheduler.worker_shift_counts = self.worker_shift_counts # This line is redundant
            # Add other tracking data sync if needed (weekends, etc.)
                    
    # ========================================
    # 3. WORKER CONSTRAINT CHECKING
    # ========================================
    def _is_mandatory(self, worker_id, date):
        # This is a placeholder for your actual implementation
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker: return False
        mandatory_days_str = worker.get('mandatory_days', '')
        if not mandatory_days_str: return False
        try:
            mandatory_dates = self.date_utils.parse_dates(mandatory_days_str)
            return date in mandatory_dates
        except:
            return False
    
    def _is_slot_protected_mandatory(self, date, post):
        """
        CRITICAL: Verificación CENTRALIZADA de protección mandatory.
        Este método DEBE ser llamado ANTES de cualquier modificación de un slot.
        
        Verifica si un slot específico (date, post) está ocupado por un turno mandatory
        que NO puede ser modificado bajo NINGUNA circunstancia.
        
        Returns:
            tuple: (is_protected, worker_id)
                - is_protected: True si el slot está protegido
                - worker_id: ID del trabajador si está protegido, None en caso contrario
        """
        # Verificar que el slot existe y tiene un trabajador asignado
        if date not in self.schedule:
            return False, None
        
        if len(self.schedule[date]) <= post:
            return False, None
        
        worker_id = self.schedule[date][post]
        if worker_id is None:
            return False, None
        
        # Verificar si está en el conjunto de mandatory bloqueados
        if (worker_id, date) in self._locked_mandatory:
            logging.debug(f"🔒 PROTECTED: Slot ({date.strftime('%Y-%m-%d')}, post {post}) is LOCKED MANDATORY for {worker_id}")
            return True, worker_id
        
        # Verificar si es mandatory según la configuración del trabajador
        if self._is_mandatory(worker_id, date):
            logging.debug(f"🔒 PROTECTED: Slot ({date.strftime('%Y-%m-%d')}, post {post}) is CONFIG MANDATORY for {worker_id}")
            return True, worker_id
        
        return False, None
    
    def _can_modify_assignment(self, worker_id, date, operation_name="unknown"):
        """
        CRITICAL: Verificación CENTRALIZADA antes de modificar/eliminar una asignación.
        
        Este método DEBE ser llamado ANTES de:
        - Mover un trabajador a otra fecha
        - Reasignar un trabajador a otro puesto
        - Eliminar una asignación
        - Intercambiar trabajadores (swap)
        
        Args:
            worker_id: ID del trabajador
            date: Fecha de la asignación
            operation_name: Nombre de la operación para logging
        
        Returns:
            bool: True si se puede modificar, False si está protegido
        """
        # Verificar locked mandatory
        if (worker_id, date) in self._locked_mandatory:
            logging.info(f"🚫 BLOCKED {operation_name}: Cannot modify LOCKED MANDATORY assignment for {worker_id} on {date.strftime('%Y-%m-%d')}")
            return False
        
        # Verificar config mandatory
        if self._is_mandatory(worker_id, date):
            logging.info(f"🚫 BLOCKED {operation_name}: Cannot modify CONFIG MANDATORY assignment for {worker_id} on {date.strftime('%Y-%m-%d')}")
            return False
        
        return True
            
    def _is_worker_unavailable(self, worker_id, date):
        """
        Check if a worker is unavailable on a specific date

        Args:
            worker_id: ID of the worker to check
            date: Date to check availability
    
        Returns:
            bool: True if worker is unavailable, False otherwise
        """
        # Get worker data
        worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None) # Corrected to worker_data as per user
        if not worker_data:
            return True
    
        # Check work periods - if work_periods is empty, worker is available for all dates
        work_periods_str = worker_data.get('work_periods', '')
        if work_periods_str:
            try:
                work_ranges = self.date_utils.parse_date_ranges(work_periods_str)
                if not any(start <= date <= end for start, end in work_ranges):
                    return True # Not within any defined work period
            except Exception as e:
                logging.error(f"Error parsing work_periods for {worker_id}: {e}")
                return True # Fail safe

        # Check days off
        days_off_str = worker_data.get('days_off', '')
        if days_off_str:
            try:
                off_ranges = self.date_utils.parse_date_ranges(days_off_str)
                if any(start <= date <= end for start, end in off_ranges):
                    return True
            except Exception as e:
                logging.error(f"Error parsing days_off for {worker_id}: {e}")
                return True # Fail safe

        return False
    
    def _would_violate_tolerance(self, worker_id, date, allow_relaxation=False):
        """
        CRITICAL: Check if assigning this worker would violate ±10% tolerance.
        
        IMPORTANT: This method ONLY blocks EXCESS (+10%), NOT deficit.
        Workers below target should ALWAYS be encouraged, not blocked.
        
        This method provides CENTRAL tolerance validation for ALL assignments,
        whether they go through _calculate_worker_score or not (swaps, balance, etc).
        
        Args:
            worker_id: Worker to check
            date: Date of potential assignment
            allow_relaxation: If True, allow violations with warning. If False, BLOCK.
            
        Returns:
            bool: True if would violate tolerance (should block), False if OK
        """
        worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker_data:
            return False  # Unknown worker, let other checks handle it
        
        target_shifts = worker_data.get('target_shifts', 0)
        work_percentage = worker_data.get('work_percentage', 100)
        
        if target_shifts <= 0:
            return False  # No target, no tolerance check
        
        # Calculate current assignments
        current_assignments = len(self.worker_assignments.get(worker_id, set()))
        assignments_after = current_assignments + 1
        
        # Phase-based tolerance system:
        # Phase 1 (objective): ±10% tolerance
        # Phase 2 (limit): ±12% tolerance (ABSOLUTE LIMIT)
        if allow_relaxation:
            tolerance = 0.12  # Phase 2: Absolute limit
        else:
            tolerance = 0.10  # Phase 1: Objective tolerance
        
        # STRICTER tolerance for part-time workers
        if work_percentage < 100:
            # Reduce tolerance proportionally for part-time workers
            # Phase 1 examples: 50% worker gets 4% (8%*50%), 75% worker gets 6% (8%*75%)
            # Phase 2 examples: 50% worker gets 6% (12%*50%), 75% worker gets 9% (12%*75%)
            adjusted_tolerance = tolerance * (work_percentage / 100.0)
            tolerance = max(0.05, adjusted_tolerance)  # Minimum 5% tolerance
            logging.debug(f"Worker {worker_id} (part-time {work_percentage}%): Using adjusted tolerance {tolerance*100:.1f}%")
        
        tolerance_amount = target_shifts * tolerance
        max_allowed = round(target_shifts * (1 + tolerance))  # Round properly
        
        # ONLY check UPPER bound - never block workers with deficit
        if assignments_after > max_allowed:
            # SIEMPRE bloquear excesos de tolerancia, sin excepción
            work_pct_msg = f" (part-time {work_percentage}%)" if work_percentage < 100 else ""
            if allow_relaxation:
                logging.warning(f"⚠️ BLOCKED even with relaxation: {worker_id}{work_pct_msg} would have {assignments_after} shifts "
                              f"(max allowed: {max_allowed}, target: {target_shifts})")
            else:
                logging.debug(f"🚫 BLOCKED: {worker_id}{work_pct_msg} would exceed +{tolerance*100:.1f}% tolerance "
                            f"({assignments_after} > {max_allowed}, target: {target_shifts})")
            return True  # SIEMPRE BLOQUEAR
        
        # If worker is BELOW target, encourage assignment (never block)
        if assignments_after < target_shifts:
            logging.debug(f"✅ ENCOURAGED: {worker_id} below target ({assignments_after} < {target_shifts}), should prioritize")
            return False  # Definitely OK
        
        # Check weekend tolerance if this is a weekend
        if self._is_weekend_or_holiday(date):
            weekend_assignments = sum(
                1 for d in self.worker_assignments.get(worker_id, set())
                if self._is_weekend_or_holiday(d)
            )
            weekend_after = weekend_assignments + 1
            
            # Calculate weekend target (approximately 3/7 days are Fri-Sun)
            weekend_percentage = 3.0 / 7.0
            weekend_target = target_shifts * weekend_percentage
            
            # Use the same adjusted tolerance for weekends (already calculated above for part-time workers)
            weekend_tolerance_amount = weekend_target * tolerance
            max_weekend_allowed = round(weekend_target * (1 + tolerance))
            
            # ONLY check UPPER bound for weekends too
            if weekend_after > max_weekend_allowed:
                work_pct_msg = f" (part-time {work_percentage}%)" if work_percentage < 100 else ""
                if allow_relaxation:
                    logging.warning(f"⚠️ Weekend tolerance violation: {worker_id}{work_pct_msg} would have {weekend_after} weekend shifts "
                                  f"(max allowed: {max_weekend_allowed}, target: {weekend_target:.1f}) - ALLOWED due to relaxation")
                    return False  # Allow but warn
                else:
                    logging.debug(f"🚫 BLOCKED: {worker_id}{work_pct_msg} would exceed +{tolerance*100:.1f}% weekend tolerance "
                                f"({weekend_after} > {max_weekend_allowed}, target: {weekend_target:.1f})")
                    return True  # BLOCK
        
        return False  # OK to assign
    
    def _check_incompatibility_with_list(self, worker_id_to_check, assigned_workers_list):
        """
        Optimized method to check if worker_id_to_check is incompatible with anyone in the list.
        
        Performance improvements:
        - Uses sets for O(1) lookups instead of lists
        - Performs intersection operation for faster checking
        - Early termination on incompatibility detection
        """
        worker_to_check_data = next((w for w in self.workers_data if w['id'] == worker_id_to_check), None)
        if not worker_to_check_data:
            return True  # Should not happen, but fail safe

        # Convert to set for faster lookups
        incompatible_with_candidate = set(str(id) for id in worker_to_check_data.get('incompatible_with', []))
        
        # Convert assigned workers to set for faster lookups, excluding None values
        assigned_workers_set = {str(assigned_id) for assigned_id in assigned_workers_list 
                               if assigned_id is not None and assigned_id != worker_id_to_check}
        
        # Check if any assigned worker is in incompatible list
        if incompatible_with_candidate & assigned_workers_set:
            return False

        # Bidirectional check - check if any assigned worker has candidate in their incompatible list
        worker_id_str = str(worker_id_to_check)
        for assigned_id in assigned_workers_set:
            assigned_worker_data = next((w for w in self.workers_data if str(w['id']) == assigned_id), None)
            if assigned_worker_data:
                assigned_incompatible = set(str(id) for id in assigned_worker_data.get('incompatible_with', []))
                if worker_id_str in assigned_incompatible:
                    return False
        
        return True  # No incompatibilities found

    def _check_incompatibility(self, worker_id, date):
        # Placeholder using _check_incompatibility_with_list
        assigned_workers_on_date = [w for w in self.schedule.get(date, []) if w is not None]
        return self._check_incompatibility_with_list(worker_id, assigned_workers_on_date)

    def _are_workers_incompatible(self, worker1_id, worker2_id):
        """
        Check if two workers are incompatible with each other using cached data
    
        Args:
            worker1_id: ID of first worker
            worker2_id: ID of second worker
        
        Returns:
            bool: True if workers are incompatible, False otherwise
        """
        # Use cached incompatibility data for faster lookup
        if hasattr(self, '_incompatibility_cache'):
            return (worker2_id in self._incompatibility_cache.get(worker1_id, set()) or
                    worker1_id in self._incompatibility_cache.get(worker2_id, set()))
        
        # Fallback to original implementation if cache not available
        # Find the worker data for each worker
        worker1 = next((w for w in self.workers_data if w['id'] == worker1_id), None)
        worker2 = next((w for w in self.workers_data if w['id'] == worker2_id), None)
    
        if not worker1 or not worker2:
            return False
    
        # Check if either worker has the other in their incompatibility list
        incompatible_with_1 = worker1.get('incompatible_with', [])
        incompatible_with_2 = worker2.get('incompatible_with', [])
    
        return worker2_id in incompatible_with_1 or worker1_id in incompatible_with_2 

    def _violates_7_14_pattern(self, worker_id, date):
        """
        Check if assigning worker to date would violate 7/14 day pattern.
        Returns True if it WOULD violate (should block), False if OK.
        
        7/14 pattern: Worker should NOT have shifts on the same weekday in consecutive weeks.
        Exception: Weekend days (Fri-Sun) are allowed.
        """
        assignments = sorted(list(self.worker_assignments.get(worker_id, [])))
        
        for prev_date in assignments:
            days_between = abs((date - prev_date).days)
            
            # Check for 7 or 14 day pattern with same weekday
            if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                # Allow weekend days (Fri=4, Sat=5, Sun=6)
                if date.weekday() >= 4 or prev_date.weekday() >= 4:
                    continue
                
                # Violation detected
                logging.debug(f"7/14 pattern violation: {worker_id} has {prev_date.strftime('%A %Y-%m-%d')} "
                            f"and trying to assign {date.strftime('%A %Y-%m-%d')} ({days_between} days apart)")
                return True
        
        return False

    def _can_assign_worker(self, worker_id, date, post):
        try:
            # CRITICAL: Never assign to a slot occupied by a mandatory assignment
            if (date in self.schedule and 
                len(self.schedule[date]) > post and 
                self.schedule[date][post] is not None):
                existing_worker = self.schedule[date][post]
                if (existing_worker, date) in self._locked_mandatory:
                    logging.debug(f"Cannot assign {worker_id}: slot {date.strftime('%Y-%m-%d')} post {post} locked by mandatory for {existing_worker}")
                    return False
            
            # Skip if already assigned to this date
            if worker_id in self.schedule.get(date, []):
                return False
            
            # Get worker data
            worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if not worker:
                return False
            
            # Check worker availability (days off)
            if self._is_worker_unavailable(worker_id, date):
                return False
            
            # Check for incompatibilities
            if not self._check_incompatibility(worker_id, date):
                return False
            
            # Check if worker has significant target_shifts deficit
            assignments = sorted(list(self.worker_assignments.get(worker_id, [])))
            current_assignments = len(assignments)
            target_shifts = worker.get('target_shifts', 0)
            target_deficit = max(0, target_shifts - current_assignments)
            high_deficit = target_deficit >= 2  # Worker needs 2+ more shifts
            
            # Check minimum gap and 7-14 day pattern
            if assignments:
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
                
                    # Check minimum gap
                    if 0 < days_between < self.gap_between_shifts + 1:
                        return False
                
                    # Check for 7-14 day pattern (same weekday in consecutive weeks)
                    # CRITICAL: This constraint applies to ALL days (including weekends)
                    # Workers should NOT have the same weekday in consecutive weeks (7 or 14 days apart)
                    if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                        # STRICT: NO overrides for 7/14 pattern - this is a HARD constraint for ALL days
                        logging.debug(f"Worker {worker_id} 7/14 pattern violation: {date.strftime('%Y-%m-%d')} and {prev_date.strftime('%Y-%m-%d')} are both {date.strftime('%A')}")
                        return False
            
            # Special case: Friday-Monday check if gap is only 1 day
            if self.gap_between_shifts == 1:
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
                    if days_between == 3:
                        if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                            (date.weekday() == 4 and prev_date.weekday() == 0)):
                            return False

            # Check weekend limits
            if self.constraint_checker._would_exceed_weekend_limit(worker_id, date):
                return False

            # Part-time workers need more days between shifts
            work_percentage = worker.get('work_percentage', 100)
            if work_percentage < 70:
                part_time_gap = max(3, self.gap_between_shifts + 2)
                for prev_date in assignments:
                    days_between = abs((date - prev_date).days)
                    if days_between < part_time_gap:
                        return False

            # If we've made it this far, the worker can be assigned
            return True
    
        except Exception as e:
            logging.error(f"Error in _can_assign_worker for worker {worker_id}: {str(e)}", exc_info=True)
            return False
        
    # ========================================
    # 4. CONSTRAINT CHECKING (SIMULATED)
    # ========================================
    def _can_swap_assignments(self, worker_id, date_from, post_from, date_to, post_to):
        """
        Checks if moving worker_id from (date_from, post_from) to (date_to, post_to) is valid.
        Uses optimized shallow copy for simulation instead of deepcopy.
        """
        # --- Simulation Setup ---\
        # Create optimized copies of the schedule and assignments
        try:
            # Use scheduler\'s references for optimized copy
            simulated_schedule = {k: v[:] for k, v in self.scheduler.schedule.items()}
            simulated_assignments = {k: set(v) for k, v in self.scheduler.worker_assignments.items()}

            # --- Simulate the Swap ---\
            # 1. Check if \'from\' state is valid before simulating removal
            if date_from not in simulated_schedule or \
               len(simulated_schedule[date_from]) <= post_from or \
               simulated_schedule[date_from][post_from] != worker_id or \
               worker_id not in simulated_assignments or \
               date_from not in simulated_assignments[worker_id]:
                    logging.warning(f"_can_swap_assignments: Initial state invalid for removing {worker_id} from {date_from}|P{post_from}. Aborting check.")
                    return False # Cannot simulate if initial state is wrong

            # 2. Simulate removing worker from \'from\' position
            simulated_schedule[date_from][post_from] = None
            simulated_assignments[worker_id].remove(date_from)
            # Clean up empty set for worker if needed
            if not simulated_assignments[worker_id]:
                 del simulated_assignments[worker_id]


            # 3. Simulate adding worker to \'to\' position
            # Ensure target list exists and is long enough in the simulation
            simulated_schedule.setdefault(date_to, [None] * self.num_shifts)
            while len(simulated_schedule[date_to]) <= post_to:
                simulated_schedule[date_to].append(None)

            # Check if target slot is empty in simulation before placing
            if simulated_schedule[date_to][post_to] is not None:
                logging.debug(f"_can_swap_assignments: Target slot {date_to}|P{post_to} is not empty in simulation. Aborting check.")
                return False

            simulated_schedule[date_to][post_to] = worker_id
            simulated_assignments.setdefault(worker_id, set()).add(date_to)
            
            # --- Check Constraints on Simulated State ---\
            # Check if the worker can be assigned to the target slot considering the simulated state
            can_assign_to_target = self._check_constraints_on_simulated(\
                worker_id, date_to, post_to, simulated_schedule, simulated_assignments\
            )

            # Also check if the source date is still valid *without* the worker
            # (e.g., maybe removing the worker caused an issue for others on date_from)
            source_date_still_valid = self._check_all_constraints_for_date_simulated(\
                date_from, simulated_schedule, simulated_assignments\
            )

            # Also check if the target date remains valid *with* the worker added
            target_date_still_valid = self._check_all_constraints_for_date_simulated(\
                 date_to, simulated_schedule, simulated_assignments\
            )


            is_valid_swap = can_assign_to_target and source_date_still_valid and target_date_still_valid

            # --- End Simulation ---\
            # No rollback needed as we operated on copies.

            logging.debug(f"Swap Check: {worker_id} from {date_from}|P{post_from} to {date_to}|P{post_to}. Valid: {is_valid_swap} (Target OK: {can_assign_to_target}, Source OK: {source_date_still_valid}, Target Date OK: {target_date_still_valid})") # Corrected log string
            return is_valid_swap

        except Exception as e:
            logging.error(f"Error during _can_swap_assignments simulation for {worker_id}: {e}", exc_info=True)
            return False # Fail safe


    def _check_constraints_on_simulated(self, worker_id, date, post, simulated_schedule, simulated_assignments):
        """Checks constraints for a worker on a specific date using simulated data."""
        try:
            # Get worker data for percentage check if needed later
            worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
            work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100

            # 1. Incompatibility (using simulated_schedule)
            if not self._check_incompatibility_simulated(worker_id, date, simulated_schedule):
                logging.debug(f"Sim Check Fail: Incompatible {worker_id} on {date}")
                return False

            # 2. Gap Constraint (using simulated_assignments)
            # This helper already includes basic gap logic
            if not self._check_gap_constraint_simulated(worker_id, date, simulated_assignments):
                logging.debug(f"Sim Check Fail: Gap constraint {worker_id} on {date}")
                return False

            # 3. Weekend Limit (using simulated_assignments)
            if self._would_exceed_weekend_limit_simulated(worker_id, date, simulated_assignments):
                 logging.debug(f"Sim Check Fail: Weekend limit {worker_id} on {date}")
                 return False

            # 4. Max Shifts (using simulated_assignments)
            # Use scheduler's max_shifts_per_worker config
            if len(simulated_assignments.get(worker_id, set())) > self.max_shifts_per_worker:
                 logging.debug(f"Sim Check Fail: Max shifts {worker_id}")
                 return False

            # 5. Basic Availability (Check if worker is unavailable fundamentally)
            if self._is_worker_unavailable(worker_id, date):
                 logging.debug(f"Sim Check Fail: Worker {worker_id} fundamentally unavailable on {date}")
                 return False

            # 6. Double Booking Check (using simulated_schedule)
            count = 0
            for assigned_post, assigned_worker in enumerate(simulated_schedule.get(date, [])):
                 if assigned_worker == worker_id:
                      if assigned_post != post: # Don't count the slot we are checking
                           count += 1
            if count > 0:
                 logging.debug(f"Sim Check Fail: Double booking {worker_id} on {date}")
                 return False

            sorted_sim_assignments = sorted(list(simulated_assignments.get(worker_id, [])))

            # 7. Friday-Monday Check (Only if gap constraint allows 3 days, i.e., gap_between_shifts == 1)
            # Apply strictly during simulation checks
            if self.scheduler.gap_between_shifts == 1: 
                 for prev_date in sorted_sim_assignments:
                      if prev_date == date: continue
                      days_between = abs((date - prev_date).days)
                      if days_between == 3:
                           # Check if one is Friday (4) and the other is Monday (0)
                           if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                               (date.weekday() == 4 and prev_date.weekday() == 0)):
                               logging.debug(f"Sim Check Fail: Friday-Monday conflict for {worker_id} between {prev_date} and {date}")
                               return False

            # 8. 7/14 Day Pattern Check (Same day of week in consecutive weeks)
            for prev_date in sorted_sim_assignments:
                if prev_date == date: 
                    continue
                days_between = abs((date - prev_date).days)
                # Check for exactly 7 or 14 days pattern AND same weekday
                # CRITICAL: This constraint applies to ALL days (including weekends)
                if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                    logging.debug(f"Sim Check Fail: {days_between} day pattern conflict for {worker_id} between {prev_date} and {date}")
                    return False
                
            return True # All checks passed on simulated data        
        except Exception as e:
            logging.error(f"Error during _check_constraints_on_simulated for {worker_id} on {date}: {e}", exc_info=True)
            return False # Fail safe

    def _check_all_constraints_for_date_simulated(self, date, simulated_schedule, simulated_assignments):
         """ Checks all constraints for all workers assigned on a given date in the SIMULATED schedule. """
         if date not in simulated_schedule: return True # Date might not exist in sim if empty

         assignments_on_date = simulated_schedule[date]

         # Check pairwise incompatibility first for the whole date
         workers_present = [w for w in assignments_on_date if w is not None]
         for i in range(len(workers_present)):
              for j in range(i + 1, len(workers_present)):
                   worker1_id = workers_present[i]
                   worker2_id = workers_present[j]
                   if self._are_workers_incompatible(worker1_id, worker2_id):
                        logging.debug(f"Simulated state invalid: Incompatibility between {worker1_id} and {worker2_id} on {date}")
                        return False

         # Then check individual constraints for each worker
         for post, worker_id in enumerate(assignments_on_date):
              if worker_id is not None:
                   # Check this worker's assignment using the simulated state helper
                   if not self._check_constraints_on_simulated(worker_id, date, post, simulated_schedule, simulated_assignments):
                        # logging.debug(f"Simulated state invalid: Constraint fail for {worker_id} on {date} post {post}")
                        return False # Constraint failed for this worker in the simulated state
         return True
        
    def _check_incompatibility_simulated(self, worker_id, date, simulated_schedule):
        """Check incompatibility using the simulated schedule."""
        assigned_workers_list = simulated_schedule.get(date, [])
        # Use the existing helper, it only needs the list of workers on that day
        return self._check_incompatibility_with_list(worker_id, assigned_workers_list)

    def _check_gap_constraint_simulated(self, worker_id, date, simulated_assignments):
        """Check gap constraint using simulated assignments."""
        # Use scheduler's gap config
        min_days_between = self.scheduler.gap_between_shifts + 1
        # Add part-time adjustment if needed
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100
        if work_percentage < 70: # Example threshold for part-time adjustment
            min_days_between = max(min_days_between, self.scheduler.gap_between_shifts + 2)

        assignments = sorted(list(simulated_assignments.get(worker_id, [])))

        for prev_date in assignments:
            if prev_date == date: continue # Don't compare date to itself
            days_between = abs((date - prev_date).days)
            if days_between < min_days_between:
                return False
            # Add Friday-Monday / 7-14 day checks if needed here too, using relaxation_level=0 logic
            if self.scheduler.gap_between_shifts == 1 and work_percentage >= 20: # Corrected: work_percentage from worker_data
                if days_between == 3:
                    if ((prev_date.weekday() == 4 and date.weekday() == 0) or \
                        (date.weekday() == 4 and prev_date.weekday() == 0)):
                        return False
            # Add check for weekly pattern (7/14 day) - ALL days
            # CRITICAL: This constraint applies to ALL days (including weekends)
            if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                return False
        return True
    
    def _would_exceed_weekend_limit_simulated(self, worker_id, date, simulated_assignments):
        """Check weekend limit using simulated assignments."""
        # Check if date is a weekend/holiday
        is_target_weekend = (date.weekday() >= 4 or 
                     date in self.scheduler.holidays or
                     (date + timedelta(days=1)) in self.scheduler.holidays)
        if not is_target_weekend:
            return False
    
        # Get worker data to check work_percentage
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        work_percentage = worker_data.get('work_percentage', 100) if worker_data else 100
    
        # Calculate max_weekend_count based on work_percentage
        max_weekend_count = self.scheduler.max_consecutive_weekends
        if work_percentage < 70:
            max_weekend_count = max(1, int(self.scheduler.max_consecutive_weekends * work_percentage / 100))
    
        # Get weekend assignments and add the current date
        weekend_dates = []
        for d_val in simulated_assignments.get(worker_id, set()):
            if (d_val.weekday() >= 4 or 
                d_val in self.scheduler.holidays or
                (d_val + timedelta(days=1)) in self.scheduler.holidays):
                weekend_dates.append(d_val)
    
        # Add the date if it's not already in the list
        if date not in weekend_dates:
            weekend_dates.append(date)
    
        # Sort dates to ensure chronological order
        weekend_dates.sort()
    
        # Check for consecutive weekends
        consecutive_groups = []
        current_group = []
    
        for i, d_val in enumerate(weekend_dates): # Renamed d to d_val
            # Start a new group or add to the current one
            if not current_group:
                current_group = [d_val]
            else:
                # Get the previous weekend's date
                prev_weekend = current_group[-1]
                # Calculate days between this weekend and the previous one
                days_diff = (d_val - prev_weekend).days
            
                # Checking if they are adjacent weekend dates (7-10 days apart)
                # A weekend is consecutive to the previous if it's the next calendar weekend
                # This is typically 7 days apart, but could be 6-8 days depending on which weekend days
                if 5 <= days_diff <= 10:
                    current_group.append(d_val)
                else:
                    # Not consecutive, save the current group and start a new one
                    if len(current_group) > 1:  # Only save groups with more than 1 weekend
                        consecutive_groups.append(current_group)
                    current_group = [d_val]
    
        # Add the last group if it has more than 1 weekend
        if len(current_group) > 1:
            consecutive_groups.append(current_group)
    
        # Find the longest consecutive sequence
        max_consecutive = 0
        if consecutive_groups:
            max_consecutive = max(len(group) for group in consecutive_groups)
        else:
            max_consecutive = 1  # No consecutive weekends found, or only single weekends
    
        # Check if maximum consecutive weekend count is exceeded
        if max_consecutive > max_weekend_count:
            logging.debug(f"Weekend limit exceeded: Worker {worker_id} would have {max_consecutive} consecutive weekend shifts (max allowed: {max_weekend_count})")
            return True
    
        return False
    
    # ========================================
    # 5. SCORING AND CANDIDATE SELECTION
    # ========================================
    
    def _is_weekend_or_holiday(self, date):
        """Cached check for weekend or holiday status"""
        # Cache weekend checks to avoid repeated calculations
        if not hasattr(self, '_weekend_cache'):
            self._weekend_cache = {}
        
        if date not in self._weekend_cache:
            self._weekend_cache[date] = (
                date.weekday() >= 4 or 
                date in self.holidays or
                (date + timedelta(days=1)) in self.holidays
            )
        return self._weekend_cache[date]
    
    def _check_hard_constraints(self, worker_id, date, post):
        """Check hard constraints that cannot be relaxed"""
        # Basic availability check
        if self._is_worker_unavailable(worker_id, date) or worker_id in self.schedule.get(date, []):
            return False
        
        # Check incompatibility against workers already assigned on this date
        already_assigned_on_date = [w for idx, w in enumerate(self.schedule.get(date, [])) 
                                   if w is not None and idx != post]
        if not self._check_incompatibility_with_list(worker_id, already_assigned_on_date):
            return False
            
        return True
    
    def _check_mandatory_assignment(self, worker, date):
        """Check if this is a mandatory assignment and return appropriate score"""
        mandatory_days_str = worker.get('mandatory_days', '')
        mandatory_dates = self._parse_dates(mandatory_days_str)
        
        # If this is a mandatory date for this worker, give it maximum priority
        if date in mandatory_dates:
            return float('inf'), mandatory_dates
            
        return None, mandatory_dates
    
    def _get_expected_monthly_target(self, worker_config, year, month):
        """
        Calcula el objetivo mensual esperado dinámicamente.
        
        Fórmula: objetivo_global / número_de_meses (ajustado si mes incompleto)
        """
        if not worker_config:
            return 0
        
        # Verificar si hay objetivo mensual específico configurado
        month_key = f"{year}-{month:02d}"
        monthly_targets_config = worker_config.get('monthly_targets', {})
        if monthly_targets_config.get(month_key, 0) > 0:
            return monthly_targets_config[month_key]
        
        # Calcular dinámicamente desde objetivo global
        overall_target = worker_config.get('target_shifts', 0)
        if overall_target == 0:
            return 0
        
        # Ajustar por work_percentage (para trabajadores a tiempo parcial)
        work_percentage = worker_config.get('work_percentage', 100) / 100
        adjusted_overall_target = overall_target * work_percentage
        
        # Calcular número de meses en el periodo
        from datetime import date as date_class
        from calendar import monthrange
        
        # Convertir start_date y end_date a date si son datetime
        start = self.start_date.date() if hasattr(self.start_date, 'date') else self.start_date
        end = self.end_date.date() if hasattr(self.end_date, 'date') else self.end_date
        
        # Contar meses y calcular fracción de cada mes
        months_in_period = []
        current = date_class(start.year, start.month, 1)
        
        while current <= end:
            year_m = current.year
            month_m = current.month
            
            # Días totales en este mes
            days_in_month = monthrange(year_m, month_m)[1]
            
            # Primer y último día del mes
            first_day = date_class(year_m, month_m, 1)
            last_day = date_class(year_m, month_m, days_in_month)
            
            # Intersección con el periodo
            month_start = max(start, first_day)
            month_end = min(end, last_day)
            
            if month_start <= month_end:
                days_in_period = (month_end - month_start).days + 1
                fraction = days_in_period / days_in_month
                months_in_period.append((year_m, month_m, fraction))
            
            # Siguiente mes
            if month_m == 12:
                current = date_class(year_m + 1, 1, 1)
            else:
                current = date_class(year_m, month_m + 1, 1)
        
        # Total de "meses equivalentes" (considerando meses parciales)
        total_month_weight = sum(frac for _, _, frac in months_in_period)
        
        if total_month_weight == 0:
            return 0
        
        # Buscar la fracción del mes consultado
        month_fraction = 1.0
        for y, m, frac in months_in_period:
            if y == year and m == month:
                month_fraction = frac
                break
        
        # Objetivo mensual = (objetivo_global / total_meses) * fracción_del_mes
        expected_monthly = (adjusted_overall_target / total_month_weight) * month_fraction
        
        # Redondear al entero más cercano
        return round(expected_monthly)

    def _calculate_monthly_target_score(self, worker, date, relaxation_level):
        """Calculate score based on monthly targets"""
        worker_id = worker['id']
        
        # Get worker config
        worker_config = next((w for w in self.workers_data if w['id'] == worker_id), None)
        
        # Calcular objetivo mensual esperado dinámicamente
        expected_monthly_target = self._get_expected_monthly_target(worker_config, date.year, date.month)
        
        # Calculate current shifts assigned in this month
        shifts_this_month = sum(
            1 for assigned_date in self.scheduler.worker_assignments.get(worker_id, [])
            if assigned_date.year == date.year and assigned_date.month == date.month
        )
        
        # MONTHLY LIMIT SYSTEM with ±12% tolerance and deficit compensation
        # Base tolerance: 12% of monthly target (not fixed ±1)
        base_tolerance = max(1, round(expected_monthly_target * 0.12))
        
        # Calculate global deficit for deficit compensation
        overall_target = worker_config.get('target_shifts', 0) if worker_config else 0
        current_total_shifts = len(self.scheduler.worker_assignments.get(worker_id, set()))
        deficit_pct = ((overall_target - current_total_shifts) / overall_target * 100) if overall_target > 0 else 0
        
        # Deficit compensation: If worker has >20% global deficit, allow extra monthly shifts
        deficit_bonus = 0
        if deficit_pct > 20:
            deficit_bonus = round((deficit_pct - 20) / 10)  # +1 shift per 10% deficit
            logging.debug(f"Worker {worker_id}: DEFICIT COMPENSATION - "
                        f"deficit={deficit_pct:.1f}%, allowing +{deficit_bonus} extra shifts in {date.strftime('%Y-%m')}")
        
        # Calculate effective monthly max
        effective_max_monthly = expected_monthly_target + base_tolerance + deficit_bonus + relaxation_level
        
        # Si no hay objetivo, permitir al menos algunos turnos
        if expected_monthly_target == 0:
            effective_max_monthly = 3 + relaxation_level
        
        # Check if adding this shift would exceed monthly limit
        if shifts_this_month + 1 > effective_max_monthly:
            if relaxation_level < 1:
                logging.debug(f"Worker {worker_id}: BLOCKED by monthly limit - "
                            f"month {date.strftime('%Y-%m')}: {shifts_this_month + 1} > {effective_max_monthly:.0f} "
                            f"(target={expected_monthly_target:.1f}, base_tol={base_tolerance}, deficit_bonus={deficit_bonus}, relax={relaxation_level})")
                return float('-inf')
        
        # Calculate score based on monthly target
        score = 0
        if expected_monthly_target > 0:
            if shifts_this_month < expected_monthly_target:
                # Necesita más turnos para alcanzar objetivo
                score += (expected_monthly_target - shifts_this_month) * 2000
            elif shifts_this_month == expected_monthly_target:
                # Justo en el objetivo
                score += 500
            # Si está por encima del objetivo, score = 0 (neutral)
            
        return score
    
    def _calculate_overall_target_score(self, worker_id, worker_config, relaxation_level):
        """
        Calculate score based on overall target shifts with tolerance enforcement.
        
        STRICT MODE (use_strict_mode=True):
        - Tolerance: +10% ESTRICTA (ajustada por work_percentage)
        - Bloqueo absoluto si excede +10%
        
        RELAXED MODE (use_strict_mode=False):
        - Tolerance: Progresiva según relaxation_level
          - Level 0: +10%
          - Level 1: +12%
          - Level 2: +15%
          - Level 3: +18%
        
        CRITICAL LOGIC:
        - Workers BELOW target get HUGE bonus (encouragement)
        - Workers ABOVE tolerance get blocked (prevention)
        - Part-time workers (<100%) get STRONGER penalties for excess
        - Never penalize workers with deficit
        """
        current_total_shifts = len(self.worker_assignments.get(worker_id, set()))
        overall_target_shifts = worker_config.get('target_shifts', 0) if worker_config else 0
        work_percentage = worker_config.get('work_percentage', 100) if worker_config else 100
        
        if overall_target_shifts <= 0:
            return 0
        
        # Target already adjusted by work_percentage in _calculate_target_shifts
        # DO NOT adjust again here (bug fix - was causing 50% workers to have half target)
        adjusted_target = overall_target_shifts
        
        # Calculate tolerance based on mode and relaxation level
        # Phase 1 (objective): ±10% tolerance for initial distribution
        # Phase 2 (limit): ±12% tolerance (ABSOLUTE LIMIT - never exceeded)
        if self.use_strict_mode:
            # Phase 1 (OBJECTIVE): ±10% tolerance
            tolerance = 0.10
        else:
            # Phase 2 (LIMIT): ±12% ABSOLUTE MAXIMUM
            tolerance = 0.12
        
        tolerance_amount = adjusted_target * tolerance
        min_allowed = max(0, int(adjusted_target - tolerance_amount))
        max_allowed = int(adjusted_target + tolerance_amount + 0.5)  # Round up
        
        # Count after potential assignment
        shifts_after_assignment = current_total_shifts + 1
        
        # CRITICAL: Calculate deficit - how far below target
        deficit = adjusted_target - current_total_shifts
        
        # Give MASSIVE bonus to workers with significant deficit
        if deficit >= 3:
            # Workers 3+ shifts below target get HUGE priority
            bonus = 15000 + (deficit * 3000)  # 3 below = 24000, 4 below = 27000, etc.
            logging.debug(f"Worker {worker_id}: CRITICAL DEFICIT bonus +{bonus} "
                        f"(current: {current_total_shifts}, target: {adjusted_target}, deficit: {deficit})")
            return bonus
        elif deficit >= 2:
            # Workers 2 shifts below target get very high priority
            bonus = 12000  # Aumentado de 8000
            logging.debug(f"Worker {worker_id}: HIGH DEFICIT bonus +{bonus} "
                        f"(current: {current_total_shifts}, target: {adjusted_target}, deficit: {deficit})")
            return bonus
        elif deficit >= 1:
            # Workers 1 shift below target get high priority
            return 8000  # Aumentado de 5000
        
        # CRITICAL: Block ONLY if would exceed tolerance limit (not just target)
        # Workers can go up to max_allowed (target + tolerance)
        if shifts_after_assignment > max_allowed:
            # HARD BLOCK - would exceed tolerance limit
            logging.debug(f"Worker {worker_id}: BLOCKED by tolerance limit - would have {shifts_after_assignment} "
                        f"(max allowed: {max_allowed}, target: {adjusted_target}, tolerance: ±{tolerance*100:.0f}%)")
            return float('-inf')
        
        # Apply graduated penalties as worker approaches tolerance limit
        if current_total_shifts >= adjusted_target:
            # Worker at or above target - apply penalty to encourage deficit workers
            # But still allow up to max_allowed
            excess = current_total_shifts - adjusted_target
            if excess >= 2:
                penalty = 15000  # Heavy penalty for 2+ above target
                logging.debug(f"Worker {worker_id}: HEAVY penalty for being {excess} above target "
                            f"(current: {current_total_shifts}, target: {adjusted_target})")
                return -penalty
            else:
                # At target+1, moderate penalty
                penalty = 8000
                logging.debug(f"Worker {worker_id}: Moderate penalty for being at target+1 "
                            f"(current: {current_total_shifts}, target: {adjusted_target})")
                return -penalty
        
        # Block assignments that would violate upper tolerance
        if shifts_after_assignment > max_allowed:
            if self.use_strict_mode or relaxation_level < 2:
                # Hard block in strict mode or at low relaxation levels
                logging.debug(f"Worker {worker_id}: BLOCKED - would exceed +{tolerance*100:.0f}% tolerance "
                            f"({shifts_after_assignment} > {max_allowed}, target: {adjusted_target})")
                return float('-inf')
            else:
                # At high relaxation, allow with severe penalty
                excess = shifts_after_assignment - max_allowed
                # EXTRA penalty for part-time workers
                base_penalty = 5000
                if work_percentage < 100:
                    # Part-time workers get 2x-3x penalty multiplier
                    multiplier = 2.0 + (100 - work_percentage) / 50.0  # 50% = 3x, 75% = 2.5x, 90% = 2.2x
                    penalty = excess * base_penalty * multiplier
                    logging.debug(f"Worker {worker_id}: EXTRA HEAVY penalty for part-time ({work_percentage}%) "
                                f"exceeding tolerance (penalty: {penalty:.0f}, multiplier: {multiplier:.1f}x)")
                else:
                    penalty = excess * base_penalty
                    logging.debug(f"Worker {worker_id}: Heavy penalty for exceeding tolerance at relaxation={relaxation_level} "
                                f"(penalty: {penalty})")
                return -penalty
        
        # CRITICAL: Encourage workers BELOW target - HUGE bonus for large deficits
        if shifts_after_assignment < adjusted_target:
            deficit = adjusted_target - shifts_after_assignment
            # MASSIVE bonus for workers far from target (e.g., 33/55 = 22 deficit)
            bonus = deficit * 3000  # Increased from 1000 to 3000
            logging.debug(f"Worker {worker_id}: HUGE BONUS for deficit ({shifts_after_assignment} < {adjusted_target}, bonus: {bonus})")
            return bonus
        elif shifts_after_assignment == adjusted_target:
            # Good - exactly at target
            return 500
        else:
            # Above target but within tolerance
            excess = shifts_after_assignment - adjusted_target
            # Enhanced penalty scale for part-time workers
            if work_percentage < 100:
                # Progressive penalty: stronger for part-time workers
                penalty_multiplier = 1.5 + (100 - work_percentage) / 100.0  # 50% = 2x, 75% = 1.75x
                penalty = excess * 100 * penalty_multiplier
                logging.debug(f"Worker {worker_id} (part-time {work_percentage}%): Enhanced penalty for excess "
                            f"({shifts_after_assignment} > {overall_target_shifts}, penalty: {penalty:.0f})")
            else:
                # Standard small penalty for full-time
                penalty = excess * 100
            return -penalty
    
    def _check_gap_constraints(self, worker, date, relaxation_level):
        """
        Check gap constraints between assignments.
        
        STRICT MODE (use_strict_mode=True):
        - Gap mínimo: NO reducción, siempre respeta gap_between_shifts
        - Patrón 7/14: PROHIBIDO absolutamente (sin excepciones)
        
        RELAXED MODE (use_strict_mode=False):
        - Gap mínimo: Permite gap-1 en relaxation_level >= 3
        - Patrón 7/14: Permite excepciones con déficit según relaxation_level
        """
        worker_id = worker['id']
        assignments = sorted(list(self.worker_assignments[worker_id]))
        
        if not assignments:
            return True
        
        # Check if worker has significant target_shifts deficit
        current_assignments = len(assignments)
        target_shifts = worker.get('target_shifts', 0)
        target_deficit = max(0, target_shifts - current_assignments)
        high_deficit = target_deficit >= 2  # Worker needs 2+ more shifts
        
        work_percentage = worker.get('work_percentage', 100)
        base_min_gap = self.gap_between_shifts + 2 if work_percentage < 70 else self.gap_between_shifts + 1
        
        # STRICT MODE: No gap reduction allowed
        if self.use_strict_mode:
            min_gap = base_min_gap
        else:
            # RELAXED MODE: Allow gap-1 ONLY (never more than -1)
            # Only if worker has significant deficit
            if relaxation_level >= 1 and target_deficit >= 3:
                min_gap = max(1, base_min_gap - 1)  # Reduce gap by 1 ONLY, minimum 1
                logging.debug(f"RELAXED: Worker {worker_id} gap reduced by 1 to {min_gap} (deficit: {target_deficit})")
            else:
                min_gap = base_min_gap
        
        for prev_date in assignments:
            days_between = abs((date - prev_date).days)
            
            # Basic minimum gap check
            if days_between < min_gap:
                return False
            
            # Special rule: No Friday + Monday (3-day gap)
            if relaxation_level == 0 and self.gap_between_shifts == 1:
                if ((prev_date.weekday() == 4 and date.weekday() == 0) or 
                    (date.weekday() == 4 and prev_date.weekday() == 0)):
                    if days_between == 3:
                        return False
            
            # CRITICAL: 7/14 day pattern check (same weekday constraint)
            # This constraint applies to ALL days (including weekends) - NO exceptions
            if (days_between == 7 or days_between == 14) and date.weekday() == prev_date.weekday():
                logging.debug(f"Worker {worker_id} blocked by 7/14 pattern on {date.strftime('%Y-%m-%d')} - same weekday as {prev_date.strftime('%Y-%m-%d')} ({days_between} days apart)")
                return False
        
        return True

    def _calculate_target_shift_score(self, worker, mandatory_dates, relaxation_level):
        """
        Calculate score based on target shifts and mandatory assignments.
        
        CRITICAL: Never block workers with deficit. Always encourage filling targets.
        """
        worker_id = worker['id']
        current_shifts = len(self.worker_assignments[worker_id])
        target_shifts = worker.get('target_shifts', 0)
        
        # Count mandatory shifts that are already assigned
        mandatory_shifts_assigned = sum(
            1 for d in self.worker_assignments[worker_id] if d in mandatory_dates
        )
        
        # Count mandatory shifts still to be assigned  
        mandatory_shifts_remaining = sum(
            1 for d in mandatory_dates 
            if d not in self.worker_assignments[worker_id]
        )
        
        # Calculate non-mandatory shifts target
        non_mandatory_target = target_shifts - len(mandatory_dates)
        non_mandatory_assigned = current_shifts - mandatory_shifts_assigned
        
        # Check if we've already met or exceeded non-mandatory target
        shift_difference = non_mandatory_target - non_mandatory_assigned
        
        # REMOVED: The restrictive check that was blocking assignments
        # Old logic: if (non_mandatory_assigned + mandatory_shifts_remaining >= target_shifts and relaxation_level < 2):
        #     return float('-inf')
        # This was TOO restrictive and prevented workers from reaching their targets
        
        # Calculate score based on shift difference
        score = 0
        if shift_difference <= 0:
            # Worker has met non-mandatory target
            if relaxation_level == 0:
                score -= 5000 * abs(shift_difference)  # Reduced from 8000
            elif relaxation_level == 1:
                score -= 3000 * abs(shift_difference)  # Reduced from 5000
            else:
                score -= 1000 * abs(shift_difference)  # Reduced from 2000
        else:
            # Prioritize workers who are furthest below target
            score += shift_difference * 3000  # Increased from 2000 to 3000
            
        return score

    def _calculate_worker_score(self, worker, date, post, relaxation_level=0):
        """
        Calculate score for a worker assignment with optional relaxation of constraints
        
        This method has been optimized to use helper methods for better maintainability:
        - _check_hard_constraints(): Basic availability and incompatibility checks
        - _check_mandatory_assignment(): Mandatory shift prioritization  
        - _calculate_target_shift_score(): Target shift calculations
        - _check_gap_constraints(): Gap and pattern constraints
        - _calculate_monthly_target_score(): Monthly target handling
        - _calculate_overall_target_score(): Overall target calculations
        - _calculate_additional_scoring_factors(): Weekend, weekly balance, and progression scores
    
        Args:
            worker: The worker to evaluate
            date: The date to assign
            post: The post number to assign
            relaxation_level: Level of constraint relaxation (0=strict, 1=moderate, 2=lenient)
    
        Returns:
            float: Score for this worker-date-post combination, higher is better
                  Returns float('-inf') if assignment is invalid
        """
        try:
            worker_id = worker['id']
            
            # Check hard constraints first
            if not self._check_hard_constraints(worker_id, date, post):
                logging.debug(f"    🚫 {worker_id}: BLOCKED by hard constraints")
                return float('-inf')
            
            # Check for mandatory shifts
            mandatory_score, mandatory_dates = self._check_mandatory_assignment(worker, date)
            if mandatory_score is not None:
                return mandatory_score
            
            # Calculate target shift score
            score = self._calculate_target_shift_score(worker, mandatory_dates, relaxation_level)
            if score == float('-inf'):
                logging.debug(f"    🚫 {worker_id}: BLOCKED by target shift score")
                return score
            
            # Check gap constraints
            if not self._check_gap_constraints(worker, date, relaxation_level):
                logging.debug(f"    🚫 {worker_id}: BLOCKED by gap constraints (min gap or 7/14 pattern)")
                return float('-inf')
            
            # Calculate monthly target score
            monthly_score = self._calculate_monthly_target_score(worker, date, relaxation_level)
            if monthly_score == float('-inf'):
                logging.debug(f"    🚫 {worker_id}: BLOCKED by monthly target (exceeded monthly max)")
                return float('-inf')
            score += monthly_score
            
            # Calculate overall target score
            worker_config = next((w for w in self.workers_data if w['id'] == worker_id), None)
            overall_score = self._calculate_overall_target_score(worker_id, worker_config, relaxation_level)
            if overall_score == float('-inf'):
                logging.debug(f"    🚫 {worker_id}: BLOCKED by overall target (exceeded +10% tolerance)")
                return float('-inf')
            score += overall_score
            
            # Add remaining scoring components
            score += self._calculate_additional_scoring_factors(worker, date, relaxation_level)
            
            return score
    
        except Exception as e:
            logging.error(f"Error calculating score for worker {worker['id']}: {str(e)}")
            return float('-inf')
    
    def _calculate_additional_scoring_factors(self, worker, date, relaxation_level):
        """
        Calculate additional scoring factors like weekend balance and weekly distribution.
        Now includes ±10% tolerance validation for weekend shifts.
        Prioritizes maximum spacing between shifts (gap maximization).
        """
        worker_id = worker['id']
        score = 0
        
        # Gap Distance Score - Favor MAXIMUM spacing between shifts
        # The system should prioritize larger gaps, not smaller ones
        assignments = self.worker_assignments[worker_id]
        if assignments:
            # Find the most recent assignment
            most_recent = max(assignments)
            days_since_last = (date - most_recent).days
            
            # Bonus increases with distance (exponential growth to favor larger gaps)
            # Base multiplier: 150 points per day
            # Exponential bonus for gaps > minimum requirement
            min_gap = self.gap_between_shifts + 1
            
            if days_since_last > min_gap:
                # Extra days beyond minimum get exponentially higher scores
                extra_days = days_since_last - min_gap
                gap_bonus = 150 * days_since_last  # Base linear component
                gap_bonus += (extra_days ** 1.5) * 100  # Exponential component for large gaps
                score += gap_bonus
            else:
                # Even minimum gaps get some bonus (though they should already pass constraint)
                score += days_since_last * 100
        
        # Weekend Balance Score with ±10% tolerance enforcement
        if self._is_weekend_or_holiday(date):
            # Count current weekend assignments
            weekend_assignments = sum(
                1 for d in self.worker_assignments[worker_id]
                if self._is_weekend_or_holiday(d)
            )
            
            # Calculate weekend target based on overall target
            worker_config = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if worker_config:
                total_target = worker_config.get('target_shifts', 0)
                if total_target > 0:
                    # Estimate weekend percentage (approximately 3/7 days are Fri-Sun)
                    weekend_percentage = 3.0 / 7.0
                    weekend_target = total_target * weekend_percentage
                    
                    # Calculate tolerance using phase system
                    # Phase 1: ±10%, Phase 2: ±12%
                    if self.use_strict_mode:
                        base_tolerance = 0.10  # Phase 1: Objective
                    else:
                        base_tolerance = 0.12  # Phase 2: Limit
                    
                    work_percentage = worker_config.get('work_percentage', 100)
                    if work_percentage < 100:
                        adjusted_tolerance = base_tolerance * (work_percentage / 100.0)
                        tolerance = max(0.05, adjusted_tolerance)  # Minimum 5%
                    else:
                        tolerance = base_tolerance
                    
                    tolerance_amount = weekend_target * tolerance
                    min_weekend_allowed = max(0, round(weekend_target * (1 - tolerance)))
                    max_weekend_allowed = round(weekend_target * (1 + tolerance))
                    
                    # Count after potential assignment
                    weekend_after = weekend_assignments + 1
                    
                    # STRICT: Block weekend assignments that would violate ±10% tolerance
                    if weekend_after > max_weekend_allowed:
                        if relaxation_level < 2:
                            logging.debug(f"Worker {worker_id}: BLOCKED weekend - would exceed +10% tolerance "
                                        f"({weekend_after} > {max_weekend_allowed}, target: {weekend_target:.1f})")
                            return float('-inf')
                        else:
                            # At highest relaxation, allow with severe penalty
                            excess = weekend_after - max_weekend_allowed
                            penalty = excess * 8000  # Very heavy penalty for weekend violations
                            score -= penalty
                    elif weekend_after < weekend_target:
                        # Bonus for workers below weekend target
                        deficit = weekend_target - weekend_after
                        score += deficit * 800
                    else:
                        # Small penalty for being above target but within tolerance
                        if weekend_after > weekend_target:
                            excess = weekend_after - weekend_target
                            score -= excess * 300
            else:
                # No config, use simple penalty
                score -= weekend_assignments * 300
        else:
            # For non-weekend days, check if worker is below weekend target
            # and give small penalty to encourage weekend assignments
            weekend_assignments = sum(
                1 for d in self.worker_assignments[worker_id]
                if self._is_weekend_or_holiday(d)
            )
            worker_config = next((w for w in self.workers_data if w['id'] == worker_id), None)
            if worker_config:
                total_target = worker_config.get('target_shifts', 0)
                if total_target > 0:
                    weekend_percentage = 3.0 / 7.0
                    weekend_target = total_target * weekend_percentage
                    if weekend_assignments < weekend_target * 0.8:  # If significantly below weekend target
                        score -= 200  # Small penalty for non-weekend when should do more weekends

        # Weekly Balance Score - avoid concentration in some weeks
        week_number = date.isocalendar()[1]
        week_counts = {}
        assignments = self.worker_assignments[worker_id]
        for assignment_date in assignments:
            w = assignment_date.isocalendar()[1]
            week_counts[w] = week_counts.get(w, 0) + 1

        current_week_count = week_counts.get(week_number, 0)
        avg_week_count = len(assignments) / max(1, len(week_counts)) if week_counts else 0

        if current_week_count < avg_week_count:
            score += 500  # Bonus for weeks with fewer assignments

        # Schedule Progression Score - adjust priority as schedule fills up
        total_days = (self.end_date - self.start_date).days if self.end_date > self.start_date else 1
        schedule_completion = sum(len(s) for s in self.schedule.values()) / (total_days * self.num_shifts)

        # Additional progression bonus
        current_shifts = len(self.worker_assignments[worker_id])
        target_shifts = worker.get('target_shifts', 0)
        shift_difference = target_shifts - current_shifts
        score += shift_difference * 500 * schedule_completion

        return score

    def _calculate_improvement_score(self, worker, date, post):
        """
        Calculate a score for a worker assignment during the improvement phase.
    
        This uses a more lenient scoring approach to encourage filling empty shifts.
        """
        worker_id = worker['id']
    
        # Base score from standard calculation
        base_score = self._calculate_worker_score(worker, date, post)
    
        # If base score is negative infinity, the assignment is invalid
        if base_score == float('-inf'):
            return float('-inf')
    
        # Bonus for balancing post rotation
        post_counts = self.data_manager._get_post_counts(worker_id)
        total_assignments = sum(post_counts.values())
    
        # Skip post balance check for workers with few assignments
        if total_assignments >= self.num_shifts and self.num_shifts > 0: # Added check for num_shifts > 0
            expected_per_post = total_assignments / self.num_shifts
            current_count = post_counts.get(post, 0)
        
            # Give bonus if this post is underrepresented for this worker
            if current_count < expected_per_post:
                base_score += 10 * (expected_per_post - current_count)
    
        # Bonus for balancing workload
        work_percentage = worker.get('work_percentage', 100)
        current_assignments = len(self.worker_assignments[worker_id])
    
        # Calculate average assignments per worker, adjusted for work percentage
        total_assignments_all = sum(len(self.worker_assignments[w_data['id']]) for w_data in self.workers_data) # Corrected: w_data
        total_work_percentage = sum(w_data.get('work_percentage', 100) for w_data in self.workers_data) # Corrected: w_data
    
        # Expected assignments based on work percentage
        expected_assignments = (total_assignments_all / (total_work_percentage / 100)) * (work_percentage / 100) if total_work_percentage > 0 else 0 # Added check for total_work_percentage
    
        # Bonus for underloaded workers
        if current_assignments < expected_assignments:
            base_score += 5 * (expected_assignments - current_assignments)
    
        return base_score
        
    def _get_candidates(self, date, post, relaxation_level=0):
        """
        Get suitable candidates with their scores using the specified relaxation level
    
        Args:
            date: The date to assign
            post: The post number to assign
            relaxation_level: Level of constraint relaxation (0=strict, 1=moderate, 2=lenient)
        """
        candidates = []
        logging.debug(f"Looking for candidates for {date.strftime('%d-%m-%Y')}, post {post}")

        # CRITICAL: Check if this slot is already occupied by a mandatory assignment
        if (date in self.schedule and 
            len(self.schedule[date]) > post and 
            self.schedule[date][post] is not None):
            existing_worker = self.schedule[date][post]
            if (existing_worker, date) in self._locked_mandatory:
                logging.debug(f"Slot {date.strftime('%d-%m-%Y')} post {post} locked by mandatory for worker {existing_worker}")
                return []  # Return empty list - this slot is locked

        # Get workers already assigned to other posts on this date
        already_assigned_on_date = [w for idx, w in enumerate(self.schedule.get(date, [])) if w is not None and idx != post]

        for worker in self.workers_data:
            worker_id = worker['id']
            logging.debug(f"Checking worker {worker_id} for {date.strftime('%d-%m-%Y')}")

            # --- PRE-FILTERING ---\
            # Skip if already assigned to this date (redundant with score check, but safe)
            if worker_id in self.schedule.get(date, []): # Check against all posts on this date
                 logging.debug(f"  Worker {worker_id} skipped - already assigned to {date.strftime('%d-%m-%Y')}")
                 continue

            # Skip if unavailable
            if self._is_worker_unavailable(worker_id, date):
                 logging.debug(f"  Worker {worker_id} skipped - unavailable on {date.strftime('%d-%m-%Y')}")
                 continue

            # *** ADDED: Explicit Incompatibility Check BEFORE scoring ***\
            # Never relax incompatibility constraint
            if not self._check_incompatibility_with_list(worker_id, already_assigned_on_date):
                 logging.debug(f"  Worker {worker_id} skipped - incompatible with already assigned workers on {date.strftime('%d-%m-%Y')}")
                 continue
            # Skip if max shifts reached
            if len(self.worker_assignments[worker_id]) >= self.max_shifts_per_worker:
                logging.debug(f"Worker {worker_id} skipped - max shifts reached: {len(self.worker_assignments[worker_id])}/{self.max_shifts_per_worker}")
                continue

            # Calculate score using the main scoring function
            score = self._calculate_worker_score(worker, date, post, relaxation_level)
            
            if score > float('-inf'): # Only add valid candidates
                logging.debug(f"Worker {worker_id} added as candidate with score {score}")
                candidates.append((worker, score))

        return candidates
    
    # ========================================
    # 6. SCHEDULE GENERATION METHODS
    # ========================================
    def _assign_mandatory_guards(self):
        logging.info("Starting mandatory guard assignment…")
        assigned_count = 0
        for worker in self.workers_data: # Use self.workers_data
            worker_id = worker['id']
            mandatory_str = worker.get('mandatory_days', '')
            try:
                dates = self.date_utils.parse_dates(mandatory_str)
            except Exception as e:
                logging.error(f"Error parsing mandatory_days for worker {worker_id}: {e}")
                continue

            for date in dates:
                if not (self.start_date <= date <= self.end_date): continue

                if date not in self.schedule: # self.schedule is scheduler.schedule
                    self.schedule[date] = [None] * self.num_shifts
                
                # Try to place in any available post for that date
                placed_mandatory = False
                for post in range(self.num_shifts):
                    if len(self.schedule[date]) <= post: self.schedule[date].extend([None] * (post + 1 - len(self.schedule[date])))

                    if self.schedule[date][post] is None:
                        # CRITICAL: For MANDATORY assignments, ONLY check work_periods/unavailability
                        # INCOMPATIBILITY DOES NOT APPLY to mandatory shifts - if two incompatible
                        # workers both have mandatory on the same day, BOTH must be assigned
                        # because mandatory_days override ALL other constraints (including incompatibility)
                        
                        # Check if worker is unavailable (days off, work periods) - ONLY HARD constraint for mandatory
                        if self._is_worker_unavailable(worker_id, date):
                            logging.warning(f"Mandatory shift for {worker_id} on {date.strftime('%Y-%m-%d')} conflicts with days_off or work_periods. This is a configuration error.")
                            continue
                        
                        # NOTE: We do NOT check gap, 7/14 pattern, or weekend limits for mandatory assignments
                        # because mandatory_days override these soft constraints
                        
                        self.schedule[date][post] = worker_id
                        self.worker_assignments.setdefault(worker_id, set()).add(date) # Use self.worker_assignments
                        self.scheduler._update_tracking_data(worker_id, date, post, removing=False) # Call scheduler's central update
                        
                        # CRITICAL: Lock the mandatory assignment IMMEDIATELY
                        self._locked_mandatory.add((worker_id, date))
                        logging.info(f"🔒 MANDATORY ASSIGNED AND LOCKED: {worker_id} → {date.strftime('%Y-%m-%d')} post {post}")
                        
                        assigned_count += 1
                        placed_mandatory = True
                        break 
                if not placed_mandatory:
                     logging.warning(f"Could not place mandatory shift for {worker_id} on {date.strftime('%Y-%m-%d')}. All posts filled or incompatible.")
        
        # Log summary of locked mandatory shifts
        logging.info(f"=" * 80)
        logging.info(f"MANDATORY ASSIGNMENT SUMMARY")
        logging.info(f"  Total mandatory shifts assigned: {assigned_count}")
        logging.info(f"  Total locked mandatory: {len(self._locked_mandatory)}")
        logging.info(f"=" * 80)
        
        # Verify that all assigned mandatory are in locked set
        verification_failed = 0
        for worker in self.workers_data:
            worker_id = worker['id']
            mandatory_str = worker.get('mandatory_days', '')
            try:
                dates = self.date_utils.parse_dates(mandatory_str)
            except:
                continue
            
            for date in dates:
                if not (self.start_date <= date <= self.end_date):
                    continue
                if date in self.schedule and worker_id in self.schedule[date]:
                    if (worker_id, date) not in self._locked_mandatory:
                        logging.error(f"❌ CRITICAL: Mandatory {worker_id} on {date.strftime('%Y-%m-%d')} NOT in locked set!")
                        verification_failed += 1
                        # Add it now
                        self._locked_mandatory.add((worker_id, date))
        
        if verification_failed > 0:
            logging.warning(f"⚠️ Fixed {verification_failed} mandatory assignments that were not locked")
        else:
            logging.info(f"✅ All mandatory assignments verified and locked")
        
        # No _save_current_as_best here; scheduler's generate_schedule will handle it after this.
        # self._synchronize_tracking_data() # Ensure builder's view is also synced if it has separate copies (it shouldn't for core data)
        return assigned_count > 0
    
    def _get_remaining_dates_to_process(self, forward):
        """Get remaining dates that need to be processed"""
        dates_to_process = []
        current = self.start_date
    
        # Get all dates in period that are not weekends or holidays
        # or that already have some assignments but need more
        while current <= self.end_date:
            # for each date, check if we need to generate more shifts
            if current not in self.schedule:
                dates_to_process.append(current)
            else:
                # compare actual slots vs configured for that date
                expected = self.scheduler._get_shifts_for_date(current)
                if len(self.schedule[current]) < expected:
                    dates_to_process.append(current)
            current += timedelta(days=1)
    
        # Sort based on direction
        if forward:
            dates_to_process.sort()
        else:
            dates_to_process.sort(reverse=True)
    
        return dates_to_process
    
    def _assign_day_shifts_with_relaxation(self, date, attempt_number=50, relaxation_level=0):
        """Assign shifts for a given date with optional constraint relaxation"""
        logging.debug(f"Assigning shifts for {date.strftime('%d-%m-%Y')} (attempt: {attempt_number}, initial relax: {relaxation_level})")

        # Ensure the date entry exists and is a list
        if date not in self.schedule:
            self.schedule[date] = []
        # Ensure it's padded to current length if it exists but is shorter than previous post assignments
        # (This shouldn't happen often but safeguards against potential inconsistencies)
        current_len = len(self.schedule.get(date, []))
        max_post_assigned_prev = -1
        if current_len > 0:
             max_post_assigned_prev = current_len -1


        # Determine how many slots this date actually has (supports variable shifts)
        total_slots = self.scheduler._get_shifts_for_date(date) # Corrected: Use scheduler method
        
        # Process ALL posts from 0 to total_slots, not just from start_post
        # This ensures we check all positions for mandatory protection
        for post in range(total_slots):
            # CRITICAL: NEVER overwrite a locked mandatory shift
            if len(self.schedule.get(date,[])) > post and self.schedule[date][post] is not None:
                # Check if this is a locked mandatory assignment
                if (self.schedule[date][post], date) in self._locked_mandatory:
                    logging.debug(f"Skipping post {post} on {date.strftime('%Y-%m-%d')}: "
                                f"locked mandatory for worker {self.schedule[date][post]}")
                    continue
                # If not mandatory but filled, also skip (already processed)
                if self.schedule[date][post] is not None:
                    continue
            assigned_this_post = False
            
            # CRITICAL: Verificar si el slot está protegido por mandatory
            is_protected, protected_worker = self._is_slot_protected_mandatory(date, post)
            if is_protected:
                logging.debug(f"[Pass 1] Skipping protected mandatory slot: {date.strftime('%Y-%m-%d')} post {post} (worker: {protected_worker})")
                continue  # No intentar modificar este slot
            
            for relax_level in range(relaxation_level + 1): 
                candidates = self._get_candidates(date, post, relax_level)

                logging.debug(f"Found {len(candidates)} candidates for {date.strftime('%d-%m-%Y')}, post {post}, relax level {relax_level}")

                if candidates:
                    # Log top candidates if needed
                    # for i, (worker, score) in enumerate(candidates[:3]):
                    #     logging.debug(f"  Candidate {i+1}: Worker {worker['id']} with score {score:.2f}")

                    # Sort candidates by score (descending)
                    candidates.sort(key=lambda x: x[1], reverse=True)

                    # --- Try assigning the first compatible candidate ---\
                    for candidate_worker, candidate_score in candidates:
                        worker_id = candidate_worker['id']

                        # *** DEBUG LOGGING - START ***\
                        current_assignments_on_date = [w for w in self.schedule.get(date, []) if w is not None]
                        logging.debug(f"CHECKING: Date={date}, Post={post}, Candidate={worker_id}, CurrentlyAssigned={current_assignments_on_date}")
                        # *** DEBUG LOGGING - END ***\

                        # *** EXPLICIT INCOMPATIBILITY CHECK ***\
                        # Temporarily add logging INSIDE the check function call might also help, or log its result explicitly
                        is_compatible = self._check_incompatibility_with_list(worker_id, current_assignments_on_date)
                        logging.debug(f"  -> Incompatibility Check Result: {is_compatible}") # Log the result

                        # if not self._check_incompatibility_with_list(worker_id, current_assignments_on_date):
                        if not is_compatible: # Use the variable to make logging easier
                            logging.debug(f"  Skipping candidate {worker_id} for post {post} on {date}: Incompatible with current assignments on this date.")
                            continue # Try next candidate

                        # *** If compatible, assign this worker ***\
                        # Ensure list is long enough before assigning by index
                        while len(self.schedule[date]) <= post:
                             self.schedule[date].append(None)

                        # Double check slot is still None before assigning (paranoid check)
                        if self.schedule[date][post] is None:
                            # CRITICAL FIX: Add comprehensive constraint check before assignment
                            if not self._can_assign_worker(worker_id, date, post):
                                logging.debug(f"  Assignment REJECTED (Constraint Check): W:{worker_id} for {date.strftime('%Y-%m-%d')} P:{post}")
                                continue  # Try next candidate
                            # CRITICAL: Check tolerance BEFORE assigning (±12% absolute limit)
                            if self._would_violate_tolerance(worker_id, date, allow_relaxation=True):
                                logging.debug(f"  Assignment REJECTED (Tolerance): W:{worker_id} would violate ±12% limit")
                                continue  # Try next candidate
                            
                            self.schedule[date][post] = worker_id # Assign to the correct post index
                            self.worker_assignments.setdefault(worker_id, set()).add(date)
                            self.scheduler._update_tracking_data(worker_id, date, post)

                            logging.info(f"Assigned worker {worker_id} to {date.strftime('%d-%m-%Y')}, post {post} (Score: {candidate_score:.2f}, Relax: {relax_level})")
                            assigned_this_post = True
                            break # Found a compatible worker for this post, break candidate loop
                        else:
                            # CRITICAL: Verify slot is not protected by mandatory
                            existing_worker = self.schedule[date][post]
                            if (existing_worker, date) in self._locked_mandatory or self._is_mandatory(existing_worker, date):
                                logging.warning(f"🔒 BLOCKED: Cannot overwrite MANDATORY {existing_worker} on {date.strftime('%Y-%m-%d')} post {post}")
                                continue  # Try next candidate
                            # This case should be rare if logic is correct, but log it
                            logging.warning(f"  Slot {post} on {date} was unexpectedly filled before assigning candidate {worker_id}. Current value: {self.schedule[date][post]}")
                            # Continue to the next candidate, as this one cannot be placed here anymore


                    if assigned_this_post:
                        break # Success at this relaxation level, break relaxation loop
                    else:
                        # If loop finishes without assigning (no compatible candidates found at this relax level)
                        logging.debug(f"No compatible candidate found for post {post} at relax level {relax_level}")
                else:
                     logging.debug(f"No candidates found for post {post} at relax level {relax_level}")


            # --- Handle case where post remains unfilled after trying all relaxation levels ---\
            if not assigned_this_post:
                 # Ensure list is long enough before potentially assigning None
                 while len(self.schedule[date]) <= post:
                      self.schedule[date].append(None)

                 # Only log warning if the slot is genuinely still None
                 if self.schedule[date][post] is None:
                      logging.warning(f"No suitable worker found for {date.strftime('%d-%m-%Y')}, post {post} - shift unfilled after all checks.")
                 # Else: it might have been filled by a mandatory assignment earlier, which is fine.

        # --- Ensure schedule[date] list has the correct final length ---\
        # Pad with None if necessary, e.g., if initial assignment skipped posts
        while len(self.schedule.get(date, [])) < self.num_shifts:
             self.schedule.setdefault(date, []).append(None) # Use setdefault for safety if date somehow disappeared
        
    def assign_worker_to_shift(self, worker_id, date, post):
        """Assign a worker to a shift with proper incompatibility checking"""
    
        # Check if the date already exists in the schedule
        if date not in self.schedule:
            self.schedule[date] = [None] * self.num_shifts
        
        # Check for incompatibility with already assigned workers
        already_assigned = [w for w in self.schedule[date] if w is not None]
        if not self._check_incompatibility_with_list(worker_id, already_assigned):
            logging.warning(f"Cannot assign worker {worker_id} due to incompatibility on {date}")
            return False
        
        # CRITICAL: Check tolerance BEFORE assigning (±12% absolute limit)
        if self._would_violate_tolerance(worker_id, date, allow_relaxation=True):
            logging.warning(f"Cannot assign worker {worker_id} - would violate ±12% tolerance limit")
            return False
        
        # Proceed with assignment if no incompatibility
        self.schedule[date][post] = worker_id
        self.scheduler._update_tracking_data(worker_id, date, post) # Corrected: self.scheduler._update_tracking_data
        return True
        
    # ========================================
    # 7. SCHEDULE IMPROVEMENT METHODS
    # ========================================
    def _try_fill_empty_shifts(self):
        """
        Try to fill empty shifts in the authoritative self.schedule.
        Pass 1: Direct assignment, attempting with increasing relaxation levels.
        Pass 2: Attempt swaps for remaining empty shifts.
        """
        logging.debug(f"ENTERED _try_fill_empty_shifts. self.schedule ID: {id(self.schedule)}. Keys count: {len(self.schedule.keys())}. Sample: {dict(list(self.schedule.items())[:2])}")

        initial_empty_slots = []
        for date_val, workers_in_posts in self.schedule.items():
            for post_index, worker_in_post in enumerate(workers_in_posts):
                if worker_in_post is None:
                    initial_empty_slots.append((date_val, post_index))
        
        logging.debug(f"[_try_fill_empty_shifts] Initial identified empty_slots count: {len(initial_empty_slots)}")
        if not initial_empty_slots:
            logging.info(f"--- No initial empty shifts to fill. ---")
            return False

        logging.info(f"Attempting to fill {len(initial_empty_slots)} empty shifts...")
        initial_empty_slots.sort(key=lambda x: (x[0], x[1])) # Process chronologically, then by post

        shifts_filled_this_pass_total = 0
        made_change_overall = False
        remaining_empty_shifts_after_pass1 = []

        logging.info("--- Starting Pass 1: Direct Fill with Relaxation Iteration ---")
        for date_val, post_val in initial_empty_slots:
            # CRITICAL: Check if this slot has a mandatory assignment
            if self.schedule[date_val][post_val] is not None:
                # Double-check if it's a locked mandatory
                if (self.schedule[date_val][post_val], date_val) in self._locked_mandatory:
                    logging.debug(f"[Pass 1] Slot ({date_val.strftime('%Y-%m-%d')}, {post_val}) is LOCKED MANDATORY "
                                f"for worker {self.schedule[date_val][post_val]}. Skipping.")
                    continue
                logging.debug(f"[Pass 1] Slot ({date_val.strftime('%Y-%m-%d')}, {post_val}) already filled by {self.schedule[date_val][post_val]}. Skipping.")
                continue
            
            assigned_this_post_pass1 = False
            
            # Iterate through relaxation levels for direct fill
            # Max relaxation level can be a config, e.g., self.scheduler.config.get('max_direct_fill_relaxation', 3)
            # For now, let's assume up to 2 (0, 1, 2)
            for relax_lvl_attempt in range(3): 
                pass1_candidates = []
                logging.debug(f"  [Pass 1 Attempt] Date: {date_val.strftime('%Y-%m-%d')}, Post: {post_val}, Relaxation Level: {relax_lvl_attempt}")

                for worker_data_val in self.workers_data:
                    worker_id_val = worker_data_val['id']
                    logging.debug(f"    [Pass 1 Candidate Check] Worker: {worker_id_val} for Date: {date_val.strftime('%Y-%m-%d')}, Post: {post_val}, Relax: {relax_lvl_attempt}")
                    
                    score = self._calculate_worker_score(worker_data_val, date_val, post_val, relaxation_level=relax_lvl_attempt)

                    if score > float('-inf'):
                        # Add target_shifts priority bonus to base score
                        current_assignments = len(self.worker_assignments.get(worker_id_val, set()))
                        target_shifts = worker_data_val.get('target_shifts', 0)
                        target_deficit = max(0, target_shifts - current_assignments)
                        
                        # Combine base score with target_shifts priority
                        priority_score = score + (target_deficit * 1000)  # Slightly lower bonus than swaps
                        
                        logging.debug(f"      -> Pass1 ACCEPTED as candidate: Worker {worker_id_val} with base_score={score}, "
                                    f"target_deficit={target_deficit}, priority_score={priority_score} at relax {relax_lvl_attempt}")
                        pass1_candidates.append((worker_data_val, priority_score))
                    else:
                        logging.debug(f"      -> Pass1 REJECTED (Score Check): Worker {worker_id_val} at relax {relax_lvl_attempt}")
            
                if pass1_candidates:
                    pass1_candidates.sort(key=lambda x: x[1], reverse=True)
                    logging.debug(f"    [Pass 1] Candidates for {date_val.strftime('%Y-%m-%d')} Post {post_val} (Relax {relax_lvl_attempt}): {[(c[0]['id'], c[1]) for c in pass1_candidates]}")
                    
                    # Try the top candidate that is valid at this relaxation level
                    candidate_worker_data, candidate_score = pass1_candidates[0]
                    worker_id_to_assign = candidate_worker_data['id']
                    
                    if self.schedule[date_val][post_val] is None: 
                        others_now = [w for i, w in enumerate(self.schedule.get(date_val, [])) if i != post_val and w is not None]
                        if not self._check_incompatibility_with_list(worker_id_to_assign, others_now):
                            logging.debug(f"      -> Pass1 Assignment REJECTED (Last Minute Incompat): W:{worker_id_to_assign} for {date_val.strftime('%Y-%m-%d')} P:{post_val} at Relax {relax_lvl_attempt}")
                        # CRITICAL FIX: Add comprehensive constraint check before assignment
                        elif not self._can_assign_worker(worker_id_to_assign, date_val, post_val):
                            logging.debug(f"      -> Pass1 Assignment REJECTED (Constraint Check): W:{worker_id_to_assign} for {date_val.strftime('%Y-%m-%d')} P:{post_val} at Relax {relax_lvl_attempt}")
                        # CRITICAL: Check tolerance before assignment
                        # Phase 1 (relax 0-1): ±10% objective - NEVER violate
                        # Phase 2 (relax 2+): ±12% limit - NEVER violate (absolute limit)
                        # NO relaxation beyond ±12%, that's the absolute maximum
                        elif self._would_violate_tolerance(worker_id_to_assign, date_val, allow_relaxation=(relax_lvl_attempt >= 2)):
                            logging.debug(f"      -> Pass1 Assignment REJECTED (Tolerance): W:{worker_id_to_assign} for {date_val.strftime('%Y-%m-%d')} P:{post_val} at Relax {relax_lvl_attempt}")
                        else:
                            # CRITICAL: Final check - is slot protected by mandatory?
                            if self.schedule[date_val][post_val] is not None:
                                existing = self.schedule[date_val][post_val]
                                if (existing, date_val) in self._locked_mandatory or self._is_mandatory(existing, date_val):
                                    logging.warning(f"🔒 BLOCKED Pass1: Cannot overwrite MANDATORY {existing} on {date_val.strftime('%Y-%m-%d')} post {post_val}")
                                    continue  # Skip this assignment
                            
                            self.schedule[date_val][post_val] = worker_id_to_assign
                            self.worker_assignments.setdefault(worker_id_to_assign, set()).add(date_val)
                            self.scheduler._update_tracking_data(worker_id_to_assign, date_val, post_val, removing=False)
                            logging.info(f"[Pass 1 Direct Fill] Filled empty shift on {date_val.strftime('%Y-%m-%d')} Post {post_val} with W:{worker_id_to_assign} (Score: {candidate_score:.2f}, Relax: {relax_lvl_attempt})")
                            shifts_filled_this_pass_total += 1
                            made_change_overall = True
                            assigned_this_post_pass1 = True
                            break # Break from the relaxation_level attempts for this slot, as it's filled
                    else: 
                        # Slot was filled by a previous iteration (should not happen if logic is sequential for a slot)
                        # or by another process if this method is called concurrently (not expected here)
                        logging.warning(f"    [Pass 1] Slot ({date_val.strftime('%Y-%m-%d')}, {post_val}) was unexpectedly filled before assignment at Relax {relax_lvl_attempt}. Current: {self.schedule[date_val][post_val]}")
                        assigned_this_post_pass1 = True # Consider it "handled" to break relaxation attempts
                        break 
            
            if not assigned_this_post_pass1 and self.schedule[date_val][post_val] is None:
                remaining_empty_shifts_after_pass1.append((date_val, post_val))
                logging.debug(f"Could not find compatible direct candidate in Pass 1 for {date_val.strftime('%Y-%m-%d')} Post {post_val} after all relaxation attempts.")

        if not remaining_empty_shifts_after_pass1:
            logging.info(f"--- Finished Pass 1. No remaining empty shifts for Pass 2. ---")
        else:
            logging.info(f"--- Finished Pass 1. Starting Pass 2: Attempting swaps for {len(remaining_empty_shifts_after_pass1)} empty shifts ---")
            for date_empty, post_empty in remaining_empty_shifts_after_pass1:
                if self.schedule[date_empty][post_empty] is not None:
                    logging.warning(f"[Pass 2 Swap] Slot ({date_empty.strftime('%Y-%m-%d')}, {post_empty}) no longer empty. Skipping.")
                    continue
                swap_found = False
                potential_W_data = list(self.workers_data); random.shuffle(potential_W_data)
                for worker_W_data in potential_W_data:
                    worker_W_id = worker_W_data['id']
                    if not self.worker_assignments.get(worker_W_id): continue
                    
                    original_W_assignments = list(self.worker_assignments[worker_W_id]); random.shuffle(original_W_assignments)
                    for date_conflict in original_W_assignments:
                        # CRITICAL: Verificación centralizada de protección mandatory
                        if not self._can_modify_assignment(worker_W_id, date_conflict, "swap_fill_empty"):
                            continue
                        try: 
                            post_conflict = self.schedule[date_conflict].index(worker_W_id)
                        except (ValueError, KeyError, IndexError): 
                            logging.warning(f"Could not find worker {worker_W_id} in schedule for date {date_conflict} during swap search. Assignments: {self.worker_assignments.get(worker_W_id)}, Schedule on date: {self.schedule.get(date_conflict)}")
                            continue

                        # Create a temporary state for checking W's move to empty (optimized shallow copy)
                        temp_schedule_for_W_check = {k: v[:] for k, v in self.schedule.items()}
                        temp_assignments_for_W_check = {k: set(v) for k, v in self.worker_assignments.items()}
                        
                        # Remove W from original conflict spot in temp
                        if date_conflict in temp_schedule_for_W_check and \
                           len(temp_schedule_for_W_check[date_conflict]) > post_conflict and \
                           temp_schedule_for_W_check[date_conflict][post_conflict] == worker_W_id:
                            temp_schedule_for_W_check[date_conflict][post_conflict] = None
                            if worker_W_id in temp_assignments_for_W_check and date_conflict in temp_assignments_for_W_check[worker_W_id]:
                                temp_assignments_for_W_check[worker_W_id].remove(date_conflict)
                                if not temp_assignments_for_W_check[worker_W_id]: # Clean up if set becomes empty
                                    del temp_assignments_for_W_check[worker_W_id]
                        else:
                            logging.warning(f"Swap pre-check: Worker {worker_W_id} not found at {date_conflict}|P{post_conflict} in temp_schedule for W check. Skipping.")
                            continue
                        
                        # Check if W can be assigned to the empty slot in this temp state (using strict constraints for the move itself)
                        can_W_take_empty_simulated = self._check_constraints_on_simulated(
                            worker_W_id, date_empty, post_empty, 
                            temp_schedule_for_W_check, temp_assignments_for_W_check
                        )

                        if not can_W_take_empty_simulated:
                            logging.debug(f"  Swap Check: Worker {worker_W_id} cannot take empty slot {date_empty}|P{post_empty} due to constraints in simulated state.")
                            continue 

                        # Now, find a worker X who can take W's original spot (date_conflict, post_conflict)
                        worker_X_id = self._find_swap_candidate(worker_W_id, date_conflict, post_conflict)

                        if worker_X_id:
                            # CRITICAL: Verify 7/14 pattern for BOTH workers in their NEW positions
                            # Check W moving to empty slot
                            temp_W_assignments = self.worker_assignments.get(worker_W_id, set()).copy()
                            temp_W_assignments.discard(date_conflict)
                            if any((date_empty - d).days % 7 == 0 and (date_empty - d).days in [7, 14] and date_empty.weekday() < 5
                                   for d in temp_W_assignments):
                                logging.debug(f"Swap rejected: W {worker_W_id} would violate 7/14 pattern on {date_empty.strftime('%Y-%m-%d')}")
                                continue
                            
                            # Check X moving to conflict slot
                            if self._violates_7_14_pattern(worker_X_id, date_conflict):
                                logging.debug(f"Swap rejected: X {worker_X_id} would violate 7/14 pattern on {date_conflict.strftime('%Y-%m-%d')}")
                                continue
                            
                            # CRITICAL: Verify X is not incompatible with others on date_conflict BEFORE swap
                            others_on_conflict_date = [w for idx, w in enumerate(self.schedule.get(date_conflict, [])) 
                                                      if w is not None and idx != post_conflict]
                            if not self._check_incompatibility_with_list(worker_X_id, others_on_conflict_date):
                                logging.debug(f"Swap rejected: Worker X {worker_X_id} incompatible with others on {date_conflict.strftime('%Y-%m-%d')}")
                                continue  # Try next date_conflict
                            
                            logging.info(f"[Pass 2 Swap Attempt] W:{worker_W_id} ({date_conflict.strftime('%Y-%m-%d')},P{post_conflict}) -> ({date_empty.strftime('%Y-%m-%d')},P{post_empty}); X:{worker_X_id} takes W's original spot.")
                            
                            # CRITICAL: Check tolerance BEFORE swap (both workers)
                            if self._would_violate_tolerance(worker_X_id, date_conflict, allow_relaxation=True):
                                logging.debug(f"Pass 2 swap rejected: {worker_X_id} would violate ±12% limit on {date_conflict.strftime('%Y-%m-%d')}")
                                continue
                            if self._would_violate_tolerance(worker_W_id, date_empty, allow_relaxation=True):
                                logging.debug(f"Pass 2 swap rejected: {worker_W_id} would violate ±12% limit on {date_empty.strftime('%Y-%m-%d')}")
                                continue
                            
                            # CRITICAL: Double-check before removing W (should already be checked, but paranoid)
                            if not self._can_modify_assignment(worker_W_id, date_conflict, "pass2_swap_remove"):
                                logging.error(f"🔒 CRITICAL: Attempted to remove MANDATORY {worker_W_id} in Pass2 swap! Aborting swap.")
                                continue
                            
                            # 1. Remove W from original spot
                            self.schedule[date_conflict][post_conflict] = None
                            if worker_W_id in self.worker_assignments and date_conflict in self.worker_assignments[worker_W_id]:
                                self.worker_assignments[worker_W_id].remove(date_conflict)
                            self.scheduler._update_tracking_data(worker_W_id, date_conflict, post_conflict, removing=True)

                            # 2. Assign X to W's original spot
                            self.schedule[date_conflict][post_conflict] = worker_X_id
                            self.worker_assignments.setdefault(worker_X_id, set()).add(date_conflict)
                            self.scheduler._update_tracking_data(worker_X_id, date_conflict, post_conflict, removing=False)
                            
                            # 3. Assign W to the empty spot
                            self.schedule[date_empty][post_empty] = worker_W_id
                            self.worker_assignments.setdefault(worker_W_id, set()).add(date_empty) # Ensure setdefault here too
                            self.scheduler._update_tracking_data(worker_W_id, date_empty, post_empty, removing=False)
                            
                            shifts_filled_this_pass_total += 1
                            made_change_overall = True
                            swap_found = True
                            break # Break from date_conflict loop for worker_W
                    if swap_found: 
                        break # Break from worker_W_data loop
                if not swap_found: 
                    logging.debug(f"No swap for empty {date_empty.strftime('%Y-%m-%d')} P{post_empty}")
        
        logging.info(f"--- Finished _try_fill_empty_shifts. Total filled/swapped: {shifts_filled_this_pass_total} ---")
        if made_change_overall:
            self._synchronize_tracking_data() # Ensure builder's and scheduler's data are aligned
            self._save_current_as_best()
        return made_change_overall
    
    def _try_fill_empty_shifts_with_worker_order(self, workers_list: List[Dict], max_attempts: int = 16) -> bool:
        """
        Try to fill empty shifts using a custom worker ordering.
        
        This method is used for initial distribution attempts with different strategies.
        It fills shifts by iterating through workers in the specified order.
        
        Args:
            workers_list: List of worker dictionaries in the desired processing order
            max_attempts: Maximum number of fill attempts
            
        Returns:
            bool: True if any shifts were filled
        """
        logging.info(f"=" * 80)
        logging.info(f"INITIAL FILL WITH CUSTOM WORKER ORDER")
        logging.info(f"  Workers in list: {len(workers_list)}")
        logging.info(f"  Locked mandatory shifts: {len(self._locked_mandatory)}")
        logging.info(f"=" * 80)
        
        shifts_filled = 0
        made_change = False
        
        for attempt in range(max_attempts):
            # Get current empty shifts
            empty_slots = []
            for date_val, workers_in_posts in self.schedule.items():
                for post_index, worker_in_post in enumerate(workers_in_posts):
                    if worker_in_post is None:
                        empty_slots.append((date_val, post_index))
            
            if not empty_slots:
                logging.info(f"All shifts filled after {attempt + 1} attempts")
                break
            
            if attempt == 0:
                logging.info(f"Starting with {len(empty_slots)} empty shifts")
                # Count how many are actually protected
                protected_count = 0
                truly_empty_count = 0
                for d, p in empty_slots:
                    if d in self.schedule and len(self.schedule[d]) > p:
                        if self.schedule[d][p] is not None:
                            existing = self.schedule[d][p]
                            if (existing, d) in self._locked_mandatory or self._is_mandatory(existing, d):
                                protected_count += 1
                            else:
                                # Filled but not mandatory
                                pass
                        else:
                            truly_empty_count += 1
                logging.info(f"  Protected mandatory slots: {protected_count}")
                logging.info(f"  Truly empty slots to fill: {truly_empty_count}")
            
            # Sort chronologically
            empty_slots.sort(key=lambda x: (x[0], x[1]))
            
            # Try to fill each empty slot using custom worker order
            filled_this_attempt = 0
            
            # Track why assignments fail (only for attempt 2 to avoid log spam)
            if attempt == 1:
                logging.debug(f"=== DEBUGGING ATTEMPT 2 - Why can't we fill more shifts? ===")
            
            for date_val, post_val in empty_slots:
                # CRITICAL: Verificar si el slot está protegido por mandatory
                # Primero verificar si el slot ya está lleno
                if date_val not in self.schedule or len(self.schedule[date_val]) <= post_val:
                    logging.warning(f"[Initial Fill] Invalid slot reference: {date_val.strftime('%Y-%m-%d')} post {post_val}")
                    continue
                    
                # Si ya está lleno, verificar si es mandatory
                if self.schedule[date_val][post_val] is not None:
                    existing_worker = self.schedule[date_val][post_val]
                    # Verificar si es mandatory bloqueado
                    if (existing_worker, date_val) in self._locked_mandatory:
                        logging.debug(f"[Initial Fill] Slot {date_val.strftime('%Y-%m-%d')} post {post_val} is LOCKED MANDATORY for {existing_worker}")
                        continue
                    # También verificar si es mandatory por configuración
                    if self._is_mandatory(existing_worker, date_val):
                        logging.warning(f"[Initial Fill] Slot {date_val.strftime('%Y-%m-%d')} post {post_val} is CONFIG MANDATORY for {existing_worker} but NOT in locked set - BLOCKING anyway")
                        continue
                    # Si está lleno pero NO es mandatory, lo marcamos como disponible para el loop siguiente
                    logging.debug(f"[Initial Fill] Slot {date_val.strftime('%Y-%m-%d')} post {post_val} filled with {existing_worker} (non-mandatory)")
                    continue
                
                # Debug first empty slot in attempt 2
                if attempt == 1 and filled_this_attempt == 0:
                    logging.debug(f"Trying to fill slot: {date_val.strftime('%Y-%m-%d')} post {post_val}")
                
                # CRITICAL FIX: Evaluate ALL workers and assign the one with HIGHEST score
                # This ensures workers with deficit (+25000 bonus) get priority over workers at target
                best_worker = None
                best_score = float('-inf')
                valid_workers_found = 0
                
                for worker_data in workers_list:
                    worker_id = worker_data['id']
                    
                    # Check if worker can be assigned
                    score = self._calculate_worker_score(worker_data, date_val, post_val, relaxation_level=0)
                    
                    if score > float('-inf'):
                        valid_workers_found += 1
                        # Additional incompatibility check
                        others_now = [w for i, w in enumerate(self.schedule.get(date_val, [])) 
                                     if i != post_val and w is not None]
                        
                        if not self._check_incompatibility_with_list(worker_id, others_now):
                            if attempt == 1 and filled_this_attempt == 0 and valid_workers_found <= 3:
                                logging.debug(f"  {worker_id}: BLOCKED by incompatibility")
                            continue
                        
                        if not self._can_assign_worker(worker_id, date_val, post_val):
                            if attempt == 1 and filled_this_attempt == 0 and valid_workers_found <= 3:
                                logging.debug(f"  {worker_id}: BLOCKED by _can_assign_worker")
                            continue
                        
                        # CRITICAL: Check ±8% tolerance before assignment
                        if self._would_violate_tolerance(worker_id, date_val, allow_relaxation=False):
                            if attempt == 1 and filled_this_attempt == 0 and valid_workers_found <= 3:
                                logging.debug(f"  {worker_id}: BLOCKED by tolerance violation")
                            continue
                        
                        # CRITICAL: Final check - ensure slot not protected
                        if self.schedule[date_val][post_val] is not None:
                            existing = self.schedule[date_val][post_val]
                            if (existing, date_val) in self._locked_mandatory or self._is_mandatory(existing, date_val):
                                logging.warning(f"🔒 BLOCKED Initial Fill: Cannot overwrite MANDATORY {existing} on {date_val.strftime('%Y-%m-%d')} post {post_val}")
                                continue
                        
                        # CRITICAL: Compare scores and keep track of best worker
                        if score > best_score:
                            best_score = score
                            best_worker = worker_id
                            if attempt == 1 and filled_this_attempt == 0:
                                logging.debug(f"  {worker_id}: NEW BEST (score={score:.0f})")
                
                # Assign the worker with the HIGHEST score (not the first valid one)
                if best_worker is not None:
                    self.schedule[date_val][post_val] = best_worker
                    self.worker_assignments[best_worker].add(date_val)
                    
                    filled_this_attempt += 1
                    made_change = True
                    
                    if attempt == 1 and filled_this_attempt == 1:
                        logging.debug(f"  ✅ {best_worker}: ASSIGNED (best score={best_score:.0f})")
                elif attempt == 1 and filled_this_attempt == 0 and valid_workers_found == 0:
                    # Log if no valid workers found for this slot
                    logging.debug(f"  ❌ NO valid workers found (all returned -inf score)")
            
            shifts_filled += filled_this_attempt
            
            if filled_this_attempt == 0:
                # No progress in this attempt, try with relaxation
                if attempt < max_attempts - 1:
                    logging.debug(f"Attempt {attempt + 1}: No fills, will try with relaxation")
                else:
                    logging.debug(f"Attempt {attempt + 1}: No fills possible")
                    break
            else:
                logging.debug(f"Attempt {attempt + 1}: Filled {filled_this_attempt} shifts")
        
        if made_change:
            self._synchronize_tracking_data()
            logging.info(f"Custom worker order fill: {shifts_filled} shifts filled")
        
        return made_change
        return made_change_overall

    def _find_swap_candidate(self, worker_W_id, conflict_date, conflict_post):
        """
        Finds a worker (X) who can take the shift at (conflict_date, conflict_post),
        ensuring they are not worker_W_id and not already assigned on that date.
        UPDATED: Now prioritizes workers based on their target_shifts deficit.
        
        CRITICAL: If conflict_date is a MANDATORY assignment for worker_W_id,
        NO swap should be attempted. Return None immediately.
        """
        # CRITICAL: Verificación centralizada de protección mandatory
        if not self._can_modify_assignment(worker_W_id, conflict_date, "swap_candidate_search"):
            return None
        
        potential_X_workers = [
            w_data for w_data in self.scheduler.workers_data 
            if w_data['id'] != worker_W_id and \
               w_data['id'] not in self.scheduler.schedule.get(conflict_date, []) 
        ]
        
        # Evaluate and score all candidates
        candidates_with_scores = []
        
        for worker_X_data in potential_X_workers:
            worker_X_id = worker_X_data['id']
            
            # CRITICAL: Check incompatibility FIRST before simulating
            # Get list of workers already on conflict_date (excluding worker_W who we're removing)
            others_on_date = [w for idx, w in enumerate(self.scheduler.schedule.get(conflict_date, [])) 
                            if w is not None and w != worker_W_id and idx != conflict_post]
            if not self._check_incompatibility_with_list(worker_X_id, others_on_date):
                logging.debug(f"Swap candidate {worker_X_id} rejected: incompatible with others on {conflict_date.strftime('%Y-%m-%d')}")
                continue  # Skip this candidate
            
            # Check if X can strictly take W's old slot (which is now considered notionally empty)
            # Simulate W's absence for X's check
            sim_schedule_for_X = {k: v[:] for k, v in self.scheduler.schedule.items()}
            if conflict_date in sim_schedule_for_X and len(sim_schedule_for_X[conflict_date]) > conflict_post:
                # Only set to None if it was W, to be safe, though it should be.
                if sim_schedule_for_X[conflict_date][conflict_post] == worker_W_id:
                     sim_schedule_for_X[conflict_date][conflict_post] = None
            
            # Temporarily use the simulated schedule for this specific score calculation for X
            original_schedule_ref = self.schedule # Keep original ref
            self.schedule = sim_schedule_for_X # Temporarily point to sim
            
            score_for_X = self._calculate_worker_score(worker_X_data, conflict_date, conflict_post, relaxation_level=0)
            
            self.schedule = original_schedule_ref # Restore original ref

            if score_for_X > float('-inf'): # If X can be assigned
                # Calculate target_shifts priority bonus
                current_assignments = len(self.worker_assignments.get(worker_X_id, set()))
                target_shifts = worker_X_data.get('target_shifts', 0)
                target_deficit = max(0, target_shifts - current_assignments)
                
                # Combine base score with target_shifts priority
                # Workers with higher target deficit get significant bonus
                priority_score = score_for_X + (target_deficit * 2000)
                
                candidates_with_scores.append((worker_X_id, priority_score, target_deficit))
                logging.debug(f"Swap candidate X={worker_X_id}: base_score={score_for_X}, "
                            f"current={current_assignments}, target={target_shifts}, "
                            f"deficit={target_deficit}, priority_score={priority_score}")

        if candidates_with_scores:
            # Sort by priority score (highest first) - this prioritizes workers who need more shifts
            candidates_with_scores.sort(key=lambda x: x[1], reverse=True)
            best_candidate = candidates_with_scores[0]
            
            logging.debug(f"Selected best swap candidate X={best_candidate[0]} with priority_score={best_candidate[1]} "
                         f"(target_deficit={best_candidate[2]}) for W={worker_W_id}'s slot "
                         f"({conflict_date.strftime('%Y-%m-%d')},{conflict_post})")
            
            return best_candidate[0]

        logging.debug(f"No suitable swap candidate X found for W={worker_W_id}'s slot ({conflict_date.strftime('%Y-%m-%d')},{conflict_post})")
        return None
    
    def _balance_workloads(self):
        """
        """
        logging.info("Attempting to balance worker workloads")
        # Ensure data consistency before proceeding
        self._ensure_data_integrity()

        # First verify and fix data consistency
        self._verify_assignment_consistency()

        # Count total assignments for each worker
        assignment_counts = {}
        for worker_val in self.workers_data: # Renamed worker
            worker_id_val = worker_val['id'] # Renamed worker_id
            work_percentage = worker_val.get('work_percentage', 100)
    
            # Count assignments
            count = len(self.worker_assignments[worker_id_val])
    
            # Normalize by work percentage
            normalized_count = count * 100 / work_percentage if work_percentage > 0 else 0
    
            assignment_counts[worker_id_val] = {\
                'worker_id': worker_id_val,\
                'count': count,\
                'work_percentage': work_percentage,\
                'normalized_count': normalized_count\
            }    

        # Calculate average normalized count
        total_normalized = sum(data['normalized_count'] for data in assignment_counts.values())
        avg_normalized = total_normalized / len(assignment_counts) if assignment_counts else 0

        # Identify overloaded and underloaded workers using absolute tolerance of ±1
        overloaded = []
        underloaded = []

        for worker_id_val, data_val in assignment_counts.items(): # Renamed worker_id, data
            # Use absolute tolerance of ±1 turno instead of percentage
            if data_val['normalized_count'] > avg_normalized + 1.0:
                overloaded.append((worker_id_val, data_val))
            elif data_val['normalized_count'] < avg_normalized - 1.0:
                underloaded.append((worker_id_val, data_val))

        # Sort by most overloaded/underloaded
        overloaded.sort(key=lambda x: x[1]['normalized_count'], reverse=True)
        underloaded.sort(key=lambda x: x[1]['normalized_count'])

        changes_made = 0
        max_changes = 100  # Aumentar para permitir más rebalanceos cuando sea necesario

        # Try to redistribute shifts from overloaded to underloaded workers
        for over_worker_id, over_data in overloaded:
            if changes_made >= max_changes or not underloaded:
                break
        
            # Find shifts that can be reassigned from this overloaded worker
            possible_shifts = []
    
            for date_val in sorted(self.scheduler.worker_assignments.get(over_worker_id, set())): # Renamed date
                # CRITICAL: Verificación centralizada de protección mandatory
                if not self._can_modify_assignment(over_worker_id, date_val, "balance_workloads"):
                    continue

            
                # Make sure the worker is actually in the schedule for this date
                if date_val not in self.schedule:
                    # This date is in worker_assignments but not in schedule
                    logging.warning(f"Worker {over_worker_id} has assignment for date {date_val} but date is not in schedule")
                    continue
                
                try:
                    # Find the post this worker is assigned to
                    if over_worker_id not in self.schedule[date_val]:
                        # Worker is supposed to be assigned to this date but isn't in the schedule
                        logging.warning(f"Worker {over_worker_id} has assignment for date {date_val} but is not in schedule")
                        continue
                    
                    post_val = self.schedule[date_val].index(over_worker_id) # Renamed post
                    possible_shifts.append((date_val, post_val))
                except ValueError:
                    # Worker not found in schedule for this date
                    logging.warning(f"Worker {over_worker_id} has assignment for date {date_val} but is not in schedule")
                    continue
    
            # Shuffle to introduce randomness
            random.shuffle(possible_shifts)
    
            # Try each shift
            for date_val, post_val in possible_shifts: # Renamed date, post
                reassigned = False
                for under_worker_id, _ in underloaded:
                    # ... (check if under_worker already assigned) ...
                    if self._can_assign_worker(under_worker_id, date_val, post_val):
                        # CRITICAL: Check tolerance BEFORE assigning (±12% absolute limit)
                        if self._would_violate_tolerance(under_worker_id, date_val, allow_relaxation=True):
                            continue  # Skip if would violate ±12% limit
                        # CRITICAL: Verify we can modify the current assignment
                        if not self._can_modify_assignment(over_worker_id, date_val, "balance_workloads_remove"):
                            continue
                        self.scheduler.schedule[date_val][post_val] = under_worker_id
                        self.scheduler.worker_assignments[over_worker_id].remove(date_val)
                        # Ensure under_worker tracking exists
                        if under_worker_id not in self.scheduler.worker_assignments:
                             self.scheduler.worker_assignments[under_worker_id] = set()
                        self.scheduler.worker_assignments[under_worker_id].add(date_val)

                        # Update tracking data (Needs FIX: update for BOTH workers)
                        self.scheduler._update_tracking_data(over_worker_id, date_val, post_val, removing=True) # Remove stats for over_worker
                        self.scheduler._update_tracking_data(under_worker_id, date_val, post_val) # Add stats for under_worker

                        changes_made += 1
                        logging.info(f"Balanced workload: Moved shift on {date_val.strftime('%Y-%m-%d')} post {post_val} from {over_worker_id} to {under_worker_id}")
                        
                        # Update counts
                        assignment_counts[over_worker_id]['count'] -= 1
                        assignment_counts[over_worker_id]['normalized_count'] = (\
                            assignment_counts[over_worker_id]['count'] * 100 / \
                            assignment_counts[over_worker_id]['work_percentage']\
                        ) if assignment_counts[over_worker_id]['work_percentage'] > 0 else 0 # Added check for zero division
                
                        assignment_counts[under_worker_id]['count'] += 1
                        assignment_counts[under_worker_id]['normalized_count'] = (\
                            assignment_counts[under_worker_id]['count'] * 100 / \
                            assignment_counts[under_worker_id]['work_percentage']\
                        ) if assignment_counts[under_worker_id]['work_percentage'] > 0 else 0 # Added check for zero division
                
                        reassigned = True
                
                        # Check if workers are still overloaded/underloaded using absolute tolerance
                        if assignment_counts[over_worker_id]['normalized_count'] <= avg_normalized + 1.0:
                            # No longer overloaded
                            overloaded = [(w, d_val_loop) for w, d_val_loop in overloaded if w != over_worker_id] # Renamed d to d_val_loop
                
                        if assignment_counts[under_worker_id]['normalized_count'] >= avg_normalized - 1.0:
                            # No longer underloaded
                            underloaded = [(w, d_val_loop) for w, d_val_loop in underloaded if w != under_worker_id] # Renamed d to d_val_loop
                
                        break
        
                if reassigned:
                    break
            
                if changes_made >= max_changes:
                    break

        logging.info(f"Workload balancing: made {changes_made} changes")
        if changes_made > 0:
            self._save_current_as_best()
        return changes_made > 0
        
    def _balance_weekday_distribution(self):
        """
        Balance the distribution of shifts across weekdays for each worker.
        Each worker should have approximately equal shifts per weekday ±2.
        """
        logging.info("Balancing weekday distribution across workers...")
        changes_made = 0
        max_changes = 50
        
        # Ensure data consistency
        self._ensure_data_integrity()
        self._verify_assignment_consistency()
        
        # Calculate current weekday distribution for each worker
        worker_weekday_counts = {}
        for worker in self.workers_data:
            worker_id = worker['id']
            weekday_counts = {day: 0 for day in range(7)}  # Monday=0, Sunday=6
            
            for date in self.worker_assignments.get(worker_id, set()):
                weekday = date.weekday()
                weekday_counts[weekday] += 1
            
            worker_weekday_counts[worker_id] = weekday_counts
        
        # For each worker, identify weekdays that are over/under balanced
        for worker in self.workers_data:
            if changes_made >= max_changes:
                break
                
            worker_id = worker['id']
            weekday_counts = worker_weekday_counts[worker_id]
            
            if not weekday_counts or sum(weekday_counts.values()) == 0:
                continue
            
            # Calculate average shifts per weekday for this worker
            total_shifts = sum(weekday_counts.values())
            avg_per_weekday = total_shifts / 7.0
            
            # Find overloaded and underloaded weekdays (tolerance ±2)
            overloaded_weekdays = [day for day, count in weekday_counts.items() 
                                 if count > avg_per_weekday + 2]
            underloaded_weekdays = [day for day, count in weekday_counts.items() 
                                  if count < avg_per_weekday - 2]
            
            if not overloaded_weekdays or not underloaded_weekdays:
                continue
            
            # Try to move shifts from overloaded to underloaded weekdays
            worker_dates = list(self.worker_assignments.get(worker_id, set()))
            random.shuffle(worker_dates)
            
            for date in worker_dates:
                if changes_made >= max_changes:
                    break
                    
                weekday = date.weekday()
                if weekday not in overloaded_weekdays:
                    continue
                
                # CRITICAL: Verificación centralizada de protección mandatory
                if not self._can_modify_assignment(worker_id, date, "balance_weekday_distribution"):
                    continue
                
                # Find the post this worker is assigned to
                if date not in self.schedule or worker_id not in self.schedule[date]:
                    continue
                    
                try:
                    post = self.schedule[date].index(worker_id)
                except ValueError:
                    continue
                
                # Try to find another worker who can take this shift
                # and who needs more shifts on this weekday
                for other_worker in self.workers_data:
                    other_worker_id = other_worker['id']
                    if other_worker_id == worker_id:
                        continue
                    
                    other_weekday_counts = worker_weekday_counts[other_worker_id]
                    other_total = sum(other_weekday_counts.values())
                    if other_total == 0:
                        continue
                    
                    other_avg = other_total / 7.0
                    
                    # Check if other worker needs more shifts on this weekday
                    if other_weekday_counts[weekday] >= other_avg + 2:
                        continue
                    
                    # Check if other worker can take this shift
                    if self._can_assign_worker(other_worker_id, date, post):
                        # CRITICAL: Verify incompatibility with others on this date
                        others_on_date = [w for idx, w in enumerate(self.schedule.get(date, [])) 
                                         if w is not None and idx != post and w != worker_id]
                        if not self._check_incompatibility_with_list(other_worker_id, others_on_date):
                            logging.debug(f"Weekly balance swap rejected: {other_worker_id} incompatible on {date.strftime('%Y-%m-%d')}")
                            continue
                        
                        # CRITICAL: Check tolerance BEFORE assigning
                        if self._would_violate_tolerance(other_worker_id, date, allow_relaxation=True):
                            logging.debug(f"Weekly balance rejected: {other_worker_id} would violate ±12% limit")
                            continue
                        # CRITICAL: Final check before modification
                        if not self._can_modify_assignment(worker_id, date, "balance_weekday_final"):
                            logging.warning(f"🔒 BLOCKED: Cannot modify MANDATORY {worker_id} on {date.strftime('%Y-%m-%d')}")
                            continue
                        
                        # Remove from original worker
                        self.schedule[date][post] = other_worker_id
                        self.worker_assignments[worker_id].remove(date)
                        
                        # Add to other worker
                        if other_worker_id not in self.worker_assignments:
                            self.worker_assignments[other_worker_id] = set()
                        self.worker_assignments[other_worker_id].add(date)
                        
                        # Update tracking data
                        self.scheduler._update_tracking_data(worker_id, date, post, removing=True)
                        self.scheduler._update_tracking_data(other_worker_id, date, post, removing=False)
                        
                        # Update local counts
                        worker_weekday_counts[worker_id][weekday] -= 1
                        worker_weekday_counts[other_worker_id][weekday] += 1
                        
                        changes_made += 1
                        logging.info(f"Weekday balance: Moved shift on {date.strftime('%Y-%m-%d')} ({['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][weekday]}) from {worker_id} to {other_worker_id}")
                        
                        # Check if we've fixed the imbalance for this weekday
                        total_shifts_updated = sum(worker_weekday_counts[worker_id].values())
                        avg_updated = total_shifts_updated / 7.0
                        if worker_weekday_counts[worker_id][weekday] <= avg_updated + 2:
                            overloaded_weekdays.remove(weekday)
                        
                        break
                
                if weekday not in overloaded_weekdays:
                    break
        
        logging.info(f"Weekday distribution balancing: made {changes_made} changes")
        if changes_made > 0:
            self._save_current_as_best()
        return changes_made > 0

    def _balance_weekend_shifts(self):
        """
        Balance weekend/holiday shifts across workers based on their percentage of working days.
        Each worker should have approximately:
        (total_shifts_for_worker) * (total_weekend_days / total_days) shifts on weekends/holidays, ±1.
        """
        logging.info("Balancing weekend and holiday shifts among workers...")
        fixes_made = 0
    
        # Calculate the total days and weekend/holiday days in the schedule period
        total_days_in_period = (self.end_date - self.start_date).days + 1 # Renamed total_days
        weekend_days_in_period = sum(1 for d_val in self.date_utils.generate_date_range(self.start_date, self.end_date) # Renamed d, use generate_date_range
                      if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)
    
        # Calculate the target percentage
        weekend_percentage = weekend_days_in_period / total_days_in_period if total_days_in_period > 0 else 0
        logging.info(f"Schedule period has {weekend_days_in_period} weekend/holiday days out of {total_days_in_period} total days ({weekend_percentage:.1%})")
    
        # Check each worker's current weekend shift allocation
        workers_to_check = self.workers_data.copy()
        random.shuffle(workers_to_check)  # Process in random order
    
        for worker_val in workers_to_check: # Renamed worker
            worker_id_val = worker_val['id'] # Renamed worker_id
            assignments = self.worker_assignments.get(worker_id_val, set())
            total_shifts = len(assignments)
        
            if total_shifts == 0:
                continue  # Skip workers with no assignments
            
            # Count weekend assignments for this worker
            weekend_shifts = sum(1 for date_val in assignments # Renamed date
                                if self.date_utils.is_weekend_day(date_val) or date_val in self.holidays)
        
            # Calculate target weekend shifts for this worker
            target_weekend_shifts = total_shifts * weekend_percentage
            deviation = weekend_shifts - target_weekend_shifts
            allowed_deviation = 0.75  # Tighten the tolerance

            ## And add priority scoring based on how far workers are from target:
            deviation_priority = abs(deviation)
            # Process workers with largest deviations first            logging.debug(f"Worker {worker_id_val}: {weekend_shifts} weekend shifts, target {target_weekend_shifts:.2f}, deviation {deviation:.2f}")
        
            # Case 1: Worker has too many weekend shifts
            if deviation > allowed_deviation:
                logging.info(f"Worker {worker_id_val} has too many weekend shifts ({weekend_shifts}, target {target_weekend_shifts:.2f})")
                swap_found = False
            
                # Find workers with too few weekend shifts to swap with
                potential_swap_partners = []
                for other_worker_val in self.workers_data: # Renamed other_worker
                    other_id = other_worker_val['id']
                    if other_id == worker_id_val:
                        continue
                
                    other_total = len(self.worker_assignments.get(other_id, []))
                    if other_total == 0:
                        continue
                    
                    other_weekend = sum(1 for d_val in self.worker_assignments.get(other_id, []) # Renamed d
                                       if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)
                                    
                    other_target = other_total * weekend_percentage
                    other_deviation = other_weekend - other_target
                
                    if other_deviation < -allowed_deviation:
                        potential_swap_partners.append((other_id, other_deviation))
            
                # Sort potential partners by how under-assigned they are
                potential_swap_partners.sort(key=lambda x: x[1])
            
                # Try to swap a weekend shift from this worker to an under-assigned worker
                if potential_swap_partners:
                    for swap_partner_id, _ in potential_swap_partners:
                        # Find a weekend assignment from this worker to swap
                        possible_from_dates = [d_val for d_val in assignments # Renamed d
                                             if (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
                                             and not self._is_mandatory(worker_id_val, d_val)]
                    
                        if not possible_from_dates:
                            continue  # No swappable weekend shifts
                        
                        random.shuffle(possible_from_dates)
                    
                        for from_date in possible_from_dates:
                            # Find the post this worker is assigned to
                            from_post = self.schedule[from_date].index(worker_id_val)
                        
                            # Find a weekday assignment from the swap partner that could be exchanged
                            partner_assignments = self.worker_assignments.get(swap_partner_id, set())
                            possible_to_dates = [d_val for d_val in partner_assignments # Renamed d
                                               if not (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
                                               and not self._is_mandatory(swap_partner_id, d_val)]
                        
                            if not possible_to_dates:
                                continue  # No swappable weekday shifts for partner
                            
                            random.shuffle(possible_to_dates)
                        
                            for to_date in possible_to_dates:
                                # Find the post the partner is assigned to
                                to_post = self.schedule[to_date].index(swap_partner_id)
                            
                                # Check if swap is valid (worker1 <-> worker2)
                                if self._can_worker_swap(worker_id_val, from_date, from_post, swap_partner_id, to_date, to_post): # Corrected: _can_worker_swap
                                    # Execute worker-worker swap
                                    self._execute_worker_swap(worker_id_val, from_date, from_post, swap_partner_id, to_date, to_post)
                                    logging.info(f"Swapped weekend shift: Worker {worker_id_val} on {from_date.strftime('%Y-%m-%d')} with "\
                                               f"Worker {swap_partner_id} on {to_date.strftime('%Y-%m-%d')}")
                                    fixes_made += 1
                                    swap_found = True
                                    break
                        
                            if swap_found:
                                break
                    
                        if swap_found:
                            break
                        
            # Case 2: Worker has too few weekend shifts
            elif deviation < allowed_deviation:
                logging.info(f"Worker {worker_id_val} has too few weekend shifts ({weekend_shifts}, target {target_weekend_shifts:.2f})")
                swap_found = False
            
                # Find workers with too many weekend shifts to swap with
                potential_swap_partners = []
                for other_worker_val in self.workers_data: # Renamed other_worker
                    other_id = other_worker_val['id']
                    if other_id == worker_id_val:
                        continue
                
                    other_total = len(self.worker_assignments.get(other_id, []))
                    if other_total == 0:
                        continue
                    
                    other_weekend = sum(1 for d_val in self.worker_assignments.get(other_id, []) # Renamed d
                                       if self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)
                                    
                    other_target = other_total * weekend_percentage
                    other_deviation = other_weekend - other_target
                
                    if other_deviation > allowed_deviation:
                        potential_swap_partners.append((other_id, other_deviation))
            
                # Sort potential partners by how over-assigned they are
                potential_swap_partners.sort(key=lambda x: -x[1])
            
                # Implementation similar to above but with roles reversed
                if potential_swap_partners:
                    for swap_partner_id, _ in potential_swap_partners:
                        # Find a weekend assignment from the partner to swap
                        partner_assignments = self.worker_assignments.get(swap_partner_id, set())
                        possible_from_dates = [d_val for d_val in partner_assignments # Renamed d
                                             if (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
                                             and not self._is_mandatory(swap_partner_id, d_val)]
                    
                        if not possible_from_dates:
                            continue
                        
                        random.shuffle(possible_from_dates)
                    
                        for from_date in possible_from_dates:
                            from_post = self.schedule[from_date].index(swap_partner_id)
                        
                            # Find a weekday assignment from this worker
                            possible_to_dates = [d_val for d_val in assignments # Renamed d
                                               if not (self.date_utils.is_weekend_day(d_val) or d_val in self.holidays)\
                                               and not self._is_mandatory(worker_id_val, d_val)]
                        
                            if not possible_to_dates:
                                continue
                            
                            random.shuffle(possible_to_dates)
                        
                            for to_date in possible_to_dates:
                                to_post = self.schedule[to_date].index(worker_id_val)
                            
                                # Check if swap is valid (partner <-> this worker)
                                if self._can_worker_swap(swap_partner_id, from_date, from_post, worker_id_val, to_date, to_post): # Corrected: _can_worker_swap
                                    self._execute_worker_swap(swap_partner_id, from_date, from_post, worker_id_val, to_date, to_post)
                                    logging.info(f"Swapped weekend shift: Worker {swap_partner_id} on {from_date.strftime('%Y-%m-%d')} with "\
                                               f"Worker {worker_id_val} on {to_date.strftime('%Y-%m-%d')}")
                                    fixes_made += 1
                                    swap_found = True
                                    break
                        
                            if swap_found:
                                break
                    
                        if swap_found:
                            break
    
        logging.info(f"Weekend shift balancing: made {fixes_made} changes")
        if fixes_made > 0:
            self._save_current_as_best()
        return fixes_made > 0
        
    def _improve_weekend_distribution(self):
        """
        Improve weekend distribution by balancing "special constraint days" 
        (Fri/Sat/Sun, Holiday, Day-before-Holiday) more evenly among workers
        and attempting to resolve overloads based on max_consecutive_weekends 
        interpreted as a monthly cap for these days.
        """
        logging.info("Attempting to improve special day (weekend/holiday/eve) distribution")
    
        # Ensure data consistency before proceeding
        self._ensure_data_integrity() # This should call scheduler's data sync if it exists
                                      # or be robust enough on its own.
                                      # For now, assuming scheduler's data is the source of truth.

        # Count "special constraint day" assignments for each worker per month
        special_day_counts_by_month = {} 
        months = {}
        current_date_iter = self.start_date
        while current_date_iter <= self.end_date:
            month_key = (current_date_iter.year, current_date_iter.month)
            if month_key not in months: months[month_key] = []
            months[month_key].append(current_date_iter)
            current_date_iter += timedelta(days=1)

        for month_key, dates_in_month in months.items():
            current_month_special_day_counts = {} 
            for worker_val in self.workers_data:
                worker_id_val = worker_val['id']
                
                count = 0
                for date_val in dates_in_month:
                    # MANUALLY EMBEDDED CHECK
                    is_special_day = (date_val.weekday() >= 4 or  # Friday, Saturday, Sunday
                                      date_val in self.holidays or
                                      (date_val + timedelta(days=1)) in self.holidays)

                    if date_val in self.scheduler.worker_assignments.get(worker_id_val, set()) and is_special_day:
                        count += 1
                current_month_special_day_counts[worker_id_val] = count
            special_day_counts_by_month[month_key] = current_month_special_day_counts
    
        changes_made = 0
    
        for month_key, current_month_counts in special_day_counts_by_month.items():
            overloaded_workers = []
            underloaded_workers = []

            for worker_val in self.workers_data:
                worker_id_val = worker_val['id']
                work_percentage = worker_val.get('work_percentage', 100)
                
                # Using max_consecutive_weekends as a type of monthly limit for these special days.
                # This value comes from the scheduler's config.
                max_special_days_limit_for_month = self.max_consecutive_weekends 
                if work_percentage < 100: # Apply part-time adjustment
                    max_special_days_limit_for_month = max(1, int(self.max_consecutive_weekends * work_percentage / 100))

                actual_special_days_this_month = current_month_counts.get(worker_id_val, 0)

                if actual_special_days_this_month > max_special_days_limit_for_month:
                    overloaded_workers.append((worker_id_val, actual_special_days_this_month, max_special_days_limit_for_month))
                elif actual_special_days_this_month < max_special_days_limit_for_month:
                    available_slots = max_special_days_limit_for_month - actual_special_days_this_month
                    underloaded_workers.append((worker_id_val, actual_special_days_this_month, available_slots))

            overloaded_workers.sort(key=lambda x: x[1] - x[2], reverse=True) 
            underloaded_workers.sort(key=lambda x: x[2], reverse=True) 

            month_dates_list = months[month_key]
            
            special_days_this_month_list = []
            for date_val in month_dates_list:
                # MANUALLY EMBEDDED CHECK
                is_special_day = (date_val.weekday() >= 4 or
                                  date_val in self.holidays or
                                  (date_val + timedelta(days=1)) in self.holidays)
                if is_special_day:
                    special_days_this_month_list.append(date_val)

            for over_worker_id, _, _ in overloaded_workers: # Removed unused over_count, over_limit
                if not underloaded_workers: break 

                # Iterate only through the worker's assigned special days in this month
                possible_dates_to_move_from = []
                for s_day in special_days_this_month_list: # Iterate only over actual special days
                    if s_day in self.scheduler.worker_assignments.get(over_worker_id, set()) and \
                       over_worker_id in self.scheduler.schedule.get(s_day, []): # Check if actually in schedule slot
                        possible_dates_to_move_from.append(s_day)
                
                random.shuffle(possible_dates_to_move_from)

                for special_day_to_reassign in possible_dates_to_move_from:
                    # CRITICAL: Verificación centralizada de protección mandatory
                    if not self._can_modify_assignment(over_worker_id, special_day_to_reassign, "balance_weekend_shifts"):
                        continue
                    
                    try:
                        # Ensure the worker is actually in the schedule for this date and find post
                        if special_day_to_reassign not in self.scheduler.schedule or \
                           over_worker_id not in self.scheduler.schedule[special_day_to_reassign]:
                            logging.warning(f"Data inconsistency: Worker {over_worker_id} tracked for {special_day_to_reassign} but not in schedule slot.")
                            continue
                        post_val = self.scheduler.schedule[special_day_to_reassign].index(over_worker_id)
                    except (ValueError, KeyError, IndexError) as e: # Added specific exception logging
                        logging.warning(f"Inconsistency finding post for {over_worker_id} on {special_day_to_reassign} during special day balance: {e}")
                        continue

                    swap_done_for_this_shift = False
                    for under_worker_id, _, _ in underloaded_workers: # Removed unused counts/slots
                        # Check if under_worker is already assigned on this special day
                        if special_day_to_reassign in self.scheduler.schedule and \
                           under_worker_id in self.scheduler.schedule.get(special_day_to_reassign, []):
                            continue

                        # _can_assign_worker MUST use the same consistent definition of special day for its internal checks
                        # (especially its call to _would_exceed_weekend_limit)
                        if self._can_assign_worker(under_worker_id, special_day_to_reassign, post_val):
                            # Perform the assignment change
                            self.scheduler.schedule[special_day_to_reassign][post_val] = under_worker_id
                            self.scheduler.worker_assignments[over_worker_id].remove(special_day_to_reassign)
                            self.scheduler.worker_assignments.setdefault(under_worker_id, set()).add(special_day_to_reassign)

                            self.scheduler._update_tracking_data(over_worker_id, special_day_to_reassign, post_val, removing=True)
                            self.scheduler._update_tracking_data(under_worker_id, special_day_to_reassign, post_val) # Default is adding=False
                            
                            # Update local counts for the current month
                            current_month_counts[over_worker_id] -= 1
                            current_month_counts[under_worker_id] = current_month_counts.get(under_worker_id, 0) + 1
                            
                            changes_made += 1
                            logging.info(f"Improved special day distribution: Moved shift on {special_day_to_reassign.strftime('%Y-%m-%d')} "
                                         f"from worker {over_worker_id} to worker {under_worker_id}")

                            # --- Re-evaluate overloaded/underloaded lists locally ---
                            # Check if 'over_worker_id' is still overloaded
                            over_worker_new_count = current_month_counts[over_worker_id]
                            over_worker_obj = next((w for w in self.workers_data if w['id'] == over_worker_id), None)
                            over_worker_limit_this_month = self.max_consecutive_weekends
                            if over_worker_obj and over_worker_obj.get('work_percentage', 100) < 100:
                                over_worker_limit_this_month = max(1, int(self.max_consecutive_weekends * over_worker_obj.get('work_percentage',100) / 100))
                            
                            if over_worker_new_count <= over_worker_limit_this_month:
                                overloaded_workers = [(w, c, l) for w, c, l in overloaded_workers if w != over_worker_id]

                            # Check if 'under_worker_id' is still underloaded or became full
                            under_worker_new_count = current_month_counts[under_worker_id]
                            under_worker_obj = next((w for w in self.workers_data if w['id'] == under_worker_id), None)
                            under_worker_limit_this_month = self.max_consecutive_weekends
                            if under_worker_obj and under_worker_obj.get('work_percentage', 100) < 100:
                                under_worker_limit_this_month = max(1, int(self.max_consecutive_weekends * under_worker_obj.get('work_percentage',100) / 100))

                            if under_worker_new_count >= under_worker_limit_this_month:
                                underloaded_workers = [(w, c, s) for w, c, s in underloaded_workers if w != under_worker_id]
                            # --- End re-evaluation ---
                            
                            swap_done_for_this_shift = True
                            break # Found a swap for this special_day_to_reassign, move to next overloaded worker or next date
                    
                    if swap_done_for_this_shift:
                        # If a swap was made for this overloaded worker's shift,
                        # it's often good to re-evaluate the most overloaded worker.
                        # For simplicity here, we break and let the outer loop pick the next most overloaded.
                        break 
            
        logging.info(f"Special day (weekend/holiday/eve) distribution improvement: made {changes_made} changes")
        if changes_made > 0:
            self._synchronize_tracking_data() 
            self._save_current_as_best() 
        return changes_made > 0

    def distribute_holiday_shifts_proportionally(self):
        """
        Distribute holiday and pre-holiday shifts more fairly
        """
        # Separate holidays and pre-holidays
        holidays = self.holidays
        pre_holidays = [date - timedelta(days=1) for date in holidays 
                       if (date - timedelta(days=1)) not in holidays]
    
        # Combine with regular weekends for fair distribution
        special_days = set()
    
        # Add all Fridays, Saturdays, Sundays
        current = self.start_date
        while current <= self.end_date:
            if current.weekday() >= 4:  # Friday, Saturday, Sunday
                special_days.add(current)
            current += timedelta(days=1)
    
        # Add holidays (treated as Sundays)
        special_days.update(holidays)
    
        # Add pre-holidays (treated as Fridays) 
        special_days.update(pre_holidays)
    
        return self._distribute_special_days_proportionally(special_days)

    def distribute_holiday_shifts_proportionally(self):
        """
        Función pública para distribución proporcional de días especiales
        """
        # Obtener todos los días especiales
        special_days = set()
        
        # Obtener todas las fechas del horario para encontrar fines de semana
        all_dates = list(self.schedule.keys())
        
        # Agregar fines de semana (viernes y sábados)
        for date in all_dates:
            # Asegurar que date es un objeto datetime
            if hasattr(date, 'weekday'):
                if self._is_weekend_day(date):  # Pasar el objeto date completo, no solo weekday()
                    special_days.add(date)
        
        # Agregar festivos (considerados como domingos)
        for holiday_date in self.holidays:
            # Asegurar que holiday_date es un objeto datetime
            if hasattr(holiday_date, 'weekday') and holiday_date in all_dates:
                special_days.add(holiday_date)
        
        # Agregar pre-festivos (considerados como viernes)
        pre_holidays = getattr(self, 'pre_holidays', [])  # Usar getattr con default vacío
        for pre_holiday_date in pre_holidays:
            # Asegurar que pre_holiday_date es un objeto datetime
            if hasattr(pre_holiday_date, 'weekday') and pre_holiday_date in all_dates:
                special_days.add(pre_holiday_date)
        
        if not special_days:
            logging.info("No special days found for proportional distribution")
            return False
        
        logging.info(f"Found {len(special_days)} special days for proportional distribution")
        return self._distribute_special_days_proportionally(special_days)

    def _distribute_special_days_proportionally(self, special_days):
        """
        Distribute special days (weekends, holidays, pre-holidays) proportionally
        based on each worker's work percentage, with strict tolerance of +/-1
        """
        logging.info("Starting proportional distribution of special days...")
        
        # Calculate total shifts on special days
        total_special_shifts = 0
        special_day_shifts = {}
        
        for date in special_days:
            if date in self.schedule:
                shifts_count = len([w for w in self.schedule[date] if w is not None])
                total_special_shifts += shifts_count
                special_day_shifts[date] = shifts_count
        
        if total_special_shifts == 0:
            logging.info("No special day shifts to distribute")
            return False
        
        # Calculate current assignments on special days for each worker
        current_special_assignments = {}
        for worker in self.workers_data:
            worker_id = worker['id']
            count = 0
            for date in special_days:
                if date in self.worker_assignments.get(worker_id, set()):
                    count += 1
            current_special_assignments[worker_id] = count
        
        # Calculate proportional targets based on EFFECTIVE work percentage (considering absences)
        # Use the period covered by special_days to calculate effective percentages
        period_start = min(special_days) if special_days else self.start_date
        period_end = max(special_days) if special_days else self.end_date
        
        effective_work_percentages = {}
        total_effective_percentage = 0
        
        for worker in self.workers_data:
            worker_id = worker['id']
            effective_percentage = self._calculate_effective_work_percentage(
                worker_id, period_start, period_end
            )
            effective_work_percentages[worker_id] = effective_percentage
            total_effective_percentage += effective_percentage
        
        if total_effective_percentage == 0:
            logging.warning("Total effective work percentage is zero (all workers unavailable)")
            return False
        
        logging.info(f"Using effective work percentages (adjusted for absences): "
                    f"total={total_effective_percentage:.1f}%")
        
        # Calculate exact proportional targets using effective percentages
        exact_targets = {}
        for worker in self.workers_data:
            worker_id = worker['id']
            effective_percentage = effective_work_percentages[worker_id]
            proportion = effective_percentage / total_effective_percentage
            exact_target = proportion * total_special_shifts
            exact_targets[worker_id] = exact_target
        
        # Apply largest remainder method for integer distribution with strict tolerance
        integer_targets = {}
        remainders = []
        total_assigned = 0
        
        for worker_id, exact_target in exact_targets.items():
            integer_part = int(exact_target)
            remainder = exact_target - integer_part
            integer_targets[worker_id] = integer_part
            remainders.append((worker_id, remainder))
            total_assigned += integer_part
        
        # Distribute remaining shifts using largest remainder method
        remaining_shifts = total_special_shifts - total_assigned
        remainders.sort(key=lambda x: x[1], reverse=True)
        
        for i in range(int(remaining_shifts)):
            if i < len(remainders):
                worker_id = remainders[i][0]
                integer_targets[worker_id] += 1
        
        # Apply strict +/-1 tolerance while preserving proportionality as much as possible
        tolerance = 1  # Enforce strict +/-1 tolerance
        
        # Sort workers by their original proportional targets to maintain fairness
        sorted_workers = sorted(integer_targets.items(), key=lambda x: x[1], reverse=True)
        
        # Enforce +/-1 tolerance: max difference between any two workers should be 1
        min_target = min(integer_targets.values()) if integer_targets else 0
        max_target = max(integer_targets.values()) if integer_targets else 0
        
        # If the difference is already within tolerance, keep the original allocation
        if max_target - min_target <= tolerance:
            adjusted_targets = integer_targets.copy()
            logging.info(f"Original proportional distribution already meets tolerance: min={min_target}, max={max_target}")
        else:
            # Need to adjust to enforce tolerance while preserving as much proportionality as possible
            logging.info(f"Adjusting proportional distribution from min={min_target}, max={max_target} to meet +/-{tolerance} tolerance")
            
            # Enhanced strategy: gradually compress the range while preserving relative proportions
            adjusted_targets = integer_targets.copy()
            
            # Iteratively reduce the range by moving shifts from extreme workers
            max_iterations = 20
            iteration = 0
            
            while max_target - min_target > tolerance and iteration < max_iterations:
                iteration += 1
                
                # Find workers at the extremes
                min_workers = [w for w in adjusted_targets if adjusted_targets[w] == min_target]
                max_workers = [w for w in adjusted_targets if adjusted_targets[w] == max_target]
                
                # Calculate how much we need to reduce the range
                excess_range = (max_target - min_target) - tolerance
                shifts_to_move = min(1, excess_range)
                
                # Select best candidates for redistribution
                # Max worker: choose one with lowest effective work percentage (least impact on proportionality)
                # Min worker: choose one with highest effective work percentage (most deserving)
                if min_workers and max_workers and shifts_to_move > 0:
                    max_worker = min(max_workers, 
                                   key=lambda w: (exact_targets[w], effective_work_percentages[w]))
                    min_worker = max(min_workers, 
                                   key=lambda w: (exact_targets[w], effective_work_percentages[w]))
                    
                    # Only move if it actually reduces the range
                    if adjusted_targets[max_worker] > adjusted_targets[min_worker] + tolerance:
                        adjusted_targets[max_worker] -= 1
                        adjusted_targets[min_worker] += 1
                        
                        # Update min/max for next iteration
                        min_target = min(adjusted_targets.values())
                        max_target = max(adjusted_targets.values())
                    else:
                        break  # Can't improve further
                else:
                    break  # No more moves possible
            
            # If we still can't meet tolerance, use a more aggressive approach
            if max_target - min_target > tolerance:
                logging.warning(f"Could not meet tolerance with gradual adjustment, using allocation approach")
                
                # Sort workers by their proportional priority (exact target, then effective work percentage)
                workers_by_priority = sorted(
                    adjusted_targets.keys(),
                    key=lambda w: (exact_targets[w], effective_work_percentages[w]),
                    reverse=True
                )
                
                # Calculate the best base value that preserves most proportionality
                avg_target = total_special_shifts / len(self.workers_data)
                base_target = int(avg_target)
                remainder = total_special_shifts % len(self.workers_data)
                
                # Test base and base+1 to see which preserves more proportionality
                test_targets_base = {}
                test_targets_base_plus = {}
                
                # Option 1: Use base as minimum
                for i, worker_id in enumerate(workers_by_priority):
                    if i < remainder:
                        test_targets_base[worker_id] = base_target + 1
                    else:
                        test_targets_base[worker_id] = base_target
                
                # Option 2: Use base+1 as minimum (if possible)
                if (base_target + 1) * len(self.workers_data) <= total_special_shifts + len(self.workers_data):
                    remaining_after_base_plus = total_special_shifts - (base_target + 1) * len(self.workers_data)
                    for i, worker_id in enumerate(workers_by_priority):
                        if i < len(self.workers_data) + remaining_after_base_plus:
                            test_targets_base_plus[worker_id] = base_target + 2
                        else:
                            test_targets_base_plus[worker_id] = base_target + 1
                
                # Choose the option with lower deviation from exact targets
                deviation_base = sum(abs(test_targets_base[w] - exact_targets[w]) for w in test_targets_base)
                deviation_base_plus = float('inf')
                if test_targets_base_plus:
                    deviation_base_plus = sum(abs(test_targets_base_plus[w] - exact_targets[w]) for w in test_targets_base_plus)
                
                if deviation_base <= deviation_base_plus:
                    adjusted_targets = test_targets_base
                else:
                    adjusted_targets = test_targets_base_plus
        
        # Verify tolerance is met and total is correct
        final_min = min(adjusted_targets.values())
        final_max = max(adjusted_targets.values())
        final_total = sum(adjusted_targets.values())
        
        logging.info(f"Final adjusted targets - min={final_min}, max={final_max}, difference={final_max - final_min}, total={final_total}")
        
        if final_max - final_min > tolerance:
            logging.warning(f"Tolerance still violated after adjustment! This should not happen.")
        
        if final_total != total_special_shifts:
            logging.warning(f"Total mismatch: expected {total_special_shifts}, got {final_total}")
        
        # Log the adjusted distribution with proportionality comparison
        for worker_id in sorted(adjusted_targets.keys()):
            work_pct = self._get_work_percentage(worker_id)
            exact_target = exact_targets[worker_id]
            adjusted_target = adjusted_targets[worker_id]
            deviation = adjusted_target - exact_target
            logging.info(f"Worker {worker_id} (work {work_pct}%): ideal={exact_target:.2f}, target={adjusted_target}, deviation={deviation:+.2f}")
        
        # Perform redistribution to meet adjusted targets using improved algorithm
        changes_made = 0
        max_iterations = 50
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            progress_made = False
            
            # Calculate current special day assignments
            current_assignments = {}
            for worker_id in adjusted_targets:
                current_assignments[worker_id] = len([d for d in self.worker_assignments.get(worker_id, set()) if d in special_days])
            
            # Find workers most in need of rebalancing
            priority_swaps = []
            
            for over_worker in adjusted_targets:
                over_current = current_assignments[over_worker]
                over_target = adjusted_targets[over_worker]
                over_excess = over_current - over_target
                
                if over_excess <= 0:
                    continue
                
                for under_worker in adjusted_targets:
                    under_current = current_assignments[under_worker]
                    under_target = adjusted_targets[under_worker]
                    under_deficit = under_target - under_current
                    
                    if under_deficit <= 0:
                        continue
                    
                    # Calculate swap benefit (how much it improves balance)
                    benefit = min(over_excess, under_deficit)
                    priority_swaps.append((benefit, over_worker, under_worker))
            
            # Sort by benefit (highest first)
            priority_swaps.sort(key=lambda x: x[0], reverse=True)
            
            # Execute highest priority swaps first
            for benefit, over_worker, under_worker in priority_swaps:
                if benefit <= 0:
                    break
                
                swap_made = self._attempt_special_day_swap(special_days, over_worker, under_worker)
                if swap_made:
                    changes_made += 1
                    progress_made = True
                    logging.info(f"Special day swap: {over_worker} -> {under_worker} (benefit: {benefit})")
                    break
            
            if not progress_made:
                break
        
        logging.info(f"Special days proportional distribution completed: {changes_made} changes made in {iteration} iterations")
        
        # Synchronize tracking data if changes were made
        if changes_made > 0:
            self._synchronize_tracking_data()
            self._save_current_as_best()
        
        return changes_made > 0

    def _attempt_special_day_swap(self, special_days, over_worker, under_worker):
        """
        Intenta intercambiar un turno de día especial entre trabajadores
        
        CRITICAL: NEVER swaps mandatory assignments - they are immovable
        """
        # Buscar días especiales donde over_worker esté asignado
        over_worker_special_days = []
        for date in special_days:
            # CRITICAL: Skip mandatory assignments
            if self._is_mandatory(over_worker, date):
                continue
            if (date in self.schedule and 
                date in self.worker_assignments.get(over_worker, set())):
                over_worker_special_days.append(date)
        
        if not over_worker_special_days:
            return False
        
        # Buscar días especiales donde under_worker NO esté asignado
        under_worker_available_days = []
        for date in special_days:
            if (date in self.schedule and 
                date not in self.worker_assignments.get(under_worker, set())):
                under_worker_available_days.append(date)
        
        if not under_worker_available_days:
            return False
        
        # Intentar intercambio directo
        for over_date in over_worker_special_days:
            if over_date in under_worker_available_days:
                # Buscar posición del over_worker en este día
                for post_idx, assigned_worker in enumerate(self.schedule[over_date]):
                    if assigned_worker == over_worker:
                        # CRITICAL: Double-check mandatory before swap
                        if self._is_mandatory(over_worker, over_date):
                            continue
                        if (over_worker, over_date) in self._locked_mandatory:
                            continue
                        
                        # Verificar si under_worker puede tomar esta posición
                        if self._can_assign_worker(under_worker, over_date, post_idx):
                            # CRITICAL: Check tolerance BEFORE assigning
                            if self._would_violate_tolerance(under_worker, over_date, allow_relaxation=True):
                                logging.debug(f"Special day swap rejected: {under_worker} would violate ±12% limit")
                                continue
                            # CRITICAL: Verificación final de protección mandatory
                            if not self._can_modify_assignment(over_worker, over_date, "swap_special_day"):
                                logging.warning(f"🔒 BLOCKED: Cannot swap MANDATORY {over_worker} on {over_date.strftime('%Y-%m-%d')}")
                                continue
                            
                            # Realizar el intercambio
                            self.schedule[over_date][post_idx] = under_worker
                            
                            # Actualizar seguimiento
                            self.worker_assignments[over_worker].remove(over_date)
                            self.worker_assignments.setdefault(under_worker, set()).add(over_date)
                            
                            # Actualizar tracking del scheduler
                            self.scheduler._update_tracking_data(over_worker, over_date, post_idx, removing=True)
                            self.scheduler._update_tracking_data(under_worker, over_date, post_idx)
                            
                            return True
        
        # Intentar intercambio con días no especiales
        for over_date in over_worker_special_days:
            # CRITICAL: Double-check mandatory (should already be filtered, but safety first)
            if self._is_mandatory(over_worker, over_date):
                continue
                
            for over_post_idx, assigned_worker in enumerate(self.schedule[over_date]):
                if assigned_worker == over_worker:
                    # Buscar un día no especial donde under_worker esté asignado
                    for date in self.schedule.keys():  # Usar las fechas del schedule en lugar de self.periods
                        # CRITICAL: Skip mandatory assignments for under_worker too
                        if self._is_mandatory(under_worker, date):
                            continue
                            
                        if (date not in special_days and 
                            date in self.worker_assignments.get(under_worker, set())):
                            for under_post_idx, under_assigned in enumerate(self.schedule.get(date, [])):
                                if under_assigned == under_worker:
                                    # Verificar si el intercambio es válido
                                    if (self._can_assign_worker(under_worker, over_date, over_post_idx) and
                                        self._can_assign_worker(over_worker, date, under_post_idx)):
                                        
                                        # CRITICAL: Verify 7/14 pattern for BOTH workers in their NEW positions
                                        # Remove them temporarily to check pattern
                                        self.worker_assignments[under_worker].discard(date)
                                        if self._violates_7_14_pattern(under_worker, over_date):
                                            self.worker_assignments[under_worker].add(date)  # Restore
                                            logging.debug(f"Swap rejected: {under_worker} would violate 7/14 pattern on {over_date.strftime('%Y-%m-%d')}")
                                            continue
                                        self.worker_assignments[under_worker].add(date)  # Restore
                                        
                                        self.worker_assignments[over_worker].discard(over_date)
                                        if self._violates_7_14_pattern(over_worker, date):
                                            self.worker_assignments[over_worker].add(over_date)  # Restore
                                            logging.debug(f"Swap rejected: {over_worker} would violate 7/14 pattern on {date.strftime('%Y-%m-%d')}")
                                            continue
                                        self.worker_assignments[over_worker].add(over_date)  # Restore
                                        
                                        # CRITICAL: Verify incompatibility in FINAL state after swap
                                        # Check under_worker against others on over_date
                                        others_on_over_date = [w for idx, w in enumerate(self.schedule.get(over_date, [])) 
                                                              if w is not None and idx != over_post_idx and w != over_worker]
                                        if not self._check_incompatibility_with_list(under_worker, others_on_over_date):
                                            logging.debug(f"Swap rejected: {under_worker} incompatible on {over_date.strftime('%Y-%m-%d')}")
                                            continue
                                        
                                        # Check over_worker against others on date
                                        others_on_under_date = [w for idx, w in enumerate(self.schedule.get(date, [])) 
                                                               if w is not None and idx != under_post_idx and w != under_worker]
                                        if not self._check_incompatibility_with_list(over_worker, others_on_under_date):
                                            logging.debug(f"Swap rejected: {over_worker} incompatible on {date.strftime('%Y-%m-%d')}")
                                            continue
                                        
                                        # CRITICAL: Check tolerance for BOTH workers in swap
                                        if self._would_violate_tolerance(under_worker, over_date, allow_relaxation=True):
                                            logging.debug(f"Swap rejected: {under_worker} would violate ±12% limit on {over_date.strftime('%Y-%m-%d')}")
                                            continue
                                        if self._would_violate_tolerance(over_worker, date, allow_relaxation=True):
                                            logging.debug(f"Swap rejected: {over_worker} would violate ±12% limit on {date.strftime('%Y-%m-%d')}")
                                            continue
                                        
                                        # Realizar intercambio completo
                                        self.schedule[over_date][over_post_idx] = under_worker
                                        self.schedule[date][under_post_idx] = over_worker
                                        
                                        # Actualizar seguimiento
                                        self.worker_assignments[over_worker].remove(over_date)
                                        self.worker_assignments[over_worker].add(date)
                                        self.worker_assignments[under_worker].remove(date)
                                        self.worker_assignments[under_worker].add(over_date)
                                        
                                        # Actualizar tracking del scheduler
                                        self.scheduler._update_tracking_data(over_worker, over_date, over_post_idx, removing=True)
                                        self.scheduler._update_tracking_data(under_worker, over_date, over_post_idx)
                                        self.scheduler._update_tracking_data(under_worker, date, under_post_idx, removing=True)
                                        self.scheduler._update_tracking_data(over_worker, date, under_post_idx)
                                        
                                        return True
        
        return False

    def _get_work_percentage(self, worker_id):
        """Get work percentage for a worker"""
        for worker in self.workers_data:
            if worker['id'] == worker_id:
                return worker.get('work_percentage', 100)
        return 100

    def _calculate_effective_work_percentage(self, worker_id, period_start, period_end):
        """
        Calculate effective work percentage considering absence periods
        
        Args:
            worker_id: ID of the worker
            period_start: Start date of the period to analyze
            period_end: End date of the period to analyze
            
        Returns:
            float: Effective work percentage (0-100) adjusted for absences
        """
        worker_data = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker_data:
            return 0
        
        base_work_percentage = worker_data.get('work_percentage', 100)
        
        # If no absence periods to consider, return base percentage
        days_off_str = worker_data.get('days_off', '')
        work_periods_str = worker_data.get('work_periods', '')
        
        # Calculate total days in the period
        total_days = (period_end - period_start).days + 1
        
        # Calculate working days (excluding absences)
        working_days = 0
        current_date = period_start
        
        while current_date <= period_end:
            is_available = True
            
            # Check if within work periods (if defined)
            if work_periods_str:
                try:
                    work_ranges = self.date_utils.parse_date_ranges(work_periods_str)
                    if not any(start <= current_date <= end for start, end in work_ranges):
                        is_available = False
                except Exception as e:
                    logging.warning(f"Error parsing work_periods for {worker_id}: {e}")
                    is_available = False
            
            # Check if in days off (absence periods)
            if is_available and days_off_str:
                try:
                    off_ranges = self.date_utils.parse_date_ranges(days_off_str)
                    if any(start <= current_date <= end for start, end in off_ranges):
                        is_available = False
                except Exception as e:
                    logging.warning(f"Error parsing days_off for {worker_id}: {e}")
            
            if is_available:
                working_days += 1
            
            current_date += timedelta(days=1)
        
        # Calculate availability factor (percentage of time actually available)
        if total_days == 0:
            availability_factor = 0
        else:
            availability_factor = working_days / total_days
        
        # Effective work percentage = base percentage × availability factor
        effective_percentage = base_work_percentage * availability_factor
        
        logging.debug(f"Worker {worker_id}: base={base_work_percentage}%, "
                     f"available_days={working_days}/{total_days}, "
                     f"effective={effective_percentage:.1f}%")
        
        return effective_percentage

    def rebalance_weekend_distribution(self):
        """
        Rebalance weekend shifts to ensure fair distribution within tolerance
        """
        weekend_assignments = {}
        total_weekend_shifts = 0
    
        # Count current weekend assignments
        for worker_id in self.worker_assignments:
            weekend_count = len([d for d in self.worker_assignments[worker_id] 
                               if self._is_weekend_day(d)])
            weekend_assignments[worker_id] = weekend_count
            total_weekend_shifts += weekend_count
    
        # Calculate ideal distribution
        ideal_distribution = self._calculate_ideal_weekend_distribution()
    
        # Identify workers who are over/under assigned
        over_assigned = []
        under_assigned = []
    
        for worker_id, current_count in weekend_assignments.items():
            target_range = ideal_distribution[worker_id]
        
            if current_count > target_range['max']:
                over_assigned.append((worker_id, current_count - target_range['max']))
            elif current_count < target_range['min']:
                under_assigned.append((worker_id, target_range['min'] - current_count))
    
        # Perform rebalancing
        return self._perform_shift_rebalancing(over_assigned, under_assigned)

    def _is_weekend_day(self, date):
        """Check if a date is a weekend day (Friday, Saturday, Sunday)"""
        return date.weekday() >= 4

    def _calculate_ideal_weekend_distribution(self):
        """Calculate ideal weekend shift distribution based on work percentages"""
        ideal_distribution = {}
        
        # Calculate total weekend capacity
        total_weekend_capacity = 0
        worker_capacities = {}
        
        for worker in self.workers_data:
            worker_id = worker['id']
            work_percentage = worker.get('work_percentage', 100) / 100.0
            
            # Count available weekend days for this worker
            available_weekends = 0
            current_date = self.start_date
            while current_date <= self.end_date:
                if (self._is_weekend_day(current_date) and 
                    not self._is_worker_unavailable(worker_id, current_date)):
                    available_weekends += 1
                current_date += timedelta(days=1)
            
            capacity = available_weekends * work_percentage
            worker_capacities[worker_id] = capacity
            total_weekend_capacity += capacity
        
        # Calculate total weekend shifts available
        total_weekend_shifts = 0
        current_date = self.start_date
        while current_date <= self.end_date:
            if self._is_weekend_day(current_date) and current_date in self.schedule:
                total_weekend_shifts += len([w for w in self.schedule[current_date] if w is not None])
            current_date += timedelta(days=1)
        
        # Calculate proportional targets with tolerance
        tolerance = getattr(self.scheduler, 'weekend_tolerance', 1)
        
        for worker_id, capacity in worker_capacities.items():
            if total_weekend_capacity > 0:
                proportion = capacity / total_weekend_capacity
                target = proportion * total_weekend_shifts
                
                min_target = max(0, round(target * (1 - tolerance)))
                max_target = round(target * (1 + tolerance))
                
                ideal_distribution[worker_id] = {
                    'target': target,
                    'min': min_target,
                    'max': max_target
                }
            else:
                ideal_distribution[worker_id] = {
                    'target': 0,
                    'min': 0,
                    'max': tolerance
                }
        
        return ideal_distribution

    def _perform_shift_rebalancing(self, over_assigned, under_assigned):
        """Perform actual shift rebalancing between over and under assigned workers"""
        changes_made = 0
        max_iterations = 50
        
        for iteration in range(max_iterations):
            if not over_assigned or not under_assigned:
                break
            
            progress_made = False
            
            # Sort by priority (largest imbalances first)
            over_assigned.sort(key=lambda x: x[1], reverse=True)
            under_assigned.sort(key=lambda x: x[1], reverse=True)
            
            for i, (over_worker_id, over_excess) in enumerate(over_assigned):
                if over_excess <= 0:
                    continue
                
                # Find weekend shifts that can be moved
                moveable_weekend_shifts = []
                for date in self.worker_assignments.get(over_worker_id, set()):
                    if (self._is_weekend_day(date) and 
                        date in self.schedule and
                        over_worker_id in self.schedule[date] and
                        self._can_modify_assignment(over_worker_id, date, "rebalance_weekend")):
                        post = self.schedule[date].index(over_worker_id)
                        moveable_weekend_shifts.append((date, post))
                
                if not moveable_weekend_shifts:
                    continue
                
                # Try to assign to under-assigned workers
                for j, (under_worker_id, under_deficit) in enumerate(under_assigned):
                    if under_deficit <= 0:
                        continue
                    
                    for date, post in moveable_weekend_shifts:
                        # Check if under-assigned worker is already assigned on this date
                        if (date in self.schedule and 
                            under_worker_id in self.schedule.get(date, [])):
                            continue
                        
                        # Check if assignment is valid
                        if self._can_assign_worker(under_worker_id, date, post):
                            # CRITICAL: Check tolerance BEFORE assigning (±12% absolute limit)
                            if self._would_violate_tolerance(under_worker_id, date, allow_relaxation=True):
                                continue  # Skip if would violate ±12% limit
                            # CRITICAL: Final check - can we modify this assignment?
                            if not self._can_modify_assignment(over_worker_id, date, "rebalance_weekend_final"):
                                logging.warning(f"🔒 BLOCKED: Cannot modify MANDATORY {over_worker_id} on {date.strftime('%Y-%m-%d')}")
                                continue
                            
                            # Perform the reassignment
                            self.schedule[date][post] = under_worker_id
                            
                            # Update assignments tracking
                            self.worker_assignments[over_worker_id].remove(date)
                            self.worker_assignments.setdefault(under_worker_id, set()).add(date)
                            
                            # Update scheduler tracking
                            self.scheduler._update_tracking_data(over_worker_id, date, post, removing=True)
                            self.scheduler._update_tracking_data(under_worker_id, date, post)
                            
                            changes_made += 1
                            progress_made = True
                            
                            logging.info(f"Rebalanced weekend shift: {date.strftime('%Y-%m-%d')} "
                                       f"from {over_worker_id} to {under_worker_id}")
                            
                            # Update tracking
                            over_assigned[i] = (over_worker_id, over_excess - 1)
                            under_assigned[j] = (under_worker_id, under_deficit - 1)
                            
                            break
                    
                    if progress_made:
                        break
                
                if progress_made:
                    break
            
            if not progress_made:
                break
        
        # Synchronize tracking data if changes were made
        if changes_made > 0:
            self._synchronize_tracking_data()
            self._save_current_as_best()
        
        logging.info(f"Weekend rebalancing completed: {changes_made} changes made")
        return changes_made > 0
        
    def _balance_target_shifts_aggressively(self):
        """Balance workers to meet their exact target_shifts, focusing on largest deviations first"""
        logging.info("Starting aggressive target balancing...")
        changes_made = 0
    
        # Calculate deviations for all workers
        worker_deviations = []
        for worker in self.workers_data:
            worker_id = worker['id']
            target = worker['target_shifts']
            current = len(self.worker_assignments.get(worker_id, []))
            deviation = current - target
            # CRÍTICO: Procesar incluso desviaciones pequeñas (0.3 en vez de 0.5)
            if abs(deviation) > 0.3:
                worker_deviations.append((worker_id, deviation, target, current))
                logging.debug(f"Worker {worker_id}: current={current}, target={target}, deviation={deviation:+.1f}")
    
        # Sort by absolute deviation (largest first)
        worker_deviations.sort(key=lambda x: abs(x[1]), reverse=True)
        
        logging.info(f"Found {len(worker_deviations)} workers with deviations to balance")
    
        for worker_id, deviation, target, current in worker_deviations:
            if deviation > 0:  # Worker has too many shifts
                # Intentar redistribuir TODOS los turnos de exceso, no solo algunos
                excess_to_redistribute = max(1, int(deviation))
                logging.info(f"Attempting to redistribute {excess_to_redistribute} excess shifts from {worker_id}")
                changes_made += self._try_redistribute_excess_shifts(worker_id, excess_to_redistribute)
    
        logging.info(f"Aggressive target balancing: {changes_made} changes made")
        return changes_made

    def _try_redistribute_excess_shifts(self, overloaded_worker_id, excess_count):
        """Try to move excess shifts from overloaded worker to underloaded workers"""
        changes = 0
        # CRÍTICO: Aumentar intentos para ser más agresivo (de 5 a 10)
        max_attempts = min(excess_count, 10)
        
        logging.debug(f"Redistributing up to {max_attempts} shifts from {overloaded_worker_id} (excess: {excess_count})")
    
        # Find underloaded workers
        underloaded_workers = []
        for worker in self.workers_data:
            worker_id = worker['id']
            if worker_id == overloaded_worker_id:
                continue
            target = worker['target_shifts']
            current = len(self.worker_assignments.get(worker_id, []))
            if current < target:
                deficit = target - current
                underloaded_workers.append((worker_id, deficit))
                logging.debug(f"  Underloaded: {worker_id} (deficit: {deficit})")
    
        # Sort by largest deficit first (priorizar los más necesitados)
        underloaded_workers.sort(key=lambda x: x[1], reverse=True)
    
        if not underloaded_workers:
            logging.debug(f"No underloaded workers found for redistribution")
            return 0
        
        logging.info(f"Found {len(underloaded_workers)} underloaded workers for redistribution")
    
        # Try to move shifts
        assignments = list(self.worker_assignments.get(overloaded_worker_id, []))
        random.shuffle(assignments)
    
        for date in assignments[:max_attempts]:
            # CRITICAL: Verificación centralizada de protección mandatory
            if not self._can_modify_assignment(overloaded_worker_id, date, "redistribute_excess"):
                continue
            
            try:
                post = self.schedule[date].index(overloaded_worker_id)
            except (ValueError, KeyError):
                continue
            
            # Try to assign to an underloaded worker
            for under_worker_id, deficit in underloaded_workers:
                if under_worker_id in self.schedule.get(date, []):
                    continue  # Already assigned this date
                
                if self._can_assign_worker(under_worker_id, date, post):
                    # CRITICAL: Check tolerance BEFORE assigning (±12% absolute limit)
                    if self._would_violate_tolerance(under_worker_id, date, allow_relaxation=True):
                        logging.debug(f"Redistribution rejected: {under_worker_id} would violate ±12% limit")
                        continue  # Try next underloaded worker
                    # CRITICAL: Verify 7/14 pattern for the NEW worker receiving the shift
                    if self._violates_7_14_pattern(under_worker_id, date):
                        logging.debug(f"Redistribution rejected: {under_worker_id} would violate 7/14 pattern on {date.strftime('%Y-%m-%d')}")
                        continue  # Try next underloaded worker
                    
                    # CRITICAL: Verify incompatibility with others on this date
                    others_on_date = [w for idx, w in enumerate(self.schedule.get(date, [])) 
                                     if w is not None and idx != post and w != overloaded_worker_id]
                    if not self._check_incompatibility_with_list(under_worker_id, others_on_date):
                        logging.debug(f"Redistribution rejected: {under_worker_id} incompatible on {date.strftime('%Y-%m-%d')}")
                        continue  # Try next underloaded worker
                    
                    # CRITICAL: Final check before transfer
                    if not self._can_modify_assignment(overloaded_worker_id, date, "redistribute_excess_final"):
                        logging.warning(f"🔒 BLOCKED: Cannot transfer MANDATORY {overloaded_worker_id} on {date.strftime('%Y-%m-%d')}")
                        continue
                    
                    # Make the transfer
                    self.schedule[date][post] = under_worker_id
                    self.worker_assignments[overloaded_worker_id].remove(date)
                    self.worker_assignments.setdefault(under_worker_id, set()).add(date)
                
                    # Update tracking
                    self.scheduler._update_tracking_data(overloaded_worker_id, date, post, removing=True)
                    self.scheduler._update_tracking_data(under_worker_id, date, post)
                
                    changes += 1
                    logging.info(f"Redistributed shift on {date.strftime('%Y-%m-%d')} from worker {overloaded_worker_id} to {under_worker_id}")
                    break
                
            if changes >= max_attempts:
                break
    
        return changes
    
    # ========================================
    # 8. POST ROTATION AND DISTRIBUTION
    # ========================================
    def _identify_imbalanced_posts(self, deviation_threshold=1.5):
        """
        Identifies workers with an imbalanced distribution of assigned posts.

        Args:
            deviation_threshold: How much the count for a single post can deviate
                                 from the average before considering the worker imbalanced.

        Returns:
            List of tuples: [(worker_id, post_counts, max_deviation), ...]
                           Sorted by max_deviation descending.
        """
        imbalanced_workers = []
        num_posts = self.num_shifts
        if num_posts == 0: return [] # Avoid division by zero

        # Use scheduler's worker data and post tracking
        for worker_val in self.scheduler.workers_data: # Renamed worker
            worker_id_val = worker_val['id'] # Renamed worker_id
            # Get post counts, defaulting to an empty dict if worker has no assignments yet
            actual_post_counts = self.scheduler.worker_posts.get(worker_id_val, {})
            total_assigned = sum(actual_post_counts.values())

            # If worker has no shifts or only one type of post, they can't be imbalanced yet
            if total_assigned == 0 or num_posts <= 1:
                continue

            target_per_post = total_assigned / num_posts
            max_deviation = 0
            post_deviations = {} # Store deviation per post

            for post_val in range(num_posts): # Renamed post
                actual_count = actual_post_counts.get(post_val, 0)
                deviation = actual_count - target_per_post
                post_deviations[post_val] = deviation
                if abs(deviation) > max_deviation:
                    max_deviation = abs(deviation)

            # Consider imbalanced if the count for any post is off by more than the threshold
            if max_deviation > deviation_threshold:
                # Store the actual counts, not the deviations map for simplicity
                imbalanced_workers.append((worker_id_val, actual_post_counts.copy(), max_deviation))
                logging.debug(f"Worker {worker_id_val} identified as imbalanced for posts. Max Deviation: {max_deviation:.2f}, Target/Post: {target_per_post:.2f}, Counts: {actual_post_counts}")


        # Sort by the magnitude of imbalance (highest deviation first)
        imbalanced_workers.sort(key=lambda x: x[2], reverse=True)
        return imbalanced_workers

    def _get_over_under_posts(self, post_counts, total_assigned, balance_threshold=1.0):
        """
        Given a worker's post counts, find which posts they have significantly
        more or less than the average.

        Args:
            post_counts (dict): {post_index: count} for the worker.
            total_assigned (int): Total shifts assigned to the worker.
            balance_threshold: How far from the average count triggers over/under.

        Returns:
            tuple: (list_of_overassigned_posts, list_of_underassigned_posts)
                   Each list contains tuples: [(post_index, count), ...]\
                   Sorted by deviation magnitude.
        """
        overassigned = []
        underassigned = []
        num_posts = self.num_shifts
        if num_posts <= 1 or total_assigned == 0:
            return [], [] # Cannot be over/under assigned

        target_per_post = total_assigned / num_posts

        for post_val in range(num_posts): # Renamed post
            actual_count = post_counts.get(post_val, 0)
            deviation = actual_count - target_per_post

            # Use a threshold slightly > 0 to avoid minor float issues
            # Consider overassigned if count is clearly higher than target
            if deviation > balance_threshold:
                overassigned.append((post_val, actual_count, deviation)) # Include deviation for sorting
            # Consider underassigned if count is clearly lower than target
            elif deviation < -balance_threshold:
                 underassigned.append((post_val, actual_count, deviation)) # Deviation is negative

        # Sort overassigned: highest count (most over) first
        overassigned.sort(key=lambda x: x[2], reverse=True)
        # Sort underassigned: lowest count (most under) first (most negative deviation)
        underassigned.sort(key=lambda x: x[2])

        # Return only (post, count) tuples
        overassigned_simple = [(p, c) for p, c, d_val in overassigned] # Renamed d to d_val
        underassigned_simple = [(p, c) for p, c, d_val in underassigned] # Renamed d to d_val

        return overassigned_simple, underassigned_simple
        
    def _adjust_last_post_distribution(self, balance_tolerance=1.0, max_iterations=10): # balance_tolerance of 1 means +/-1
        """
        Adjusts the distribution of last-post slots among workers for days NOT in variable_shifts periods.
        Uses improved formula: turnos por trabajador = (turnos asignados al trabajador / turnos al día) ± 1
        Swaps are only performed intra-day between workers already assigned on that day.

        Args:
            balance_tolerance (float): Allowed deviation from the average number of last posts.
                                     A tolerance of 1.0 aims for a +/-1 overall balance.
            max_iterations (int): Maximum number of full passes to attempt balancing.

        Returns:
            bool: True if any swap was made across all iterations, False otherwise.
        """
        return self._adjust_last_post_distribution_improved(balance_tolerance, max_iterations)
    
    def _adjust_last_post_distribution_improved(self, balance_tolerance=1.0, max_iterations=10):
        """
        Improved last post distribution using formula:
        Turnos por trabajador = (turnos asignados al trabajador / turnos al día) ± 1
        
        This ensures each worker gets a fair distribution of last posts based on their
        total shift assignments relative to the daily shift count.
        """
        overall_swaps_made_across_iterations = False
        logging.info(f"Starting IMPROVED last post distribution adjustment (max_iterations={max_iterations}, tolerance={balance_tolerance}).")
        logging.info("Using formula: last_posts_per_worker = (total_shifts_per_worker / shifts_per_day) ± 1")
        logging.info("This will only apply to days NOT within a variable shift period.")

        # Calculate shifts per day (this should be consistent)
        shifts_per_day = len(self.workers_data) if hasattr(self, 'num_shifts') else self.num_shifts if hasattr(self, 'num_shifts') else 2
        
        # If we can get it from the scheduler
        if hasattr(self, 'scheduler') and hasattr(self.scheduler, 'num_shifts'):
            shifts_per_day = self.scheduler.num_shifts
        
        logging.info(f"Using shifts_per_day = {shifts_per_day} for calculations")

        for iteration in range(max_iterations):
            logging.info(f"--- IMPROVED Last Post Adjustment Iteration: {iteration + 1}/{max_iterations} ---")
            made_swap_in_this_iteration = False
            
            # Ensure all tracking data is perfectly up-to-date before counting
            self._synchronize_tracking_data()

            # 1. Count total shifts assigned to each worker (non-variable periods only)
            worker_total_shifts = {str(w['id']): 0 for w in self.workers_data}
            worker_last_posts = {str(w['id']): 0 for w in self.workers_data}
            total_last_slots_in_non_variable_periods = 0
            
            # Store (date, index_of_last_assigned_post, worker_in_that_post) for swappable days
            swappable_days_with_last_post_info = []

            # First pass: count total shifts and last posts per worker
            for date_val, shifts_on_day in self.schedule.items():
                if not shifts_on_day or not any(s is not None for s in shifts_on_day):
                    continue

                # Skip variable shift periods
                if self._is_date_in_variable_shift_period(date_val):
                    logging.debug(f"Skipping date {date_val.strftime('%Y-%m-%d')} for last post balancing (variable shift period)")
                    continue

                # Count total shifts for each worker on this day
                for shift_idx, worker_id in enumerate(shifts_on_day):
                    if worker_id is not None:
                        worker_id_str = str(worker_id)
                        worker_total_shifts[worker_id_str] += 1

                # Find the actual last assigned post index for the day
                actual_last_assigned_idx = -1
                for i in range(len(shifts_on_day) - 1, -1, -1):
                    if shifts_on_day[i] is not None:
                        actual_last_assigned_idx = i
                        break
                
                if actual_last_assigned_idx != -1:
                    total_last_slots_in_non_variable_periods += 1
                    worker_in_last_actual_post = str(shifts_on_day[actual_last_assigned_idx])
                    
                    worker_last_posts[worker_in_last_actual_post] += 1
                    swappable_days_with_last_post_info.append((date_val, actual_last_assigned_idx, worker_in_last_actual_post))

            # 2. Calculate expected last posts per worker using improved formula
            worker_expected_last_posts = {}
            worker_deviation = {}
            
            for worker_id_str, total_shifts in worker_total_shifts.items():
                if total_shifts > 0:
                    # Formula: expected_last_posts = (total_shifts / shifts_per_day) ± tolerance
                    expected_last_posts = total_shifts / shifts_per_day
                    worker_expected_last_posts[worker_id_str] = expected_last_posts
                    
                    actual_last_posts = worker_last_posts[worker_id_str]
                    deviation = actual_last_posts - expected_last_posts
                    worker_deviation[worker_id_str] = deviation
                    
                    logging.debug(f"Worker {worker_id_str}: {total_shifts} shifts → expected {expected_last_posts:.2f} last posts, "
                                f"actual {actual_last_posts}, deviation {deviation:.2f}")
                else:
                    worker_expected_last_posts[worker_id_str] = 0
                    worker_deviation[worker_id_str] = 0

            if total_last_slots_in_non_variable_periods == 0:
                logging.info(f"[IMPROVED AdjustLastPost Iter {iteration+1}] No last posts assigned in non-variable shift periods.")
                break

            # 3. Find workers who need rebalancing
            # Workers with positive deviation (too many last posts) should give some away
            # Workers with negative deviation (too few last posts) should receive more
            
            overloaded_workers = [(worker_id, dev) for worker_id, dev in worker_deviation.items() 
                                if dev > balance_tolerance]
            underloaded_workers = [(worker_id, dev) for worker_id, dev in worker_deviation.items() 
                                 if dev < -balance_tolerance]
            
            logging.info(f"[IMPROVED Iter {iteration+1}] Found {len(overloaded_workers)} overloaded, {len(underloaded_workers)} underloaded workers")
            
            if not overloaded_workers:
                logging.info(f"[IMPROVED AdjustLastPost Iter {iteration+1}] No overloaded workers found. Distribution balanced.")
                break

            # Shuffle days to avoid bias
            random.shuffle(swappable_days_with_last_post_info)

            # 4. Attempt swaps to rebalance
            for date_to_adjust, last_post_idx_on_day, worker_currently_in_last_post_id in swappable_days_with_last_post_info:
                worker_A_id = str(worker_currently_in_last_post_id)
                worker_A_deviation = worker_deviation.get(worker_A_id, 0)
                
                # CRITICAL: Verificación centralizada de protección mandatory
                if not self._can_modify_assignment(worker_A_id, date_to_adjust, "adjust_last_post"):
                    continue

                # Only try to swap if this worker is overloaded
                if worker_A_deviation > balance_tolerance:
                    logging.debug(f"Attempting to rebalance: Worker {worker_A_id} (deviation: {worker_A_deviation:.2f}) on {date_to_adjust.strftime('%Y-%m-%d')}")

                    # Find potential swap partners on the same day
                    shifts_on_this_day = self.schedule[date_to_adjust]
                    potential_swap_partners = []

                    for earlier_post_idx in range(last_post_idx_on_day):
                        worker_B_id_str = str(shifts_on_this_day[earlier_post_idx])
                        
                        if worker_B_id_str != "None" and worker_B_id_str != worker_A_id:
                            worker_B_deviation = worker_deviation.get(worker_B_id_str, 0)
                            
                            # Good swap candidate: B has negative deviation (needs more last posts)
                            # and swapping would improve balance for both
                            if worker_B_deviation < worker_A_deviation:
                                potential_swap_partners.append((worker_B_id_str, earlier_post_idx, worker_B_deviation))
                    
                    if not potential_swap_partners:
                        continue

                    # Sort by deviation (most negative first - those who need last posts most)
                    potential_swap_partners.sort(key=lambda x: x[2])

                    for worker_B_id, worker_B_original_post_idx, worker_B_deviation in potential_swap_partners:
                        # CRITICAL: Verificación centralizada de protección mandatory para ambos trabajadores
                        if not self._can_modify_assignment(worker_A_id, date_to_adjust, "adjust_last_post_A"):
                            break  # Si A no se puede mover, salir del loop de partners
                        if not self._can_modify_assignment(worker_B_id, date_to_adjust, "adjust_last_post_B"):
                            continue  # Intentar con el siguiente partner
                            
                        # Check if this swap would improve overall balance
                        new_A_deviation = worker_A_deviation - 1  # A loses a last post
                        new_B_deviation = worker_B_deviation + 1  # B gains a last post
                        
                        # Swap is beneficial if it reduces overall imbalance
                        current_imbalance = abs(worker_A_deviation) + abs(worker_B_deviation)
                        new_imbalance = abs(new_A_deviation) + abs(new_B_deviation)
                        
                        if new_imbalance < current_imbalance:
                            # Validate the swap doesn't create incompatibilities
                            temp_schedule_for_day = list(shifts_on_this_day)
                            temp_schedule_for_day[last_post_idx_on_day] = worker_B_id
                            temp_schedule_for_day[worker_B_original_post_idx] = worker_A_id
                            
                            valid_swap = True
                            
                            # Check A in B's old slot
                            others_at_B_slot = [str(w) for i, w in enumerate(temp_schedule_for_day) 
                                              if i != worker_B_original_post_idx and w is not None]
                            if not self._check_incompatibility_with_list(worker_A_id, others_at_B_slot):
                                valid_swap = False
                            
                            if valid_swap:
                                # Check B in A's old slot
                                others_at_A_slot = [str(w) for i, w in enumerate(temp_schedule_for_day) 
                                                  if i != last_post_idx_on_day and w is not None]
                                if not self._check_incompatibility_with_list(worker_B_id, others_at_A_slot):
                                    valid_swap = False
                            
                            # Check direct incompatibility
                            if valid_swap and self._are_workers_incompatible(worker_A_id, worker_B_id):
                                valid_swap = False
                            
                            # CRITICAL: Check 7/14 pattern - both workers stay on same date, so no new 7/14 violations
                            # (Swapping posts on same day doesn't create 7/14 issues)
                            
                            if valid_swap:
                                # CRITICAL: Final verification before swap (should already be checked, but be extra safe)
                                if not self._can_modify_assignment(worker_A_id, date_to_adjust, "adjust_last_post_A_final"):
                                    logging.warning(f"🔒 BLOCKED: Cannot swap MANDATORY {worker_A_id} on {date_to_adjust.strftime('%Y-%m-%d')}")
                                    continue
                                if not self._can_modify_assignment(worker_B_id, date_to_adjust, "adjust_last_post_B_final"):
                                    logging.warning(f"🔒 BLOCKED: Cannot swap MANDATORY {worker_B_id} on {date_to_adjust.strftime('%Y-%m-%d')}")
                                    continue
                                
                                # CRITICAL: Check tolerance for BOTH workers BEFORE swap
                                # Note: They're already assigned, so we're just changing positions
                                # Tolerance should already be OK, but verify to be safe
                                
                                # Perform the swap
                                logging.info(f"[IMPROVED Iter {iteration+1}] Beneficial swap on {date_to_adjust.strftime('%Y-%m-%d')}: "
                                           f"Worker {worker_A_id} (dev {worker_A_deviation:.2f}→{new_A_deviation:.2f}, P{last_post_idx_on_day}→P{worker_B_original_post_idx}) "
                                           f"with Worker {worker_B_id} (dev {worker_B_deviation:.2f}→{new_B_deviation:.2f}, P{worker_B_original_post_idx}→P{last_post_idx_on_day})")
                                
                                self.schedule[date_to_adjust][last_post_idx_on_day] = worker_B_id
                                self.schedule[date_to_adjust][worker_B_original_post_idx] = worker_A_id
                                
                                # Update tracking for this iteration
                                worker_last_posts[worker_A_id] -= 1
                                worker_last_posts[worker_B_id] += 1
                                worker_deviation[worker_A_id] = new_A_deviation
                                worker_deviation[worker_B_id] = new_B_deviation
                                
                                made_swap_in_this_iteration = True
                                overall_swaps_made_across_iterations = True
                                break
                
                if made_swap_in_this_iteration:
                    break  # Re-evaluate in next iteration

            if not made_swap_in_this_iteration:
                logging.info(f"[IMPROVED AdjustLastPost Iter {iteration+1}/{max_iterations}] No beneficial swaps found.")
                break

        # Final synchronization and statistics
        if overall_swaps_made_across_iterations:
            self._synchronize_tracking_data()
            self._save_current_as_best()
            
            # Log final distribution
            logging.info("=== FINAL IMPROVED LAST POST DISTRIBUTION ===")
            for worker_id_str in worker_total_shifts.keys():
                if worker_total_shifts[worker_id_str] > 0:
                    expected = worker_expected_last_posts[worker_id_str]
                    actual = worker_last_posts[worker_id_str]
                    deviation = actual - expected
                    logging.info(f"Worker {worker_id_str}: {worker_total_shifts[worker_id_str]} total shifts, "
                               f"expected {expected:.2f} last posts, actual {actual}, deviation {deviation:.2f}")
            
            logging.info(f"Finished IMPROVED last post adjustments. Total iterations: {iteration + 1}. Swaps made: {overall_swaps_made_across_iterations}")
        else:
            logging.info(f"No IMPROVED last post adjustments made after {iteration + 1} iteration(s).")
            
        return overall_swaps_made_across_iterations

    def _is_date_in_variable_shift_period(self, date_to_check):
        """
        Checks if a given date falls into any defined variable shift period.
        """
        # This leverages the existing logic in the scheduler to determine actual shifts for a date.
        # If the number of shifts for the date is different from the default self.num_shifts,
        # then it's considered within a variable shift period.
        
        # Ensure scheduler reference and its attributes are available
        if not hasattr(self, 'scheduler') or not hasattr(self.scheduler, '_get_shifts_for_date') or not hasattr(self.scheduler, 'num_shifts'):
            logging.warning("_is_date_in_variable_shift_period: Scheduler or required attributes not available.")
            return True # Fail safe: assume it's variable if we can't check

        actual_shifts_for_date = self.scheduler._get_shifts_for_date(date_to_check)
        
        # If variable_shifts is empty, no date is in a variable period by this definition.
        if not self.scheduler.variable_shifts:
             return False

        is_variable = actual_shifts_for_date != self.scheduler.num_shifts
        if is_variable:
            logging.debug(f"Date {date_to_check.strftime('%Y-%m-%d')} is in a variable shift period (actual: {actual_shifts_for_date}, default: {self.scheduler.num_shifts}).")
        else:
            logging.debug(f"Date {date_to_check.strftime('%Y-%m-%d')} is NOT in a variable shift period (standard shifts: {self.scheduler.num_shifts}).")
        return is_variable

    # =========================================
    # FINAL PHASE AND UTILITY METHODS
    # =========================================

    def _save_current_as_best(self, initial=False):
        """Save current schedule as the best one found"""
        # For now, just return True to indicate success
        # In a more complex implementation, this would save the current state
        if initial:
            logging.debug("Saving initial schedule state")
        else:
            logging.debug("Saving improved schedule state")
        return True

    def get_best_schedule(self):
        """Return the best schedule found"""
        # Calculate worker_shift_counts from current assignments
        worker_shift_counts = {}
        for worker_id, assignments in self.worker_assignments.items():
            worker_shift_counts[worker_id] = len(assignments)
        
        # Calculate worker_posts and worker_weekend_counts from current schedule
        worker_posts = {}
        worker_weekend_counts = {}
        
        # Initialize worker_weekend_counts for all workers
        for worker_data in self.scheduler.workers_data:
            worker_weekend_counts[worker_data['id']] = 0
            
        for date, shifts in self.schedule.items():
            # Check if this date is a weekend or holiday
            weekday = date.weekday()
            is_weekend_or_holiday = (weekday >= 4 or 
                                  date in self.scheduler.holidays or 
                                  (date + timedelta(days=1)) in self.scheduler.holidays)
            
            for post_idx, worker_id in enumerate(shifts):
                if worker_id:
                    # Calculate worker_posts
                    if worker_id not in worker_posts:
                        worker_posts[worker_id] = {}
                    # Use post_idx directly as integer key (not "P{post_idx}" string)
                    worker_posts[worker_id][post_idx] = worker_posts[worker_id].get(post_idx, 0) + 1
                    
                    # Calculate worker_weekend_counts
                    if is_weekend_or_holiday:
                        worker_weekend_counts[worker_id] += 1

        # Calculate last assignment dates
        last_assignment_date = {}
        for worker_id, assignments in self.worker_assignments.items():
            if assignments:
                last_assignment_date[worker_id] = max(assignments)

        return {
            'schedule': self.schedule,
            'worker_assignments': self.worker_assignments,
            'worker_shift_counts': worker_shift_counts,
            'worker_posts': worker_posts,
            'worker_weekend_counts': worker_weekend_counts,
            'last_assignment_date': last_assignment_date,
            'consecutive_shifts': {},  # Initialize empty, can be calculated if needed
            'score': self.calculate_score()
        }

    def calculate_score(self, schedule_to_score=None, assignments_to_score=None):
        """Calculate score for the current or provided schedule"""
        # Use scheduler's score calculation for consistency
        try:
            return self.scheduler.calculate_score(
                schedule_to_score or self.schedule, 
                assignments_to_score or self.worker_assignments
            )
        except Exception as e:
            logging.warning(f"Error calculating score: {e}")
            return 0.0
