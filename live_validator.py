"""
Live validation engine for real-time constraint checking.
Provides instant validation when schedule changes are made.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
import logging
from dataclasses import dataclass
from enum import Enum

from event_bus import get_event_bus, EventType


class ValidationSeverity(Enum):
    """Severity levels for validation results"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class ValidationResult:
    """Result of a validation check"""
    is_valid: bool
    severity: ValidationSeverity
    message: str
    constraint_type: str
    affected_items: List[str] = None
    suggestions: List[str] = None
    
    def __post_init__(self):
        if self.affected_items is None:
            self.affected_items = []
        if self.suggestions is None:
            self.suggestions = []


@dataclass
class ConflictInfo:
    """Information about a detected conflict"""
    conflict_type: str
    description: str
    severity: ValidationSeverity
    workers_involved: List[str]
    dates_involved: List[datetime]
    resolution_suggestions: List[str] = None
    
    def __post_init__(self):
        if self.resolution_suggestions is None:
            self.resolution_suggestions = []


class LiveValidator:
    """Real-time constraint validation engine"""
    
    def __init__(self, scheduler):
        """
        Initialize the live validator
        
        Args:
            scheduler: The main Scheduler instance
        """
        self.scheduler = scheduler
        self.event_bus = get_event_bus()
        
        # Validation caches for performance
        self._constraint_cache: Dict[str, ValidationResult] = {}
        self._conflict_cache: Dict[str, List[ConflictInfo]] = {}
        
        # Subscribe to relevant events
        self._setup_event_listeners()
        
        logging.info("LiveValidator initialized")
    
    def _setup_event_listeners(self):
        """Set up event listeners for automatic validation"""
        self.event_bus.subscribe(EventType.SHIFT_ASSIGNED, self._on_shift_assigned)
        self.event_bus.subscribe(EventType.SHIFT_UNASSIGNED, self._on_shift_unassigned)
        self.event_bus.subscribe(EventType.SHIFT_SWAPPED, self._on_shift_swapped)
    
    def validate_assignment(self, worker_id: str, shift_date: datetime, post_index: int) -> ValidationResult:
        """
        Validate a potential worker assignment with data synchronization checks.
        
        Args:
            worker_id: ID of worker to validate
            shift_date: Date of the shift
            post_index: Post index (0-based)
            
        Returns:
            ValidationResult with validation details
        """
        try:
            # ENHANCED: Ensure data synchronization before validation
            if hasattr(self.scheduler, '_ensure_data_synchronization'):
                if not self.scheduler._ensure_data_synchronization():
                    return ValidationResult(
                        is_valid=False,
                        severity=ValidationSeverity.ERROR,
                        message="Data synchronization issues detected before validation",
                        constraint_type="data_synchronization",
                        suggestions=["Run schedule rebuild to fix synchronization issues"]
                    )
            
            # Check basic availability
            availability_result = self._check_worker_availability(worker_id, shift_date)
            if not availability_result.is_valid:
                return availability_result
            
            # Check incompatibility constraints
            incompatibility_result = self._check_incompatibility_constraints(worker_id, shift_date)
            if not incompatibility_result.is_valid:
                return incompatibility_result
            
            # Check gap constraints
            gap_result = self._check_gap_constraints(worker_id, shift_date)
            if not gap_result.is_valid:
                return gap_result
            
            # Check weekend/holiday limits
            weekend_result = self._check_weekend_limits(worker_id, shift_date)
            if not weekend_result.is_valid:
                return weekend_result
            
            # Check workload balance
            workload_result = self._check_workload_balance(worker_id, shift_date)
            # Note: Workload imbalance is usually a warning, not an error
            
            return ValidationResult(
                is_valid=True,
                severity=ValidationSeverity.INFO,
                message="Assignment is valid",
                constraint_type="all",
                suggestions=[]  # Simplified for now
            )
            
        except Exception as e:
            logging.error(f"Error validating assignment: {e}")
            return ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Validation error: {str(e)}",
                constraint_type="system_error"
            )
    
    def validate_schedule_integrity(self, check_partial: bool = False) -> List[ValidationResult]:
        """
        Validate the entire schedule for integrity including data synchronization
        
        Args:
            check_partial: If True, validate even partially filled schedules
            
        Returns:
            List of validation results
        """
        results = []
        
        try:
            # ENHANCED: First check data synchronization
            sync_result = self._check_data_synchronization()
            results.append(sync_result)
            
            # Check for constraint violations
            constraint_violations = self._find_all_constraint_violations()
            results.extend(constraint_violations)
            
            # Check for schedule completeness
            if not check_partial:
                completeness_result = self._check_schedule_completeness()
                results.append(completeness_result)
            
            # Check workload distribution
            distribution_results = self._check_workload_distribution()
            results.extend(distribution_results)
            
            # Check schedule balance
            balance_results = self._check_schedule_balance()
            results.extend(balance_results)
            
            # Emit validation event
            self.event_bus.emit(
                EventType.VALIDATION_RESULT,
                total_checks=len(results),
                errors=len([r for r in results if r.severity == ValidationSeverity.ERROR]),
                warnings=len([r for r in results if r.severity == ValidationSeverity.WARNING])
            )
            
            return results
            
        except Exception as e:
            logging.error(f"Error validating schedule integrity: {e}")
            return [ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Schedule validation failed: {str(e)}",
                constraint_type="system_error"
            )]
    
    def _check_data_synchronization(self) -> ValidationResult:
        """
        Check data synchronization between worker_assignments and schedule
        
        Returns:
            ValidationResult with synchronization status
        """
        try:
            if hasattr(self.scheduler, '_validate_data_synchronization'):
                is_synchronized, validation_report = self.scheduler._validate_data_synchronization()
                
                if is_synchronized:
                    return ValidationResult(
                        is_valid=True,
                        severity=ValidationSeverity.INFO,
                        message="Data structures are properly synchronized",
                        constraint_type="data_synchronization"
                    )
                else:
                    # Count issues
                    summary = validation_report.get('summary', {})
                    workers_with_issues = summary.get('workers_with_issues', 0)
                    missing_count = summary.get('missing_from_tracking', 0)
                    extra_count = summary.get('extra_in_tracking', 0)
                    
                    return ValidationResult(
                        is_valid=False,
                        severity=ValidationSeverity.ERROR,
                        message=f"Data synchronization issues: {workers_with_issues} workers affected, {missing_count} missing, {extra_count} extra assignments",
                        constraint_type="data_synchronization",
                        suggestions=[
                            "Run schedule rebuild to fix synchronization",
                            "Check for assignment/unassignment operations that might not update both structures"
                        ]
                    )
            else:
                return ValidationResult(
                    is_valid=True,
                    severity=ValidationSeverity.WARNING,
                    message="Data synchronization validation not available",
                    constraint_type="data_synchronization"
                )
                
        except Exception as e:
            logging.error(f"Error checking data synchronization: {e}")
            return ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Synchronization check failed: {str(e)}",
                constraint_type="data_synchronization"
            )
    
    def detect_conflicts(self, date_range: Optional[Tuple[datetime, datetime]] = None) -> List[ConflictInfo]:
        """
        Detect conflicts in the schedule
        
        Args:
            date_range: Optional date range to check (start, end)
            
        Returns:
            List of detected conflicts
        """
        conflicts = []
        
        try:
            # Get date range to check
            if date_range:
                start_date, end_date = date_range
            else:
                start_date = self.scheduler.start_date
                end_date = self.scheduler.end_date
            
            # Check for incompatible workers on same shifts
            incompatibility_conflicts = self._detect_incompatibility_conflicts(start_date, end_date)
            conflicts.extend(incompatibility_conflicts)
            
            # Check for gap violations
            gap_conflicts = self._detect_gap_violations(start_date, end_date)
            conflicts.extend(gap_conflicts)
            
            # Check for weekend limit violations
            weekend_conflicts = self._detect_weekend_violations(start_date, end_date)
            conflicts.extend(weekend_conflicts)
            
            # Check for workload imbalances
            workload_conflicts = self._detect_workload_imbalances()
            conflicts.extend(workload_conflicts)
            
            return conflicts
            
        except Exception as e:
            logging.error(f"Error detecting conflicts: {e}")
            return []
    
    def get_suggestions_for_date(self, shift_date: datetime, post_index: int) -> List[str]:
        """
        Get worker suggestions for a specific date and post
        
        Args:
            shift_date: Date of the shift
            post_index: Post index
            
        Returns:
            List of suggested workers
        """
        suggestions = []
        
        try:
            for worker in self.scheduler.workers_data:
                worker_id = worker['id']
                
                # Skip if worker is already assigned on this date
                if shift_date in self.scheduler.worker_assignments.get(worker_id, set()):
                    continue
                
                # Validate the assignment
                result = self.validate_assignment(worker_id, shift_date, post_index)
                
                if result.is_valid:
                    # Calculate priority based on various factors
                    priority = self._calculate_assignment_priority(worker_id, shift_date)
                    suggestions.append({
                        'worker_id': worker_id,
                        'priority': priority,
                        'reason': self._get_suggestion_reason(worker_id, shift_date)
                    })
                elif result.severity == ValidationSeverity.WARNING:
                    # Include with lower priority if only warnings
                    priority = self._calculate_assignment_priority(worker_id, shift_date) * 0.5
                    suggestions.append({
                        'worker_id': worker_id,
                        'priority': priority,
                        'reason': f"Available (with warnings): {result.message}"
                    })
            
            # Sort by priority
            suggestions.sort(key=lambda x: x['priority'], reverse=True)
            
            return [s['worker_id'] + ': ' + s['reason'] for s in suggestions[:5]]
            
        except Exception as e:
            logging.error(f"Error generating suggestions: {e}")
            return []
    
    def _check_worker_availability(self, worker_id: str, shift_date: datetime) -> ValidationResult:
        """Check if worker is available on the given date"""
        # Check if worker is already assigned
        if shift_date in self.scheduler.worker_assignments.get(worker_id, set()):
            return ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Worker {worker_id} is already assigned on {shift_date.strftime('%Y-%m-%d')}",
                constraint_type="double_assignment"
            )
        
        # Check days off and work periods using existing constraint checker
        if hasattr(self.scheduler, 'constraint_checker'):
            if self.scheduler.constraint_checker._is_worker_unavailable(worker_id, shift_date):
                return ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"Worker {worker_id} is unavailable on {shift_date.strftime('%Y-%m-%d')}",
                    constraint_type="unavailable"
                )
        
        return ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Worker is available",
            constraint_type="availability"
        )
    
    def _check_incompatibility_constraints(self, worker_id: str, shift_date: datetime) -> ValidationResult:
        """Check for incompatible workers on the same date"""
        if shift_date not in self.scheduler.schedule:
            return ValidationResult(
                is_valid=True,
                severity=ValidationSeverity.INFO,
                message="No incompatibility issues",
                constraint_type="incompatibility"
            )
        
        # Get workers already assigned on this date
        assigned_workers = [w for w in self.scheduler.schedule[shift_date] if w is not None]
        
        # Check incompatibilities using existing constraint checker
        if hasattr(self.scheduler, 'constraint_checker'):
            for assigned_worker in assigned_workers:
                if self.scheduler.constraint_checker._are_workers_incompatible(worker_id, assigned_worker):
                    return ValidationResult(
                        is_valid=False,
                        severity=ValidationSeverity.ERROR,
                        message=f"Worker {worker_id} is incompatible with {assigned_worker}",
                        constraint_type="incompatibility",
                        affected_items=[assigned_worker]
                    )
        
        return ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="No incompatibility issues",
            constraint_type="incompatibility"
        )
    
    def _check_gap_constraints(self, worker_id: str, shift_date: datetime) -> ValidationResult:
        """Check minimum gap between shifts including 7/14 day pattern and Friday-Monday rules"""
        worker_assignments = self.scheduler.worker_assignments.get(worker_id, set())
        
        # Get worker data for part-time adjustments
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        if not worker_data:
            return ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Worker {worker_id} not found",
                constraint_type="gap_constraint"
            )
        
        work_percentage = worker_data.get('work_percentage', 100)
        
        # Determine minimum days required between shifts
        # FIXED: gap_between_shifts is the actual minimum days between shifts
        # If gap_between_shifts=0, workers can work consecutive days (days_between=1 is OK)
        # If gap_between_shifts=1, workers need at least 1 day gap (days_between=2 minimum)
        min_required_days_between = self.scheduler.gap_between_shifts + 1
        
        # Part-time workers might need a larger gap
        if work_percentage < 70:
            min_required_days_between = max(min_required_days_between, self.scheduler.gap_between_shifts + 2)
        
        # If gap_between_shifts is 0, allow consecutive work (no gap validation needed)
        if self.scheduler.gap_between_shifts == 0:
            return ValidationResult(
                is_valid=True,
                severity=ValidationSeverity.INFO,
                message="Gap constraint not applicable (consecutive work allowed)",
                constraint_type="gap_constraint"
            )
        
        for assigned_date in worker_assignments:
            days_between = abs((shift_date - assigned_date).days)
            
            # Basic gap check
            if days_between < min_required_days_between:
                return ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"Gap constraint violated: {days_between} days from {assigned_date.strftime('%Y-%m-%d')} (minimum: {min_required_days_between})",
                    constraint_type="gap_constraint",
                    affected_items=[assigned_date.strftime('%Y-%m-%d')]
                )
            
            # Friday-Monday rule: only apply if base gap allows for 3-day difference
            if self.scheduler.gap_between_shifts <= 1:
                if days_between == 3:
                    if ((assigned_date.weekday() == 4 and shift_date.weekday() == 0) or 
                        (shift_date.weekday() == 4 and assigned_date.weekday() == 0)):
                        return ValidationResult(
                            is_valid=False,
                            severity=ValidationSeverity.ERROR,
                            message=f"Friday-Monday rule violated: {days_between} days from {assigned_date.strftime('%Y-%m-%d')} (Friday-Monday not allowed)",
                            constraint_type="gap_constraint",
                            affected_items=[assigned_date.strftime('%Y-%m-%d')]
                        )
            
            # 7/14 day pattern check: prevent same weekday assignments exactly 7 or 14 days apart
            # IMPORTANT: This constraint only applies to regular weekdays (Mon-Thu), 
            # NOT to weekend days (Fri-Sun) where consecutive assignments are normal
            if (days_between == 7 or days_between == 14) and shift_date.weekday() == assigned_date.weekday():
                # Allow weekend days to be assigned on same weekday 7/14 days apart
                if shift_date.weekday() >= 4 or assigned_date.weekday() >= 4:  # Fri, Sat, Sun
                    continue  # Skip this constraint for weekend days
                return ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"7/14 day pattern violated: {days_between} days from {assigned_date.strftime('%Y-%m-%d')} (same weekday not allowed)",
                    constraint_type="gap_constraint",
                    affected_items=[assigned_date.strftime('%Y-%m-%d')]
                )
        
        return ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Gap constraints satisfied",
            constraint_type="gap_constraint"
        )
    
    def _check_weekend_limits(self, worker_id: str, shift_date: datetime) -> ValidationResult:
        """Check weekend/holiday shift limits for existing assignments"""
        # Check if this is a weekend/holiday day using the same logic as constraint checker
        is_weekend_or_holiday = (
            shift_date.weekday() >= 4 or  # Friday, Saturday, Sunday
            shift_date in self.scheduler.holidays or
            (shift_date + timedelta(days=1)) in self.scheduler.holidays  # Day before holiday
        )
        
        if not is_weekend_or_holiday:
            return ValidationResult(
                is_valid=True,
                severity=ValidationSeverity.INFO,
                message="Not a weekend/holiday shift",
                constraint_type="weekend_limit"
            )
        
        # For existing assignments, we need to check if the current allocation is valid
        # without double-counting the date being validated
        if hasattr(self.scheduler, 'constraint_checker'):
            # Check if this worker is already assigned on this date
            current_assignments = self.scheduler.worker_assignments.get(worker_id, set())
            is_already_assigned = shift_date in current_assignments
            
            if is_already_assigned:
                # For existing assignments, temporarily remove the date and check if re-adding would be valid
                # This prevents double-counting in the constraint checker
                temp_assignments = current_assignments.copy()
                temp_assignments.discard(shift_date)
                original_assignments = self.scheduler.worker_assignments[worker_id]
                self.scheduler.worker_assignments[worker_id] = temp_assignments
                
                try:
                    would_exceed = self.scheduler.constraint_checker._would_exceed_weekend_limit(worker_id, shift_date)
                finally:
                    # Restore original assignments
                    self.scheduler.worker_assignments[worker_id] = original_assignments
                
                if would_exceed:
                    return ValidationResult(
                        is_valid=False,
                        severity=ValidationSeverity.ERROR,
                        message=f"Weekend/holiday limit exceeded for worker {worker_id}",
                        constraint_type="weekend_limit"
                    )
            else:
                # For new assignments, use the normal check
                if self.scheduler.constraint_checker._would_exceed_weekend_limit(worker_id, shift_date):
                    return ValidationResult(
                        is_valid=False,
                        severity=ValidationSeverity.ERROR,
                        message=f"Weekend/holiday limit would be exceeded for worker {worker_id}",
                        constraint_type="weekend_limit"
                    )
        
        return ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Weekend/holiday limits satisfied",
            constraint_type="weekend_limit"
        )
    
    def _check_workload_balance(self, worker_id: str, shift_date: datetime) -> ValidationResult:
        """Check if assignment maintains workload balance"""
        current_assignments = len(self.scheduler.worker_assignments.get(worker_id, set()))
        
        # Calculate target assignments based on work percentage
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        if worker_data:
            work_percentage = worker_data.get('work_percentage', 100)
            # This is a simplified check - could be more sophisticated
            if work_percentage < 100 and current_assignments > 10:  # Arbitrary threshold
                return ValidationResult(
                    is_valid=True,
                    severity=ValidationSeverity.WARNING,
                    message=f"Part-time worker {worker_id} has high workload",
                    constraint_type="workload_balance"
                )
        
        return ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Workload balance acceptable",
            constraint_type="workload_balance"
        )
    
    def _find_all_constraint_violations(self) -> List[ValidationResult]:
        """Find all constraint violations in the current schedule"""
        violations = []
        
        # Track workers per day to detect double assignments
        daily_assignments = {}
        
        for date, shifts in self.scheduler.schedule.items():
            if date not in daily_assignments:
                daily_assignments[date] = []
            
            for post_index, worker_id in enumerate(shifts):
                if worker_id is not None:
                    # Check for double assignments on the same day
                    if worker_id in daily_assignments[date]:
                        violations.append(ValidationResult(
                            is_valid=False,
                            severity=ValidationSeverity.ERROR,
                            message=f"Worker {worker_id} assigned multiple times on {date.strftime('%Y-%m-%d')}",
                            constraint_type="double_assignment"
                        ))
                    else:
                        daily_assignments[date].append(worker_id)
                    
                    # Validate other constraints (availability, gaps, incompatibility)
                    result = self._validate_existing_assignment(worker_id, date, post_index)
                    if not result.is_valid:
                        violations.append(result)
        
        return violations
    
    def _validate_existing_assignment(self, worker_id: str, shift_date: datetime, post_index: int) -> ValidationResult:
        """
        Validate an existing assignment without checking if worker is already assigned
        (since we're validating existing assignments)
        """
        try:
            # ENHANCED: Ensure data synchronization before validation
            if hasattr(self.scheduler, '_ensure_data_synchronization'):
                if not self.scheduler._ensure_data_synchronization():
                    return ValidationResult(
                        is_valid=False,
                        severity=ValidationSeverity.ERROR,
                        message="Data synchronization issues detected before validation",
                        constraint_type="data_synchronization",
                        suggestions=["Run schedule rebuild to fix synchronization issues"]
                    )
            
            # Check basic availability (without double assignment check)
            availability_result = self._check_worker_basic_availability(worker_id, shift_date)
            if not availability_result.is_valid:
                return availability_result
            
            # Check incompatibility constraints
            incompatibility_result = self._check_incompatibility_constraints(worker_id, shift_date)
            if not incompatibility_result.is_valid:
                return incompatibility_result
            
            # Check gap constraints
            gap_result = self._check_gap_constraints(worker_id, shift_date)
            if not gap_result.is_valid:
                return gap_result
            
            # Check weekend limits
            weekend_result = self._check_weekend_limits(worker_id, shift_date)
            if not weekend_result.is_valid:
                return weekend_result
            
            # Check workload balance
            workload_result = self._check_workload_balance(worker_id, shift_date)
            if not workload_result.is_valid:
                return workload_result
                
            return ValidationResult(
                is_valid=True,
                severity=ValidationSeverity.INFO,
                message="Assignment is valid",
                constraint_type="general"
            )
            
        except Exception as e:
            logging.error(f"Error validating existing assignment: {e}")
            return ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Validation error: {str(e)}",
                constraint_type="system_error"
            )
    
    def _check_worker_basic_availability(self, worker_id: str, shift_date: datetime) -> ValidationResult:
        """Check basic worker availability without double assignment check"""
        # Check days off and work periods using existing constraint checker
        if hasattr(self.scheduler, 'constraint_checker'):
            if self.scheduler.constraint_checker._is_worker_unavailable(worker_id, shift_date):
                return ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"Worker {worker_id} is unavailable on {shift_date.strftime('%Y-%m-%d')}",
                    constraint_type="unavailable"
                )
        
        return ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Worker is available",
            constraint_type="availability"
        )
    
    def _check_schedule_completeness(self) -> ValidationResult:
        """Check if schedule is complete"""
        total_slots = 0
        filled_slots = 0
        
        for date, shifts in self.scheduler.schedule.items():
            total_slots += len(shifts)
            filled_slots += len([s for s in shifts if s is not None])
        
        if filled_slots == 0:
            return ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Schedule is empty",
                constraint_type="completeness"
            )
        
        coverage_percentage = (filled_slots / total_slots) * 100 if total_slots > 0 else 0
        
        if coverage_percentage < 90:
            return ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.WARNING,
                message=f"Schedule coverage is low: {coverage_percentage:.1f}%",
                constraint_type="completeness"
            )
        
        return ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message=f"Schedule coverage: {coverage_percentage:.1f}%",
            constraint_type="completeness"
        )
    
    def _check_workload_distribution(self) -> List[ValidationResult]:
        """Check workload distribution across workers"""
        results = []
        
        # Calculate workload statistics
        workloads = {}
        for worker_id, assignments in self.scheduler.worker_assignments.items():
            workloads[worker_id] = len(assignments)
        
        if not workloads:
            return results
        
        avg_workload = sum(workloads.values()) / len(workloads)
        max_workload = max(workloads.values())
        min_workload = min(workloads.values())
        
        # Check for significant imbalances
        threshold = avg_workload * 0.3  # 30% deviation threshold
        
        for worker_id, workload in workloads.items():
            if abs(workload - avg_workload) > threshold:
                severity = ValidationSeverity.WARNING if abs(workload - avg_workload) < threshold * 2 else ValidationSeverity.ERROR
                results.append(ValidationResult(
                    is_valid=severity != ValidationSeverity.ERROR,
                    severity=severity,
                    message=f"Worker {worker_id} workload imbalance: {workload} vs avg {avg_workload:.1f}",
                    constraint_type="workload_distribution",
                    affected_items=[worker_id]
                ))
        
        return results
    
    def _check_schedule_balance(self) -> List[ValidationResult]:
        """Check various aspects of schedule balance"""
        results = []
        
        # Add more balance checks as needed
        # This is a placeholder for future balance validations
        
        return results
    
    def _detect_incompatibility_conflicts(self, start_date: datetime, end_date: datetime) -> List[ConflictInfo]:
        """Detect incompatible workers assigned to same shifts"""
        conflicts = []
        
        current_date = start_date
        while current_date <= end_date:
            if current_date in self.scheduler.schedule:
                assigned_workers = [w for w in self.scheduler.schedule[current_date] if w is not None]
                
                # Check all pairs of assigned workers
                for i, worker1 in enumerate(assigned_workers):
                    for worker2 in assigned_workers[i+1:]:
                        if hasattr(self.scheduler, 'constraint_checker'):
                            if self.scheduler.constraint_checker._are_workers_incompatible(worker1, worker2):
                                conflicts.append(ConflictInfo(
                                    conflict_type="incompatibility",
                                    description=f"Incompatible workers {worker1} and {worker2} on same date",
                                    severity=ValidationSeverity.ERROR,
                                    workers_involved=[worker1, worker2],
                                    dates_involved=[current_date],
                                    resolution_suggestions=[
                                        f"Reassign {worker1} to a different date",
                                        f"Reassign {worker2} to a different date"
                                    ]
                                ))
            
            current_date += timedelta(days=1)
        
        return conflicts
    
    def _detect_gap_violations(self, start_date: datetime, end_date: datetime) -> List[ConflictInfo]:
        """Detect gap constraint violations"""
        conflicts = []
        
        for worker_id, assignments in self.scheduler.worker_assignments.items():
            assignments_list = sorted(list(assignments))
            
            for i in range(len(assignments_list) - 1):
                gap = (assignments_list[i+1] - assignments_list[i]).days
                if 0 < gap < self.scheduler.gap_between_shifts:
                    conflicts.append(ConflictInfo(
                        conflict_type="gap_violation",
                        description=f"Worker {worker_id} has {gap} day gap (minimum: {self.scheduler.gap_between_shifts})",
                        severity=ValidationSeverity.ERROR,
                        workers_involved=[worker_id],
                        dates_involved=[assignments_list[i], assignments_list[i+1]],
                        resolution_suggestions=[
                            f"Reassign worker {worker_id} from one of the conflicting dates"
                        ]
                    ))
        
        return conflicts
    
    def _detect_weekend_violations(self, start_date: datetime, end_date: datetime) -> List[ConflictInfo]:
        """Detect weekend/holiday limit violations"""
        conflicts = []
        
        # This would implement weekend limit checking
        # Implementation depends on the specific weekend limit logic
        
        return conflicts
    
    def _detect_workload_imbalances(self) -> List[ConflictInfo]:
        """Detect significant workload imbalances"""
        conflicts = []
        
        workloads = {worker_id: len(assignments) 
                    for worker_id, assignments in self.scheduler.worker_assignments.items()}
        
        if not workloads:
            return conflicts
        
        avg_workload = sum(workloads.values()) / len(workloads)
        
        for worker_id, workload in workloads.items():
            deviation = abs(workload - avg_workload)
            if deviation > avg_workload * 0.5:  # 50% deviation threshold
                conflicts.append(ConflictInfo(
                    conflict_type="workload_imbalance",
                    description=f"Worker {worker_id} has significant workload imbalance: {workload} vs avg {avg_workload:.1f}",
                    severity=ValidationSeverity.WARNING,
                    workers_involved=[worker_id],
                    dates_involved=[],
                    resolution_suggestions=[
                        f"Redistribute shifts to balance workload for worker {worker_id}"
                    ]
                ))
        
        return conflicts
    
    def _calculate_assignment_priority(self, worker_id: str, shift_date: datetime) -> float:
        """Calculate priority score for assignment suggestion"""
        priority = 1.0
        
        # Factor in current workload (prefer less loaded workers)
        current_workload = len(self.scheduler.worker_assignments.get(worker_id, set()))
        avg_workload = sum(len(assignments) for assignments in self.scheduler.worker_assignments.values()) / len(self.scheduler.worker_assignments)
        if current_workload < avg_workload:
            priority += 0.5
        
        # Factor in work percentage
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        if worker_data:
            work_percentage = worker_data.get('work_percentage', 100)
            priority *= work_percentage / 100
        
        # Factor in recent assignments (prefer workers with more gap)
        worker_assignments = self.scheduler.worker_assignments.get(worker_id, set())
        if worker_assignments:
            last_assignment = max(worker_assignments)
            days_since_last = (shift_date - last_assignment).days
            if days_since_last > self.scheduler.gap_between_shifts:
                priority += 0.3
        else:
            priority += 0.5  # Prefer workers with no assignments
        
        return priority
    
    def _get_suggestion_reason(self, worker_id: str, shift_date: datetime) -> str:
        """Get human-readable reason for suggestion"""
        current_workload = len(self.scheduler.worker_assignments.get(worker_id, set()))
        
        if current_workload == 0:
            return "No current assignments"
        
        worker_assignments = self.scheduler.worker_assignments.get(worker_id, set())
        if worker_assignments:
            last_assignment = max(worker_assignments)
            days_since_last = (shift_date - last_assignment).days
            return f"Good gap: {days_since_last} days since last shift"
        
        return "Available and suitable"
    
    def _on_shift_assigned(self, event):
        """Handle shift assignment events"""
        # Clear relevant caches
        self._constraint_cache.clear()
        self._conflict_cache.clear()
        
        # Validate the assignment
        worker_id = event.data.get('worker_id')
        shift_date = datetime.fromisoformat(event.data.get('shift_date'))
        post_index = event.data.get('post_index')
        
        if worker_id and shift_date is not None and post_index is not None:
            result = self.validate_assignment(worker_id, shift_date, post_index)
            if not result.is_valid:
                self.event_bus.emit(
                    EventType.CONSTRAINT_VIOLATION,
                    worker_id=worker_id,
                    shift_date=shift_date.isoformat(),
                    post_index=post_index,
                    violation_type=result.constraint_type,
                    message=result.message
                )
    
    def _on_shift_unassigned(self, event):
        """Handle shift unassignment events"""
        # Clear relevant caches
        self._constraint_cache.clear()
        self._conflict_cache.clear()
    
    def _on_shift_swapped(self, event):
        """Handle shift swap events"""
        # Clear relevant caches
        self._constraint_cache.clear()
        self._conflict_cache.clear()
        
        # Validate both new assignments
        worker1 = event.data.get('worker1')
        worker2 = event.data.get('worker2')
        
        if worker1:
            shift_date2 = datetime.fromisoformat(event.data.get('shift_date2'))
            post_index2 = event.data.get('post_index2')
            result = self.validate_assignment(worker1, shift_date2, post_index2)
            if not result.is_valid:
                self.event_bus.emit(
                    EventType.CONSTRAINT_VIOLATION,
                    worker_id=worker1,
                    shift_date=shift_date2.isoformat(),
                    post_index=post_index2,
                    violation_type=result.constraint_type,
                    message=result.message
                )
        
        if worker2:
            shift_date1 = datetime.fromisoformat(event.data.get('shift_date1'))
            post_index1 = event.data.get('post_index1')
            result = self.validate_assignment(worker2, shift_date1, post_index1)
            if not result.is_valid:
                self.event_bus.emit(
                    EventType.CONSTRAINT_VIOLATION,
                    worker_id=worker2,
                    shift_date=shift_date1.isoformat(),
                    post_index=post_index1,
                    violation_type=result.constraint_type,
                    message=result.message
                )
