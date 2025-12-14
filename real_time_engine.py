"""
Real-time processing engine for schedule operations.
Coordinates all real-time features and provides unified interface.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Callable
import logging
from dataclasses import dataclass
from threading import Lock, Thread
import time

from event_bus import get_event_bus, EventType, ScheduleEvent
from incremental_updater import IncrementalUpdater, UpdateResult
from live_validator import LiveValidator, ValidationResult, ConflictInfo
from change_tracker import ChangeTracker
from exceptions import SchedulerError


@dataclass
class RealTimeOperationResult:
    """Result of a real-time operation"""
    success: bool
    message: str
    operation_id: str
    validation_results: List[ValidationResult] = None
    conflicts: List[ConflictInfo] = None
    suggestions: List[str] = None
    
    def __post_init__(self):
        if self.validation_results is None:
            self.validation_results = []
        if self.conflicts is None:
            self.conflicts = []
        if self.suggestions is None:
            self.suggestions = []


class RealTimeEngine:
    """Core real-time processing engine that coordinates all real-time features"""
    
    def __init__(self, scheduler):
        """
        Initialize the real-time engine
        
        Args:
            scheduler: The main Scheduler instance
        """
        self.scheduler = scheduler
        self.event_bus = get_event_bus()
        
        # Initialize real-time components
        self.incremental_updater = IncrementalUpdater(scheduler)
        self.live_validator = LiveValidator(scheduler)
        self.change_tracker = ChangeTracker(scheduler)
        
        # Real-time state management
        self._active_operations: Dict[str, Any] = {}
        self._operation_lock = Lock()
        self._performance_metrics: Dict[str, Any] = {}
        
        # Setup event listeners
        self._setup_event_listeners()
        
        logging.info("RealTimeEngine initialized")
    
    def _setup_event_listeners(self):
        """Set up event listeners for real-time monitoring"""
        self.event_bus.subscribe(EventType.CONSTRAINT_VIOLATION, self._on_constraint_violation)
        self.event_bus.subscribe(EventType.VALIDATION_RESULT, self._on_validation_result)
    
    def assign_worker_real_time(self, 
                               worker_id: str, 
                               shift_date: datetime, 
                               post_index: int,
                               user_id: Optional[str] = None,
                               validate: bool = True,
                               force: bool = False) -> RealTimeOperationResult:
        """
        Assign worker to shift with real-time validation and feedback
        
        Args:
            worker_id: ID of worker to assign
            shift_date: Date of the shift
            post_index: Post index (0-based)
            user_id: ID of user making the change
            validate: Whether to perform validation
            force: Skip constraint validation if True
            
        Returns:
            RealTimeOperationResult with comprehensive feedback
        """
        operation_id = f"assign_{worker_id}_{shift_date.strftime('%Y%m%d')}_{post_index}_{datetime.now().timestamp()}"
        
        try:
            with self._operation_lock:
                self._active_operations[operation_id] = {
                    'type': 'assignment',
                    'start_time': datetime.now(),
                    'user_id': user_id
                }
            
            # Pre-validation if requested
            validation_results = []
            conflicts = []
            suggestions = []
            
            if validate and not force:
                validation_result = self.live_validator.validate_assignment(worker_id, shift_date, post_index)
                validation_results.append(validation_result)
                
                if not validation_result.is_valid:
                    # Get suggestions for alternatives
                    suggestions = self.live_validator.get_suggestions_for_date(shift_date, post_index)
                    
                    return RealTimeOperationResult(
                        success=False,
                        message=f"Assignment validation failed: {validation_result.message}",
                        operation_id=operation_id,
                        validation_results=validation_results,
                        suggestions=suggestions
                    )
            
            # Perform the assignment
            update_result = self.incremental_updater.assign_worker_to_shift(
                worker_id, shift_date, post_index, user_id, force
            )
            
            if update_result.success:
                # Post-assignment validation and conflict detection
                if validate:
                    post_conflicts = self.live_validator.detect_conflicts((shift_date, shift_date))
                    conflicts.extend(post_conflicts)
                
                # Get optimization suggestions
                suggestions = self._get_optimization_suggestions(shift_date)
                
                return RealTimeOperationResult(
                    success=True,
                    message=update_result.message,
                    operation_id=operation_id,
                    validation_results=validation_results,
                    conflicts=conflicts,
                    suggestions=suggestions
                )
            else:
                return RealTimeOperationResult(
                    success=False,
                    message=update_result.message,
                    operation_id=operation_id,
                    validation_results=validation_results,
                    conflicts=update_result.conflicts if hasattr(update_result, 'conflicts') else [],
                    suggestions=update_result.suggestions if hasattr(update_result, 'suggestions') else []
                )
                
        except Exception as e:
            logging.error(f"Error in real-time assignment: {e}")
            return RealTimeOperationResult(
                success=False,
                message=f"Assignment failed: {str(e)}",
                operation_id=operation_id
            )
        finally:
            with self._operation_lock:
                if operation_id in self._active_operations:
                    end_time = datetime.now()
                    duration = (end_time - self._active_operations[operation_id]['start_time']).total_seconds()
                    self._update_performance_metrics('assignment', duration)
                    del self._active_operations[operation_id]
    
    def unassign_worker_real_time(self, 
                                 shift_date: datetime, 
                                 post_index: int,
                                 user_id: Optional[str] = None) -> RealTimeOperationResult:
        """
        Unassign worker from shift with real-time feedback
        
        Args:
            shift_date: Date of the shift
            post_index: Post index (0-based)
            user_id: ID of user making the change
            
        Returns:
            RealTimeOperationResult with feedback
        """
        operation_id = f"unassign_{shift_date.strftime('%Y%m%d')}_{post_index}_{datetime.now().timestamp()}"
        
        try:
            with self._operation_lock:
                self._active_operations[operation_id] = {
                    'type': 'unassignment',
                    'start_time': datetime.now(),
                    'user_id': user_id
                }
            
            # Perform the unassignment
            update_result = self.incremental_updater.unassign_worker_from_shift(
                shift_date, post_index, user_id
            )
            
            if update_result.success:
                # Get suggestions for replacement
                suggestions = self.live_validator.get_suggestions_for_date(shift_date, post_index)
                
                return RealTimeOperationResult(
                    success=True,
                    message=update_result.message,
                    operation_id=operation_id,
                    suggestions=suggestions
                )
            else:
                return RealTimeOperationResult(
                    success=False,
                    message=update_result.message,
                    operation_id=operation_id
                )
                
        except Exception as e:
            logging.error(f"Error in real-time unassignment: {e}")
            return RealTimeOperationResult(
                success=False,
                message=f"Unassignment failed: {str(e)}",
                operation_id=operation_id
            )
        finally:
            with self._operation_lock:
                if operation_id in self._active_operations:
                    end_time = datetime.now()
                    duration = (end_time - self._active_operations[operation_id]['start_time']).total_seconds()
                    self._update_performance_metrics('unassignment', duration)
                    del self._active_operations[operation_id]
    
    def swap_workers_real_time(self, 
                              shift_date1: datetime, post_index1: int,
                              shift_date2: datetime, post_index2: int,
                              user_id: Optional[str] = None,
                              validate: bool = True,
                              force: bool = False) -> RealTimeOperationResult:
        """
        Swap workers between shifts with real-time validation
        
        Args:
            shift_date1: Date of first shift
            post_index1: Post index of first shift
            shift_date2: Date of second shift
            post_index2: Post index of second shift
            user_id: ID of user making the change
            validate: Whether to perform validation
            force: Skip constraint validation if True
            
        Returns:
            RealTimeOperationResult with feedback
        """
        operation_id = f"swap_{shift_date1.strftime('%Y%m%d')}_{post_index1}_{shift_date2.strftime('%Y%m%d')}_{post_index2}_{datetime.now().timestamp()}"
        
        try:
            with self._operation_lock:
                self._active_operations[operation_id] = {
                    'type': 'swap',
                    'start_time': datetime.now(),
                    'user_id': user_id
                }
            
            validation_results = []
            conflicts = []
            
            # Pre-validation if requested
            if validate and not force:
                # Get current workers
                worker1 = self.scheduler.schedule.get(shift_date1, [None] * self.scheduler.num_shifts)[post_index1]
                worker2 = self.scheduler.schedule.get(shift_date2, [None] * self.scheduler.num_shifts)[post_index2]
                
                # Validate swap constraints
                if worker1:
                    val_result = self.live_validator.validate_assignment(worker1, shift_date2, post_index2)
                    validation_results.append(val_result)
                
                if worker2:
                    val_result = self.live_validator.validate_assignment(worker2, shift_date1, post_index1)
                    validation_results.append(val_result)
                
                # Check if any validation failed
                failed_validations = [v for v in validation_results if not v.is_valid]
                if failed_validations:
                    return RealTimeOperationResult(
                        success=False,
                        message="Swap validation failed",
                        operation_id=operation_id,
                        validation_results=validation_results
                    )
            
            # Perform the swap
            update_result = self.incremental_updater.swap_workers(
                shift_date1, post_index1, shift_date2, post_index2, user_id, force
            )
            
            if update_result.success:
                # Post-swap conflict detection
                if validate:
                    conflicts1 = self.live_validator.detect_conflicts((shift_date1, shift_date1))
                    conflicts2 = self.live_validator.detect_conflicts((shift_date2, shift_date2))
                    conflicts.extend(conflicts1)
                    conflicts.extend(conflicts2)
                
                return RealTimeOperationResult(
                    success=True,
                    message=update_result.message,
                    operation_id=operation_id,
                    validation_results=validation_results,
                    conflicts=conflicts
                )
            else:
                return RealTimeOperationResult(
                    success=False,
                    message=update_result.message,
                    operation_id=operation_id,
                    validation_results=validation_results
                )
                
        except Exception as e:
            logging.error(f"Error in real-time swap: {e}")
            return RealTimeOperationResult(
                success=False,
                message=f"Swap failed: {str(e)}",
                operation_id=operation_id
            )
        finally:
            with self._operation_lock:
                if operation_id in self._active_operations:
                    end_time = datetime.now()
                    duration = (end_time - self._active_operations[operation_id]['start_time']).total_seconds()
                    self._update_performance_metrics('swap', duration)
                    del self._active_operations[operation_id]
    
    def validate_schedule_real_time(self, 
                                   quick_check: bool = False) -> RealTimeOperationResult:
        """
        Perform real-time schedule validation
        
        Args:
            quick_check: If True, perform only essential validations
            
        Returns:
            RealTimeOperationResult with validation details
        """
        operation_id = f"validate_{datetime.now().timestamp()}"
        
        try:
            with self._operation_lock:
                self._active_operations[operation_id] = {
                    'type': 'validation',
                    'start_time': datetime.now()
                }
            
            # Perform validation
            validation_results = self.live_validator.validate_schedule_integrity(check_partial=quick_check)
            
            # Detect conflicts
            conflicts = self.live_validator.detect_conflicts()
            
            # Determine overall success
            errors = [v for v in validation_results if not v.is_valid]
            success = len(errors) == 0
            
            message = f"Validation complete: {len(errors)} errors, {len(conflicts)} conflicts"
            
            return RealTimeOperationResult(
                success=success,
                message=message,
                operation_id=operation_id,
                validation_results=validation_results,
                conflicts=conflicts
            )
            
        except Exception as e:
            logging.error(f"Error in real-time validation: {e}")
            return RealTimeOperationResult(
                success=False,
                message=f"Validation failed: {str(e)}",
                operation_id=operation_id
            )
        finally:
            with self._operation_lock:
                if operation_id in self._active_operations:
                    end_time = datetime.now()
                    duration = (end_time - self._active_operations[operation_id]['start_time']).total_seconds()
                    self._update_performance_metrics('validation', duration)
                    del self._active_operations[operation_id]
    
    def undo_last_change(self, user_id: Optional[str] = None) -> RealTimeOperationResult:
        """
        Undo the last schedule change
        
        Args:
            user_id: ID of user performing the undo
            
        Returns:
            RealTimeOperationResult with undo details
        """
        operation_id = f"undo_{datetime.now().timestamp()}"
        
        try:
            if not self.change_tracker.can_undo():
                return RealTimeOperationResult(
                    success=False,
                    message="No changes available to undo",
                    operation_id=operation_id
                )
            
            # Perform undo
            undone_change = self.change_tracker.undo(user_id)
            
            if undone_change:
                return RealTimeOperationResult(
                    success=True,
                    message=f"Undid: {undone_change.description}",
                    operation_id=operation_id
                )
            else:
                return RealTimeOperationResult(
                    success=False,
                    message="Failed to undo change",
                    operation_id=operation_id
                )
                
        except Exception as e:
            logging.error(f"Error in undo operation: {e}")
            return RealTimeOperationResult(
                success=False,
                message=f"Undo failed: {str(e)}",
                operation_id=operation_id
            )
    
    def redo_last_change(self, user_id: Optional[str] = None) -> RealTimeOperationResult:
        """
        Redo the last undone change
        
        Args:
            user_id: ID of user performing the redo
            
        Returns:
            RealTimeOperationResult with redo details
        """
        operation_id = f"redo_{datetime.now().timestamp()}"
        
        try:
            if not self.change_tracker.can_redo():
                return RealTimeOperationResult(
                    success=False,
                    message="No changes available to redo",
                    operation_id=operation_id
                )
            
            # Perform redo
            redone_change = self.change_tracker.redo(user_id)
            
            if redone_change:
                return RealTimeOperationResult(
                    success=True,
                    message=f"Redid: {redone_change.description}",
                    operation_id=operation_id
                )
            else:
                return RealTimeOperationResult(
                    success=False,
                    message="Failed to redo change",
                    operation_id=operation_id
                )
                
        except Exception as e:
            logging.error(f"Error in redo operation: {e}")
            return RealTimeOperationResult(
                success=False,
                message=f"Redo failed: {str(e)}",
                operation_id=operation_id
            )
    
    def get_real_time_analytics(self) -> Dict[str, Any]:
        """
        Get real-time analytics and metrics
        
        Returns:
            Dictionary with various analytics data
        """
        try:
            # Active operations
            with self._operation_lock:
                active_ops = len(self._active_operations)
                active_op_types = [op['type'] for op in self._active_operations.values()]
            
            # Change tracking stats
            change_state = self.change_tracker.get_current_state_info()
            
            # Event bus stats
            event_stats = self.event_bus.get_stats()
            
            # Schedule coverage
            total_slots = sum(len(shifts) for shifts in self.scheduler.schedule.values())
            filled_slots = sum(1 for shifts in self.scheduler.schedule.values() 
                             for worker in shifts if worker is not None)
            coverage = (filled_slots / total_slots * 100) if total_slots > 0 else 0
            
            # Worker workload distribution
            workloads = {worker_id: len(assignments) 
                        for worker_id, assignments in self.scheduler.worker_assignments.items()}
            avg_workload = sum(workloads.values()) / len(workloads) if workloads else 0
            
            return {
                'timestamp': datetime.now().isoformat(),
                'active_operations': {
                    'count': active_ops,
                    'types': active_op_types
                },
                'schedule_metrics': {
                    'total_slots': total_slots,
                    'filled_slots': filled_slots,
                    'coverage_percentage': round(coverage, 2)
                },
                'workload_distribution': {
                    'average_workload': round(avg_workload, 2),
                    'min_workload': min(workloads.values()) if workloads else 0,
                    'max_workload': max(workloads.values()) if workloads else 0,
                    'worker_count': len(workloads)
                },
                'change_tracking': change_state,
                'event_system': event_stats,
                'performance_metrics': self._performance_metrics.copy()
            }
            
        except Exception as e:
            logging.error(f"Error getting real-time analytics: {e}")
            return {'error': str(e)}
    
    def get_suggestions_for_optimization(self, limit: int = 5) -> List[str]:
        """
        Get suggestions for schedule optimization
        
        Args:
            limit: Maximum number of suggestions to return
            
        Returns:
            List of optimization suggestions
        """
        suggestions = []
        
        try:
            # Detect conflicts and suggest resolutions
            conflicts = self.live_validator.detect_conflicts()
            for conflict in conflicts[:limit//2]:
                suggestions.extend(conflict.resolution_suggestions[:1])
            
            # Workload balancing suggestions
            workloads = {worker_id: len(assignments) 
                        for worker_id, assignments in self.scheduler.worker_assignments.items()}
            
            if workloads:
                avg_workload = sum(workloads.values()) / len(workloads)
                overloaded = [w for w, load in workloads.items() if load > avg_workload * 1.3]
                underloaded = [w for w, load in workloads.items() if load < avg_workload * 0.7]
                
                if overloaded and underloaded:
                    suggestions.append(f"Consider redistributing shifts from {overloaded[0]} to {underloaded[0]}")
            
            # Coverage improvement suggestions
            empty_slots = []
            for date, shifts in self.scheduler.schedule.items():
                for i, worker in enumerate(shifts):
                    if worker is None:
                        empty_slots.append((date, i))
            
            if empty_slots and len(suggestions) < limit:
                date, post = empty_slots[0]
                available_workers = self.live_validator.get_suggestions_for_date(date, post)
                if available_workers:
                    suggestions.append(f"Fill empty slot on {date.strftime('%Y-%m-%d')} post {post+1}: {available_workers[0]}")
            
            return suggestions[:limit]
            
        except Exception as e:
            logging.error(f"Error getting optimization suggestions: {e}")
            return [f"Error generating suggestions: {str(e)}"]
    
    def _get_optimization_suggestions(self, shift_date: datetime) -> List[str]:
        """Get optimization suggestions for a specific date"""
        suggestions = []
        
        try:
            # Check for empty slots on the same date
            if shift_date in self.scheduler.schedule:
                empty_posts = [i for i, worker in enumerate(self.scheduler.schedule[shift_date]) 
                              if worker is None]
                
                if empty_posts:
                    suggestions.append(f"Consider filling empty posts: {', '.join(map(str, empty_posts))}")
            
            # Check for workload balancing opportunities
            suggestions.extend(self.get_suggestions_for_optimization(3))
            
        except Exception as e:
            logging.error(f"Error getting optimization suggestions for {shift_date}: {e}")
        
        return suggestions[:3]  # Limit suggestions
    
    def _update_performance_metrics(self, operation_type: str, duration: float):
        """Update performance metrics for operations"""
        if operation_type not in self._performance_metrics:
            self._performance_metrics[operation_type] = {
                'count': 0,
                'total_time': 0.0,
                'avg_time': 0.0,
                'min_time': float('inf'),
                'max_time': 0.0
            }
        
        metrics = self._performance_metrics[operation_type]
        metrics['count'] += 1
        metrics['total_time'] += duration
        metrics['avg_time'] = metrics['total_time'] / metrics['count']
        metrics['min_time'] = min(metrics['min_time'], duration)
        metrics['max_time'] = max(metrics['max_time'], duration)
    
    def _on_constraint_violation(self, event):
        """Handle constraint violation events"""
        logging.warning(f"Constraint violation detected: {event.data}")
    
    def _on_validation_result(self, event):
        """Handle validation result events"""
        logging.info(f"Validation completed: {event.data['errors']} errors, {event.data['warnings']} warnings")