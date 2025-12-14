"""
Incremental schedule update logic for real-time modifications.
Enables smart schedule updates without full regeneration.
"""

from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any, Set
import logging
from dataclasses import dataclass
from copy import deepcopy

from event_bus import get_event_bus, EventType, ScheduleEvent
from exceptions import SchedulerError


@dataclass
class UpdateResult:
    """Result of an incremental update operation"""
    success: bool
    message: str
    conflicts: List[str] = None
    suggestions: List[str] = None
    rollback_data: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.conflicts is None:
            self.conflicts = []
        if self.suggestions is None:
            self.suggestions = []
        if self.rollback_data is None:
            self.rollback_data = {}


class IncrementalUpdater:
    """Handles incremental schedule updates with validation and conflict resolution"""
    
    def __init__(self, scheduler):
        """
        Initialize the incremental updater
        
        Args:
            scheduler: The main Scheduler instance
        """
        self.scheduler = scheduler
        self.event_bus = get_event_bus()
        
        # Caches for performance
        self._validation_cache: Dict[str, Any] = {}
        
        logging.info("IncrementalUpdater initialized")
    
    def assign_worker_to_shift(self, 
                              worker_id: str, 
                              shift_date: datetime, 
                              post_index: int,
                              user_id: Optional[str] = None,
                              force: bool = False) -> UpdateResult:
        """
        Assign a worker to a specific shift
        
        Args:
            worker_id: ID of worker to assign
            shift_date: Date of the shift
            post_index: Post index (0-based)
            user_id: ID of user making the change
            force: Skip constraint validation if True
            
        Returns:
            UpdateResult with success status and details
        """
        try:
            # Validate inputs
            if not self._validate_assignment_inputs(worker_id, shift_date, post_index):
                return UpdateResult(False, "Invalid assignment parameters")
            
            # Create rollback data
            rollback_data = self._create_assignment_rollback_data(shift_date, post_index)
            
            # Check constraints unless forced
            if not force:
                constraint_result = self._check_assignment_constraints(worker_id, shift_date, post_index)
                if not constraint_result.success:
                    return constraint_result
            
            # Store previous assignment
            previous_worker = self.scheduler.schedule.get(shift_date, [None] * self.scheduler.num_shifts)[post_index]
            
            # CRITICAL: If there's a previous worker and it's a mandatory assignment, block the reassignment
            if previous_worker and hasattr(self.scheduler, 'schedule_builder') and self.scheduler.schedule_builder:
                if self.scheduler.schedule_builder._is_mandatory(previous_worker, shift_date):
                    logging.error(f"BLOCKED assignment: Cannot replace worker {previous_worker} on {shift_date.strftime('%Y-%m-%d')} - this is a MANDATORY assignment")
                    return UpdateResult(
                        False,
                        f"Cannot assign: Post is occupied by worker {previous_worker} who has a MANDATORY assignment on {shift_date.strftime('%Y-%m-%d')}",
                        rollback_data=rollback_data
                    )
            
            # Perform the assignment
            if shift_date not in self.scheduler.schedule:
                self.scheduler.schedule[shift_date] = [None] * self.scheduler.num_shifts
            
            self.scheduler.schedule[shift_date][post_index] = worker_id
            
            # Update tracking data
            self._update_assignment_tracking(worker_id, shift_date, post_index, previous_worker)
            
            # Emit event
            self.event_bus.emit(
                EventType.SHIFT_ASSIGNED,
                user_id=user_id,
                worker_id=worker_id,
                shift_date=shift_date.isoformat(),
                post_index=post_index,
                previous_worker=previous_worker
            )
            
            logging.info(f"Worker {worker_id} assigned to {shift_date.strftime('%Y-%m-%d')} post {post_index}")
            
            return UpdateResult(
                True, 
                f"Successfully assigned {worker_id}",
                rollback_data=rollback_data
            )
            
        except Exception as e:
            logging.error(f"Error assigning worker: {e}")
            return UpdateResult(False, f"Assignment failed: {str(e)}")
    
    def unassign_worker_from_shift(self, 
                                  shift_date: datetime, 
                                  post_index: int,
                                  user_id: Optional[str] = None) -> UpdateResult:
        """
        Remove worker assignment from a specific shift
        
        Args:
            shift_date: Date of the shift
            post_index: Post index (0-based)
            user_id: ID of user making the change
            
        Returns:
            UpdateResult with success status and details
        """
        try:
            # Validate inputs
            if not self._validate_unassignment_inputs(shift_date, post_index):
                return UpdateResult(False, "Invalid unassignment parameters")
            
            # Get current assignment
            current_worker = self.scheduler.schedule.get(shift_date, [None] * self.scheduler.num_shifts)[post_index]
            if current_worker is None:
                return UpdateResult(False, "No worker assigned to this shift")
            
            # CRITICAL: Check if this is a mandatory assignment - cannot unassign
            if hasattr(self.scheduler, 'schedule_builder') and self.scheduler.schedule_builder:
                if self.scheduler.schedule_builder._is_mandatory(current_worker, shift_date):
                    logging.warning(f"Cannot unassign worker {current_worker} from {shift_date.strftime('%d-%m-%Y')} - this is a MANDATORY assignment")
                    return UpdateResult(False, f"Cannot unassign: Worker {current_worker} has a MANDATORY assignment on {shift_date.strftime('%d-%m-%Y')}")
            
            # Create rollback data
            rollback_data = self._create_unassignment_rollback_data(shift_date, post_index, current_worker)
            
            # Perform the unassignment
            self.scheduler.schedule[shift_date][post_index] = None
            
            # Update tracking data
            self._update_unassignment_tracking(current_worker, shift_date, post_index)
            
            # Emit event
            self.event_bus.emit(
                EventType.SHIFT_UNASSIGNED,
                user_id=user_id,
                worker_id=current_worker,
                shift_date=shift_date.isoformat(),
                post_index=post_index
            )
            
            logging.info(f"Worker {current_worker} unassigned from {shift_date.strftime('%Y-%m-%d')} post {post_index}")
            
            return UpdateResult(
                True, 
                f"Successfully unassigned {current_worker}",
                rollback_data=rollback_data
            )
            
        except Exception as e:
            logging.error(f"Error unassigning worker: {e}")
            return UpdateResult(False, f"Unassignment failed: {str(e)}")
    
    def swap_workers(self, 
                    shift_date1: datetime, post_index1: int,
                    shift_date2: datetime, post_index2: int,
                    user_id: Optional[str] = None,
                    force: bool = False) -> UpdateResult:
        """
        Swap two worker assignments
        
        Args:
            shift_date1: Date of first shift
            post_index1: Post index of first shift
            shift_date2: Date of second shift
            post_index2: Post index of second shift
            user_id: ID of user making the change
            force: Skip constraint validation if True
            
        Returns:
            UpdateResult with success status and details
        """
        try:
            # Get current assignments
            worker1 = self.scheduler.schedule.get(shift_date1, [None] * self.scheduler.num_shifts)[post_index1]
            worker2 = self.scheduler.schedule.get(shift_date2, [None] * self.scheduler.num_shifts)[post_index2]
            
            if worker1 is None and worker2 is None:
                return UpdateResult(False, "No workers to swap")
            
            # Create rollback data
            rollback_data = {
                'shift1': {'date': shift_date1, 'post': post_index1, 'worker': worker1},
                'shift2': {'date': shift_date2, 'post': post_index2, 'worker': worker2}
            }
            
            # Check constraints unless forced
            if not force:
                constraint_result = self._check_swap_constraints(
                    worker1, shift_date1, post_index1,
                    worker2, shift_date2, post_index2
                )
                if not constraint_result.success:
                    return constraint_result
            
            # Perform the swap
            if shift_date1 not in self.scheduler.schedule:
                self.scheduler.schedule[shift_date1] = [None] * self.scheduler.num_shifts
            if shift_date2 not in self.scheduler.schedule:
                self.scheduler.schedule[shift_date2] = [None] * self.scheduler.num_shifts
            
            self.scheduler.schedule[shift_date1][post_index1] = worker2
            self.scheduler.schedule[shift_date2][post_index2] = worker1
            
            # Update tracking data for both workers
            if worker1:
                self._update_unassignment_tracking(worker1, shift_date1, post_index1)
                if worker1 != worker2:  # Don't double-update if same worker
                    self._update_assignment_tracking(worker1, shift_date2, post_index2, worker2)
            
            if worker2 and worker2 != worker1:
                self._update_unassignment_tracking(worker2, shift_date2, post_index2)
                self._update_assignment_tracking(worker2, shift_date1, post_index1, worker1)
            
            # Emit event
            self.event_bus.emit(
                EventType.SHIFT_SWAPPED,
                user_id=user_id,
                worker1=worker1,
                worker2=worker2,
                shift_date1=shift_date1.isoformat(),
                post_index1=post_index1,
                shift_date2=shift_date2.isoformat(),
                post_index2=post_index2
            )
            
            logging.info(f"Swapped workers: {worker1} <-> {worker2}")
            
            return UpdateResult(
                True, 
                f"Successfully swapped {worker1 or 'empty'} and {worker2 or 'empty'}",
                rollback_data=rollback_data
            )
            
        except Exception as e:
            logging.error(f"Error swapping workers: {e}")
            return UpdateResult(False, f"Swap failed: {str(e)}")
    
    def bulk_update(self, updates: List[Dict[str, Any]], user_id: Optional[str] = None) -> UpdateResult:
        """
        Perform multiple updates as a single operation
        
        Args:
            updates: List of update operations
            user_id: ID of user making the changes
            
        Returns:
            UpdateResult with overall success status
        """
        try:
            rollback_operations = []
            successful_updates = 0
            
            for i, update in enumerate(updates):
                operation = update.get('operation')
                
                if operation == 'assign':
                    result = self.assign_worker_to_shift(
                        update['worker_id'],
                        datetime.fromisoformat(update['shift_date']),
                        update['post_index'],
                        user_id=user_id,
                        force=update.get('force', False)
                    )
                elif operation == 'unassign':
                    result = self.unassign_worker_from_shift(
                        datetime.fromisoformat(update['shift_date']),
                        update['post_index'],
                        user_id=user_id
                    )
                elif operation == 'swap':
                    result = self.swap_workers(
                        datetime.fromisoformat(update['shift_date1']),
                        update['post_index1'],
                        datetime.fromisoformat(update['shift_date2']),
                        update['post_index2'],
                        user_id=user_id,
                        force=update.get('force', False)
                    )
                else:
                    logging.warning(f"Unknown operation in bulk update: {operation}")
                    continue
                
                if result.success:
                    successful_updates += 1
                    rollback_operations.append({
                        'operation': operation,
                        'rollback_data': result.rollback_data,
                        'index': i
                    })
                else:
                    logging.warning(f"Bulk update operation {i} failed: {result.message}")
            
            # Emit bulk update event
            self.event_bus.emit(
                EventType.BULK_UPDATE,
                user_id=user_id,
                total_operations=len(updates),
                successful_operations=successful_updates,
                failed_operations=len(updates) - successful_updates
            )
            
            return UpdateResult(
                successful_updates > 0,
                f"Bulk update: {successful_updates}/{len(updates)} operations successful",
                rollback_data={'operations': rollback_operations}
            )
            
        except Exception as e:
            logging.error(f"Error in bulk update: {e}")
            return UpdateResult(False, f"Bulk update failed: {str(e)}")
    
    def _validate_assignment_inputs(self, worker_id: str, shift_date: datetime, post_index: int) -> bool:
        """Validate inputs for worker assignment"""
        if not worker_id:
            return False
        if not isinstance(shift_date, datetime):
            return False
        if not (0 <= post_index < self.scheduler.num_shifts):
            return False
        return True
    
    def _validate_unassignment_inputs(self, shift_date: datetime, post_index: int) -> bool:
        """Validate inputs for worker unassignment"""
        if not isinstance(shift_date, datetime):
            return False
        if not (0 <= post_index < self.scheduler.num_shifts):
            return False
        return True
    
    def _check_assignment_constraints(self, worker_id: str, shift_date: datetime, post_index: int) -> UpdateResult:
        """Check if assignment violates constraints"""
        try:
            # Use the existing constraint checker
            can_assign, reason = self.scheduler.constraint_checker._check_constraints(
                worker_id, shift_date
            )
            
            if not can_assign:
                suggestions = self._generate_assignment_suggestions(shift_date, post_index)
                return UpdateResult(
                    False, 
                    f"Constraint violation: {reason}",
                    conflicts=[reason],
                    suggestions=suggestions
                )
            
            return UpdateResult(True, "Constraints satisfied")
            
        except Exception as e:
            logging.error(f"Error checking assignment constraints: {e}")
            return UpdateResult(False, f"Constraint check failed: {str(e)}")
    
    def _check_swap_constraints(self, worker1: Optional[str], shift_date1: datetime, post_index1: int,
                               worker2: Optional[str], shift_date2: datetime, post_index2: int) -> UpdateResult:
        """Check if swap violates constraints"""
        constraints_ok = True
        conflicts = []
        
        # CRITICAL: Check if either assignment is mandatory - mandatory assignments cannot be swapped
        if worker1:
            from schedule_builder import ScheduleBuilder
            # We need to check if worker1 has a mandatory assignment on shift_date1
            if hasattr(self.scheduler, 'schedule_builder') and self.scheduler.schedule_builder:
                if self.scheduler.schedule_builder._is_mandatory(worker1, shift_date1):
                    constraints_ok = False
                    conflicts.append(f"Worker {worker1} has a MANDATORY assignment on {shift_date1.strftime('%d-%m-%Y')} - cannot swap")
        
        if worker2:
            # Check if worker2 has a mandatory assignment on shift_date2
            if hasattr(self.scheduler, 'schedule_builder') and self.scheduler.schedule_builder:
                if self.scheduler.schedule_builder._is_mandatory(worker2, shift_date2):
                    constraints_ok = False
                    conflicts.append(f"Worker {worker2} has a MANDATORY assignment on {shift_date2.strftime('%d-%m-%Y')} - cannot swap")
        
        # If any mandatory assignment involved, stop here
        if not constraints_ok:
            return UpdateResult(False, "Cannot swap: one or more assignments are MANDATORY and cannot be moved", conflicts=conflicts)
        
        # Check worker1 constraints for shift2
        if worker1:
            # For 7/14 day pattern and gap constraints, we need to keep the original assignments
            # but for incompatibility, we need to consider the swap
            can_assign, reason = self.scheduler.constraint_checker._check_constraints(worker1, shift_date2)
            if not can_assign:
                constraints_ok = False
                conflicts.append(f"Worker {worker1} cannot take shift 2: {reason}")
            
            # For incompatibility check, consider workers on shift2 excluding worker2 who will be swapped
            if shift_date2 in self.scheduler.schedule:
                assigned_workers_shift2 = [w for i, w in enumerate(self.scheduler.schedule[shift_date2]) 
                                         if w is not None and i != post_index2]
                for assigned_worker in assigned_workers_shift2:
                    if self.scheduler.constraint_checker._are_workers_incompatible(worker1, assigned_worker):
                        constraints_ok = False
                        conflicts.append(f"Worker {worker1} is incompatible with {assigned_worker} on shift 2")
        
        # Check worker2 constraints for shift1
        if worker2:
            # For 7/14 day pattern and gap constraints, we need to keep the original assignments
            # but for incompatibility, we need to consider the swap
            can_assign, reason = self.scheduler.constraint_checker._check_constraints(worker2, shift_date1)
            if not can_assign:
                constraints_ok = False
                conflicts.append(f"Worker {worker2} cannot take shift 1: {reason}")
            
            # For incompatibility check, consider workers on shift1 excluding worker1 who will be swapped
            if shift_date1 in self.scheduler.schedule:
                assigned_workers_shift1 = [w for i, w in enumerate(self.scheduler.schedule[shift_date1]) 
                                         if w is not None and i != post_index1]
                for assigned_worker in assigned_workers_shift1:
                    if self.scheduler.constraint_checker._are_workers_incompatible(worker2, assigned_worker):
                        constraints_ok = False
                        conflicts.append(f"Worker {worker2} is incompatible with {assigned_worker} on shift 1")
        
        if not constraints_ok:
            return UpdateResult(False, "Swap would violate constraints", conflicts=conflicts)
        
        return UpdateResult(True, "Swap constraints satisfied")
    
    def _generate_assignment_suggestions(self, shift_date: datetime, post_index: int) -> List[str]:
        """Generate alternative worker suggestions for assignment"""
        suggestions = []
        
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            can_assign, _ = self.scheduler.constraint_checker._check_constraints(worker_id, shift_date)
            if can_assign:
                suggestions.append(f"Worker {worker_id} is available")
                if len(suggestions) >= 3:  # Limit suggestions
                    break
        
        return suggestions
    
    def _create_assignment_rollback_data(self, shift_date: datetime, post_index: int) -> Dict[str, Any]:
        """Create rollback data for assignment operation"""
        current_worker = None
        if shift_date in self.scheduler.schedule:
            current_worker = self.scheduler.schedule[shift_date][post_index]
        
        return {
            'operation': 'assignment',
            'shift_date': shift_date.isoformat(),
            'post_index': post_index,
            'previous_worker': current_worker
        }
    
    def _create_unassignment_rollback_data(self, shift_date: datetime, post_index: int, worker_id: str) -> Dict[str, Any]:
        """Create rollback data for unassignment operation"""
        return {
            'operation': 'unassignment',
            'shift_date': shift_date.isoformat(),
            'post_index': post_index,
            'worker_id': worker_id
        }
    
    def _update_assignment_tracking(self, worker_id: str, shift_date: datetime, post_index: int, previous_worker: Optional[str]):
        """Update tracking data when assigning a worker"""
        # Remove previous worker's tracking if exists
        if previous_worker:
            self.scheduler._update_tracking_data(previous_worker, shift_date, post_index, removing=True)
        
        # Add new worker's tracking
        self.scheduler._update_tracking_data(worker_id, shift_date, post_index, removing=False)
    
    def _update_unassignment_tracking(self, worker_id: str, shift_date: datetime, post_index: int):
        """Update tracking data when unassigning a worker"""
        self.scheduler._update_tracking_data(worker_id, shift_date, post_index, removing=True)