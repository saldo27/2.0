"""
Real-time change tracking system for the scheduler.
Provides undo/redo functionality and complete audit trail of all schedule modifications.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)

class OperationType(Enum):
    """Types of operations that can be tracked"""
    ASSIGN_WORKER = "assign_worker"
    UNASSIGN_WORKER = "unassign_worker"
    SWAP_WORKERS = "swap_workers"
    BULK_ASSIGN = "bulk_assign"
    SCHEDULE_REGENERATE = "schedule_regenerate"
    ADJUSTMENT_APPLY = "adjustment_apply"

@dataclass
class ChangeRecord:
    """Record of a single change operation"""
    id: str
    timestamp: datetime
    user_id: str
    operation_type: OperationType
    description: str
    rollback_data: Dict[str, Any]
    forward_data: Dict[str, Any]
    affected_workers: List[str]
    affected_dates: List[str]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['operation_type'] = self.operation_type.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ChangeRecord':
        """Create from dictionary"""
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        data['operation_type'] = OperationType(data['operation_type'])
        return cls(**data)

class ChangeTracker:
    """
    Tracks all schedule changes with complete undo/redo capability.
    Maintains an audit trail for compliance and analysis.
    """
    
    def __init__(self, max_history: int = 1000):
        """
        Initialize change tracker
        
        Args:
            max_history: Maximum number of changes to keep in history
        """
        self.max_history = max_history
        self.changes: List[ChangeRecord] = []
        self.current_position = -1  # Position in the undo/redo stack
        self._change_id_counter = 0
        
        logger.info(f"ChangeTracker initialized with max_history={max_history}")
    
    def record_change(
        self,
        user_id: str,
        operation_type: OperationType,
        description: str,
        rollback_data: Dict[str, Any],
        forward_data: Dict[str, Any],
        affected_workers: List[str] = None,
        affected_dates: List[str] = None,
        metadata: Dict[str, Any] = None
    ) -> str:
        """
        Record a new change operation
        
        Args:
            user_id: ID of user making the change
            operation_type: Type of operation
            description: Human-readable description
            rollback_data: Data needed to undo the operation
            forward_data: Data needed to redo the operation
            affected_workers: List of affected worker IDs
            affected_dates: List of affected date strings
            metadata: Additional metadata
            
        Returns:
            Change ID
        """
        # Generate unique change ID
        self._change_id_counter += 1
        change_id = f"change_{self._change_id_counter}_{int(datetime.now().timestamp())}"
        
        # Create change record
        change_record = ChangeRecord(
            id=change_id,
            timestamp=datetime.now(),
            user_id=user_id,
            operation_type=operation_type,
            description=description,
            rollback_data=rollback_data,
            forward_data=forward_data,
            affected_workers=affected_workers or [],
            affected_dates=affected_dates or [],
            metadata=metadata or {}
        )
        
        # If we're not at the end of the history, truncate future changes
        if self.current_position < len(self.changes) - 1:
            self.changes = self.changes[:self.current_position + 1]
        
        # Add new change
        self.changes.append(change_record)
        self.current_position = len(self.changes) - 1
        
        # Maintain history limit
        if len(self.changes) > self.max_history:
            removed_changes = len(self.changes) - self.max_history
            self.changes = self.changes[removed_changes:]
            self.current_position -= removed_changes
        
        logger.info(f"Recorded change: {change_id} - {description}")
        return change_id
    
    def can_undo(self) -> bool:
        """Check if undo is possible"""
        return self.current_position >= 0
    
    def can_redo(self) -> bool:
        """Check if redo is possible"""
        return self.current_position < len(self.changes) - 1
    
    def get_undo_operation(self) -> Optional[ChangeRecord]:
        """Get the next operation that would be undone"""
        if not self.can_undo():
            return None
        return self.changes[self.current_position]
    
    def get_redo_operation(self) -> Optional[ChangeRecord]:
        """Get the next operation that would be redone"""
        if not self.can_redo():
            return None
        return self.changes[self.current_position + 1]
    
    def mark_undo_applied(self) -> Optional[ChangeRecord]:
        """Mark that an undo operation has been applied"""
        if not self.can_undo():
            return None
        
        change_record = self.changes[self.current_position]
        self.current_position -= 1
        
        logger.info(f"Undo applied: {change_record.id}")
        return change_record
    
    def mark_redo_applied(self) -> Optional[ChangeRecord]:
        """Mark that a redo operation has been applied"""
        if not self.can_redo():
            return None
        
        self.current_position += 1
        change_record = self.changes[self.current_position]
        
        logger.info(f"Redo applied: {change_record.id}")
        return change_record
    
    def get_change_history(
        self,
        limit: int = 50,
        user_id: str = None,
        operation_types: List[OperationType] = None,
        since: datetime = None
    ) -> List[ChangeRecord]:
        """
        Get change history with optional filtering
        
        Args:
            limit: Maximum number of changes to return
            user_id: Filter by user ID
            operation_types: Filter by operation types
            since: Filter changes since this timestamp
            
        Returns:
            List of change records
        """
        filtered_changes = self.changes
        
        # Apply filters
        if user_id:
            filtered_changes = [c for c in filtered_changes if c.user_id == user_id]
        
        if operation_types:
            filtered_changes = [c for c in filtered_changes if c.operation_type in operation_types]
        
        if since:
            filtered_changes = [c for c in filtered_changes if c.timestamp >= since]
        
        # Sort by timestamp (most recent first) and apply limit
        filtered_changes.sort(key=lambda x: x.timestamp, reverse=True)
        return filtered_changes[:limit]
    
    def get_audit_trail(
        self,
        worker_id: str = None,
        date: str = None,
        operation_types: List[OperationType] = None
    ) -> List[ChangeRecord]:
        """
        Get audit trail for specific worker, date, or operation types
        
        Args:
            worker_id: Filter by worker ID
            date: Filter by date string
            operation_types: Filter by operation types
            
        Returns:
            List of relevant change records
        """
        filtered_changes = self.changes
        
        if worker_id:
            filtered_changes = [
                c for c in filtered_changes 
                if worker_id in c.affected_workers
            ]
        
        if date:
            filtered_changes = [
                c for c in filtered_changes 
                if date in c.affected_dates
            ]
        
        if operation_types:
            filtered_changes = [
                c for c in filtered_changes 
                if c.operation_type in operation_types
            ]
        
        # Sort by timestamp
        filtered_changes.sort(key=lambda x: x.timestamp)
        return filtered_changes
    
    def export_audit_data(self) -> Dict[str, Any]:
        """
        Export complete audit data for compliance
        
        Returns:
            Dictionary containing all audit data
        """
        return {
            'export_timestamp': datetime.now().isoformat(),
            'total_changes': len(self.changes),
            'current_position': self.current_position,
            'changes': [change.to_dict() for change in self.changes]
        }
    
    def import_audit_data(self, data: Dict[str, Any]) -> bool:
        """
        Import audit data from export
        
        Args:
            data: Exported audit data
            
        Returns:
            True if successful
        """
        try:
            self.changes = [
                ChangeRecord.from_dict(change_data) 
                for change_data in data['changes']
            ]
            self.current_position = data['current_position']
            
            logger.info(f"Imported {len(self.changes)} change records")
            return True
        except Exception as e:
            logger.error(f"Failed to import audit data: {e}")
            return False
    
    def clear_history(self) -> None:
        """Clear all change history"""
        self.changes.clear()
        self.current_position = -1
        logger.info("Change history cleared")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about tracked changes
        
        Returns:
            Dictionary with change statistics
        """
        if not self.changes:
            return {
                'total_changes': 0,
                'can_undo': False,
                'can_redo': False
            }
        
        # Count by operation type
        operation_counts = {}
        for change in self.changes:
            op_type = change.operation_type.value
            operation_counts[op_type] = operation_counts.get(op_type, 0) + 1
        
        # Count by user
        user_counts = {}
        for change in self.changes:
            user_id = change.user_id
            user_counts[user_id] = user_counts.get(user_id, 0) + 1
        
        return {
            'total_changes': len(self.changes),
            'current_position': self.current_position,
            'can_undo': self.can_undo(),
            'can_redo': self.can_redo(),
            'operation_counts': operation_counts,
            'user_counts': user_counts,
            'oldest_change': self.changes[0].timestamp.isoformat() if self.changes else None,
            'newest_change': self.changes[-1].timestamp.isoformat() if self.changes else None
        }
    
    def get_current_state_info(self) -> Dict[str, Any]:
        """
        Get current state information for real-time analytics
        
        Returns:
            Dictionary with current state information
        """
        return {
            'total_changes': len(self.changes),
            'current_position': self.current_position,
            'can_undo': self.can_undo(),
            'can_redo': self.can_redo(),
            'pending_changes': len(self.changes) - self.current_position if self.changes else 0,
            'last_change': self.changes[self.current_position - 1].description if self.current_position > 0 else None,
            'history_size': len(self.changes),
            'max_history': self.max_history
        }