"""
Collaboration management system for multi-user real-time scheduling.
Provides resource locking, conflict resolution, and session management.
"""

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import uuid

logger = logging.getLogger(__name__)

class LockType(Enum):
    """Types of resource locks"""
    WORKER_ASSIGNMENT = "worker_assignment"
    SHIFT_EDIT = "shift_edit"
    SCHEDULE_GENERATION = "schedule_generation"
    BULK_OPERATION = "bulk_operation"

class ConflictResolution(Enum):
    """Conflict resolution strategies"""
    LAST_WRITER_WINS = "last_writer_wins"
    FIRST_WRITER_WINS = "first_writer_wins"
    MANUAL_RESOLUTION = "manual_resolution"
    AUTOMATIC_MERGE = "automatic_merge"

@dataclass
class ResourceLock:
    """Represents a lock on a resource"""
    lock_id: str
    user_id: str
    lock_type: LockType
    resource_id: str
    acquired_at: datetime
    expires_at: datetime
    metadata: Dict[str, Any]
    
    def is_expired(self) -> bool:
        """Check if lock has expired"""
        return datetime.now() > self.expires_at
    
    def time_remaining(self) -> int:
        """Get seconds remaining before expiration"""
        remaining = (self.expires_at - datetime.now()).seconds
        return max(0, remaining)

@dataclass
class UserSession:
    """Represents an active user session"""
    user_id: str
    session_id: str
    started_at: datetime
    last_activity: datetime
    active_locks: Set[str]
    permissions: Set[str]
    metadata: Dict[str, Any]
    
    def is_active(self, timeout_minutes: int = 30) -> bool:
        """Check if session is still active"""
        timeout = datetime.now() - timedelta(minutes=timeout_minutes)
        return self.last_activity > timeout
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.now()

@dataclass
class ConflictRecord:
    """Record of a scheduling conflict"""
    conflict_id: str
    conflict_type: str
    resource_id: str
    users_involved: List[str]
    detected_at: datetime
    resolution_strategy: ConflictResolution
    resolved: bool
    resolution_data: Dict[str, Any]

class CollaborationManager:
    """
    Manages multi-user collaboration for the scheduler.
    Handles resource locking, conflict detection, and session management.
    """
    
    def __init__(
        self, 
        default_lock_timeout: int = 300,  # 5 minutes
        session_timeout: int = 1800,      # 30 minutes
        cleanup_interval: int = 60        # 1 minute
    ):
        """
        Initialize collaboration manager
        
        Args:
            default_lock_timeout: Default lock timeout in seconds
            session_timeout: Session timeout in seconds
            cleanup_interval: Cleanup interval in seconds
        """
        self.default_lock_timeout = default_lock_timeout
        self.session_timeout = session_timeout
        self.cleanup_interval = cleanup_interval
        
        # Thread-safe data structures
        self._lock = threading.RLock()
        self.active_locks: Dict[str, ResourceLock] = {}
        self.user_sessions: Dict[str, UserSession] = {}
        self.conflicts: Dict[str, ConflictRecord] = {}
        
        # Lock queues for waiting users
        self.lock_queues: Dict[str, List[Tuple[str, datetime]]] = {}
        
        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        logger.info("CollaborationManager initialized")
    
    def create_session(
        self, 
        user_id: str, 
        permissions: Set[str] = None,
        metadata: Dict[str, Any] = None
    ) -> str:
        """
        Create a new user session
        
        Args:
            user_id: User ID
            permissions: Set of user permissions
            metadata: Additional session metadata
            
        Returns:
            Session ID
        """
        with self._lock:
            session_id = str(uuid.uuid4())
            
            session = UserSession(
                user_id=user_id,
                session_id=session_id,
                started_at=datetime.now(),
                last_activity=datetime.now(),
                active_locks=set(),
                permissions=permissions or set(),
                metadata=metadata or {}
            )
            
            self.user_sessions[session_id] = session
            logger.info(f"Created session {session_id} for user {user_id}")
            
            return session_id
    
    def end_session(self, session_id: str) -> bool:
        """
        End a user session and release all locks
        
        Args:
            session_id: Session ID to end
            
        Returns:
            True if session was ended successfully
        """
        with self._lock:
            session = self.user_sessions.get(session_id)
            if not session:
                return False
            
            # Release all locks held by this session
            for lock_id in list(session.active_locks):
                self.release_lock(lock_id, session.user_id)
            
            # Remove session
            del self.user_sessions[session_id]
            logger.info(f"Ended session {session_id} for user {session.user_id}")
            
            return True
    
    def update_session_activity(self, session_id: str) -> bool:
        """
        Update session activity timestamp
        
        Args:
            session_id: Session ID
            
        Returns:
            True if session was updated
        """
        with self._lock:
            session = self.user_sessions.get(session_id)
            if session:
                session.update_activity()
                return True
            return False
    
    def acquire_lock(
        self, 
        user_id: str,
        lock_type: LockType,
        resource_id: str,
        timeout_seconds: int = None,
        metadata: Dict[str, Any] = None,
        wait: bool = False
    ) -> Optional[str]:
        """
        Acquire a lock on a resource
        
        Args:
            user_id: User requesting the lock
            lock_type: Type of lock
            resource_id: Resource to lock
            timeout_seconds: Lock timeout (uses default if None)
            metadata: Additional lock metadata
            wait: Whether to wait if resource is locked
            
        Returns:
            Lock ID if successful, None otherwise
        """
        with self._lock:
            timeout = timeout_seconds or self.default_lock_timeout
            lock_key = f"{lock_type.value}:{resource_id}"
            
            # Check if resource is already locked
            existing_lock = None
            for lock in self.active_locks.values():
                if (lock.lock_type == lock_type and 
                    lock.resource_id == resource_id and 
                    not lock.is_expired()):
                    existing_lock = lock
                    break
            
            if existing_lock:
                if existing_lock.user_id == user_id:
                    # User already has the lock, extend it
                    existing_lock.expires_at = datetime.now() + timedelta(seconds=timeout)
                    logger.info(f"Extended lock {existing_lock.lock_id} for user {user_id}")
                    return existing_lock.lock_id
                
                if wait:
                    # Add to queue
                    if lock_key not in self.lock_queues:
                        self.lock_queues[lock_key] = []
                    self.lock_queues[lock_key].append((user_id, datetime.now()))
                    logger.info(f"User {user_id} queued for lock on {lock_key}")
                    return None
                else:
                    logger.warning(f"Resource {resource_id} is locked by {existing_lock.user_id}")
                    return None
            
            # Create new lock
            lock_id = str(uuid.uuid4())
            lock = ResourceLock(
                lock_id=lock_id,
                user_id=user_id,
                lock_type=lock_type,
                resource_id=resource_id,
                acquired_at=datetime.now(),
                expires_at=datetime.now() + timedelta(seconds=timeout),
                metadata=metadata or {}
            )
            
            self.active_locks[lock_id] = lock
            
            # Update user session
            for session in self.user_sessions.values():
                if session.user_id == user_id:
                    session.active_locks.add(lock_id)
                    session.update_activity()
                    break
            
            logger.info(f"Acquired lock {lock_id} on {resource_id} for user {user_id}")
            return lock_id
    
    def release_lock(self, lock_id: str, user_id: str) -> bool:
        """
        Release a lock
        
        Args:
            lock_id: Lock ID to release
            user_id: User releasing the lock
            
        Returns:
            True if lock was released successfully
        """
        with self._lock:
            lock = self.active_locks.get(lock_id)
            if not lock:
                return False
            
            if lock.user_id != user_id:
                logger.warning(f"User {user_id} cannot release lock {lock_id} owned by {lock.user_id}")
                return False
            
            # Remove lock
            del self.active_locks[lock_id]
            
            # Update user session
            for session in self.user_sessions.values():
                if session.user_id == user_id:
                    session.active_locks.discard(lock_id)
                    session.update_activity()
                    break
            
            # Process lock queue
            lock_key = f"{lock.lock_type.value}:{lock.resource_id}"
            if lock_key in self.lock_queues and self.lock_queues[lock_key]:
                next_user, queued_at = self.lock_queues[lock_key].pop(0)
                logger.info(f"Processing queued lock request for user {next_user}")
                # Note: This would trigger a callback to notify the waiting user
            
            logger.info(f"Released lock {lock_id} for user {user_id}")
            return True
    
    def check_lock_status(self, lock_type: LockType, resource_id: str) -> Optional[ResourceLock]:
        """
        Check if a resource is locked
        
        Args:
            lock_type: Type of lock to check
            resource_id: Resource to check
            
        Returns:
            Active lock if exists, None otherwise
        """
        with self._lock:
            for lock in self.active_locks.values():
                if (lock.lock_type == lock_type and 
                    lock.resource_id == resource_id and 
                    not lock.is_expired()):
                    return lock
            return None
    
    def get_user_locks(self, user_id: str) -> List[ResourceLock]:
        """
        Get all locks held by a user
        
        Args:
            user_id: User ID
            
        Returns:
            List of active locks
        """
        with self._lock:
            return [
                lock for lock in self.active_locks.values()
                if lock.user_id == user_id and not lock.is_expired()
            ]
    
    def detect_conflict(
        self,
        operation_type: str,
        resource_id: str,
        user_id: str,
        proposed_change: Dict[str, Any]
    ) -> Optional[ConflictRecord]:
        """
        Detect potential conflicts
        
        Args:
            operation_type: Type of operation being attempted
            resource_id: Resource being modified
            user_id: User attempting the operation
            proposed_change: Details of proposed change
            
        Returns:
            ConflictRecord if conflict detected, None otherwise
        """
        with self._lock:
            # Check for resource locks by other users
            for lock in self.active_locks.values():
                if (lock.resource_id == resource_id and 
                    lock.user_id != user_id and 
                    not lock.is_expired()):
                    
                    conflict_id = str(uuid.uuid4())
                    conflict = ConflictRecord(
                        conflict_id=conflict_id,
                        conflict_type="resource_locked",
                        resource_id=resource_id,
                        users_involved=[user_id, lock.user_id],
                        detected_at=datetime.now(),
                        resolution_strategy=ConflictResolution.FIRST_WRITER_WINS,
                        resolved=False,
                        resolution_data={
                            'operation_type': operation_type,
                            'proposed_change': proposed_change,
                            'blocking_lock': lock.lock_id
                        }
                    )
                    
                    self.conflicts[conflict_id] = conflict
                    logger.warning(f"Conflict detected: {conflict_id}")
                    return conflict
            
            return None
    
    def resolve_conflict(
        self,
        conflict_id: str,
        resolution: ConflictResolution,
        resolution_data: Dict[str, Any] = None
    ) -> bool:
        """
        Resolve a conflict
        
        Args:
            conflict_id: Conflict to resolve
            resolution: Resolution strategy
            resolution_data: Additional resolution data
            
        Returns:
            True if conflict was resolved
        """
        with self._lock:
            conflict = self.conflicts.get(conflict_id)
            if not conflict:
                return False
            
            conflict.resolution_strategy = resolution
            conflict.resolved = True
            conflict.resolution_data.update(resolution_data or {})
            
            logger.info(f"Resolved conflict {conflict_id} using {resolution.value}")
            return True
    
    def get_active_sessions(self) -> List[UserSession]:
        """Get all active sessions"""
        with self._lock:
            now = datetime.now()
            timeout_threshold = now - timedelta(seconds=self.session_timeout)
            
            return [
                session for session in self.user_sessions.values()
                if session.last_activity > timeout_threshold
            ]
    
    def get_collaboration_status(self) -> Dict[str, Any]:
        """
        Get overall collaboration status
        
        Returns:
            Dictionary with collaboration statistics
        """
        with self._lock:
            active_sessions = self.get_active_sessions()
            
            return {
                'active_sessions': len(active_sessions),
                'active_locks': len([l for l in self.active_locks.values() if not l.is_expired()]),
                'pending_conflicts': len([c for c in self.conflicts.values() if not c.resolved]),
                'users_online': len(set(session.user_id for session in active_sessions)),
                'lock_queues': {k: len(v) for k, v in self.lock_queues.items()},
                'session_details': [
                    {
                        'user_id': session.user_id,
                        'active_locks': len(session.active_locks),
                        'last_activity': session.last_activity.isoformat()
                    }
                    for session in active_sessions
                ]
            }
    
    def _cleanup_loop(self):
        """Background cleanup of expired locks and sessions"""
        while True:
            try:
                time.sleep(self.cleanup_interval)
                self._cleanup_expired_resources()
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
    
    def _cleanup_expired_resources(self):
        """Clean up expired locks and inactive sessions"""
        with self._lock:
            now = datetime.now()
            
            # Clean up expired locks
            expired_locks = [
                lock_id for lock_id, lock in self.active_locks.items()
                if lock.is_expired()
            ]
            
            for lock_id in expired_locks:
                lock = self.active_locks.pop(lock_id)
                logger.info(f"Cleaned up expired lock {lock_id}")
                
                # Update user sessions
                for session in self.user_sessions.values():
                    session.active_locks.discard(lock_id)
            
            # Clean up inactive sessions
            timeout_threshold = now - timedelta(seconds=self.session_timeout)
            inactive_sessions = [
                session_id for session_id, session in self.user_sessions.items()
                if session.last_activity < timeout_threshold
            ]
            
            for session_id in inactive_sessions:
                self.end_session(session_id)
            
            # Clean up old conflicts (older than 24 hours)
            old_threshold = now - timedelta(hours=24)
            old_conflicts = [
                conflict_id for conflict_id, conflict in self.conflicts.items()
                if conflict.detected_at < old_threshold and conflict.resolved
            ]
            
            for conflict_id in old_conflicts:
                del self.conflicts[conflict_id]
            
            if expired_locks or inactive_sessions or old_conflicts:
                logger.info(
                    f"Cleanup completed: {len(expired_locks)} locks, "
                    f"{len(inactive_sessions)} sessions, {len(old_conflicts)} conflicts"
                )

# Singleton instance for global access
collaboration_manager = None

def get_collaboration_manager(**kwargs) -> CollaborationManager:
    """Get singleton collaboration manager instance"""
    global collaboration_manager
    if collaboration_manager is None:
        collaboration_manager = CollaborationManager(**kwargs)
    return collaboration_manager