class SchedulerError(Exception):
    """Base exception for all Scheduler errors"""
    pass

class ConfigurationError(SchedulerError):
    """Invalid or missing configuration parameters"""
    pass

class ConstraintViolationError(SchedulerError):
    """A scheduling constraint could not be satisfied"""
    pass

class DataIntegrityError(SchedulerError):
    """Corrupt, inconsistent, or unreadable data"""
    pass
