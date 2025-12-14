"""
Performance Cache Module for AI-Powered Workload Demand Forecasting System

This module provides caching infrastructure to optimize expensive calculations
and reduce redundant computations throughout the application.
"""

import logging
import time
import hashlib
import pickle
from functools import wraps, lru_cache
from typing import Any, Dict, Optional, Callable, Tuple
from datetime import datetime, timedelta
from threading import Lock


class PerformanceCache:
    """
    High-performance caching system with TTL support and memory management
    """
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        """
        Initialize the performance cache
        
        Args:
            max_size: Maximum number of cached items
            default_ttl: Default time-to-live in seconds
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = Lock()
        
        logging.info(f"PerformanceCache initialized with max_size={max_size}, default_ttl={default_ttl}")
    
    def _generate_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """Generate a unique cache key from function arguments"""
        try:
            # Create a hashable representation of arguments
            hashable_args = self._make_hashable(args)
            hashable_kwargs = self._make_hashable(sorted(kwargs.items()) if kwargs else {})
            
            key_data = {
                'func': func_name,
                'args': hashable_args,
                'kwargs': hashable_kwargs
            }
            
            # Use pickle to serialize complex objects consistently
            serialized = pickle.dumps(key_data, protocol=pickle.HIGHEST_PROTOCOL)
            
            # Generate SHA256 hash for consistent key generation
            return hashlib.sha256(serialized).hexdigest()
            
        except Exception as e:
            # Fallback to string representation if pickle fails
            logging.warning(f"Failed to pickle cache key data: {e}")
            fallback_key = f"{func_name}_{hash(str(args))}_{hash(str(kwargs))}"
            return hashlib.sha256(fallback_key.encode()).hexdigest()
    
    def _make_hashable(self, obj):
        """Convert objects to hashable representations"""
        if isinstance(obj, dict):
            return tuple(sorted((k, self._make_hashable(v)) for k, v in obj.items()))
        elif isinstance(obj, list):
            return tuple(self._make_hashable(item) for item in obj)
        elif isinstance(obj, set):
            return tuple(sorted(self._make_hashable(item) for item in obj))
        elif hasattr(obj, '__dict__'):
            # For custom objects, use their string representation
            return str(obj)
        else:
            try:
                # Try to hash the object
                hash(obj)
                return obj
            except TypeError:
                # If it's not hashable, convert to string
                return str(obj)
    
    def _is_expired(self, cache_entry: Dict[str, Any]) -> bool:
        """Check if cache entry has expired"""
        if 'expires_at' not in cache_entry:
            return True
        return time.time() > cache_entry['expires_at']
    
    def _evict_expired_entries(self) -> None:
        """Remove expired entries from cache"""
        current_time = time.time()
        expired_keys = [
            key for key, entry in self._cache.items()
            if 'expires_at' in entry and current_time > entry['expires_at']
        ]
        
        for key in expired_keys:
            del self._cache[key]
            if key in self._access_times:
                del self._access_times[key]
    
    def _evict_lru_entries(self) -> None:
        """Remove least recently used entries when cache is full"""
        while len(self._cache) >= self.max_size:
            # Find least recently used key
            lru_key = min(self._access_times.keys(), key=lambda k: self._access_times[k])
            
            del self._cache[lru_key]
            del self._access_times[lru_key]
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        with self._lock:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            
            # Check if expired
            if self._is_expired(entry):
                del self._cache[key]
                if key in self._access_times:
                    del self._access_times[key]
                return None
            
            # Update access time
            self._access_times[key] = time.time()
            
            return entry['value']
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with optional TTL"""
        with self._lock:
            # Clean up expired entries
            self._evict_expired_entries()
            
            # Evict LRU entries if necessary
            if len(self._cache) >= self.max_size:
                self._evict_lru_entries()
            
            # Calculate expiration time
            ttl = ttl or self.default_ttl
            expires_at = time.time() + ttl
            
            # Store the entry
            self._cache[key] = {
                'value': value,
                'expires_at': expires_at,
                'created_at': time.time()
            }
            
            self._access_times[key] = time.time()
    
    def cached_call(self, func: Callable, args: tuple = (), kwargs: dict = None, ttl: Optional[int] = None) -> Any:
        """Execute function with caching"""
        kwargs = kwargs or {}
        key = self._generate_key(func.__name__, args, kwargs)
        
        # Try to get from cache first
        cached_result = self.get(key)
        if cached_result is not None:
            return cached_result
        
        # Execute function and cache result
        try:
            result = func(*args, **kwargs)
            self.set(key, result, ttl)
            return result
        except Exception as e:
            logging.error(f"Error executing cached function {func.__name__}: {e}")
            raise
    
    def invalidate(self, pattern: Optional[str] = None) -> int:
        """Invalidate cache entries, optionally matching a pattern"""
        with self._lock:
            if pattern is None:
                # Clear all entries
                count = len(self._cache)
                self._cache.clear()
                self._access_times.clear()
                return count
            
            # Remove entries matching pattern
            keys_to_remove = [key for key in self._cache.keys() if pattern in key]
            
            for key in keys_to_remove:
                del self._cache[key]
                if key in self._access_times:
                    del self._access_times[key]
            
            return len(keys_to_remove)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            current_time = time.time()
            
            # Count expired entries
            expired_count = sum(
                1 for entry in self._cache.values()
                if 'expires_at' in entry and current_time > entry['expires_at']
            )
            
            return {
                'total_entries': len(self._cache),
                'expired_entries': expired_count,
                'active_entries': len(self._cache) - expired_count,
                'max_size': self.max_size,
                'memory_usage_estimate': len(str(self._cache)),  # Rough estimate
                'cache_utilization': len(self._cache) / self.max_size * 100
            }


# Global cache instance
_global_cache: Optional[PerformanceCache] = None


def get_cache() -> PerformanceCache:
    """Get the global cache instance"""
    global _global_cache
    if _global_cache is None:
        _global_cache = PerformanceCache()
    return _global_cache


def cached(ttl: int = 3600, cache_instance: Optional[PerformanceCache] = None):
    """
    Decorator for caching function results
    
    Args:
        ttl: Time to live in seconds
        cache_instance: Optional specific cache instance to use
    """
    def decorator(func: Callable) -> Callable:
        cache = cache_instance or get_cache()
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return cache.cached_call(func, args, kwargs, ttl)
        
        # Add cache management methods to the wrapped function
        wrapper.cache_invalidate = lambda pattern=None: cache.invalidate(pattern)
        wrapper.cache_stats = lambda: cache.get_stats()
        
        return wrapper
    
    return decorator


def memoize(maxsize: int = 128):
    """
    Simple memoization decorator using functools.lru_cache
    
    Args:
        maxsize: Maximum size of the LRU cache
    """
    def decorator(func: Callable) -> Callable:
        cached_func = lru_cache(maxsize=maxsize)(func)
        
        # Add cache management methods
        cached_func.cache_clear = cached_func.cache_clear
        cached_func.cache_info = cached_func.cache_info
        
        return cached_func
    
    return decorator


def time_function(func: Callable) -> Callable:
    """
    Decorator to measure and log function execution time
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            # Log performance metrics
            if execution_time > 1.0:  # Log slow operations
                logging.info(f"PERFORMANCE: {func.__name__} took {execution_time:.3f}s")
            elif execution_time > 0.1:  # Debug log medium operations
                logging.debug(f"PERFORMANCE: {func.__name__} took {execution_time:.3f}s")
            
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logging.error(f"PERFORMANCE: {func.__name__} failed after {execution_time:.3f}s: {e}")
            raise
    
    return wrapper


class PerformanceMonitor:
    """Monitor and track performance metrics"""
    
    def __init__(self):
        self.metrics: Dict[str, list] = {}
        self._lock = Lock()
    
    def record_metric(self, metric_name: str, value: float, timestamp: Optional[datetime] = None) -> None:
        """Record a performance metric"""
        timestamp = timestamp or datetime.now()
        
        with self._lock:
            if metric_name not in self.metrics:
                self.metrics[metric_name] = []
            
            self.metrics[metric_name].append({
                'value': value,
                'timestamp': timestamp
            })
            
            # Keep only last 1000 measurements per metric
            if len(self.metrics[metric_name]) > 1000:
                self.metrics[metric_name] = self.metrics[metric_name][-1000:]
    
    def get_metric_stats(self, metric_name: str, since: Optional[datetime] = None) -> Dict[str, float]:
        """Get statistics for a specific metric"""
        with self._lock:
            if metric_name not in self.metrics:
                return {}
            
            values = self.metrics[metric_name]
            
            if since:
                values = [m for m in values if m['timestamp'] >= since]
            
            if not values:
                return {}
            
            numeric_values = [m['value'] for m in values]
            
            return {
                'count': len(numeric_values),
                'min': min(numeric_values),
                'max': max(numeric_values),
                'avg': sum(numeric_values) / len(numeric_values),
                'latest': numeric_values[-1] if numeric_values else 0
            }
    
    def get_all_metrics(self) -> Dict[str, Dict[str, float]]:
        """Get statistics for all metrics"""
        with self._lock:
            return {
                metric_name: self.get_metric_stats(metric_name)
                for metric_name in self.metrics.keys()
            }


# Global performance monitor
_global_monitor: Optional[PerformanceMonitor] = None


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance"""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = PerformanceMonitor()
    return _global_monitor


def monitor_performance(metric_name: str):
    """Decorator to monitor function performance"""
    def decorator(func: Callable) -> Callable:
        monitor = get_performance_monitor()
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                monitor.record_metric(metric_name, execution_time)
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                monitor.record_metric(f"{metric_name}_error", execution_time)
                raise
        
        return wrapper
    
    return decorator