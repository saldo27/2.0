"""Tests for saldo27.performance_cache — caching and monitoring."""

import time

import pytest

from saldo27.performance_cache import PerformanceCache, PerformanceMonitor, cached, memoize

# ── PerformanceCache ───────────────────────────────────────────────


@pytest.fixture
def cache():
    return PerformanceCache(max_size=5, default_ttl=10)


def test_set_and_get(cache):
    cache.set("key1", "value1")
    assert cache.get("key1") == "value1"


def test_get_missing_key(cache):
    assert cache.get("nonexistent") is None


def test_ttl_expiration():
    # Use a cache with very short default TTL
    cache = PerformanceCache(max_size=5, default_ttl=1)
    cache.set("key1", "value1", ttl=1)
    # Should be available immediately
    assert cache.get("key1") == "value1"
    # After TTL expires it should be gone
    time.sleep(1.1)
    assert cache.get("key1") is None


def test_max_size_eviction(cache):
    for i in range(10):
        cache.set(f"key{i}", f"value{i}")
    stats = cache.get_stats()
    assert stats["total_entries"] <= 5


def test_invalidate_all(cache):
    cache.set("a", 1)
    cache.set("b", 2)
    count = cache.invalidate()
    assert count >= 2
    assert cache.get("a") is None
    assert cache.get("b") is None


def test_invalidate_by_pattern(cache):
    cache.set("user:1", "alice")
    cache.set("user:2", "bob")
    cache.set("post:1", "hello")
    count = cache.invalidate(pattern="user")
    assert count == 2
    assert cache.get("user:1") is None
    assert cache.get("post:1") == "hello"


def test_cached_call(cache):
    call_count = 0

    def expensive():
        nonlocal call_count
        call_count += 1
        return 42

    assert cache.cached_call(expensive) == 42
    assert cache.cached_call(expensive) == 42
    assert call_count == 1  # Only called once due to cache


def test_get_stats_structure(cache):
    cache.set("x", 1)
    stats = cache.get_stats()
    assert "total_entries" in stats
    assert "active_entries" in stats
    assert "max_size" in stats


# ── @cached decorator ──────────────────────────────────────────────


def test_cached_decorator():
    call_count = 0

    @cached(ttl=60)
    def compute(x):
        nonlocal call_count
        call_count += 1
        return x * 2

    assert compute(5) == 10
    assert compute(5) == 10
    assert call_count == 1


def test_cached_decorator_different_args():
    call_count = 0

    @cached(ttl=60)
    def compute(x):
        nonlocal call_count
        call_count += 1
        return x * 2

    compute(1)
    compute(2)
    assert call_count == 2


# ── @memoize decorator ────────────────────────────────────────────


def test_memoize_caches():
    call_count = 0

    @memoize(maxsize=32)
    def fib(n):
        nonlocal call_count
        call_count += 1
        if n < 2:
            return n
        return fib(n - 1) + fib(n - 2)

    assert fib(10) == 55
    # Without memoization, this would be ~177 calls
    assert call_count == 11


def test_memoize_cache_clear():
    @memoize(maxsize=32)
    def add(a, b):
        return a + b

    add(1, 2)
    add.cache_clear()
    # Should still work after clear
    assert add(1, 2) == 3


# ── PerformanceMonitor ─────────────────────────────────────────────


def test_record_and_get_metric():
    monitor = PerformanceMonitor()
    monitor.record_metric("latency", 0.5)
    monitor.record_metric("latency", 1.0)
    monitor.record_metric("latency", 1.5)

    stats = monitor.get_metric_stats("latency")
    assert stats["count"] == 3
    assert stats["min"] == 0.5
    assert stats["max"] == 1.5
    assert stats["avg"] == pytest.approx(1.0)


def test_get_metric_stats_empty():
    monitor = PerformanceMonitor()
    stats = monitor.get_metric_stats("nonexistent")
    # Returns empty dict for unknown metrics
    assert stats == {}
