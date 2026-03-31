"""Tests for saldo27.event_bus — pub/sub event system."""

from datetime import datetime

import pytest

from saldo27.event_bus import EventBus, EventType, ScheduleEvent, reset_event_bus


@pytest.fixture(autouse=True)
def clean_event_bus():
    """Reset singleton between tests."""
    reset_event_bus()
    yield
    reset_event_bus()


@pytest.fixture
def bus():
    return EventBus(max_history=100)


# ── ScheduleEvent ──────────────────────────────────────────────────


def test_event_creation():
    event = ScheduleEvent(event_type=EventType.SHIFT_ASSIGNED)
    assert event.event_type == EventType.SHIFT_ASSIGNED
    assert isinstance(event.timestamp, datetime)
    assert event.event_id  # auto-generated


def test_event_to_dict():
    event = ScheduleEvent(
        event_type=EventType.WORKER_ADDED,
        user_id="admin",
        data={"worker": "DOC001"},
    )
    d = event.to_dict()
    assert d["event_type"] == EventType.WORKER_ADDED.value
    assert d["user_id"] == "admin"
    assert d["data"]["worker"] == "DOC001"


def test_event_roundtrip():
    original = ScheduleEvent(
        event_type=EventType.SCHEDULE_GENERATED,
        user_id="test",
        data={"score": 85.5},
    )
    d = original.to_dict()
    restored = ScheduleEvent.from_dict(d)
    assert restored.event_type == original.event_type
    assert restored.user_id == original.user_id
    assert restored.data == original.data


# ── EventBus subscribe / publish ───────────────────────────────────


def test_subscribe_and_publish(bus):
    received = []
    bus.subscribe(EventType.SHIFT_ASSIGNED, lambda e: received.append(e))

    event = ScheduleEvent(event_type=EventType.SHIFT_ASSIGNED, data={"shift": 1})
    bus.publish(event)

    assert len(received) == 1
    assert received[0].data["shift"] == 1


def test_publish_does_not_trigger_other_subscribers(bus):
    received = []
    bus.subscribe(EventType.SHIFT_ASSIGNED, lambda e: received.append(e))

    bus.publish(ScheduleEvent(event_type=EventType.WORKER_ADDED))
    assert len(received) == 0


def test_multiple_subscribers(bus):
    count_a = []
    count_b = []
    bus.subscribe(EventType.SHIFT_ASSIGNED, lambda e: count_a.append(1))
    bus.subscribe(EventType.SHIFT_ASSIGNED, lambda e: count_b.append(1))

    bus.publish(ScheduleEvent(event_type=EventType.SHIFT_ASSIGNED))
    assert len(count_a) == 1
    assert len(count_b) == 1


def test_unsubscribe(bus):
    received = []
    callback = lambda e: received.append(e)
    bus.subscribe(EventType.SHIFT_ASSIGNED, callback)
    bus.unsubscribe(EventType.SHIFT_ASSIGNED, callback)

    bus.publish(ScheduleEvent(event_type=EventType.SHIFT_ASSIGNED))
    assert len(received) == 0


# ── EventBus emit shorthand ───────────────────────────────────────


def test_emit_creates_and_publishes_event(bus):
    received = []
    bus.subscribe(EventType.WORKER_ADDED, lambda e: received.append(e))

    bus.emit(EventType.WORKER_ADDED, user_id="admin", worker="DOC001")
    assert len(received) == 1
    assert received[0].data["worker"] == "DOC001"


# ── Event history ──────────────────────────────────────────────────


def test_event_history_records_events(bus):
    bus.emit(EventType.SHIFT_ASSIGNED)
    bus.emit(EventType.SHIFT_UNASSIGNED)

    history = bus.get_event_history()
    assert len(history) == 2


def test_event_history_filter_by_type(bus):
    bus.emit(EventType.SHIFT_ASSIGNED)
    bus.emit(EventType.WORKER_ADDED)
    bus.emit(EventType.SHIFT_ASSIGNED)

    history = bus.get_event_history(event_type=EventType.SHIFT_ASSIGNED)
    assert len(history) == 2


def test_event_history_limit(bus):
    for _ in range(10):
        bus.emit(EventType.SHIFT_ASSIGNED)

    history = bus.get_event_history(limit=3)
    assert len(history) == 3


def test_clear_history(bus):
    bus.emit(EventType.SHIFT_ASSIGNED)
    bus.clear_history()
    assert len(bus.get_event_history()) == 0


def test_max_history_enforced():
    bus = EventBus(max_history=5)
    for _ in range(10):
        bus.emit(EventType.SHIFT_ASSIGNED)

    history = bus.get_event_history()
    assert len(history) <= 5


# ── Stats ──────────────────────────────────────────────────────────


def test_get_stats(bus):
    bus.emit(EventType.SHIFT_ASSIGNED)
    stats = bus.get_stats()
    assert isinstance(stats, dict)
