"""Tests for saldo27.change_tracker — undo/redo and audit trail."""

import pytest
from saldo27.change_tracker import ChangeTracker, OperationType, ChangeRecord


@pytest.fixture
def tracker():
    return ChangeTracker(max_history=50)


# ── Recording changes ──────────────────────────────────────────────


def test_record_change_returns_id(tracker):
    change_id = tracker.record_change(
        user_id="admin",
        operation_type=OperationType.ASSIGN_WORKER,
        description="Assigned DOC001 to shift 1",
        rollback_data={"shift": 1, "prev_worker": None},
        forward_data={"shift": 1, "worker": "DOC001"},
        affected_workers=["DOC001"],
        affected_dates=["2026-03-15"],
    )
    assert isinstance(change_id, str)
    assert len(change_id) > 0


def test_record_multiple_changes(tracker):
    for i in range(5):
        tracker.record_change(
            user_id="admin",
            operation_type=OperationType.ASSIGN_WORKER,
            description=f"Change {i}",
            rollback_data={},
            forward_data={},
        )
    history = tracker.get_change_history()
    assert len(history) == 5


# ── Undo / redo ────────────────────────────────────────────────────


def test_can_undo_after_change(tracker):
    tracker.record_change(
        user_id="admin",
        operation_type=OperationType.ASSIGN_WORKER,
        description="test",
        rollback_data={"x": 1},
        forward_data={"x": 2},
    )
    assert tracker.can_undo() is True


def test_cannot_undo_with_no_changes(tracker):
    assert tracker.can_undo() is False


def test_cannot_redo_with_no_undone_changes(tracker):
    assert tracker.can_redo() is False


def test_get_undo_operation(tracker):
    tracker.record_change(
        user_id="admin",
        operation_type=OperationType.ASSIGN_WORKER,
        description="assign",
        rollback_data={"worker": None},
        forward_data={"worker": "DOC001"},
    )
    op = tracker.get_undo_operation()
    assert op is not None
    assert op.rollback_data == {"worker": None}


def test_mark_undo_then_redo(tracker):
    tracker.record_change(
        user_id="admin",
        operation_type=OperationType.ASSIGN_WORKER,
        description="assign",
        rollback_data={},
        forward_data={},
    )
    # Undo
    undone = tracker.mark_undo_applied()
    assert undone is not None
    assert tracker.can_redo() is True

    # Redo
    redone = tracker.mark_redo_applied()
    assert redone is not None
    assert tracker.can_redo() is False


def test_new_change_clears_redo_stack(tracker):
    tracker.record_change(
        user_id="admin",
        operation_type=OperationType.ASSIGN_WORKER,
        description="first",
        rollback_data={},
        forward_data={},
    )
    tracker.mark_undo_applied()
    assert tracker.can_redo() is True

    # New change should clear redo
    tracker.record_change(
        user_id="admin",
        operation_type=OperationType.ASSIGN_WORKER,
        description="second",
        rollback_data={},
        forward_data={},
    )
    assert tracker.can_redo() is False


# ── History filtering ──────────────────────────────────────────────


def test_filter_by_user(tracker):
    tracker.record_change("alice", OperationType.ASSIGN_WORKER, "a", {}, {})
    tracker.record_change("bob", OperationType.ASSIGN_WORKER, "b", {}, {})
    tracker.record_change("alice", OperationType.ASSIGN_WORKER, "c", {}, {})

    history = tracker.get_change_history(user_id="alice")
    assert len(history) == 2


def test_filter_by_operation_type(tracker):
    tracker.record_change("admin", OperationType.ASSIGN_WORKER, "a", {}, {})
    tracker.record_change("admin", OperationType.SWAP_WORKERS, "b", {}, {})

    history = tracker.get_change_history(
        operation_types=[OperationType.SWAP_WORKERS]
    )
    assert len(history) == 1


# ── Audit trail ────────────────────────────────────────────────────


def test_audit_trail_by_worker(tracker):
    tracker.record_change(
        "admin", OperationType.ASSIGN_WORKER, "assign",
        {}, {}, affected_workers=["DOC001"],
    )
    tracker.record_change(
        "admin", OperationType.ASSIGN_WORKER, "assign other",
        {}, {}, affected_workers=["DOC002"],
    )

    trail = tracker.get_audit_trail(worker_id="DOC001")
    assert len(trail) == 1


def test_audit_trail_by_date(tracker):
    tracker.record_change(
        "admin", OperationType.ASSIGN_WORKER, "assign",
        {}, {}, affected_dates=["2026-03-15"],
    )

    trail = tracker.get_audit_trail(date="2026-03-15")
    assert len(trail) == 1


# ── Export / import ────────────────────────────────────────────────


def test_export_import_roundtrip(tracker):
    tracker.record_change("admin", OperationType.ASSIGN_WORKER, "test", {}, {})
    exported = tracker.export_audit_data()

    new_tracker = ChangeTracker()
    result = new_tracker.import_audit_data(exported)
    assert result is True
    assert len(new_tracker.get_change_history()) == 1


# ── Statistics ─────────────────────────────────────────────────────


def test_get_statistics(tracker):
    tracker.record_change("admin", OperationType.ASSIGN_WORKER, "a", {}, {})
    stats = tracker.get_statistics()
    assert stats["total_changes"] == 1
    assert "operation_counts" in stats


def test_clear_history(tracker):
    tracker.record_change("admin", OperationType.ASSIGN_WORKER, "a", {}, {})
    tracker.clear_history()
    assert len(tracker.get_change_history()) == 0


# ── Max history enforcement ────────────────────────────────────────


def test_max_history_limit():
    tracker = ChangeTracker(max_history=3)
    for i in range(10):
        tracker.record_change("admin", OperationType.ASSIGN_WORKER, f"c{i}", {}, {})
    history = tracker.get_change_history()
    assert len(history) <= 3
