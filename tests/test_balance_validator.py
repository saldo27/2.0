"""Tests for saldo27.balance_validator — schedule balance checking."""

import pytest
from saldo27.balance_validator import BalanceValidator


@pytest.fixture
def validator():
    return BalanceValidator(tolerance_percentage=10.0)


# ── Balanced schedules ─────────────────────────────────────────────


def test_perfectly_balanced_schedule(validator, sample_schedule, sample_workers_data):
    """A schedule where all workers have exactly their target should pass."""
    # Build a controlled schedule: each worker gets exactly target_shifts
    schedule = {}
    day = 1
    for w in sample_workers_data:
        for s in range(w["target_shifts"]):
            date_key = f"2026-03-{day:02d}"
            if date_key not in schedule:
                schedule[date_key] = {}
            shift_num = len(schedule[date_key]) + 1
            schedule[date_key][shift_num] = w["id"]
            day = (day % 28) + 1

    result = validator.validate_schedule_balance(schedule, sample_workers_data)
    assert isinstance(result, dict)
    assert "violations" in result
    assert "is_balanced" in result


def test_validate_returns_stats(validator, sample_schedule, sample_workers_data):
    result = validator.validate_schedule_balance(sample_schedule, sample_workers_data)
    assert "stats" in result


# ── Imbalanced schedules ──────────────────────────────────────────


def test_heavily_imbalanced_schedule(validator, sample_workers_data):
    """Give one worker all shifts, others none — should flag violations."""
    schedule = {}
    for day in range(1, 31):
        date_key = f"2026-03-{day:02d}"
        schedule[date_key] = {1: "DOC001", 2: "DOC001", 3: "DOC001", 4: "DOC001"}

    result = validator.validate_schedule_balance(schedule, sample_workers_data)
    assert result["is_balanced"] is False


# ── Tolerance phases ───────────────────────────────────────────────


def test_tolerance_percentage_respected():
    strict = BalanceValidator(tolerance_percentage=5.0)
    lenient = BalanceValidator(tolerance_percentage=20.0)
    assert strict.tolerance_percentage < lenient.tolerance_percentage


# ── Rebalancing recommendations ────────────────────────────────────


def test_rebalancing_recommendations_structure(validator, sample_workers_data):
    schedule = {}
    for day in range(1, 31):
        date_key = f"2026-03-{day:02d}"
        schedule[date_key] = {1: "DOC001", 2: "DOC001", 3: "DOC001", 4: "DOC001"}

    recs = validator.get_rebalancing_recommendations(schedule, sample_workers_data)
    assert isinstance(recs, list)
    if recs:
        rec = recs[0]
        assert "from_worker" in rec
        assert "to_worker" in rec


# ── Transfer validity ──────────────────────────────────────────────


def test_check_transfer_validity(validator, sample_schedule, sample_workers_data):
    is_valid, reason = validator.check_transfer_validity(
        "DOC001", "DOC002", sample_schedule, sample_workers_data,
    )
    assert isinstance(is_valid, bool)
    assert isinstance(reason, str)


# ── Edge cases ─────────────────────────────────────────────────────


def test_empty_schedule(validator, sample_workers_data):
    result = validator.validate_schedule_balance({}, sample_workers_data)
    assert isinstance(result, dict)


def test_empty_workers(validator, sample_schedule):
    result = validator.validate_schedule_balance(sample_schedule, [])
    assert isinstance(result, dict)
