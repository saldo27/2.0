"""Tests for saldo27.balance_validator — schedule balance checking."""

import pytest

from saldo27.balance_validator import BalanceValidator


@pytest.fixture
def validator():
    return BalanceValidator()


# ── Balanced schedules ─────────────────────────────────────────────


def test_perfectly_balanced_schedule(validator, sample_schedule, sample_workers_data):
    """A schedule where all workers have exactly their target should pass."""
    # Build a controlled schedule: each worker gets exactly target_shifts
    schedule = {}
    day = 1
    for w in sample_workers_data:
        for _s in range(w["target_shifts"]):
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


def _single_worker_schedule(worker_id: str, count: int) -> dict:
    return {f"2026-03-{day:02d}": [worker_id] for day in range(1, count + 1)}


def _classification(validator, worker: dict, count: int) -> str:
    result = validator.validate_schedule_balance(_single_worker_schedule(worker["id"], count), [worker])
    return next(level for level, infos in result["violations"].items() if infos)


@pytest.mark.parametrize(
    ("count", "expected"),
    [(20, "within_tolerance"), (22, "within_tolerance"), (18, "within_tolerance"), (17, "critical")],
)
def test_classification_uses_strict_and_absolute_limits(validator, count, expected):
    worker = {"id": "W1", "target_shifts": 20, "work_percentage": 100}
    assert _classification(validator, worker, count) == expected


def test_classification_emergency_band_between_strict_and_absolute(validator):
    worker = {"id": "W1", "target_shifts": 25, "work_percentage": 100}
    # strict max int(27.5)=27, absolute max int(28.25)=28
    assert _classification(validator, worker, 27) == "within_tolerance"
    assert _classification(validator, worker, 28) == "within_emergency"
    assert _classification(validator, worker, 29) == "critical"


def test_classification_manual_worker_is_exact(validator):
    worker = {"id": "W1", "target_shifts": 20, "work_percentage": 100, "auto_calculate_shifts": False}
    assert _classification(validator, worker, 20) == "within_tolerance"
    assert _classification(validator, worker, 21) == "critical"


def test_classification_part_time_limit_is_scaled(validator):
    worker = {"id": "W1", "target_shifts": 20, "work_percentage": 50}
    # 50 % → strict 5 %, absolute 6.5 %: max 21 either way
    assert _classification(validator, worker, 21) == "within_tolerance"
    assert _classification(validator, worker, 22) == "critical"


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
        "DOC001",
        "DOC002",
        sample_schedule,
        sample_workers_data,
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
