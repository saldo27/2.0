import ast
import inspect
import textwrap
from datetime import datetime
from unittest.mock import MagicMock

from saldo27.advanced_distribution_engine import AdvancedDistributionEngine
from saldo27.scheduler import Scheduler
from saldo27.scheduler_config import SchedulerConfig
from saldo27.scheduler_core import SchedulerCore


def test_sanitize_restored_attempt_state_removes_non_mandatory_prefill():
    workers_data = [
        {
            "id": "DOC001",
            "name": "DOC001",
            "target_shifts": 3,
            "work_percentage": 100,
            "mandatory_days": "01-03-2026",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
        {
            "id": "DOC002",
            "name": "DOC002",
            "target_shifts": 3,
            "work_percentage": 100,
            "mandatory_days": "",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
        {
            "id": "DOC003",
            "name": "DOC003",
            "target_shifts": 3,
            "work_percentage": 100,
            "mandatory_days": "",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
        {
            "id": "DOC004",
            "name": "DOC004",
            "target_shifts": 3,
            "work_percentage": 100,
            "mandatory_days": "",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
    ]
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 3),
            "num_shifts": 4,
            "workers_data": workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 4,
            "max_consecutive_weekends": 3,
        }
    )
    scheduler_core = SchedulerCore(scheduler)

    assert scheduler_core._initialize_schedule_phase() is True
    assert scheduler_core._assign_mandatory_phase() is True

    assert len(scheduler.schedule_builder._locked_mandatory) == 1

    contaminated_date = datetime(2026, 3, 2)
    scheduler.schedule[contaminated_date][0] = "DOC002"
    scheduler._synchronize_tracking_data()

    stats = scheduler_core._sanitize_restored_attempt_state()

    assert scheduler.schedule[contaminated_date][0] is None
    assert scheduler.worker_assignments["DOC002"] == set()
    assert scheduler.worker_shift_counts["DOC002"] == 0

    assert stats == {
        "total_slots": 12,
        "protected_slots": 1,
        "stray_prefilled_slots": 1,
        "empty_slots": 11,
    }


# ── Balance coordination regression tests ─────────────────────────────────────


def _make_scheduler_core(workers_data, start, end, num_shifts=2, gap=3):
    """Helper: create a Scheduler + SchedulerCore for regression tests."""
    scheduler = Scheduler(
        {
            "start_date": start,
            "end_date": end,
            "num_shifts": num_shifts,
            "workers_data": workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": gap,
            "max_consecutive_weekends": 3,
        }
    )
    return SchedulerCore(scheduler), scheduler


def test_scheduler_config_exposes_default_balance_tolerance():
    """DEFAULT_BALANCE_TOLERANCE must exist and be included in get_default_config."""
    assert hasattr(SchedulerConfig, "DEFAULT_BALANCE_TOLERANCE")
    cfg = SchedulerConfig.get_default_config()
    assert "balance_tolerance" in cfg
    assert cfg["balance_tolerance"] == SchedulerConfig.DEFAULT_BALANCE_TOLERANCE


def test_shared_balance_validator_wired_into_iterative_optimizer():
    """SchedulerCore must wire its shared BalanceValidator into IterativeOptimizer."""
    workers_data = [
        {
            "id": "W1",
            "name": "W1",
            "target_shifts": 4,
            "work_percentage": 100,
            "mandatory_days": "",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
        {
            "id": "W2",
            "name": "W2",
            "target_shifts": 4,
            "work_percentage": 100,
            "mandatory_days": "",
            "days_off": "",
            "work_periods": "",
            "incompatible_with": [],
            "is_incompatible": False,
        },
    ]
    core, _ = _make_scheduler_core(workers_data, datetime(2026, 3, 1), datetime(2026, 3, 20))
    # Both must be the SAME object
    assert core.iterative_optimizer.balance_validator is core._shared_balance_validator


def test_advanced_distribution_engine_blocks_overloaded_worker():
    """_calculate_global_balance_bonus must return -inf for a worker > target + tolerance."""
    worker = {
        "id": "W1",
        "target_shifts": 4,
        "mandatory_days": "",
    }
    scheduler = MagicMock()
    scheduler.config = {"balance_tolerance": 1}
    # Worker has 6 non-mandatory assignments → 2 above target (> target+1)
    scheduler.worker_assignments = {"W1": {datetime(2026, 3, i) for i in range(1, 7)}}
    scheduler.workers_data = [worker]

    builder = MagicMock()
    builder.date_utils = None

    engine = AdvancedDistributionEngine(scheduler, builder)
    bonus = engine._calculate_global_balance_bonus("W1")
    assert bonus == float("-inf"), f"Expected -inf for overloaded worker, got {bonus}"


def test_advanced_distribution_engine_allows_underloaded_worker():
    """_calculate_global_balance_bonus must return positive bonus for worker below target."""
    worker = {
        "id": "W1",
        "target_shifts": 6,
        "mandatory_days": "",
    }
    scheduler = MagicMock()
    scheduler.config = {"balance_tolerance": 1}
    # Worker has only 3 assignments → 3 below target
    scheduler.worker_assignments = {"W1": {datetime(2026, 3, i) for i in range(1, 4)}}
    scheduler.workers_data = [worker]

    builder = MagicMock()
    builder.date_utils = None

    engine = AdvancedDistributionEngine(scheduler, builder)
    bonus = engine._calculate_global_balance_bonus("W1")
    assert bonus > 0, f"Expected positive bonus for underloaded worker, got {bonus}"


def test_strict_balance_optimizer_not_called_in_finalization_phase():
    """_finalization_phase must NOT call balance_optimizer.optimize_balance.

    The single authoritative balance pass is Phase 3.6. A second call in
    _finalization_phase would create an interference loop.
    """
    src = textwrap.dedent(inspect.getsource(SchedulerCore._finalization_phase))
    tree = ast.parse(src)
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(getattr(node, "func", None), ast.Attribute)
        and node.func.attr == "optimize_balance"
    ]
    assert calls == [], (
        "_finalization_phase still calls optimize_balance — "
        "remove the duplicate StrictBalanceOptimizer invocation"
    )

