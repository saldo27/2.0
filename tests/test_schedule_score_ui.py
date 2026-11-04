"""Shared Score del Reparto helpers used by Estadísticas / Ajuste Final / Revisión.

Covers ``compute_schedule_score_breakdown`` against a live generation scheduler
(the Estadísticas path) and a mutated post-adjustment schedule (the Ajuste Final
path), plus label coverage for the full UI breakdown.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch

import streamlit as st

from saldo27.app_streamlit import (
    _SCORE_COMPONENT_LABELS,
    compute_schedule_score_breakdown,
)
from saldo27.optimization_metrics import OptimizationMetrics
from saldo27.scheduler import Scheduler


class _Session(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __setattr__(self, key, value):
        self[key] = value


def _worker(worker_id: str, name: str, **overrides):
    worker = {
        "id": worker_id,
        "name": name,
        "target_shifts": 7,
        "_raw_target": 7,
        "work_percentage": 100,
        "work_periods": "",
        "mandatory_days": "",
        "days_off": "",
        "incompatible_with": [],
        "auto_calculate_shifts": True,
        "has_cadence": False,
        "no_last_post": False,
        "only_last_post": False,
        "is_incompatible": False,
    }
    worker.update(overrides)
    return worker


def _march_days(n: int = 14) -> list[datetime]:
    return [datetime(2026, 3, 1) + timedelta(days=i) for i in range(n)]


def _apply_balanced(scheduler: Scheduler) -> None:
    """Rotate A/B/C/D evenly and keep worker_assignments in sync for scoring."""
    roster = ["A", "B", "C", "D"]
    days = _march_days()
    assignments: dict[str, set[datetime]] = {wid: set() for wid in roster}
    schedule: dict[datetime, list[str]] = {}
    for offset, day in enumerate(days):
        w1, w2 = roster[(offset * 2) % 4], roster[(offset * 2 + 1) % 4]
        schedule[day] = [w1, w2]
        assignments[w1].add(day)
        assignments[w2].add(day)
    scheduler.schedule = schedule
    scheduler.worker_assignments = assignments


def _apply_unbalanced(scheduler: Scheduler) -> None:
    """Pile every post onto A — workload_balance must drop vs the balanced layout."""
    days = _march_days()
    scheduler.schedule = {day: ["A", "A"] for day in days}
    scheduler.worker_assignments = {
        "A": set(days),
        "B": set(),
        "C": set(),
        "D": set(),
    }


def _build_scheduler() -> Scheduler:
    workers = [
        _worker("A", "Ana"),
        _worker("B", "Bruno"),
        _worker("C", "Carla"),
        _worker("D", "Diego"),
    ]
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 14),
            "num_shifts": 2,
            "workers_data": workers,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 3,
            "gap_periods": [],
            "max_consecutive_weekends": 3,
        }
    )
    for worker in scheduler.workers_data:
        worker["target_shifts"] = 7
        worker["_raw_target"] = 7
    _apply_balanced(scheduler)
    return scheduler


def _session_for(scheduler: Scheduler) -> _Session:
    return _Session(
        {
            "scheduler": scheduler,
            "config": {"gap_between_shifts": 3, "gap_periods": []},
            "gap_periods_text": "",
            "workers_data": list(scheduler.workers_data),
            "revision_name_mapping": {},
        }
    )


def test_score_component_labels_cover_all_weights():
    assert set(_SCORE_COMPONENT_LABELS) == set(OptimizationMetrics.SCORE_WEIGHTS)


def test_compute_breakdown_for_generated_schedule_has_full_components():
    """Estadísticas path: score the live generated scheduler directly."""
    scheduler = _build_scheduler()
    with patch.object(st, "session_state", _session_for(scheduler)):
        breakdown = compute_schedule_score_breakdown(scheduler)

    assert breakdown is not None
    assert "overall" in breakdown
    assert 0.0 <= float(breakdown["overall"]) <= 100.0
    components = breakdown["components"]
    for key in OptimizationMetrics.SCORE_WEIGHTS:
        assert key in components
    assert set(breakdown["weights"]) == set(OptimizationMetrics.SCORE_WEIGHTS)


def test_post_fa_schedule_score_reflects_mutated_assignments():
    """Ajuste Final path: scoring the post-mutation schedule changes components."""
    scheduler = _build_scheduler()
    with patch.object(st, "session_state", _session_for(scheduler)):
        before = compute_schedule_score_breakdown(scheduler)
        assert before is not None

        _apply_unbalanced(scheduler)
        after = compute_schedule_score_breakdown(scheduler)
        assert after is not None

        assert float(after["components"]["workload_balance"]) < float(before["components"]["workload_balance"])
        assert float(after["overall"]) < float(before["overall"])


def test_pre_fa_snapshot_score_independent_of_live_schedule():
    """Estadísticas pre-FA: scoring a frozen snapshot must ignore later live edits."""
    scheduler = _build_scheduler()
    pre_schedule = dict(scheduler.schedule)
    pre_assignments = {wid: set(dates) for wid, dates in scheduler.worker_assignments.items()}

    with patch.object(st, "session_state", _session_for(scheduler)):
        _apply_unbalanced(scheduler)
        live_score = compute_schedule_score_breakdown(scheduler)

        scheduler.schedule = pre_schedule
        scheduler.worker_assignments = pre_assignments
        pre_score = compute_schedule_score_breakdown(scheduler)

    assert live_score is not None and pre_score is not None
    assert float(pre_score["components"]["workload_balance"]) > float(live_score["components"]["workload_balance"])
    assert float(pre_score["overall"]) > float(live_score["overall"])
