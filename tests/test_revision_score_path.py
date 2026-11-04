"""Revisión UI scoring path — mirrors Score breakdown + Verificación wiring.

These tests exercise ``compute_schedule_score_breakdown`` / ``check_violations``
with a mocked Streamlit session (live overlay, gap_periods, name mapping), not
OptimizationMetrics alone. They encode the three regressions Luis still saw
after PR #33.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import streamlit as st

from saldo27.app_streamlit import (
    build_revision_display_scheduler,
    check_violations,
    compute_schedule_score_breakdown,
)
from saldo27.scheduler import Scheduler
from saldo27.utilities import normalize_gap_periods


class _Session(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __setattr__(self, key, value):
        self[key] = value


class _FakeAnalyzer:
    def __init__(self, calendar_data, shifts_per_day=2):
        self.calendar_data = calendar_data
        self.shifts_per_day = shifts_per_day
        self.holidays = []


def _plantilla_worker(worker_id: str, name: str, **overrides):
    worker = {
        "id": worker_id,
        "name": name,
        "target_shifts": 10,
        "_raw_target": 10,
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


def _live_scheduler(workers, *, gap_between_shifts=4, gap_periods=None):
    scheduler = Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 31),
            "num_shifts": 2,
            "workers_data": [dict(w) for w in workers],
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": gap_between_shifts,
            "gap_periods": gap_periods or [],
            "max_consecutive_weekends": 3,
        }
    )
    for worker, src in zip(scheduler.workers_data, workers, strict=False):
        worker["target_shifts"] = src.get("target_shifts", 10)
        worker["_raw_target"] = src.get("_raw_target", worker["target_shifts"])
        worker["mandatory_days"] = src.get("mandatory_days", "")
        worker["incompatible_with"] = list(src.get("incompatible_with") or [])
    return scheduler


def _calendar_for_days(assignments: dict[str, list[int]]):
    """Build analyzer calendar_data from worker → day-of-month lists."""
    by_day: dict[int, list[tuple[str, int]]] = {}
    for worker_id, days in assignments.items():
        for day in days:
            slots = by_day.setdefault(day, [])
            slots.append((worker_id, len(slots)))
    calendar = []
    for day in sorted(by_day):
        positioned = by_day[day]
        calendar.append(
            {
                "date": datetime(2026, 3, day),
                "workers": [name for name, _ in positioned],
                "workers_with_position": positioned,
            }
        )
    return calendar


def test_revision_score_balance_and_weekend_survive_sixty_percent_live_overlay():
    """Revisión Score labels Balance de carga / Fines de semana must not show 0 on ~60% fill."""
    plantilla = [
        _plantilla_worker("DOC1", "Ana", target_shifts=10, _raw_target=10),
        _plantilla_worker("DOC2", "Bea", target_shifts=10, _raw_target=10),
    ]
    gen = _live_scheduler(plantilla, gap_periods=[])
    periods = normalize_gap_periods([{"start_date": datetime(2026, 3, 1), "end_date": datetime(2026, 3, 31), "gap": 2}])
    session = _Session(
        scheduler=gen,
        config={"gap_between_shifts": 4, "gap_periods": periods, "max_consecutive_weekends": 3},
        workers_data=plantilla,
        schedule=gen.schedule,
        revision_name_mapping={},
    )
    analyzer = _FakeAnalyzer(
        _calendar_for_days(
            {
                "Ana": [1, 5, 9, 13, 17, 21],
                "Bea": [2, 6, 10, 14, 18, 22],
            }
        )
    )

    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(analyzer, workers_config=plantilla)
        breakdown = compute_schedule_score_breakdown(display, scoring_workers=list(gen.workers_data))

    assert breakdown is not None
    assert breakdown["components"]["workload_balance"] > 0.0
    assert breakdown["components"]["weekend_balance"] > 0.0


def test_revision_verificacion_skips_dual_mandatory_incompat_via_name_mapping():
    """Mandatory-caused incompat must not appear under Incompatibilidades on Revisión."""
    plantilla = [
        _plantilla_worker(
            "DOC1",
            "Ana García",
            mandatory_days="01-03-2026",
            incompatible_with=["DOC2"],
            target_shifts=2,
            _raw_target=2,
        ),
        _plantilla_worker(
            "DOC2",
            "Bea Pérez",
            mandatory_days="01-03-2026",
            incompatible_with=["DOC1"],
            target_shifts=2,
            _raw_target=2,
        ),
    ]
    gen = _live_scheduler(plantilla)
    session = _Session(
        scheduler=gen,
        config={"gap_between_shifts": 4, "gap_periods": [], "max_consecutive_weekends": 3},
        workers_data=plantilla,
        schedule=gen.schedule,
        revision_name_mapping={"Ana": "Ana García", "Bea": "Bea Pérez"},
    )
    analyzer = _FakeAnalyzer(
        [
            {
                "date": datetime(2026, 3, 1),
                "workers": ["Ana", "Bea"],
                "workers_with_position": [("Ana", 0), ("Bea", 1)],
            }
        ]
    )

    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(analyzer, workers_config=plantilla)
        # Simulate incomplete stamp: clear mandatory/incompat that matching missed.
        for worker in display.workers_data:
            worker["mandatory_days"] = ""
            worker["incompatible_with"] = []
        violations = check_violations(display)
        breakdown = compute_schedule_score_breakdown(display, scoring_workers=list(gen.workers_data))

    assert violations.get("incompatibilidades") == []
    assert breakdown is not None
    assert breakdown["components"]["constraint_penalty"] == 0.0


def test_revision_dias_minimos_uses_session_gap_periods_over_live():
    """Sidebar gap_periods must drive Días mínimos even when live generation has none/stale."""
    plantilla = [_plantilla_worker("DOC1", "Ana", target_shifts=4, _raw_target=4)]
    # Live was generated without gap variable.
    gen = _live_scheduler(plantilla, gap_between_shifts=4, gap_periods=[])
    session_periods = normalize_gap_periods(
        [{"start_date": datetime(2026, 3, 1), "end_date": datetime(2026, 3, 15), "gap": 2}]
    )
    session = _Session(
        scheduler=gen,
        config={"gap_between_shifts": 4, "gap_periods": session_periods, "max_consecutive_weekends": 3},
        workers_data=plantilla,
        schedule=gen.schedule,
        revision_name_mapping={},
    )
    # Mon–Wed inside relaxed period (OK under gap 2) + Mon–Wed outside (violation under 4).
    analyzer = _FakeAnalyzer(_calendar_for_days({"Ana": [2, 4, 16, 18]}))

    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(analyzer, workers_config=plantilla)
        # Stale live periods must not stick if somehow present on the display object.
        display.gap_periods = normalize_gap_periods(
            [{"start_date": datetime(2026, 1, 1), "end_date": datetime(2026, 1, 31), "gap": 6}]
        )
        violations = check_violations(display)

    assert violations["gap_minimo"] == ["Ana: 16-03-2026 → 18-03-2026 (2 días, mínimo 4)"]
    assert display.gap_periods == session_periods
