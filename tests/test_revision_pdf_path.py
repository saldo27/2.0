"""Revisión path mirrors Luis’s PDF import + plantilla workflow.

Uses docs/MarzoJunio26.pdf and docs/Nav26-Jun27.json / reparto JSON so failures
match what the Revisión tab shows after CalendarFileProcessor → ScheduleAnalyzer
→ build_revision_display_scheduler → score / Verificación.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import streamlit as st

from saldo27.app_streamlit import (
    build_revision_display_scheduler,
    check_violations,
    compute_schedule_score_breakdown,
)
from saldo27.application.generation_flow import collect_constraint_violations
from saldo27.schedule_analyzer import CalendarFileProcessor, ScheduleAnalyzer
from saldo27.utilities import normalize_gap_periods

DOCS = Path(__file__).resolve().parents[1] / "docs"
PDF_PATH = DOCS / "MarzoJunio26.pdf"
PLANTILLA_PATH = DOCS / "Nav26-Jun27.json"
REPARTO_PATH = DOCS / "reparto_20261223_20270630.json"


class _Session(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __setattr__(self, key, value):
        self[key] = value


def _load_plantilla(path: Path = PLANTILLA_PATH) -> list[dict]:
    workers = json.loads(path.read_text())
    if isinstance(workers, dict):
        workers = workers.get("workers_data") or workers.get("workers") or []
    for worker in workers:
        worker.setdefault("name", worker["id"])
    return workers


def _analyzer_from_pdf(shifts_per_day: int = 4) -> ScheduleAnalyzer:
    text = CalendarFileProcessor().extract_text_from_pdf(PDF_PATH.read_bytes())
    analyzer = ScheduleAnalyzer(
        start_date=datetime(2026, 3, 1),
        calendar_text=text,
        holidays=[],
        shifts_per_day=shifts_per_day,
    )
    analyzer.parse_calendar()
    assert len(analyzer.calendar_data) > 30
    return analyzer


def test_pdf_plantilla_revision_balance_and_weekend_not_zero():
    """Mar–Jun PDF + Nav–Jun plantilla (no live gen) must not floor Balance/FS at 0.

    Manual plantilla targets (e.g. ANGEL=4) vs ~18 PDF assignments previously
    overlaid as scoring objectives; fill>1 skipped underfill scale → zeros.
    """
    plantilla = _load_plantilla()
    analyzer = _analyzer_from_pdf()
    session = _Session(
        scheduler=None,
        config={"gap_between_shifts": 4, "gap_periods": [], "max_consecutive_weekends": 3},
        workers_data=plantilla,
        schedule=None,
        revision_name_mapping={},
    )
    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(analyzer, workers_config=plantilla)
        breakdown = compute_schedule_score_breakdown(display, scoring_workers=None)

    assert breakdown is not None
    assert breakdown["components"]["workload_balance"] > 0.0
    assert breakdown["components"]["weekend_balance"] > 0.0


def test_pdf_revision_gap_periods_from_session_adjust_dias_minimos():
    """Sidebar gap_periods covering the PDF must change Días mínimos floors."""
    plantilla = _load_plantilla()
    analyzer = _analyzer_from_pdf()
    periods = normalize_gap_periods(
        [{"start_date": datetime(2026, 3, 1), "end_date": datetime(2026, 6, 30), "gap": 2}]
    )
    session = _Session(
        scheduler=None,
        config={"gap_between_shifts": 4, "gap_periods": periods, "max_consecutive_weekends": 3},
        workers_data=plantilla,
        schedule=None,
        revision_name_mapping={},
    )
    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(analyzer, workers_config=plantilla)
        display.gap_periods = []  # stale
        violations = check_violations(display)

    assert display.gap_periods == periods
    assert all("mínimo 4" not in item for item in violations.get("gap_minimo") or [])
    # Close pairs that remain illegal under gap 2 still report mínimo 2.
    assert any("mínimo 2" in item for item in violations.get("gap_minimo") or [])


def test_reparto_dual_mandatory_incompat_not_penalised_on_revision():
    """KIKO+VALLE both mandatory on 25-12-2026 in exported reparto must not appear as incompat."""
    data = json.loads(REPARTO_PATH.read_text())
    workers = data["workers_data"]
    for worker in workers:
        worker.setdefault("name", worker["id"])
    calendar_data = []
    for ds, posts in data["schedule"].items():
        day = datetime.strptime(ds, "%Y-%m-%d").date()  # PDF analyzer uses date
        positioned = [(name, i) for i, name in enumerate(posts) if name]
        calendar_data.append(
            {"date": day, "workers": [n for n, _ in positioned], "workers_with_position": positioned}
        )

    class _Analyzer:
        shifts_per_day = 4

        def __init__(self, rows):
            self.calendar_data = rows
            self.holidays = []

    periods = normalize_gap_periods(data["config"].get("gap_periods") or [])
    session = _Session(
        scheduler=None,
        config={
            "gap_between_shifts": data["config"]["gap_between_shifts"],
            "gap_periods": periods,
            "max_consecutive_weekends": 3,
        },
        workers_data=workers,
        schedule=None,
        revision_name_mapping={},
    )
    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(_Analyzer(calendar_data), workers_config=workers)
        violations = check_violations(display)
        breakdown = compute_schedule_score_breakdown(display, scoring_workers=None)

    assert data["schedule"]["2026-12-25"] == ["JULIA", "KIKO", "VALLE", "BEA"]
    assert violations.get("incompatibilidades") == []
    assert breakdown is not None
    assert breakdown["components"]["constraint_penalty"] == 0.0


def test_mandatory_assigned_in_pdf_not_flagged_even_with_date_objects():
    """Mandatory present in the imported calendar must not show 'falta obligatorio'."""
    plantilla = _load_plantilla()
    analyzer = _analyzer_from_pdf()
    # Pick a worker/day that the PDF actually contains.
    sample = next(row for row in analyzer.calendar_data if row.get("workers"))
    worker_id = sample["workers"][0]
    day: date = sample["date"] if isinstance(sample["date"], date) else sample["date"].date()
    for worker in plantilla:
        if worker["id"] == worker_id:
            worker["mandatory_days"] = day.strftime("%d-%m-%Y")

    session = _Session(
        scheduler=None,
        config={"gap_between_shifts": 4, "gap_periods": [], "max_consecutive_weekends": 3},
        workers_data=plantilla,
        schedule=None,
        revision_name_mapping={},
    )
    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(analyzer, workers_config=plantilla)
        # Simulate fragile assignment typing: mix date into tracking.
        assigned = set(display.worker_assignments.get(worker_id, set()))
        assigned.add(day)  # raw date alongside datetimes
        display.worker_assignments[worker_id] = assigned
        violations = check_violations(display)

    assert not any(
        item.startswith(f"{worker_id}: falta obligatorio {day.strftime('%d-%m-%Y')}")
        for item in violations.get("mandatory") or []
    )


def test_mandatory_outside_imported_calendar_days_not_flagged():
    """A mandatory on a day absent from the PDF calendar must not be 'falta obligatorio'."""
    plantilla = _load_plantilla()
    analyzer = _analyzer_from_pdf()
    # 01-03-2026 is before the PDF's first Monday block (starts 02-03-2026).
    for worker in plantilla:
        if worker["id"] == "BEA":
            worker["mandatory_days"] = "01-03-2026"

    session = _Session(
        scheduler=None,
        config={},
        workers_data=plantilla,
        schedule=None,
        revision_name_mapping={},
    )
    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(analyzer, workers_config=plantilla)
        # Widen start so a naive range check would include Mar 1.
        display.start_date = datetime(2026, 3, 1)
        violations = collect_constraint_violations(display)

    assert "BEA: falta obligatorio 01-03-2026" not in (violations.get("mandatory") or [])
