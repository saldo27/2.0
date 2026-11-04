"""Pinned Revisión regression: Luis Guardias Verano Tripletes fixtures.

Fixtures (committed under tests/fixtures/):
  - Guardias_Verano_Tripletes.pdf
  - Nav26-Jun27.json

Settings: period 2026-12-23→2027-06-30, gap 3 then 4, max consecutive weekends 3.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import streamlit as st

from saldo27.app_streamlit import (
    build_revision_display_scheduler,
    check_violations,
    compute_schedule_score_breakdown,
)
from saldo27.schedule_analyzer import CalendarFileProcessor, ScheduleAnalyzer
from saldo27.utilities import normalize_gap_periods

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PDF_PATH = FIXTURES / "Guardias_Verano_Tripletes.pdf"
PLANTILLA_PATH = FIXTURES / "Nav26-Jun27.json"

_GAP_MSG = re.compile(
    r"(?P<worker>.+): (?P<d1>\d{2}-\d{2}-\d{4}) → (?P<d2>\d{2}-\d{2}-\d{4}) "
    r"\((?P<between>\d+) días, mínimo (?P<min>\d+)\)"
)


class _Session(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __setattr__(self, key, value):
        self[key] = value


def _load_plantilla() -> list[dict]:
    workers = json.loads(PLANTILLA_PATH.read_text())
    if isinstance(workers, dict):
        workers = workers.get("workers_data") or workers.get("workers") or []
    for worker in workers:
        worker.setdefault("name", worker["id"])
    return workers


def _luis_gap_periods():
    return normalize_gap_periods(
        [
            {"start_date": datetime(2026, 12, 23), "end_date": datetime(2027, 1, 9), "gap": 3},
            {"start_date": datetime(2027, 1, 10), "end_date": datetime(2027, 6, 30), "gap": 4},
        ]
    )


def _analyzer() -> ScheduleAnalyzer:
    text = CalendarFileProcessor().extract_text_from_pdf(PDF_PATH.read_bytes())
    analyzer = ScheduleAnalyzer(
        start_date=datetime(2026, 12, 23),
        calendar_text=text,
        holidays=[],
        shifts_per_day=4,
    )
    analyzer.parse_calendar()
    assert len(analyzer.calendar_data) > 100
    return analyzer


def test_luis_tripletes_revision_end_to_end():
    """Exact fixtures/settings: Balance/FS ≠ 0, dual-mandatory exempt, gaps 3→4, no false MARINA miss."""
    plantilla = _load_plantilla()
    periods = _luis_gap_periods()
    session = _Session(
        scheduler=None,
        config={
            "gap_between_shifts": 4,
            "gap_periods": periods,
            "max_consecutive_weekends": 3,
        },
        workers_data=plantilla,
        schedule=None,
        revision_name_mapping={},
    )
    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(_analyzer(), workers_config=plantilla)
        violations = check_violations(display)
        breakdown = compute_schedule_score_breakdown(display, scoring_workers=None)

    assert breakdown is not None
    components = breakdown["components"]
    assert components["workload_balance"] > 0.0
    assert components["weekend_balance"] > 0.0

    incompat = violations.get("incompatibilidades") or []
    # Dual-mandatory Christmas pair (KIKO+VALLE) must not be listed or penalised.
    assert not any("25-12-2026" in item for item in incompat)
    assert components["constraint_penalty"] >= 0.0
    # Plantilla is_incompatible clique must still surface non-mandatory clashes
    # (sync used to wipe empty incompatible_with → zero incompat forever).
    assert any("CHEMA" in item and "VALLE" in item for item in incompat)

    gaps = violations.get("gap_minimo") or []
    # Luis exact early pairs: diff==3 under period gap 3 must NOT be violations.
    for needle in (
        "27-12-2026 → 30-12-2026",
        "02-01-2027 → 05-01-2027",
        "05-01-2027 → 08-01-2027",
    ):
        assert not any(needle in item for item in gaps), gaps
    # Late window floor is 4 (part-time may report 5).
    assert any("mínimo 4" in item or "mínimo 5" in item for item in gaps)
    # Early window uses floor 3: CHEMA 23-12→26-12 (3 days) must not be flagged
    # (would appear as mínimo 4 if period gaps were ignored).
    assert not any("23-12-2026" in item and "26-12-2026" in item for item in gaps)
    late_floor_seen = False
    for item in gaps:
        match = _GAP_MSG.search(item)
        if not match:
            continue
        d1 = datetime.strptime(match.group("d1"), "%d-%m-%Y")
        d2 = datetime.strptime(match.group("d2"), "%d-%m-%Y")
        min_req = int(match.group("min"))
        if d1 >= datetime(2027, 1, 10) and d2 >= datetime(2027, 1, 10):
            assert min_req >= 4, item
            late_floor_seen = True
        if d2 <= datetime(2027, 1, 9):
            # Global default is 4; early period is 3 (+1 only for <60% jornada).
            assert min_req in (3, 4), item
    assert late_floor_seen

    mandatory = violations.get("mandatory") or []
    assert "MARINA: falta obligatorio 24-12-2026" not in mandatory
    # PDF OCR token MARNA on 24-12 must have been aliased onto MARINA.
    dec24 = display.schedule.get(datetime(2026, 12, 24)) or []
    assert "MARINA" in dec24
    assert "MARNA" not in dec24


def test_luis_gap_periods_text_applies_when_config_list_empty():
    """Sidebar text area must drive Días mínimos even if config['gap_periods'] is []."""
    plantilla = _load_plantilla()
    session = _Session(
        scheduler=None,
        config={
            "gap_between_shifts": 4,
            "gap_periods": [],  # stale / unsynced
            "max_consecutive_weekends": 3,
        },
        workers_data=plantilla,
        schedule=None,
        revision_name_mapping={},
        gap_periods_text=("23-12-2026 / 09-01-2027: 3\n10-01-2027 / 30-06-2027: 4"),
    )
    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(_analyzer(), workers_config=plantilla)
        violations = check_violations(display)

    gaps = violations.get("gap_minimo") or []
    for needle in (
        "27-12-2026 → 30-12-2026",
        "02-01-2027 → 05-01-2027",
        "05-01-2027 → 08-01-2027",
    ):
        assert not any(needle in item for item in gaps), (needle, gaps)


def test_luis_dual_mandatory_christmas_not_in_incompat_or_penalty():
    """KIKO+VALLE both mandatory on 25-12 must not appear in incompat or raise penalty alone."""
    plantilla = _load_plantilla()
    periods = _luis_gap_periods()
    session = _Session(
        scheduler=None,
        config={"gap_between_shifts": 4, "gap_periods": periods, "max_consecutive_weekends": 3},
        workers_data=plantilla,
        schedule=None,
        revision_name_mapping={},
    )
    with patch.object(st, "session_state", session):
        display = build_revision_display_scheduler(_analyzer(), workers_config=plantilla)
        # Isolate Dec 25: clear other days so only dual-mandatory clash remains.
        only = {datetime(2026, 12, 25): list(display.schedule[datetime(2026, 12, 25)])}
        display.schedule = only
        display._synchronize_tracking_data()
        violations = check_violations(display)
        breakdown = compute_schedule_score_breakdown(display, scoring_workers=None)

    assert display.schedule[datetime(2026, 12, 25)] == ["JULIA", "KIKO", "VALLE", "BEA"]
    assert violations.get("incompatibilidades") == []
    assert breakdown is not None
    assert breakdown["components"]["constraint_penalty"] == 0.0
