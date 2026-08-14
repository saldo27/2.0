from __future__ import annotations

import threading
import time
from datetime import date, datetime

from saldo27.application import generation_flow
from saldo27.application.contracts import GenerationProgressEvent
from saldo27.application.generation_flow import (
    GenerationUICallbacks,
    collect_constraint_violations,
    execute_generation_workflow,
    prepare_generation_workflow,
)
from saldo27.application.use_cases import GenerateScheduleRequest, GenerateScheduleResult
from saldo27.scheduler import Scheduler


def _build_request(sample_workers_data):
    return GenerateScheduleRequest(
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 3),
        holidays=[],
        variable_shifts=[],
        workers_data=sample_workers_data,
        config={"num_shifts": 4},
    )


def _build_scheduler(sample_workers_data):
    return Scheduler(
        {
            "start_date": datetime(2026, 3, 1),
            "end_date": datetime(2026, 3, 3),
            "num_shifts": 4,
            "workers_data": sample_workers_data,
            "holidays": [],
            "variable_shifts": [],
            "gap_between_shifts": 3,
            "max_consecutive_weekends": 3,
        }
    )


def test_prepare_generation_workflow_rejects_demo_limit(sample_workers_data):
    request = _build_request(sample_workers_data)

    result = prepare_generation_workflow(request, limitations={"max_workers": 2, "max_days": None})

    assert result.success is False
    assert result.workflow is None
    assert result.message == "Límite de 2 trabajadores excedido"
    assert [message.level for message in result.user_messages] == ["error", "info"]


def test_collect_constraint_violations_formats_core_output(sample_workers_data, monkeypatch):
    scheduler = _build_scheduler(sample_workers_data)
    violations = [
        {
            "type": "incompatibility",
            "date": datetime(2026, 3, 1),
            "worker_id": "DOC001",
            "incompatible_id": "DOC002",
        },
        {
            "type": "weekly_pattern",
            "worker_id": "DOC003",
            "date1": datetime(2026, 3, 8),
            "date2": datetime(2026, 3, 15),
            "days_between": 7,
        },
    ]
    monkeypatch.setattr(scheduler, "_check_schedule_constraints", lambda: violations)

    result = collect_constraint_violations(scheduler)

    assert result == {
        "incompatibilidades": ["01-03-2026: DOC001 ↔ DOC002"],
        "patron_7_14": ["DOC003: 08-03-2026 → 15-03-2026 (7 días)"],
        "mandatory": [],
    }


def test_execute_generation_workflow_returns_success_summary(sample_workers_data, monkeypatch):
    request = _build_request(sample_workers_data)
    preparation = prepare_generation_workflow(request, limitations={"max_workers": None, "max_days": None})
    assert preparation.workflow is not None
    workflow = preparation.workflow
    workflow.poll_interval_seconds = 0.01

    statuses: list[tuple[str, str]] = []

    def fake_generate_schedule(request, *, prepared, progress_callback):
        progress_callback(
            GenerationProgressEvent(
                phase="initialize",
                stage="started",
                timestamp=datetime.now(),
            )
        )
        time.sleep(0.02)
        return GenerateScheduleResult(True, workflow.scheduler, "✅ Horario generado")

    monkeypatch.setattr(generation_flow, "generate_schedule", fake_generate_schedule)
    monkeypatch.setattr(generation_flow, "build_generation_summary_log", lambda scheduler: "resumen final")

    callbacks = GenerationUICallbacks(
        show_status=lambda message, level: statuses.append((message, level)),
        show_log=lambda messages: None,
        is_cancel_requested=lambda: False,
        clear_cancel_request=lambda: None,
    )

    result = execute_generation_workflow(workflow, callbacks)

    assert result.success is True
    assert result.final_log_text == "resumen final"
    assert statuses[0] == ("⚙️ Iniciando generación del calendario...", "info")
    assert any(message == "⚙️ Fase 1 · Inicializando estructura del calendario" for message, _ in statuses)


def test_execute_generation_workflow_cancels_running_scheduler(sample_workers_data, monkeypatch):
    request = _build_request(sample_workers_data)
    preparation = prepare_generation_workflow(request, limitations={"max_workers": None, "max_days": None})
    assert preparation.workflow is not None
    workflow = preparation.workflow
    workflow.poll_interval_seconds = 0.01

    cancel_once = {"value": True}
    release = threading.Event()

    def fake_generate_schedule(request, *, prepared, progress_callback):
        release.set()
        while not workflow.scheduler.is_cancellation_requested():
            time.sleep(0.005)
        return GenerateScheduleResult(False, workflow.scheduler, "cancelled")

    monkeypatch.setattr(generation_flow, "generate_schedule", fake_generate_schedule)

    callbacks = GenerationUICallbacks(
        show_status=lambda message, level: None,
        show_log=lambda messages: None,
        is_cancel_requested=lambda: cancel_once.pop("value", False),
        clear_cancel_request=lambda: None,
    )

    result = execute_generation_workflow(workflow, callbacks)

    assert release.is_set() is True
    assert result.cancelled is True
    assert result.message == "🛑 Generación cancelada"
    assert workflow.scheduler.is_cancellation_requested() is True
