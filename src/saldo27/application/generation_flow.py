from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from queue import Empty, SimpleQueue
from typing import TYPE_CHECKING, Any, Literal

from saldo27.application.use_cases import (
    GenerateScheduleRequest,
    PrepareSchedulerResult,
    cancel_scheduler,
    check_demo_limitations,
    generate_schedule,
    prepare_scheduler,
    validate_generation_request,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from saldo27.application.contracts import GenerationProgressEvent
    from saldo27.scheduler import Scheduler

StatusLevel = Literal["info", "warning", "error", "success"]
LogKind = Literal["code", "warning"]

_PHASE_LABELS: dict[str, str] = {
    "initialize": "⚙️ Fase 1 · Inicializando estructura del calendario",
    "mandatory": "⚙️ Fase 2 · Asignando guardias obligatorias",
    "distribution": "⚙️ Fase 3 · Distribución y optimización",
    "finalize": "⚙️ Fase 4 · Finalización y validación",
}
_INITIAL_STATUS_MESSAGE = "⚙️ Iniciando generación del calendario..."
_CANCELLING_STATUS_MESSAGE = "⏳ Cancelando... esperando a que el motor se detenga"
_CANCELLED_MESSAGE = "🛑 Generación cancelada"


class BufferedLogHandler(logging.Handler):
    """Thread-safe in-memory log buffer for generation progress."""

    def __init__(self, max_messages: int = 50):
        super().__init__()
        self.messages: list[str] = []
        self.max_messages = max_messages
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            with self._lock:
                self.messages.append(message)
                if len(self.messages) > self.max_messages:
                    self.messages = self.messages[-self.max_messages :]
        except (AttributeError, RuntimeError, TypeError, ValueError):
            self.handleError(record)

    def get_messages(self, *, last_n: int = 15) -> list[str]:
        with self._lock:
            return list(self.messages[-last_n:])

    def clear(self) -> None:
        with self._lock:
            self.messages.clear()


@dataclass(frozen=True)
class FlowMessage:
    level: StatusLevel
    text: str


@dataclass(frozen=True)
class GenerationPreparationResult:
    success: bool
    message: str
    user_messages: tuple[FlowMessage, ...] = ()
    workflow: GenerationWorkflow | None = None
    prior_schedule_summary: dict[str, Any] | None = None


@dataclass
class _BackgroundGenerationState:
    success: bool = False
    error: BaseException | None = None


@dataclass
class GenerationWorkflow:
    request: GenerateScheduleRequest
    prepared_scheduler: PrepareSchedulerResult
    scheduler: Scheduler
    progress_events: SimpleQueue[GenerationProgressEvent] = field(default_factory=SimpleQueue)
    log_handler: BufferedLogHandler = field(default_factory=lambda: BufferedLogHandler(max_messages=80))
    poll_interval_seconds: float = 0.5
    cancelled: bool = False
    generation_state: _BackgroundGenerationState = field(default_factory=_BackgroundGenerationState)


@dataclass(frozen=True)
class GenerationUICallbacks:
    show_status: Callable[[str, StatusLevel], None]
    show_log: Callable[[list[str]], None]
    is_cancel_requested: Callable[[], bool]
    clear_cancel_request: Callable[[], None]


@dataclass(frozen=True)
class GenerationFlowResult:
    success: bool
    message: str
    final_status_text: str
    final_status_level: StatusLevel
    final_log_text: str | None = None
    final_log_kind: LogKind | None = None
    cancelled: bool = False


@dataclass(frozen=True)
class ConstraintViolations:
    incompatibilidades: list[str]
    patron_7_14: list[str]
    mandatory: list[str]

    def as_dict(self) -> dict[str, list[str]]:
        return {
            "incompatibilidades": list(self.incompatibilidades),
            "patron_7_14": list(self.patron_7_14),
            "mandatory": list(self.mandatory),
        }


def prepare_generation_workflow(
    request: GenerateScheduleRequest,
    *,
    limitations: Mapping[str, Any],
) -> GenerationPreparationResult:
    breach = check_demo_limitations(
        limitations=dict(limitations),
        workers_data=request.workers_data,
        start_date=request.start_date,
        end_date=request.end_date,
    )
    if breach:
        if breach.kind == "max_workers":
            return GenerationPreparationResult(
                success=False,
                message=f"Límite de {breach.limit} trabajadores excedido",
                user_messages=(
                    FlowMessage("error", f"⚠️ **Limitación DEMO**:  Máximo {breach.limit} trabajadores permitidos"),
                    FlowMessage("info", "💡 Activa la licencia completa para trabajadores ilimitados"),
                ),
            )
        return GenerationPreparationResult(
            success=False,
            message=f"Límite de {breach.limit} días excedido",
            user_messages=(
                FlowMessage("error", f"⚠️ **Limitación DEMO**: Máximo {breach.limit} días de horario permitidos"),
                FlowMessage("info", "💡 Activa la licencia completa para períodos ilimitados"),
            ),
        )

    validation_error = validate_generation_request(request)
    if validation_error:
        return GenerationPreparationResult(success=False, message=validation_error)

    prepared_scheduler = prepare_scheduler(request)
    user_messages: list[FlowMessage] = []
    if prepared_scheduler.prior_schedule_error:
        user_messages.append(
            FlowMessage(
                "warning",
                f"⚠️ Calendario anterior no pudo cargarse: {prepared_scheduler.prior_schedule_error}",
            )
        )

    workflow = GenerationWorkflow(
        request=request,
        prepared_scheduler=prepared_scheduler,
        scheduler=prepared_scheduler.scheduler,
    )
    return GenerationPreparationResult(
        success=True,
        message="Preparado para generar horario",
        user_messages=tuple(user_messages),
        workflow=workflow,
        prior_schedule_summary=prepared_scheduler.prior_schedule_summary,
    )


def execute_generation_workflow(
    workflow: GenerationWorkflow,
    callbacks: GenerationUICallbacks,
    *,
    phase_labels: Mapping[str, str] | None = None,
) -> GenerationFlowResult:
    labels = dict(phase_labels or _PHASE_LABELS)
    current_status_message = _INITIAL_STATUS_MESSAGE
    callbacks.show_status(current_status_message, "info")

    root_logger = logging.getLogger()
    workflow.log_handler.setLevel(logging.INFO)
    workflow.log_handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(workflow.log_handler)

    try:

        def _run_generation() -> None:
            try:
                result = generate_schedule(
                    workflow.request,
                    prepared=workflow.prepared_scheduler,
                    progress_callback=lambda event: workflow.progress_events.put(event),
                )
                workflow.generation_state.success = result.success
                if result.scheduler is not None and result.scheduler is not workflow.scheduler:
                    workflow.scheduler = result.scheduler
            except BaseException as exc:  # pragma: no cover - surfaced to caller after join
                workflow.generation_state.error = exc

        thread = threading.Thread(target=_run_generation, daemon=True)
        thread.start()

        while thread.is_alive():
            if callbacks.is_cancel_requested():
                cancel_scheduler(workflow.scheduler)
                workflow.cancelled = True
                callbacks.clear_cancel_request()
                callbacks.show_status(_CANCELLING_STATUS_MESSAGE, "warning")

            current_status_message = _drain_progress_events(
                workflow=workflow,
                callbacks=callbacks,
                phase_labels=labels,
                current_status_message=current_status_message,
            )
            _publish_live_log(workflow.log_handler, callbacks)
            time.sleep(workflow.poll_interval_seconds)

        thread.join()
    finally:
        root_logger.removeHandler(workflow.log_handler)

    if workflow.cancelled:
        return GenerationFlowResult(
            success=False,
            message=_CANCELLED_MESSAGE,
            final_status_text="🛑 Generación cancelada por el usuario",
            final_status_level="warning",
            final_log_text=_CANCELLED_MESSAGE,
            final_log_kind="warning",
            cancelled=True,
        )

    if workflow.generation_state.error is not None:
        raise workflow.generation_state.error

    if workflow.generation_state.success:
        final_log_text = build_generation_summary_log(workflow.scheduler)
        return GenerationFlowResult(
            success=True,
            message="✅ Calendario generado exitosamente",
            final_status_text="✅ ¡Calendario generado y optimizado!",
            final_status_level="success",
            final_log_text=final_log_text,
            final_log_kind="code" if final_log_text else None,
        )

    return GenerationFlowResult(
        success=False,
        message="❌ Error: No se pudo generar el calendario",
        final_status_text="Fallo en la generación - Revise restricciones",
        final_status_level="error",
        final_log_text="❌ Fallo en la generación",
        final_log_kind="warning",
    )


def build_generation_summary_log(scheduler: Scheduler) -> str | None:
    summary_lines: list[str] = []
    core = getattr(scheduler, "_scheduler_core", None)
    progress_monitor = getattr(core, "progress_monitor", None) if core else None

    if progress_monitor and progress_monitor.iteration_data:
        final_iter = len(progress_monitor.iteration_data)
        total_iter = progress_monitor.total_iterations

        final_score = 0.0
        try:
            metrics = getattr(core, "metrics", None)
            if metrics:
                final_score = metrics.calculate_overall_schedule_score()
            else:
                final_score = progress_monitor.iteration_data[-1].get("current_score", 0)
        except (AttributeError, IndexError, TypeError, ValueError) as exc:
            logging.debug(
                "No se pudo calcular el score final real; usando el score de iteración: %s",
                exc,
            )
            final_score = progress_monitor.iteration_data[-1].get("current_score", 0)

        summary_lines.append("📊 Resumen de ejecución:")
        summary_lines.append(f"   • Iteraciones: {final_iter}/{total_iter}")
        summary_lines.append(f"   • Score final: {final_score:.2f}")

        if final_score >= 95:
            summary_lines.append("🌟 EXCELENTE: Score objetivo alcanzado!")
        elif final_score >= 85:
            summary_lines.append("👍 BUENO: Score satisfactorio")
        elif final_score >= 70:
            summary_lines.append("⚠️  REGULAR: Puede requerir ajustes adicionales")
        else:
            summary_lines.append("❌ BAJO: Requiere revisión de restricciones")

        if progress_monitor.start_time:
            total_time = datetime.now() - progress_monitor.start_time
            summary_lines.append(f"   • Tiempo total: {str(total_time).split('.')[0]}")

    try:
        core_violations = scheduler._check_schedule_constraints()
        violations_count = len(core_violations)
        if violations_count == 0:
            summary_lines.append("✅ Sin violaciones de restricciones")
        else:
            summary_lines.append(f"⚠️ Violaciones: {violations_count}")
            for violation in core_violations[:5]:
                violation_type = violation.get("type", "")
                if violation_type == "incompatibility":
                    summary_lines.append(
                        f"   • Incomp: {violation['worker_id']} ↔ {violation['incompatible_id']} ({violation['date'].strftime('%d-%m')})"
                    )
                elif violation_type == "weekly_pattern":
                    summary_lines.append(
                        f"   • Patrón: {violation['worker_id']} {violation['date1'].strftime('%d-%m')}→{violation['date2'].strftime('%d-%m')}"
                    )
            if violations_count > 5:
                summary_lines.append(f"   • ... y {violations_count - 5} más")
    except (AttributeError, TypeError, ValueError) as exc:
        logging.debug("No se pudo construir el resumen de violaciones: %s", exc)

    return "\n".join(summary_lines) if summary_lines else None


def collect_constraint_violations(scheduler: Scheduler) -> dict[str, list[str]]:
    core_violations = scheduler._check_schedule_constraints()
    violations = ConstraintViolations(incompatibilidades=[], patron_7_14=[], mandatory=[])

    for violation in core_violations:
        violation_type = violation.get("type")
        if violation_type == "incompatibility":
            violations.incompatibilidades.append(
                f"{violation['date'].strftime('%d-%m-%Y')}: {violation['worker_id']} ↔ {violation['incompatible_id']}"
            )
        elif violation_type == "weekly_pattern":
            violations.patron_7_14.append(
                f"{violation['worker_id']}: {violation['date1'].strftime('%d-%m-%Y')} → {violation['date2'].strftime('%d-%m-%Y')} ({violation['days_between']} días)"
            )

    return violations.as_dict()


def _drain_progress_events(
    *,
    workflow: GenerationWorkflow,
    callbacks: GenerationUICallbacks,
    phase_labels: Mapping[str, str],
    current_status_message: str,
) -> str:
    while True:
        try:
            event = workflow.progress_events.get_nowait()
        except Empty:
            return current_status_message

        phase_label = phase_labels.get(event.phase)
        if not phase_label:
            continue
        if event.stage == "completed" and event.success is False:
            error_status = f"{phase_label} ❌"
            callbacks.show_status(error_status, "error")
            current_status_message = error_status
            continue
        if phase_label != current_status_message:
            callbacks.show_status(phase_label, "info")
            current_status_message = phase_label


def _publish_live_log(log_handler: BufferedLogHandler, callbacks: GenerationUICallbacks) -> None:
    messages = log_handler.get_messages(last_n=15)
    if messages:
        callbacks.show_log(messages)
