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


CONSTRAINT_VIOLATION_KEYS: tuple[str, ...] = (
    "incompatibilidades",
    "gap_minimo",
    "patron_7_14",
    "patron_viernes_lunes",
    "gap2_fin_semana",
    "mandatory",
    "fuera_cadencia",
    "dias_no_disponibles",
    "periodos_trabajo",
    "fines_semana_consecutivos",
    "ultimo_puesto",
)


@dataclass
class ConstraintViolations:
    incompatibilidades: list[str] = field(default_factory=list)
    gap_minimo: list[str] = field(default_factory=list)
    patron_7_14: list[str] = field(default_factory=list)
    patron_viernes_lunes: list[str] = field(default_factory=list)
    gap2_fin_semana: list[str] = field(default_factory=list)
    mandatory: list[str] = field(default_factory=list)
    fuera_cadencia: list[str] = field(default_factory=list)
    dias_no_disponibles: list[str] = field(default_factory=list)
    periodos_trabajo: list[str] = field(default_factory=list)
    fines_semana_consecutivos: list[str] = field(default_factory=list)
    ultimo_puesto: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, list[str]]:
        return {key: list(getattr(self, key)) for key in CONSTRAINT_VIOLATION_KEYS}


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
    """Collect hard-constraint violations for the Verificación UI.

    Maps core checker types onto the UI category keys and adds availability /
    Rosell-flag / consecutive-weekend scans that the spacing checker does not
    emit as schedule-wide reports.
    """
    from saldo27.constraint_checker import _normalize_day, config_forced_dates
    from saldo27.utilities import is_date_in_ranges, is_fixed_cadence, worker_flag_enabled

    core_violations = scheduler._check_schedule_constraints()
    violations = ConstraintViolations()
    date_utils = scheduler.date_utils
    holidays = getattr(scheduler, "holidays", None) or []
    workers_by_id = {str(w["id"]): w for w in scheduler.workers_data}
    protected_by_worker = {
        wid: {day for raw in config_forced_dates(worker, date_utils) for day in (_normalize_day(raw),) if day}
        for wid, worker in workers_by_id.items()
    }

    def _spacing_forced(worker_id: object, date1: object, date2: object) -> bool:
        """True when either end is mandatory/cadence — UI must not list it as a gap/pattern hit."""
        protected = protected_by_worker.get(str(worker_id), set())
        d1 = _normalize_day(date1)
        d2 = _normalize_day(date2)
        return (d1 is not None and d1 in protected) or (d2 is not None and d2 in protected)

    for violation in core_violations:
        violation_type = violation.get("type")
        if violation_type == "incompatibility":
            # Dual-mandatory already skipped in the core checker; keep a date-safe
            # belt-and-suspenders filter so Revisión never lists Christmas-style clashes.
            day = _normalize_day(violation.get("date"))
            wid = str(violation.get("worker_id", ""))
            other = str(violation.get("incompatible_id", ""))
            if (
                day is not None
                and day in protected_by_worker.get(wid, ())
                and day in protected_by_worker.get(other, ())
            ):
                continue
            violations.incompatibilidades.append(
                f"{violation['date'].strftime('%d-%m-%Y')}: {violation['worker_id']} ↔ {violation['incompatible_id']}"
            )
        elif violation_type == "min_rest_days":
            if _spacing_forced(violation.get("worker_id"), violation.get("date1"), violation.get("date2")):
                continue
            violations.gap_minimo.append(
                f"{violation['worker_id']}: {violation['date1'].strftime('%d-%m-%Y')} → "
                f"{violation['date2'].strftime('%d-%m-%Y')} "
                f"({violation['days_between']} días, mínimo {violation.get('min_required', '?')})"
            )
        elif violation_type == "weekly_pattern":
            if _spacing_forced(violation.get("worker_id"), violation.get("date1"), violation.get("date2")):
                continue
            violations.patron_7_14.append(
                f"{violation['worker_id']}: {violation['date1'].strftime('%d-%m-%Y')} → "
                f"{violation['date2'].strftime('%d-%m-%Y')} ({violation['days_between']} días)"
            )
        elif violation_type == "friday_monday_pattern":
            if _spacing_forced(violation.get("worker_id"), violation.get("date1"), violation.get("date2")):
                continue
            violations.patron_viernes_lunes.append(
                f"{violation['worker_id']}: {violation['date1'].strftime('%d-%m-%Y')} → "
                f"{violation['date2'].strftime('%d-%m-%Y')}"
            )
        elif violation_type == "gap2_weekend":
            if _spacing_forced(violation.get("worker_id"), violation.get("date1"), violation.get("date2")):
                continue
            violations.gap2_fin_semana.append(
                f"{violation['worker_id']}: {violation['date1'].strftime('%d-%m-%Y')} → "
                f"{violation['date2'].strftime('%d-%m-%Y')}"
            )
        elif violation_type == "outside_cadence":
            violations.fuera_cadencia.append(
                f"{violation['worker_id']}: {violation['date'].strftime('%d-%m-%Y')} fuera de cadencia"
            )
        elif violation_type == "consecutive_last_post":
            violations.ultimo_puesto.append(
                f"{violation['worker_id']}: {violation['date1'].strftime('%d-%m-%Y')} → "
                f"{violation['date2'].strftime('%d-%m-%Y')} "
                f"({violation['run_length']} guardias consecutivas)"
            )

    checker = getattr(scheduler, "constraint_checker", None)

    schedule_days = {day for key in scheduler.schedule for day in (_normalize_day(key),) if day is not None}

    for worker in scheduler.workers_data:
        worker_id = str(worker["id"])
        assigned_days = sorted(
            {
                day
                for value in (scheduler.worker_assignments.get(worker_id, ()) or [])
                for day in (_normalize_day(value),)
                if day is not None
            }
        )

        # Missing mandatory / cadence dates — only for days present in the
        # imported/generated calendar (Revisión PDFs often omit leading empty
        # weekdays). Compare by calendar day so date vs datetime cannot false-flag.
        try:
            mandatory_dates = config_forced_dates(worker, date_utils)
        except Exception:
            mandatory_dates = set()
        start_day = _normalize_day(scheduler.start_date)
        end_day = _normalize_day(scheduler.end_date)
        for mandatory_date in sorted(mandatory_dates):
            day = _normalize_day(mandatory_date)
            if day is None:
                continue
            if day not in schedule_days:
                continue
            in_range = True
            if start_day is not None and day < start_day:
                in_range = False
            if end_day is not None and day > end_day:
                in_range = False
            if in_range and day not in assigned_days:
                violations.mandatory.append(f"{worker_id}: falta obligatorio {day.strftime('%d-%m-%Y')}")

        for assigned_date in assigned_days:
            if worker.get("days_off") and is_date_in_ranges(assigned_date, worker["days_off"]):
                violations.dias_no_disponibles.append(f"{worker_id}: {assigned_date.strftime('%d-%m-%Y')} en days_off")
            work_periods = worker.get("work_periods") or ""
            if work_periods.strip():
                try:
                    ranges = date_utils.parse_date_ranges(work_periods)
                except Exception:
                    ranges = []
                if ranges and not any(start <= assigned_date <= end for start, end in ranges):
                    violations.periodos_trabajo.append(
                        f"{worker_id}: {assigned_date.strftime('%d-%m-%Y')} fuera de work_periods"
                    )

            if (
                checker is not None
                and not is_fixed_cadence(worker)
                and date_utils.is_weekend_day(assigned_date, holidays)
            ):
                run = checker._consecutive_weekend_run(worker_id, assigned_date, assigned_days)
                max_run = getattr(scheduler, "max_consecutive_weekends", None)
                if max_run is not None and run > max_run:
                    msg = (
                        f"{worker_id}: {assigned_date.strftime('%d-%m-%Y')} "
                        f"({run} fines/festivos consecutivos > {max_run})"
                    )
                    if msg not in violations.fines_semana_consecutivos:
                        violations.fines_semana_consecutivos.append(msg)

    # Rosell flags on the filled schedule
    for date, shifts in scheduler.schedule.items():
        if not shifts:
            continue
        last_idx = scheduler.last_post_index(date)
        for post, worker_id in enumerate(shifts):
            if worker_id is None:
                continue
            worker = workers_by_id.get(str(worker_id))
            if worker is None:
                continue
            is_last = post == last_idx
            if worker_flag_enabled(worker, "no_last_post") and is_last:
                violations.ultimo_puesto.append(f"{worker_id}: {date.strftime('%d-%m-%Y')} en Rosell con no_last_post")
            if worker_flag_enabled(worker, "only_last_post") and not is_last:
                violations.ultimo_puesto.append(
                    f"{worker_id}: {date.strftime('%d-%m-%Y')} fuera de Rosell con only_last_post"
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
