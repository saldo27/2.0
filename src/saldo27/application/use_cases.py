from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal

from saldo27.scheduler import Scheduler


@dataclass(frozen=True)
class GenerateScheduleRequest:
    start_date: datetime | date
    end_date: datetime | date
    holidays: list[datetime]
    variable_shifts: list[dict[str, Any]]
    workers_data: list[dict[str, Any]]
    config: dict[str, Any]
    prior_schedule_raw: bytes | None = None


@dataclass(frozen=True)
class GenerateScheduleResult:
    success: bool
    scheduler: Scheduler | None
    message: str


@dataclass(frozen=True)
class DemoLimitBreach:
    kind: Literal["max_workers", "max_days"]
    limit: int
    actual: int


@dataclass(frozen=True)
class PrepareSchedulerResult:
    scheduler: Scheduler
    prior_schedule_error: str | None = None
    prior_schedule_summary: dict[str, Any] | None = None


def _to_datetime(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, datetime.min.time())


def check_demo_limitations(
    *,
    limitations: dict[str, Any],
    workers_data: list[dict[str, Any]],
    start_date: datetime | date,
    end_date: datetime | date,
) -> DemoLimitBreach | None:
    max_workers = limitations.get("max_workers")
    if max_workers and len(workers_data) > max_workers:
        return DemoLimitBreach(kind="max_workers", limit=max_workers, actual=len(workers_data))

    max_days = limitations.get("max_days")
    if max_days:
        start = _to_datetime(start_date)
        end = _to_datetime(end_date)
        days = (end - start).days + 1
        if days > max_days:
            return DemoLimitBreach(kind="max_days", limit=max_days, actual=days)

    return None


def validate_generation_request(request: GenerateScheduleRequest) -> str | None:
    start_date = _to_datetime(request.start_date)
    end_date = _to_datetime(request.end_date)

    if not request.workers_data:
        return "❌ Error: No hay trabajadores configurados"

    if start_date >= end_date:
        return "❌ Error: La fecha final debe ser posterior a la inicial"

    return None


_VALID_PIPELINE_PHASES: frozenset[str] = frozenset({"initialize", "mandatory", "distribution", "finalize"})
_REQUIRED_PIPELINE_PHASES: frozenset[str] = frozenset({"initialize", "finalize"})


def _validate_pipeline_phases(phases: Any) -> list[str]:
    """Validate the ``pipeline_phases`` config value and return a clean list.

    Raises ``ValueError`` with a descriptive message if the value is not a
    non-empty list of known phase names or if mandatory phases are missing.
    """
    if not isinstance(phases, list) or not phases:
        raise ValueError(
            f"'pipeline_phases' debe ser una lista no vacía. "
            f"Fases válidas: {sorted(_VALID_PIPELINE_PHASES)}"
        )
    unknown = [p for p in phases if p not in _VALID_PIPELINE_PHASES]
    if unknown:
        raise ValueError(
            f"Fases desconocidas en 'pipeline_phases': {unknown}. "
            f"Fases válidas: {sorted(_VALID_PIPELINE_PHASES)}"
        )
    missing = _REQUIRED_PIPELINE_PHASES - set(phases)
    if missing:
        raise ValueError(
            f"'pipeline_phases' debe incluir las fases requeridas: {sorted(missing)}"
        )
    return list(phases)


def build_scheduler_config(request: GenerateScheduleRequest) -> dict[str, Any]:
    start_date = _to_datetime(request.start_date)
    end_date = _to_datetime(request.end_date)

    raw_phases = request.config.get("pipeline_phases")
    pipeline_phases = _validate_pipeline_phases(raw_phases) if raw_phases is not None else None

    return {
        "start_date": start_date,
        "end_date": end_date,
        "num_shifts": request.config.get("num_shifts", 4),
        "workers_data": request.workers_data,
        "holidays": request.holidays,
        "variable_shifts": request.variable_shifts,
        "gap_between_shifts": request.config.get("gap_between_shifts", 3),
        "max_consecutive_weekends": request.config.get("max_consecutive_weekends", 3),
        "enable_proportional_weekends": request.config.get("enable_proportional_weekends", True),
        "weekend_tolerance": request.config.get("weekend_tolerance", 1),
        "cache_enabled": request.config.get("cache_enabled", False),
        "lazy_evaluation": request.config.get("lazy_evaluation", False),
        "batch_size": request.config.get("batch_size", 100),
        "max_improvement_loops": request.config.get("max_improvement_loops", 150),
        "last_post_adjustment_max_iterations": request.config.get("last_post_adjustment_max_iterations", 10),
        "max_complete_attempts": request.config.get("max_complete_attempts", 5),
        "pipeline_phases": pipeline_phases,
    }


def prepare_scheduler(request: GenerateScheduleRequest) -> PrepareSchedulerResult:
    scheduler = Scheduler(build_scheduler_config(request))

    prior_schedule_error: str | None = None
    prior_schedule_summary: dict[str, Any] | None = None

    if request.prior_schedule_raw:
        from io import BytesIO

        load_result = scheduler.load_prior_schedule_data(BytesIO(request.prior_schedule_raw))
        if load_result.get("error"):
            prior_schedule_error = str(load_result["error"])
        else:
            prior_schedule_summary = load_result.get("summary", {})

    return PrepareSchedulerResult(
        scheduler=scheduler,
        prior_schedule_error=prior_schedule_error,
        prior_schedule_summary=prior_schedule_summary,
    )


def generate_schedule(
    request: GenerateScheduleRequest,
    *,
    prepared: PrepareSchedulerResult | None = None,
) -> GenerateScheduleResult:
    validation_error = validate_generation_request(request)
    if validation_error:
        return GenerateScheduleResult(False, None, validation_error)

    if prepared is None:
        prepared = prepare_scheduler(request)
    scheduler = prepared.scheduler

    # Ensure the cancel flag is clean before starting a new generation.
    scheduler._cancelled = False
    success = scheduler.generate_schedule()
    if prepared.prior_schedule_error:
        message = (
            f"⚠️ Calendario anterior no pudo cargarse: {prepared.prior_schedule_error}\n✅ Horario generado"
            if success
            else f"⚠️ Calendario anterior no pudo cargarse: {prepared.prior_schedule_error}\n❌ No se pudo generar horario"
        )
        return GenerateScheduleResult(success, scheduler, message)

    return GenerateScheduleResult(
        success, scheduler, "✅ Horario generado" if success else "❌ No se pudo generar horario"
    )


def cancel_scheduler(scheduler: Scheduler) -> None:
    """Signal the scheduler to stop generation at the next cancellation checkpoint."""
    scheduler._cancelled = True


def validate_schedule(scheduler: Scheduler) -> dict[str, Any]:
    return scheduler.validate_and_fix_final_schedule()


def run_simulation(request: GenerateScheduleRequest) -> GenerateScheduleResult:
    simulated_config = dict(request.config)
    simulated_config["is_simulation"] = True
    simulated_request = GenerateScheduleRequest(
        start_date=request.start_date,
        end_date=request.end_date,
        holidays=request.holidays,
        variable_shifts=request.variable_shifts,
        workers_data=request.workers_data,
        config=simulated_config,
        prior_schedule_raw=request.prior_schedule_raw,
    )
    return generate_schedule(simulated_request)


def export_schedule(scheduler: Scheduler, output_format: str = "txt") -> Any:
    if output_format.lower() == "json":
        return scheduler.export_schedule_json()
    return scheduler.export_schedule(format=output_format)
