from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

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


def _to_datetime(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, datetime.min.time())


def build_scheduler_config(request: GenerateScheduleRequest) -> dict[str, Any]:
    start_date = _to_datetime(request.start_date)
    end_date = _to_datetime(request.end_date)

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
        "pipeline_phases": request.config.get("pipeline_phases"),
    }


def generate_schedule(request: GenerateScheduleRequest) -> GenerateScheduleResult:
    start_date = _to_datetime(request.start_date)
    end_date = _to_datetime(request.end_date)

    if not request.workers_data:
        return GenerateScheduleResult(False, None, "❌ Error: No hay trabajadores configurados")

    if start_date >= end_date:
        return GenerateScheduleResult(False, None, "❌ Error: La fecha final debe ser posterior a la inicial")

    scheduler = Scheduler(build_scheduler_config(request))

    if request.prior_schedule_raw:
        from io import BytesIO

        load_result = scheduler.load_prior_schedule_data(BytesIO(request.prior_schedule_raw))
        if load_result.get("error"):
            return GenerateScheduleResult(False, scheduler, f"⚠️ Calendario anterior no pudo cargarse: {load_result['error']}")

    success = scheduler.generate_schedule()
    return GenerateScheduleResult(success, scheduler, "✅ Horario generado" if success else "❌ No se pudo generar horario")


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
