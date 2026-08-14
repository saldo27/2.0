from datetime import date, datetime

from saldo27.application.use_cases import (
    GenerateScheduleRequest,
    build_scheduler_config,
    check_demo_limitations,
    validate_generation_request,
)


def test_build_scheduler_config_converts_dates(sample_workers_data):
    request = GenerateScheduleRequest(
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 3),
        holidays=[],
        variable_shifts=[],
        workers_data=sample_workers_data,
        config={"num_shifts": 4},
    )

    config = build_scheduler_config(request)

    assert isinstance(config["start_date"], datetime)
    assert isinstance(config["end_date"], datetime)
    assert config["workers_data"] is sample_workers_data


def test_check_demo_limitations_detects_worker_limit(sample_workers_data):
    breach = check_demo_limitations(
        limitations={"max_workers": 2, "max_days": None},
        workers_data=sample_workers_data,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 31),
    )

    assert breach is not None
    assert breach.kind == "max_workers"
    assert breach.limit == 2
    assert breach.actual == len(sample_workers_data)


def test_check_demo_limitations_detects_days_limit(sample_workers_data):
    breach = check_demo_limitations(
        limitations={"max_workers": None, "max_days": 7},
        workers_data=sample_workers_data,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 15),
    )

    assert breach is not None
    assert breach.kind == "max_days"
    assert breach.limit == 7
    assert breach.actual == 15


def test_validate_generation_request_rejects_invalid_range(sample_workers_data):
    request = GenerateScheduleRequest(
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 10),
        holidays=[],
        variable_shifts=[],
        workers_data=sample_workers_data,
        config={"num_shifts": 4},
    )

    assert validate_generation_request(request) == "❌ Error: La fecha final debe ser posterior a la inicial"
