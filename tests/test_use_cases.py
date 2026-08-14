from datetime import date, datetime

from saldo27.application.use_cases import GenerateScheduleRequest, build_scheduler_config


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
