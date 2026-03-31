"""Shared test fixtures for saldo27."""

from datetime import datetime, timedelta

import pytest


@pytest.fixture
def sample_holidays():
    """Spanish public holidays for a test month (March 2026)."""
    return [
        datetime(2026, 3, 19),  # San José
    ]


@pytest.fixture
def sample_workers_data():
    """Minimal list of worker dicts matching the app's schema."""
    return [
        {
            "id": "DOC001",
            "name": "DOC001",
            "target_shifts": 6,
            "work_percentage": 100,
            "work_periods": "01-03-2026;31-03-2026",
            "mandatory_dates": "",
            "days_off": "",
            "incompatible_with": [],
            "is_incompatible_all": False,
        },
        {
            "id": "DOC002",
            "name": "DOC002",
            "target_shifts": 6,
            "work_percentage": 100,
            "work_periods": "01-03-2026;31-03-2026",
            "mandatory_dates": "",
            "days_off": "",
            "incompatible_with": [],
            "is_incompatible_all": False,
        },
        {
            "id": "DOC003",
            "name": "DOC003",
            "target_shifts": 3,
            "work_percentage": 50,
            "work_periods": "01-03-2026;31-03-2026",
            "mandatory_dates": "",
            "days_off": "15-03-2026",
            "incompatible_with": ["DOC001"],
            "is_incompatible_all": False,
        },
        {
            "id": "DOC004",
            "name": "DOC004",
            "target_shifts": 6,
            "work_percentage": 100,
            "work_periods": "01-03-2026;31-03-2026",
            "mandatory_dates": "10-03-2026",
            "days_off": "",
            "incompatible_with": [],
            "is_incompatible_all": False,
        },
    ]


@pytest.fixture
def march_2026_dates():
    """All dates in March 2026 as datetime objects."""
    start = datetime(2026, 3, 1)
    return [start + timedelta(days=i) for i in range(31)]


@pytest.fixture
def sample_schedule(march_2026_dates, sample_workers_data):
    """A minimal schedule dict: {date_str: {shift_num: worker_id}}."""
    schedule = {}
    workers = [w["id"] for w in sample_workers_data]
    for i, d in enumerate(march_2026_dates):
        key = d.strftime("%Y-%m-%d")
        schedule[key] = {}
        for shift in range(1, 5):
            schedule[key][shift] = workers[(i + shift) % len(workers)]
    return schedule
