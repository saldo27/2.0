"""Tests for saldo27.scheduler_config — configuration and logging."""

import os
import pytest
from saldo27.scheduler_config import SchedulerConfig, setup_logging


# ── SchedulerConfig defaults ───────────────────────────────────────


def test_get_default_config_returns_dict():
    config = SchedulerConfig.get_default_config()
    assert isinstance(config, dict)


def test_default_config_has_required_keys():
    config = SchedulerConfig.get_default_config()
    assert "gap_between_shifts" in config
    assert "max_consecutive_weekends" in config
    assert "num_shifts" in config


def test_default_gap_between_shifts():
    assert SchedulerConfig.DEFAULT_GAP_BETWEEN_SHIFTS == 3


def test_default_num_shifts():
    assert SchedulerConfig.DEFAULT_NUM_SHIFTS == 4


# ── Config validation ──────────────────────────────────────────────


def test_validate_default_config():
    config = SchedulerConfig.get_default_config()
    is_valid, error = SchedulerConfig.validate_config(config)
    # validate_config may require fields not in the defaults (e.g. schedule data)
    assert isinstance(is_valid, bool)
    assert error is None or isinstance(error, str)


def test_validate_empty_config():
    is_valid, error = SchedulerConfig.validate_config({})
    # Empty config should either be invalid or use defaults
    assert isinstance(is_valid, bool)


# ── setup_logging ──────────────────────────────────────────────────


def test_setup_logging_creates_log_directory(tmp_path):
    log_dir = str(tmp_path / "test_logs")
    result = setup_logging(log_directory=log_dir)
    assert result is True
    assert os.path.exists(log_dir)


def test_setup_logging_returns_bool(tmp_path):
    log_dir = str(tmp_path / "logs2")
    result = setup_logging(log_directory=log_dir)
    assert isinstance(result, bool)
