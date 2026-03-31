"""Tests for saldo27.exceptions — exception hierarchy."""

import pytest

from saldo27.exceptions import (
    ConfigurationError,
    ConstraintViolationError,
    DataIntegrityError,
    SchedulerError,
)


def test_scheduler_error_is_base():
    err = SchedulerError("something broke")
    assert isinstance(err, Exception)
    assert str(err) == "something broke"


def test_configuration_error_inherits_from_scheduler_error():
    err = ConfigurationError("bad config")
    assert isinstance(err, SchedulerError)
    assert isinstance(err, Exception)


def test_constraint_violation_error_inherits_from_scheduler_error():
    err = ConstraintViolationError("gap too small")
    assert isinstance(err, SchedulerError)


def test_data_integrity_error_inherits_from_scheduler_error():
    err = DataIntegrityError("corrupt data")
    assert isinstance(err, SchedulerError)


def test_exceptions_are_catchable_as_scheduler_error():
    for cls in (ConfigurationError, ConstraintViolationError, DataIntegrityError):
        with pytest.raises(SchedulerError):
            raise cls("test")


def test_exceptions_preserve_args():
    err = ConfigurationError("msg", 42)
    assert err.args == ("msg", 42)
