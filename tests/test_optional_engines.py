import types

import pytest

from saldo27.infrastructure.optional_engines import (
    EngineCapabilities,
    OptionalEngineSpec,
    _validate_capabilities,
    load_optional_engines,
    ortools_available,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_scheduler():
    return types.SimpleNamespace()


class _GoodEngine:
    """Minimal engine that satisfies the OptionalEngine protocol."""

    CAPABILITIES = EngineCapabilities(
        name="GoodEngine",
        real_time_editing=True,
        requires_scheduler_attr="real_time_engine",
    )

    def __init__(self, scheduler):
        self.scheduler = scheduler


class _NoCapEngine:
    """Engine that forgets to declare CAPABILITIES."""

    def __init__(self, scheduler):
        self.scheduler = scheduler


class _WrongTypeCapEngine:
    """Engine that declares CAPABILITIES with the wrong type."""

    CAPABILITIES = "not-an-EngineCapabilities"

    def __init__(self, scheduler):
        self.scheduler = scheduler


class _WrongAttrCapEngine:
    """Engine whose CAPABILITIES.requires_scheduler_attr doesn't match its spec."""

    CAPABILITIES = EngineCapabilities(
        name="WrongAttr",
        requires_scheduler_attr="wrong_attr",
    )

    def __init__(self, scheduler):
        self.scheduler = scheduler


# ---------------------------------------------------------------------------
# EngineCapabilities
# ---------------------------------------------------------------------------


def test_engine_capabilities_defaults():
    caps = EngineCapabilities(name="Test")
    assert caps.real_time_editing is False
    assert caps.predictive_analytics is False
    assert caps.cp_sat_optimization is False
    assert caps.requires_scheduler_attr == ""


def test_engine_capabilities_frozen():
    caps = EngineCapabilities(name="Test")
    with pytest.raises((AttributeError, TypeError)):
        caps.name = "Other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Protocol check
# ---------------------------------------------------------------------------


def test_optional_engine_protocol_satisfied_by_good_engine():
    assert hasattr(_GoodEngine, "CAPABILITIES")
    assert isinstance(_GoodEngine.CAPABILITIES, EngineCapabilities)


# ---------------------------------------------------------------------------
# _validate_capabilities
# ---------------------------------------------------------------------------

_REAL_TIME_SPEC = OptionalEngineSpec(
    config_flag="enable_real_time",
    module_path="saldo27.real_time_engine",
    class_name="RealTimeEngine",
    scheduler_attr="real_time_engine",
)


def test_validate_capabilities_passes_good_engine():
    assert _validate_capabilities(_GoodEngine, _REAL_TIME_SPEC) is True


def test_validate_capabilities_fails_missing_capabilities(recwarn):
    result = _validate_capabilities(_NoCapEngine, _REAL_TIME_SPEC)
    assert result is False


def test_validate_capabilities_fails_wrong_type(recwarn):
    result = _validate_capabilities(_WrongTypeCapEngine, _REAL_TIME_SPEC)
    assert result is False


def test_validate_capabilities_fails_wrong_scheduler_attr(recwarn):
    result = _validate_capabilities(_WrongAttrCapEngine, _REAL_TIME_SPEC)
    assert result is False


def test_validate_capabilities_passes_empty_requires_attr():
    """requires_scheduler_attr='' means 'no check' — should pass regardless."""

    class _NeutralEngine:
        CAPABILITIES = EngineCapabilities(name="Neutral")

        def __init__(self, scheduler):
            pass

    spec = OptionalEngineSpec(
        config_flag="x",
        module_path="x",
        class_name="X",
        scheduler_attr="some_attr",
    )
    assert _validate_capabilities(_NeutralEngine, spec) is True


# ---------------------------------------------------------------------------
# load_optional_engines — import failure
# ---------------------------------------------------------------------------


def test_optional_loader_disables_unavailable_engines(monkeypatch):
    scheduler = _make_scheduler()

    def _raise_import_error(_module):
        raise ModuleNotFoundError("missing")

    monkeypatch.setattr("saldo27.infrastructure.optional_engines.import_module", _raise_import_error)

    loaded = load_optional_engines(
        scheduler,
        {
            "enable_real_time": True,
            "enable_predictive_analytics": True,
        },
    )

    assert loaded == {}
    assert scheduler.real_time_engine is None
    assert scheduler.predictive_analytics is None


# ---------------------------------------------------------------------------
# load_optional_engines — capabilities contract enforcement
# ---------------------------------------------------------------------------


def _spec_for(engine_cls, scheduler_attr: str = "test_engine") -> OptionalEngineSpec:
    return OptionalEngineSpec(
        config_flag="enable_test",
        module_path="fake.module",
        class_name=engine_cls.__name__,
        scheduler_attr=scheduler_attr,
    )


def _load_with_fake_module(monkeypatch, engine_cls, scheduler_attr: str = "test_engine"):
    """Patch import_module and OPTIONAL_ENGINE_SPECS so only engine_cls is tested."""
    import types as _types

    fake_module = _types.ModuleType("fake.module")
    setattr(fake_module, engine_cls.__name__, engine_cls)

    spec = _spec_for(engine_cls, scheduler_attr)

    monkeypatch.setattr(
        "saldo27.infrastructure.optional_engines.import_module",
        lambda _path: fake_module,
    )
    monkeypatch.setattr(
        "saldo27.infrastructure.optional_engines.OPTIONAL_ENGINE_SPECS",
        (spec,),
    )

    scheduler = _make_scheduler()
    loaded = load_optional_engines(scheduler, {"enable_test": True})
    return scheduler, loaded


def test_load_disables_engine_missing_capabilities(monkeypatch):
    scheduler, loaded = _load_with_fake_module(monkeypatch, _NoCapEngine)
    assert loaded == {}
    assert scheduler.test_engine is None


def test_load_disables_engine_wrong_capabilities_type(monkeypatch):
    scheduler, loaded = _load_with_fake_module(monkeypatch, _WrongTypeCapEngine)
    assert loaded == {}
    assert scheduler.test_engine is None


def test_load_accepts_engine_with_valid_capabilities(monkeypatch):
    class _ValidEngine:
        CAPABILITIES = EngineCapabilities(
            name="Valid",
            real_time_editing=True,
            requires_scheduler_attr="test_engine",
        )

        def __init__(self, scheduler):
            self.scheduler = scheduler

    scheduler, loaded = _load_with_fake_module(monkeypatch, _ValidEngine, "test_engine")
    assert "test_engine" in loaded
    assert scheduler.test_engine is loaded["test_engine"]


def test_load_engine_disabled_by_config(monkeypatch):
    """Engine is not loaded when its config flag is False."""

    class _ValidEngine:
        CAPABILITIES = EngineCapabilities(name="Valid", requires_scheduler_attr="test_engine")

        def __init__(self, scheduler):
            pass

    spec = _spec_for(_ValidEngine, "test_engine")
    monkeypatch.setattr("saldo27.infrastructure.optional_engines.OPTIONAL_ENGINE_SPECS", (spec,))

    scheduler = _make_scheduler()
    loaded = load_optional_engines(scheduler, {"enable_test": False})

    assert loaded == {}
    assert scheduler.test_engine is None


# ---------------------------------------------------------------------------
# Real engines satisfy the contract
# ---------------------------------------------------------------------------


def test_real_time_engine_has_valid_capabilities():
    from saldo27.real_time_engine import RealTimeEngine

    caps = RealTimeEngine.CAPABILITIES
    assert isinstance(caps, EngineCapabilities)
    assert caps.real_time_editing is True
    assert caps.requires_scheduler_attr == "real_time_engine"


def test_predictive_analytics_engine_has_valid_capabilities():
    from saldo27.predictive_analytics import PredictiveAnalyticsEngine

    caps = PredictiveAnalyticsEngine.CAPABILITIES
    assert isinstance(caps, EngineCapabilities)
    assert caps.predictive_analytics is True
    assert caps.requires_scheduler_attr == "predictive_analytics"


# ---------------------------------------------------------------------------
# ortools_available
# ---------------------------------------------------------------------------


def test_ortools_available_returns_bool():
    result = ortools_available()
    assert isinstance(result, bool)


def test_ortools_available_cached(monkeypatch):
    """Second call must not re-probe (cached result reused)."""
    import saldo27.infrastructure.optional_engines as _mod

    # Reset cache so we control the probe
    monkeypatch.setattr(_mod, "_ortools_available", None)

    call_count = 0

    real_import = _mod.import_module

    def _counting_import(name):
        nonlocal call_count
        if "ortools" in name:
            call_count += 1
        return real_import(name)

    monkeypatch.setattr(_mod, "import_module", _counting_import)

    # Two calls should trigger at most one probe
    _mod.ortools_available()
    _mod.ortools_available()
    assert call_count <= 1


def test_ortools_available_false_when_missing(monkeypatch):
    import saldo27.infrastructure.optional_engines as _mod

    monkeypatch.setattr(_mod, "_ortools_available", None)
    monkeypatch.setattr(_mod, "import_module", lambda _name: (_ for _ in ()).throw(ImportError("no ortools")))

    assert _mod.ortools_available() is False
