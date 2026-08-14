"""
infrastructure/optional_engines.py
====================================
Formal contract for optional engines.

Every optional engine must declare a ``CAPABILITIES`` class attribute of type
:class:`EngineCapabilities`.  The loader enforces this contract at initialisation
time: engines that omit the attribute or declare incompatible capabilities produce
a clear diagnostic and are disabled rather than silently misbehaving.

OR-Tools availability is exposed through :func:`ortools_available` so that any
module that needs CP-SAT can ask once via the canonical path instead of issuing
its own ``importlib.import_module`` call.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Public contract types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EngineCapabilities:
    """Declares what an optional engine supports.

    Attributes:
        name: Human-readable engine name used in log messages.
        real_time_editing: Engine supports interactive, per-shift mutations.
        predictive_analytics: Engine supports demand forecasting / insights.
        cp_sat_optimization: Engine uses OR-Tools CP-SAT for optimisation.
        requires_scheduler_attr: Name of the attribute the engine will be
            stored under on the ``Scheduler`` instance.  Must match the
            ``scheduler_attr`` in the corresponding :class:`OptionalEngineSpec`.
    """

    name: str
    real_time_editing: bool = False
    predictive_analytics: bool = False
    cp_sat_optimization: bool = False
    requires_scheduler_attr: str = ""


@runtime_checkable
class OptionalEngine(Protocol):
    """Protocol that every optional engine must satisfy.

    Engines only need to expose ``CAPABILITIES``; the rest of their public API
    is engine-specific.  The protocol is deliberately minimal so that existing
    engines require only a one-line addition.
    """

    CAPABILITIES: EngineCapabilities


# ---------------------------------------------------------------------------
# OR-Tools availability (single authoritative check)
# ---------------------------------------------------------------------------

_ortools_available: bool | None = None  # cached after first probe


def ortools_available() -> bool:
    """Return ``True`` if ``ortools.sat.python.cp_model`` can be imported.

    The result is cached after the first call so subsequent checks are free.
    """
    global _ortools_available
    if _ortools_available is None:
        try:
            import_module("ortools.sat.python.cp_model")
            _ortools_available = True
            logging.debug("optional_engines: OR-Tools CP-SAT disponible.")
        except ImportError:
            _ortools_available = False
            logging.info("optional_engines: OR-Tools no disponible — Fase CP-SAT deshabilitada.")
    return _ortools_available


# ---------------------------------------------------------------------------
# Engine spec registry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OptionalEngineSpec:
    config_flag: str
    module_path: str
    class_name: str
    scheduler_attr: str
    extra_args_config_key: str | None = None
    default_enabled: bool = False
    required: bool = False


OPTIONAL_ENGINE_SPECS: tuple[OptionalEngineSpec, ...] = (
    OptionalEngineSpec(
        config_flag="enable_real_time",
        module_path="saldo27.real_time_engine",
        class_name="RealTimeEngine",
        scheduler_attr="real_time_engine",
    ),
    OptionalEngineSpec(
        config_flag="enable_predictive_analytics",
        module_path="saldo27.predictive_analytics",
        class_name="PredictiveAnalyticsEngine",
        scheduler_attr="predictive_analytics",
        extra_args_config_key="predictive_analytics_config",
        default_enabled=True,
    ),
)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _validate_capabilities(engine_cls: type, spec: OptionalEngineSpec) -> bool:
    """Return *True* if *engine_cls* satisfies the capabilities contract.

    Emits a warning and returns *False* on any violation so the caller can
    disable the engine instead of letting it run in an undefined state.
    """
    caps = getattr(engine_cls, "CAPABILITIES", None)
    if caps is None:
        logging.warning(
            "%s no declara CAPABILITIES — motor deshabilitado. "
            "Añade un atributo de clase 'CAPABILITIES: EngineCapabilities'.",
            spec.class_name,
        )
        return False
    if not isinstance(caps, EngineCapabilities):
        logging.warning(
            "%s.CAPABILITIES debe ser EngineCapabilities, encontrado %r — motor deshabilitado.",
            spec.class_name,
            type(caps),
        )
        return False
    if caps.requires_scheduler_attr and caps.requires_scheduler_attr != spec.scheduler_attr:
        logging.warning(
            "%s.CAPABILITIES.requires_scheduler_attr=%r no coincide con spec.scheduler_attr=%r "
            "— motor deshabilitado.",
            spec.class_name,
            caps.requires_scheduler_attr,
            spec.scheduler_attr,
        )
        return False
    return True


def load_optional_engines(scheduler: Any, config: dict[str, Any]) -> dict[str, Any]:
    """Initialise all registered optional engines according to *config*.

    For each engine spec:
    1. The scheduler attribute is set to ``None`` (safe default).
    2. If the config flag is falsy the engine is skipped.
    3. The module is imported; on ``ImportError`` the engine is disabled.
    4. The ``CAPABILITIES`` contract is validated; violations disable the engine.
    5. The engine is instantiated; exceptions disable the engine (or re-raise
       if ``spec.required`` is True).

    Returns a mapping of ``scheduler_attr → engine_instance`` for every engine
    that was successfully loaded.
    """
    loaded: dict[str, Any] = {}

    for spec in OPTIONAL_ENGINE_SPECS:
        setattr(scheduler, spec.scheduler_attr, None)
        if not config.get(spec.config_flag, spec.default_enabled):
            continue

        try:
            module = import_module(spec.module_path)
        except ImportError as exc:
            if spec.required:
                raise
            logging.warning("%s no disponible (módulo no instalado) — deshabilitado: %s", spec.class_name, exc)
            continue

        engine_cls = getattr(module, spec.class_name)

        if not _validate_capabilities(engine_cls, spec):
            if spec.required:
                raise RuntimeError(
                    f"{spec.class_name} no satisface el contrato de EngineCapabilities y es requerido."
                )
            continue

        try:
            if spec.extra_args_config_key:
                engine = engine_cls(scheduler, config.get(spec.extra_args_config_key, {}))
            else:
                engine = engine_cls(scheduler)
            setattr(scheduler, spec.scheduler_attr, engine)
            loaded[spec.scheduler_attr] = engine
            logging.info("%s inicializado (capacidades: %s)", spec.class_name, engine_cls.CAPABILITIES)
        except Exception as exc:
            logging.error(
                "Fallo al inicializar %s — el motor quedará deshabilitado. Error: %s",
                spec.class_name,
                exc,
                exc_info=True,
            )
            if spec.required:
                raise

    return loaded
