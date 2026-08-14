from __future__ import annotations

import logging
from dataclasses import dataclass
from importlib import import_module
from typing import Any


@dataclass(frozen=True)
class OptionalEngineSpec:
    config_flag: str
    module_path: str
    class_name: str
    scheduler_attr: str
    extra_args_config_key: str | None = None


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
    ),
)


def load_optional_engines(scheduler: Any, config: dict[str, Any]) -> dict[str, Any]:
    loaded: dict[str, Any] = {}

    for spec in OPTIONAL_ENGINE_SPECS:
        setattr(scheduler, spec.scheduler_attr, None)
        if not config.get(spec.config_flag, spec.config_flag == "enable_predictive_analytics"):
            continue

        try:
            module = import_module(spec.module_path)
            engine_cls = getattr(module, spec.class_name)
            if spec.extra_args_config_key:
                engine = engine_cls(scheduler, config.get(spec.extra_args_config_key, {}))
            else:
                engine = engine_cls(scheduler)
            setattr(scheduler, spec.scheduler_attr, engine)
            loaded[spec.scheduler_attr] = engine
            logging.info("%s initialized", spec.class_name)
        except Exception as exc:
            logging.warning("%s not available - disabled: %s", spec.class_name, exc)

    return loaded
