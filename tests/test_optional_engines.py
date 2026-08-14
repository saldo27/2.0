import types

from saldo27.infrastructure.optional_engines import load_optional_engines


def test_optional_loader_disables_unavailable_engines(monkeypatch):
    scheduler = types.SimpleNamespace()

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
