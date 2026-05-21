# hook-streamlit.py
import importlib

try:
    _hooks = importlib.import_module("PyInstaller.utils.hooks")
    collect_submodules = _hooks.collect_submodules
    collect_data_files = _hooks.collect_data_files
except ModuleNotFoundError:
    # Permite análisis estático en entornos sin PyInstaller instalado.
    def collect_submodules(*_args, **_kwargs):
        return []

    def collect_data_files(*_args, **_kwargs):
        return []

# Excluir streamlit.hello explícitamente
excludedimports = ['streamlit.hello']

# Incluir solo módulos necesarios
hiddenimports = [
    'streamlit.web.cli',
    'streamlit.runtime.scriptrunner',
    'streamlit.runtime.state',
]
