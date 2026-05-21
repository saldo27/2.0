# hook-orjson.py
import importlib

try:
	_hooks = importlib.import_module("PyInstaller.utils.hooks")
	collect_dynamic_libs = _hooks.collect_dynamic_libs
except ModuleNotFoundError:
	# Permite análisis estático en entornos sin PyInstaller instalado.
	def collect_dynamic_libs(*_args, **_kwargs):
		return []

binaries = collect_dynamic_libs("orjson")
