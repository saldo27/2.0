# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import copy_metadata, collect_all
import os

block_cipher = None
SPEC_DIR = os.path.dirname(os.path.abspath(SPEC))

# ===== METADATA =====
datas = []
datas += copy_metadata('streamlit')
datas += copy_metadata('reportlab')

try:
    jaraco_datas, jaraco_binaries, jaraco_hiddenimports = collect_all('jaraco')
    datas += jaraco_datas
except:
    jaraco_hiddenimports = []

try:
    pkg_datas, pkg_binaries, pkg_hiddenimports = collect_all('pkg_resources')
    datas += pkg_datas
except:
    pkg_hiddenimports = []

# ===== MÓDULOS ESENCIALES =====
essential_modules = [
    'app_streamlit.py',
    'scheduler.py',
    'scheduler_config.py',
    'scheduler_core.py',
    'utilities. py',
    'exceptions.py',
    'statistics_calculator.py',
    'constraint_checker.py',
    'balance_validator.py',
    'worker_eligibility.py',
    'iterative_optimizer.py',
    'adjustment_utils.py',
    'pdf_exporter.py',
    'license_manager.py',
    'performance_cache.py',
    'real_time_engine.py',
    'change_tracker.py',
    'incremental_updater.py',
    'live_validator.py',
    'event_bus.py',
    'predictive_analytics.py',
    'predictive_optimizer.py',
    'demand_forecaster.py',
    'historical_data_manager.py',
]

for module in essential_modules:
    module_path = os.path.join(SPEC_DIR, module)
    if os.path.exists(module_path):
        datas.append((module_path, '.'))
        print(f"✓ {module}")

# ===== EXCLUSIONES =====
excludes = [
    'PyQt5', 
    'PyQt6', 
    'PySide2', 
    'PySide6',
    'tkinter', 
    'wx', 
    'kivy', 
    'kivymd', 
    'pygame',
    'MySQLdb',
    'psycopg2',
    'matplotlib', 
    'torch', 
    'tensorflow', 
    'sklearn',
    'langchain', 
    'openai', 
    'transformers',
    'IPython', 
    'jupyter', 
    'notebook',
    'pytest', 
    'unittest', 'doctest',
    'flask', 
    'django', 
    'fastapi',
    'streamlit.hello',
    'streamlit.external.langchain',
    'typeguard',
    'setuptools._vendor.typeguard',
]

# ===== HIDDENIMPORTS =====
hiddenimports = [
    'streamlit',
    'streamlit.web.cli',
    'streamlit.runtime.scriptrunner',
    'streamlit.runtime.state',
    'pandas',
    'pandas.core',
    'numpy',
    'plotly. graph_objects',
    'plotly.express',
    'reportlab',
    'reportlab.lib',
    'reportlab.platypus',
    'email',
    'email.mime',
    'importlib_metadata',
] + jaraco_hiddenimports + pkg_hiddenimports

# ===== ANALYSIS =====
a = Analysis(
    [os.path.join(SPEC_DIR, 'run_app.py')],
    pathex=[SPEC_DIR],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[SPEC_DIR],
    hooksconfig={},
    runtime_hooks=['rthook_streamlit.py'],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='GuardiasApp',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='icon.ico',
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='GuardiasApp',
)