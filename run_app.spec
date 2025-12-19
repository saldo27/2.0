# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import copy_metadata, collect_data_files
import sys
import os

block_cipher = None

SPEC_DIR = os.path.dirname(os.path.abspath(SPEC))

# ===== METADATA =====
datas = []
datas += copy_metadata('streamlit')
datas += copy_metadata('reportlab')
datas += copy_metadata('importlib_metadata')

# ===== MÓDULOS STREAMLIT =====
essential_modules = [
    'app_streamlit.py',
    'scheduler.py',
    'scheduler_config.py',
    'scheduler_core.py',
    'utilities.py',
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

included_count = 0
missing_modules = []

for module in essential_modules: 
    module_path = os.path.join(SPEC_DIR, module)
    if os.path.exists(module_path):
        datas.append((module_path, '.'))
        print(f"✓ {module}")
        included_count += 1
    else:
        print(f"⚠ FALTA: {module}")
        missing_modules. append(module)

print(f"\n📦 Total módulos incluidos: {included_count}/{len(essential_modules)}")

# ===== EXCLUSIONES =====
excludes = [
    'PyQt5', 'PyQt6', 'PySide2', 'PySide6', 
    'tkinter', 'wx', 'kivy', 'matplotlib',
    'torch', 'tensorflow', 'sklearn', 'scipy',
    'langchain', 'openai', 'transformers',
    'IPython', 'jupyter', 'notebook', 
    'pytest', 'unittest', 'doctest',
    'shapely', 'geopandas', 'fiona', 'gdal',
    'flask', 'django', 'fastapi', 'aiohttp',
    'sqlalchemy', 'psycopg2', 'pymongo', 'mysql',
    'setuptools', 'pip', 'wheel', 
    'pkg_resources. py2_warn', 'pkg_resources.extern',
    'streamlit.external. langchain', 'streamlit.hello',
    'advanced_distribution_engine', 'strict_balance_optimizer',
    'adaptive_iterations', 'backtracking_manager',
    'dynamic_priority_manager', 'operation_prioritizer',
    'schedule_builder', 'real_time_ui',
    'collaboration_manager', 'data_manager',
    'websocket_handler', 'shift_tolerance_validator',
    'optimization_metrics', 'exporters',
    'progress_monitor', 'validate_config', 'main',
]

# ===== HIDDENIMPORTS =====
hiddenimports = [
    # Streamlit core
    'streamlit',
    'streamlit.web.cli',
    'streamlit.runtime. scriptrunner',
    'streamlit.runtime.state',
    
    # Data processing
    'pandas',
    'pandas.core',
    'pandas.core.computation',
    'numpy',
    'numpy.core',
    
    # Plotting
    'plotly. graph_objects',
    'plotly.express',
    'plotly.subplots',
    
    # PDF
    'reportlab',
    'reportlab.lib',
    'reportlab.lib.colors',
    'reportlab.lib.pagesizes',
    'reportlab.lib.styles',
    'reportlab.lib.units',
    'reportlab.platypus',
    'reportlab.pdfgen',
    
    # Email y metadata (necesarios para Streamlit)
    'email',
    'email.mime',
    'email.mime. text',
    'email.mime.multipart',
    'email.mime.base',
    'importlib_metadata',
    'importlib_resources',
    
    # Standard library
    'json', 'csv', 'logging', 'datetime', 'pathlib',
    'collections', 'copy', 'functools', 'traceback',
    'threading', 'queue',
    
    # TUS MÓDULOS
    'app_streamlit',
    'scheduler',
    'scheduler_config',
    'scheduler_core',
    'utilities',
    'exceptions',
    'statistics_calculator',
    'constraint_checker',
    'balance_validator',
    'worker_eligibility',
    'iterative_optimizer',
    'adjustment_utils',
    'pdf_exporter',
    'license_manager',
    'performance_cache',
    'real_time_engine',
    'change_tracker',
    'incremental_updater',
    'live_validator',
    'event_bus',
    'predictive_analytics',
    'predictive_optimizer',
    'demand_forecaster',
    'historical_data_manager',
]

# ===== ANALYSIS =====
a = Analysis(
    [os.path.join(SPEC_DIR, 'run_app.py')],
    pathex=[SPEC_DIR],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# ===== FILTRAR BINARIOS =====
print("\n🔍 Filtrando binarios pesados...")
original_binaries = len(a.binaries)

a.binaries = [
    (name, path, type_) 
    for name, path, type_ in a.binaries
    if not any(exclude in name.lower() for exclude in [
        'qt5', 'qt6', 'tcl86', 'tk86', 'd3dcompiler',
        'opengl32sw', 'mfc140', '_test', 'test_',
    ])
]

removed_binaries = original_binaries - len(a.binaries)
print(f"   Binarios:  {original_binaries} → {len(a.binaries)} (eliminados: {removed_binaries})")

# ===== FILTRAR MÓDULOS PYTHON =====
original_pure = len(a.pure)

a.pure = [
    (name, path, type_) 
    for name, path, type_ in a.pure
    if not any(exclude in name. lower() for exclude in [
        'test. ', 'tests.', 'setuptools. ', 'pip.',
        'distutils.', 'lib2to3.', 'pydoc_data.',
    ])
]

removed_pure = original_pure - len(a. pure)
print(f"   Módulos Python: {original_pure} → {len(a. pure)} (eliminados: {removed_pure})")

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# ===== EXE (ONEDIR) =====
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='GuardiasApp',
    debug=False,
    bootloader_ignore_signals=False,
    strip=True,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='icon.ico',
)

# ===== COLLECT =====
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

print(f"\n✅ Compilación completada - Modo ONEDIR")
print(f"   Módulos: {included_count}")