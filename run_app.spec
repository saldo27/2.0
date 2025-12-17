# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils. hooks import copy_metadata, collect_data_files
import sys
import os

block_cipher = None

# ===== OBTENER DIRECTORIO BASE =====
# Asegurar que estamos en el directorio correcto
SPEC_DIR = os.path.dirname(os.path.abspath(SPEC))
print(f"📁 Working directory: {SPEC_DIR}")

# ===== RECOPILAR METADATA Y DATOS =====
datas = []
datas += copy_metadata('streamlit')
datas += copy_metadata('pandas')
datas += copy_metadata('plotly')
datas += copy_metadata('altair')
datas += copy_metadata('pillow')
datas += copy_metadata('reportlab')
datas += collect_data_files('streamlit')
# ===== INCLUIR TODOS LOS MÓDULOS DE TU APLICACIÓN =====
# Lista completa de archivos . py del repositorio saldo27/2.0
app_modules = [
    'adaptive_iterations.py',
    'adjustment_utils.py',
    'advanced_distribution_engine.py', 
    'app_streamlit.py',
    'backtracking_manager.py',
    'balance_validator.py',
    'change_tracker.py',  
    'collaboration_manager.py',
    'constraint_checker.py',
    'data_manager.py',
    'demand_forecaster.py',
    'dynamic_priority_manager.py',
    'event_bus.py',
    'exceptions.py',
    'exporters.py',
    'historical_data_manager.py',
    'incremental_updater.py',
    'iterative_optimizer.py',
    'live_validator.py',
    'main. py',
    'operation_prioritizer.py',
    'optimization_metrics.py',
    'pdf_exporter.py',
    'performance_cache.py',
    'predictive_analytics.py',
    'predictive_optimizer.py',
    'progress_monitor.py',
    'real_time_engine.py',
    'real_time_ui.py',
    'scheduler.py',
    'schedule_builder.py',
    'scheduler_config.py',
    'scheduler_core.py',  
    'shift_tolerance_validator.py',
    'statistics_calculator.py',  
    'strict_balance_optimizer.py',
    'utilities.py',
    'validate_config.py',
    'websocket_handler.py',
    'worker_eligibility.py',

  	
    # Agregar cualquier otro archivo . py que tengas
]

excludes=[
    'PyQt5', 
    'PySide6', 
    'PyQt6', 
    'tkinter', 
    'matplotlib', 
    'kivy',
    'streamlit.external. langchain',
    'langchain',
    'torch',                    # ← Agregar
    'torch.utils',              # ← Agregar
    'torch.utils.tensorboard',  # ← Agregar
    'tensorboard',              # ← Agregar
    'tensorflow',               # ← Agregar (por si acaso)
    'notebook',              # ← Agregar
    'notebook.services',     # ← Agregar
    'jupyter',               # ← Agregar
    'ipython',               # ← Agregar
    'django',                          # ← Agregar
    'django.db',                       # ← Agregar
    'django.db.backends',              # ← Agregar
    'django.db.backends.oracle',       # ← Agregar
    'sqlalchemy',                      # ← Agregar (por si acaso)
    'flask',                           # ← Agregar (por si acaso)
],

# Agregar cada módulo con ruta absoluta
for module in app_modules:
    module_path = os.path.join(SPEC_DIR, module)
    if os.path.exists(module_path):
        datas.append((module_path, '.'))
        print(f"✓ Included:  {module} (from {module_path})")
    else:
        print(f"⚠ WARNING: Module {module} not found at {module_path}!")

print(f"\n📦 Total modules to include: {len([d for d in datas if d[0]. endswith('.py')])}")

# ===== HIDDEN IMPORTS =====
hiddenimports = [
    # Streamlit core
    'streamlit',
    'streamlit.web.cli',
    'streamlit.web.bootstrap',
    'streamlit.runtime',
    'streamlit.runtime.scriptrunner',
    'streamlit.runtime.scriptrunner.magic_funcs',
    'streamlit. elements',
    'streamlit. elements.form',
    'streamlit.elements.widgets',
    'streamlit.components.v1',
    
    # Data processing
    'pandas',
    'pandas.core',
    'pandas.core.computation',
    'numpy',
    'numpy.core',
    
    # Plotting
    'plotly.graph_objects',
    'plotly. graph_objs',
    'plotly.express',
    'plotly.subplots',
    'altair',
    
    # PDF generation - MÁS ESPECÍFICO
    'reportlab',
    'reportlab.lib',
    'reportlab.lib.colors',
    'reportlab.lib.pagesizes',
    'reportlab.lib.styles',           # ← Asegurar que está
    'reportlab.lib.units',
    'reportlab.lib.enums',            # ← Agregar
    'reportlab.platypus',
    'reportlab.platypus.paragraph',   # ← Agregar
    'reportlab.platypus. tables',      # ← Agregar
    'reportlab.pdfbase',              # ← Agregar
    'reportlab.pdfbase. pdfmetrics',   # ← Agregar
    'reportlab.pdfbase._fontdata',    # ← Agregar
    'reportlab. rl_config',            # ← Agregar

    # Validators
    'validators',                      # ← Asegurar que está
    'validators.domain',               # ← Agregar
    'validators.email',                # ← Agregar
    'validators.url',                  # ← Agregar
    
    # Other dependencies
    'pydeck',
    'click',
    'validators',
    'watchdog',
    'watchdog.observers',
    'tornado',
    'tornado.web',
    'pyarrow',
    'pyarrow.parquet',
    'PIL',
    'PIL.Image',
    'requests',
    'zoneinfo',
    
    # Standard library
    'json',
    'csv',
    'logging',
    'traceback',
    'copy',
    'collections',
    'threading',
    'dataclasses',
    'enum',
    'functools',
    'hashlib',
    'pickle',
    'calendar',
    'pathlib',
    'datetime',
    'typing',
]

# ===== ANALYSIS =====
a = Analysis(
    [os.path.join(SPEC_DIR, 'run_app.py')],  # Ruta absoluta
    pathex=[SPEC_DIR],  # Agregar directorio al path
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'PyQt5', 
        'PySide6', 
        'PyQt6', 
        'tkinter', 
        'matplotlib', 
        'kivy',
        'streamlit.external. langchain',
        'langchain',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a. pure, a.zipped_data, cipher=block_cipher)

# ===== EXE (ONE-DIRECTORY MODE) =====
exe = EXE(
    pyz,
    a. scripts,
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
