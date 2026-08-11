# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import copy_metadata, collect_submodules, collect_data_files
import os
import glob

block_cipher = None
SPEC_DIR = os.path.dirname(os.path.abspath(SPEC))

# ===== METADATA =====
# copy_metadata() is required because streamlit/reportlab read their own
# package metadata (version, etc.) via importlib.metadata at runtime.
datas = []
datas += copy_metadata('streamlit')
datas += copy_metadata('reportlab')

# NOTE: `collect_all('jaraco')` and `collect_all('pkg_resources')` used to be
# forced here, but none of this project's runtime dependencies (streamlit,
# reportlab, pandas, numpy, plotly, scikit-learn, statsmodels, ortools,
# pdfplumber, openpyxl) actually import `pkg_resources`/`jaraco` at runtime.
# `collect_all` bundles the *entire* package (including vendored data and
# unrelated submodules), which added noticeable size for no functional
# benefit. If a future dependency needs them, prefer a targeted
# `collect_submodules`/hiddenimports entry over `collect_all`.

# ===== RECOLECTAR STREAMLIT (solo código, sin duplicar .py como datos) =====
# include_py_files=True duplicated every streamlit .py source file as a data
# file on top of the compiled modules already pulled in via hiddenimports,
# effectively shipping streamlit's source twice. Data files (non-.py assets
# such as the static frontend build, config templates, etc.) are still
# collected normally without include_py_files.
#
# NOTE: `hook_streamlit.py` in this directory (underscore) is NOT picked up
# automatically by PyInstaller — auto-discovered hooks must be named
# `hook-<module>.py` (hyphen). That's why streamlit hiddenimports are
# collected manually below via `collect_submodules('streamlit')` instead of
# relying on that file's smaller, curated hiddenimports list.
streamlit_datas = collect_data_files('streamlit')
datas += streamlit_datas
streamlit_hiddenimports = collect_submodules('streamlit')
print(f"Streamlit: {len(streamlit_hiddenimports)} submódulos incluidos")
# =========================================

# ===== INCLUIR TODOS LOS ARCHIVOS .PY DEL PROYECTO =====
py_files = glob.glob(os.path.join(SPEC_DIR, '*.py'))
for py_file in py_files: 
    basename = os.path. basename(py_file)
    if basename != 'run_app.py':
        datas. append((py_file, '.'))
        print(f"Incluido:  {basename}")
# ========================================================

# ===== EXCLUSIONES =====
# NOTE: 'sklearn' (scikit-learn) must NOT be excluded — it is a real,
# declared dependency used by demand_forecaster.py/predictive_analytics.py
# for the demand forecasting feature. It used to be listed here, which
# silently disabled that feature in built .exe releases.
excludes = [
    'PyQt5', 'PyQt6', 'PySide2', 'PySide6',
    'tkinter', 'wx', 'kivy', 'kivymd', 'pygame',
    'matplotlib', 'torch', 'tensorflow',
    'langchain', 'openai', 'transformers',
    'IPython', 'jupyter', 'notebook',
    'pytest', 'unittest', 'doctest',
    'flask', 'django', 'fastapi',
    'streamlit. hello',
    'typeguard',
    # Heavy packages not used by this project or its runtime dependencies;
    # harmless no-ops if they are not present, kept as a safeguard against
    # them being pulled in accidentally by a future dependency bump.
    'bokeh', 'numba', 'PIL.ImageQt',
]

# ===== HIDDENIMPORTS =====
hiddenimports = [
    'pandas',
    'pandas.core',
    'numpy',
    'plotly. graph_objects',
    'plotly.express',
    'plotly.io',           # ← Módulo principal de I/O
    'plotly.io.json',      # ← Submódulo JSON (sin guion bajo)
    'orjson',              # ← Librería de serialización JSON rápi
    'plotly.subplots',
    'plotly.validators',
    # orjson para serialización rápida
    'orjson',
    'reportlab',
    'reportlab. lib',
    'reportlab. platypus',
    'reportlab.pdfbase',           
    'reportlab.pdfbase.ttfonts',   
    'reportlab.pdfbase.pdfmetrics',
    'reportlab.lib.colors',        
    'reportlab.lib.pagesizes',     
    'email',
    'email.mime',
    'importlib_metadata',
    'scheduler_core',
    'schedule_builder',
    'iterative_optimizer',
    'predictive_optimizer',
    'balance_validator',
    'adjustment_utils',
    'pdf_exporter',
] + streamlit_hiddenimports

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

pyz = PYZ(a. pure, a.zipped_data, cipher=block_cipher)

# DLLs known to be corrupted or unstable when compressed by UPX; keep them
# uncompressed while still UPX-compressing the rest of the bundle.
UPX_EXCLUDE = [
    'vcruntime140.dll', 'vcruntime140_1.dll', 'msvcp140.dll',
    'python3.dll', 'python310.dll', 'python311.dll', 'python312.dll',
    'api-ms-win-*.dll',
]

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
    upx_exclude=UPX_EXCLUDE,
    console=False,
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
    upx_exclude=UPX_EXCLUDE,
    name='GuardiasApp',
)