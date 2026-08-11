# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import copy_metadata, collect_submodules, collect_data_files
import os

block_cipher = None
SPEC_DIR = os.path.dirname(os.path.abspath(SPEC))
# The project code lives in src/saldo27 (installable package), one level up
# from this packaging/ directory — NOT alongside this .spec file.
PROJECT_ROOT = os.path.dirname(SPEC_DIR)
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')
PACKAGE_DIR = os.path.join(SRC_DIR, 'saldo27')

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

# ===== RECOLECTAR EL PAQUETE saldo27 =====
# El código de la aplicación es un paquete instalable real
# (src/saldo27/__init__.py), no un conjunto de scripts sueltos, así que sus
# submódulos se recolectan igual que los de streamlit —vía
# collect_submodules— en lugar de copiar manualmente cada .py como dato.
saldo27_hiddenimports = collect_submodules('saldo27')
print(f"saldo27: {len(saldo27_hiddenimports)} submódulos incluidos")

# `app_streamlit.py` is the one exception: `run_app.py` hands its path to
# streamlit's CLI (`streamlit run <script>`), which needs an actual .py file
# on disk at runtime (sys._MEIPASS) — not just the compiled module bundled
# via collect_submodules above. Copy just that file to the bundle root.
datas.append((os.path.join(PACKAGE_DIR, 'app_streamlit.py'), '.'))
# ==========================================

# NOTE sobre tamaño: el mayor contribuyente conocido al tamaño final es
# `ortools` (motor CP-SAT usado de forma opcional en
# final_adjustment_engine.py para el ajuste final). Es una dependencia
# funcional real (no se puede excluir sin perder esa función) y trae
# binarios nativos de C++ que no se benefician de los filtros de datos de
# más abajo. Si el tamaño sigue siendo un problema, la única palanca
# adicional realista sería ofrecer una build opcional sin ortools/
# scikit-learn/statsmodels para usuarios que no necesiten ajuste avanzado
# ni pronóstico de demanda.

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
    # streamlit.external.langchain: streamlit's optional langchain callback
    # integration is unused by this project. On some Python/typing_extensions
    # combinations, PyInstaller's collect_submodules('streamlit') triggers a
    # harmless "Failed to collect submodules" WARNING while probing it
    # (ForwardRef._evaluate() TypeError); excluding it here keeps intent
    # explicit even though the warning itself happens during the probe and
    # can't be fully suppressed.
    'streamlit.external.langchain',
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
    'saldo27',
    'saldo27.scheduler_core',
    'saldo27.schedule_builder',
    'saldo27.iterative_optimizer',
    'saldo27.predictive_optimizer',
    'saldo27.balance_validator',
    'saldo27.adjustment_utils',
    'saldo27.pdf_exporter',
] + streamlit_hiddenimports + saldo27_hiddenimports

# ===== ANALYSIS =====
a = Analysis(
    [os.path.join(PACKAGE_DIR, 'run_app.py')],
    pathex=[SRC_DIR],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[SPEC_DIR],
    hooksconfig={},
    runtime_hooks=[os.path.join(SPEC_DIR, 'rthook_streamlit.py')],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# ===== FILTRAR DATOS NO NECESARIOS EN TIEMPO DE EJECUCIÓN =====
# Se aplica DESPUÉS de Analysis() porque los hooks de PyInstaller para
# dependencias como scipy/sklearn/pandas/statsmodels (que recogen TODOS los
# archivos de datos del paquete vía collect_data_files) se ejecutan durante
# el propio Analysis() y solo se ven reflejados en `a.datas`, no en la lista
# `datas` construida manualmente más arriba.
# `a.datas` usa formato TOC: tuplas (nombre_destino, ruta_origen, tipo).
_TEST_DATA_MARKERS = (
    os.sep + 'tests' + os.sep,
    os.sep + 'test' + os.sep,
    os.sep + 'testing' + os.sep,
)


def _is_test_fixture(src_path):
    normalized = os.path.normpath(src_path)
    return any(marker in normalized for marker in _TEST_DATA_MARKERS)


# Babel (dependencia transitoria de streamlit/altair) incluye datos de
# localización compilados (locale-data/*.dat) para ~1000 locales. La app solo
# necesita el fallback universal ('root'), inglés (usado por defecto por
# varias librerías) y español (locale de la propia aplicación, ver
# app_streamlit.py: locale.setlocale(locale.LC_TIME, "es_ES...")).
_BABEL_LOCALE_ALLOWLIST = {'root.dat', 'en.dat', 'en_US.dat', 'es.dat', 'es_ES.dat'}


def _is_prunable_babel_locale(src_path):
    normalized = os.path.normpath(src_path)
    marker = os.sep + 'locale-data' + os.sep
    if marker not in normalized:
        return False
    return os.path.basename(normalized) not in _BABEL_LOCALE_ALLOWLIST


_datas_before = len(a.datas)
a.datas = [
    (dest, src, typ)
    for (dest, src, typ) in a.datas
    if not _is_test_fixture(src) and not _is_prunable_babel_locale(src)
]
print(f"Datos descartados (fixtures de tests / locales no usados): {_datas_before - len(a.datas)}")
# ================================================================

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
    icon=os.path.join(PROJECT_ROOT, 'icon.ico'),
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