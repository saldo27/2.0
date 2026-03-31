# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import copy_metadata, collect_all, collect_submodules, collect_data_files
import os
import glob

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

# ===== RECOLECTAR STREAMLIT COMPLETO =====
streamlit_datas = collect_data_files('streamlit', include_py_files=True)
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
excludes = [
    'PyQt5', 'PyQt6', 'PySide2', 'PySide6',
    'tkinter', 'wx', 'kivy', 'kivymd', 'pygame',
    'matplotlib', 'torch', 'tensorflow', 'sklearn',
    'langchain', 'openai', 'transformers',
    'IPython', 'jupyter', 'notebook',
    'pytest', 'unittest', 'doctest',
    'flask', 'django', 'fastapi',
    'streamlit. hello',
    'typeguard',
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
] + streamlit_hiddenimports + jaraco_hiddenimports + pkg_hiddenimports

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
    upx_exclude=[],
    name='GuardiasApp',
)