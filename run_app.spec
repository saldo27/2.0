# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import copy_metadata, collect_all, collect_submodules
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

# ===== RECOLECTAR TODOS LOS SUBMÓDULOS DE STREAMLIT =====
streamlit_submodules = collect_submodules('streamlit. runtime')
streamlit_submodules += collect_submodules('streamlit.web')
print(f"Submódulos de Streamlit encontrados: {len(streamlit_submodules)}")
# ========================================================

# ===== AGREGAR ARCHIVOS ESTÁTICOS DE STREAMLIT =====
import streamlit
import glob

streamlit_dir = os.path.dirname(streamlit.__file__)
streamlit_static = os.path.join(streamlit_dir, 'static')
if os.path.exists(streamlit_static):
    datas.append((streamlit_static, 'streamlit/static'))
    print(f"Agregando Streamlit static desde:  {streamlit_static}")
# ===================================================

# ===== INCLUIR TODOS LOS ARCHIVOS .PY DEL PROYECTO =====
py_files = glob.glob(os.path.join(SPEC_DIR, '*.py'))
for py_file in py_files:
    basename = os.path.basename(py_file)
    if basename != 'run_app.py': 
        datas.append((py_file, '.'))
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
    'streamlit.hello',
    'streamlit.external.langchain',
    'typeguard',
    'setuptools._vendor.typeguard',
]

# ===== HIDDENIMPORTS =====
hiddenimports = [
    'streamlit',
    'streamlit. web.cli',
    'pandas',
    'pandas.core',
    'numpy',
    'plotly. graph_objects',
    'plotly.express',
    'reportlab',
    'reportlab. lib',
    'reportlab. platypus',
    'email',
    'email.mime',
    'importlib_metadata',
] + streamlit_submodules + jaraco_hiddenimports + pkg_hiddenimports  # ← AGREGAR streamlit_submodules

# ===== ANALYSIS =====
a = Analysis(
    [os. path.join(SPEC_DIR, 'run_app.py')],
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