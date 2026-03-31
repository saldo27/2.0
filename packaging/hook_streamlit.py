# hook-streamlit.py
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

# Excluir streamlit.hello explícitamente
excludedimports = ['streamlit.hello']

# Incluir solo módulos necesarios
hiddenimports = [
    'streamlit.web.cli',
    'streamlit.runtime.scriptrunner',
    'streamlit.runtime.state',
]
