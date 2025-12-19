# rthook_streamlit.py
import sys
import types

# Crear módulo dummy para streamlit.hello
if 'streamlit.hello' not in sys.modules:
    streamlit_hello = types.ModuleType('streamlit.hello')
    streamlit_hello.__file__ = '<runtime_hook_dummy>'
    streamlit_hello.__path__ = []
    sys.modules['streamlit.hello'] = streamlit_hello
    print("✓ Runtime hook:  streamlit.hello dummy creado")
