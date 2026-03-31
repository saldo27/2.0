# rthook_streamlit.py
import sys
import types

# Crear módulo dummy para streamlit.hello
if 'streamlit. hello' not in sys.modules:
    streamlit_hello = types.ModuleType('streamlit.hello')
    streamlit_hello.__file__ = '<runtime_hook_dummy>'
    streamlit_hello.__path__ = []
    
    # Agregar atributo streamlit_app
    def dummy_streamlit_app():
        pass
    
    streamlit_hello.streamlit_app = dummy_streamlit_app
    sys.modules['streamlit.hello'] = streamlit_hello
    print("Runtime hook:  streamlit.hello dummy creado con streamlit_app")
