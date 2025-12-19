import os
import sys
import multiprocessing
import webbrowser
import time
import threading

# Configurar entorno
os.environ['STREAMLIT_SERVER_ENABLE_STATIC_SERVING'] = 'false'
os.environ['STREAMLIT_BROWSER_GATHER_USAGE_STATS'] = 'false'
os.environ['STREAMLIT_SERVER_HEADLESS'] = 'true'
os.environ['STREAMLIT_GLOBAL_DEVELOPMENT_MODE'] = 'false'

# ===== HABILITAR LOGS CON UTF-8 =====
if getattr(sys, 'frozen', False):
    log_dir = os. path.dirname(sys. executable)
    sys.stdout = open(os.path.join(log_dir, 'streamlit_stdout.log'), 'w', encoding='utf-8', buffering=1)
    sys.stderr = open(os.path. join(log_dir, 'streamlit_stderr.log'), 'w', encoding='utf-8', buffering=1)
# =====================================

# Solucionar sys.stdin
if not sys.stdin or not hasattr(sys.stdin, 'isatty') or not sys.stdin.isatty():
    sys.stdin = open(os.devnull, 'r')

# ===== PARCHE MEJORADO:  Crear módulo dummy streamlit. hello =====
import types

streamlit_hello = types.ModuleType('streamlit.hello')
streamlit_hello.__file__ = '<dummy>'
streamlit_hello.__path__ = []

# Crear streamlit_app como MÓDULO, no función
streamlit_app_module = types.ModuleType('streamlit_app')
streamlit_app_module.__file__ = '<dummy_app>'
streamlit_hello.streamlit_app = streamlit_app_module

sys.modules['streamlit.hello'] = streamlit_hello
sys.modules['streamlit.hello. streamlit_app'] = streamlit_app_module
# ==============================================================

# Importar Streamlit
import streamlit.web.cli as stcli

def open_browser():
    """Abrir el navegador después de 5 segundos"""
    time. sleep(5)
    webbrowser.open('http://localhost:8501')

def run_streamlit():
    if getattr(sys, 'frozen', False):
        application_path = sys._MEIPASS
    else:  
        application_path = os.path.dirname(os.path. abspath(__file__))
    
    app_script = os. path.join(application_path, 'app_streamlit.py')
    
    if not os.path.exists(app_script):
        print("ERROR: No se encuentra app_streamlit.py")
        sys.exit(1)
    
    print(f"Iniciando Streamlit desde:  {app_script}")
    
    # Iniciar thread para abrir navegador
    browser_thread = threading.Thread(target=open_browser, daemon=True)
    browser_thread.start()
    
    sys.argv = [
        "streamlit",
        "run",
        app_script,
        "--server.port=8501",
        "--server.headless=true",
        "--browser.serverAddress=localhost",
        "--browser.gatherUsageStats=false",
        "--server.enableStaticServing=false",
        "--global.developmentMode=false",
    ]
    
    try:
        print("Ejecutando Streamlit...")
        sys.exit(stcli.main())
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    run_streamlit()
