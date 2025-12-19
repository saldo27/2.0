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

# Solucionar sys.stdin/stdout
if not sys.stdin or not hasattr(sys. stdin, 'isatty') or not sys.stdin.isatty():
    sys.stdin = open(os.devnull, 'r')
if not sys.stdout:
    sys.stdout = open(os.devnull, 'w')
if not sys.stderr:
    sys. stderr = open(os.devnull, 'w')

# PARCHE: Crear módulo dummy streamlit.hello
import types
streamlit_hello = types. ModuleType('streamlit. hello')
streamlit_hello.__file__ = '<dummy>'
streamlit_hello.__path__ = []
sys.modules['streamlit. hello'] = streamlit_hello

# Importar Streamlit
import streamlit. web.cli as stcli

def open_browser():
    """Abrir el navegador después de 3 segundos"""
    time. sleep(3)
    webbrowser.open('http://localhost:8501')

def run_streamlit():
    if getattr(sys, 'frozen', False):
        application_path = sys._MEIPASS
    else:
        application_path = os.path.dirname(os.path.abspath(__file__))
    
    app_script = os.path.join(application_path, 'app_streamlit.py')
    
    if not os.path.exists(app_script):
        print(f"Error: No se encuentra {app_script}")
        sys.exit(1)
    
    # ===== INICIAR THREAD PARA ABRIR NAVEGADOR =====
    browser_thread = threading.Thread(target=open_browser, daemon=True)
    browser_thread.start()
    # ================================================
    
    sys.argv = [
        "streamlit", "run", app_script,
        "--server.port=8501",
        "--server.headless=false",
        "--browser.serverAddress=localhost",
        "--browser.gatherUsageStats=false",
        "--server.enableStaticServing=false",
        "--global.developmentMode=false",
    ]
    
    try:
        sys.exit(stcli.main())
    except SystemExit as e: 
        sys.exit(e.code)
    except Exception as e: 
        print(f"Error:  {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    run_streamlit()
