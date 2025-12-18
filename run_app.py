import sys
import os
from streamlit. web import cli as stcli
import traceback
import webbrowser
import time
from threading import Thread

def open_browser():
    """Abrir navegador después de un pequeño delay"""
    print("⏳ Esperando a que Streamlit inicie...")
    time.sleep(5)  # Esperar 5 segundos para que Streamlit inicie completamente
    print("🌐 Abriendo navegador...")
    webbrowser.open('http://localhost:8501')

if __name__ == '__main__':  
    try: 
        # Determinar la ruta de ejecución
        if getattr(sys, 'frozen', False):
            # Si está empaquetado con PyInstaller
            application_path = sys._MEIPASS
        else: 
            # Si se ejecuta desde Python
            application_path = os. path.dirname(os.path. abspath(__file__))
        
        # Cambiar al directorio de la aplicación
        os.chdir(application_path)
        
        # Agregar al path para que encuentre todos los módulos
        sys.path. insert(0, application_path)
        
        # Ruta al archivo principal
        app_file = os.path.join(application_path, "app_streamlit.py")
        
        print(f"📁 Application path: {application_path}")
        print(f"📄 App file: {app_file}")
        print(f"✅ App file exists: {os.path.exists(app_file)}")
        
        # Configurar argumentos de Streamlit
        sys.argv = [
            "streamlit",
            "run",
            app_file,
            "--server.headless=true",
            "--global.developmentMode=false",
            "--browser.serverAddress=localhost",
            "--browser.gatherUsageStats=false",
        ]
        
        print("\n🚀 Iniciando Streamlit...")
        print("="*60)
        
        # Iniciar thread para abrir navegador automáticamente
        browser_thread = Thread(target=open_browser, daemon=True)
        browser_thread.start()
        
        # Ejecutar Streamlit
        sys.exit(stcli.main())
        
    except Exception as e:
        # Capturar y mostrar errores
        error_msg = f"ERROR: {str(e)}\n\n{traceback.format_exc()}"
        
        # Guardar en archivo
        try:
            error_file = os.path.join(os. getcwd(), "error_log.txt")
            with open(error_file, "w", encoding="utf-8") as f:
                f.write(error_msg)
            print(f"\n❌ Error guardado en:  {error_file}")
        except:  
            pass
        
        # Mostrar en consola
        print("\n" + "="*60)
        print("❌ ERROR FATAL")
        print("="*60)
        print(error_msg)
        print("="*60)
        input("\nPresione Enter para cerrar...")
        sys.exit(1)
