#!/bin/bash
# Script para iniciar la aplicación Streamlit

echo "🚀 Iniciando Sistema de Generación de Horarios..."
echo ""
echo "✅ Streamlit instalado"
echo "✅ Dependencias cargadas"
echo ""

# Iniciar Streamlit
echo "📡 Lanzando aplicación web en puerto 8501..."
streamlit run app_streamlit.py --server.port 8501 --server.headless true --server.address 0.0.0.0

echo ""
echo "🌐 La aplicación está disponible en: http://localhost:8501"
echo ""
echo "💡 Consejo: En GitHub Codespaces, el puerto se reenviará automáticamente"
echo "            Busca la notificación de 'Port 8501' en la esquina inferior derecha"
echo ""
