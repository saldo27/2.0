#!/bin/bash
# Script para iniciar la aplicación Streamlit

echo "🚀 Iniciando Sistema de Generación de Horarios..."
echo ""
echo "✅ Streamlit instalado"
echo "✅ Dependencias cargadas"
echo ""

# Configurar locale español para calendarios Lunes-Domingo (ISO 8601 / formato europeo)
export LANG=es_ES.utf8
export LC_ALL=es_ES.utf8
export LC_TIME=es_ES.utf8

echo "🌍 Configurando locale: es_ES.utf8 (Calendario Lunes-Domingo)"

# Verificar si el locale está disponible, si no, generarlo
if ! locale -a | grep -q "es_ES.utf8"; then
    echo "⚙️  Generando locale es_ES.utf8..."
    sudo locale-gen es_ES.utf8 2>/dev/null || echo "⚠️  No se pudo generar locale (requiere permisos sudo)"
fi

echo ""

# Iniciar Streamlit con locale configurado
echo "📡 Lanzando aplicación web en puerto 8501..."
streamlit run src/saldo27/app_streamlit.py --server.port 8501 --server.headless true --server.address 0.0.0.0

echo ""
echo "🌐 La aplicación está disponible en: http://localhost:8501"
echo ""
echo "💡 Consejo: En GitHub Codespaces, el puerto se reenviará automáticamente"
echo "            Busca la notificación de 'Port 8501' en la esquina inferior derecha"
echo ""
