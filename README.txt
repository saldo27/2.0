================================================================================
                    GuardiasApp - Aplicación para Distribución de Guardias
                                  Versión 2.0
================================================================================

DESCRIPCIÓN:
------------
GuardiasApp es una aplicación profesional para la generación automática de 
horarios de guardias médicas con optimización avanzada y balance equitativo.

CARACTERÍSTICAS: 
----------------
✓ Generación automática de horarios optimizados
✓ Balance proporcional de turnos y fines de semana
✓ Gestión de incompatibilidades entre trabajadores
✓ Días obligatorios y días libres configurables
✓ Períodos de trabajo personalizados
✓ Exportación a PDF y CSV
✓ Análisis predictivo y simulador de escenarios
✓ Verificación automática de restricciones

REQUISITOS DEL SISTEMA:
-----------------------
- Sistema Operativo: Windows 10/11 (64-bit)
- RAM:  Mínimo 4 GB (recomendado 8 GB)
- Espacio en disco: 500 MB libres
- Resolución de pantalla: 1280x720 o superior
- No requiere instalación de Python

INSTALACIÓN:
------------
1. Ejecutar GuardiasApp_Setup_v2.0.exe
2. Seguir las instrucciones del asistente de instalación
3. Lanzar desde el acceso directo del escritorio o menú inicio

PRIMERA EJECUCIÓN:
------------------
1. Abrir GuardiasApp
2. Ir a "Gestión de Médicos"
3. Agregar trabajadores (manual o importar JSON)
4. Configurar fechas y parámetros en la barra lateral
5. Presionar "🚀 Generar Calendario"

VERSIÓN DEMO:
-------------
La versión DEMO incluye las siguientes limitaciones:
- Máximo 10 generaciones de horarios
- Máximo 15 trabajadores
- Máximo 31 días de período
- Marca de agua en PDFs exportados

Para desbloquear funcionalidades completas, contactar para obtener una 
clave de licencia. 

ACTIVAR LICENCIA COMPLETA:
---------------------------
1. En la barra lateral, expandir "🔑 Activar Licencia"
2. Introducir la clave proporcionada
3. Presionar "Activar"

Formato de clave: GP-XXXX-XXXX-XXXX

ARCHIVOS DE CONFIGURACIÓN:
---------------------------
Los datos de licencia y uso se guardan en:
%USERPROFILE%\. guardiasapp\

- license.dat: Información de licencia activada
- usage.dat: Contador de usos (solo en modo DEMO)

EXPORTACIÓN DE DATOS:
----------------------
- CSV:  Formato compatible con Excel
- PDF: Tres tipos de reportes disponibles
  * Resumen Ejecutivo (Global)
  * Calendario Visual Completo
  * Estadísticas y Desglose Detallado
- JSON: Respaldo completo (trabajadores + configuración + calendario)

FORMATOS DE IMPORTACIÓN:
-------------------------
Trabajadores (JSON):
[
  {
    "id": "DOC001",
    "work_percentage": 100,
    "target_shifts": 0,
    "auto_calculate_shifts": true,
    "mandatory_days": "01-01-2026; 15-01-2026",
    "days_off": "20-01-2026",
    "work_periods": "01-01-2026 - 31-12-2026",
    "is_incompatible": false,
    "incompatible_with": []
  }
]

SOLUCIÓN DE PROBLEMAS:
----------------------
P:  La aplicación no inicia
R: Verificar que no esté bloqueada por antivirus/firewall

P: Error al generar calendario
R: Verificar restricciones (días obligatorios vs días libres)
   Reducir número de incompatibilidades
   Aumentar tolerancia de desviación

P: "Límite de usos alcanzado"
R: Contactar para obtener licencia completa

P: PDF con marca de agua
R: Función de versión DEMO.  Activar licencia completa. 

CONTACTO Y SOPORTE:
-------------------
Email: luisherrerapara@gmail.com
Web: https://github.com/saldo27/2.0

Para reportar errores o solicitar funcionalidades, usar el sistema de 
issues en GitHub o contactar por email. 

CRÉDITOS:
---------
Desarrollado por:  Luis Herrera Para
Versión: 2.0
Fecha:  Diciembre 2025

COPYRIGHT:
----------
© 2025 Luis Herrera Para. Todos los derechos reservados. 

HISTORIAL DE VERSIONES:
------------------------
v2.0 (Dic 2025):
- Interfaz Streamlit moderna
- Sistema de licencias DEMO
- Análisis predictivo y simulador What-If
- Exportación mejorada de PDFs
- Optimización de rendimiento

v1.0 (Anterior):
- Interfaz Kivy
- Generación básica de horarios

================================================================================
                          ¡Gracias por usar GuardiasApp!
================================================================================