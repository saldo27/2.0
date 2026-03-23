================================================================================
                    GuardiasApp - Aplicación para Distribución de Guardias
                                  Versión 2.6
================================================================================

DESCRIPCIÓN:
------------
GuardiasApp es una aplicación profesional para la generación automática de 
horarios de guardias médicas con optimización avanzada y balance equitativo.

CARACTERÍSTICAS: 
----------------
✓ Generación automática de horarios optimizados
✓ Balance proporcional de turnos y fines de semana
✓ Balance equitativo de turnos en días puente
✓ Gestión de incompatibilidades entre trabajadores
✓ Días obligatorios y días libres configurables
✓ Períodos de trabajo personalizados
✓ Exportación a PDF y CSV
✓ Análisis predictivo y simulador de escenarios
✓ Verificación automática de restricciones
✓ Análisis de calendarios de guardias (tab "Revisión")
✓ Cálculo de estadísticas por trabajador (Fin de Semana, Festivos, Rosell, Puentes)
✓ Estadísticas y gráficos de turnos en puente
✓ Detección de guardias consecutivas
✓ Exportación de análisis a PDF y Excel
✓ Interfaz optimizada con columnas de ancho fijo
✓ Configuración de locale español para calendarios L-D
✓ Importación de calendario previo para constraints cross-período (NUEVO v2.6)
✓ Ajuste automático de cuotas basado en historial del período anterior (NUEVO v2.6)
✓ Distribución de fines de semana con memoria de período previo (NUEVO v2.6)
✓ Estrategias de distribución inicial diversificadas mediante GRASP-RCL (NUEVO v2.6)

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
6. Para analizar calendarios: ir a tab "Revisión", cargar archivo PDF/Excel,
   configurar fecha inicial y festivos, luego presionar "🚀 Analizar Horario"

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
  * Análisis de Guardias (Tab Revisión)
- Excel: Estadísticas y alertas de guardias consecutivas (Tab Revisión)
- JSON: Respaldo completo (trabajadores + configuración + calendario)
         Este formato es también el que se usa para importar un calendario
         previo en el expander "📅 Calendario Anterior" de la barra lateral.

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
Versión: 2.6
Fecha:  Marzo 2026

COPYRIGHT:
----------
© 2025 Luis Herrera Para. Todos los derechos reservados. 

CALENDARIO PREVIO (NUEVO v2.6):
--------------------------------
El sistema puede cargar un calendario exportado de un período anterior para
tener en cuenta la carga de trabajo ya realizada al generar el nuevo reparto.

Cómo usarlo:
1. Generar y exportar el calendario del período anterior (botón
   "💾 Descargar Respaldo Completo (JSON)" en Importar/Exportar).
2. En la barra lateral del nuevo reparto, expandir "📅 Calendario Anterior".
3. Cargar el archivo JSON del período anterior y pulsar "📥 Cargar".
4. Generar el nuevo calendario normalmente con "🚀 Generar Calendario".

Qué hace el sistema con el calendario previo:
- Huecos mínimos: los últimos turnos del período anterior se tienen en cuenta
  para no violar el hueco mínimo configurado entre guardias en los primeros
  días del nuevo período.
- Patrón mismo día de semana 7/14 días: se bloquea también cruzando el límite
  de período (si un trabajador hizo guardia el último lunes del mes anterior,
  no se le asignará el primer lunes del mes siguiente).
- Viernes-Lunes: la regla de no asignar viernes+lunes consecutivos se aplica
  respetando el fin del período anterior.
- Fines de semana consecutivos: la cuenta de fines de semana consecutivos
  continúa desde el último fin de semana del período anterior.
- Cuota proporcional de fines de semana: el cap de fines de semana del nuevo
  período se reduce en función de los fines de semana ya trabajados en el
  período previo.
- Ajuste de target de turnos: si un trabajador hizo más (o menos) guardias
  de las previstas en el período anterior, su cuota del nuevo período se
  reduce (o aumenta) en la misma cantidad para compensar el desequilibrio.
- Prioridad de asignación: los conteos del período anterior se suman a los
  del nuevo período para que el motor priorice siempre a los trabajadores
  con menos guardias acumuladas.

Nota: Solo se tienen en cuenta asignaciones de los últimos 90 días anteriores
al inicio del nuevo período para evitar que períodos muy lejanos distorsionen
las restricciones.

HISTORIAL DE VERSIONES:
------------------------
v2.6 (Marzo 2026):
- NUEVO: Importación de calendario previo (expander "📅 Calendario Anterior")
- Constraints cross-período: huecos mínimos, patrón 7/14 días, viernes-lunes
  y fines de semana consecutivos se respetan cruzando el límite de período
- Ajuste automático de cuota de turnos según sobre/infra-entrega del período
  anterior (delta = turnos_reales - turnos_objetivo del período previo)
- Distribución proporcional de fines de semana con memoria del período previo
- Prioridad de asignación incorpora carga acumulada del período anterior
- Corrección de bug: 7 errores de distribución de fines de semana y puentes
  (round/int inconsistency, off-by-one en underloaded removal, falta de
  comprobación de semana del receptor, paso post-finalización ausente,
  tolerancia del paso estricto de Last Post incorrecta, ternario roto en
  shifts_per_day, GRASP_ALPHA demasiado estrecho)
- Corrección de diversidad de estrategias en Fase 1: todas las estrategias
  A-Z y Z-A producían calendarios idénticos; reemplazadas por GRASP-RCL con
  semillas únicas; solo se conserva 1 estrategia determinista de cada tipo

v2.5 (Febrero 2026):
- Estadísticas completas de turnos en días puente (gráficos y tablas)
- Integración de datos de puente en pestaña Revisión
- Corrección de fórmula de objetivo de puentes (basada en shifts individuales)
- Detección automática de períodos puente (Thu/Fri/Mon/Tue + festivos)
- Tolerancia estricta para puentes: ±0.5 shifts
- Ancho de columna fijo (72px) en tablas de Estadísticas y Revisión
- Indicadores (L-D) en todos los selectores de fecha
- Configuración de locale español (es_ES.utf8) para calendarios
- Tooltips de ayuda en calendarios explicando formato Lunes-Domingo
- Limpieza de interfaz: eliminada información redundante de puentes
- Mejoras de usabilidad y consistencia visual en toda la aplicación

v2.2 (Enero 2026):
- NUEVO: Tab "Revisión" para análisis de calendarios de guardias
- Integración de funcionalidad sched-anal
- Parsing de calendarios con formato: línea de días + N filas de trabajadores
- Cálculo de estadísticas detalladas por trabajador:
  * Guardias totales, Viernes, Sábado, Domingo
  * Guardias en Fin de Semana (incluyendo Festivos y PreFestivos)
  * Rosell: Guardias en última posición + Porcentaje
  * Desglose por mes
- Detección automática de guardias consecutivas
- Conversión automática de festivos/prefestivos a categorías especiales
- PreFestivo (día anterior a festivo) cuenta como Viernes solo para Lun-Jue
- Exportación de análisis a PDF y Excel
- Mapeo automático de nombres compuestos (ej: "LUIS H")
- Configuración flexible de guardias por día (shifts_per_day)

v2.1 (Enero 2026):
- Interfaz mejorada de Gestión de Médicos
- Funcionalidad de edición de trabajadores con carga automática de datos
- Campo reactivo "Guardias/mes" que aparece/desaparece dinámicamente
- Modo automático vs manual para cálculo de guardias
- Mejor manejo de session_state en Streamlit
- Validación mejorada de formularios
- Soporte para períodos personalizados por trabajador
- Gestión completa de incompatibilidades (global o individual)

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
