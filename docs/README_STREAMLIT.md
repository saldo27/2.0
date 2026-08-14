# Sistema de Generación de Horarios - Interfaz Streamlit

**Versión actual: 3.2 (Agosto 2026)**

## 🚀 Inicio Rápido

### Ejecutar la aplicación

```bash
# Con el comando instalado (recomendado)
uv run saldo27

# O directamente con Streamlit
uv run streamlit run src/saldo27/app_streamlit.py
```

La aplicación se abrirá automáticamente en tu navegador en `http://localhost:8501`

### En GitHub Codespaces

```bash
uv run streamlit run src/saldo27/app_streamlit.py --server.port 8501
```

## 📋 Funcionalidades

### 1. **Gestión de Médicos** 👥
- ✅ Agregar/editar trabajadores con formulario interactivo
- ✅ Configurar turnos objetivo, porcentaje laboral
- ✅ Definir incompatibilidades entre trabajadores
- ✅ Asignar días obligatorios y días fuera
- ✅ Importar/exportar desde JSON
- ✅ Vista de lista con todos los trabajadores

### 2. **Generación de Horarios** 📅
- ✅ Seleccionar mes y año
- ✅ Configurar parámetros (tolerancia, turnos por día, días entre turnos)
- ✅ Carga de horario previo para restricciones entre períodos
- ✅ Generación con indicador de progreso en tiempo real
- ✅ Visualización del calendario en tabla interactiva
- ✅ Descarga de calendario en CSV y JSON
- ✅ Descarga de PDFs generados

### 3. **Estadísticas** 📊
- ✅ Métricas de cobertura total
- ✅ Tabla de asignaciones por trabajador
- ✅ Comparación objetivo vs asignado (gráfico)
- ✅ Visualización de desviaciones (gráfico)
- ✅ Código de colores según tolerancia
- ✅ Estadísticas de turnos en puente
- ✅ Gráficos de asignación y desviación de puentes
- ✅ Tabla resumen con objetivo vs asignado de puentes
- ✅ Columnas de ancho fijo (72px) para mejor legibilidad

### 4. **Verificación de Restricciones** ⚠️
- ✅ Verificación de incompatibilidades
- ✅ Verificación del patrón 7/14 días
- ✅ Verificación de turnos obligatorios
- ✅ Contador de violaciones
- ✅ Detalles expandibles de cada violación
- ✅ Recomendaciones automáticas

### 5. **Predictive Analytics** 🔮
- ✅ Predicción de demanda a 30 días
- ✅ Recomendaciones de optimización basadas en IA
- ✅ Insights automáticos de cobertura y balance
- ✅ Análisis de tendencias históricas
- ✅ Sugerencias priorizadas

### 6. **Revisión de Calendario** 🔍
- ✅ Carga de archivos PDF, Excel o CSV de horarios existentes
- ✅ Análisis automático del archivo cargado
- ✅ Detección de alertas y anomalías
- ✅ Exportación del análisis a Excel

## 🔀 Dual-Mode Scheduler

- **Modo estricto**: Distribución inicial al 90-95% con todas las restricciones
- **Modo relajado**: Optimización iterativa hasta 98-100% de cobertura
- Intentos iniciales configurables (5-60)
- Cambio automático de modo cuando el estricto no alcanza el umbral
- Seguimiento de métricas de calidad en cada intento

## ⚡ Real-Time Editing

- Asignación interactiva de médicos a turnos directamente en el calendario
- Funcionalidad Undo/Redo completa con historial de cambios
- Validación en vivo de restricciones al editar
- Registro de auditoría de todas las modificaciones

## 🔑 Sistema de Licencias

- **Modo DEMO**: Hasta 10 generaciones, máximo 15 trabajadores, máximo 62 días
- **Licencia completa**: Sin limitaciones; clave con formato `GP-XXXX-XXXX-XXXX-XXXX`
- Activación desde el sidebar lateral de la aplicación
- La clave se almacena cifrada en `~/.guardiasapp/license.dat`

## 🔁 Continuidad Entre Períodos (Prior Schedule)

- Carga un JSON exportado del mes anterior para respetar restricciones de frontera
- Extrae: últimas fechas trabajadas, conteos de fines de semana/puentes y totales
- Garantiza que el gap mínimo entre turnos se respete al cruzar el cambio de mes

## 🎯 Ventajas sobre Kivy

| Característica | Kivy | Streamlit |
|----------------|------|-----------|
| Funciona sin GUI | ❌ | ✅ |
| Interfaz web moderna | ❌ | ✅ |
| Gráficos interactivos | ⚠️ | ✅ |
| Desarrollo rápido | ⚠️ | ✅ |
| Funciona en Codespaces | ❌ | ✅ |
| Responsive | ⚠️ | ✅ |
| Descarga de archivos | ⚠️ | ✅ |
| Auto-recarga en cambios | ❌ | ✅ |
| Real-time editing | ❌ | ✅ |
| Predictive analytics | ❌ | ✅ |
| Dual-mode optimization | ⚠️ | ✅ |
| Revisión de archivos | ❌ | ✅ |
| Sistema de licencias | ❌ | ✅ |

## 📁 Archivos y Estructura

```
src/saldo27/
  app_streamlit.py          # Aplicación principal Streamlit
  scheduler.py              # Orquestador principal
  scheduler_core.py         # Bucle de optimización
  schedule_builder.py       # Construcción inicial del horario
  bridge_manager.py         # Detección y balance de puentes
  change_tracker.py         # Undo/redo y auditoría
  real_time_engine.py       # Motor de edición en tiempo real
  license_manager.py        # Gestión de licencias DEMO/completa
  prior_schedule_handler.py # Carga de horario previo (cross-period)
  schedule_analyzer.py      # Analizador de archivos PDF/Excel/CSV
  final_adjustment_engine.py# Ajuste final de desviaciones
  predictive_analytics.py   # Insights y recomendaciones IA
  demand_forecaster.py      # Predicción de demanda
  application/              # Casos de uso y pipeline de generación
  domain/                   # Modelos de dominio (ScheduleState)
  infrastructure/           # Motores opcionales (OR-Tools)
trabajadores_ejemplo.json   # Ejemplo de configuración de trabajadores
```

## 🔧 Configuración

### Parámetros Ajustables (Sidebar)

1. **Mes/Año**: Selecciona el período a generar
2. **Tolerancia**: Porcentaje permitido de desviación (5-20%)
3. **Turnos por día**: Número de puestos a cubrir (1-10)
4. **Días mínimos entre turnos**: Gap de descanso (0-7 días)
5. **Modo Dual-Scheduler**: Activa optimización en dos fases
6. **Edición en tiempo real**: Activa edición manual con undo/redo
7. **Predictive Analytics**: Activa análisis predictivo
8. **Horario previo**: Carga JSON del mes anterior para restricciones entre períodos

### Formato JSON de Trabajadores

```json
[
  {
    "id": "TRAB001",
    "target_shifts": 12,
    "work_percentage": 1.0,
    "is_incompatible": false,
    "incompatible_with": ["TRAB002"],
    "mandatory_dates": ["01-12-2024", "15-12-2024"],
    "days_off": ["20-12-2024"]
  }
]
```

## 🎨 Tabs Principales

1. **👥 Gestión de Médicos** — Formulario de alta/edición, carga/descarga JSON, lista de trabajadores
2. **📅 Calendario Generado** — Métricas, tabla del calendario, descarga CSV/PDF/JSON
3. **📊 Estadísticas** — Asignaciones, gráficos comparativos, balance de puentes
4. **⚠️ Verificación de Restricciones** — Resumen de violaciones, detalles y recomendaciones
5. **🔮 Predictive Analytics** — Insights, gráficos de demanda, recomendaciones
6. **🔍 Revisión** — Carga y análisis de archivos PDF/Excel/CSV externos

## 🐛 Restricciones Verificadas

- ✅ **Turnos Obligatorios**: Protegidos durante toda la generación
- ✅ **Incompatibilidades**: Trabajadores incompatibles no en mismo día
- ✅ **Patrón 7/14 Días**: Mismo día de semana a 7 o 14 días — nunca se relaja
- ✅ **Gap entre Turnos**: Días mínimos de descanso (relajable solo si ≥3 turnos pendientes)
- ✅ **Balance de Fines de Semana**: Distribución proporcional
- ✅ **Balance de Puentes**: Distribución equitativa de turnos en días puente (tolerancia ±0.5)
- ✅ **Tolerancia**: Desviación máxima respecto al objetivo (+10% max, nunca se incrementa el target)
- ✅ **Continuidad entre períodos**: Gap respetado en el cambio de mes (vía horario previo)

## 🆕 Historial de Versiones

### v3.2 (Agosto 2026)
- Tab **🔍 Revisión**: análisis de archivos PDF, Excel y CSV de horarios existentes
- Sistema de **licencias DEMO/completa** con activación por clave
- **Continuidad entre períodos**: carga de horario previo para restricciones de frontera
- Motor de **ajuste final** (`final_adjustment_engine.py`) post-generación
- **OR-Tools** como motor de optimización opcional (via `infrastructure/optional_engines.py`)
- Capa de **aplicación** (`application/`), **dominio** (`domain/`) e **infraestructura** (`infrastructure/`)
- Calendarios con **Lunes como primer día** (JavaScript MutationObserver)
- Indicadores `(L-D)` en selectores de fecha

### v2.5 (Febrero 2026)
- Estadísticas completas de turnos en puente (3 gráficos dedicados)
- Columnas de ancho fijo (72px) en tablas del calendario
- Configuración de locale `es_ES.utf8` para calendarios

### v2.1
- Dual-Mode Scheduler (estricto + relajado)
- Edición en tiempo real con undo/redo
- Predictive Analytics con forecasting de demanda

## 💡 Consejos de Uso

1. **Primer uso**: Carga `trabajadores_ejemplo.json` para probar
2. **Licencia**: Activa tu clave en el sidebar para eliminar limitaciones DEMO
3. **Dual-Mode**: Activa para mayor calidad (90-95% → 98-100% de cobertura)
4. **Real-Time**: Activa para ajustar manualmente turnos con undo/redo
5. **Prior schedule**: Carga el JSON del mes anterior para respetar gaps en el cambio de mes
6. **Revisión**: Usa la tab 🔍 para analizar horarios existentes en PDF o Excel
7. **Generación**: Puede tomar 2-5 minutos dependiendo de la complejidad
8. **PDFs**: Se generan automáticamente durante la generación
9. **Estadísticas**: Usa los gráficos de puentes para verificar el balance equitativo

## 🚀 Próximos Pasos

- [ ] Historial de generaciones entre meses
- [ ] Comparación entre meses
- [ ] Exportación a iCal
- [ ] Notificaciones por email
- [ ] API REST para integración
