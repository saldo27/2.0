# Sistema de Generación de Horarios - Interfaz Streamlit

**Versión actual: 2.5 (Febrero 2026)**

## 🚀 Inicio Rápido

### Ejecutar la aplicación

```bash
streamlit run app_streamlit.py
```

La aplicación se abrirá automáticamente en tu navegador en `http://localhost:8501`

### En GitHub Codespaces

La aplicación detectará automáticamente el puerto y te dará una URL para acceder:
```bash
streamlit run app_streamlit.py --server.port 8501
```

## 📋 Funcionalidades

### 1. **Gestión de Trabajadores** 👥
- ✅ Agregar/editar trabajadores con formulario interactivo
- ✅ Configurar turnos objetivo, porcentaje laboral
- ✅ Definir incompatibilidades entre trabajadores
- ✅ Asignar días obligatorios
- ✅ Importar/exportar desde JSON
- ✅ Vista de lista con todos los trabajadores

### 2. **Generación de Horarios** 📅
- ✅ Seleccionar mes y año
- ✅ Configurar parámetros (tolerancia, turnos por día, días entre turnos)
- ✅ Generación con indicador de progreso
- ✅ Visualización del calendario en tabla interactiva
- ✅ Descarga de calendario en CSV
- ✅ Descarga de PDFs generados

### 3. **Estadísticas** 📊
- ✅ Métricas de cobertura total
- ✅ Tabla de asignaciones por trabajador
- ✅ Comparación objetivo vs asignado (gráfico)
- ✅ Visualización de desviaciones (gráfico)
- ✅ Código de colores según tolerancia
- ✅ **NUEVO v2.5**: Estadísticas de turnos en puente
- ✅ **NUEVO v2.5**: Gráficos de asignación y desviación de puentes
- ✅ **NUEVO v2.5**: Tabla resumen con objetivo vs asignado de puentes
- ✅ **NUEVO v2.5**: Columnas de ancho fijo (72px) para mejor legibilidad

### 4. **Verificación de Restricciones** ⚠️
- ✅ Verificación de incompatibilidades
- ✅ Verificación del patrón 7/14 días
- ✅ Verificación de turnos obligatorios
- ✅ Contador de violaciones
- ✅ Detalles expandibles de cada violación
- ✅ Recomendaciones automáticas

### 5. **Dual-Mode Scheduler** 🔀 (NEW)
- ✅ Strict initial distribution mode
- ✅ Relaxed iterative optimization mode
- ✅ Configurable initial attempts (5-60)
- ✅ Automatic mode switching
- ✅ Quality metrics tracking

### 6. **Real-Time Editing** ⚡ (NEW)
- ✅ Interactive worker assignment
- ✅ Undo/Redo functionality
- ✅ Live validation feedback
- ✅ Change history tracking
- ✅ Manual shift adjustments

### 7. **Predictive Analytics** 🔮 (NEW)
- ✅ AI-powered demand forecasting
- ✅ Optimization recommendations
- ✅ Key insights and warnings
- ✅ Historical trend analysis
- ✅ Priority-based suggestions

## 🎯 Ventajas sobre Kivy

| Característica | Kivy | Streamlit |
|---------------|------|-----------|
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

## 📁 Archivos

- **`app_streamlit.py`**: Aplicación principal Streamlit
- **`trabajadores_ejemplo.json`**: Ejemplo de configuración de trabajadores
- **`main.py`**: Aplicación Kivy original (legacy)

## 🔧 Configuración

### Parámetros Ajustables (Sidebar)

1. **Mes/Año**: Selecciona el período a generar
2. **Tolerancia**: Porcentaje permitido de desviación (5-20%)
3. **Turnos por día**: Número de puestos a cubrir (1-10)
4. **Días mínimos entre turnos**: Gap de descanso (0-7 días)

### Formato JSON de Trabajadores

```json
[
  {
    "id": "TRAB001",
    "target_shifts": 12,
    "work_percentage": 1.0,
    "is_incompatible": false,
    "incompatible_with": ["TRAB002"],
    "mandatory_dates": ["01-12-2024", "15-12-2024"]
  }
]
```

## 🎨 Interfaz

### Tabs Principales

1. **👥 Gestión de Trabajadores**
   - Formulario para agregar/editar
   - Carga/descarga de JSON
   - Lista de trabajadores configurados

2. **📅 Calendario Generado**
   - Métricas de cobertura
   - Tabla del calendario completo
   - Descarga de CSV y PDFs

3. **📊 Estadísticas**
   - Métricas generales
   - Tabla de asignaciones
   - Gráficos comparativos
   - Gráfico de desviaciones

4. **⚠️ Verificación de Restricciones**
   - Resumen de violaciones
   - Detalles por tipo de restricción
   - Recomendaciones

5. **🔮 Predictive Analytics** (NEW)
   - Key insights and warnings
   - Demand forecasting charts
   - Optimization recommendations
   - Historical analysis

## 🐛 Restricciones Verificadas

- ✅ **Turnos Obligatorios**: Protegidos durante toda la generación
- ✅ **Incompatibilidades**: Trabajadores incompatibles no en mismo día
- ✅ **Patrón 7/14 Días**: Mismo día de semana a 7 o 14 días
- ✅ **Gap entre Turnos**: Días mínimos de descanso
- ✅ **Balance de Fines de Semana**: Distribución proporcional
- ✅ **Balance de Puentes**: Distribución equitativa de turnos en días puente
- ✅ **Tolerancia**: Desviación máxima respecto al objetivo

## 🆕 New Features (v2.5) - Febrero 2026

### Estadísticas de Turnos en Puente 🌉
- **Visualización completa**: Tres gráficos dedicados en pestaña Estadísticas
  - Gráfico comparativo de turnos asignados en puente por trabajador
  - Gráfico de desviaciones respecto al objetivo de puentes  
  - Tabla resumen con asignado, objetivo y desviación
- **Integración en Revisión**: Columnas "Puente" y "% Puente" en tabla de estadísticas
- **Cálculo preciso**: Objetivo basado en shifts individuales (no períodos)
  - Fórmula: `(total_puente_shifts / total_FTE) × worker_FTE`
- **Tolerancia estricta**: ±0.5 shifts (más estricta que fin de semana)
- **Detección automática**: Identifica puentes (Thu/Fri/Mon/Tue adyacentes a festivos)
- **Código de colores**: Verde (dentro de tolerancia) / Rojo (fuera de tolerancia)

### Mejoras de Interfaz de Usuario 🎨
- **Columnas de ancho fijo**: 72px (≈9 caracteres) para tablas consistentes
- **Indicadores de calendario**: Etiquetas "(L-D)" en todos los selectores de fecha
- **Configuración de locale**: Sistema configurado para español (es_ES.utf8)
- **Ayuda contextual**: Tooltips explicando formato Lunes-Domingo en calendarios
- **Limpieza de UI**: Removida información redundante de detección de puentes

## 🆕 New Features (v2.1)

### Dual-Mode Scheduler
- **Strict Initial Distribution**: 90-95% coverage with all constraints
- **Relaxed Optimization**: 98-100% coverage with controlled relaxation
- **Configurable Attempts**: 5-60 initial attempts for best quality
- **Smart Relaxation**: 
  - Target: +10% max (never increases)
  - Gap: -1 reduction only if worker needs ≥3 shifts
  - **Pattern 7/14: NEVER relaxed** - immovable hard constraint
  - **Never relaxes**: Mandatory, Incompatibilities, Days Off, Pattern 7/14

### Real-Time Editing
- **Interactive Assignment**: Click to assign workers to shifts
- **Undo/Redo**: Full change history with rollback
- **Live Validation**: Instant constraint checking
- **Change Tracking**: Audit trail of all modifications

### Predictive Analytics
- **Demand Forecasting**: 30-day demand predictions
- **AI Recommendations**: Priority-based optimization suggestions
- **Smart Insights**: Automatic coverage and balance analysis
- **Historical Trends**: Performance tracking over time

## 💡 Consejos de Uso

1. **Primer uso**: Carga `trabajadores_ejemplo.json` para probar
2. **Dual-Mode**: Enable for better quality (90-95% → 98-100% coverage)
3. **Real-Time**: Enable to manually adjust schedules with undo/redo
4. **Predictive**: Enable for AI-powered insights and recommendations
5. **Generación**: Puede tomar 2-5 minutos dependiendo de la complejidad
6. **Violaciones**: Si aparecen muchas, ajusta parámetros o trabajadores
7. **PDFs**: Se generan automáticamente durante la generación
8. **Estadísticas**: Usa las gráficas para identificar trabajadores sobrecargados

## 🚀 Próximos Pasos

- [ ] Historial de generaciones
- [ ] Comparación entre meses
- [ ] Edición manual de turnos en calendario
- [ ] Exportación a diferentes formatos (Excel, iCal)
- [ ] Notificaciones por email
- [ ] API REST para integración

## 📝 Notas

- La aplicación guarda el estado en `st.session_state`
- Los cambios en trabajadores requieren regenerar el horario
- Los PDFs se guardan en el directorio actual
- Los logs se guardan en `logs/scheduler.log`
