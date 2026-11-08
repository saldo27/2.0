# Saldo27

Sistema de generación y análisis de horarios de guardias para personal sanitario, construido con Streamlit.

## Resumen

Saldo27 permite:

- generar calendarios mensuales optimizados
- respetar restricciones de descanso, incompatibilidades y días obligatorios/libres
- equilibrar carga, fines de semana y turnos en días puente
- analizar calendarios existentes en PDF, Excel o CSV
- exportar resultados y métricas

## Características principales

### Generación de horarios

- Generación automática de calendarios mensuales
- Configuración de turnos por día y gap mínimo entre guardias, con intervalos de fechas que usan otro mínimo
- Soporte para calendario previo para restricciones entre períodos
- Optimización con validación de restricciones y métricas de calidad

### Gestión de personal

- Alta y edición de trabajadores
- Objetivos de turnos por trabajador
- Porcentaje de jornada
- Incompatibilidades entre trabajadores
- Días obligatorios y días no disponibles

### Validación y balance

- Verificación de incompatibilidades
- Control de descansos mínimos, global o por intervalo de fechas (si las dos guardias piden mínimos distintos, se exige el mayor); los períodos de gap de la UI llegan íntegros al motor de generación
- Restricciones de patrón semanal 7/14 días
- Límite de Rosell (último puesto) consecutivos: reparación preferente por swap el mismo día y, si no, desasignación con relleno del hueco
- Balance de fines de semana (protegido en el Ajuste Final frente a intercambios laborable↔finde)
- Balance de turnos en días puente (objetivos de cadencia coherentes con `_raw_target`)
- Objetivo mensual fijo garantizado para trabajadores con número fijo de guardias/mes, incluso tras la corrección final de restricciones
- Prorrateo automático del objetivo mensual (manual y automático) según días disponibles por `work_periods` y `days_off` (vacaciones/permisos)
- Aviso de posible conflicto cuando un día obligatorio (`mandatory_days`) cae dentro de un período de vacaciones o fuera del período de trabajo configurado
- Score de calidad con zonas sin penalización (carga ±10% y fines de semana ±15%, ambas con un máximo de ±1 guardia; puentes ±0.5; rotación ±20%) y agregación media + peor caso (25%), **neutral ante desbalances estructurales** de config (`no_last_post`, `only_last_post`, cliques de incompatibilidad, cadencia, mandatory, huecos no rellenables); las violaciones causadas por mandatory/cadencia no penalizan; gaps por `gap_periods`
- **Score del Reparto** (mismo desglose) en Revisión, Estadísticas (calendario generado; snapshot pre-FA si hubo Ajuste Final) y Ajuste Final (post-FA); en Revisión el score se alinea con el de generación

### Análisis y exportación

- Exportación de calendarios a JSON, CSV y PDF
- Análisis de horarios externos en PDF, Excel y CSV (Revisión: score/Verificación alineados con generación; aliases OCR; sync de gaps del sidebar)
- Estadísticas por trabajador y globales
- Visualizaciones y reportes para revisión

### Licenciamiento

- Modo DEMO con límites de uso
- Activación de licencia completa desde la aplicación

## Inicio rápido

### Requisitos

- Python 3.10 o superior
- `uv` para gestionar dependencias y ejecutar el proyecto

### Instalación

```bash
uv sync
```

### Ejecutar la aplicación

```bash
uv run saldo27
```

Alternativa:

```bash
uv run streamlit run src/saldo27/app_streamlit.py
```

La aplicación se sirve por defecto en `http://localhost:8501`.

## Flujo básico de uso

1. Cargar o crear la plantilla de trabajadores.
2. Configurar mes, año y parámetros de generación.
3. Opcionalmente cargar un calendario previo en JSON.
4. Generar el calendario.
5. Revisar métricas, restricciones y estadísticas.
6. Exportar el resultado en el formato necesario.

## Estructura del proyecto

```text
src/saldo27/
  application/           # Casos de uso y flujo de generación
  domain/                # Modelos de dominio y estado del calendario
  infrastructure/        # Integraciones y motores opcionales
  app_streamlit.py       # Interfaz principal
  run_app.py             # Entrada CLI: `saldo27`
  structural_score.py    # Baselines de scoring conscientes de la config
  optimization_metrics.py
  adaptive_iterations.py
  final_adjustment_engine.py
tests/
  e2e/                   # Pruebas end-to-end con Playwright (CI en ubuntu-24.04)
docs/                    # Documentación complementaria
packaging/               # Empaquetado Windows / PyInstaller
```

## Desarrollo

### Comandos útiles

```bash
# Instalar dependencias
uv sync

# Ejecutar la app
uv run saldo27

# Lint
uv run ruff check src/ tests/

# Formato
uv run ruff format src/ tests/

# Tipado
uv run ty check

# Dependencias declaradas
uv run deptry src/
```

### Tests

```bash
# Unit tests
uv run pytest tests/ -m "not e2e"

# E2E
uv run pytest tests/e2e/ -m e2e

# Suite completa
uv run pytest
```

## Documentación adicional

- `docs/README_STREAMLIT.md` — descripción funcional ampliada de la interfaz
- `docs/README.txt` — documentación orientada a distribución de escritorio
- `docs/ejecutar.md` — notas de ejecución

## Notas importantes

- Los imports en `src/saldo27/` deben ser absolutos.
- El proyecto usa rutas relativas al directorio de trabajo para archivos generados por el usuario.
- OR-Tools es opcional a nivel de arquitectura, pero está declarado como dependencia del proyecto.
- El sistema admite continuidad entre períodos mediante importación de un JSON exportado previamente.
- El score mide calidad del motor **dado** el condicionamiento del reparto: no castiga techos impuestos por incompatibilidad + `no_last_post`, spillover de Rosell hacia quien sí puede, ni huecos que nadie puede cubrir; tampoco penaliza violaciones producidas por días mandatory/cadencia.
- En Revisión, el score y la Verificación usan los mismos objetivos y `gap_periods` que la generación (preferencia live / sidebar sincronizado).

## Cambios recientes (v3.6)

- **Score estructural**: `structural_score.py` alimenta `OptimizationMetrics` y `AdaptiveIterationManager.overall_quality` con baselines factibles (cliques de incompat, Rosell estructural, fill solo de huecos rellenables).
- **Revisión = score de generación**: mismos objetivos/penalizaciones; mandatory/cadencia no penalizan gap ni incompat dual-mandatory; gaps por `gap_periods` (sync sidebar/sesión).
- **Revisión PDF+plantilla**: Balance/FS en calendarios incompletos; clique incompat + exenciones mandatory; aliases OCR día-conscientes; sin falsos “mandatory no asignado”.
- **Score del Reparto** también en **Estadísticas** (pre-FA si hubo FA) y **Ajuste Final** (post-FA).
- **Rosell consecutivos**: la reparación ya no deja puestos vacíos sin intentar swap/relleno.
- **Ajuste Final / balance WE**: intercambios laborable↔finde y optimizadores de carga no empeoran el balance de fines de semana; swap de puesto el mismo día sin error de sincronización de tracking.
- **Cadencia y puentes**: el objetivo de puentes de trabajadores con cadencia usa el objetivo de turnos real (`_raw_target`), no `target_shifts=0`.
- **`gap_periods`**: los intervalos de mínimo distintos configurados en la UI se pasan al `Scheduler` en la generación y al score/Verificación.
- **CI e2e**: job Playwright fijado a `ubuntu-24.04` por compatibilidad de browsers.

## Estado del proyecto

Versión de la aplicación (Streamlit): `3.6`.
Versión del paquete en `pyproject.toml`: `2.5.0`.
