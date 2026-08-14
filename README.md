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
- Configuración de turnos por día y gap mínimo entre guardias
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
- Control de descansos mínimos
- Restricciones de patrón semanal 7/14 días
- Balance de fines de semana
- Balance de turnos en días puente

### Análisis y exportación

- Exportación de calendarios a JSON, CSV y PDF
- Análisis de horarios externos en PDF, Excel y CSV
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
  application/      # Casos de uso y flujo de generación
  domain/           # Modelos de dominio y estado del calendario
  infrastructure/   # Integraciones y motores opcionales
  app_streamlit.py  # Interfaz principal
  run_app.py        # Entrada CLI: `saldo27`
tests/
  e2e/              # Pruebas end-to-end con Playwright
docs/               # Documentación complementaria
packaging/          # Empaquetado Windows / PyInstaller
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

## Estado del proyecto

Versión del paquete en `pyproject.toml`: `2.5.0`.
