# 🔍 Guía de Uso - Pestaña "Revisión"

## Descripción General

La pestaña **"Revisión"** permite cargar y analizar archivos de horarios de guardias en múltiples formatos (PDF, Excel, CSV) para generar reportes estadísticos detallados.

## Funcionalidades Principales

### 1. 📂 Carga de Archivos
- Soporta formatos: **PDF, Excel (.xlsx, .xls), CSV**
- Extracción automática de texto del calendario
- Preview del contenido extraído antes del análisis

### 2. ⚙️ Configuración
- **Fecha inicial**: Especifique cuándo comienza el horario cargado
- **Festivos**: Se cargan automáticamente desde la configuración del sidebar (sección "Período de Reparto")
- **Mapeo de nombres**: Convierte nombres abreviados a nombres completos (opcional)
  - Formato: `ABREV=Nombre Completo` (uno por línea)
  - Ejemplo: `MAR=MARÍA`, `JOSE=JOSÉ`, `SANT=SANTI`

### 3. 🔍 Análisis Automático
- Calcula estadísticas por trabajador:
  - Total de guardias asignadas
  - Guardias en fin de semana (Viernes, Sábado, Domingo)
  - Porcentaje de guardias en fin de semana
  - Desglose mensual automático
- Detecta alertas: guardias consecutivas del mismo trabajador

### 4. 📊 Visualizaciones Interactivas
- **Gráfico de barras**: Total de guardias por trabajador
- **Gráfico de barras**: % de guardias en fin de semana
- **Gráfico de pastel**: Distribución Viernes/Sábado/Domingo
- **Gráfico horizontal**: Top 10 trabajadores con más guardias

### 5. 📥 Exportación de Reportes
- **CSV**: Exporta tabla de estadísticas completa
- **PDF**: Genera reporte profesional con resumen y tabla detallada
- **Excel**: Exporta con múltiples hojas (Estadísticas + Alertas)

## Flujo de Uso Paso a Paso

### Paso 1: Preparar Festivos (Una sola vez)
1. Ir al **Sidebar** → **"Período de Reparto"**
2. En la sección **"🎉 Festivos"**, editar las fechas festivas en formato DD-MM-YYYY
3. Los festivos se aplicarán automáticamente a todos los análisis

### Paso 2: Cargar Archivo
1. En la pestaña **"🔍 Revisión"**
2. Hacer clic en **"Seleccione archivo"**
3. Elegir un archivo PDF, Excel o CSV con el horario
4. El archivo se procesará automáticamente

### Paso 3: Configurar Análisis (Opcional)
1. **Fecha inicial**: Establecer la fecha donde comienza el calendario
2. **Mapeo de nombres**: Si los nombres están abreviados, agregar equivalencias (opcional)
3. Los festivos ya están cargados del sistema

### Paso 4: Ejecutar Análisis
1. Hacer clic en **"🚀 Analizar Horario"**
2. Esperar a que termine el análisis
3. Se mostrarán resultados automáticamente

### Paso 5: Revisar Resultados
- **Resumen**: Métricas clave (Total trabajadores, Guardias, % F.S.)
- **Tabla**: Estadísticas completas por trabajador
- **Alertas**: Guardias consecutivas detectadas
- **Gráficos**: Visualizaciones interactivas

### Paso 6: Exportar Reportes
- **CSV**: Para procesamiento en hojas de cálculo
- **PDF**: Para presentación profesional
- **Excel**: Para análisis adicional

## Formato de Entrada Esperado

### Formato de Calendario (Texto)
```
22 23 24 25 26 27 28           (Números de días - 7 columnas = 1 semana)
MANUEL MAR SANTI LOLA ELENA... (Médico 1 para cada día: Lun-Dom)
ELENA JOSE LUIS H LUIS R JUAN... (Médico 2 para siguiente semana Lun-Dom)
LAURA JAVIER ANA RUTH CARLOS... (Médico 3 para siguiente semana Lun-Dom)

29 30 31
MANUEL ELENA LAURA
JOSE MAR JAVIER
```

**Estructura**:
- Cada línea representa una semana completa (7 días: Lun-Dom)
- Exactamente 7 nombres por línea
- Primera línea OPCIONAL: números de los días
- Los números se detectan automáticamente y se saltan

**Nombres Compuestos** (detección automática):
- Si un nombre es una **sola letra** después de una palabra completa, se combinan automáticamente
- ✅ `LUIS H LUIS R CARLOS` → Detecta: "LUIS H", "LUIS R", "CARLOS"
- ✅ `MAR MANUEL SANTI` → Detecta: "MAR", "MANUEL", "SANTI"
- Los nombres como "Mar", "Luis H", "Luis R" son nombres COMPLETOS
- NO hay abreviaturas automáticas: "Mar" ≠ "María"

### Formato de Mapeo de Nombres
```
MAR=MARÍA
JOSE=JOSÉ
REQUE=LUIS REQUENA
SANT=SANTI
```

## Definiciones

| Término | Descripción |
|---------|------------|
| **Total Guardias** | Número total de guardias asignadas a cada trabajador |
| **Viernes/Sábado/Domingo** | Guardias en cada día específico de fin de semana |
| **Total FS** | Total de guardias en fin de semana (Vie+Sab+Dom) |
| **% FS** | Porcentaje de guardias que caen en fin de semana |
| **Consecutivas** | Número de veces que el trabajador tiene guardias en días consecutivos |
| **Mes: [Mes]** | Guardias distribuidas por cada mes del período |

## Alertas

### ⚠️ Guardias Consecutivas
Se genera una alerta cuando un trabajador tiene guardias en dos días consecutivos. 
Esto es importante para:
- Descanso adecuado
- Prevención de sobrecarga
- Cumplimiento de convenios laborales

## Ejemplos de Uso

### Ejemplo 1: Validar Distribución Equitativa
1. Cargar el horario generado
2. Revisar tabla de estadísticas
3. Comparar columnas de "Total" para verificar que sea similar entre trabajadores
4. Exportar a CSV para análisis detallado

### Ejemplo 2: Auditoría de Cumplimiento
1. Cargar el horario
2. Revisar alertas de guardias consecutivas
3. Generar PDF de reporte para archivos
4. Documentar cualquier violación de políticas

### Ejemplo 3: Análisis de Fin de Semana
1. Cargar el horario
2. Revisar gráfico de "% Fin de Semana"
3. Verificar que la distribución sea equilibrada
4. Identificar si algún trabajador tiene más guardias en fin de semana

## Troubleshooting

### Problema: "Error procesando archivo"
**Solución**: Verificar que el archivo esté en formato correcto y no esté dañado

### Problema: No se detectan trabajadores
**Solución**: Asegurar que los nombres estén separados por espacios y cada línea sea un trabajador

### Problema: Fechas incorrectas en análisis
**Solución**: Verificar la "Fecha inicial" en la configuración coincida con el calendario

### Problema: Nombres no se expanden correctamente
**Solución**: Revisar el mapeo de nombres - debe tener formato exacto: `ABREV=Completo`

## Contacto y Soporte

Para reportar errores o sugerencias, contacte al equipo de desarrollo.

---

**Versión**: 1.0  
**Última actualización**: Enero 2026
