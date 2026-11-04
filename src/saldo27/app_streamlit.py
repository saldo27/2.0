"""
Sistema de Generación de Calendario de guardias - Interfaz Streamlit
Reemplazo moderno de la interfaz Kivy con funcionalidad web

Versión: 3.6 (Noviembre 2026)
"""

# IMPORTANTE: Configurar locale ANTES de importar streamlit
# Esto asegura que los calendarios muestren Lunes-Domingo (formato español/ISO 8601)
import os
from typing import Any, cast

os.environ["LANG"] = "es_ES.utf8"
os.environ["LC_ALL"] = "es_ES.utf8"
os.environ["LC_TIME"] = "es_ES.utf8"

import copy
import io
import json
import logging
import traceback
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from saldo27.application.generation_flow import (
    GenerationUICallbacks,
    build_generation_summary_log,
    collect_constraint_violations,
    execute_generation_workflow,
    prepare_generation_workflow,
)
from saldo27.application.use_cases import (
    GenerateScheduleRequest,
    run_simulation,
)
from saldo27.license_manager import license_manager
from saldo27.optimization_metrics import (
    OptimizationMetrics,
    apply_scoring_objectives,
    restore_scoring_objectives,
)
from saldo27.scheduler import Scheduler
from saldo27.scheduler_config import SchedulerConfig, setup_logging
from saldo27.utilities import (
    format_gap_periods_text,
    normalize_gap_periods,
    parse_gap_periods_text,
    worker_flag_enabled,
)

# Suprimir debug output de librerías externas
logging.getLogger("pdfplumber").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Constante de versión
APP_VERSION = "3.6"

# ===== IMPORTS FORZADOS PARA PYINSTALLER =====
# Estos módulos se importan dinámicamente en otros archivos,
# pero PyInstaller necesita verlos explícitamente aquí
from saldo27.schedule_analyzer import CalendarFileProcessor, PDFReportGenerator, ScheduleAnalyzer

print("✓ Módulos críticos importados explícitamente")
# ==============================================

# Configuración de la página DEBE ser lo primero
st.set_page_config(
    page_title="Aplicación para Distribución de Guardias",
    page_icon="📅",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Configurar locale para calendarios (Lunes como primer día de la semana)
import locale

try:
    locale.setlocale(locale.LC_TIME, "es_ES.utf8")
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")
    except locale.Error:
        try:
            locale.setlocale(locale.LC_TIME, "es_ES")
        except locale.Error:
            pass  # Mantener locale por defecto si no está disponible


# Configurar logging (solo una vez por sesión)
@st.cache_resource
def initialize_logging():
    """Initialize logging only once per session to avoid infinite rerun loops"""
    setup_logging()
    return True


initialize_logging()

# Custom CSS y JavaScript para mejor apariencia y configuración de calendarios
st.markdown(
    """
<style>
    .main > div {
        padding-top: 2rem;
    }
    .stButton>button {
        width: 100%;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
    }
    .warning-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
    }
</style>

""",
    unsafe_allow_html=True,
)

# Inyectar JavaScript para calendarios Lunes-Domingo
# st.markdown() ignora <script>; components.html() ejecuta JS real vía window.parent
components.html(
    """
<script>
(function() {
    var MAX_RETRIES = 60;
    var retries = 0;

    function shiftSundayToEnd(table) {
        // Evitar procesar la misma tabla dos veces
        if (table.dataset.mondayFixed) return;
        table.dataset.mondayFixed = '1';

        // Cabecera (th)
        var headRow = table.querySelector('thead tr');
        if (headRow && headRow.cells.length > 0) {
            var sun = headRow.cells[0];
            headRow.appendChild(sun);          // mueve (no copia)
        }

        // Filas de días (td)
        table.querySelectorAll('tbody tr').forEach(function(row) {
            if (row.cells.length > 0) {
                row.appendChild(row.cells[0]); // mueve la celda Domingo al final
            }
        });
    }

    function fixAllCalendars(root) {
        (root || window.parent.document).querySelectorAll(
            'table[role="grid"]'
        ).forEach(shiftSundayToEnd);
    }

    function waitForParent() {
        try {
            var parentDoc = window.parent.document;
            if (!parentDoc) { retry(); return; }

            // Corregir calendarios ya abiertos
            fixAllCalendars(parentDoc);

            // Observar futuras aperturas de calendario
            var observer = new MutationObserver(function(mutations) {
                mutations.forEach(function(m) {
                    m.addedNodes.forEach(function(node) {
                        if (node.nodeType === 1) {
                            // Si el nodo añadido contiene tablas de calendario
                            if (node.querySelector) {
                                node.querySelectorAll('table[role="grid"]').forEach(shiftSundayToEnd);
                            }
                            if (node.matches && node.matches('table[role="grid"]')) {
                                shiftSundayToEnd(node);
                            }
                        }
                    });
                });
            });
            observer.observe(parentDoc.body, { childList: true, subtree: true });

        } catch(e) { retry(); }
    }

    function retry() {
        if (retries++ < MAX_RETRIES) setTimeout(waitForParent, 150);
    }

    waitForParent();
})();
</script>
""",
    height=0,
)

# Inicializar session state
if "workers_data" not in st.session_state:
    st.session_state.workers_data = []
if "schedule" not in st.session_state:
    st.session_state.schedule = None
if "scheduler" not in st.session_state:
    st.session_state.scheduler = None
if "generation_log" not in st.session_state:
    st.session_state.generation_log = []
if "config" not in st.session_state:
    st.session_state.config = SchedulerConfig.get_default_config()
if "fa_pending_result" not in st.session_state:
    st.session_state.fa_pending_result = None

if "schedule_pre_fa" not in st.session_state:
    st.session_state.schedule_pre_fa = None

if "license_checked" not in st.session_state:
    st.session_state.license_checked = True
    can_use, message, remaining = license_manager.can_use()
    st.session_state.can_use = can_use
    st.session_state.license_message = message
    st.session_state.uses_remaining = remaining
    st.session_state.limitations = license_manager.get_limitations()

# Predictive analytics
if "predictive_enabled" not in st.session_state:
    st.session_state.predictive_enabled = False
if "demand_forecasts" not in st.session_state:
    st.session_state.demand_forecasts = None
if "optimization_recommendations" not in st.session_state:
    st.session_state.optimization_recommendations = []
if "analytics_insights" not in st.session_state:
    st.session_state.analytics_insights = []

# Schedule Analysis (Revisión)
if "revision_stats" not in st.session_state:
    st.session_state.revision_stats = None
if "revision_display_scheduler" not in st.session_state:
    st.session_state.revision_display_scheduler = None
if "revision_score" not in st.session_state:
    st.session_state.revision_score = None
if "revision_alerts" not in st.session_state:
    st.session_state.revision_alerts = None
if "revision_analyzer" not in st.session_state:
    st.session_state.revision_analyzer = None
if "revision_calendar_text" not in st.session_state:
    st.session_state.revision_calendar_text = ""
if "revision_name_mapping" not in st.session_state:
    st.session_state.revision_name_mapping = {}
if "sidebar_holidays" not in st.session_state:
    st.session_state.sidebar_holidays = []
if "sidebar_holidays_input" not in st.session_state:
    st.session_state.sidebar_holidays_input = ""

# File upload tracking to prevent infinite rerun loops
if "file_upload_counter" not in st.session_state:
    st.session_state.file_upload_counter = 0
if "schedule_upload_counter" not in st.session_state:
    st.session_state.schedule_upload_counter = 0
if "prior_schedule_loaded" not in st.session_state:
    st.session_state.prior_schedule_loaded = False
if "prior_schedule_data" not in st.session_state:
    st.session_state.prior_schedule_data = None  # summary dict from load_prior_schedule_data()
if "prior_schedule_raw" not in st.session_state:
    st.session_state.prior_schedule_raw = None  # raw bytes when scheduler not yet created


# Funciones auxiliares
def load_workers_from_file(uploaded_file):
    """Cargar Médicos desde archivo JSON con validación y compatibilidad"""
    try:
        data = json.load(uploaded_file)

        if not isinstance(data, list):
            return False, "❌ El archivo JSON debe contener una lista de médicos"

        validated_data = []
        count = 0

        for item in data:
            if not isinstance(item, dict) or "id" not in item:
                continue

            # Construir objeto trabajador asegurando todos los campos
            worker = {
                "id": str(item["id"]),
                "target_shifts": int(item.get("target_shifts", 0)),
                "work_percentage": float(item.get("work_percentage", 100)),
                "is_incompatible": bool(item.get("is_incompatible", False)),
                "incompatible_with": item.get("incompatible_with", []),
                "mandatory_days": str(item.get("mandatory_days", "")),
                "days_off": str(item.get("days_off", "")),
                "work_periods": str(item.get("work_periods", "")),
                "auto_calculate_shifts": bool(item.get("auto_calculate_shifts", True)),
                "no_last_post": bool(item.get("no_last_post", False)),
                "only_last_post": bool(item.get("only_last_post", False)),
                "has_cadence": bool(item.get("has_cadence", False)),
                "cadence_days": int(item.get("cadence_days", 0) or 0),
                "cadence_start_date": str(item.get("cadence_start_date", "") or ""),
            }

            # Compatibilidad con formato antiguo (mandatory_dates lista)
            if "mandatory_dates" in item and isinstance(item["mandatory_dates"], list):
                if not worker["mandatory_days"]:
                    worker["mandatory_days"] = "; ".join(item["mandatory_dates"])

            # Compatibilidad con campos antiguos custom_start/end
            if not worker["work_periods"]:
                start = item.get("custom_start_date")
                end = item.get("custom_end_date")
                if start and end:
                    worker["work_periods"] = f"{start} - {end}"

            validated_data.append(worker)
            count += 1

        if count == 0:
            return False, "⚠️ No se encontraron médicos válidos en el archivo"

        st.session_state.workers_data = validated_data
        return True, f"✅ {count} médicos importados correctamente"

    except json.JSONDecodeError:
        return False, "❌ Error: El archivo no es un JSON válido"
    except Exception as e:
        return False, f"❌ Error al procesar datos: {e!s}"


def save_workers_to_file():
    """Guardar Médicos en JSON"""
    return json.dumps(st.session_state.workers_data, indent=2, ensure_ascii=False)


def load_schedule_from_json(uploaded_file):
    """Cargar Calendario y Configuración desde archivo JSON"""
    try:
        data = json.load(uploaded_file)

        # 0. Check format type
        if isinstance(data, list):
            return (
                False,
                "❌ Este archivo parece contener solo lista de médicos. Use el importador de 'Trabajadores' más abajo.",
            )

        # 1. Validar estructura básica (Relaxed)
        if "workers_data" not in data:
            keys_found = list(data.keys())
            if "worker_metrics" in keys_found:
                return (
                    False,
                    f"❌ Error: Este parece ser un archivo de Análisis Histórico (Analytics), no un respaldo de calendario completo. Claves encontradas: {keys_found}",
                )
            return (
                False,
                f"❌ El archivo no contiene datos de trabajadores ('workers_data'). Claves encontradas: {keys_found}",
            )

        # 2. Preparar Workers Data (no asignar a session_state hasta validar todo)
        new_workers_data = data["workers_data"]

        # 3. Cargar Configuración
        config = st.session_state.config.copy()

        # Parse Fechas (Robust)
        try:
            # Try to find dates in different places
            s_date_val = data.get("start_date")
            e_date_val = data.get("end_date")

            # Fallback to schedule_period if available
            if not s_date_val and "schedule_period" in data:
                s_date_val = data["schedule_period"].get("start_date")
                e_date_val = data["schedule_period"].get("end_date")

            if s_date_val and e_date_val:
                if isinstance(s_date_val, str):
                    start_date = datetime.fromisoformat(s_date_val)
                else:
                    start_date = s_date_val

                if isinstance(e_date_val, str):
                    end_date = datetime.fromisoformat(e_date_val)
                else:
                    end_date = e_date_val

                # Ensure datetime
                if not isinstance(start_date, datetime):
                    start_date = datetime.combine(start_date, datetime.min.time())
                if not isinstance(end_date, datetime):
                    end_date = datetime.combine(end_date, datetime.min.time())

                config["start_date"] = start_date
                config["end_date"] = end_date
            else:
                # If no dates found, keep existing or warn?
                # We'll rely on existing config if file doesn't have dates
                pass

        except Exception as e:
            # Don't fail the whole load just for dates, use existing if needed
            logging.warning(f"Date parsing warning: {e}")

        # Cargar otros parámetros si existen
        if "num_shifts" in data:
            config["num_shifts"] = data["num_shifts"]

        # Holidays
        if "holidays" in data:
            holidays = []
            for h in data["holidays"]:
                try:
                    if isinstance(h, str):
                        holidays.append(datetime.fromisoformat(h))
                    else:
                        holidays.append(h)
                except (ValueError, TypeError):
                    pass
            config["holidays"] = holidays

        if "variable_shifts" in data:
            config["variable_shifts"] = data["variable_shifts"]

        if "gap_between_shifts" in data:
            try:
                config["gap_between_shifts"] = int(data["gap_between_shifts"])
            except (TypeError, ValueError):
                pass

        if "gap_periods" in data:
            config["gap_periods"] = normalize_gap_periods(data["gap_periods"])
            st.session_state.gap_periods_text = format_gap_periods_text(config["gap_periods"])

        # Commit validated data to session_state
        st.session_state.workers_data = new_workers_data
        st.session_state.config = config

        # 4. Reconstruir Scheduler y Schedule
        if data.get("schedule"):
            try:
                # Reconstruir objeto schedule {datetime: [workers]}
                schedule = {}
                for date_str, workers in data["schedule"].items():
                    try:
                        dt = datetime.fromisoformat(date_str)
                        schedule[dt] = workers
                    except (ValueError, TypeError):
                        pass

                if schedule:
                    # Crear scheduler dummy con esta config
                    scheduler = Scheduler(config)
                    scheduler.schedule = schedule
                    scheduler.workers_data = new_workers_data
                    scheduler._repair_data_synchronization()

                    st.session_state.scheduler = scheduler
                    st.session_state.schedule = schedule
                    # A freshly imported schedule invalidates any Ajuste Final
                    # result computed against the previous one.
                    st.session_state.fa_pending_result = None
                    st.session_state.schedule_pre_fa = copy.deepcopy(schedule)

                    return True, "✅ Calendario y configuración importados correctamente"
            except Exception as e:
                logging.error(f"Schedule reconstruction error: {e}")
                return (
                    True,
                    "⚠️ Configuración cargada, pero hubo error reconstruyendo el calendario exacto. Genere nuevamente.",
                )

        return True, "✅ Configuración importada (Recuerde generar el reparto nuevamente)"

    except json.JSONDecodeError:
        return False, "❌ Error: El archivo no es un JSON válido"
    except Exception as e:
        return False, f"❌ Error al procesar datos: {e!s}"


def generate_schedule_internal(start_date, end_date, holidays, variable_shifts):
    """Generar el horario internamente"""
    try:
        request = GenerateScheduleRequest(
            start_date=start_date,
            end_date=end_date,
            holidays=holidays,
            variable_shifts=variable_shifts,
            workers_data=st.session_state.workers_data,
            config=st.session_state.config,
            prior_schedule_raw=st.session_state.get("prior_schedule_raw"),
        )

        preparation = prepare_generation_workflow(request, limitations=st.session_state.limitations)
        for user_message in preparation.user_messages:
            getattr(st, user_message.level)(user_message.text)
        if not preparation.success or preparation.workflow is None:
            return False, preparation.message

        workflow = preparation.workflow
        scheduler = workflow.scheduler
        st.session_state.scheduler = scheduler

        if preparation.prior_schedule_summary is not None:
            st.session_state.prior_schedule_data = preparation.prior_schedule_summary

        status_text = st.empty()
        cancel_placeholder = st.empty()
        st.session_state.generation_cancelled = False
        st.session_state._cancel_requested = False

        if cancel_placeholder.button("⛔ Cancelar generación", key="cancel_generation"):
            st.session_state._cancel_requested = True

        def _show_status(message: str, level: str) -> None:
            getattr(status_text, level)(message)

        def _show_log(messages: list[str]) -> None:
            log_placeholder = st.session_state.get("_sidebar_log_placeholder")
            if log_placeholder and messages:
                log_placeholder.code("\n".join(messages), language=None)

        callbacks = GenerationUICallbacks(
            show_status=_show_status,
            show_log=_show_log,
            is_cancel_requested=lambda: bool(st.session_state.get("_cancel_requested")),
            clear_cancel_request=lambda: st.session_state.__setitem__("_cancel_requested", False),
        )
        try:
            result = execute_generation_workflow(workflow, callbacks)
            st.session_state.generation_cancelled = result.cancelled
        finally:
            cancel_placeholder.empty()
        scheduler = workflow.scheduler

        log_placeholder = st.session_state.get("_sidebar_log_placeholder")
        if log_placeholder and result.final_log_text and result.final_log_kind == "code":
            log_placeholder.code(result.final_log_text, language=None)
            st.session_state._sidebar_log_content = ("code", result.final_log_text)
        elif log_placeholder and result.final_log_text and result.final_log_kind == "warning":
            log_placeholder.warning(result.final_log_text)
            st.session_state._sidebar_log_content = ("warning", result.final_log_text)

        getattr(status_text, result.final_status_level)(result.final_status_text)

        if result.success:
            if st.session_state.limitations["mode"] == "DEMO":
                uses = license_manager.increment_usage()
                st.session_state.uses_remaining = license_manager.DEMO_MAX_USES - uses

                if st.session_state.uses_remaining <= 3:
                    st.warning(f"⚠️ **Atención**: Solo quedan **{st.session_state.uses_remaining}** usos en modo DEMO")

                st.info(f"✅ Generación #{uses} completada.  Quedan {st.session_state.uses_remaining} usos.")

            st.session_state.schedule = scheduler.schedule
            # A freshly generated schedule invalidates any Ajuste Final
            # result computed against the previous one.
            st.session_state.fa_pending_result = None
            st.session_state.schedule_pre_fa = copy.deepcopy(scheduler.schedule)
            return True, result.message

        return False, result.message

    except Exception as e:
        error_msg = f"Error en generación: {e!s}"
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        return False, f"❌ Error: {e!s}"


def get_schedule_dataframe():
    """Convertir calendario a DataFrame para visualización"""
    if st.session_state.schedule is None:
        return None

    schedule = st.session_state.schedule

    # Crear DataFrame
    dates = sorted(schedule.keys())
    data = []

    for day in dates:
        workers = schedule[day]
        row = {
            "Fecha": day.strftime("%d-%m-%Y"),
            "Día": ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"][day.weekday()],
        }
        for i, worker in enumerate(workers):
            row[f"Puesto {i + 1}"] = worker if worker else "-"
        data.append(row)

    df = pd.DataFrame(data)
    # For days with fewer posts than the maximum (variable_shifts), pandas creates NaN
    # in the extra columns.  Replace those with a distinct marker '---' so they are
    # visually distinguishable from real empty slots ('-') and are never counted as
    # real guard slots in any coverage calculation.
    return df.fillna("---")


def compute_schedule_changes(
    schedule_before: dict[datetime, list],
    schedule_after: dict[datetime, list],
) -> list[dict[str, Any]]:
    """
    Compara dos snapshots de ``schedule`` ({fecha: [worker_por_puesto]}) y
    devuelve la lista de slots (fecha, puesto) cuyo trabajador asignado
    cambió, con el valor anterior y el nuevo.

    Usada para mostrar al usuario exactamente qué cambió tras ejecutar el
    ⚖️ Ajuste Final, independientemente de en qué sub-fase (balance de
    turnos, fines de semana, puentes u OR-Tools) se produjo el cambio.
    """
    changes: list[dict[str, Any]] = []
    all_dates = set(schedule_before.keys()) | set(schedule_after.keys())
    for date_val in sorted(all_dates):
        posts_before = schedule_before.get(date_val, [])
        posts_after = schedule_after.get(date_val, [])
        max_posts = max(len(posts_before), len(posts_after))
        for post in range(max_posts):
            old_worker = posts_before[post] if post < len(posts_before) else None
            new_worker = posts_after[post] if post < len(posts_after) else None
            if old_worker != new_worker:
                changes.append(
                    {
                        "Fecha": date_val.strftime("%d-%m-%Y"),
                        "Puesto": post + 1,
                        "Trabajador (Antes)": old_worker or "-",
                        "Trabajador (Después)": new_worker or "-",
                    }
                )
    return changes


def _norm_worker_key(value: object) -> str:
    """Normalize worker id/name for matching (case/space insensitive)."""
    return " ".join(str(value).strip().casefold().split())


def _plantilla_workers_by_id(
    workers_config: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Index plantilla workers by normalized id and name for resilient matching."""
    if workers_config is None:
        workers_config = collect_rosell_flag_sources()
    indexed: dict[str, dict[str, Any]] = {}
    for worker in workers_config:
        if not isinstance(worker, dict) or worker.get("id") is None:
            continue
        indexed[_norm_worker_key(worker["id"])] = worker
        name = worker.get("name")
        if name:
            indexed.setdefault(_norm_worker_key(name), worker)
    return indexed


def collect_rosell_flag_sources(
    extra_sources: list[list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    """Merge worker dicts that may carry ``no_last_post`` / ``only_last_post``.

    Sources (later sources only promote True flags onto earlier ones):
    1. ``st.session_state.workers_data`` (plantilla UI)
    2. Live generation ``scheduler.workers_data`` when present
    3. Any ``extra_sources`` (e.g. snapshot taken before swapping the active scheduler)
    """
    merged: dict[str, dict[str, Any]] = {}

    def _ingest(workers: list[dict[str, Any]] | None) -> None:
        for worker in workers or []:
            if not isinstance(worker, dict) or worker.get("id") is None:
                continue
            key = _norm_worker_key(worker["id"])
            current = merged.get(key)
            if current is None:
                merged[key] = dict(worker)
                continue
            # Promote Rosell flags; keep the richest known record.
            if worker_flag_enabled(worker, "no_last_post"):
                current["no_last_post"] = True
                current["only_last_post"] = False
            elif worker_flag_enabled(worker, "only_last_post") and not worker_flag_enabled(current, "no_last_post"):
                current["only_last_post"] = True

    _ingest(list(st.session_state.get("workers_data") or []))
    live = st.session_state.get("scheduler")
    if live is not None and getattr(live, "workers_data", None):
        _ingest(list(live.workers_data))
    for source in extra_sources or []:
        _ingest(list(source))
    return list(merged.values())


def sync_rosell_flags_from_plantilla(scheduler: Scheduler, workers_config: list[dict[str, Any]] | None = None) -> int:
    """Promote ``no_last_post`` / ``only_last_post`` from the plantilla onto ``scheduler``.

    Only turns flags **on** from the plantilla. Never clears a True already on
    the scheduler (a stale/incomplete plantilla must not wipe generation flags).
    Returns how many workers were changed.
    """
    plantilla = _plantilla_workers_by_id(workers_config)
    if not plantilla:
        return 0

    updated = 0
    for worker in scheduler.workers_data:
        cfg = plantilla.get(_norm_worker_key(worker.get("id", "")))
        if cfg is None:
            name = worker.get("name")
            if name:
                cfg = plantilla.get(_norm_worker_key(name))
        if cfg is None:
            continue
        before_no = worker_flag_enabled(worker, "no_last_post")
        before_only = worker_flag_enabled(worker, "only_last_post")
        no_last = before_no or worker_flag_enabled(cfg, "no_last_post")
        only_last = (not no_last) and (before_only or worker_flag_enabled(cfg, "only_last_post"))
        if before_no != no_last or before_only != only_last:
            updated += 1
        worker["no_last_post"] = no_last
        worker["only_last_post"] = only_last
    return updated


def _rosell_objective_for_stats(
    scheduler: Scheduler,
    worker_data: dict[str, Any] | None,
    *,
    shift_target: int | float,
    plantilla_cfg: dict[str, Any] | None,
) -> tuple[int, bool, bool]:
    """Return ``(obj_rosell, no_last, only_last)`` for the statistics table.

    ``no_last_post`` → 0. ``only_last_post`` → 100% of the shift objective shown
    in the same row. Flags are the OR of scheduler worker + plantilla.
    """
    no_last = worker_flag_enabled(worker_data, "no_last_post") or worker_flag_enabled(plantilla_cfg, "no_last_post")
    only_last = (not no_last) and (
        worker_flag_enabled(worker_data, "only_last_post") or worker_flag_enabled(plantilla_cfg, "only_last_post")
    )
    if worker_data is not None:
        worker_data["no_last_post"] = no_last
        worker_data["only_last_post"] = only_last
    if no_last:
        return 0, True, False
    if only_last:
        try:
            return round(float(shift_target)), False, True
        except (TypeError, ValueError):
            return 0, False, True
    if worker_data is None:
        return 0, False, False
    return round(scheduler.get_last_post_objective(worker_data)), False, False


def get_worker_statistics(workers_config: list[dict[str, Any]] | None = None):
    """Obtener estadísticas de asignaciones por médico usando el motor central.

    ``workers_config`` is the authoritative source of Rosell flags when the active
    scheduler is a Revision display object (plantilla / generation snapshot).
    """
    if st.session_state.scheduler is None:
        return None

    # Usar el calculador de estadísticas centralizado
    scheduler = st.session_state.scheduler
    flag_source = workers_config if workers_config is not None else collect_rosell_flag_sources()
    # Promote missing Rosell flags from plantilla; never wipe flags already on.
    sync_rosell_flags_from_plantilla(scheduler, flag_source)
    plantilla = _plantilla_workers_by_id(flag_source)
    core_stats = scheduler.stats.calculate_statistics()

    # Rosell = último puesto de cada día. Con variable_shifts el índice del
    # último puesto es len(shifts)-1 de ese día, no un índice fijo num_shifts-1.
    # Pre-count covered Rosell assignments per worker from the schedule
    rosell_by_worker: dict[str, int] = {}
    for _date, shifts in scheduler.schedule.items():
        if not shifts:
            continue
        last_worker = shifts[-1]
        if last_worker is not None:
            rosell_by_worker[last_worker] = rosell_by_worker.get(last_worker, 0) + 1

    stats = []
    for worker_id, data in core_stats["workers"].items():
        # Obtener worker data para acceder a _raw_target y _mandatory_count
        worker_data = next((w for w in scheduler.workers_data if w["id"] == worker_id), None)

        # target_shifts es el ajustado (después de restar mandatory)
        # Queremos mostrar el objetivo TOTAL (raw_target) que incluye mandatory
        if worker_data and "_raw_target" in worker_data:
            target = worker_data["_raw_target"]  # Objetivo total incluyendo mandatory
        else:
            # Fallback: si no existe _raw_target, usar target_shifts
            target = data["target_shifts"]

        current = data["total_shifts"]
        deviation = current - target
        deviation_pct = (deviation / target * 100) if target > 0 else 0

        # Weekend statistics (includes Fri/Sat/Sun + holidays + pre-holidays)
        weekend_shifts = data.get("weekend_shifts", 0)
        # Target de weekend proporcional: total_target * ratio_weekend
        weekend_target = round(scheduler.get_weekend_objective(worker_data or {"_raw_target": target}))
        weekend_deviation = weekend_shifts - weekend_target
        weekend_deviation_pct = (weekend_deviation / weekend_target * 100) if weekend_target > 0 else 0

        # Rosell: no_last → 0; only_last → 100% del Objetivo de turnos de la fila.
        plantilla_cfg = plantilla.get(_norm_worker_key(worker_id))
        if plantilla_cfg is None and worker_data and worker_data.get("name"):
            plantilla_cfg = plantilla.get(_norm_worker_key(worker_data["name"]))
        rosell_count = rosell_by_worker.get(worker_id, 0)
        rosell_target, is_no_last, _is_only_last = _rosell_objective_for_stats(
            scheduler, worker_data, shift_target=target, plantilla_cfg=plantilla_cfg
        )
        rosell_deviation = rosell_count - rosell_target
        if is_no_last and rosell_count > 0:
            rosell_deviation_pct = float("inf")
        elif rosell_target > 0:
            rosell_deviation_pct = rosell_deviation / rosell_target * 100
        elif rosell_count > 0:
            rosell_deviation_pct = float("inf")
        else:
            rosell_deviation_pct = 0.0

        # Format the Rosell deviation percentage
        if rosell_deviation_pct == float("inf"):
            rosell_dev_pct_str = "⚠️ VIOL"
        else:
            rosell_dev_pct_str = f"{rosell_deviation_pct:+.1f}%"

        stats.append(
            {
                "Médico": worker_id,
                "Objetivo": target,
                "Asignados": current,
                "Desviación": deviation,
                "Desv. %": f"{deviation_pct:+.1f}%",
                "Obj. Weekend": weekend_target,
                "Weekend": weekend_shifts,
                "Desv. Wknd": weekend_deviation,
                "Desv. Wknd %": f"{weekend_deviation_pct:+.1f}%",
                "Obj. Rosell": rosell_target,
                "Rosell": rosell_count,
                "Desv. Rosell": rosell_deviation,
                "Desv. Rosell %": rosell_dev_pct_str,
            }
        )

    return pd.DataFrame(stats)


def build_summary_pdf_stats_data(scheduler: Scheduler) -> dict[str, Any]:
    """Construye el payload de estadísticas usado por el PDF global."""
    holidays_set = set(scheduler.holidays) if scheduler.holidays else set()

    stats_data: dict[str, Any] = {
        "period_start": scheduler.start_date,
        "period_end": scheduler.end_date,
        "workers": {},
        "worker_shifts": {},
    }

    for worker in scheduler.workers_data:
        w_id = worker["id"]
        assignments = [d for d, w in scheduler.schedule.items() if w_id in w]

        post_counts: dict[int, int] = {}
        weekday_counts: dict[int, int] = {}
        shift_list: list[dict[str, Any]] = []
        last_post_count = 0

        for assigned_date in assignments:
            day_shifts = scheduler.schedule.get(assigned_date, [])
            try:
                p_idx = day_shifts.index(w_id)
                post_counts[p_idx] = post_counts.get(p_idx, 0) + 1
                # Rosell = último puesto real del día (cubierto), no índice fijo
                if day_shifts and p_idx == len(day_shifts) - 1:
                    last_post_count += 1
            except (ValueError, KeyError):
                p_idx = -1

            wd = assigned_date.weekday()
            weekday_counts[wd] = weekday_counts.get(wd, 0) + 1

            is_holiday = assigned_date in holidays_set
            is_pre_holiday = (assigned_date + timedelta(days=1)) in holidays_set
            is_weekend_day = scheduler.date_utils.is_weekend_day(assigned_date, holidays_set)

            shift_list.append(
                {
                    "date": assigned_date,
                    "day": assigned_date.strftime("%A"),
                    "post": p_idx + 1 if p_idx >= 0 else 0,
                    "is_weekend": is_weekend_day or is_holiday or is_pre_holiday,
                    "is_holiday": is_holiday,
                    "is_pre_holiday": is_pre_holiday,
                }
            )

        weekends_count = sum(1 for d in assignments if scheduler.date_utils.is_weekend_day(d, holidays_set))
        holidays_count = sum(1 for d in assignments if (d in holidays_set or (d + timedelta(days=1)) in holidays_set))

        stats_data["workers"][w_id] = {
            "total": len(assignments),
            "weekends": weekends_count,
            "holidays": holidays_count,
            "last_post": last_post_count,
            "weekday_counts": weekday_counts,
            "post_counts": post_counts,
        }
        stats_data["worker_shifts"][w_id] = shift_list

    return stats_data


def refresh_generated_report_pdfs(scheduler: Scheduler, pdf_exporter_cls: type) -> tuple[list[str], list[str]]:
    """
    Regenera los PDFs ya existentes vinculados al calendario/estadísticas.

    Returns:
        tuple[list[str], list[str]]: (archivos regenerados, errores de regeneración).
    """
    config = {
        "schedule": scheduler.schedule,
        "workers_data": scheduler.workers_data,
        "num_shifts": scheduler.num_shifts,
        "holidays": scheduler.holidays,
    }
    exporter = pdf_exporter_cls(config)

    period_str = f"{scheduler.start_date.strftime('%Y%m%d')}_{scheduler.end_date.strftime('%Y%m%d')}"
    refreshed_files: list[str] = []
    errors: list[str] = []

    summary_default = Path(f"summary_global_{period_str}.pdf")
    summary_full_period = Path("summary_global_full_period.pdf")
    if summary_default.exists() or summary_full_period.exists():
        try:
            refreshed = exporter.export_summary_pdf(build_summary_pdf_stats_data(scheduler))
            if refreshed:
                refreshed_files.append(refreshed)
        except Exception as exc:
            errors.append(f"Resumen Ejecutivo: {exc}")

    calendar_default = Path(f"schedule_all_months_{period_str}.pdf")
    if calendar_default.exists():
        try:
            refreshed = exporter.export_all_months_calendar(filename=calendar_default.as_posix())
            if refreshed:
                refreshed_files.append(refreshed)
        except Exception as exc:
            errors.append(f"Calendario Visual Completo: {exc}")

    worker_stats = Path("worker_statistics.pdf")
    if worker_stats.exists():
        try:
            refreshed = exporter.export_worker_statistics(filename=worker_stats.as_posix())
            if refreshed:
                refreshed_files.append(refreshed)
        except Exception as exc:
            errors.append(f"Estadísticas y Desglose Detallado: {exc}")

    return refreshed_files, errors


def render_final_adjustment_result(result: dict[str, Any]) -> None:
    """
    Renderiza el resultado del ⚖️ Ajuste Final (FinalAdjustmentEngine).

    Se extrae en una función independiente porque el resultado se persiste en
    ``st.session_state.fa_pending_result`` y se renderiza en el siguiente
    rerun de Streamlit (ANTES de volver a pulsar el botón), evitando que el
    ``st.rerun()`` posterior a la ejecución del motor descarte los mensajes
    de éxito/estadísticas antes de que el usuario llegue a verlos.

    A diferencia de otros mensajes efímeros, este bloque incluye un botón
    interactivo (exportar PDF), por lo que NO se limpia tras un único
    render: permanece en session_state hasta que una nueva ejecución del
    Ajuste Final lo sobrescriba o se genere/importe un calendario nuevo
    (ver limpieza de ``fa_pending_result`` en esos puntos).
    """
    total_swaps = result["total_swaps"]
    fa_stats = result["fa_stats"]
    refreshed_pdfs = result["refreshed_pdfs"]
    pdf_refresh_errors = result["pdf_refresh_errors"]
    before_metrics = result["before_metrics"]
    after_metrics = result["after_metrics"]

    if total_swaps > 0:
        ortools_n = fa_stats.get("ortools_reassignments", 0)
        ortools_msg = f", {ortools_n} reasignación(es) OR-Tools" if ortools_n else ""
        st.success(
            f"✅ Ajuste completado: {fa_stats['shift_swaps']} swap(s) de turno, "
            f"{fa_stats['weekend_swaps']} swap(s) de fin-de-semana, "
            f"{fa_stats['bridge_swaps']} swap(s) de puente, "
            f"{fa_stats.get('weekend_last_post_swaps', 0)} swap(s) de último puesto en finde"
            f"{ortools_msg}."
        )
        if refreshed_pdfs:
            st.info(f"🔄 PDFs actualizados: {', '.join(refreshed_pdfs)}")
        if pdf_refresh_errors:
            for pdf_err in pdf_refresh_errors:
                st.warning(f"⚠️ No se pudo actualizar un PDF: {pdf_err}")
    else:
        st.info("ℹ️ El calendario ya estaba bien equilibrado. No se realizaron cambios.")

    weekend_errors = fa_stats.get("weekend_errors", 0)
    if weekend_errors:
        st.error(
            f"Desviación de fines de semana superior al ±15% en {weekend_errors} trabajador(es). "
            "Esa desviación queda penalizada como error."
        )

    # Show before/after comparison table
    rows = []
    for wid, bef in before_metrics.items():
        aft = after_metrics.get(wid, bef)
        rows.append(
            {
                "Médico": bef["name"],
                "Turnos Obj.": bef["shift_target"],
                "Turnos (Antes)": bef["shift_assigned"],
                "Turnos (Después)": aft["shift_assigned"],
                "Desv. Turnos": f"{aft['shift_deviation']:+d}",
                "Wknd Obj.": bef["weekend_target"],
                "Wknd (Antes)": bef["weekend_assigned"],
                "Wknd (Después)": aft["weekend_assigned"],
                "Desv. Wknd": f"{aft['weekend_deviation']:+d}",
                "Puente Obj.": bef["bridge_target"],
                "Puente (Antes)": bef["bridge_assigned"],
                "Puente (Después)": aft["bridge_assigned"],
                "Desv. Puente": f"{aft['bridge_deviation']:+d}",
            }
        )
    if rows:
        with st.expander("📊 Detalle por médico (antes → después del ajuste)", expanded=False):
            st.dataframe(pd.DataFrame(rows), hide_index=True)

    # Show the specific slot-level changes made (regardless of which sub-phase
    # produced them: shift/weekend/bridge balance or the OR-Tools refinement).
    changes = result.get("changes") or []
    if changes:
        with st.expander(f"🔄 Cambios realizados ({len(changes)})", expanded=True):
            st.dataframe(pd.DataFrame(changes), hide_index=True, width="stretch")

    # Show the updated calendar with an option to export it to PDF, so the
    # user doesn't have to scroll back up to the general "Calendario
    # Detallado" section to see the effect of the adjustment.
    st.markdown("##### 📅 Calendario actualizado (tras el ajuste)")
    _fa_df = get_schedule_dataframe()
    if _fa_df is not None:
        st.dataframe(_fa_df, width="stretch", height=400, hide_index=True)

        if st.button("📄 Exportar calendario actualizado (PDF)", key="btn_export_fa_calendar"):
            _fa_scheduler = st.session_state.scheduler
            if _fa_scheduler is None:
                st.warning("⚠️ No hay un calendario activo para exportar.")
            else:
                try:
                    from saldo27.pdf_exporter import PDFExporter

                    _fa_pdf_config = {
                        "schedule": _fa_scheduler.schedule,
                        "workers_data": _fa_scheduler.workers_data,
                        "num_shifts": _fa_scheduler.num_shifts,
                        "holidays": _fa_scheduler.holidays,
                    }
                    _fa_exporter = PDFExporter(_fa_pdf_config)
                    _fa_period_str = (
                        f"{_fa_scheduler.start_date.strftime('%Y%m%d')}_{_fa_scheduler.end_date.strftime('%Y%m%d')}"
                    )
                    _fa_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    _fa_filename = f"schedule_all_months_adjusted_{_fa_period_str}_{_fa_ts}.pdf"
                    _fa_generated = _fa_exporter.export_all_months_calendar(filename=_fa_filename)
                    if _fa_generated:
                        st.success(f"✅ PDF generado: {_fa_generated}")
                        st.rerun()
                    else:
                        st.error("No se pudo generar el archivo (nombre de archivo vacío)")
                except Exception as _fa_pdf_exc:
                    st.error(f"Error al generar PDF: {_fa_pdf_exc}")
                    logging.error("FinalAdjustmentEngine PDF export error", exc_info=True)
    else:
        st.info("ℹ️ No hay calendario disponible para mostrar.")


@contextmanager
def temporary_schedule_snapshot(schedule_snapshot: dict | None):
    """Temporarily point the live scheduler at ``schedule_snapshot`` for UI rendering.

    Used so the Estadísticas tab can show the pre-Ajuste-Final repartition while
    the active ``scheduler.schedule`` remains the post-adjustment one.
    """
    scheduler = st.session_state.scheduler
    if scheduler is None or schedule_snapshot is None:
        yield False
        return
    original_schedule = scheduler.schedule
    original_session_schedule = st.session_state.schedule
    try:
        scheduler.schedule = copy.deepcopy(schedule_snapshot)
        st.session_state.schedule = scheduler.schedule
        if hasattr(scheduler, "_synchronize_tracking_data"):
            scheduler._synchronize_tracking_data()
        yield True
    finally:
        scheduler.schedule = original_schedule
        st.session_state.schedule = original_session_schedule
        if hasattr(scheduler, "_synchronize_tracking_data"):
            scheduler._synchronize_tracking_data()


@contextmanager
def temporary_active_scheduler(scheduler: Scheduler | None):
    """Temporarily replace ``st.session_state.scheduler`` / ``schedule`` for rendering."""
    if scheduler is None:
        yield False
        return
    original_scheduler = st.session_state.scheduler
    original_session_schedule = st.session_state.schedule
    try:
        st.session_state.scheduler = scheduler
        st.session_state.schedule = scheduler.schedule
        yield True
    finally:
        st.session_state.scheduler = original_scheduler
        st.session_state.schedule = original_session_schedule


def build_revision_display_scheduler(analyzer, workers_config: list[dict[str, Any]] | None = None) -> Scheduler:
    """Build a display-only Scheduler from a parsed ``ScheduleAnalyzer`` calendar.

    Objectives (``_raw_target`` / ``target_shifts``) are set to each worker's
    actual assignment count so the shared Estadísticas panel can render the
    same columns/charts without inventing unreachable targets.

    Rosell flags (``no_last_post`` / ``only_last_post``) are copied from the
    configured plantilla when available, so Obj. Rosell stays 0 / 100%.
    """
    num_shifts = int(getattr(analyzer, "shifts_per_day", 4) or 4)
    schedule: dict[datetime, list] = {}
    assignment_counts: dict[str, int] = {}

    for day_data in getattr(analyzer, "calendar_data", []) or []:
        day = day_data.get("date")
        if day is None:
            continue
        if not isinstance(day, datetime):
            day = datetime.combine(day, datetime.min.time())
        slots: list = [None] * num_shifts
        positioned = day_data.get("workers_with_position") or []
        if positioned:
            for worker_name, pos in positioned:
                if worker_name and 0 <= int(pos) < num_shifts:
                    slots[int(pos)] = worker_name
        else:
            for idx, worker_name in enumerate(day_data.get("workers") or []):
                if worker_name and idx < num_shifts:
                    slots[idx] = worker_name
        schedule[day] = slots
        for worker_name in slots:
            if worker_name:
                assignment_counts[worker_name] = assignment_counts.get(worker_name, 0) + 1

    if not schedule:
        raise ValueError("El analizador no tiene días parseados para mostrar estadísticas.")

    if workers_config is None:
        workers_config = collect_rosell_flag_sources()
    configured_by_id = _plantilla_workers_by_id(workers_config)

    # Prefer the live generation scheduler for scoring objectives and gap settings —
    # those are what produced the "Score final" during reparto.
    live_scheduler = st.session_state.get("scheduler")
    live_by_id: dict[str, dict[str, Any]] = {}
    if live_scheduler is not None and getattr(live_scheduler, "workers_data", None):
        live_by_id = _plantilla_workers_by_id(list(live_scheduler.workers_data))

    # Short calendar names → plantilla names via Revisión name_mapping.
    name_mapping = dict(st.session_state.get("revision_name_mapping") or {})

    def _lookup_cfg(calendar_id: str, index: dict[str, dict[str, Any]]) -> dict[str, Any]:
        cfg = index.get(_norm_worker_key(calendar_id), {})
        if cfg:
            return cfg
        mapped = name_mapping.get(calendar_id) or name_mapping.get(str(calendar_id).upper())
        if mapped:
            return index.get(_norm_worker_key(mapped), {})
        return {}

    # Remap plantilla incompatible_with IDs → calendar worker keys (often names).
    plantilla_key_to_calendar: dict[str, str] = {}
    for calendar_id in assignment_counts:
        cfg = _lookup_cfg(calendar_id, configured_by_id) or _lookup_cfg(calendar_id, live_by_id)
        plantilla_key_to_calendar[_norm_worker_key(calendar_id)] = calendar_id
        for short, long in name_mapping.items():
            if _norm_worker_key(long) == _norm_worker_key(calendar_id) or (
                cfg
                and _norm_worker_key(long)
                in {_norm_worker_key(cfg.get("id", "")), _norm_worker_key(cfg.get("name", ""))}
            ):
                plantilla_key_to_calendar[_norm_worker_key(short)] = calendar_id
        if cfg:
            plantilla_key_to_calendar[_norm_worker_key(cfg.get("id", ""))] = calendar_id
            if cfg.get("name"):
                plantilla_key_to_calendar[_norm_worker_key(cfg["name"])] = calendar_id

    def _remap_incompatible_with(raw: list | str | None) -> list[str]:
        if isinstance(raw, str):
            items = [raw] if raw.strip() else []
        else:
            items = list(raw or [])
        remapped: list[str] = []
        for other in items:
            mapped = plantilla_key_to_calendar.get(_norm_worker_key(other))
            if mapped:
                remapped.append(mapped)
        return remapped

    def _scoring_targets(cfg: dict[str, Any], live_cfg: dict[str, Any], count: int) -> tuple[int, int]:
        """Objectives for OptimizationMetrics (not the display "objetivo = detectadas")."""
        source = live_cfg or cfg
        if not source:
            return count, count
        if bool(source.get("has_cadence")):
            raw = source.get("_raw_target")
            try:
                raw_i = int(raw) if raw is not None else count
            except (TypeError, ValueError):
                raw_i = count
            # Cadence length from a different period must not dwarf/underfill the PDF slice.
            if not live_cfg and count > 0 and raw_i > 0:
                ratio = count / raw_i
                if ratio < 0.5 or ratio > 1.5:
                    return 0, count
            return 0, raw_i
        try:
            target = int(source.get("target_shifts", 0) or 0)
        except (TypeError, ValueError):
            target = 0
        try:
            raw = source.get("_raw_target")
            raw_i = int(raw) if raw is not None else target
        except (TypeError, ValueError):
            raw_i = target
        # Plantilla auto workers often still have 0/0 before generation — keep counts.
        if not live_cfg and target == 0 and raw_i == 0:
            return count, count
        # Without a live generation scheduler, plantilla targets belong to the
        # configured generation period. A Revisión PDF often covers another
        # slice (e.g. Mar–Jun PDF + Nav–Jun plantilla with manual target=4 vs
        # 18 detected) — overlaying those objectives floors Balance/FS at 0.
        if not live_cfg and count > 0 and target > 0:
            ratio = count / target
            if ratio < 0.5 or ratio > 1.5:
                return count, count
        return target, raw_i

    workers_data = []
    for worker_id in sorted(assignment_counts):
        count = assignment_counts[worker_id]
        cfg = _lookup_cfg(worker_id, configured_by_id)
        live_cfg = _lookup_cfg(worker_id, live_by_id)
        if not live_cfg and cfg.get("id") is not None:
            live_cfg = live_by_id.get(_norm_worker_key(cfg["id"]), {})
        if not live_cfg and cfg.get("name"):
            live_cfg = live_by_id.get(_norm_worker_key(cfg["name"]), {})
        no_last = worker_flag_enabled(cfg, "no_last_post") or worker_flag_enabled(live_cfg, "no_last_post")
        only_last = (not no_last) and (
            worker_flag_enabled(cfg, "only_last_post") or worker_flag_enabled(live_cfg, "only_last_post")
        )
        score_target, score_raw = _scoring_targets(cfg, live_cfg, count)
        # Prefer live mandatory/cadence (post TargetCalculator / cadence expand).
        source_flags = live_cfg or cfg
        workers_data.append(
            {
                "id": worker_id,
                "name": str(cfg.get("name") or live_cfg.get("name") or worker_id),
                "target_shifts": count,
                "_raw_target": count,
                # Stashed for score overlay so Revisión matches reparto score.
                "_score_target_shifts": score_target,
                "_score_raw_target": score_raw,
                "work_percentage": float(source_flags.get("work_percentage", cfg.get("work_percentage", 100)) or 100),
                # Plantilla fields needed for Verificación de Restricciones / score exemptions
                "work_periods": str(source_flags.get("work_periods", cfg.get("work_periods", "")) or ""),
                "mandatory_days": str(source_flags.get("mandatory_days", cfg.get("mandatory_days", "")) or ""),
                "days_off": str(source_flags.get("days_off", cfg.get("days_off", "")) or ""),
                "incompatible_with": _remap_incompatible_with(
                    source_flags.get("incompatible_with", cfg.get("incompatible_with"))
                ),
                "is_incompatible": bool(source_flags.get("is_incompatible", cfg.get("is_incompatible", False))),
                "auto_calculate_shifts": bool(
                    source_flags.get("auto_calculate_shifts", cfg.get("auto_calculate_shifts", True))
                ),
                "no_last_post": no_last,
                "only_last_post": only_last,
                "has_cadence": bool(source_flags.get("has_cadence", cfg.get("has_cadence", False))),
                "cadence_days": int(source_flags.get("cadence_days", cfg.get("cadence_days", 0)) or 0),
                "cadence_start_date": str(
                    source_flags.get("cadence_start_date", cfg.get("cadence_start_date", "")) or ""
                ),
            }
        )

    holidays = []
    for holiday in getattr(analyzer, "holidays", []) or []:
        if isinstance(holiday, datetime):
            holidays.append(holiday)
        else:
            holidays.append(datetime.combine(holiday, datetime.min.time()))

    start_date = min(schedule)
    end_date = max(schedule)
    session_cfg = st.session_state.get("config") or {}
    # Prefer sidebar gap variable when set so Revisión/Verificación track the UI,
    # even if a prior generation left a different (or empty) list on the live scheduler.
    session_periods = normalize_gap_periods(session_cfg.get("gap_periods") or [])
    live_periods = list(getattr(live_scheduler, "gap_periods", None) or []) if live_scheduler is not None else []
    gap_periods = session_periods if session_periods else live_periods
    if session_cfg.get("gap_between_shifts") is not None:
        gap_between_shifts = int(session_cfg["gap_between_shifts"])
    elif live_scheduler is not None and getattr(live_scheduler, "gap_between_shifts", None) is not None:
        gap_between_shifts = int(live_scheduler.gap_between_shifts)
    else:
        gap_between_shifts = 3
    if live_scheduler is not None and getattr(live_scheduler, "max_consecutive_weekends", None) is not None:
        max_consecutive_weekends = int(live_scheduler.max_consecutive_weekends)
    else:
        max_consecutive_weekends = int(session_cfg.get("max_consecutive_weekends") or 3)

    display = Scheduler(
        {
            "start_date": start_date,
            "end_date": end_date,
            "num_shifts": num_shifts,
            "workers_data": workers_data,
            "holidays": holidays,
            "variable_shifts": list(
                getattr(live_scheduler, "variable_shifts", None) or session_cfg.get("variable_shifts") or []
            ),
            "gap_between_shifts": gap_between_shifts,
            "gap_periods": gap_periods,
            "max_consecutive_weekends": max_consecutive_weekends,
        }
    )
    # TargetCalculator overwrites targets on init; restore actual counts for display stats,
    # and re-stamp scoring objectives (cadence expand may have rewritten mandatory_days).
    for worker in display.workers_data:
        count = assignment_counts.get(worker["id"], 0)
        score_target = worker.get("_score_target_shifts", count)
        score_raw = worker.get("_score_raw_target", count)
        # Preserve plantilla/live mandatory_days if cadence expand wiped a pre-expanded list
        # that was already stamped from the live scheduler.
        preserved_mandatory = worker.get("mandatory_days", "")
        worker["target_shifts"] = count
        worker["_raw_target"] = count
        worker["_score_target_shifts"] = score_target
        worker["_score_raw_target"] = score_raw
        if preserved_mandatory:
            worker["mandatory_days"] = preserved_mandatory

    display.schedule = schedule
    display.schedule_builder = getattr(display, "schedule_builder", None)
    if display.schedule_builder is None:
        from saldo27.schedule_builder import ScheduleBuilder

        display.schedule_builder = ScheduleBuilder(display)
    display._synchronize_tracking_data()
    # Re-apply after Scheduler init in case ingestion/cadence paths touch worker dicts.
    sync_rosell_flags_from_plantilla(display, workers_config)
    return display


def render_assignment_statistics(
    *,
    title_suffix: str = "",
    caption: str | None = None,
    key_prefix: str = "stats",
    workers_config: list[dict[str, Any]] | None = None,
) -> None:
    """Render the assignment / weekend / bridge statistics panel for the current schedule view.

    ``key_prefix`` must be unique per call site: this panel is shown both in
    Estadísticas (pre-FA) and Ajuste Final (post-FA), and Streamlit requires
    distinct element keys for duplicated plotly charts / dataframes.

    ``workers_config`` carries Rosell flags when the active scheduler is a
    Revision display object swapped in via ``temporary_active_scheduler``.
    """
    header = "📊 Estadísticas de Asignación" + (f" — {title_suffix}" if title_suffix else "")
    st.header(header)
    if caption:
        st.caption(caption)
    _k = key_prefix

    if st.session_state.scheduler is None:
        st.info("ℹ️ No hay calendario generado. Use el botón '🚀 Generar Reparto' en la barra lateral.")
    else:
        # Estadísticas por trabajador
        stats_df = get_worker_statistics(workers_config=workers_config)

        if stats_df is not None:
            # Métricas generales - Totales
            st.subheader("📈 Resumen General")
            col1, col2, col3 = st.columns(3)

            # CORRECCIÓN: Total Objetivo son los slots totales a cubrir según la
            # configuración del scheduler (num_shifts × días del rango), no los
            # tamaños reales de las listas del dict (que pueden quedarse cortos).
            scheduler = st.session_state.scheduler
            total_target = sum(
                scheduler._get_shifts_for_date(d)
                for d in scheduler._get_date_range(scheduler.start_date, scheduler.end_date)
            )
            total_assigned = stats_df["Asignados"].sum()
            avg_deviation = stats_df["Desviación"].mean()

            with col1:
                st.metric("Total Objetivo", total_target)
            with col2:
                st.metric("Total Asignado", total_assigned, f"{total_assigned - total_target:+d}")
            with col3:
                st.metric("Desviación Promedio", f"{avg_deviation:+.1f}")

            # Métricas de Weekend
            st.subheader("🌙 Resumen Fines de semana")
            st.caption("*Incluye: Viernes, Sábado, Domingo, Festivos y Pre-festivos*")
            col4, col5, col6 = st.columns(3)

            # CORRECCIÓN: Objetivo Weekend debe ser el número real de slots de weekend a cubrir
            # Usa _get_shifts_for_date para respetar la configuración (no len(shifts) del dict)
            from datetime import timedelta

            holidays_set = set(scheduler.holidays) if scheduler.holidays else set()
            total_weekend_target = 0
            for d in scheduler._get_date_range(scheduler.start_date, scheduler.end_date):
                is_weekend = scheduler.date_utils.is_weekend_day(d, holidays_set)
                if is_weekend:
                    total_weekend_target += scheduler._get_shifts_for_date(d)

            total_weekend_assigned = stats_df["Weekend"].sum()
            avg_weekend_deviation = stats_df["Desv. Wknd"].mean()

            with col4:
                st.metric("Objetivo Fines de Semana", total_weekend_target)
            with col5:
                st.metric(
                    "Fines de semana Asignados",
                    total_weekend_assigned,
                    f"{total_weekend_assigned - total_weekend_target:+d}",
                )
            with col6:
                st.metric("Desv. Weekend Prom.", f"{avg_weekend_deviation:+.1f}")

            st.markdown("---")

            # Tabla de estadísticas
            st.subheader("📋 Estadísticas por Médico")
            st.caption("*Fin de semana: Viernes, Sábado, Domingo, Festivos y Pre-festivos*")

            # Colorear según desviación
            def color_deviation(val):
                if isinstance(val, str) and "%" in val:
                    pct = float(val.replace("%", "").replace("+", ""))
                    if abs(pct) <= 10:
                        return "background-color: #d4edda"
                    elif abs(pct) <= 15:
                        return "background-color: #fff3cd"
                    else:
                        return "background-color: #f8d7da"
                return ""

            styled_df = stats_df.style.map(color_deviation, subset=["Desv. %", "Desv. Wknd %", "Desv. Rosell %"])

            # Configurar ancho de columnas (9 caracteres ≈ 72px)
            column_config_stats = {}
            for col in styled_df.columns:
                column_config_stats[col] = st.column_config.Column(col, width=72)

            st.dataframe(
                styled_df,
                width="stretch",
                hide_index=True,
                column_config=column_config_stats,
                key=f"{_k}_worker_stats_table",
            )

            # ---- Tabla de turnos por médico y mes ----
            st.markdown("---")
            st.subheader("📅 Turnos Asignados por Mes")

            import locale
            from collections import OrderedDict

            scheduler = st.session_state.scheduler
            month_names_es = {
                1: "Ene",
                2: "Feb",
                3: "Mar",
                4: "Abr",
                5: "May",
                6: "Jun",
                7: "Jul",
                8: "Ago",
                9: "Sep",
                10: "Oct",
                11: "Nov",
                12: "Dic",
            }

            # Determine which (year, month) combos exist in the schedule
            months_in_schedule = OrderedDict()
            for date in sorted(scheduler.schedule.keys()):
                key = (date.year, date.month)
                if key not in months_in_schedule:
                    months_in_schedule[key] = f"{month_names_es[date.month]} {date.year}"

            # Build per-worker, per-month counts
            # Use the schedule as single source of truth (same as calculate_statistics)
            # to stay in sync with the per-worker table above.
            monthly_rows = []
            for worker in scheduler.workers_data:
                wid = worker["id"]
                row = {"Médico": wid}
                month_counts: dict[tuple[int, int], int] = {}
                for d, shifts in scheduler.schedule.items():
                    count = shifts.count(wid)
                    if count:
                        key = (d.year, d.month)
                        month_counts[key] = month_counts.get(key, 0) + count
                total = 0
                for mkey, mname in months_in_schedule.items():
                    cnt = month_counts.get(mkey, 0)
                    row[mname] = cnt
                    total += cnt
                row["Total"] = total
                monthly_rows.append(row)

            # Totals row
            totals_row: dict[str, Any] = {"Médico": "TOTAL"}
            grand_total = 0
            for mkey, mname in months_in_schedule.items():
                col_sum = sum(r[mname] for r in monthly_rows)
                totals_row[mname] = col_sum
                grand_total += col_sum
            totals_row["Total"] = grand_total
            monthly_rows.append(totals_row)

            monthly_df = pd.DataFrame(monthly_rows)

            # Configure column widths (8 chars ≈ 64px)
            column_config_monthly = {col: st.column_config.Column(col, width=64) for col in monthly_df.columns}

            st.dataframe(
                monthly_df,
                width="stretch",
                hide_index=True,
                column_config=column_config_monthly,
                key=f"{_k}_monthly_table",
            )

            # Gráfico de barras
            st.markdown("---")
            st.subheader("📊 Comparación Objetivo vs Asignado")

            fig = go.Figure()
            fig.add_trace(
                go.Bar(name="Objetivo", x=stats_df["Médico"], y=stats_df["Objetivo"], marker_color="lightblue")
            )
            fig.add_trace(
                go.Bar(name="Asignado", x=stats_df["Médico"], y=stats_df["Asignados"], marker_color="darkblue")
            )

            fig.update_layout(barmode="group", xaxis_title="Médico", yaxis_title="Número de Turnos", height=400)

            st.plotly_chart(fig, width="stretch", key=f"{_k}_obj_vs_asg")

            # Gráfico de desviación
            st.markdown("---")
            st.subheader("📈 Desviación por Médico")

            fig2 = px.bar(
                stats_df,
                x="Médico",
                y="Desviación",
                color="Desviación",
                color_continuous_scale=["red", "yellow", "green", "yellow", "red"],
                color_continuous_midpoint=0,
            )

            fig2.update_layout(height=400)
            st.plotly_chart(fig2, width="stretch", key=f"{_k}_deviation")

            # Gráfico de Weekend (Vie/Sab/Dom + Festivos + Pre-festivos)
            st.markdown("---")
            st.subheader("🌙 Turnos de Finde por Médico")
            st.caption("*Incluye: Viernes, Sábado, Domingo, Festivos y Pre-festivos*")

            fig3 = go.Figure()
            fig3.add_trace(
                go.Bar(
                    name="Objetivo Findes", x=stats_df["Médico"], y=stats_df["Obj. Weekend"], marker_color="lightsalmon"
                )
            )
            fig3.add_trace(
                go.Bar(name="Weekend Asignados", x=stats_df["Médico"], y=stats_df["Weekend"], marker_color="darkred")
            )

            fig3.update_layout(barmode="group", xaxis_title="Médico", yaxis_title="Turnos de Weekend", height=400)

            st.plotly_chart(fig3, width="stretch", key=f"{_k}_weekend_asg")

            # Gráfico de desviación de weekend
            st.markdown("---")
            st.subheader("📉 Desviación de Weekend por Médico")

            fig4 = px.bar(
                stats_df,
                x="Médico",
                y="Desv. Wknd",
                color="Desv. Wknd",
                color_continuous_scale=["red", "yellow", "green", "yellow", "red"],
                color_continuous_midpoint=0,
            )

            fig4.update_layout(height=400, yaxis_title="Desviación (turnos)")
            st.plotly_chart(fig4, width="stretch", key=f"{_k}_weekend_dev")

            # Gráficos de Puentes (Bridge)
            st.markdown("---")
            st.subheader("🌉 Turnos de Puente por Médico")
            st.caption(
                "*Puente: periodos de 3-4 días que incluyen festivos adyacentes a fin de semana (Jue-Dom, Vie-Dom, Vie-Lun, Vie-Mar). Se cuenta cada turno individual en días de puente.*"
            )

            # Verificar si el scheduler tiene soporte para puentes
            scheduler = st.session_state.scheduler
            has_bridge_support = (
                hasattr(scheduler, "worker_bridge_counts")
                and hasattr(scheduler, "get_bridge_objective_for_worker")
                and hasattr(scheduler, "count_bridges_for_worker")
            )

            if not has_bridge_support:
                st.info(
                    "ℹ️ El reparto actual no incluye información de puentes, genere uno nuevo para ver estas estadísticas."
                )
            else:
                # Obtener estadísticas de puentes
                bridge_stats = {}

                for worker in scheduler.workers_data:
                    worker_id = worker["id"]
                    assigned_bridges = scheduler.count_bridges_for_worker(worker_id)
                    objective = scheduler.get_bridge_objective_for_worker(worker_id)
                    deviation = assigned_bridges - objective
                    bridge_stats[worker_id] = {
                        "Asignados": assigned_bridges,
                        "Objetivo": objective,
                        "Desviación": deviation,
                    }

                # Crear DataFrame de puentes
                bridge_df = pd.DataFrame.from_dict(bridge_stats, orient="index").reset_index()
                bridge_df.columns = ["Médico", "Asignados", "Objetivo", "Desviación"]

                # Gráfico comparativo de puentes
                fig5 = go.Figure()
                fig5.add_trace(
                    go.Bar(
                        name="Objetivo Turnos Puente",
                        x=bridge_df["Médico"],
                        y=bridge_df["Objetivo"],
                        marker_color="lightgreen",
                    )
                )
                fig5.add_trace(
                    go.Bar(
                        name="Turnos Puente Asignados",
                        x=bridge_df["Médico"],
                        y=bridge_df["Asignados"],
                        marker_color="darkgreen",
                    )
                )

                fig5.update_layout(
                    barmode="group", xaxis_title="Médico", yaxis_title="Número de Turnos en Puente", height=400
                )

                st.plotly_chart(fig5, width="stretch", key=f"{_k}_bridge_asg")

                # Gráfico de desviación de puentes
                st.markdown("---")
                st.subheader("📉 Desviación de Turnos en Puente por Médico")
                st.caption("*Tolerancia objetivo: ±0.5 guardias*")

                # Añadir indicador de tolerancia
                bridge_df["Dentro_Tolerancia"] = bridge_df["Desviación"].abs() <= 0.5

                fig6 = go.Figure()

                # Barras coloreadas según tolerancia
                colors = ["green" if x else "red" for x in bridge_df["Dentro_Tolerancia"]]

                fig6.add_trace(
                    go.Bar(
                        x=bridge_df["Médico"],
                        y=bridge_df["Desviación"],
                        marker_color=colors,
                        text=bridge_df["Desviación"].round(2),
                        textposition="outside",
                    )
                )

                # Líneas de tolerancia
                fig6.add_hline(
                    y=0.5,
                    line_dash="dash",
                    line_color="orange",
                    annotation_text="Tolerancia +0.5",
                    annotation_position="right",
                )
                fig6.add_hline(
                    y=-0.5,
                    line_dash="dash",
                    line_color="orange",
                    annotation_text="Tolerancia -0.5",
                    annotation_position="right",
                )
                fig6.add_hline(y=0, line_dash="solid", line_color="gray", line_width=1)

                fig6.update_layout(
                    height=400, yaxis_title="Desviación (turnos)", xaxis_title="Médico", showlegend=False
                )
                st.plotly_chart(fig6, width="stretch", key=f"{_k}_bridge_dev")

                # Tabla resumen de puentes
                st.markdown("---")
                st.subheader("📋 Resumen de Puentes por Médico")

                # Formatear la tabla
                bridge_display_df = bridge_df.copy()
                bridge_display_df["Objetivo"] = bridge_display_df["Objetivo"].round(2)
                bridge_display_df["Desviación"] = bridge_display_df["Desviación"].apply(lambda x: f"{x:+.2f}")
                bridge_display_df["Estado"] = bridge_display_df["Dentro_Tolerancia"].apply(
                    lambda x: "✅ OK" if x else "⚠️ Fuera de tolerancia"
                )
                bridge_display_df = bridge_display_df[["Médico", "Objetivo", "Asignados", "Desviación", "Estado"]]

                # Colorear según estado
                def color_bridge_status(row):
                    if "⚠️" in str(row["Estado"]):
                        return ["background-color: #f8d7da"] * len(row)
                    else:
                        return ["background-color: #d4edda"] * len(row)

                styled_bridge_df = bridge_display_df.style.apply(color_bridge_status, axis=1)
                # Configurar ancho de columnas (9 caracteres ≈ 72px)
                column_config_bridge = {
                    col: st.column_config.Column(col, width=72) for col in bridge_display_df.columns
                }
                st.dataframe(
                    styled_bridge_df,
                    width="stretch",
                    hide_index=True,
                    column_config=column_config_bridge,
                    key=f"{_k}_bridge_table",
                )


def sync_scheduler_gap_settings(scheduler: Scheduler, session_cfg: dict[str, Any] | None = None) -> None:
    """Prefer sidebar gap settings for Verificación / Revisión scoring.

    Session ``gap_periods`` always wins when non-empty so editing the gap
    variable after generation still adjusts "Días mínimos entre guardias".
    Falls back to the scheduler's existing periods only when the sidebar is empty.
    """
    cfg = session_cfg if session_cfg is not None else (st.session_state.get("config") or {})
    session_periods = normalize_gap_periods(cfg.get("gap_periods") or [])
    if session_periods:
        scheduler.gap_periods = session_periods
    if cfg.get("gap_between_shifts") is not None:
        scheduler.gap_between_shifts = int(cfg["gap_between_shifts"])


def sync_revision_constraints_from_sources(
    scheduler: Scheduler,
    *,
    source_workers: list[dict[str, Any]] | None = None,
) -> None:
    """Push plantilla/live mandatory + incompat onto a Revisión display scheduler."""
    from saldo27.optimization_metrics import sync_worker_constraint_fields

    sources = list(source_workers or [])
    if not sources:
        live = st.session_state.get("scheduler")
        if live is not None and getattr(live, "workers_data", None):
            sources.extend(list(live.workers_data))
        sources.extend(list(st.session_state.get("workers_data") or []))
    name_mapping = dict(st.session_state.get("revision_name_mapping") or {})
    sync_worker_constraint_fields(scheduler, sources, name_mapping=name_mapping)


def check_violations(scheduler: Scheduler | None = None):
    """Verificar violaciones de restricciones usando el motor central.

    Prefers sidebar ``gap_periods`` / ``gap_between_shifts`` (gap variable) and
    syncs mandatory/incompat from plantilla/live so dual-mandatory clashes are
    exempted on Revisión display schedulers.
    """
    active = scheduler if scheduler is not None else st.session_state.scheduler
    if active is None:
        return {}

    sync_scheduler_gap_settings(active)
    sync_revision_constraints_from_sources(active)
    return collect_constraint_violations(active)


CONSTRAINT_VIOLATION_CATEGORIES: list[dict[str, str]] = [
    {
        "key": "incompatibilidades",
        "title": "🚫 Incompatibilidades",
        "hint": "Médicos marcados como incompatibles no deben coincidir el mismo día.",
        "recommendation": "Revise las incompatibilidades configuradas en los trabajadores.",
    },
    {
        "key": "gap_minimo",
        "title": "⏳ Días mínimos entre guardias",
        "hint": (
            "Mínimo global, o el del intervalo de cada fecha. "
            "Si las dos fechas piden mínimos distintos, se exige el mayor. "
            "Por debajo del 60 % de jornada se suma un día."
        ),
        "recommendation": "Aumente la plantilla o reduzca el gap si las violaciones son inevitables.",
    },
    {
        "key": "patron_7_14",
        "title": "📅 Patrón 7/14 días",
        "hint": "Evita dos guardias del mismo día de la semana a exactamente 7 o 14 días.",
        "recommendation": "Ajuste días obligatorios o aumente el número de trabajadores.",
    },
    {
        "key": "patron_viernes_lunes",
        "title": "🔀 Patrón viernes–lunes",
        "hint": "Bloquea el par viernes↔lunes (3 días de separación).",
        "recommendation": "Revise asignaciones cercanas al fin de semana o los días obligatorios.",
    },
    {
        "key": "gap2_fin_semana",
        "title": "🌉 Puente fin de semana (gap 2)",
        "hint": "Con gap efectivo = 2, evita pares que cruzan el fin de semana (jue-sáb, vie-dom, etc.).",
        "recommendation": "Revise el gap configurado o reasigne una de las dos fechas del par.",
    },
    {
        "key": "mandatory",
        "title": "🔒 Turnos obligatorios",
        "hint": "Días marcados como obligatorios (o cadencia) deben quedar asignados.",
        "recommendation": "Algunos turnos obligatorios faltan o fueron liberados en la optimización.",
    },
    {
        "key": "fuera_cadencia",
        "title": "🔁 Fuera de cadencia",
        "hint": "Un médico con cadencia solo puede tener guardias en las fechas de su patrón.",
        "recommendation": "Elimine las asignaciones extra o regenere el reparto.",
    },
    {
        "key": "dias_no_disponibles",
        "title": "🚫 Días no disponibles",
        "hint": "No se debe asignar a un médico en sus días fuera (vacaciones / days_off).",
        "recommendation": "Revise los rangos de días no disponibles del trabajador afectado.",
    },
    {
        "key": "periodos_trabajo",
        "title": "📆 Períodos de trabajo",
        "hint": "Si hay work_periods, solo se asigna dentro de esos rangos.",
        "recommendation": "Amplíe el período de trabajo o quite asignaciones fuera de rango.",
    },
    {
        "key": "fines_semana_consecutivos",
        "title": "📆 Fines de semana consecutivos",
        "hint": "Máximo de fines de semana/festivos consecutivos (semanas ISO).",
        "recommendation": "Suba el límite o redistribuya fines de semana entre la plantilla.",
    },
    {
        "key": "ultimo_puesto",
        "title": "🔢 Último puesto",
        "hint": "Un médico no puede tener 2 o más Rosell seguidos en sus propias guardias. También respeta no_last_post / only_last_post.",
        "recommendation": "Revise flags de último puesto o reasigne el puesto del día conflictivo.",
    },
]


_SCORE_COMPONENT_LABELS: dict[str, str] = {
    "fill_rate": "Cobertura",
    "workload_balance": "Balance de carga",
    "weekend_balance": "Fines de semana",
    "bridge_balance": "Puentes",
    "post_rotation": "Rotación de puestos",
    "constraint_penalty": "Penalización (violaciones)",
}


def compute_schedule_score_breakdown(
    scheduler: Scheduler | None,
    *,
    scoring_workers: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Compute the overall schedule score breakdown for a display/active scheduler.

    ``scoring_workers`` (typically the live generation ``workers_data``) overlays
    ``target_shifts`` / ``_raw_target`` so Revisión uses the same objectives as
    reparto. When omitted, workers may still carry ``_score_target_shifts`` /
    ``_score_raw_target`` stamped by :func:`build_revision_display_scheduler`.

    Always enables incomplete-calendar scaling: this helper is the Revisión /
    UI score path, where live objectives often dwarf a partial uploaded calendar.
    """
    if scheduler is None:
        return None
    snapshot = None
    try:
        sync_scheduler_gap_settings(scheduler)
        sync_revision_constraints_from_sources(scheduler, source_workers=scoring_workers)
        # Prefer explicit source; otherwise apply stamped _score_* fields.
        source = scoring_workers
        if source is None and any("_score_target_shifts" in (w or {}) for w in (scheduler.workers_data or [])):
            source = []
        name_mapping = dict(st.session_state.get("revision_name_mapping") or {})
        if source is not None:
            snapshot = apply_scoring_objectives(scheduler, source, name_mapping=name_mapping)
        return OptimizationMetrics(scheduler, scale_incomplete_objectives=True).get_score_breakdown()
    except Exception as exc:
        logging.warning("No se pudo calcular el score del reparto: %s", exc)
        return None
    finally:
        if snapshot is not None:
            restore_scoring_objectives(snapshot)


def render_schedule_score(
    *,
    scheduler: Scheduler | None = None,
    breakdown: dict[str, Any] | None = None,
    title: str = "🎯 Score del Reparto",
    caption: str | None = None,
) -> None:
    """Render the overall schedule score and weighted component breakdown."""
    st.header(title)
    if breakdown is None:
        breakdown = compute_schedule_score_breakdown(
            scheduler if scheduler is not None else st.session_state.get("scheduler")
        )
    if not breakdown:
        st.info("ℹ️ No se pudo calcular el score del reparto con el calendario actual.")
        return

    overall = float(breakdown.get("overall", 0.0))
    components = breakdown.get("components") or {}
    weights = breakdown.get("weights") or dict(OptimizationMetrics.SCORE_WEIGHTS)

    if overall >= 95:
        verdict = "🌟 Excelente"
        tone = "success"
    elif overall >= 85:
        verdict = "👍 Bueno"
        tone = "success"
    elif overall >= 70:
        verdict = "⚠️ Regular"
        tone = "warning"
    else:
        verdict = "❌ Bajo"
        tone = "error"

    st.caption(
        caption
        or (
            "Mismo score que usa el optimizador (cobertura, balance de carga, "
            "fines de semana, puentes, rotación de puestos y penalización por violaciones)."
        )
    )

    col_score, col_verdict, col_bar = st.columns([1, 1, 2])
    with col_score:
        st.metric("Score global", f"{overall:.1f} / 100")
    with col_verdict:
        getattr(st, tone)(verdict)
    with col_bar:
        st.progress(min(1.0, max(0.0, overall / 100.0)))

    st.markdown("---")
    st.subheader("Componentes")
    cols = st.columns(3)
    ordered_keys = [
        "fill_rate",
        "workload_balance",
        "weekend_balance",
        "bridge_balance",
        "post_rotation",
        "constraint_penalty",
    ]
    for idx, key in enumerate(ordered_keys):
        raw = float(components.get(key, 0.0))
        weight = float(weights.get(key, 0.0))
        contribution = raw * weight if key != "constraint_penalty" else -(raw * weight)
        label = _SCORE_COMPONENT_LABELS.get(key, key)
        with cols[idx % 3]:
            if key == "constraint_penalty":
                st.metric(
                    label,
                    f"{raw:.1f}",
                    f"peso {weight:.0%} → {contribution:+.1f}",
                    delta_color="inverse",
                )
            else:
                st.metric(
                    label,
                    f"{raw:.1f}",
                    f"peso {weight:.0%} → {contribution:+.1f}",
                )


def render_constraint_verification(
    *,
    violations: dict[str, list[str]] | None = None,
    title: str = "⚠️ Verificación de Restricciones",
    caption: str | None = None,
) -> None:
    """Render the shared constraint-verification panel (Verificación / Revisión)."""
    st.header(title)
    if violations is None:
        if st.session_state.scheduler is None:
            st.info("ℹ️ No hay calendario generado. Use el botón '🚀 Generar Reparto' en la barra lateral.")
            return
        violations = check_violations()

    total_violations = sum(len(violations.get(cat["key"], [])) for cat in CONSTRAINT_VIOLATION_CATEGORIES)

    if total_violations == 0:
        st.success("✅ ¡Excelente! No se encontraron violaciones de restricciones")
    else:
        st.error(f"❌ Se encontraron {total_violations} violaciones de restricciones")

    st.caption(
        caption
        or (
            "Comprueba todas las restricciones hard del sistema: incompatibilidades, gaps, "
            "patrones 7/14 y viernes–lunes, puente gap-2, obligatorias, días fuera, "
            "períodos de trabajo, fines de semana consecutivos y reglas de último puesto."
        )
    )

    st.markdown("---")

    def _render_violation_category(category: dict[str, str]) -> int:
        items = violations.get(category["key"], [])
        count = len(items)
        st.subheader(category["title"])
        st.caption(category["hint"])
        if count == 0:
            st.success("✅ 0 violaciones")
        else:
            st.error(f"❌ {count} violaciones")
            with st.expander("Ver detalles"):
                for item in items[:20]:
                    st.write(f"• {item}")
                if count > 20:
                    st.write(f"... y {count - 20} más")
        return count

    category_counts: dict[str, int] = {}
    for row_start in range(0, len(CONSTRAINT_VIOLATION_CATEGORIES), 3):
        cols = st.columns(3)
        for col, category in zip(cols, CONSTRAINT_VIOLATION_CATEGORIES[row_start : row_start + 3], strict=False):
            with col:
                category_counts[category["key"]] = _render_violation_category(category)

    if total_violations > 0:
        st.markdown("---")
        st.subheader("💡 Recomendaciones")
        for category in CONSTRAINT_VIOLATION_CATEGORIES:
            if category_counts.get(category["key"], 0) > 0:
                st.warning(f"⚠️ {category['recommendation']}")


# ==================== PREDICTIVE ANALYTICS ====================


def generate_demand_forecasts():
    """Generar pronósticos de demanda"""
    if not st.session_state.predictive_enabled or st.session_state.scheduler is None:
        return False, "Predictive analytics not enabled", None

    try:
        scheduler = st.session_state.scheduler

        # Check if predictive analytics exists
        if hasattr(scheduler, "generate_demand_forecasts"):
            result = scheduler.generate_demand_forecasts(forecast_days=30)
            if result.get("success"):
                forecasts = result.get("forecasts", {})
                st.session_state.demand_forecasts = forecasts
                return True, "Forecasts generated successfully", forecasts
            return False, result.get("message", "Forecast generation failed"), None
        else:
            # Fallback: basic heuristic forecasting
            schedule = scheduler.schedule
            if not schedule:
                return False, "No schedule data available", None

            # Calculate average daily demand
            total_slots = sum(len([w for w in workers if w]) for workers in schedule.values())
            avg_daily = total_slots / len(schedule) if schedule else 0

            # Simple forecast: assume same average for next 30 days
            forecasts = {"daily_demand": [avg_daily] * 30, "method": "basic_heuristic", "confidence": "low"}

            st.session_state.demand_forecasts = forecasts
            return True, "Basic forecasts generated", forecasts
    except Exception as e:
        return False, f"Error: {e!s}", None


def get_optimization_recommendations():
    """Obtener recomendaciones de optimización"""
    if not st.session_state.predictive_enabled or st.session_state.scheduler is None:
        return []

    try:
        scheduler = st.session_state.scheduler

        # Check if predictive optimizer exists
        if hasattr(scheduler, "run_predictive_optimization"):
            result = scheduler.run_predictive_optimization()
            if result.get("success"):
                recommendations = result.get("optimization_results", {}).get("optimization_recommendations", [])
                st.session_state.optimization_recommendations = recommendations
                return recommendations

        # Fallback: basic recommendations based on statistics
        recommendations = []
        stats_df = get_worker_statistics()

        if stats_df is not None:
            # Find overloaded workers
            for _, row in stats_df.iterrows():
                deviation = row["Desviación"]
                if deviation > 3:
                    recommendations.append(
                        {
                            "type": "overload",
                            "worker": row["Médico"],
                            "message": f"{row['Médico']} has {deviation} extra shifts",
                            "priority": "high" if deviation > 5 else "medium",
                        }
                    )
                elif deviation < -3:
                    recommendations.append(
                        {
                            "type": "underload",
                            "worker": row["Médico"],
                            "message": f"{row['Médico']} needs {abs(deviation)} more shifts",
                            "priority": "medium",
                        }
                    )

        st.session_state.optimization_recommendations = recommendations
        return recommendations
    except Exception as e:
        logging.error(f"Error getting recommendations: {e}")
        return []


def get_predictive_insights():
    """Obtener insights predictivos"""
    if not st.session_state.predictive_enabled:
        return []

    insights = []

    # Analyze current schedule
    if st.session_state.scheduler:
        scheduler = st.session_state.scheduler
        schedule = scheduler.schedule

        if schedule:
            # Coverage insight
            total_slots = sum(len(workers) for workers in schedule.values())
            filled_slots = sum(len([w for w in workers if w]) for workers in schedule.values())
            coverage = (filled_slots / total_slots * 100) if total_slots > 0 else 0

            if coverage < 95:
                insights.append(
                    {
                        "type": "warning",
                        "title": "Low Coverage",
                        "message": f"Current coverage is {coverage:.1f}%. Consider adding more workers or adjusting constraints.",
                    }
                )
            elif coverage >= 98:
                insights.append(
                    {
                        "type": "success",
                        "title": "Excellent Coverage",
                        "message": f"Schedule has {coverage:.1f}% coverage. Well balanced!",
                    }
                )

            # Balance insight
            stats_df = get_worker_statistics()
            if stats_df is not None:
                avg_deviation = stats_df["Desviación"].abs().mean()
                if avg_deviation > 2:
                    insights.append(
                        {
                            "type": "info",
                            "title": "Balance Opportunity",
                            "message": f"Average deviation is {avg_deviation:.1f} shifts. Consider rebalancing.",
                        }
                    )

    st.session_state.analytics_insights = insights
    return insights


def show_license_info():
    """Mostrar información de licencia en sidebar"""
    st.sidebar.markdown("---")

    limitations = st.session_state.limitations

    if limitations["mode"] == "DEMO":
        st.sidebar.warning("🔓 **MODO DEMO**")

        with st.sidebar.expander("ℹ️ Limitaciones DEMO", expanded=True):
            st.write(f"👥 Máx. médicos: **{limitations['max_workers']}**")
            st.write(f"📅 Máx. días horario: **{limitations['max_days']}**")
            st.write(f"🎯 Usos restantes: **{st.session_state.uses_remaining}/{license_manager.DEMO_MAX_USES}**")
            if limitations["watermark"]:
                st.write("💧 Marca de agua en PDFs")

        if st.session_state.uses_remaining is not None and st.session_state.uses_remaining <= 3:
            st.sidebar.error(f"⚠️ Solo quedan **{st.session_state.uses_remaining}** usos!")

        st.sidebar.markdown("---")

        with st.sidebar.expander("🔑 Activar Licencia", expanded=False):
            with st.form("activation_sidebar"):
                license_key = st.text_input("Clave de Licencia:", placeholder="GP-XXXX-XXXX-XXXX-XXXX", max_chars=25)

                submit = st.form_submit_button("Activar", width="stretch")

                if submit and license_key:
                    success, message = license_manager.activate_license(license_key)
                    if success:
                        st.success(message)
                        st.balloons()
                        import time

                        time.sleep(2)
                        st.rerun()
                    else:
                        st.error(message)

            st.caption("📧 Contacto: luisherrerapara@gmail.com")

        # ===== MODO DESARROLLADOR (comentado para producción) =====
        # st.sidebar.markdown("---")
        # if st.sidebar.checkbox("🔧 Modo Desarrollador", value=False):
        #     if st.sidebar.button("🔄 Resetear DEMO"):
        #         license_manager. reset_demo()
        #         st.sidebar.success("✅ Demo reseteado")
        #         import time
        #         time.sleep(1)
        #         st.rerun()
        # ===== FIN =====

    else:
        st.sidebar.success("✅ **LICENCIA COMPLETA**")
        st.sidebar.caption("🎉 Todas las funciones desbloqueadas")


# ==================== INTERFAZ PRINCIPAL ====================

# Header
st.title(f"📅 Sistema de Generación de Guardias - v{APP_VERSION}")
st.markdown("---")

# ===== AGREGAR AQUÍ:  Mostrar info y verificar bloqueo =====
# Mostrar info de licencia en sidebar
show_license_info()

# Verificar si puede usar la aplicación
if not st.session_state.can_use:
    st.error("🔒 **Límite de Usos Alcanzado**")

    st.markdown("""
    ### Has alcanzado el límite de la versión DEMO

    La versión DEMO permite **10 generaciones de horarios** para que puedas
    evaluar todas las funcionalidades de GuardiasApp.
    """)

    # Obtener estadísticas
    stats = license_manager.get_usage_stats()
    uses = stats["uses"]
    first_use = stats["first_use"][:10] if stats["first_use"] else "N/A"

    # Mostrar info
    st.info(f"""**Estadísticas de uso:**
- ✅ Generaciones realizadas: **{uses}**
- 📅 Primer uso: **{first_use}**
""")

    st.markdown("---")
    st.markdown("### 🔑 Activar Licencia Completa")

    col1, col2 = st.columns([2, 1])

    with col1, st.form("activation_form_main"):
        license_key = st.text_input(
            "Introduce tu clave de licencia:", placeholder="GP-XXXX-XXXX-XXXX", help="Ejemplo: GP-AB12-CD34-5678"
        )

        submit = st.form_submit_button("🚀 Activar Licencia", width="stretch")

        if submit and license_key:
            success, message = license_manager.activate_license(license_key)
            if success:
                st.success(message)
                st.balloons()
                import time

                time.sleep(2)
                st.rerun()
            else:
                st.error(message)

    with col2:
        st.markdown("""
**Beneficios Licencia Completa:**
- ♾️ Generaciones ilimitadas
- 👥 Trabajadores ilimitados
- 📅 Días ilimitados
- 📄 PDFs sin marca de agua
- 🆘 Soporte prioritario
""")

    st.markdown("---")
    st.info("**¿Necesitas una licencia?**\n\n📧 Contacta:  luisherrerapara@gmail.com")

    st.stop()  # Detener ejecución

# Sidebar - Configuración y Controles
with st.sidebar:
    st.header("⚙️ Configuración")

    # Importación y Exportación
    with st.expander("📂 Restaurar sesión / Exportar Calendario", expanded=False):
        # Importar
        sched_file = st.file_uploader(
            "Cargar JSON Completo", type="json", key=f"schedule_uploader_{st.session_state.schedule_upload_counter}"
        )
        if sched_file is not None:
            if st.button("🔄 Restaurar Datos"):
                success, msg = load_schedule_from_json(sched_file)
                if success:
                    st.success(msg)
                    st.session_state.schedule_upload_counter += 1
                    st.rerun()
                else:
                    st.error(msg)

        st.markdown("---")

        # Exportar
        if st.session_state.workers_data:
            # Prepare full export data
            export_data = st.session_state.config.copy()

            # Helper function to convert datetime objects recursively
            def convert_datetime_to_string(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, dict):
                    return {k: convert_datetime_to_string(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_datetime_to_string(item) for item in obj]
                else:
                    return obj

            # Convert all datetime objects in the config
            export_data = convert_datetime_to_string(export_data)
            assert isinstance(export_data, dict)

            # Add metadata for prior-schedule handler compatibility
            if "start_date" in st.session_state.config and "end_date" in st.session_state.config:
                sd = st.session_state.config["start_date"]
                ed = st.session_state.config["end_date"]
                export_data["metadata"] = {
                    "period_start": sd.strftime("%Y-%m-%d") if isinstance(sd, datetime) else str(sd),
                    "period_end": ed.strftime("%Y-%m-%d") if isinstance(ed, datetime) else str(ed),
                    "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

            # Add workers data
            export_data["workers_data"] = st.session_state.workers_data

            # Add schedule if exists
            if st.session_state.schedule:
                # Convert schedule keys (datetime) to strings
                sched_export = {}
                for k, v in st.session_state.schedule.items():
                    sched_export[k.isoformat()] = v
                export_data["schedule"] = sched_export

            # Export button
            st.download_button(
                label="💾 Descargar Respaldo Completo (JSON)",
                data=json.dumps(export_data, indent=2, ensure_ascii=False),
                file_name=f"schedule_full_export_{datetime.now().strftime('%Y%m%d')}.json",
                mime="application/json",
            )

    # ── Calendario anterior (cross-period constraints) ──────────────────────
    with st.expander("📅 Calendario Anterior", expanded=False):
        st.caption(
            "Carga un calendario de meses anteriores para respetar constraints "
            "cross-período: huecos mínimos entre turnos y fines de semana consecutivos."
        )
        prior_file = st.file_uploader(
            "Cargar JSON de calendario anterior",
            type="json",
            key=f"prior_schedule_uploader_{st.session_state.schedule_upload_counter}",
        )
        col_load, col_clear = st.columns(2)
        with col_load:
            if prior_file is not None and st.button("📥 Cargar", key="btn_load_prior"):
                # Always store raw bytes so future Scheduler instances can re-apply
                raw_bytes = prior_file.read()
                st.session_state.prior_schedule_raw = raw_bytes
                _sched: Scheduler | None = st.session_state.get("scheduler")
                if _sched is not None:
                    from io import BytesIO

                    result = _sched.load_prior_schedule_data(BytesIO(raw_bytes))
                    if result.get("error"):
                        st.error(result["error"])
                    else:
                        st.session_state.prior_schedule_data = result.get("summary", {})
                        st.session_state.prior_schedule_loaded = True
                        st.success(f"✅ Cargado: {len(st.session_state.prior_schedule_data)} trabajadores")
                else:
                    st.session_state.prior_schedule_loaded = True
                    st.info("El calendario anterior se aplicará al generar el nuevo reparto.")
        with col_clear:
            if st.session_state.prior_schedule_loaded and st.button("🗑️ Limpiar", key="btn_clear_prior"):
                _sched2: Scheduler | None = st.session_state.get("scheduler")
                if _sched2 is not None:
                    _sched2.clear_prior_schedule_data()
                st.session_state.prior_schedule_data = None
                st.session_state.prior_schedule_raw = None
                st.session_state.prior_schedule_loaded = False
                st.rerun()

        if st.session_state.prior_schedule_loaded:
            summary = st.session_state.prior_schedule_data
            if summary:
                import pandas as pd

                df_prior = pd.DataFrame(
                    [
                        {
                            "Trabajador": wid,
                            "Turnos": v.get("shifts", 0),
                            "Weekends": v.get("weekends", 0),
                            "Último turno": (v["last_date"].strftime("%d/%m/%Y") if v.get("last_date") else "—"),
                        }
                        for wid, v in summary.items()
                    ]
                )
                st.dataframe(df_prior, use_container_width=True)
            else:
                st.info("Calendario cargado (los datos se aplicarán al generar el reparto).")

    # Período de reparto (Fecha Inicial - Fecha Final)
    st.subheader("📅 Período de Reparto")

    today = date.today()
    default_end = date(today.year, 12, 31)
    if default_end <= today:
        default_end = date(today.year + 1, 12, 31)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Fecha Inicial (L-D)",
            value=today,
            help="Fecha de inicio del período a programar. Calendario: Lunes a Domingo",
            format="DD/MM/YYYY",
        )
    with col2:
        end_date = st.date_input(
            "Fecha Final (L-D)",
            value=default_end,
            help="Fecha de fin del período a programar. Calendario: Lunes a Domingo",
            format="DD/MM/YYYY",
        )

    # Validar fechas
    if start_date >= end_date:
        st.error("⚠️ La fecha final debe ser posterior a la inicial")

    st.markdown("---")

    # Festivos (Holidays)
    st.subheader("🎉 Festivos")
    holidays_input = st.text_area(
        "Fechas festivas (una por línea, formato: DD-MM-YYYY)",
        value="24-12-2026\n25-12-2026\n31-12-2026\n01-01-2027\n05-01-2027\n06-01-2027\n07-01-2027\n19-03-2027\n25-03-2027\n26-03-2027\n01-05-2027\n09-06-2027\n16-08-2027\n24-09-2027\n12-10-2027\n01-11-2027\n06-12-2027\n07-12-2027\n24-12-2027\n25-12-2027\n31-12-2027\n01-01-2028",
        height=100,
        help="Días festivos donde se aplicarán reglas especiales",
    )

    # Parsear festivos
    holidays = []
    for line in holidays_input.strip().split("\n"):
        line = line.strip()
        if line:
            try:
                holiday_date = datetime.strptime(line, "%d-%m-%Y")
                holidays.append(holiday_date)
            except ValueError:
                st.warning(f"⚠️ Fecha inválida ignorada: {line}")

    # Guardar festivos en session_state para acceso desde otras tabs
    st.session_state.sidebar_holidays = holidays
    st.session_state.sidebar_holidays_input = holidays_input

    if holidays:
        st.success(f"✅ {len(holidays)} festivos configurados")

    st.markdown("---")

    # Parámetros del sistema
    st.subheader("⚙️ Parámetros Globales")

    # Período con número de guardias por defecto
    num_shifts = st.number_input(
        "Guardias por día (por defecto)",
        min_value=1,
        max_value=10,
        value=st.session_state.config.get("num_shifts", 4),
        help="Número de Guardias a cubrir por día",
    )
    st.session_state.config["num_shifts"] = num_shifts

    # Variable shifts (períodos con diferente número de guardias)
    with st.expander("📊 Períodos con guardias/día variables"):
        st.markdown("**Periodos de días con diferente número de guardias**")

        variable_shifts_text = st.text_area(
            "Formato: DD-MM-YYYY / DD-MM-YYYY: número",
            value="",
            height=100,
            help="Periodos con diferente número de guardias. Un periodo por línea. Ejemplo: 01-08-2026 / 31-08-2026: 2",
        )

        variable_shifts = []
        for line in variable_shifts_text.strip().split("\n"):
            line = line.strip()
            if ":" in line:
                try:
                    dates_part, shifts_str = line.rsplit(":", 1)
                    shifts_num = int(shifts_str.strip())
                    if "/" in dates_part:
                        start_str, end_str = dates_part.split("/")
                        start_date_obj = datetime.strptime(start_str.strip(), "%d-%m-%Y")
                        end_date_obj = datetime.strptime(end_str.strip(), "%d-%m-%Y")
                    else:
                        start_date_obj = datetime.strptime(dates_part.strip(), "%d-%m-%Y")
                        end_date_obj = start_date_obj
                    variable_shifts.append(
                        {"start_date": start_date_obj, "end_date": end_date_obj, "shifts": shifts_num}
                    )
                except (ValueError, TypeError):
                    st.warning(f"⚠️ Línea inválida: {line}")

        if variable_shifts:
            st.success(f"✅ {len(variable_shifts)} días con turnos variables")

        st.session_state.config["variable_shifts"] = variable_shifts

    col_gap, col_weekends = st.columns(2)

    with col_gap:
        gap_between_shifts = st.number_input(
            "Días mínimos entre guardias",
            min_value=0,
            max_value=7,
            value=st.session_state.config.get("gap_between_shifts", 3),
            help=(
                "Diferencia mínima en días entre dos guardias del mismo médico. "
                "Con 4, lunes→viernes es válido y lunes→jueves no. "
                "Los médicos con menos del 60 % de jornada usan este valor + 1. "
                "Un intervalo de fechas puede usar otro mínimo."
            ),
        )
        st.session_state.config["gap_between_shifts"] = gap_between_shifts

    with col_weekends:
        max_consecutive_weekends = st.number_input(
            "Fines de semana consecutivos máx.",
            min_value=1,
            max_value=5,
            value=st.session_state.config.get("max_consecutive_weekends", 3),
            help="Número máximo de fines de semana consecutivos que puede trabajar un trabajador",
        )
        st.session_state.config["max_consecutive_weekends"] = max_consecutive_weekends

    with st.expander("📅 Períodos con días mínimos distintos"):
        st.caption(
            "Fuera de estos intervalos se usa el mínimo global. "
            "Si las dos guardias caen en mínimos distintos, se exige el mayor. "
            "Si dos intervalos se solapan, vale el de la línea de más abajo. "
            "Quien tiene menos del 60 % de jornada suma un día a ese mínimo."
        )
        if "gap_periods_text" not in st.session_state:
            st.session_state.gap_periods_text = format_gap_periods_text(st.session_state.config.get("gap_periods", []))
        st.text_area(
            "Formato: DD-MM-YYYY / DD-MM-YYYY: días",
            height=100,
            key="gap_periods_text",
            help="Un intervalo por línea. También vale un solo día. Ejemplo: 01-08-2026 / 31-08-2026: 2",
        )
        gap_periods, gap_period_warnings = parse_gap_periods_text(st.session_state.gap_periods_text)
        for warning in gap_period_warnings:
            st.warning(warning)
        if gap_periods:
            count = len(gap_periods)
            noun = "intervalo" if count == 1 else "intervalos"
            st.success(f"✅ {count} {noun} con mínimo propio")
        st.session_state.config["gap_periods"] = gap_periods

    # Configuración adicional de fines de semana
    with st.expander("⚙️ Configuración Avanzada de Fines de Semana"):
        enable_proportional = st.checkbox(
            "Habilitar balance proporcional de fines de semana",
            value=st.session_state.config.get("enable_proportional_weekends", True),
            help="Distribuir fines de semana proporcionalmente según el porcentaje laboral de cada trabajador",
        )
        st.session_state.config["enable_proportional_weekends"] = enable_proportional

        weekend_tolerance = st.slider(
            "Tolerancia de fines de semana (±)",
            min_value=0,
            max_value=3,
            value=st.session_state.config.get("weekend_tolerance", 1),
            help=(
                "Tope de generación: este número de guardias de fin de semana por encima del objetivo proporcional. "
                "En Ajuste final, una desviación superior al ±15% de ese objetivo se penaliza como error."
            ),
        )
        st.session_state.config["weekend_tolerance"] = weekend_tolerance

    # Predictive Analytics
    with st.expander("🔮 Predictive Analytics"):
        enable_predictive = st.checkbox(
            "Enable predictive analytics",
            value=st.session_state.config.get("enable_predictive_analytics", False),
            help="Enable AI-powered demand forecasting and optimization recommendations",
        )
        st.session_state.config["enable_predictive_analytics"] = enable_predictive
        st.session_state.predictive_enabled = enable_predictive

        if enable_predictive:
            st.success("✅ Predictive analytics enabled")
            st.caption("View forecasts and recommendations in the Analytics tab")

    st.markdown("---")

    # Botón de generación
    st.subheader("🚀 Generar Reparto")

    if len(st.session_state.workers_data) == 0:
        st.warning("⚠️ Primero agregue médicos")
        generate_button = st.button("🚀 Generar", disabled=True, type="primary")
    else:
        st.info(f"📊 {len(st.session_state.workers_data)} trabajadores configurados")
        generate_button = st.button("🚀 Generar Calendario", type="primary")

    if generate_button:
        with st.spinner("Generando calendario... esto puede tomar varios minutos"):
            try:
                success, message = generate_schedule_internal(
                    start_date, end_date, holidays, st.session_state.config.get("variable_shifts", [])
                )
                if success:
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)
            except Exception as e:
                st.error(f"❌ Error crítico durante la generación: {e!s}")
                with st.expander("Ver detalles del error"):
                    st.code(traceback.format_exc())

    st.markdown("---")

    # Información del sistema
    with st.expander("ℹ️ Información del Sistema"):
        st.markdown("""
        **Restricciones implementadas:**
        - ✅ Guardias obligatorias protegidas
        - ✅ Incompatibilidades entre médicos
        - ✅ Patrón 7/14 días (mismo día de semana)
        - ✅ Patrón viernes–lunes
        - ✅ Días mínimos entre guardias configurables, también por intervalo de fechas
        - ✅ Puente fin de semana con gap = 2
        - ✅ Fines de semana consecutivos máximos
        - ✅ Balance proporcional de fines de semana
        - ✅ Días fuera (no disponibles)
        - ✅ Períodos personalizados por médico
        - ✅ Reglas de último puesto (no_last_post / only_last_post)
        - ✅ Guardias variables por día/período

        **Parámetros configurables:**
        - 📅 Período de reparto (fecha inicial/final)
        - 🎉 Festivos
        - 🔢 Guardias por día (por defecto y variables)
        - ⏳ Gap entre guardias (global y por intervalo)
        - 📆 Fines de semana consecutivos
        - 📊 Tolerancia general y de fines de semana
        """)

    # Contenedor para mensajes de progreso durante la generación
    st.markdown("---")
    sidebar_progress_container = st.container()
    sidebar_progress_container.caption("📋 **Log de Progreso**")
    _new_log_ph = sidebar_progress_container.empty()
    st.session_state._sidebar_log_placeholder = _new_log_ph
    # Re-display persisted log content after a rerun
    _saved_log = st.session_state.get("_sidebar_log_content")
    if isinstance(_saved_log, tuple) and len(_saved_log) == 2:
        _log_type, _log_text = _saved_log
        if _log_type == "code":
            _new_log_ph.code(_log_text, language=None)
        elif _log_type == "warning":
            _new_log_ph.warning(_log_text)

# Tabs principales
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
    [
        "👥 Gestión de Médicos",
        "📅 Calendario Generado",
        "📊 Estadísticas",
        "⚖️ Ajuste Final",
        "⚠️ Verificación de Restricciones",
        "🔮 Predictive Analytics",
        "🔍 Revisión",
    ]
)

# ==================== TAB 1: GESTIÓN DE TRABAJADORES ====================
with tab1:
    st.header("👥 Gestión de Médicos")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Agregar/Editar Médico")

        # Inicializar buffers si no existen (para evitar que muestren None)
        if "work_periods_buffer" not in st.session_state:
            st.session_state.work_periods_buffer = ""
        if "mandatory_dates_buffer" not in st.session_state:
            st.session_state.mandatory_dates_buffer = ""
        if "days_off_buffer" not in st.session_state:
            st.session_state.days_off_buffer = ""

        # Si el formulario se guardó/limpió en el rerun anterior, resetear aquí los
        # valores de los widgets (ANTES de que se instancien más abajo en este mismo
        # run). Modificar st.session_state[key] de un widget DESPUÉS de haber sido
        # instanciado en el mismo run lanza StreamlitAPIException, por eso el reset
        # no puede hacerse en el propio manejador de submit/clear.
        if st.session_state.get("pending_form_reset", False):
            st.session_state.worker_id_input = ""
            st.session_state.slider_work_percentage_form = 100
            st.session_state.auto_calc_checkbox = True
            st.session_state.guardias_mes_input = 4
            st.session_state.work_periods_textarea = ""
            st.session_state.is_incompatible_checkbox = False
            st.session_state.no_last_post_checkbox = False
            st.session_state.only_last_post_checkbox = False
            st.session_state.form_mandatory_dates_area = ""
            st.session_state.form_days_off_area = ""
            st.session_state.has_cadence_checkbox = False
            st.session_state.cadence_days_input = 1
            st.session_state.cadence_start_date_input = None
            st.session_state.incompatible_with_multiselect = []
            st.session_state.pending_form_reset = False

        # Mostrar indicador si estamos en modo edición
        if st.session_state.get("editing_worker"):
            st.info(f"✏️ **Modo edición:** Editando a {st.session_state.get('editing_worker')}")

            # Solo cargar buffers UNA VEZ (cuando buffers_loaded es False)
            if not st.session_state.get("buffers_loaded", False):
                # Inicializar el ID con el buffer
                if "worker_id_buffer" in st.session_state and st.session_state.worker_id_buffer:
                    st.session_state.worker_id_input = st.session_state.worker_id_buffer
                # Inicializar auto_calc_checkbox con buffer
                if "auto_calc_buffer" in st.session_state:
                    st.session_state.auto_calc_checkbox = st.session_state.auto_calc_buffer
                # Inicializar guardias_mes_input con buffer
                if "guardias_mes_buffer" in st.session_state:
                    st.session_state.guardias_mes_input = st.session_state.guardias_mes_buffer
                # Inicializar campos de cadencia con buffer
                if "has_cadence_buffer" in st.session_state:
                    st.session_state.has_cadence_checkbox = st.session_state.has_cadence_buffer
                if "cadence_days_buffer" in st.session_state:
                    st.session_state.cadence_days_input = st.session_state.cadence_days_buffer
                if "cadence_start_date_buffer" in st.session_state and st.session_state.cadence_start_date_buffer:
                    st.session_state.cadence_start_date_input = st.session_state.cadence_start_date_buffer
                # Cargar el resto de buffers correspondientes a widgets DENTRO del form
                # (porcentaje de jornada, periodos, mandatory, days off, incompatibilidades).
                # CRÍTICO: esto debe ejecutarse SOLO una vez (al entrar en modo edición),
                # nunca en el rerun que dispara el propio submit del form, o de lo
                # contrario el valor recién introducido por el usuario (p.ej. el slider
                # de Porcentaje de Jornada) sería sobreescrito por el valor antiguo del
                # buffer justo antes de guardarse.
                if "work_percentage_buffer" in st.session_state and st.session_state.work_percentage_buffer:
                    st.session_state.slider_work_percentage_form = int(st.session_state.work_percentage_buffer)
                if "work_periods_buffer" in st.session_state and st.session_state.work_periods_buffer:
                    st.session_state.work_periods_textarea = st.session_state.work_periods_buffer
                if "mandatory_dates_buffer" in st.session_state and st.session_state.mandatory_dates_buffer:
                    st.session_state.form_mandatory_dates_area = st.session_state.mandatory_dates_buffer
                if "days_off_buffer" in st.session_state and st.session_state.days_off_buffer:
                    st.session_state.form_days_off_area = st.session_state.days_off_buffer
                if "incompatible_buffer" in st.session_state:
                    st.session_state.is_incompatible_checkbox = st.session_state.incompatible_buffer
                if "incompatible_with_buffer" in st.session_state and st.session_state.incompatible_with_buffer:
                    st.session_state.incompatible_with_multiselect = st.session_state.incompatible_with_buffer
                if "no_last_post_buffer" in st.session_state:
                    st.session_state.no_last_post_checkbox = st.session_state.no_last_post_buffer
                if "only_last_post_buffer" in st.session_state:
                    st.session_state.only_last_post_checkbox = st.session_state.only_last_post_buffer
                # Marcar que ya se cargaron los buffers
                st.session_state.buffers_loaded = True

        # ID del Médico - FUERA DEL FORM para acceso global
        st.markdown("**👤 Identificación**")
        worker_id = st.text_input("ID del Médico *", placeholder="Ej: TRAB001", key="worker_id_input")

        # Número de Guardias - FUERA DEL FORM para REACTIVIDAD
        st.markdown("**📊 Número de Guardias**")

        col_guards_a, col_guards_b = st.columns(2)
        with col_guards_a:
            auto_calculate = st.checkbox(
                "Calcular automáticamente",
                key="auto_calc_checkbox",
                help="El sistema calculará la asignación según el período y porcentaje",
            )
        with col_guards_b:
            if not auto_calculate:
                guardias_per_month = st.number_input(
                    "Guardias/mes",
                    min_value=0,
                    key="guardias_mes_input",
                    help="Número de guardias por mes (constraint para el período)",
                )
            else:
                st.info("ℹ️ Se calcularán automáticamente")
                guardias_per_month = 0

        col_cad_a, col_cad_b, col_cad_c = st.columns(3)
        with col_cad_a:
            has_cadence = st.checkbox(
                "Cadencia",
                key="has_cadence_checkbox",
                help=(
                    "Guardias asignadas cada X días desde una fecha de inicio. "
                    "Esta asignación es obligatoria, inamovible y exclusiva: el médico "
                    "solo tendrá las guardias propias de la cadencia (ningún cambio "
                    "salvo puesto de last post el mismo día de cadencia)."
                ),
            )
        with col_cad_b:
            cadence_days = st.number_input(
                "días",
                min_value=1,
                key="cadence_days_input",
                disabled=not has_cadence,
                help="Número de días entre guardias consecutivas de la cadencia",
            )
        with col_cad_c:
            cadence_start_date = st.date_input(
                "Fecha de inicio",
                key="cadence_start_date_input",
                disabled=not has_cadence,
                format="DD/MM/YYYY",
                help="Fecha en la que comienza la cadencia (primera guardia)",
            )

        with st.form("worker_form"):
            # El ID lo pasamos desde session_state
            # Porcentaje de Jornada
            st.markdown("**📋 Información Básica**")
            col_a, col_b = st.columns(2)
            with col_a:
                work_percentage = st.slider(
                    "Porcentaje de Jornada",
                    0,
                    100,
                    step=1,
                    key="slider_work_percentage_form",
                    help="100% = tiempo completo, 50% = media jornada",
                )
            with col_b:
                st.write("")  # Espaciador

            # Períodos de Trabajo
            st.markdown("**📅 ¿Sus Períodos de Trabajo difieren del general?**")
            work_periods = st.text_area(
                "Rangos de fechas disponibles (uno por línea o separados por punto y coma)",
                placeholder="01-01-2026 - 31-03-2026; 01-06-2026 - 31-12-2026",
                help="Formato: DD-MM-YYYY - DD-MM-YYYY. Si se deja vacío, se asume disponibilidad total en el período global.",
                key="work_periods_textarea",
            )

            # Incompatibilidades (actualizado a multiselect)
            st.markdown("**🚫 Incompatibilidades**")
            # Fila 1: checkboxes de marcapasos y Rosell
            col_inc1, col_inc2 = st.columns(2)
            with col_inc1:
                is_incompatible = st.checkbox(
                    "Implantador de Marcapasos",
                    help="Este médico no puede coincidir con otros marcados igual",
                    key="is_incompatible_checkbox",
                )
            with col_inc2:
                no_last_post = st.checkbox(
                    "No asignar Rosell",
                    help="Este médico no puede tener last posts (último puesto) asignados",
                    key="no_last_post_checkbox",
                )
            # Fila 2: Solo Rosell y multiselect de IDs incompatibles
            col_inc3, col_inc4 = st.columns(2)
            with col_inc3:
                only_last_post = st.checkbox(
                    "Solo Rosell",
                    help="Este médico SOLO puede ser asignado en el Rosell.",
                    key="only_last_post_checkbox",
                    disabled=no_last_post,
                )
            with col_inc4:
                # Obtener lista de otros médicos para el multiselect
                existing_ids = [w["id"] for w in st.session_state.workers_data if w["id"] != worker_id]

                incompatible_with = st.multiselect(
                    "Incompatible con IDs específicos",
                    options=existing_ids,
                    disabled=is_incompatible,
                    help="Seleccione los médicos con los que NO puede coincidir",
                    key="incompatible_with_multiselect",
                )

            # Días obligatorios
            st.markdown("**✅ Guardias Obligatorias (Mandatory)**")
            if has_cadence:
                st.caption(
                    "🔁 Este médico tiene Cadencia activa: las fechas obligatorias se calculan "
                    "automáticamente a partir de la cadencia y este campo se ignora."
                )
            mandatory_dates = st.text_area(
                "Fechas obligatorias (una por línea o separadas por punto y coma)",
                placeholder="01-12-2026; 15-12-2026; 25-12-2026",
                height=80,
                help="Días en los que DEBE trabajar obligatoriamente",
                key="form_mandatory_dates_area",
                disabled=has_cadence,
            )

            # Días fuera (nueva funcionalidad)
            st.markdown("**❌ Días Fuera (No disponible)**")
            days_off = st.text_area(
                "Fechas no disponibles (una por línea o separadas por punto y coma)",
                placeholder="10-12-2026; 20-12-2026; 30-12-2026",
                height=80,
                help="Días en los que NO puede tener asignación de guardias (vacaciones, permisos, etc.)",
                key="form_days_off_area",
            )

            col_submit, col_clear = st.columns(2)
            with col_submit:
                # Cambiar texto del botón según modo edición
                is_editing = st.session_state.get("editing_worker") is not None
                button_label = "✏️ Actualizar Médico" if is_editing else "➕ Agregar Médico"
                submit = st.form_submit_button(button_label, type="primary")
            with col_clear:
                clear = st.form_submit_button("🗑️ Limpiar Formulario")

            if submit:
                # Obtener worker_id desde session_state (FUERA DEL FORM)
                form_worker_id = st.session_state.get("worker_id_input", "").strip()

                if not form_worker_id:
                    st.error("❌ ID del Médico es obligatorio")
                else:
                    # Detectar si estamos editando
                    is_editing = st.session_state.get("editing_worker") is not None
                    # Parsear incompatibilidades
                    incomp_list = []
                    if not is_incompatible and incompatible_with:
                        incomp_list = incompatible_with

                    # Parsear días obligatorios
                    mandatory_list = []
                    if mandatory_dates:
                        # Normalizar separadores
                        dates_str = mandatory_dates.replace("\n", ";").replace(",", ";")
                        parts = [x.strip() for x in dates_str.split(";") if x.strip()]
                        worker_data_mandatory = ";".join(parts)  # Guardar como string normalizado
                    else:
                        worker_data_mandatory = ""

                    # Parsear días fuera
                    if days_off:
                        dates_str = days_off.replace("\n", ";").replace(",", ";")
                        parts = [x.strip() for x in dates_str.split(";") if x.strip()]
                        worker_data_days_off = ";".join(parts)
                    else:
                        worker_data_days_off = ""

                    # Parsear work periods
                    if work_periods:
                        dates_str = work_periods.replace("\n", ";")
                        parts = [x.strip() for x in dates_str.split(";") if x.strip()]
                        worker_data_work_periods = ";".join(parts)
                    else:
                        worker_data_work_periods = ""

                    # Obtener auto_calculate del session state (FUERA DEL FORM)
                    auto_calculate_flag = st.session_state.get("auto_calc_checkbox", True)
                    work_percentage_value = st.session_state.get("slider_work_percentage_form", 100)

                    # Obtener target_shifts según el flag
                    if not auto_calculate_flag:
                        # Usar el valor del input de "Guardias/mes" (FUERA DEL FORM)
                        target_shifts_value = st.session_state.get("guardias_mes_input", 4)
                    else:
                        # Será calculado automáticamente
                        target_shifts_value = 0

                    # Obtener datos de cadencia (FUERA DEL FORM)
                    has_cadence_flag = st.session_state.get("has_cadence_checkbox", False)
                    cadence_days_value = st.session_state.get("cadence_days_input", 1)
                    cadence_start_date_value = st.session_state.get("cadence_start_date_input")
                    cadence_start_date_str = (
                        cadence_start_date_value.strftime("%d-%m-%Y") if cadence_start_date_value else ""
                    )
                    if has_cadence_flag and not cadence_start_date_str:
                        st.error("❌ Debe indicar una Fecha de inicio para la cadencia")
                        st.stop()

                    # La cadencia es obligatoria y exclusiva: sobreescribe el
                    # cálculo automático/manual y los días obligatorios manuales
                    if has_cadence_flag:
                        auto_calculate_flag = False
                        target_shifts_value = 0

                    # Crear/actualizar trabajador
                    worker_data = {
                        "id": form_worker_id,
                        "target_shifts": target_shifts_value,
                        "work_percentage": work_percentage_value,  # Corrected: Scale 0-100, not 0-1
                        "is_incompatible": is_incompatible,
                        "incompatible_with": incomp_list,
                        "no_last_post": no_last_post,
                        "only_last_post": only_last_post and not no_last_post,
                        "mandatory_days": worker_data_mandatory,  # Renamed to match scheduler and used string
                        "days_off": worker_data_days_off,  # New field
                        "work_periods": worker_data_work_periods,  # New field
                        "auto_calculate_shifts": auto_calculate_flag,
                        "has_cadence": has_cadence_flag,
                        "cadence_days": int(cadence_days_value) if has_cadence_flag else 0,
                        "cadence_start_date": cadence_start_date_str if has_cadence_flag else "",
                    }

                    # Verificar si ya existe
                    existing_idx = None
                    for idx, w in enumerate(st.session_state.workers_data):
                        if w["id"] == form_worker_id:
                            existing_idx = idx
                            break

                    if existing_idx is not None:
                        st.session_state.workers_data[existing_idx] = worker_data
                        st.success(f"✅ Médico {form_worker_id} actualizado")
                    else:
                        st.session_state.workers_data.append(worker_data)
                        st.success(f"✅ Médico {form_worker_id} agregado")

                    # Limpiar estado de edición y formulario
                    st.session_state.editing_worker = None
                    st.session_state.buffers_loaded = False  # Reset flag
                    # Limpiar buffers
                    st.session_state.worker_id_buffer = ""
                    st.session_state.work_percentage_buffer = 100
                    st.session_state.auto_calc_buffer = True
                    st.session_state.guardias_mes_buffer = 4
                    st.session_state.work_periods_buffer = ""
                    st.session_state.incompatible_buffer = False
                    st.session_state.incompatible_with_buffer = []
                    st.session_state.no_last_post_buffer = False
                    st.session_state.only_last_post_buffer = False
                    st.session_state.mandatory_dates_buffer = ""
                    st.session_state.days_off_buffer = ""
                    st.session_state.has_cadence_buffer = False
                    st.session_state.cadence_days_buffer = 1
                    st.session_state.cadence_start_date_buffer = None

                    # Marcar que hay que resetear los widgets del formulario en el
                    # próximo run, ANTES de que se instancien (ver bloque al inicio
                    # de esta pestaña). No se puede modificar aquí directamente el
                    # session_state de estos widgets porque ya fueron instanciados
                    # en este mismo run.
                    st.session_state.pending_form_reset = True

                    st.rerun()

            if clear:
                # Limpiar todos los campos
                st.session_state.editing_worker = None
                st.session_state.buffers_loaded = False  # Reset flag
                # Limpiar buffers
                st.session_state.worker_id_buffer = ""
                st.session_state.work_percentage_buffer = 100
                st.session_state.auto_calc_buffer = True
                st.session_state.guardias_mes_buffer = 4
                st.session_state.work_periods_buffer = ""
                st.session_state.incompatible_buffer = False
                st.session_state.incompatible_with_buffer = []
                st.session_state.no_last_post_buffer = False
                st.session_state.only_last_post_buffer = False
                st.session_state.mandatory_dates_buffer = ""
                st.session_state.days_off_buffer = ""
                st.session_state.has_cadence_buffer = False
                st.session_state.cadence_days_buffer = 1
                st.session_state.cadence_start_date_buffer = None

                # Marcar que hay que resetear los widgets del formulario en el
                # próximo run, ANTES de que se instancien (ver bloque al inicio
                # de esta pestaña). No se puede modificar aquí directamente el
                # session_state de estos widgets porque ya fueron instanciados
                # en este mismo run.
                st.session_state.pending_form_reset = True

                st.success("✅ Formulario limpiado")
                st.rerun()

    with col2:
        st.subheader("Gestión de Datos")

        # Cargar desde archivo
        uploaded_file = st.file_uploader(
            "📁 Cargar desde JSON", type=["json"], key=f"worker_uploader_{st.session_state.file_upload_counter}"
        )
        if uploaded_file is not None:
            success, message = load_workers_from_file(uploaded_file)
            if success:
                st.success(message)
                st.session_state.file_upload_counter += 1
                st.rerun()
            else:
                st.error(message)

        # Guardar a archivo
        if len(st.session_state.workers_data) > 0:
            json_str = save_workers_to_file()
            st.download_button(
                label="💾 Descargar JSON",
                data=json_str,
                file_name=f"trabajadores_{datetime.now().strftime('%Y%m%d')}.json",
                mime="application/json",
            )

        # Limpiar todos
        if st.button("🗑️ Eliminar Todos los Médicos", type="secondary"):
            if st.session_state.workers_data:
                st.session_state.workers_data = []
                st.success("✅ Todos los trabajadores eliminados")
                st.rerun()

    # Lista de trabajadores
    st.markdown("---")
    st.subheader(f"📋 Médicos Configurados ({len(st.session_state.workers_data)})")

    if len(st.session_state.workers_data) > 0:
        for idx, worker in enumerate(st.session_state.workers_data):
            # Título del trabajador
            if worker.get("has_cadence"):
                title = (
                    f"👤 {worker['id']} - 🔁 Cadencia: cada {worker.get('cadence_days', 0)} días "
                    f"desde {worker.get('cadence_start_date', '?')}"
                )
            elif worker.get("auto_calculate_shifts", True):
                title = f"👤 {worker['id']} - Objetivo: 🔄 Automático ({worker.get('work_percentage', 1):.0f}%)"
            else:
                guardias_mes = worker.get("target_shifts", 0)
                title = f"👤 {worker['id']} - Constraint: {guardias_mes} guardias/mes"

            with st.expander(title):
                col_info, col_actions = st.columns([3, 1])

                with col_info:
                    # Información básica
                    st.write(f"**Porcentaje jornada:** {worker.get('work_percentage', 1):.0f}%")

                    # Mostrar objetivo de turnos claramente
                    if worker.get("has_cadence"):
                        st.write(
                            f"**🔁 Cadencia:** guardia cada {worker.get('cadence_days', 0)} días, "
                            f"empezando el {worker.get('cadence_start_date', '?')} "
                            "(obligatoria e inamovible; sin guardias adicionales)"
                        )
                    elif worker.get("auto_calculate_shifts", True):
                        st.write("**🔄 Guardias objetivo:** Se calculará automáticamente según el período")
                    else:
                        st.write(
                            f"**📊 Constraint:** {worker.get('target_shifts', 0)} guardias/mes (calculado para todo el período)"
                        )

                    # Período personalizado
                    if worker.get("custom_start_date") or worker.get("custom_end_date"):
                        start = worker.get("custom_start_date", "N/A")
                        end = worker.get("custom_end_date", "N/A")
                        st.write(f"**Período personalizado:** {start} → {end}")

                    # Incompatibilidades
                    if worker.get("is_incompatible"):
                        st.write("**Incompatibilidad:** ⚠️ Incompatible con otros trabajadores marcados")
                    elif worker.get("incompatible_with"):
                        st.write(f"**Incompatible con:** {', '.join(worker['incompatible_with'])}")
                    if worker.get("no_last_post"):
                        st.write("**Puesto Rosell:** 🚫 No asignar Rosell")
                    elif worker.get("only_last_post"):
                        st.write("**Puesto Rosell:** 🎯 Solo Rosell")

                    # Días obligatorios
                    mandatory_days_str = worker.get("mandatory_days", "")
                    if mandatory_days_str:
                        mandatory_list_display = [d.strip() for d in mandatory_days_str.split(";") if d.strip()]
                        mandatory_count = len(mandatory_list_display)
                        st.write(f"**✅ Días obligatorios:** {mandatory_count} día(s)")
                        if mandatory_count <= 5:
                            st.write(f"   {', '.join(mandatory_list_display)}")
                        else:
                            st.write(f"   {', '.join(mandatory_list_display[:5])} ... y {mandatory_count - 5} más")

                    # Días fuera
                    days_off_str = worker.get("days_off", "")
                    if days_off_str:
                        days_off_list_display = [d.strip() for d in days_off_str.split(";") if d.strip()]
                        days_off_count = len(days_off_list_display)
                        st.write(f"**❌ Días fuera:** {days_off_count} día(s)")
                        if days_off_count <= 5:
                            st.write(f"   {', '.join(days_off_list_display)}")
                        else:
                            st.write(f"   {', '.join(days_off_list_display[:5])} ... y {days_off_count - 5} más")

                with col_actions:
                    col_edit, col_del = st.columns(2)
                    with col_edit:
                        if st.button("✏️ Editar", key=f"edit_{idx}"):
                            # Cargar datos del trabajador en session_state para edición
                            # Usar claves _buffer para evitar conflicto con widgets existentes
                            st.session_state.worker_id_buffer = worker["id"]
                            st.session_state.work_percentage_buffer = worker.get("work_percentage", 100)
                            st.session_state.auto_calc_buffer = worker.get("auto_calculate_shifts", True)

                            # Guardias/mes si no es automático
                            if not worker.get("auto_calculate_shifts", True):
                                st.session_state.guardias_mes_buffer = worker.get("target_shifts", 4)

                            # Cadencia
                            st.session_state.has_cadence_buffer = worker.get("has_cadence", False)
                            st.session_state.cadence_days_buffer = worker.get("cadence_days", 1) or 1
                            cadence_start_str = worker.get("cadence_start_date", "")
                            if cadence_start_str:
                                try:
                                    st.session_state.cadence_start_date_buffer = datetime.strptime(
                                        cadence_start_str, "%d-%m-%Y"
                                    ).date()
                                except ValueError:
                                    st.session_state.cadence_start_date_buffer = None
                            else:
                                st.session_state.cadence_start_date_buffer = None

                            # Parsear work_periods de string a formato normal
                            work_periods_str = worker.get("work_periods", "")
                            if work_periods_str:
                                st.session_state.work_periods_buffer = work_periods_str.replace(";", "\n")
                            else:
                                st.session_state.work_periods_buffer = ""

                            # Incompatibilidades
                            st.session_state.incompatible_buffer = worker.get("is_incompatible", False)
                            st.session_state.incompatible_with_buffer = worker.get("incompatible_with", [])
                            st.session_state.no_last_post_buffer = worker.get("no_last_post", False)
                            st.session_state.only_last_post_buffer = worker.get("only_last_post", False)

                            # Días obligatorios
                            mandatory_days_str = worker.get("mandatory_days", "")
                            if mandatory_days_str:
                                st.session_state.mandatory_dates_buffer = mandatory_days_str.replace(";", "\n")
                            else:
                                st.session_state.mandatory_dates_buffer = ""

                            # Días fuera
                            days_off_str = worker.get("days_off", "")
                            if days_off_str:
                                st.session_state.days_off_buffer = days_off_str.replace(";", "\n")
                            else:
                                st.session_state.days_off_buffer = ""

                            # Establecer modo de edición y resetear flag de buffers
                            st.session_state.editing_worker = worker["id"]
                            st.session_state.buffers_loaded = False  # Para que se carguen los buffers
                            st.rerun()

                    with col_del:
                        if st.button("🗑️ Eliminar", key=f"del_{idx}"):
                            st.session_state.workers_data.pop(idx)
                            st.success(f"✅ {worker['id']} eliminado")
                            st.rerun()
    else:
        st.info("ℹ️ No hay trabajadores configurados. Agregue trabajadores usando el formulario arriba.")

# ==================== TAB 2: CALENDARIO ====================
with tab2:
    st.header("📅 Calendario Generado")

    if st.session_state.schedule is None:
        st.info("ℹ️ No hay calendario generado. Use el botón '🚀 Generar Calendario' en la barra lateral.")
    else:
        # Obtener DataFrame
        df = get_schedule_dataframe()

        if df is not None:
            # Métricas rápidas
            col1, col2, col3, col4 = st.columns(4)

            # Denominator: use the scheduler's configured expectation (num_shifts per
            # date over the full range), NOT the dict's actual list lengths, because
            # the dict can be missing keys or have shorter lists for transient reasons.
            # This correctly gives 184 * 4 = 736 when there are no variable_shifts,
            # and still accounts for variable_shifts days when they exist.
            _sched_obj = st.session_state.scheduler  # Scheduler instance
            assert _sched_obj is not None
            _sched_raw = st.session_state.schedule
            total_possible = sum(
                _sched_obj._get_shifts_for_date(d)
                for d in _sched_obj._get_date_range(_sched_obj.start_date, _sched_obj.end_date)
            )
            total_slots = sum(sum(1 for w in shifts if w is not None) for shifts in _sched_raw.values())
            coverage = (total_slots / total_possible * 100) if total_possible > 0 else 0

            with col1:
                st.metric("Días programados", len(df))
            with col2:
                st.metric("Guardias cubiertas", f"{total_slots}/{total_possible}")
            with col3:
                st.metric("Cobertura", f"{coverage:.1f}%")
            with col4:
                # Contar PDFs generados
                _pdf_count = len(list(Path(".").glob("*.pdf")))
                st.metric("PDFs generados", _pdf_count)

            st.markdown("---")

            # Tabla del calendario
            st.subheader("📋 Calendario Detallado")
            st.dataframe(df, width="stretch", height=600, hide_index=True)

            # Descargar como CSV
            csv = df.to_csv(index=False).encode("utf-8")

            # Obtener fechas del scheduler
            if st.session_state.scheduler:
                config = st.session_state.scheduler.config
                start = config["start_date"]
                end = config["end_date"]
                filename = f"calendario_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.csv"
            else:
                filename = f"calendario_{datetime.now().strftime('%Y%m%d')}.csv"

            st.download_button(label="📥 Descargar Calendario (CSV)", data=csv, file_name=filename, mime="text/csv")

            # seccion de descarga de PDFs
            st.markdown("---")
            st.subheader("📄 Reportes PDF")

            # Selector de tipo de reporte
            report_type = st.radio(
                "Tipo de reporte:",
                ["Resumen Ejecutivo (Global)", "Calendario Visual Completo", "Estadísticas y Desglose Detallado"],
                help="Seleccione el formato de documento que desea generar",
            )

            # Importar PDFExporter de forma segura
            _pdf_exporter: type | None = None
            try:
                from saldo27.pdf_exporter import PDFExporter

                _pdf_exporter = PDFExporter
            except ImportError:
                st.error("Error: No se encontró el módulo pdf_exporter.py")

            if st.button("📄 Generar Informe PDF", type="primary"):
                if st.session_state.scheduler and _pdf_exporter is not None:
                    with st.spinner(f"Generando {report_type}..."):
                        try:
                            # Configuración común para el exportador
                            scheduler = st.session_state.scheduler
                            config = {
                                "schedule": scheduler.schedule,
                                "workers_data": scheduler.workers_data,
                                "num_shifts": scheduler.num_shifts,
                                "holidays": scheduler.holidays,
                            }
                            exporter = _pdf_exporter(config)
                            filename = None

                            if report_type == "Resumen Ejecutivo (Global)":
                                filename = exporter.export_summary_pdf(build_summary_pdf_stats_data(scheduler))

                            elif report_type == "Calendario Visual Completo":
                                # Genera todos los meses en horizontal
                                filename = exporter.export_all_months_calendar()

                            elif report_type == "Estadísticas y Desglose Detallado":
                                # Genera reporte específico de estadísticas
                                filename = exporter.export_worker_statistics()

                            if filename:
                                st.success(f"✅ Informe generado: {filename}")
                                st.rerun()
                            else:
                                st.error("No se pudo generar el archivo (nombre de archivo vacío)")

                        except Exception as e:
                            st.error(f"Error al generar PDF: {e}")
                            logging.error(f"PDF Export Error: {e}", exc_info=True)
                else:
                    if not st.session_state.scheduler:
                        st.warning("⚠️ Primero debe generar un calendario")

            st.markdown("##### Descargas Disponibles")

            _pdf_paths: list[Path] = list(Path(".").glob("*.pdf"))
            available_pdfs = cast("list[Path]", sorted(_pdf_paths, key=lambda p: p.stat().st_mtime, reverse=True))
            if available_pdfs:
                for pdf_file in available_pdfs:
                    col_del, col_down = st.columns([0.2, 0.8])
                    # No delete button for simplicity, just download list
                    with open(pdf_file, "rb") as f:
                        file_label = (
                            f"📥 {pdf_file.name} ({datetime.fromtimestamp(pdf_file.stat().st_mtime).strftime('%H:%M')})"
                        )
                        st.download_button(
                            label=file_label,
                            data=f.read(),
                            file_name=pdf_file.name,
                            mime="application/pdf",
                            key=f"dl_{pdf_file.name}",
                        )
            else:
                st.info("ℹ️ No se encontraron archivos PDF generados")


# ==================== TAB 3: ESTADÍSTICAS (reparto pre-Ajuste Final) ====================
with tab3:
    _pre_fa = st.session_state.get("schedule_pre_fa")
    _fa_done = st.session_state.get("fa_pending_result") is not None
    if st.session_state.scheduler is None:
        st.header("📊 Estadísticas de Asignación")
        st.info("ℹ️ No hay calendario generado. Use el botón '🚀 Generar Reparto' en la barra lateral.")
    elif _fa_done and _pre_fa is not None:
        with temporary_schedule_snapshot(_pre_fa):
            render_assignment_statistics(
                caption="📌 Datos del reparto **antes** de ejecutar el Ajuste Final.",
                key_prefix="stats_pre_fa",
            )
    else:
        render_assignment_statistics(
            caption="📌 Aún no se ha ejecutado el Ajuste Final: estas cifras coinciden con el calendario actual.",
            key_prefix="stats_current",
        )

# ==================== TAB 4: AJUSTE FINAL ====================
with tab4:
    st.header("⚖️ Ajuste Final")
    st.caption(
        "Ciclo de reequilibrio post-generación: optimiza la distribución de turnos, "
        "fines de semana y puentes respetando todas las restricciones. "
        "El calendario y las estadísticas de esta pestaña reflejan el resultado **después** del ajuste."
    )

    _fa_pending_result = st.session_state.get("fa_pending_result")
    if _fa_pending_result is not None:
        render_final_adjustment_result(_fa_pending_result)

    if st.button("⚖️ Ejecutar Ajuste Final", type="secondary", key="btn_final_adjustment"):
        _sched_fa = st.session_state.scheduler
        _refreshed_pdfs: list[str] = []
        _pdf_refresh_errors: list[str] = []
        if _sched_fa is None or _sched_fa.schedule_builder is None:
            st.warning("⚠️ El calendario debe estar completamente generado antes de ejecutar el ajuste final.")
        else:
            with st.spinner("Ejecutando ajuste final… esto puede tardar unos segundos."):
                try:
                    from saldo27.final_adjustment_engine import FinalAdjustmentEngine

                    _schedule_before_fa = copy.deepcopy(_sched_fa.schedule)
                    # Freeze pre-FA schedule for the Estadísticas tab.
                    st.session_state.schedule_pre_fa = copy.deepcopy(_schedule_before_fa)

                    engine = FinalAdjustmentEngine(_sched_fa)
                    _before_metrics = engine.compute_metrics()
                    _fa_results = engine.run(max_iterations=300)
                    _after_metrics = _fa_results["after"]
                    _fa_stats = _fa_results["stats"]
                    _total_swaps = (
                        _fa_stats["shift_swaps"]
                        + _fa_stats["weekend_swaps"]
                        + _fa_stats["bridge_swaps"]
                        + _fa_stats.get("weekend_last_post_swaps", 0)
                        + _fa_stats.get("ortools_reassignments", 0)
                    )
                    _fa_changes = compute_schedule_changes(_schedule_before_fa, _sched_fa.schedule)

                    if _total_swaps > 0:
                        try:
                            from saldo27.pdf_exporter import PDFExporter

                            _refreshed_pdfs, _pdf_refresh_errors = refresh_generated_report_pdfs(_sched_fa, PDFExporter)
                        except Exception as _pdf_exc:
                            _pdf_refresh_errors.append(str(_pdf_exc))

                except Exception as _fa_exc:
                    st.error(f"Error en el ajuste final: {_fa_exc}")
                    logging.error("FinalAdjustmentEngine error", exc_info=True)
                    _fa_results = None
                finally:
                    st.session_state.schedule = _sched_fa.schedule

            if _fa_results is not None:
                try:
                    _fa_new_log_text = build_generation_summary_log(_sched_fa)
                except Exception:
                    logging.error("Error recalculating summary log after Ajuste Final", exc_info=True)
                    _fa_new_log_text = None
                if _fa_new_log_text:
                    st.session_state._sidebar_log_content = ("code", _fa_new_log_text)

                st.session_state.fa_pending_result = {
                    "total_swaps": _total_swaps,
                    "fa_stats": _fa_stats,
                    "refreshed_pdfs": _refreshed_pdfs,
                    "pdf_refresh_errors": _pdf_refresh_errors,
                    "before_metrics": _before_metrics,
                    "after_metrics": _after_metrics,
                    "changes": _fa_changes,
                }
                st.rerun()

    st.markdown("---")
    st.subheader("📊 Estadísticas tras el Ajuste Final")
    if st.session_state.scheduler is None:
        st.info("ℹ️ No hay calendario generado.")
    elif st.session_state.get("fa_pending_result") is None:
        st.info("ℹ️ Ejecute el Ajuste Final para ver aquí el calendario y las estadísticas resultantes.")
    else:
        render_assignment_statistics(
            title_suffix="tras Ajuste Final",
            caption="📌 Datos del reparto **después** del Ajuste Final (calendario activo).",
            key_prefix="stats_post_fa",
        )

# ==================== TAB 5: VERIFICACIÓN ====================
with tab5:
    render_constraint_verification()

# ==================== TAB 6: PREDICTIVE ANALYTICS ====================
with tab6:
    st.header("🔮 Predictive Analytics")

    if not st.session_state.predictive_enabled:
        st.info(
            "ℹ️ Predictive analytics is disabled. Enable it in the sidebar to access AI-powered forecasting and recommendations."
        )
    elif st.session_state.scheduler is None:
        st.info("ℹ️ No hay reparto generado. Generate a schedule first to access predictive analytics.")
    else:
        # Insights Summary
        st.subheader("💡 Key Insights")
        insights = get_predictive_insights()

        if insights:
            for insight in insights:
                if insight["type"] == "success":
                    st.success(f"**{insight['title']}**: {insight['message']}")
                elif insight["type"] == "warning":
                    st.warning(f"**{insight['title']}**: {insight['message']}")
                elif insight["type"] == "info":
                    st.info(f"**{insight['title']}**: {insight['message']}")
        else:
            st.info("No insights available yet. Generate more schedules to build historical data.")

        st.markdown("---")

        # Demand Forecasting
        st.subheader("📈 Demand Forecasting")

        col_forecast_btn, col_forecast_info = st.columns([1, 2])

        with col_forecast_btn:
            if st.button("🔮 Generate Forecasts", type="primary"):
                with st.spinner("Generating demand forecasts..."):
                    success, message, forecasts = generate_demand_forecasts()
                    if success:
                        st.success(message)
                    else:
                        st.error(message)

        with col_forecast_info:
            if st.session_state.demand_forecasts:
                method = st.session_state.demand_forecasts.get("method", "unknown")
                st.caption(f"📊 Forecast method: {method}")

        # Display forecasts
        if st.session_state.demand_forecasts:
            forecasts = st.session_state.demand_forecasts

            if "daily_demand" in forecasts:
                st.markdown("**Predicted Daily Demand (Next 30 Days)**")

                # Create forecast chart
                forecast_data = pd.DataFrame(
                    {
                        "Day": list(range(1, len(forecasts["daily_demand"]) + 1)),
                        "Predicted Demand": forecasts["daily_demand"],
                    }
                )

                fig = px.line(forecast_data, x="Day", y="Predicted Demand", title="Demand Forecast", markers=True)
                fig.update_layout(height=400)
                st.plotly_chart(fig, width="stretch")

                # Statistics
                col_avg, col_max, col_min = st.columns(3)
                with col_avg:
                    st.metric(
                        "Average Demand", f"{sum(forecasts['daily_demand']) / len(forecasts['daily_demand']):.1f}"
                    )
                with col_max:
                    st.metric("Peak Demand", f"{max(forecasts['daily_demand']):.1f}")
                with col_min:
                    st.metric("Minimum Demand", f"{min(forecasts['daily_demand']):.1f}")

        st.markdown("---")

        st.markdown("---")

        # What-If Simulator
        st.subheader("🧪 Simulador de Escenarios (What-If)")
        st.info("Simula cambios en la demanda o recursos sin afectar el calendario actual.")

        with st.expander("⚙️ Configurar Escenario", expanded=True):
            col_sim_1, col_sim_2 = st.columns(2)

            with col_sim_1:
                st.markdown("#### 👥 Recursos (Médicos)")
                sim_extra_workers = st.number_input(
                    "Modificar plantilla de médicos (+/-)",
                    min_value=-5,
                    max_value=5,
                    value=0,
                    help="Positivo: Contratar extra. Negativo: Eliminar/Bajas. (Ej: -1 elimina un médico)",
                )

                # Date Range for WORKERS simulation
                st.caption("Periodo afectado (Médicos)")
                col_wd1, col_wd2 = st.columns(2)
                with col_wd1:
                    sim_workers_start = st.date_input(
                        "Desde (L-D)", value=None, key="sim_w_start", help="Calendario: Lunes a Domingo"
                    )
                with col_wd2:
                    sim_workers_end = st.date_input(
                        "Hasta (L-D)", value=None, key="sim_w_end", help="Calendario: Lunes a Domingo"
                    )

            with col_sim_2:
                st.markdown("#### 🏥 Demanda (Guardias)")
                sim_shift_change = st.number_input(
                    "Cambio en guardias/día (+/-)",
                    min_value=-10,
                    max_value=10,
                    value=0,
                    step=1,
                    help="Ej: +1 aumenta 1 guardia/día en el periodo seleccionado (o todo el periodo si no se define)",
                )

                # Date Range for SHIFTS simulation
                st.caption("Periodo afectado (Guardias)")
                col_sd1, col_sd2 = st.columns(2)
                with col_sd1:
                    sim_shifts_start = st.date_input(
                        "Desde (L-D)", value=None, key="sim_s_start", help="Calendario: Lunes a Domingo"
                    )
                with col_sd2:
                    sim_shifts_end = st.date_input(
                        "Hasta (L-D)", value=None, key="sim_s_end", help="Calendario: Lunes a Domingo"
                    )

            run_simulation_clicked = st.button("🚀 Ejecutar Simulación", type="primary")

        if run_simulation_clicked:
            with st.spinner("Ejecutando simulación de escenario..."):
                try:
                    # 1. Clonar configuración actual
                    sim_config = st.session_state.config.copy()

                    # Asegurar que start_date y end_date están en la configuración
                    # (Vienen de variables locales del sidebar, no siempre están en session_state.config)
                    # Convertir a datetime puro si son fechas de streamlit (date)
                    if isinstance(start_date, datetime):
                        sim_config["start_date"] = start_date
                    else:
                        sim_config["start_date"] = datetime.combine(start_date, datetime.min.time())

                    if isinstance(end_date, datetime):
                        sim_config["end_date"] = end_date
                    else:
                        sim_config["end_date"] = datetime.combine(end_date, datetime.min.time())

                    sim_workers = copy.deepcopy(st.session_state.workers_data)

                    # 2. Aplicar modificaciones

                    # === MODIFICACIONES DE MÉDICOS ===
                    sim_workers_period_str = ""
                    if sim_workers_start and sim_workers_end:
                        sim_workers_period_str = (
                            f"{sim_workers_start.strftime('%d-%m-%Y')} - {sim_workers_end.strftime('%d-%m-%Y')}"
                        )

                    if sim_extra_workers > 0:
                        # AÑADIR trabajadores
                        for i in range(sim_extra_workers):
                            new_worker = {
                                "id": f"SIM_DOC_{i + 1}",
                                "target_shifts": 0,
                                "work_percentage": 100,
                                "auto_calculate_shifts": True,
                                "mandatory_days": "",
                                "days_off": "",
                                "incompatible_with": [],
                            }
                            # Si hay periodo, limitar periodo de trabajo
                            if sim_workers_period_str:
                                new_worker["work_periods"] = sim_workers_period_str

                            sim_workers.append(new_worker)

                    elif sim_extra_workers < 0:
                        # QUITAR trabajadores
                        num_to_remove = abs(sim_extra_workers)

                        if sim_workers_period_str:
                            # Si hay periodo definido, NO eliminamos, sino que añadimos days_off (Baja temporal)
                            # Afectamos a los últimos workers de la lista (simulando que son los que 'sobran' o aleatorios)
                            target_workers = sim_workers[-num_to_remove:]
                            for w in target_workers:
                                current_off = w.get("days_off", "")
                                if current_off:
                                    w["days_off"] = f"{current_off}; {sim_workers_period_str}"
                                else:
                                    w["days_off"] = sim_workers_period_str
                        else:
                            # Si NO hay periodo, eliminación total
                            # Eliminamos los últimos de la lista para no romper IDs complejos si es posible
                            if len(sim_workers) >= num_to_remove:
                                sim_workers = sim_workers[:-num_to_remove]
                            else:
                                sim_workers = []  # Eliminar todos si pide quitar más de los que hay

                    # === MODIFICACIONES DE TURNOS (VARIABLE SHIFTS) ===
                    # Ajustar turnos por día (Variable Shifts)
                    if sim_shift_change != 0:
                        # Determinar rango de fechas afectado para TURNOS
                        if sim_shifts_start and sim_shifts_end:
                            range_start = datetime.combine(sim_shifts_start, datetime.min.time())
                            range_end = datetime.combine(sim_shifts_end, datetime.min.time())
                        else:
                            range_start = sim_config["start_date"]
                            range_end = sim_config["end_date"]

                        # Mapa actual de variable_shifts
                        existing_var_shifts = {}
                        for vs in sim_config.get("variable_shifts", []):
                            # El formato en config es {'start_date': ..., 'end_date': ..., 'shifts': ...}
                            # Asumimos rangos de 1 día como genera la UI por defecto
                            d = vs.get("start_date")
                            s = vs.get("shifts")
                            if d and s is not None:
                                existing_var_shifts[d] = s
                        base_shifts = sim_config["num_shifts"]

                        # Aplicar cambios
                        current_date = range_start
                        while current_date <= range_end:
                            # Obtener valor actual para este día (o base si no existe específico)
                            # Normalizar fecha a datetime sin hora para coincidencia
                            d_key = current_date

                            current_val = existing_var_shifts.get(d_key, base_shifts)
                            new_val = max(0, current_val + sim_shift_change)
                            existing_var_shifts[d_key] = new_val

                            current_date += timedelta(days=1)

                        # Reconstruir lista variable_shifts
                        sim_config["variable_shifts"] = [
                            {"start_date": d, "end_date": d, "shifts": n} for d, n in existing_var_shifts.items()
                        ]

                    sim_config["workers_data"] = sim_workers

                    # Add safety flag for simulation
                    sim_config["is_simulation"] = True

                    # 3. Generar horario simulado (sin guardar en session_state)
                    sim_request = GenerateScheduleRequest(
                        start_date=sim_config["start_date"],
                        end_date=sim_config["end_date"],
                        holidays=sim_config.get("holidays", []),
                        variable_shifts=sim_config.get("variable_shifts", []),
                        workers_data=sim_workers,
                        config=sim_config,
                    )
                    sim_result = run_simulation(sim_request)
                    success = sim_result.success
                    sim_scheduler = sim_result.scheduler

                    if success and sim_scheduler is not None:
                        st.success("✅ Simulación completada")

                        # 4. Comparar resultados
                        # Calcular métricas escenario BASE
                        base_scheduler = st.session_state.scheduler
                        base_uncovered = base_scheduler.num_shifts * (
                            (base_scheduler.end_date - base_scheduler.start_date).days + 1
                        ) - sum(len(v) for v in base_scheduler.schedule.values())
                        # Nota: La métrica de 'uncovered' real depende de si hay huecos.
                        # Asumimos que el scheduler intenta llenar todo.
                        # Una mejor métrica es "Desviación Media"

                        # Calcular métricas escenario SIMULADO
                        sim_uncovered = sim_scheduler.num_shifts * (
                            (sim_scheduler.end_date - sim_scheduler.start_date).days + 1
                        ) - sum(len(v) for v in sim_scheduler.schedule.values())

                        # Mostrar Comparativa
                        st.subheader("📊 Resultados Comparativos")

                        # Helper for avg shifts/month
                        def calc_avg_shifts_month(sch):
                            # Get stats
                            stats_data = sch.stats.calculate_statistics()
                            # Get all workers stats
                            workers_stats = stats_data.get("workers", {})

                            if not workers_stats:
                                return 0

                            # Calculate total assigned shifts
                            total_assigned = sum(w.get("total_shifts", 0) for w in workers_stats.values())

                            # Calculate months duration
                            days = (sch.end_date - sch.start_date).days + 1
                            months = days / 30.44  # Approx avg month length
                            if months < 1:
                                months = 1

                            # Average per worker per month
                            total_workers = len(workers_stats)
                            if total_workers == 0:
                                return 0

                            return (total_assigned / total_workers) / months

                        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                        with col_m1:
                            st.metric(
                                "Total Médicos",
                                len(sim_workers),
                                delta=len(sim_workers) - len(st.session_state.workers_data),
                            )
                        with col_m2:
                            base_avg_month = calc_avg_shifts_month(base_scheduler)
                            sim_avg_month = calc_avg_shifts_month(sim_scheduler)
                            st.metric(
                                "Guardias/Mes (Avg)",
                                f"{sim_avg_month:.1f}",
                                delta=f"{sim_avg_month - base_avg_month:.1f}",
                            )
                        with col_m3:
                            # Calcular desviación promedio absoluta
                            def calc_avg_dev(sch):
                                stats_data = sch.stats.calculate_statistics()
                                workers_stats = stats_data.get("workers", {})
                                if not workers_stats:
                                    return 0

                                total_dev = 0
                                count = 0
                                for w_id, w_data in workers_stats.items():
                                    dev = w_data.get("total_shifts", 0) - w_data.get("target_shifts", 0)
                                    total_dev += abs(dev)
                                    count += 1

                                return total_dev / count if count > 0 else 0

                            base_dev = calc_avg_dev(base_scheduler)
                            sim_dev = calc_avg_dev(sim_scheduler)

                            st.metric(
                                "Desviación Promedio",
                                f"{sim_dev:.2f}",
                                delta=f"{base_dev - sim_dev:.2f}",
                                delta_color="inverse",
                            )
                            # Nota: delta positivo en equidad es malo si significa más desviación, por eso inverse

                        # Visualización de Impacto
                        st.caption("Una desviación promedio menor indica un reparto más equitativo.")

                    else:
                        st.error("La simulación no pudo encontrar una solución viable con estos parámetros.")

                except Exception as e:
                    st.error(f"Error en simulación: {e!s}")
                    logging.error(traceback.format_exc())

        st.markdown("---")

        # Optimization Recommendations
        st.subheader("🎯 Optimization Recommendations")

        if st.button("🔄 Refresh Recommendations"):
            with st.spinner("Analyzing schedule..."):
                get_optimization_recommendations()
                st.rerun()

        recommendations = st.session_state.optimization_recommendations

        if recommendations:
            st.info(f"Found {len(recommendations)} recommendations")

            # Group by priority
            high_priority = [r for r in recommendations if r.get("priority") == "high"]
            medium_priority = [r for r in recommendations if r.get("priority") == "medium"]
            low_priority = [r for r in recommendations if r.get("priority") == "low"]

            if high_priority:
                st.markdown("**🔴 High Priority**")
                for rec in high_priority:
                    st.error(f"• {rec['message']}")

            if medium_priority:
                st.markdown("**🟡 Medium Priority**")
                for rec in medium_priority:
                    st.warning(f"• {rec['message']}")

            if low_priority:
                st.markdown("**🟢 Low Priority**")
                for rec in low_priority:
                    st.info(f"• {rec['message']}")
        else:
            st.success("✅ No optimization recommendations. Schedule looks good!")

        st.markdown("---")

        # Historical Analysis
        st.subheader("📊 Historical Analysis")

        with st.expander("View Historical Trends"):
            st.markdown("**Schedule Performance Over Time**")
            st.caption("Historical data will be available after generating multiple schedules.")

            # Placeholder for historical charts
            st.info("ℹ️ Historical analysis requires multiple schedule generations to build trend data.")

# ==================== TAB 7: REVISIÓN ====================
with tab7:
    st.header("🔍 Revisión de Calendario")
    st.markdown("Cargue un archivo (PDF, Excel o CSV) para analizarlo y generar reportes.")

    # Sección 1: Carga de archivo
    st.subheader("📂 Carga de Archivo")

    # Opción A: Seleccionar archivo local del directorio
    local_extensions = (".pdf", ".xlsx", ".xls", ".csv")
    local_files = sorted(
        [f.name for f in Path(".").iterdir() if f.is_file() and f.name.lower().endswith(local_extensions)]
    )

    col_local, col_upload, col_info = st.columns([2, 2, 1])

    with col_local:
        st.markdown("**Opción A: Archivo del directorio**")
        selected_local = st.selectbox(
            "Archivos disponibles",
            options=["(ninguno)"] + local_files,
            index=0,
            help="Archivos PDF, Excel o CSV encontrados en el directorio de trabajo",
        )
        if selected_local != "(ninguno)" and st.button("📂 Cargar archivo local", key="btn_load_local_file"):
            try:
                processor = CalendarFileProcessor()
                calendar_text = processor.process_local_file(Path(".") / selected_local)
                st.session_state.revision_calendar_text = calendar_text
                st.success(f"✅ Archivo «{selected_local}» cargado correctamente")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error procesando archivo: {e!s}")
                st.session_state.revision_calendar_text = ""

    with col_upload:
        st.markdown("**Opción B: Subir archivo**")
        uploaded_file = st.file_uploader(
            "Seleccione archivo (PDF, Excel, CSV)",
            type=["pdf", "xlsx", "xls", "csv"],
            help="Archivo con guardias a analizar",
        )

    with col_info:
        st.markdown("**Formatos soportados:**")
        st.caption("• PDF\n• Excel (.xlsx, .xls)\n• CSV")

    if uploaded_file is not None:
        # Only process if file changed (avoid re-processing on every rerun)
        file_id = f"{uploaded_file.name}_{uploaded_file.size}"
        if file_id != st.session_state.get("_revision_last_file_id"):
            try:
                # Procesar archivo subido
                processor = CalendarFileProcessor()
                calendar_text = processor.process_file(uploaded_file)
                st.session_state.revision_calendar_text = calendar_text
                st.session_state._revision_last_file_id = file_id
                st.success("✅ Archivo subido cargado correctamente")
            except Exception as e:
                st.error(f"❌ Error procesando archivo: {e!s}")
                st.session_state.revision_calendar_text = ""

    # Preview del texto extraído (funciona para ambas fuentes)
    if st.session_state.revision_calendar_text:
        with st.expander("👁️ Preview del contenido extraído"):
            preview_text = st.session_state.revision_calendar_text
            st.text_area(
                "Contenido del archivo:",
                value=preview_text[:500] + "..." if len(preview_text) > 500 else preview_text,
                height=150,
                disabled=True,
            )

    # Sección 2: Configuración
    if st.session_state.revision_calendar_text:
        st.markdown("---")
        st.subheader("⚙️ Configuración")

        col1, col2, col3 = st.columns(3)

        with col1:
            start_date_revision = st.date_input(
                "Fecha inicial del reparto (L-D)",
                value=datetime.now(),
                format="DD/MM/YYYY",
                help="Fecha de inicio del calendario cargado. Calendario de Lunes a Domingo",
            )

        with col2:
            shifts_per_day_revision = st.number_input(
                "Guardias por día", min_value=1, max_value=10, value=4, help="Número de guardias por día"
            )

        with col3:
            st.markdown("**Festivos cargados del sistema:**")
            # Obtener festivos del sidebar
            holidays_from_sidebar = st.session_state.get("sidebar_holidays", [])
            if holidays_from_sidebar:
                st.caption(f"✅ {len(holidays_from_sidebar)} festivos configurados")
                with st.expander("👁️ Ver/Editar festivos"):
                    # Mostrar festivos en formato DD-MM-YYYY
                    holidays_text = "\n".join([h.strftime("%d-%m-%Y") for h in holidays_from_sidebar])
                    st.text_area(
                        "Festivos (uno por línea, formato: DD-MM-YYYY)",
                        value=holidays_text,
                        height=100,
                        disabled=True,
                        help="Para editar, use la sección 'Período de Reparto' en el Sidebar",
                    )
                    st.info(
                        "💡 Para agregar o modificar festivos, vaya al **Sidebar** → **Período de Reparto** → **🎉 Festivos**"
                    )
            else:
                st.warning("⚠️ No hay festivos configurados")
                st.info("💡 Configure festivos en el **Sidebar** → **Período de Reparto** → **🎉 Festivos**")

        # Sección de mapeo de nombres
        st.markdown("**Mapeo de nombres (opcional)**")
        st.caption("Solo si desea expandir abreviaturas o cambiar nombres")
        st.info("💡 Los nombres compuestos como 'LUIS H' se detectan automáticamente. NO use guiones.")

        name_mapping_text = st.text_area(
            "Formato: CORTO=COMPLETO (uno por línea, completamente opcional)",
            value="",
            height=80,
            help="Ej: Si en archivo dice 'MAR' y quiere que aparezca como 'MARÍA', ingrese: MAR=MARÍA",
        )

        # Procesar mapeo de nombres
        name_mapping = {}
        for line in name_mapping_text.strip().split("\n"):
            line = line.strip()
            if "=" in line and line:
                short, long = line.split("=", 1)
                name_mapping[short.strip()] = long.strip()

        st.session_state.revision_name_mapping = name_mapping

        # Sección 3: Análisis
        st.markdown("---")
        st.subheader("🔍 Análisis")

        if st.button("🚀 Analizar Reparto", key="btn_analyze_schedule"):
            try:
                with st.spinner("Analizando ..."):
                    # Convertir start_date a datetime si es date object
                    if isinstance(start_date_revision, date) and not isinstance(start_date_revision, datetime):
                        start_datetime = datetime.combine(start_date_revision, datetime.min.time())
                    else:
                        start_datetime = start_date_revision

                    # Convertir holidays a datetime si son date objects
                    holidays_datetime = []
                    for h in holidays_from_sidebar:
                        if isinstance(h, date) and not isinstance(h, datetime):
                            holidays_datetime.append(datetime.combine(h, datetime.min.time()))
                        else:
                            holidays_datetime.append(h)

                    logging.info(f"Analizando calendario con {len(holidays_datetime)} festivos")
                    logging.info(f"Festivos: {[h.strftime('%Y-%m-%d') for h in holidays_datetime]}")

                    # Crear analizador
                    analyzer = ScheduleAnalyzer(
                        start_date=start_datetime,
                        calendar_text=st.session_state.revision_calendar_text,
                        name_mapping=name_mapping,
                        holidays=holidays_datetime,
                        shifts_per_day=shifts_per_day_revision,
                    )

                    logging.info(f"Períodos de puente detectados: {len(analyzer.bridge_periods)}")
                    for bp in analyzer.bridge_periods:
                        logging.info(
                            f"  Puente {bp['type']}: {bp['start_date'].strftime('%Y-%m-%d')} a {bp['end_date'].strftime('%Y-%m-%d')}"
                        )

                    # Parsear y calcular estadísticas
                    analyzer.parse_calendar()
                    df_stats = analyzer.calculate_statistics()
                    alerts = analyzer.get_alerts()
                    display_scheduler = build_revision_display_scheduler(analyzer)

                    # Guardar en session state
                    st.session_state.revision_stats = df_stats  # formato legacy (PDF de revisión)
                    st.session_state.revision_alerts = alerts
                    st.session_state.revision_analyzer = analyzer
                    st.session_state.revision_display_scheduler = display_scheduler

                    st.success("✅ Análisis completado")

            except Exception as e:
                st.error(f"❌ Error en análisis: {e!s}")
                logging.error(f"Analysis error: {e}")

        # Mostrar resultados si existen
        if st.session_state.revision_stats is not None:
            st.markdown("---")
            _rev_display = None
            _rev_analyzer = st.session_state.get("revision_analyzer")
            # Snapshot flag sources BEFORE swapping the active scheduler for display.
            # temporary_active_scheduler replaces session.scheduler with the revision
            # display object, so generation-scheduler flags would otherwise be lost.
            _gen_workers = []
            _live = st.session_state.get("scheduler")
            if _live is not None and getattr(_live, "workers_data", None):
                _gen_workers = list(_live.workers_data)
            _flag_source = collect_rosell_flag_sources(extra_sources=[_gen_workers])
            _flag_hits = sum(
                1
                for w in _flag_source
                if worker_flag_enabled(w, "no_last_post") or worker_flag_enabled(w, "only_last_post")
            )

            # Siempre reconstruir con la plantilla/reparto actual.
            if _rev_analyzer is not None:
                try:
                    _rev_display = build_revision_display_scheduler(_rev_analyzer, workers_config=_flag_source)
                    sync_rosell_flags_from_plantilla(_rev_display, _flag_source)
                    st.session_state.revision_display_scheduler = _rev_display
                except Exception as _rev_build_exc:
                    logging.error("No se pudo reconstruir el scheduler de revisión: %s", _rev_build_exc)
                    _rev_display = st.session_state.get("revision_display_scheduler")
                    if _rev_display is not None:
                        sync_rosell_flags_from_plantilla(_rev_display, _flag_source)
            else:
                _rev_display = st.session_state.get("revision_display_scheduler")
                if _rev_display is not None:
                    sync_rosell_flags_from_plantilla(_rev_display, _flag_source)

            if _rev_display is not None:
                if _flag_hits == 0:
                    st.warning(
                        "⚠️ No hay flags Rosell (no_last_post / only_last_post) en la plantilla "
                        "ni en el reparto activo. Obj. Rosell usará el reparto proporcional. "
                        "Cargue los médicos configurados o genere/importe un reparto con esos flags."
                    )
                with temporary_active_scheduler(_rev_display):
                    render_assignment_statistics(
                        title_suffix="Revisión",
                        caption=(
                            "📌 Mismo formato que Estadísticas / Ajuste Final. "
                            "Turnos: objetivo = guardias detectadas. "
                            "Rosell: no_last_post → Obj. 0; only_last_post → Obj. 100% "
                            f"(flags detectados en plantilla/reparto: {_flag_hits})."
                        ),
                        key_prefix="stats_revision",
                        workers_config=_flag_source,
                    )
                    st.session_state.revision_stats_ui = get_worker_statistics(workers_config=_flag_source)

                    st.markdown("---")
                    # Score with generation objectives (not display "detectadas" targets).
                    _rev_score = compute_schedule_score_breakdown(
                        _rev_display,
                        scoring_workers=_gen_workers if _gen_workers else None,
                    )
                    st.session_state.revision_score = _rev_score
                    render_schedule_score(
                        breakdown=_rev_score,
                        title="🎯 Score del Reparto — Revisión",
                        caption=(
                            "Mismo score que el reparto (cobertura 35 %, carga 25 %, FDS 10 %, "
                            "puentes 5 %, puestos 15 %, penalización 10 %). Objetivos de scoring "
                            "desde plantilla/reparto activo; violaciones por mandatory/cadencia "
                            "y gaps dentro de períodos variables no penalizan."
                        ),
                    )

                    st.markdown("---")
                    _rev_violations = check_violations(_rev_display)
                    render_constraint_verification(
                        violations=_rev_violations,
                        title="⚠️ Verificación de Restricciones — Revisión",
                        caption=(
                            "Mismo formato que la pestaña Verificación. "
                            "Se aplica al calendario analizado usando la plantilla/reparto "
                            "cargados (gaps, incompatibilidades, Rosell, days_off, etc.)."
                        ),
                    )
            else:
                st.warning("⚠️ No se pudieron reconstruir las estadísticas en el formato estándar.")

            # Mostrar alertas de guardias consecutivas en CAJA SEPARADA
            st.markdown("---")
            st.subheader("⚠️ Alerta de Guardias Consecutivas")

            # Obtener alertas del analyzer (guardado en session_state)
            alerts_df = st.session_state.get("revision_alerts")

            if alerts_df is not None and not alerts_df.empty:
                # Mostrar cada alerta en una caja de warning
                for _, alert_row in alerts_df.iterrows():
                    worker = alert_row["Médico"]
                    fecha1 = alert_row["Fecha 1"]
                    fecha2 = alert_row["Fecha 2"]
                    st.warning(f"**{worker}**: Guardias consecutivas los días {fecha1} y {fecha2}")
            else:
                st.success("✅ No hay guardias consecutivas detectadas")

            # Dobletes: guardias con Gap = 2 (un día libre entre ambas)
            st.markdown("---")
            st.subheader("🔁 Dobletes (Gap = 2)")

            _doblete_analyzer = st.session_state.get("revision_analyzer")
            _dobletes = []
            if _doblete_analyzer and hasattr(_doblete_analyzer, "calendar_data") and _doblete_analyzer.calendar_data:
                _date_workers: dict = {}
                for _day in _doblete_analyzer.calendar_data:
                    _date_workers[_day["date"]] = set(_day.get("workers", []))

                for _d in sorted(_date_workers.keys()):
                    _d2 = _d + timedelta(days=2)
                    if _d2 in _date_workers:
                        for _w in _date_workers[_d] & _date_workers[_d2]:
                            _dobletes.append(
                                {
                                    "Médico": _w,
                                    "Fecha 1": _d.strftime("%d-%m-%Y"),
                                    "Fecha 2": _d2.strftime("%d-%m-%Y"),
                                }
                            )

            if _dobletes:
                for _db in _dobletes:
                    st.info(f"**{_db['Médico']}**: Doblete los días {_db['Fecha 1']} y {_db['Fecha 2']} (Gap = 2)")
            else:
                st.success("✅ No hay dobletes detectados")

            # Exportación
            st.markdown("---")
            st.subheader("📥 Exportación")

            exp_col1, exp_col2, exp_col3 = st.columns(3)

            _export_stats = st.session_state.get("revision_stats_ui")
            if _export_stats is None:
                _export_stats = st.session_state.revision_stats

            with exp_col1:
                # Exportar CSV (mismo formato que el panel de Estadísticas cuando esté disponible)
                csv_data = _export_stats.to_csv(index=False)
                st.download_button(
                    label="📊 Descargar Estadísticas (CSV)",
                    data=csv_data,
                    file_name=f"estadisticas_guardias_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )

            with exp_col2:
                # Exportar PDF (sigue usando el DataFrame legacy del analizador)
                if st.button("📄 Generar Reporte PDF"):
                    try:
                        with st.spinner("Generando PDF..."):
                            generator = PDFReportGenerator()
                            pdf_content = generator.generate_report(
                                st.session_state.revision_analyzer,
                                st.session_state.revision_stats,
                                include_charts=False,
                            )

                            st.download_button(
                                label="⬇️ Descargar Reporte PDF",
                                data=pdf_content,
                                file_name=f"reporte_guardias_{datetime.now().strftime('%Y%m%d')}.pdf",
                                mime="application/pdf",
                            )
                            st.success("✅ PDF generado exitosamente")
                    except Exception as e:
                        st.error(f"❌ Error generando PDF: {e!s}")

            with exp_col3:
                # Exportar Excel
                buffer = io.BytesIO()
                with pd.ExcelWriter(cast("Any", buffer), engine="openpyxl") as writer:
                    _export_stats.to_excel(writer, sheet_name="Estadísticas", index=False)
                    if st.session_state.revision_alerts is not None and not st.session_state.revision_alerts.empty:
                        st.session_state.revision_alerts.to_excel(writer, sheet_name="Alertas", index=False)

                buffer.seek(0)
                st.download_button(
                    label="📑 Descargar Excel",
                    data=buffer.getvalue(),
                    file_name=f"guardias_analisis_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
    else:
        st.info("ℹ️ Cargue un archivo de reparto para comenzar el análisis")

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: gray;'>"
    "Sistema de Generación de Guardias v3.6 | "
    "Interfaz Streamlit | "
    f"Todos los derechos reservados, 2026©,  {datetime.now().year}"
    "</div>",
    unsafe_allow_html=True,
)
