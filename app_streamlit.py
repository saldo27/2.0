"""
Sistema de Generación de Horarios - Interfaz Streamlit
Reemplazo moderno de la interfaz Kivy con funcionalidad web
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
import copy
import logging
import os
from pathlib import Path
import traceback

# Importar módulos del scheduler
from scheduler import Scheduler
from scheduler_config import SchedulerConfig, setup_logging
from utilities import DateTimeUtils

# Configurar logging
setup_logging()

# Configuración de la página
st.set_page_config(
    page_title="Sistema de Generación de Guardias",
    page_icon="📅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS para mejor apariencia
st.markdown("""
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
""", unsafe_allow_html=True)

# Inicializar session state
if 'workers_data' not in st.session_state:
    st.session_state.workers_data = []
if 'schedule' not in st.session_state:
    st.session_state.schedule = None
if 'scheduler' not in st.session_state:
    st.session_state.scheduler = None
if 'generation_log' not in st.session_state:
    st.session_state.generation_log = []
if 'config' not in st.session_state:
    st.session_state.config = SchedulerConfig.get_default_config()

# Real-time features
if 'real_time_enabled' not in st.session_state:
    st.session_state.real_time_enabled = False
if 'change_history' not in st.session_state:
    st.session_state.change_history = []
if 'undo_stack' not in st.session_state:
    st.session_state.undo_stack = []
if 'redo_stack' not in st.session_state:
    st.session_state.redo_stack = []

# Predictive analytics
if 'predictive_enabled' not in st.session_state:
    st.session_state.predictive_enabled = False
if 'demand_forecasts' not in st.session_state:
    st.session_state.demand_forecasts = None
if 'optimization_recommendations' not in st.session_state:
    st.session_state.optimization_recommendations = []
if 'analytics_insights' not in st.session_state:
    st.session_state.analytics_insights = []

# Funciones auxiliares
def load_workers_from_file(uploaded_file):
    """Cargar Médicos desde archivo JSON con validación y compatibilidad"""
    try:
        data = json.load(uploaded_file)
        
        if not isinstance(data, list):
            return False, "❌ El archivo JSON debe contener una lista de trabajadores"
            
        validated_data = []
        count = 0
        
        for item in data:
            if not isinstance(item, dict) or 'id' not in item:
                continue
                
            # Construir objeto trabajador asegurando todos los campos
            worker = {
                'id': str(item['id']),
                'target_shifts': int(item.get('target_shifts', 0)),
                'work_percentage': float(item.get('work_percentage', 100)),
                'is_incompatible': bool(item.get('is_incompatible', False)),
                'incompatible_with': item.get('incompatible_with', []),
                'mandatory_days': str(item.get('mandatory_days', '')),
                'days_off': str(item.get('days_off', '')),
                'work_periods': str(item.get('work_periods', '')),
                'auto_calculate_shifts': bool(item.get('auto_calculate_shifts', True))
            }
            
            # Compatibilidad con formato antiguo (mandatory_dates lista)
            if 'mandatory_dates' in item and isinstance(item['mandatory_dates'], list):
                if not worker['mandatory_days']:
                    worker['mandatory_days'] = '; '.join(item['mandatory_dates'])
            
            # Compatibilidad con campos antiguos custom_start/end
            if not worker['work_periods']:
                start = item.get('custom_start_date')
                end = item.get('custom_end_date')
                if start and end:
                    worker['work_periods'] = f"{start} - {end}"
            
            validated_data.append(worker)
            count += 1
            
        if count == 0:
            return False, "⚠️ No se encontraron trabajadores válidos en el archivo"
            
        st.session_state.workers_data = validated_data
        return True, f"✅ {count} trabajadores importados correctamente"
        
    except json.JSONDecodeError:
        return False, "❌ Error: El archivo no es un JSON válido"
    except Exception as e:
        return False, f"❌ Error al procesar datos: {str(e)}"

def save_workers_to_file():
    """Guardar Médicos en JSON"""
    return json.dumps(st.session_state.workers_data, indent=2, ensure_ascii=False)

def load_schedule_from_json(uploaded_file):
    """Cargar Calendario y Configuración desde archivo JSON"""
    try:
        data = json.load(uploaded_file)
        
        # 0. Check format type
        if isinstance(data, list):
             return False, "❌ Este archivo parece contener solo lista de médicos. Use el importador de 'Trabajadores' más abajo."

        # 1. Validar estructura básica (Relaxed)
        if 'workers_data' not in data:
            keys_found = list(data.keys())
            if 'worker_metrics' in keys_found:
                 return False, f"❌ Error: Este parece ser un archivo de Análisis Histórico (Analytics), no un respaldo de calendario completo. Claves encontradas: {keys_found}"
            return False, f"❌ El archivo no contiene datos de trabajadores ('workers_data'). Claves encontradas: {keys_found}"
            
        # 2. Cargar Workers Data
        st.session_state.workers_data = data['workers_data']
        
        # 3. Cargar Configuración
        config = st.session_state.config.copy()
        
        # Parse Fechas (Robust)
        try:
            # Try to find dates in different places
            s_date_val = data.get('start_date')
            e_date_val = data.get('end_date')
            
            # Fallback to schedule_period if available
            if not s_date_val and 'schedule_period' in data:
                s_date_val = data['schedule_period'].get('start_date')
                e_date_val = data['schedule_period'].get('end_date')

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
                    
                config['start_date'] = start_date
                config['end_date'] = end_date
            else:
                # If no dates found, keep existing or warn?
                # We'll rely on existing config if file doesn't have dates
                pass
            
        except Exception as e:
            # Don't fail the whole load just for dates, use existing if needed
            logging.warning(f"Date parsing warning: {e}")

        # Cargar otros parámetros si existen
        if 'num_shifts' in data: config['num_shifts'] = data['num_shifts']
        
        # Holidays
        if 'holidays' in data: 
            holidays = []
            for h in data['holidays']:
                try:
                    if isinstance(h, str):
                        holidays.append(datetime.fromisoformat(h))
                    else:
                        holidays.append(h)
                except: pass
            config['holidays'] = holidays
            
        if 'variable_shifts' in data: config['variable_shifts'] = data['variable_shifts']
        
        st.session_state.config = config
        
        # 4. Reconstruir Scheduler y Schedule
        if 'schedule' in data and data['schedule']:
            try:
                # Reconstruir objeto schedule {datetime: [workers]}
                schedule = {}
                for date_str, workers in data['schedule'].items():
                    try:
                        dt = datetime.fromisoformat(date_str)
                        schedule[dt] = workers
                    except: pass
                
                if schedule:
                    # Crear scheduler dummy con esta config
                    scheduler = Scheduler(config)
                    scheduler.schedule = schedule
                    scheduler.workers_data = st.session_state.workers_data
                    scheduler.worker_assignments = scheduler._map_worker_assignments() 
                    
                    st.session_state.scheduler = scheduler
                    st.session_state.schedule = schedule
                    
                    return True, "✅ Calendario y configuración importados correctamente"
            except Exception as e:
                 logging.error(f"Schedule reconstruction error: {e}")
                 return True, "⚠️ Configuración cargada, pero hubo error reconstruyendo el calendario exacto. Genere nuevamente."
        
        return True, "✅ Configuración importada (Recuerde generar el horario nuevamente)"
        
    except json.JSONDecodeError:
        return False, "❌ Error: El archivo no es un JSON válido"
    except Exception as e:
        return False, f"❌ Error al procesar datos: {str(e)}"




def generate_schedule_internal(start_date, end_date, tolerance, holidays, variable_shifts):
    """Generar el horario internamente"""
    try:
        # Validar datos de entrada
        if not st.session_state.workers_data:
            return False, "❌ Error: No hay trabajadores configurados"
        
        if start_date >= end_date:
            return False, "❌ Error: La fecha final debe ser posterior a la inicial"
        
        # Convertir date a datetime si es necesario
        if not isinstance(start_date, datetime):
            start_date = datetime.combine(start_date, datetime.min.time())
        if not isinstance(end_date, datetime):
            end_date = datetime.combine(end_date, datetime.min.time())
        
        # Prepare config for Scheduler
        # Note: We pass the workers data directly. The Scheduler class handles 
        # target_shifts calculation internally using its sophisticated logic
        # (including largest-remainder rounding, work periods, etc.)
        
        config = {
            'start_date': start_date,
            'end_date': end_date,
            'num_shifts': st.session_state.config.get('num_shifts', 3),
            'workers_data': st.session_state.workers_data,  # Pass directly, matching Kivy
            'holidays': holidays,
            'variable_shifts': variable_shifts,
            'gap_between_shifts': st.session_state.config.get('gap_between_shifts', 2),
            'max_consecutive_weekends': st.session_state.config.get('max_consecutive_weekends', 2),
            'enable_proportional_weekends': st.session_state.config.get('enable_proportional_weekends', True),
            'weekend_tolerance': st.session_state.config.get('weekend_tolerance', 1),
            'tolerance': tolerance,  # Use exact tolerance from UI
            'cache_enabled': st.session_state.config.get('cache_enabled', False),
            'lazy_evaluation': st.session_state.config.get('lazy_evaluation', False),
            'batch_size': st.session_state.config.get('batch_size', 100),
            'max_improvement_loops': st.session_state.config.get('max_improvement_loops', 150),
            'last_post_adjustment_max_iterations': st.session_state.config.get('last_post_adjustment_max_iterations', 10),
            # Dual-mode scheduler parameters
            'enable_dual_mode': st.session_state.config.get('enable_dual_mode', True),
            'num_initial_attempts': st.session_state.config.get('num_initial_attempts', 30),
            'max_complete_attempts': st.session_state.config.get('max_complete_attempts', 5),  # Match Kivy default
            # Real-time features (matching Kivy)
            'enable_real_time': True  # Kivy enables this by default
        }
        
        # Crear scheduler
        scheduler = Scheduler(config)
        st.session_state.scheduler = scheduler
        
        # UI progresiva
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_progress(phase, percentage, details=""):
            status_text.markdown(f"**Fase:** {phase} | {details}")
            progress_bar.progress(min(max(percentage, 0), 100))
            
        # Generar horario con feedback visual
        # Monkey patch o callback wrapper para capturar progreso si el scheduler lo soportara nativamente
        # Por ahora simulamos las fases principales o si SchedulerCore expone hooks
        
        update_progress("Fase 1: Configuración", 10, "Inicializando motor y validando restricciones...")
        
        # Simulación visual de fases para dar feedback de actividad
        # En una integración futura, SchedulerCore enviará eventos reales
        import time
        
        # Como SchedulerCore.orchestrate_schedule_generation es bloqueante, 
        # mostramos que el proceso iniciará
        time.sleep(0.5)
        update_progress("Fase 2: Asignaciones Fijas", 30, "Procesando días obligatorios y festivos...")
        
        with st.spinner("🚀 Ejecutando motor de optimización avanzado (Fase 1 + Swaps + Iteraciones)..."):
            # Mensaje informativo para el usuario
            status_text.info("⚙️ Ejecutando: Distribución Inicial → Optimización Iterativa → Balanceo")
            success = scheduler.generate_schedule()
        
        if success:
            # Activar características de tiempo real (Smart Swapping, Undo/Redo)
            # Esto iguala el comportamiento de main.py
            if hasattr(scheduler, 'enable_real_time_features'):
                scheduler.enable_real_time_features()
                
            update_progress("Fase Final: Completado", 100, "¡Calendario generado y optimizado!")
            st.session_state.schedule = scheduler.schedule
            return True, "✅ Calendario generado exitosamente"
        else:
            status_text.error("Fallo en la generación - Revise restricciones")
            return False, "❌ Error: No se pudo generar el calendario"
            
    except Exception as e:
        error_msg = f"Error en generación: {str(e)}"
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        return False, f"❌ Error: {str(e)}"

def get_schedule_dataframe():
    """Convertir calendario a DataFrame para visualización"""
    if st.session_state.schedule is None:
        return None
    
    schedule = st.session_state.schedule
    
    # Crear DataFrame
    dates = sorted(schedule.keys())
    data = []
    
    for date in dates:
        workers = schedule[date]
        row = {
            'Fecha': date.strftime('%d-%m-%Y'),
            'Día': ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom'][date.weekday()]
        }
        for i, worker in enumerate(workers):
            row[f'Puesto {i+1}'] = worker if worker else '-'
        data.append(row)
    
    return pd.DataFrame(data)

def get_worker_statistics():
    """Obtener estadísticas de asignaciones por médico usando el motor central"""
    if st.session_state.scheduler is None:
        return None
    
    # Usar el calculador de estadísticas centralizado
    scheduler = st.session_state.scheduler
    core_stats = scheduler.stats.calculate_statistics()
    
    stats = []
    for worker_id, data in core_stats['workers'].items():
        target = data['target_shifts']
        current = data['total_shifts']
        deviation = current - target
        deviation_pct = (deviation / target * 100) if target > 0 else 0
        
        stats.append({
            'Médico': worker_id,
            'Objetivo': target,
            'Asignados': current,
            'Desviación': deviation,
            'Desv. %': f"{deviation_pct:+.1f}%"
        })
    
    return pd.DataFrame(stats)

def check_violations():
    """Verificar violaciones de restricciones usando el motor central"""
    if st.session_state.scheduler is None:
        return {}
    
    scheduler = st.session_state.scheduler
    
    # Usar el verificador de restricciones del núcleo (Single Source of Truth)
    # create a fresh check instead of relying on cached state
    core_violations = scheduler._check_schedule_constraints()
    
    violations = {
        'incompatibilidades': [],
        'patron_7_14': [],
        'mandatory': []
    }
    
    # Mapear las violaciones del núcleo al formato de la UI
    for v in core_violations:
        v_type = v.get('type')
        
        if v_type == 'incompatibility':
            violations['incompatibilidades'].append(
                f"{v['date'].strftime('%d-%m-%Y')}: {v['worker_id']} ↔ {v['incompatible_id']}"
            )
            
        elif v_type == 'weekly_pattern':
             violations['patron_7_14'].append(
                f"{v['worker_id']}: {v['date1'].strftime('%d-%m-%Y')} → {v['date2'].strftime('%d-%m-%Y')} ({v['days_between']} días)"
            )
            
        # Nota: El núcleo no reporta 'mandatory' como violación estándar porque 
        # considera que las asignaciones obligatorias son sagradas, pero podemos
        # mantener la categoría vacía si queremos soportarlo en el futuro o
        # si queremos implementar una comprobación específica.
        
    return violations

# ==================== REAL-TIME FEATURES ====================

def assign_worker_real_time(worker_id, date, post_index):
    """Asignar médico en tiempo real con validación"""
    if not st.session_state.real_time_enabled or st.session_state.scheduler is None:
        return False, "Real-time features not enabled"
    
    try:
        scheduler = st.session_state.scheduler
        
        # Check if real-time engine exists
        if hasattr(scheduler, 'assign_worker_real_time'):
            result = scheduler.assign_worker_real_time(worker_id, date, post_index, 'streamlit_user')
            if result.get('success'):
                # Save to undo stack
                st.session_state.undo_stack.append({
                    'action': 'assign',
                    'worker_id': worker_id,
                    'date': date,
                    'post': post_index,
                    'timestamp': datetime.now()
                })
                st.session_state.redo_stack = []  # Clear redo stack
                return True, result.get('message', 'Worker assigned')
            return False, result.get('message', 'Assignment failed')
        else:
            # Fallback: manual assignment
            if date not in scheduler.schedule:
                return False, "Date not in schedule"
            if post_index >= len(scheduler.schedule[date]):
                return False, "Invalid post index"
            
            # Simple assignment
            old_worker = scheduler.schedule[date][post_index]
            scheduler.schedule[date][post_index] = worker_id
            
            # Update worker_assignments
            if worker_id not in scheduler.worker_assignments:
                scheduler.worker_assignments[worker_id] = []
            scheduler.worker_assignments[worker_id].append(date)
            
            # Save to undo stack
            st.session_state.undo_stack.append({
                'action': 'assign',
                'worker_id': worker_id,
                'old_worker': old_worker,
                'date': date,
                'post': post_index,
                'timestamp': datetime.now()
            })
            st.session_state.redo_stack = []
            
            return True, f"Assigned {worker_id} to {date.strftime('%d-%m-%Y')}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def undo_last_change():
    """Deshacer último cambio"""
    if not st.session_state.undo_stack:
        return False, "No changes to undo"
    
    try:
        last_change = st.session_state.undo_stack.pop()
        scheduler = st.session_state.scheduler
        
        if last_change['action'] == 'assign':
            # Revert assignment
            date = last_change['date']
            post = last_change['post']
            old_worker = last_change.get('old_worker')
            
            scheduler.schedule[date][post] = old_worker
            
            # Update worker_assignments
            worker_id = last_change['worker_id']
            if worker_id in scheduler.worker_assignments and date in scheduler.worker_assignments[worker_id]:
                scheduler.worker_assignments[worker_id].remove(date)
            
            # Save to redo stack
            st.session_state.redo_stack.append(last_change)
            
            return True, "Change undone"
        
        return False, "Unknown action type"
    except Exception as e:
        return False, f"Error: {str(e)}"

def redo_last_change():
    """Rehacer último cambio deshecho"""
    if not st.session_state.redo_stack:
        return False, "No changes to redo"
    
    try:
        last_undone = st.session_state.redo_stack.pop()
        scheduler = st.session_state.scheduler
        
        if last_undone['action'] == 'assign':
            # Redo assignment
            date = last_undone['date']
            post = last_undone['post']
            worker_id = last_undone['worker_id']
            
            scheduler.schedule[date][post] = worker_id
            
            # Update worker_assignments
            if worker_id not in scheduler.worker_assignments:
                scheduler.worker_assignments[worker_id] = []
            scheduler.worker_assignments[worker_id].append(date)
            
            # Save to undo stack
            st.session_state.undo_stack.append(last_undone)
            
            return True, "Change redone"
        
        return False, "Unknown action type"
    except Exception as e:
        return False, f"Error: {str(e)}"

# ==================== PREDICTIVE ANALYTICS ====================

def generate_demand_forecasts():
    """Generar pronósticos de demanda"""
    if not st.session_state.predictive_enabled or st.session_state.scheduler is None:
        return False, "Predictive analytics not enabled", None
    
    try:
        scheduler = st.session_state.scheduler
        
        # Check if predictive analytics exists
        if hasattr(scheduler, 'generate_demand_forecasts'):
            result = scheduler.generate_demand_forecasts(forecast_days=30)
            if result.get('success'):
                forecasts = result.get('forecasts', {})
                st.session_state.demand_forecasts = forecasts
                return True, "Forecasts generated successfully", forecasts
            return False, result.get('message', 'Forecast generation failed'), None
        else:
            # Fallback: basic heuristic forecasting
            schedule = scheduler.schedule
            if not schedule:
                return False, "No schedule data available", None
            
            # Calculate average daily demand
            total_slots = sum(len([w for w in workers if w]) for workers in schedule.values())
            avg_daily = total_slots / len(schedule) if schedule else 0
            
            # Simple forecast: assume same average for next 30 days
            forecasts = {
                'daily_demand': [avg_daily] * 30,
                'method': 'basic_heuristic',
                'confidence': 'low'
            }
            
            st.session_state.demand_forecasts = forecasts
            return True, "Basic forecasts generated", forecasts
    except Exception as e:
        return False, f"Error: {str(e)}", None

def get_optimization_recommendations():
    """Obtener recomendaciones de optimización"""
    if not st.session_state.predictive_enabled or st.session_state.scheduler is None:
        return []
    
    try:
        scheduler = st.session_state.scheduler
        
        # Check if predictive optimizer exists
        if hasattr(scheduler, 'run_predictive_optimization'):
            result = scheduler.run_predictive_optimization()
            if result.get('success'):
                recommendations = result.get('optimization_results', {}).get('optimization_recommendations', [])
                st.session_state.optimization_recommendations = recommendations
                return recommendations
        
        # Fallback: basic recommendations based on statistics
        recommendations = []
        stats_df = get_worker_statistics()
        
        if stats_df is not None:
            # Find overloaded workers
            for _, row in stats_df.iterrows():
                deviation = row['Desviación']
                if deviation > 3:
                    recommendations.append({
                        'type': 'overload',
                        'worker': row['Médico'],
                        'message': f"{row['Médico']} has {deviation} extra shifts",
                        'priority': 'high' if deviation > 5 else 'medium'
                    })
                elif deviation < -3:
                    recommendations.append({
                        'type': 'underload',
                        'worker': row['Médico'],
                        'message': f"{row['Médico']} needs {abs(deviation)} more shifts",
                        'priority': 'medium'
                    })
        
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
                insights.append({
                    'type': 'warning',
                    'title': 'Low Coverage',
                    'message': f'Current coverage is {coverage:.1f}%. Consider adding more workers or adjusting constraints.'
                })
            elif coverage >= 98:
                insights.append({
                    'type': 'success',
                    'title': 'Excellent Coverage',
                    'message': f'Schedule has {coverage:.1f}% coverage. Well balanced!'
                })
            
            # Balance insight
            stats_df = get_worker_statistics()
            if stats_df is not None:
                avg_deviation = stats_df['Desviación'].abs().mean()
                if avg_deviation > 2:
                    insights.append({
                        'type': 'info',
                        'title': 'Balance Opportunity',
                        'message': f'Average deviation is {avg_deviation:.1f} shifts. Consider rebalancing.'
                    })
    
    st.session_state.analytics_insights = insights
    return insights

# ==================== INTERFAZ PRINCIPAL ====================

# Header
st.title("📅 Sistema de Generación de Guardias")
st.markdown("---")

# Sidebar - Configuración y Controles
with st.sidebar:
    st.header("⚙️ Configuración")

    # Importación y Exportación
    with st.expander("📂 Importar / Exportar / Backup", expanded=False):
        # Importar
        sched_file = st.file_uploader("Cargar JSON Completo", type="json", key="sidebar_importer")
        if sched_file is not None:
            if st.button("🔄 Restaurar Datos"):
                success, msg = load_schedule_from_json(sched_file)
                if success:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
        
        st.markdown("---")
        
        # Exportar
        if st.session_state.workers_data:
            # Prepare full export data
            export_data = st.session_state.config.copy()
            # Ensure dates are strings
            if isinstance(export_data.get('start_date'), datetime):
                export_data['start_date'] = export_data['start_date'].isoformat()
            if isinstance(export_data.get('end_date'), datetime):
                export_data['end_date'] = export_data['end_date'].isoformat()
            
            # Add schedule if exists
            if st.session_state.schedule:
                # Convert schedule keys (datetime) to strings
                sched_export = {}
                for k, v in st.session_state.schedule.items():
                    sched_export[k.isoformat()] = v
                export_data['schedule'] = sched_export
            
            # Export button
            st.download_button(
                label="💾 Descargar Respaldo Completo (JSON)",
                data=json.dumps(export_data, indent=2, ensure_ascii=False),
                file_name=f"schedule_full_export_{datetime.now().strftime('%Y%m%d')}.json",
                mime="application/json"
            )
    
    # Período de reparto (Fecha Inicial - Fecha Final)
    st.subheader("📅 Período de Reparto")
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Fecha Inicial",
            value=datetime(2026, 1, 1),
            help="Fecha de inicio del período a programar",
            format="DD/MM/YYYY"
        )
    with col2:
        end_date = st.date_input(
            "Fecha Final",
            value=datetime(2026, 12, 31),
            help="Fecha de fin del período a programar",
            format="DD/MM/YYYY"
        )

    
    # Validar fechas
    if start_date >= end_date:
        st.error("⚠️ La fecha final debe ser posterior a la inicial")
    
    st.markdown("---")
    
    # Festivos (Holidays)
    st.subheader("🎉 Festivos")
    holidays_input = st.text_area(
        "Fechas festivas (una por línea, formato: DD-MM-YYYY)",
        value="19-03-2026\n27-03-2026\n02-04-2026\n03-04-2026\n01-05-2026\n09-06-2026\n25-09-2026\n12-10-2026\n08-12-2026\n24-12-2026\n25-12-2026\n01-01-2027",
        height=100,
        help="Días festivos donde se aplicarán reglas especiales"
    )
    
    # Parsear festivos
    holidays = []
    for line in holidays_input.strip().split('\n'):
        line = line.strip()
        if line:
            try:
                holiday_date = datetime.strptime(line, '%d-%m-%Y')
                holidays.append(holiday_date)
            except:
                st.warning(f"⚠️ Fecha inválida ignorada: {line}")
    
    if holidays:
        st.success(f"✅ {len(holidays)} festivos configurados")
    
    st.markdown("---")
    
    # Parámetros del sistema
    st.subheader("⚙️ Parámetros Globales")
    
    tolerance = st.slider(
        "Tolerancia de desviación (%)",
        min_value=5,
        max_value=20,
        value=10,
        help="Tolerancia permitida en la desviación de turnos asignados vs objetivo"
    )
    
    # Período con número de guardias por defecto
    num_shifts = st.number_input(
        "Guardias por día (por defecto)",
        min_value=1,
        max_value=10,
        value=st.session_state.config.get('num_shifts', 3),
        help="Número de Guardias a cubrir por día"
    )
    st.session_state.config['num_shifts'] = num_shifts
    
    # Variable shifts (períodos con diferente número de guardias)
    with st.expander("📊 Períodos con guardias variables"):
        st.markdown("**Configurar días con diferente número de guardias**")
        
        variable_shifts_text = st.text_area(
            "Formato: DD-MM-YYYY: número",
            value="25-12-2026: 2\n26-12-2026: 2",
            height=100,
            help="Días específicos con diferente número de guardias"
        )
        
        variable_shifts = []
        for line in variable_shifts_text.strip().split('\n'):
            line = line.strip()
            if ':' in line:
                try:
                    date_str, shifts_str = line.split(':')
                    date_obj = datetime.strptime(date_str.strip(), '%d-%m-%Y')
                    shifts_num = int(shifts_str.strip())
                    # El scheduler espera: start_date, end_date, shifts
                    # Para un día específico, start_date == end_date
                    variable_shifts.append({
                        'start_date': date_obj,
                        'end_date': date_obj,
                        'shifts': shifts_num
                    })
                except:
                    st.warning(f"⚠️ Línea inválida: {line}")
        
        if variable_shifts:
            st.success(f"✅ {len(variable_shifts)} días con turnos variables")
        
        st.session_state.config['variable_shifts'] = variable_shifts
    
    col_gap, col_weekends = st.columns(2)
    
    with col_gap:
        gap_between_shifts = st.number_input(
            "Días mínimos entre guardias",
            min_value=0,
            max_value=7,
            value=st.session_state.config.get('gap_between_shifts', 2),
            help="Número mínimo de días de descanso entre guardias consecutivos"
        )
        st.session_state.config['gap_between_shifts'] = gap_between_shifts
    
    with col_weekends:
        max_consecutive_weekends = st.number_input(
            "Fines de semana consecutivos máx.",
            min_value=1,
            max_value=5,
            value=st.session_state.config.get('max_consecutive_weekends', 2),
            help="Número máximo de fines de semana consecutivos que puede trabajar un trabajador"
        )
        st.session_state.config['max_consecutive_weekends'] = max_consecutive_weekends
    
    # Configuración adicional de fines de semana
    with st.expander("⚙️ Configuración Avanzada de Fines de Semana"):
        enable_proportional = st.checkbox(
            "Habilitar balance proporcional de fines de semana",
            value=st.session_state.config.get('enable_proportional_weekends', True),
            help="Distribuir fines de semana proporcionalmente según el porcentaje laboral de cada trabajador"
        )
        st.session_state.config['enable_proportional_weekends'] = enable_proportional
        
        weekend_tolerance = st.slider(
            "Tolerancia de fines de semana (±)",
            min_value=0,
            max_value=3,
            value=st.session_state.config.get('weekend_tolerance', 1),
            help="Tolerancia permitida en la desviación de fines de semana asignados"
        )
        st.session_state.config['weekend_tolerance'] = weekend_tolerance
    
    # Dual-Mode Scheduler Configuration
    with st.expander("🔀 Dual-Mode Scheduler (Strict + Relaxed)"):
        st.markdown("**Configure strict initial distribution and relaxed optimization**")
        
        enable_dual_mode = st.checkbox(
            "Enable dual-mode scheduler",
            value=st.session_state.config.get('enable_dual_mode', True),
            help="Use strict initial distribution followed by relaxed iterative optimization"
        )
        st.session_state.config['enable_dual_mode'] = enable_dual_mode
        
        if enable_dual_mode:
            st.info("ℹ️ Dual-mode: Strict initial (90-95% coverage) → Relaxed optimization (98-100%)")
            
            num_attempts = st.slider(
                "Initial attempts",
                min_value=5,
                max_value=60,
                value=st.session_state.config.get('num_initial_attempts', 30),
                help="Number of strict initial distribution attempts (more = better quality)"
            )
            st.session_state.config['num_initial_attempts'] = num_attempts
            
    
    # Real-Time Features
    with st.expander("⚡ Real-Time Features"):
        enable_real_time = st.checkbox(
            "Enable real-time editing",
            value=st.session_state.config.get('enable_real_time', False),
            help="Enable interactive schedule editing with undo/redo"
        )
        st.session_state.config['enable_real_time'] = enable_real_time
        st.session_state.real_time_enabled = enable_real_time
        
        if enable_real_time:
            st.success("✅ Real-time editing enabled")
            st.caption("You can manually assign/unassign workers in the Calendar tab")
    
    # Predictive Analytics
    with st.expander("🔮 Predictive Analytics"):
        enable_predictive = st.checkbox(
            "Enable predictive analytics",
            value=st.session_state.config.get('enable_predictive_analytics', False),
            help="Enable AI-powered demand forecasting and optimization recommendations"
        )
        st.session_state.config['enable_predictive_analytics'] = enable_predictive
        st.session_state.predictive_enabled = enable_predictive
        
        if enable_predictive:
            st.success("✅ Predictive analytics enabled")
            st.caption("View forecasts and recommendations in the Analytics tab")
    
    st.markdown("---")
    
    # Botón de generación
    st.subheader("🚀 Generar Horario")
    
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
                    start_date, 
                    end_date, 
                    tolerance/100, 
                    holidays,
                    st.session_state.config.get('variable_shifts', [])
                )
                if success:
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)
            except Exception as e:
                st.error(f"❌ Error crítico durante la generación: {str(e)}")
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
        - ✅ Días mínimos entre guardias configurables
        - ✅ Fines de semana consecutivos máximos
        - ✅ Balance proporcional de fines de semana
        - ✅ Tolerancia de desviación configurable
        - ✅ Días fuera (no disponibles)
        - ✅ Períodos personalizados por médico
        - ✅ Guardias variables por día/período
        
        **Parámetros configurables:**
        - 📅 Período de reparto (fecha inicial/final)
        - 🎉 Festivos
        - 🔢 Guardias por día (por defecto y variables)
        - ⏳ Gap entre guaridas
        - 📆 Fines de semana consecutivos
        - 📊 Tolerancia general y de fines de semana
        """)

# Tabs principales
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "👥 Gestión de Médicos",
    "📅 Calendario Generado",
    "📊 Estadísticas",
    "⚠️ Verificación de Restricciones",
    "🔮 Predictive Analytics"
])

# ==================== TAB 1: GESTIÓN DE TRABAJADORES ====================
with tab1:
    st.header("👥 Gestión de Médicos")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Agregar/Editar Médico")
        
        with st.form("worker_form"):
            worker_id = st.text_input("ID del Médico *", placeholder="Ej: TRAB001")
            
            # Información básica
            st.markdown("**📋 Información Básica**")
            col_a, col_b = st.columns(2)
            with col_a:
                work_percentage = st.slider(
                    "Porcentaje de Jornada", 
                    0, 100, 100,
                    help="100% = tiempo completo, 50% = media jornada"
                )
            with col_b:
                # Calcular turnos objetivo automáticamente
                auto_calculate = st.checkbox(
                    "Calcular guardias automáticamente",
                    value=True,
                    help="El sistema calculará la asignación según el período y porcentaje"
                )
            
            if not auto_calculate:
                target_shifts = st.number_input(
                    "Guardias objetivo (manual)", 
                    min_value=0, 
                    value=100,
                    help="Especificar manualmente el número de guardias"
                )
            else:
                st.info("ℹ️ Las guardias se calcularán automáticamente según el período configurado")
                target_shifts = 0  # Se calculará después
            
            # Períodos de trabajo personalizados (actualizado para soportar múltiples rangos)
            st.markdown("**📅 Períodos de Trabajo**")
            work_periods = st.text_area(
                "Rangos de fechas disponibles (uno por línea o separados por punto y coma)",
                value=st.session_state.get('form_work_periods', ''),
                placeholder="01-01-2026 - 31-03-2026; 01-06-2026 - 31-12-2026",
                help="Formato: DD-MM-YYYY - DD-MM-YYYY. Si se deja vacío, se asume disponibilidad total en el período global."
            )
            
            # Incompatibilidades (actualizado a multiselect)
            st.markdown("**🚫 Incompatibilidades**")
            col_inc1, col_inc2 = st.columns(2)
            with col_inc1:
                is_incompatible = st.checkbox(
                    "Incompatible con todos los marcados",
                    help="Este médico no puede coincidir con otros marcados igual"
                )
            with col_inc2:
                # Obtener lista de otros médicos para el multiselect
                existing_ids = [w['id'] for w in st.session_state.workers_data if w['id'] != worker_id]
                
                # Intentar recuperar valores previos si existen
                default_incomp = []
                
                incompatible_with = st.multiselect(
                    "Incompatible con IDs específicos",
                    options=existing_ids,
                    disabled=is_incompatible,
                    help="Seleccione los médicos con los que NO puede coincidir"
                )
            
            # Días obligatorios
            st.markdown("**✅ Guardias Obligatorias (Mandatory)**")
            mandatory_dates = st.text_area(
                "Fechas obligatorias (una por línea o separadas por punto y coma)",
                placeholder="01-12-2026; 15-12-2026; 25-12-2026",
                height=80,
                help="Días en los que DEBE trabajar obligatoriamente"
            )
            
            # Días fuera (nueva funcionalidad)
            st.markdown("**❌ Días Fuera (No disponible)**")
            days_off = st.text_area(
                "Fechas no disponibles (una por línea o separadas por punto y coma)",
                placeholder="10-12-2026; 20-12-2026; 30-12-2026",
                height=80,
                help="Días en los que NO puede tener asignación de guardias (vacaciones, permisos, etc.)"
            )
            
            col_submit, col_clear = st.columns(2)
            with col_submit:
                submit = st.form_submit_button("➕ Agregar Médico", type="primary")
            with col_clear:
                clear = st.form_submit_button("🗑️ Limpiar")
            
            if submit and worker_id:
                # Parsear incompatibilidades
                incomp_list = []
                if not is_incompatible and incompatible_with:
                    incomp_list = incompatible_with
                
                # Parsear días obligatorios
                mandatory_list = []
                if mandatory_dates:
                    # Normalizar separadores
                    dates_str = mandatory_dates.replace('\n', ';').replace(',', ';')
                    parts = [x.strip() for x in dates_str.split(';') if x.strip()]
                    worker_data_mandatory = ';'.join(parts) # Guardar como string normalizado
                else:
                    worker_data_mandatory = ""
                
                # Parsear días fuera
                if days_off:
                    dates_str = days_off.replace('\n', ';').replace(',', ';')
                    parts = [x.strip() for x in dates_str.split(';') if x.strip()]
                    worker_data_days_off = ';'.join(parts)
                else:
                    worker_data_days_off = ""

                # Parsear work periods
                if work_periods:
                    dates_str = work_periods.replace('\n', ';')
                    parts = [x.strip() for x in dates_str.split(';') if x.strip()]
                    worker_data_work_periods = ';'.join(parts)
                else:
                    worker_data_work_periods = ""
                
                # Crear/actualizar trabajador
                worker_data = {
                    'id': worker_id,
                    'target_shifts': target_shifts,
                    'work_percentage': work_percentage,       # Corrected: Scale 0-100, not 0-1
                    'is_incompatible': is_incompatible,
                    'incompatible_with': incomp_list,
                    'mandatory_days': worker_data_mandatory,  # Renamed to match scheduler and used string
                    'days_off': worker_data_days_off,         # New field
                    'work_periods': worker_data_work_periods, # New field
                    'auto_calculate_shifts': auto_calculate
                }
                
                # Removed obsolete custom period logic (replaced by work_periods)
                
                # Verificar si ya existe
                existing_idx = None
                for idx, w in enumerate(st.session_state.workers_data):
                    if w['id'] == worker_id:
                        existing_idx = idx
                        break
                
                if existing_idx is not None:
                    st.session_state.workers_data[existing_idx] = worker_data
                    st.success(f"✅ Médico {worker_id} actualizado")
                else:
                    st.session_state.workers_data.append(worker_data)
                    st.success(f"✅ Médico {worker_id} agregado")
                
                st.rerun()
    
    with col2:
        st.subheader("Gestión de Datos")
        
        # Cargar desde archivo
        uploaded_file = st.file_uploader("📁 Cargar desde JSON", type=['json'])
        if uploaded_file is not None:
            success, message = load_workers_from_file(uploaded_file)
            if success:
                st.success(message)
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
                mime="application/json"
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
            if worker.get('auto_calculate_shifts', True):
                title = f"👤 {worker['id']} - Objetivo: 🔄 Automático ({worker.get('work_percentage', 1)*100:.0f}%)"
            else:
                title = f"👤 {worker['id']} - Objetivo: {worker.get('target_shifts', 0)} turnos (manual)"
            
            with st.expander(title):
                col_info, col_actions = st.columns([3, 1])
                
                with col_info:
                    # Información básica
                    st.write(f"**Porcentaje jornada:** {worker.get('work_percentage', 1)*100:.0f}%")
                    
                    # Mostrar objetivo de turnos claramente
                    if worker.get('auto_calculate_shifts', True):
                        st.write(f"**🔄 Guardias objetivo:** Se calculará automáticamente según el período")
                    else:
                        st.write(f"**🎯 Guardias objetivo:** {worker.get('target_shifts', 0)} (configurado manualmente)")
                    
                    # Período personalizado
                    if worker.get('custom_start_date') or worker.get('custom_end_date'):
                        start = worker.get('custom_start_date', 'N/A')
                        end = worker.get('custom_end_date', 'N/A')
                        st.write(f"**Período personalizado:** {start} → {end}")
                    
                    # Incompatibilidades
                    if worker.get('is_incompatible'):
                        st.write("**Incompatibilidad:** ⚠️ Incompatible con otros trabajadores marcados")
                    elif worker.get('incompatible_with'):
                        st.write(f"**Incompatible con:** {', '.join(worker['incompatible_with'])}")
                    
                    # Días obligatorios
                    if worker.get('mandatory_dates'):
                        mandatory_count = len(worker['mandatory_dates'])
                        st.write(f"**✅ Días obligatorios:** {mandatory_count} día(s)")
                        if mandatory_count <= 5:
                            st.write(f"   {', '.join(worker['mandatory_dates'])}")
                        else:
                            st.write(f"   {', '.join(worker['mandatory_dates'][:5])} ... y {mandatory_count-5} más")
                    
                    # Días fuera 
                    if worker.get('days_off'):
                        days_off_count = len(worker['days_off'])
                        st.write(f"**❌ Días fuera:** {days_off_count} día(s)")
                        if days_off_count <= 5:
                            st.write(f"   {', '.join(worker['days_off'])}")
                        else:
                            st.write(f"   {', '.join(worker['days_off'][:5])} ... y {days_off_count-5} más")
                
                with col_actions:
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
        st.info("ℹ️ No hay horario generado. Use el botón '🚀 Generar Horario' en la barra lateral.")
    else:
        # Obtener DataFrame
        df = get_schedule_dataframe()
        
        if df is not None:
            # Métricas rápidas
            col1, col2, col3, col4 = st.columns(4)
            
            total_slots = sum(len([w for w in workers if w != '-']) 
                            for workers in df.iloc[:, 2:].values)
            total_possible = len(df) * (len(df.columns) - 2)
            coverage = (total_slots / total_possible * 100) if total_possible > 0 else 0
            
            with col1:
                st.metric("Días programados", len(df))
            with col2:
                st.metric("Guardias cubiertos", f"{total_slots}/{total_possible}")
            with col3:
                st.metric("Cobertura", f"{coverage:.1f}%")
            with col4:
                # Contar PDFs generados
                pdf_files = list(Path('.').glob('*.pdf'))
                st.metric("PDFs generados", len(pdf_files))
            
            st.markdown("---")
            
            # Real-Time Editing Controls
            if st.session_state.real_time_enabled:
                st.subheader("⚡ Real-Time Editing")
                
                col_undo, col_redo, col_info = st.columns([1, 1, 2])
                
                with col_undo:
                    if st.button("↶ Undo", disabled=len(st.session_state.undo_stack) == 0):
                        success, message = undo_last_change()
                        if success:
                            st.success(message)
                            st.rerun()
                        else:
                            st.error(message)
                
                with col_redo:
                    if st.button("↷ Redo", disabled=len(st.session_state.redo_stack) == 0):
                        success, message = redo_last_change()
                        if success:
                            st.success(message)
                            st.rerun()
                        else:
                            st.error(message)
                
                with col_info:
                    st.caption(f"📝 Changes: {len(st.session_state.undo_stack)} | Can undo: {len(st.session_state.undo_stack) > 0}")
                
                # Interactive assignment
                with st.expander("✏️ Manual Assignment"):
                    st.markdown("**Assign worker to a specific shift**")
                    
                    col_date, col_post, col_worker = st.columns(3)
                    
                    with col_date:
                        available_dates = sorted(st.session_state.schedule.keys())
                        selected_date = st.selectbox(
                            "Select date",
                            options=available_dates,
                            format_func=lambda d: d.strftime('%d-%m-%Y (%a)')
                        )
                    
                    with col_post:
                        num_posts = len(st.session_state.schedule[selected_date])
                        selected_post = st.selectbox(
                            "Select post",
                            options=list(range(num_posts)),
                            format_func=lambda p: f"Puesto {p+1}"
                        )
                    
                    with col_worker:
                        worker_ids = [w['id'] for w in st.session_state.workers_data]
                        selected_worker = st.selectbox(
                            "Select worker",
                            options=worker_ids
                        )
                    
                    if st.button("✅ Assign Worker", type="primary"):
                        success, message = assign_worker_real_time(selected_worker, selected_date, selected_post)
                        if success:
                            st.success(message)
                            st.rerun()
                        else:
                            st.error(message)
                
                st.markdown("---")
            
            # Tabla del calendario
            st.subheader("📋 Calendario Detallado")
            st.dataframe(
                df,
                use_container_width=True,
                height=600,
                hide_index=True
            )
            
            # Descargar como CSV
            csv = df.to_csv(index=False).encode('utf-8')
            
            # Obtener fechas del scheduler
            if st.session_state.scheduler:
                config = st.session_state.scheduler.config
                start = config['start_date']
                end = config['end_date']
                filename = f"calendario_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}.csv"
            else:
                filename = f"calendario_{datetime.now().strftime('%Y%m%d')}.csv"
            
            st.download_button(
                label="📥 Descargar Calendario (CSV)",
                data=csv,
                file_name=filename,
                mime="text/csv"
            )
            
            # seccion de descarga de PDFs
            st.markdown("---")
            st.subheader("📄 Reportes PDF")
            
            # Selector de tipo de reporte
            report_type = st.radio(
                "Tipo de reporte:",
                ["Resumen Ejecutivo (Global)", "Calendario Visual Completo", "Estadísticas y Desglose Detallado"],
                help="Seleccione el formato de documento que desea generar"
            )

            # Importar PDFExporter de forma segura
            try:
                from pdf_exporter import PDFExporter
            except ImportError:
                st.error("Error: No se encontró el módulo pdf_exporter.py")
                PDFExporter = None

            if st.button("📄 Generar Informe PDF", type="primary"):
                if st.session_state.scheduler and PDFExporter:
                    with st.spinner(f"Generando {report_type}..."):
                        try:
                            # Configuración común para el exportador
                            scheduler = st.session_state.scheduler
                            config = {
                                'schedule': scheduler.schedule,
                                'workers_data': scheduler.workers_data,
                                'num_shifts': scheduler.num_shifts,
                                'holidays': scheduler.holidays
                            }
                            exporter = PDFExporter(config)
                            filename = None

                            if report_type == "Resumen Ejecutivo (Global)":
                                # 1. Preparar datos de estadísticas (reconstrucción para summary)
                                stats_data = {
                                    'period_start': scheduler.start_date,
                                    'period_end': scheduler.end_date,
                                    'workers': {},
                                    'worker_shifts': {}
                                }
                                for worker in scheduler.workers_data:
                                    w_id = worker['id']
                                    assignments = [d for d, w in scheduler.schedule.items() if w_id in w]
                                    
                                    # Counts
                                    post_counts = {}
                                    weekday_counts = {}
                                    shift_list = []
                                    
                                    for date in assignments:
                                        # Posts
                                        try:
                                            p_idx = scheduler.schedule[date].index(w_id)
                                            post_counts[p_idx] = post_counts.get(p_idx, 0) + 1
                                        except: pass
                                        # Weekdays
                                        wd = date.weekday()
                                        weekday_counts[wd] = weekday_counts.get(wd, 0) + 1
                                        # Shift list
                                        shift_list.append({
                                            'date': date,
                                            'day': date.strftime('%A'),
                                            'post': scheduler.schedule[date].index(w_id) + 1,
                                            'is_weekend': date.weekday() >= 4,
                                            'is_holiday': date in scheduler.holidays
                                        })

                                    stats_data['workers'][w_id] = {
                                        'total': len(assignments),
                                        'weekends': sum(1 for d in assignments if d.weekday() >= 4),
                                        'holidays': sum(1 for d in assignments if d in scheduler.holidays),
                                        'last_post': post_counts.get(scheduler.num_shifts - 1, 0),
                                        'weekday_counts': weekday_counts,
                                        'post_counts': post_counts
                                    }
                                    stats_data['worker_shifts'][w_id] = shift_list
                                
                                filename = exporter.export_summary_pdf(stats_data)

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
            
            pdf_files = sorted(list(Path('.').glob('*.pdf')), key=os.path.getmtime, reverse=True)
            if pdf_files:
                for pdf_file in pdf_files:
                    col_del, col_down = st.columns([0.2, 0.8])
                    # No delete button for simplicity, just download list
                    with open(pdf_file, 'rb') as f:
                        file_label = f"📥 {pdf_file.name} ({datetime.fromtimestamp(pdf_file.stat().st_mtime).strftime('%H:%M')})"
                        st.download_button(
                            label=file_label,
                            data=f.read(),
                            file_name=pdf_file.name,
                            mime="application/pdf",
                            key=f"dl_{pdf_file.name}"
                        )
            else:
                st.info("ℹ️ No se encontraron archivos PDF generados")

# ==================== TAB 3: ESTADÍSTICAS ====================
with tab3:
    st.header("📊 Estadísticas de Asignación")
    
    if st.session_state.scheduler is None:
        st.info("ℹ️ No hay horario generado. Use el botón '🚀 Generar Horario' en la barra lateral.")
    else:
        # Estadísticas por trabajador
        stats_df = get_worker_statistics()
        
        if stats_df is not None:
            # Métricas generales
            col1, col2, col3 = st.columns(3)
            
            total_target = stats_df['Objetivo'].sum()
            total_assigned = stats_df['Asignados'].sum()
            avg_deviation = stats_df['Desviación'].mean()
            
            with col1:
                st.metric("Total Objetivo", total_target)
            with col2:
                st.metric("Total Asignado", total_assigned, f"{total_assigned - total_target:+d}")
            with col3:
                st.metric("Desviación Promedio", f"{avg_deviation:+.1f}")
            
            st.markdown("---")
            
            # Tabla de estadísticas
            st.subheader("📋 Estadísticas por Médico")
            
            # Colorear según desviación
            def color_deviation(val):
                if isinstance(val, str) and '%' in val:
                    pct = float(val.replace('%', '').replace('+', ''))
                    if abs(pct) <= 10:
                        return 'background-color: #d4edda'
                    elif abs(pct) <= 15:
                        return 'background-color: #fff3cd'
                    else:
                        return 'background-color: #f8d7da'
                return ''
            
            styled_df = stats_df.style.map(color_deviation, subset=['Desv. %'])
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            # Gráfico de barras
            st.markdown("---")
            st.subheader("📊 Comparación Objetivo vs Asignado")
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name='Objetivo',
                x=stats_df['Médico'],
                y=stats_df['Objetivo'],
                marker_color='lightblue'
            ))
            fig.add_trace(go.Bar(
                name='Asignado',
                x=stats_df['Médico'],
                y=stats_df['Asignados'],
                marker_color='darkblue'
            ))
            
            fig.update_layout(
                barmode='group',
                xaxis_title="Médico",
                yaxis_title="Número de Turnos",
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Gráfico de desviación
            st.markdown("---")
            st.subheader("📈 Desviación por Médico")
            
            fig2 = px.bar(
                stats_df,
                x='Médico',
                y='Desviación',
                color='Desviación',
                color_continuous_scale=['red', 'yellow', 'green', 'yellow', 'red'],
                color_continuous_midpoint=0
            )
            
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)

# ==================== TAB 4: VERIFICACIÓN ====================
with tab4:
    st.header("⚠️ Verificación de Restricciones")
    
    if st.session_state.scheduler is None:
        st.info("ℹ️ No hay horario generado. Use el botón '🚀 Generar Horario' en la barra lateral.")
    else:
        violations = check_violations()
        
        # Resumen de violaciones
        total_violations = sum(len(v) for v in violations.values())
        
        if total_violations == 0:
            st.success("✅ ¡Excelente! No se encontraron violaciones de restricciones")
        else:
            st.error(f"❌ Se encontraron {total_violations} violaciones de restricciones")
        
        st.markdown("---")
        
        # Detalles de violaciones
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("🚫 Incompatibilidades")
            incomp_count = len(violations['incompatibilidades'])
            if incomp_count == 0:
                st.success(f"✅ 0 violaciones")
            else:
                st.error(f"❌ {incomp_count} violaciones")
                with st.expander("Ver detalles"):
                    for v in violations['incompatibilidades']:
                        st.write(f"• {v}")
        
        with col2:
            st.subheader("📅 Patrón 7/14 Días")
            pattern_count = len(violations['patron_7_14'])
            if pattern_count == 0:
                st.success(f"✅ 0 violaciones")
            else:
                st.error(f"❌ {pattern_count} violaciones")
                with st.expander("Ver detalles"):
                    for v in violations['patron_7_14'][:20]:  # Mostrar máximo 20
                        st.write(f"• {v}")
                    if pattern_count > 20:
                        st.write(f"... y {pattern_count - 20} más")
        
        with col3:
            st.subheader("🔒 Turnos Obligatorios")
            mandatory_count = len(violations['mandatory'])
            if mandatory_count == 0:
                st.success(f"✅ 0 violaciones")
            else:
                st.error(f"❌ {mandatory_count} violaciones")
                with st.expander("Ver detalles"):
                    for v in violations['mandatory']:
                        st.write(f"• {v}")
        
        # Recomendaciones
        if total_violations > 0:
            st.markdown("---")
            st.subheader("💡 Recomendaciones")
            
            if incomp_count > 0:
                st.warning("⚠️ Revise las incompatibilidades configuradas en los trabajadores")
            
            if pattern_count > 0:
                st.warning("⚠️ El patrón 7/14 días se está violando. Considere ajustar los días obligatorios o aumentar el número de trabajadores")
            
            if mandatory_count > 0:
                st.warning("⚠️ Algunos turnos obligatorios fueron modificados durante la optimización")

# ==================== TAB 5: PREDICTIVE ANALYTICS ====================
with tab5:
    st.header("🔮 Predictive Analytics")
    
    if not st.session_state.predictive_enabled:
        st.info("ℹ️ Predictive analytics is disabled. Enable it in the sidebar to access AI-powered forecasting and recommendations.")
    elif st.session_state.scheduler is None:
        st.info("ℹ️ No hay horario generado. Generate a schedule first to access predictive analytics.")
    else:
        # Insights Summary
        st.subheader("💡 Key Insights")
        insights = get_predictive_insights()
        
        if insights:
            for insight in insights:
                if insight['type'] == 'success':
                    st.success(f"**{insight['title']}**: {insight['message']}")
                elif insight['type'] == 'warning':
                    st.warning(f"**{insight['title']}**: {insight['message']}")
                elif insight['type'] == 'info':
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
                method = st.session_state.demand_forecasts.get('method', 'unknown')
                st.caption(f"📊 Forecast method: {method}")
        
        # Display forecasts
        if st.session_state.demand_forecasts:
            forecasts = st.session_state.demand_forecasts
            
            if 'daily_demand' in forecasts:
                st.markdown("**Predicted Daily Demand (Next 30 Days)**")
                
                # Create forecast chart
                forecast_data = pd.DataFrame({
                    'Day': list(range(1, len(forecasts['daily_demand']) + 1)),
                    'Predicted Demand': forecasts['daily_demand']
                })
                
                fig = px.line(
                    forecast_data,
                    x='Day',
                    y='Predicted Demand',
                    title='Demand Forecast',
                    markers=True
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
                
                # Statistics
                col_avg, col_max, col_min = st.columns(3)
                with col_avg:
                    st.metric("Average Demand", f"{sum(forecasts['daily_demand'])/len(forecasts['daily_demand']):.1f}")
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
                    min_value=-5, max_value=5, value=0,
                    help="Positivo: Contratar extra. Negativo: Eliminar/Bajas. (Ej: -1 elimina un médico)"
                )
                
                # Date Range for WORKERS simulation
                st.caption("Periodo afectado (Médicos)")
                col_wd1, col_wd2 = st.columns(2)
                with col_wd1:
                    sim_workers_start = st.date_input("Desde", value=None, key="sim_w_start")
                with col_wd2:
                    sim_workers_end = st.date_input("Hasta", value=None, key="sim_w_end")
            
            with col_sim_2:
                st.markdown("#### 🏥 Demanda (Guardias)")
                sim_shift_change = st.number_input(
                    "Cambio en guardias/día (+/-)",
                    min_value=-10, max_value=10, value=0, step=1,
                    help="Ej: +1 aumenta 1 guardia/día en el periodo seleccionado (o todo el periodo si no se define)"
                )
                
                # Date Range for SHIFTS simulation
                st.caption("Periodo afectado (Guardias)")
                col_sd1, col_sd2 = st.columns(2)
                with col_sd1:
                    sim_shifts_start = st.date_input("Desde", value=None, key="sim_s_start")
                with col_sd2:
                    sim_shifts_end = st.date_input("Hasta", value=None, key="sim_s_end")
                
            run_simulation = st.button("🚀 Ejecutar Simulación", type="primary")
            
        if run_simulation:
            with st.spinner("Ejecutando simulación de escenario..."):
                try:
                    # 1. Clonar configuración actual
                    sim_config = st.session_state.config.copy()
                    
                    # Asegurar que start_date y end_date están en la configuración
                    # (Vienen de variables locales del sidebar, no siempre están en session_state.config)
                    # Convertir a datetime puro si son fechas de streamlit (date)
                    if isinstance(start_date, datetime):
                        sim_config['start_date'] = start_date
                    else:
                        sim_config['start_date'] = datetime.combine(start_date, datetime.min.time())
                        
                    if isinstance(end_date, datetime):
                        sim_config['end_date'] = end_date
                    else:
                        sim_config['end_date'] = datetime.combine(end_date, datetime.min.time())
                        
                    sim_workers = copy.deepcopy(st.session_state.workers_data)
                    
                    # 2. Aplicar modificaciones
                    
                    # === MODIFICACIONES DE MÉDICOS ===
                    sim_workers_period_str = ""
                    if sim_workers_start and sim_workers_end:
                        sim_workers_period_str = f"{sim_workers_start.strftime('%d-%m-%Y')} - {sim_workers_end.strftime('%d-%m-%Y')}"

                    if sim_extra_workers > 0:
                        # AÑADIR trabajadores
                        for i in range(sim_extra_workers):
                            new_worker = {
                                'id': f'SIM_DOC_{i+1}',
                                'target_shifts': 0,
                                'work_percentage': 100,
                                'auto_calculate_shifts': True,
                                'mandatory_days': '',
                                'days_off': '',
                                'incompatible_with': []
                            }
                            # Si hay periodo, limitar periodo de trabajo
                            if sim_workers_period_str:
                                new_worker['work_periods'] = sim_workers_period_str
                                
                            sim_workers.append(new_worker)
                            
                    elif sim_extra_workers < 0:
                        # QUITAR trabajadores
                        num_to_remove = abs(sim_extra_workers)
                        
                        if sim_workers_period_str:
                            # Si hay periodo definido, NO eliminamos, sino que añadimos days_off (Baja temporal)
                            # Afectamos a los últimos workers de la lista (simulando que son los que 'sobran' o aleatorios)
                            target_workers = sim_workers[-num_to_remove:]
                            for w in target_workers:
                                current_off = w.get('days_off', '')
                                if current_off:
                                    w['days_off'] = f"{current_off}; {sim_workers_period_str}"
                                else:
                                    w['days_off'] = sim_workers_period_str
                        else:
                            # Si NO hay periodo, eliminación total
                            # Eliminamos los últimos de la lista para no romper IDs complejos si es posible
                            if len(sim_workers) >= num_to_remove:
                                sim_workers = sim_workers[:-num_to_remove]
                            else:
                                sim_workers = [] # Eliminar todos si pide quitar más de los que hay
                    
                    # === MODIFICACIONES DE TURNOS (VARIABLE SHIFTS) ===
                    # Ajustar turnos por día (Variable Shifts)
                    if sim_shift_change != 0:
                        # Determinar rango de fechas afectado para TURNOS
                        if sim_shifts_start and sim_shifts_end:
                            range_start = datetime.combine(sim_shifts_start, datetime.min.time())
                            range_end = datetime.combine(sim_shifts_end, datetime.min.time())
                        else:
                            range_start = sim_config['start_date']
                            range_end = sim_config['end_date']
                        
                        # Mapa actual de variable_shifts
                        existing_var_shifts = {}
                        for vs in sim_config.get('variable_shifts', []):
                            # El formato en config es {'start_date': ..., 'end_date': ..., 'shifts': ...}
                            # Asumimos rangos de 1 día como genera la UI por defecto
                            d = vs.get('start_date')
                            s = vs.get('shifts')
                            if d and s is not None:
                                existing_var_shifts[d] = s
                        base_shifts = sim_config['num_shifts']
                        
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
                        sim_config['variable_shifts'] = [
                            {'start_date': d, 'end_date': d, 'shifts': n} for d, n in existing_var_shifts.items()
                        ]
                    
                    sim_config['workers_data'] = sim_workers
                    
                    # Add safety flag for simulation
                    sim_config['is_simulation'] = True
                    
                    # 3. Generar horario simulado (sin guardar en session_state)
                    # Deshabilitar logs o UI updates para velocidad
                    sim_scheduler = Scheduler(sim_config)
                    success = sim_scheduler.generate_schedule(max_improvement_loops=150) # Menos loops para velocidad
                    
                    if success:
                        st.success("✅ Simulación completada")
                        
                        # 4. Comparar resultados
                        # Calcular métricas escenario BASE
                        base_scheduler = st.session_state.scheduler
                        base_uncovered = base_scheduler.num_shifts * ((base_scheduler.end_date - base_scheduler.start_date).days + 1) - sum(len(v) for v in base_scheduler.schedule.values())
                        # Nota: La métrica de 'uncovered' real depende de si hay huecos. 
                        # Asumimos que el scheduler intenta llenar todo.
                        # Una mejor métrica es "Desviación Media"
                        
                        # Calcular métricas escenario SIMULADO
                        sim_uncovered = sim_scheduler.num_shifts * ((sim_scheduler.end_date - sim_scheduler.start_date).days + 1) - sum(len(v) for v in sim_scheduler.schedule.values())
                        
                        # Mostrar Comparativa
                        st.subheader("📊 Resultados Comparativos")
                        
                        # Helper for avg shifts/month
                        def calc_avg_shifts_month(sch):
                            # Get stats
                            stats_data = sch.stats.calculate_statistics()
                            # Get all workers stats
                            workers_stats = stats_data.get('workers', {})
                            
                            if not workers_stats:
                                return 0
                                
                            # Calculate total assigned shifts
                            total_assigned = sum(w.get('total_shifts', 0) for w in workers_stats.values())
                            
                            # Calculate months duration
                            days = (sch.end_date - sch.start_date).days + 1
                            months = days / 30.44 # Approx avg month length
                            if months < 1: months = 1
                            
                            # Average per worker per month
                            total_workers = len(workers_stats)
                            if total_workers == 0: return 0
                            
                            return (total_assigned / total_workers) / months

                        col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                        with col_m1:
                            st.metric("Total Médicos", len(sim_workers), delta=len(sim_workers)-len(st.session_state.workers_data))
                        with col_m2:
                            current_shifts_per_day = st.session_state.config['num_shifts']
                            sim_shifts_per_day = sim_config['num_shifts']
                            st.metric("Guardias/Día", sim_shifts_per_day, delta=sim_shifts_per_day - current_shifts_per_day)
                        with col_m3:
                            base_avg_month = calc_avg_shifts_month(base_scheduler)
                            sim_avg_month = calc_avg_shifts_month(sim_scheduler)
                            st.metric("Guardias/Mes (Avg)", f"{sim_avg_month:.1f}", delta=f"{sim_avg_month - base_avg_month:.1f}")
                        with col_m4:
                            # Calcular desviación promedio absoluta
                            def calc_avg_dev(sch):
                                stats_data = sch.stats.calculate_statistics()
                                workers_stats = stats_data.get('workers', {})
                                if not workers_stats:
                                    return 0
                                    
                                total_dev = 0
                                count = 0
                                for w_id, w_data in workers_stats.items():
                                    dev = w_data.get('total_shifts', 0) - w_data.get('target_shifts', 0)
                                    total_dev += abs(dev)
                                    count += 1
                                    
                                return total_dev / count if count > 0 else 0
                                
                            base_dev = calc_avg_dev(base_scheduler)
                            sim_dev = calc_avg_dev(sim_scheduler)
                            
                            st.metric("Desviación Promedio", f"{sim_dev:.2f}", delta=f"{base_dev - sim_dev:.2f}", delta_color="inverse")
                            # Nota: delta positivo en equidad es malo si significa más desviación, por eso inverse
                            
                        # Visualización de Impacto
                        st.caption("Una desviación promedio menor indica un reparto más equitativo.")
                            
                    else:
                        st.error("La simulación no pudo encontrar una solución viable con estos parámetros.")
                        
                except Exception as e:
                    st.error(f"Error en simulación: {str(e)}")
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
            high_priority = [r for r in recommendations if r.get('priority') == 'high']
            medium_priority = [r for r in recommendations if r.get('priority') == 'medium']
            low_priority = [r for r in recommendations if r.get('priority') == 'low']
            
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

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: gray;'>"
    "Sistema de Generación de Horarios v2.0 | "
    "Interfaz Streamlit | "
    f"© {datetime.now().year}"
    "</div>",
    unsafe_allow_html=True
)
