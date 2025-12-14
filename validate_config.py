#!/usr/bin/env python3
"""
Script para validar configuración y detectar conflictos en mandatory shifts
"""

import json
from datetime import datetime

def load_config(filename='schedule_config.json'):
    """Carga configuración desde JSON"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        return None

def parse_date(date_str):
    """Parsea fecha en formato DD-MM-YYYY o YYYY-MM-DD"""
    try:
        return datetime.strptime(date_str, '%d-%m-%Y')
    except ValueError:
        return datetime.strptime(date_str, '%Y-%m-%d')

def parse_date_ranges(ranges_str):
    """Parsea rangos de fechas separados por ;"""
    if not ranges_str:
        return []
    
    ranges = []
    for range_part in ranges_str.split(';'):
        range_part = range_part.strip()
        if ' - ' in range_part:
            start_str, end_str = range_part.split(' - ')
            start = parse_date(start_str.strip())
            end = parse_date(end_str.strip())
            ranges.append((start, end))
    
    return ranges

def is_date_in_ranges(date, ranges):
    """Verifica si una fecha está dentro de algún rango"""
    for start, end in ranges:
        if start <= date <= end:
            return True
    return False

def validate_configuration():
    """Valida configuración y detecta conflictos"""
    print("=" * 80)
    print("VALIDACIÓN DE CONFIGURACIÓN")
    print("=" * 80)
    
    config = load_config()
    if not config:
        return
    
    workers_data = config.get('workers_data', [])
    num_shifts = config.get('num_shifts', 4)
    
    # Construir diccionario de incompatibilidades
    incompatibilities = {}
    for worker in workers_data:
        worker_id = worker['id']
        incomp_list = worker.get('incompatible_with', [])
        if incomp_list:
            incompatibilities[worker_id] = set(incomp_list)
    
    # Verificar conflictos
    errors = []
    warnings = []
    
    print("\n🔍 VERIFICANDO MANDATORY SHIFTS...\n")
    
    # Agrupar mandatory por fecha
    mandatory_by_date = {}
    
    for worker in workers_data:
        worker_id = worker['id']
        worker_name = worker.get('name', 'Unknown')
        mandatory_str = worker.get('mandatory_days', '')
        
        if not mandatory_str:
            continue
        
        # Parsear work_periods
        work_periods_str = worker.get('work_periods', '')
        work_ranges = parse_date_ranges(work_periods_str)
        
        # Parsear mandatory dates
        dates_str = mandatory_str.replace(';', ',')
        date_strings = [d.strip() for d in dates_str.split(',') if d.strip()]
        
        for date_str in date_strings:
            try:
                mandatory_date = parse_date(date_str)
                
                # VALIDACIÓN 1: Verificar si mandatory está dentro de work_periods
                if work_ranges and not is_date_in_ranges(mandatory_date, work_ranges):
                    error_msg = (f"❌ ERROR: {worker_id} ({worker_name}) tiene mandatory "
                               f"el {date_str} pero NO está dentro de sus work_periods: "
                               f"{work_periods_str}")
                    errors.append(error_msg)
                    print(error_msg)
                
                # Agrupar por fecha para detectar incompatibilidades
                if mandatory_date not in mandatory_by_date:
                    mandatory_by_date[mandatory_date] = []
                mandatory_by_date[mandatory_date].append({
                    'id': worker_id,
                    'name': worker_name
                })
                
            except ValueError as e:
                error_msg = f"❌ ERROR: No se pudo parsear fecha '{date_str}' para {worker_id}: {e}"
                errors.append(error_msg)
                print(error_msg)
    
    # VALIDACIÓN 2: Verificar incompatibilidades entre mandatory del mismo día
    print("\n🔍 VERIFICANDO INCOMPATIBILIDADES...\n")
    
    for date, workers_list in sorted(mandatory_by_date.items()):
        date_str = date.strftime('%d-%m-%Y')
        
        # Si hay más mandatory que turnos disponibles
        if len(workers_list) > num_shifts:
            error_msg = (f"❌ ERROR: {date_str} tiene {len(workers_list)} mandatory "
                       f"pero solo hay {num_shifts} turnos disponibles")
            errors.append(error_msg)
            print(error_msg)
        
        # Verificar incompatibilidades entre los workers mandatory de ese día
        for i, worker1 in enumerate(workers_list):
            for worker2 in workers_list[i+1:]:
                w1_id = worker1['id']
                w2_id = worker2['id']
                
                # Verificar si son incompatibles
                if w1_id in incompatibilities and w2_id in incompatibilities[w1_id]:
                    error_msg = (f"❌ ERROR: {date_str} tiene mandatory incompatibles: "
                               f"{w1_id} ({worker1['name']}) y {w2_id} ({worker2['name']})")
                    errors.append(error_msg)
                    print(error_msg)
    
    # VALIDACIÓN 3: Advertencias sobre días con muchos mandatory
    print("\n🔍 VERIFICANDO CARGA DE MANDATORY...\n")
    
    for date, workers_list in sorted(mandatory_by_date.items()):
        date_str = date.strftime('%d-%m-%Y')
        
        if len(workers_list) == num_shifts:
            warning_msg = (f"⚠️  WARNING: {date_str} tiene {num_shifts} mandatory "
                         f"(todos los turnos son obligatorios)")
            warnings.append(warning_msg)
            print(warning_msg)
        elif len(workers_list) > num_shifts * 0.7:
            warning_msg = (f"⚠️  WARNING: {date_str} tiene {len(workers_list)}/{num_shifts} "
                         f"turnos mandatory (>70%)")
            warnings.append(warning_msg)
            print(warning_msg)
    
    # Resumen
    print("\n" + "=" * 80)
    print("RESUMEN DE VALIDACIÓN")
    print("=" * 80)
    print(f"❌ Errores encontrados:      {len(errors)}")
    print(f"⚠️  Advertencias:             {len(warnings)}")
    
    if errors:
        print("\n🔧 ACCIONES REQUERIDAS:")
        print("Los errores deben corregirse en schedule_config.json antes de generar el schedule.")
    else:
        print("\n✅ No se encontraron errores críticos en la configuración.")
    
    return len(errors) == 0

if __name__ == "__main__":
    validate_configuration()
