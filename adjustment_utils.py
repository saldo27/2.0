"""
Utilidades para el ajuste y corrección de turnos
Permite calcular desviaciones y generar intercambios sugeridos
"""
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any

class TurnAdjustmentManager:
    """Maneja el cálculo de desviaciones y las correcciones de turnos"""
    
    def __init__(self, schedule_config):
        """
        Inicializa el gestor de ajustes
        
        Args:
            schedule_config: Configuración del horario con schedule, workers_data, etc.
        """
        self.schedule_config = schedule_config
        self.schedule = schedule_config.get('schedule', {})
        self.workers_data = schedule_config.get('workers_data', [])
        self.num_shifts = schedule_config.get('num_shifts', 0)
        self.holidays = schedule_config.get('holidays', [])
        
    def calculate_deviations(self) -> List[Dict]:
        """
        Calcula las desviaciones de cada trabajador respecto al objetivo
        
        Returns:
            Lista de diccionarios con información de desviación por trabajador
        """
        worker_stats = {}
        
        # Inicializar estadísticas para cada trabajador usando su target_shifts real
        for worker in self.workers_data:
            worker_id = worker['id']
            target_shifts = worker.get('target_shifts', 0)  # Usar el target_shifts ya calculado
            worker_stats[worker_id] = {
                'name': worker_id,
                'assigned': 0,
                'target': target_shifts,  # Usar el valor real del sistema
                'percentage': worker.get('work_percentage', 100)
            }
        
        # Contar turnos realmente asignados en el schedule
        for date, workers in self.schedule.items():
            for worker_id in workers:
                if worker_id and worker_id in worker_stats:
                    worker_stats[worker_id]['assigned'] += 1
        
        # Calcular desviaciones usando los target_shifts reales
        deviations = []
        for worker_id, stats in worker_stats.items():
            deviation = stats['assigned'] - stats['target']
            
            deviations.append({
                'name': stats['name'],
                'assigned': stats['assigned'],
                'target': stats['target'],
                'deviation': deviation,
                'percentage': stats['percentage']
            })
        
        return sorted(deviations, key=lambda x: abs(x['deviation']), reverse=True)
    
    def find_best_swaps(self, deviations: List[Dict], max_suggestions: int = 5) -> List[Dict]:
        """
        Encuentra los mejores intercambios para equilibrar las desviaciones
        
        Args:
            deviations: Lista de desviaciones por trabajador
            max_suggestions: Número máximo de sugerencias a devolver
            
        Returns:
            Lista de intercambios sugeridos
        """
        # Separar trabajadores con exceso y déficit de turnos
        overassigned = [d for d in deviations if d['deviation'] > 1]
        underassigned = [d for d in deviations if d['deviation'] < -1]
        
        suggestions = []
        
        # Buscar intercambios entre trabajadores con exceso y déficit
        for over_worker in overassigned:
            for under_worker in underassigned:
                swaps = self._find_swaps_between_workers(
                    over_worker['name'], 
                    under_worker['name']
                )
                
                for swap in swaps:
                    improvement = min(abs(over_worker['deviation']), abs(under_worker['deviation']))
                    swap['improvement'] = improvement
                    suggestions.append(swap)
        
        # Ordenar por mejora potencial y retornar las mejores
        suggestions.sort(key=lambda x: x['improvement'], reverse=True)
        return suggestions[:max_suggestions]
    
    def _find_swaps_between_workers(self, worker1: str, worker2: str) -> List[Dict]:
        """
        Encuentra intercambios posibles entre dos trabajadores específicos
        
        Args:
            worker1: ID del primer trabajador (con exceso de turnos)
            worker2: ID del segundo trabajador (con déficit de turnos)
            
        Returns:
            Lista de intercambios posibles
        """
        swaps = []
        
        # Encontrar días donde worker1 está asignado
        worker1_assigned_dates = []
        available_dates_for_worker2 = []
        
        for date, workers in self.schedule.items():
            if worker1 in workers:
                # worker1 tiene turno este día
                worker1_assigned_dates.append(date)
            elif worker2 not in workers and len(workers) < self.num_shifts:
                # Hay espacio disponible y worker2 no está asignado
                if self._can_worker_take_shift(worker2, date):
                    available_dates_for_worker2.append(date)
        
        # Opción 1: Transferencia directa (worker1 libera turno, worker2 lo toma)
        for date in worker1_assigned_dates[:5]:  # Limitar para rendimiento
            if self._can_worker_take_shift(worker2, date):
                # Verificar que worker1 puede liberar este turno
                if self._can_worker_release_shift(worker1, date):
                    swap = {
                        'type': 'direct_transfer',
                        'worker1': worker1,
                        'worker2': worker2,
                        'date': date,
                        'date_str': date.strftime('%d-%m-%Y'),
                        'description': f"Transferir turno de {worker1} a {worker2} el {date.strftime('%d-%m-%Y')}"
                    }
                    swaps.append(swap)
        
        # Opción 2: Intercambio de días (worker1 y worker2 cambian turnos)
        worker2_assigned_dates = []
        for date, workers in self.schedule.items():
            if worker2 in workers:
                worker2_assigned_dates.append(date)
        
        for w1_date in worker1_assigned_dates[:3]:
            for w2_date in worker2_assigned_dates[:3]:
                if (w1_date != w2_date and 
                    self._can_worker_take_shift(worker2, w1_date) and
                    self._can_worker_take_shift(worker1, w2_date)):
                    swap = {
                        'type': 'mutual_exchange',
                        'worker1': worker1,
                        'worker2': worker2,
                        'date1': w1_date,
                        'date2': w2_date,
                        'date_str': f"{w1_date.strftime('%d-%m-%Y')} ↔ {w2_date.strftime('%d-%m-%Y')}",
                        'description': f"Intercambio: {worker1} da {w1_date.strftime('%d-%m-%Y')}, recibe {w2_date.strftime('%d-%m-%Y')}"
                    }
                    swaps.append(swap)
        
        return swaps
    
    def _can_worker_release_shift(self, worker_id: str, date: datetime) -> bool:
        """
        Verifica si un trabajador puede liberar un turno (no es obligatorio)
        
        Args:
            worker_id: ID del trabajador
            date: Fecha del turno
            
        Returns:
            True si el trabajador puede liberar el turno (False si es mandatory)
        """
        # Encontrar datos del trabajador
        worker_data = None
        for worker in self.workers_data:
            if worker['id'] == worker_id:
                worker_data = worker
                break
        
        if not worker_data:
            return False
        
        # CRITICAL: Verificar si el día es obligatorio (mandatory_days)
        # Los mandatory_days son INAMOVIBLES y nunca pueden ser liberados
        mandatory_str = worker_data.get('mandatory_days', '')
        if mandatory_str.strip():
            try:
                # Usar el mismo método que el resto del código para parsear fechas
                from utilities import DateTimeUtils
                date_utils = DateTimeUtils()
                mandatory_dates = date_utils.parse_dates(mandatory_str)
                
                # Verificar si la fecha es mandatory
                for mandatory_date in mandatory_dates:
                    if mandatory_date.date() == date.date():
                        logging.info(f"Worker {worker_id} CANNOT release shift on {date.strftime('%d-%m-%Y')} - it is a MANDATORY assignment")
                        return False  # NO puede liberar días obligatorios
            except Exception as e:
                logging.error(f"Error parsing mandatory_days for worker {worker_id}: {e}")
                # En caso de error al parsear, asumir que NO puede liberar (fail-safe)
                return False
        
        return True
    
    def _can_worker_take_shift(self, worker_id: str, date: datetime) -> bool:
        """
        Verifica si un trabajador puede tomar un turno en una fecha específica
        
        Args:
            worker_id: ID del trabajador
            date: Fecha del turno
            
        Returns:
            True si el trabajador puede tomar el turno
        """
        # Encontrar datos del trabajador
        worker_data = None
        for worker in self.workers_data:
            if worker['id'] == worker_id:
                worker_data = worker
                break
        
        if not worker_data:
            return False
        
        # Verificar períodos de trabajo
        if not self._is_date_in_work_periods(date, worker_data.get('work_periods', '')):
            return False
        
        # Verificar días fuera
        if self._is_date_in_days_off(date, worker_data.get('days_off', '')):
            return False
        
        # Verificar restricciones de días mínimos entre turnos
        gap_between_shifts = self.schedule_config.get('gap_between_shifts', 3)
        if not self._check_minimum_gap(worker_id, date, gap_between_shifts):
            return False
        
        return True
    
    def _is_date_in_work_periods(self, date: datetime, work_periods_str: str) -> bool:
        """Verifica si la fecha está dentro de los períodos de trabajo"""
        if not work_periods_str.strip():
            return True  # Si no hay restricciones, siempre disponible
        
        try:
            for period in work_periods_str.split(';'):
                period = period.strip()
                if ' - ' in period:
                    start_str, end_str = period.split(' - ')
                    start_date = datetime.strptime(start_str.strip(), '%d-%m-%Y')
                    end_date = datetime.strptime(end_str.strip(), '%d-%m-%Y')
                    if start_date <= date <= end_date:
                        return True
                else:
                    # Fecha única
                    single_date = datetime.strptime(period.strip(), '%d-%m-%Y')
                    if single_date.date() == date.date():
                        return True
            return False
        except:
            return True  # En caso de error, asumir disponible
    
    def _is_date_in_days_off(self, date: datetime, days_off_str: str) -> bool:
        """Verifica si la fecha está en los días fuera"""
        if not days_off_str.strip():
            return False
        
        try:
            for period in days_off_str.split(';'):
                period = period.strip()
                if ' - ' in period:
                    start_str, end_str = period.split(' - ')
                    start_date = datetime.strptime(start_str.strip(), '%d-%m-%Y')
                    end_date = datetime.strptime(end_str.strip(), '%d-%m-%Y')
                    if start_date <= date <= end_date:
                        return True
                else:
                    # Fecha única
                    single_date = datetime.strptime(period.strip(), '%d-%m-%Y')
                    if single_date.date() == date.date():
                        return True
            return False
        except:
            return False
    
    def _check_minimum_gap(self, worker_id: str, target_date: datetime, min_gap: int) -> bool:
        """Verifica que haya suficiente distancia entre turnos"""
        worker_shifts = []
        for date, workers in self.schedule.items():
            if worker_id in workers:
                worker_shifts.append(date)
        
        # Verificar distancia mínima con turnos existentes
        for shift_date in worker_shifts:
            days_diff = abs((target_date - shift_date).days)
            if days_diff < min_gap:
                return False
        
        return True
    
    def apply_swap(self, swap: Dict) -> Dict:
        """
        Aplica un intercambio sugerido al horario
        
        Args:
            swap: Diccionario con información del intercambio
            
        Returns:
            Nuevo horario actualizado
        """
        new_schedule = self.schedule.copy()
        
        if swap['type'] == 'direct_transfer':
            # Transferencia directa: worker1 da su turno a worker2
            date = swap['date']
            if date in new_schedule:
                workers = new_schedule[date].copy()
                for i, worker in enumerate(workers):
                    if worker == swap['worker1']:
                        workers[i] = swap['worker2']
                        break
                new_schedule[date] = workers
        
        elif swap['type'] == 'mutual_exchange':
            # Intercambio mutuo: worker1 y worker2 intercambian turnos
            date1 = swap['date1']
            date2 = swap['date2']
            
            # Intercambiar en date1
            if date1 in new_schedule:
                workers1 = new_schedule[date1].copy()
                for i, worker in enumerate(workers1):
                    if worker == swap['worker1']:
                        workers1[i] = swap['worker2']
                        break
                new_schedule[date1] = workers1
            
            # Intercambiar en date2
            if date2 in new_schedule:
                workers2 = new_schedule[date2].copy()
                for i, worker in enumerate(workers2):
                    if worker == swap['worker2']:
                        workers2[i] = swap['worker1']
                        break
                new_schedule[date2] = workers2
        
        return new_schedule
