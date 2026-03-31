"""
Bridge Manager - Gestión de Puentes (Bridge Holidays)
Versión: 2.0 (Febrero 2026)

Funcionalidades:
- Detección automática de puentes basada en festivos
- Tracking de puentes asignados por trabajador
- Cálculo de balance de distribución equitativa
"""

from datetime import datetime, timedelta, date
from typing import List, Dict, Set, Optional, Any
import logging

logger = logging.getLogger(__name__)


class BridgeManager:
    """
    Gestiona la detección y tracking de puentes (bridge holidays).
    
    Un puente ocurre cuando un festivo se solapa con un fin de semana:
    - Festivo Jueves → Puente Jueves a Domingo (4 días)
    - Festivo Viernes → Puente Viernes a Domingo (3 días)
    - Festivo Lunes → Puente Viernes a Lunes (3 días)
    - Festivo Martes → Puente Viernes a Martes (4 días)
    """
    
    def __init__(self):
        """Initialize the bridge manager"""
        self.bridges = []  # List of detected bridges
        self.bridge_days_set = set()  # Set of all days that are part of a bridge
        logger.info("BridgeManager initialized")
    
    def detect_bridges(self, holidays: List[datetime], start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """
        Detecta todos los puentes en el período basándose en los festivos.
        
        Args:
            holidays: Lista de fechas festivas
            start_date: Fecha inicial del período
            end_date: Fecha final del período
        
        Returns:
            List[dict]: Lista de puentes detectados con información detallada
        """
        if not holidays:
            logger.info("No holidays provided, no bridges to detect")
            self.bridges = []
            self.bridge_days_set = set()
            return []
        
        bridges = []
        bridge_days = set()
        
        # Convert holidays to date objects if needed
        holiday_dates = []
        for h in holidays:
            if isinstance(h, datetime):
                holiday_dates.append(h.date())
            elif isinstance(h, date):
                holiday_dates.append(h)
            else:
                logger.warning(f"Invalid holiday format: {h}")
        
        # Sort holidays for processing
        holiday_dates.sort()
        
        for holiday in holiday_dates:
            # Check if holiday is within the schedule period
            if isinstance(start_date, datetime):
                start = start_date.date()
            else:
                start = start_date
            
            if isinstance(end_date, datetime):
                end = end_date.date()
            else:
                end = end_date
            
            if holiday < start or holiday > end:
                continue
            
            # Get weekday (0=Monday, 6=Sunday)
            weekday = holiday.weekday()
            
            bridge_info = None
            
            # Check if it creates a bridge
            if weekday == 3:  # Thursday
                # Bridge: Thursday to Sunday (4 days)
                bridge_start = holiday
                bridge_end = holiday + timedelta(days=3)  # Sunday
                bridge_info = {
                    'holiday_date': holiday,
                    'weekday': weekday,
                    'weekday_name': 'Jueves',
                    'bridge_type': 'posterior',
                    'bridge_start': bridge_start,
                    'bridge_end': bridge_end,
                    'bridge_days': self._get_date_range(bridge_start, bridge_end)
                }
            
            elif weekday == 4:  # Friday
                # Bridge: Friday to Sunday (3 days)
                bridge_start = holiday
                bridge_end = holiday + timedelta(days=2)  # Sunday
                bridge_info = {
                    'holiday_date': holiday,
                    'weekday': weekday,
                    'weekday_name': 'Viernes',
                    'bridge_type': 'posterior',
                    'bridge_start': bridge_start,
                    'bridge_end': bridge_end,
                    'bridge_days': self._get_date_range(bridge_start, bridge_end)
                }
            
            elif weekday == 0:  # Monday
                # Bridge: Friday to Monday (3 days)
                bridge_start = holiday - timedelta(days=3)  # Previous Friday
                bridge_end = holiday
                bridge_info = {
                    'holiday_date': holiday,
                    'weekday': weekday,
                    'weekday_name': 'Lunes',
                    'bridge_type': 'anterior',
                    'bridge_start': bridge_start,
                    'bridge_end': bridge_end,
                    'bridge_days': self._get_date_range(bridge_start, bridge_end)
                }
            
            elif weekday == 1:  # Tuesday
                # Bridge: Friday to Tuesday (4 days)
                bridge_start = holiday - timedelta(days=4)  # Previous Friday
                bridge_end = holiday
                bridge_info = {
                    'holiday_date': holiday,
                    'weekday': weekday,
                    'weekday_name': 'Martes',
                    'bridge_type': 'anterior',
                    'bridge_start': bridge_start,
                    'bridge_end': bridge_end,
                    'bridge_days': self._get_date_range(bridge_start, bridge_end)
                }
            
            # If a bridge was detected, add it
            if bridge_info:
                # Only include if bridge days are within schedule period
                valid_bridge_days = [d for d in bridge_info['bridge_days'] if start <= d <= end]
                if valid_bridge_days:
                    bridge_info['bridge_days'] = valid_bridge_days
                    bridges.append(bridge_info)
                    bridge_days.update(valid_bridge_days)
                    logger.info(f"Bridge detected: {holiday} ({bridge_info['weekday_name']}) - "
                              f"{bridge_info['bridge_type']} - {len(valid_bridge_days)} days")
        
        # Store for quick access
        self.bridges = bridges
        self.bridge_days_set = bridge_days
        
        logger.info(f"Total bridges detected: {len(bridges)}, Total bridge days: {len(bridge_days)}")
        return bridges
    
    def is_bridge_day(self, check_date: datetime) -> bool:
        """
        Verifica si una fecha es parte de un puente.
        
        Args:
            check_date: Fecha a verificar
        
        Returns:
            bool: True si la fecha es parte de un puente
        """
        if isinstance(check_date, datetime):
            check_date = check_date.date()
        
        return check_date in self.bridge_days_set
    
    def get_bridge_for_date(self, check_date: datetime) -> Optional[Dict[str, Any]]:
        """
        Obtiene información del puente para una fecha específica.
        
        Args:
            check_date: Fecha a verificar
        
        Returns:
            dict: Información del puente si existe, None en caso contrario
        """
        if isinstance(check_date, datetime):
            check_date = check_date.date()
        
        for bridge in self.bridges:
            if check_date in bridge['bridge_days']:
                return bridge
        
        return None
    
    def calculate_bridge_stats(self, worker_assignments: Dict[str, List[datetime]], 
                               workers: Dict[str, Dict]) -> Dict[str, Any]:
        """
        Calcula estadísticas de puentes por trabajador.
        
        Args:
            worker_assignments: Dict {worker_id: [date1, date2, ...]}
            workers: Dict {worker_id: worker_data}
        
        Returns:
            dict: Estadísticas de puentes
        """
        stats = {
            'total_bridges': len(self.bridges),
            'total_bridge_days': len(self.bridge_days_set),
            'worker_bridge_counts': {},
            'worker_bridge_normalized': {},
            'bridge_balance': {},
            'average_normalized': 0.0
        }
        
        if not self.bridges:
            return stats
        
        # Calculate bridge counts per worker
        total_normalized = 0.0
        worker_count = 0
        
        for worker_id, assignments in worker_assignments.items():
            if worker_id not in workers:
                continue
            
            worker = workers[worker_id]
            work_percentage = worker.get('work_percentage', 100) / 100.0
            
            # Count bridge days for this worker
            bridge_count = 0
            for assignment_date in assignments:
                if isinstance(assignment_date, datetime):
                    assignment_date = assignment_date.date()
                if assignment_date in self.bridge_days_set:
                    bridge_count += 1
            
            stats['worker_bridge_counts'][worker_id] = bridge_count
            
            # Normalize by work percentage
            normalized = bridge_count / work_percentage if work_percentage > 0 else 0
            stats['worker_bridge_normalized'][worker_id] = normalized
            
            total_normalized += normalized
            worker_count += 1
        
        # Calculate average and balance
        if worker_count > 0:
            avg_normalized = total_normalized / worker_count
            stats['average_normalized'] = avg_normalized
            
            for worker_id in stats['worker_bridge_normalized']:
                normalized = stats['worker_bridge_normalized'][worker_id]
                balance = normalized - avg_normalized
                stats['bridge_balance'][worker_id] = balance
        
        return stats
    
    def get_bridge_count_for_worker(self, worker_id: str, 
                                    worker_assignments: Dict[str, List[datetime]]) -> int:
        """
        Obtiene el número de días de puente asignados a un trabajador.
        
        Args:
            worker_id: ID del trabajador
            worker_assignments: Dict {worker_id: [date1, date2, ...]}
        
        Returns:
            int: Número de días de puente asignados
        """
        if worker_id not in worker_assignments:
            return 0
        
        count = 0
        for assignment_date in worker_assignments[worker_id]:
            if isinstance(assignment_date, datetime):
                assignment_date = assignment_date.date()
            if assignment_date in self.bridge_days_set:
                count += 1
        
        return count
    
    @staticmethod
    def _get_date_range(start_date: date, end_date: date) -> List[date]:
        """
        Genera lista de fechas entre start_date y end_date (inclusive).
        
        Args:
            start_date: Fecha inicial
            end_date: Fecha final
        
        Returns:
            List[date]: Lista de fechas
        """
        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates
