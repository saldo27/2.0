"""
Validador de tolerancia de shifts
Valida que los shifts asignados estén dentro del rango de tolerancia +/-8% del target_shift
"""
import logging
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta

class ShiftToleranceValidator:
    """Validador para asegurar que los shifts asignados respeten la tolerancia por fases
    
    Sistema de tolerancia por fases:
    - Fase 1 (Initial): ±8% (objetivo estricto)
    - Fase 2 (Emergency): ±12% (límite absoluto, solo si cobertura < 95%)
    
    IMPORTANTE: Este validador verifica contra los límites configurados.
    El enforcement activo está en schedule_builder._would_violate_tolerance()
    """
    
    def __init__(self, scheduler):
        """
        Initialize the tolerance validator
        
        Args:
            scheduler: The main Scheduler object
        """
        self.scheduler = scheduler
        self.workers_data = scheduler.workers_data
        self.schedule = scheduler.schedule
        # Phase 1 tolerance: ±8% (strict target)
        self.tolerance_percentage = 8.0
        # Phase 2 tolerance: ±12% (absolute maximum)
        self.emergency_tolerance_percentage = 12.0
        
    def calculate_tolerance_bounds(self, target_shifts: int) -> Tuple[int, int]:
        """
        Calcula los límites de tolerancia para un target_shifts dado
        
        Args:
            target_shifts: Número objetivo de turnos
            
        Returns:
            Tuple con (min_shifts, max_shifts) dentro de la tolerancia
        """
        if target_shifts <= 0:
            return (0, 0)
            
        tolerance_amount = target_shifts * (self.tolerance_percentage / 100.0)
        min_shifts = max(0, int(target_shifts - tolerance_amount))
        max_shifts = int(target_shifts + tolerance_amount + 0.5)  # Round up for max
        
        return (min_shifts, max_shifts)
    
    def validate_worker_shift_count(self, worker_id: str, is_weekend_only: bool = False) -> Dict[str, Any]:
        """
        Valida que un trabajador específico esté dentro de la tolerancia
        
        Args:
            worker_id: ID del trabajador
            is_weekend_only: Si True, solo cuenta shifts de weekend
            
        Returns:
            Dict con información de validación
        """
        worker = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker:
            return {
                'valid': False,
                'error': f'Worker {worker_id} not found'
            }
            
        target_shifts = worker.get('target_shifts', 0)
        assigned_shifts = self._count_assigned_shifts(worker_id, is_weekend_only)
        
        min_shifts, max_shifts = self.calculate_tolerance_bounds(target_shifts)
        
        is_valid = min_shifts <= assigned_shifts <= max_shifts
        deviation_percentage = 0.0
        if target_shifts > 0:
            deviation_percentage = ((assigned_shifts - target_shifts) / target_shifts) * 100
        
        return {
            'worker_id': worker_id,
            'target_shifts': target_shifts,
            'assigned_shifts': assigned_shifts,
            'min_allowed': min_shifts,
            'max_allowed': max_shifts,
            'valid': is_valid,
            'deviation_percentage': deviation_percentage,
            'is_weekend_only': is_weekend_only
        }
    
    def validate_all_workers(self) -> List[Dict[str, Any]]:
        """
        Valida todos los trabajadores para shifts regulares
        
        Returns:
            Lista de resultados de validación para todos los trabajadores
        """
        results = []
        for worker in self.workers_data:
            worker_id = worker['id']
            validation = self.validate_worker_shift_count(worker_id, is_weekend_only=False)
            results.append(validation)
        
        return results
    
    def validate_weekend_shifts(self) -> List[Dict[str, Any]]:
        """
        Valida todos los trabajadores específicamente para shifts de weekend
        
        Returns:
            Lista de resultados de validación para shifts de weekend
        """
        results = []
        for worker in self.workers_data:
            worker_id = worker['id']
            
            # Para weekend shifts, calculamos un target proporcional
            total_target = worker.get('target_shifts', 0)
            weekend_target = self._calculate_weekend_target(worker_id, total_target)
            
            # Temporalmente actualizamos el target para validar weekends
            original_target = worker.get('target_shifts', 0)
            worker['target_shifts'] = weekend_target
            
            validation = self.validate_worker_shift_count(worker_id, is_weekend_only=True)
            
            # Restauramos el target original
            worker['target_shifts'] = original_target
            
            results.append(validation)
        
        return results
    
    def get_workers_outside_tolerance(self, is_weekend_only: bool = False) -> List[Dict[str, Any]]:
        """
        Obtiene lista de trabajadores que están fuera de la tolerancia
        
        Args:
            is_weekend_only: Si True, verifica solo shifts de weekend
            
        Returns:
            Lista de trabajadores fuera de tolerancia
        """
        if is_weekend_only:
            validations = self.validate_weekend_shifts()
        else:
            validations = self.validate_all_workers()
            
        return [v for v in validations if not v['valid']]
    
    def suggest_shift_adjustments(self, is_weekend_only: bool = False) -> List[Dict[str, Any]]:
        """
        Sugiere ajustes para trabajadores fuera de tolerancia
        
        Args:
            is_weekend_only: Si True, sugiere ajustes solo para weekends
            
        Returns:
            Lista de sugerencias de ajuste
        """
        outside_tolerance = self.get_workers_outside_tolerance(is_weekend_only)
        suggestions = []
        
        # Separar trabajadores con exceso y déficit
        overassigned = [w for w in outside_tolerance if w['assigned_shifts'] > w['max_allowed']]
        underassigned = [w for w in outside_tolerance if w['assigned_shifts'] < w['min_allowed']]
        
        for over_worker in overassigned:
            excess = over_worker['assigned_shifts'] - over_worker['max_allowed']
            
            for under_worker in underassigned:
                deficit = under_worker['min_allowed'] - under_worker['assigned_shifts']
                
                # Sugerir transferencia
                transfer_amount = min(excess, deficit)
                if transfer_amount > 0:
                    suggestions.append({
                        'type': 'transfer',
                        'from_worker': over_worker['worker_id'],
                        'to_worker': under_worker['worker_id'],
                        'shifts_to_transfer': transfer_amount,
                        'is_weekend_only': is_weekend_only,
                        'priority': abs(over_worker['deviation_percentage']) + abs(under_worker['deviation_percentage'])
                    })
        
        # Ordenar por prioridad (mayor desviación = mayor prioridad)
        suggestions.sort(key=lambda x: x['priority'], reverse=True)
        
        return suggestions
    
    def _count_assigned_shifts(self, worker_id: str, is_weekend_only: bool = False) -> int:
        """
        Cuenta los shifts asignados a un trabajador
        
        Args:
            worker_id: ID del trabajador
            is_weekend_only: Si True, solo cuenta shifts de weekend
            
        Returns:
            Número de shifts asignados
        """
        count = 0
        holidays_set = set(self.scheduler.holidays)
        
        # Asegurar que tenemos el horario más actual
        if hasattr(self.scheduler, 'schedule') and self.scheduler.schedule:
            schedule_to_use = self.scheduler.schedule
        else:
            schedule_to_use = self.schedule
        
        for date, assigned_workers in schedule_to_use.items():
            # Contar cada puesto donde el trabajador está asignado
            if assigned_workers:  # Verificar que no sea None
                for worker_in_post in assigned_workers:
                    if worker_in_post == worker_id:
                        if is_weekend_only:
                            # Solo contar si es weekend o holiday
                            if (date.weekday() >= 4 or  # Friday=4, Saturday=5, Sunday=6
                                date in holidays_set or
                                (date + timedelta(days=1)) in holidays_set):
                                count += 1
                        else:
                            count += 1
        
        return count
    
    def _calculate_weekend_target(self, worker_id: str, total_target: int) -> int:
        """
        Calcula el target de shifts de weekend proporcional
        
        Args:
            worker_id: ID del trabajador
            total_target: Target total de shifts
            
        Returns:
            Target proporcional para weekends
        """
        if total_target <= 0:
            return 0
            
        # Contar total de días en el período
        total_days = (self.scheduler.end_date - self.scheduler.start_date).days + 1
        
        # Contar días de weekend en el período
        weekend_days = 0
        current_date = self.scheduler.start_date
        holidays_set = set(self.scheduler.holidays)
        
        while current_date <= self.scheduler.end_date:
            if (current_date.weekday() >= 4 or  # Friday, Saturday, Sunday
                current_date in holidays_set):
                weekend_days += 1
            current_date += timedelta(days=1)
        
        if weekend_days == 0 or total_days == 0:
            return 0
            
        # Calcular proporción de weekend
        weekend_proportion = weekend_days / total_days
        weekend_target = int(total_target * weekend_proportion + 0.5)  # Round to nearest
        
        return weekend_target
    
    def log_tolerance_report(self) -> None:
        """
        Genera y registra un reporte completo de tolerancia
        """
        logging.info("=== SHIFT TOLERANCE VALIDATION REPORT ===")
        
        # Reporte general de shifts
        general_validations = self.validate_all_workers()
        valid_count = sum(1 for v in general_validations if v['valid'])
        
        logging.info(f"General Shifts: {valid_count}/{len(general_validations)} workers within tolerance")
        
        for validation in general_validations:
            if not validation['valid']:
                logging.warning(
                    f"Worker {validation['worker_id']}: "
                    f"{validation['assigned_shifts']} shifts "
                    f"(target: {validation['target_shifts']}, "
                    f"allowed: {validation['min_allowed']}-{validation['max_allowed']}, "
                    f"deviation: {validation['deviation_percentage']:.1f}%)"
                )
        
        # Reporte de weekend shifts
        weekend_validations = self.validate_weekend_shifts()
        weekend_valid_count = sum(1 for v in weekend_validations if v['valid'])
        
        logging.info(f"Weekend Shifts: {weekend_valid_count}/{len(weekend_validations)} workers within tolerance")
        
        for validation in weekend_validations:
            if not validation['valid']:
                logging.warning(
                    f"Worker {validation['worker_id']} (weekends): "
                    f"{validation['assigned_shifts']} shifts "
                    f"(target: {validation['target_shifts']}, "
                    f"allowed: {validation['min_allowed']}-{validation['max_allowed']}, "
                    f"deviation: {validation['deviation_percentage']:.1f}%)"
                )
        
        # Sugerencias de ajuste
        general_suggestions = self.suggest_shift_adjustments(is_weekend_only=False)
        weekend_suggestions = self.suggest_shift_adjustments(is_weekend_only=True)
        
        if general_suggestions:
            logging.info("General shift adjustment suggestions:")
            for suggestion in general_suggestions[:5]:  # Top 5
                logging.info(f"  Transfer {suggestion['shifts_to_transfer']} shifts from "
                           f"{suggestion['from_worker']} to {suggestion['to_worker']}")
        
        if weekend_suggestions:
            logging.info("Weekend shift adjustment suggestions:")
            for suggestion in weekend_suggestions[:5]:  # Top 5
                logging.info(f"  Transfer {suggestion['shifts_to_transfer']} weekend shifts from "
                           f"{suggestion['from_worker']} to {suggestion['to_worker']}")
        
        logging.info("=== END TOLERANCE VALIDATION REPORT ===")