"""
Balance Validator - Validación estricta de balance de turnos
=============================================================

Sistema que garantiza que las desviaciones de turnos se mantengan dentro de límites
estrictamente controlados durante todo el proceso de optimización.

SISTEMA DE TOLERANCIA POR FASES:
- Fase 1 (Initial): ±8% tolerancia objetivo estricta
- Fase 2 (Emergency): ±12% LÍMITE ABSOLUTO (solo si cobertura < 95%)
- Crítico: >12% NUNCA debe ocurrir (sistema debe bloquear)

IMPORTANTE: Este validador clasifica violaciones. El enforcement activo
está en schedule_builder._would_violate_tolerance()
"""

import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime


class BalanceValidator:
    """Validador estricto de balance de turnos con sistema de fases"""
    
    def __init__(self, tolerance_percentage: float = 8.0):
        """
        Initialize balance validator
        
        Args:
            tolerance_percentage: Tolerancia objetivo en porcentaje (default: 8% para Fase 1)
        """
        self.tolerance_percentage = tolerance_percentage
        self.emergency_limit = 12.0  # Fase 2: LÍMITE ABSOLUTO ±12%
        self.critical_threshold = 12.0  # Cualquier cosa >12% es un error del sistema
        
        logging.info(f"BalanceValidator initialized with phase system:")
        logging.info(f"  Phase 1 target: ±{tolerance_percentage}%")
        logging.info(f"  Phase 2 ABSOLUTE LIMIT: ±{self.emergency_limit}%")
        logging.info(f"  Critical threshold: >{self.critical_threshold}% (SHOULD NEVER OCCUR)")
    
    def validate_schedule_balance(self, schedule: Dict, workers_data: List[Dict]) -> Dict:
        """
        Valida el balance completo del horario
        
        Args:
            schedule: Horario actual
            workers_data: Datos de trabajadores con target_shifts
            
        Returns:
            Dict con estadísticas de balance y violaciones
        """
        violations = {
            'within_tolerance': [],     # Fase 1: Dentro de ±8%
            'within_emergency': [],     # Fase 2: Entre 8% y 12% (límite absoluto)
            'critical': [],             # >12% (NO debería ocurrir - error del sistema)
            'extreme': []               # >12% (deprecated, same as critical now)
        }
        
        stats = {
            'total_workers': len(workers_data),
            'max_deviation': 0.0,
            'avg_deviation': 0.0,
            'total_deviation': 0.0
        }
        
        for worker in workers_data:
            worker_id = worker['id']
            target_shifts = worker.get('target_shifts', 0)
            
            if target_shifts == 0:
                continue
            
            # Contar turnos asignados
            assigned_shifts = self._count_worker_shifts(worker_id, schedule)
            
            # Calcular desviación
            deviation = assigned_shifts - target_shifts
            deviation_percentage = (deviation / target_shifts * 100) if target_shifts > 0 else 0
            abs_deviation = abs(deviation_percentage)
            
            worker_info = {
                'worker_id': worker_id,
                'target': target_shifts,
                'assigned': assigned_shifts,
                'deviation': deviation,
                'deviation_percentage': deviation_percentage,
                'abs_deviation': abs_deviation
            }
            
            # Clasificar por severidad según sistema de fases
            if abs_deviation <= self.tolerance_percentage:
                # Fase 1: Within target
                violations['within_tolerance'].append(worker_info)
            elif abs_deviation <= self.emergency_limit:
                # Fase 2: Within absolute limit (should only occur if Phase 2 activated)
                violations['within_emergency'].append(worker_info)
            else:
                # >12% = CRITICAL ERROR - system should have blocked this
                violations['critical'].append(worker_info)
                violations['extreme'].append(worker_info)  # Keep for backward compatibility
            
            # Actualizar estadísticas
            stats['max_deviation'] = max(stats['max_deviation'], abs_deviation)
            stats['total_deviation'] += abs_deviation
        
        if stats['total_workers'] > 0:
            stats['avg_deviation'] = stats['total_deviation'] / stats['total_workers']
        
        # Log resumen con sistema de fases
        logging.info(f"📊 Balance Validation Summary (Phase System):")
        logging.info(f"   Phase 1 target (≤{self.tolerance_percentage}%): {len(violations['within_tolerance'])} workers")
        logging.info(f"   Phase 2 range ({self.tolerance_percentage}%-{self.emergency_limit}%): {len(violations['within_emergency'])} workers")
        logging.info(f"   CRITICAL - Beyond absolute limit (>{self.emergency_limit}%): {len(violations['critical'])} workers")
        logging.info(f"   Max deviation: {stats['max_deviation']:.1f}%")
        logging.info(f"   Avg deviation: {stats['avg_deviation']:.1f}%")
        
        # Warnings para problemas críticos
        if violations['critical']:
            logging.error(f"🚨 SYSTEM ERROR: {len(violations['critical'])} workers EXCEED ±12% ABSOLUTE LIMIT:")
            for worker_info in violations['critical']:
                logging.error(f"      {worker_info['worker_id']}: {worker_info['deviation_percentage']:+.1f}% "
                            f"({worker_info['assigned']}/{worker_info['target']} shifts)")
        
        if violations['critical']:
            logging.warning(f"⚠️  {len(violations['critical'])} workers with CRITICAL deviations:")
            for worker_info in violations['critical']:
                logging.warning(f"      {worker_info['worker_id']}: {worker_info['deviation_percentage']:+.1f}% "
                              f"({worker_info['assigned']}/{worker_info['target']} shifts)")
        
        return {
            'violations': violations,
            'stats': stats,
            'is_balanced': len(violations['critical']) == 0 and len(violations['extreme']) == 0
        }
    
    def _count_worker_shifts(self, worker_id: str, schedule: Dict) -> int:
        """Cuenta los turnos asignados a un trabajador"""
        count = 0
        
        for date, assignments in schedule.items():
            if assignments:
                for worker in assignments:
                    # Comparar con diferentes formatos de ID
                    if worker == worker_id or worker == f"Worker {worker_id}" or str(worker) == str(worker_id):
                        count += 1
        
        return count
    
    def get_rebalancing_recommendations(self, schedule: Dict, workers_data: List[Dict]) -> List[Dict]:
        """
        Obtiene recomendaciones específicas para rebalancear el horario
        
        Returns:
            Lista de recomendaciones ordenadas por prioridad
        """
        validation_result = self.validate_schedule_balance(schedule, workers_data)
        violations = validation_result['violations']
        
        recommendations = []
        
        # Combinar trabajadores con exceso y déficit
        overloaded = violations['extreme'] + violations['critical'] + violations['within_emergency']
        overloaded = [w for w in overloaded if w['deviation'] > 0]
        overloaded.sort(key=lambda x: x['abs_deviation'], reverse=True)
        
        underloaded = violations['extreme'] + violations['critical'] + violations['within_emergency']
        underloaded = [w for w in underloaded if w['deviation'] < 0]
        underloaded.sort(key=lambda x: x['abs_deviation'], reverse=True)
        
        # Generar recomendaciones de transferencia
        for over_worker in overloaded:
            for under_worker in underloaded:
                # Calcular cuántos turnos transferir
                over_excess = over_worker['assigned'] - over_worker['target']
                under_deficit = under_worker['target'] - under_worker['assigned']
                
                shifts_to_transfer = min(over_excess, under_deficit)
                
                if shifts_to_transfer > 0:
                    priority = over_worker['abs_deviation'] + under_worker['abs_deviation']
                    
                    recommendations.append({
                        'from_worker': over_worker['worker_id'],
                        'to_worker': under_worker['worker_id'],
                        'shifts_to_transfer': shifts_to_transfer,
                        'priority': priority,
                        'from_deviation': over_worker['deviation_percentage'],
                        'to_deviation': under_worker['deviation_percentage']
                    })
        
        # Ordenar por prioridad
        recommendations.sort(key=lambda x: x['priority'], reverse=True)
        
        if recommendations:
            logging.info(f"💡 Top rebalancing recommendations:")
            for i, rec in enumerate(recommendations[:5], 1):
                logging.info(f"   {i}. Transfer {rec['shifts_to_transfer']} shifts from "
                           f"{rec['from_worker']} ({rec['from_deviation']:+.1f}%) to "
                           f"{rec['to_worker']} ({rec['to_deviation']:+.1f}%)")
        
        return recommendations
    
    def check_transfer_validity(self, from_worker_id: str, to_worker_id: str,
                               schedule: Dict, workers_data: List[Dict]) -> Tuple[bool, str]:
        """
        Verifica si una transferencia de turno mejoraría el balance global
        
        Returns:
            (is_valid, reason)
        """
        # Encontrar datos de trabajadores
        from_worker = next((w for w in workers_data if w['id'] == from_worker_id), None)
        to_worker = next((w for w in workers_data if w['id'] == to_worker_id), None)
        
        if not from_worker or not to_worker:
            return False, "Worker not found"
        
        # Calcular estado actual
        from_assigned = self._count_worker_shifts(from_worker_id, schedule)
        to_assigned = self._count_worker_shifts(to_worker_id, schedule)
        
        from_target = from_worker.get('target_shifts', 0)
        to_target = to_worker.get('target_shifts', 0)
        
        # Calcular desviaciones actuales
        from_deviation = abs(from_assigned - from_target) / from_target * 100 if from_target > 0 else 0
        to_deviation = abs(to_assigned - to_target) / to_target * 100 if to_target > 0 else 0
        
        # Calcular desviaciones después de la transferencia
        from_deviation_after = abs(from_assigned - 1 - from_target) / from_target * 100 if from_target > 0 else 0
        to_deviation_after = abs(to_assigned + 1 - to_target) / to_target * 100 if to_target > 0 else 0
        
        # Verificar que ambas desviaciones mejoren o al menos no empeoren
        from_improves = from_deviation_after < from_deviation
        to_improves = to_deviation_after < to_deviation
        
        # La transferencia es válida si:
        # 1. Ambos mejoran, o
        # 2. Uno mejora significativamente y el otro no empeora mucho
        if from_improves and to_improves:
            return True, "Both workers improve"
        elif from_improves and to_deviation_after <= self.emergency_limit:
            return True, "Source improves, destination stays within emergency limit"
        elif to_improves and from_deviation_after <= self.emergency_limit:
            return True, "Destination improves, source stays within emergency limit"
        else:
            return False, f"Transfer would worsen balance (from: {from_deviation:.1f}%→{from_deviation_after:.1f}%, to: {to_deviation:.1f}%→{to_deviation_after:.1f}%)"
