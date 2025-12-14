import logging
from typing import Dict, List, Tuple, Callable, Any
from datetime import datetime


class OperationPrioritizer:
    """
    Sistema de priorización dinámica de operaciones de optimización
    """
    
    def __init__(self, scheduler, metrics):
        self.scheduler = scheduler
        self.metrics = metrics
        
        # Configuración de prioridades base
        self.base_priorities = {
            'fill_empty_shifts': 10,  # Máxima prioridad
            'balance_workloads': 8,
            'balance_weekday_distribution': 6,
            'improve_weekend_distribution': 5,
            'distribute_holiday_shifts_proportionally': 4,
            'rebalance_weekend_distribution': 3,
            'adjust_last_post_distribution': 2,
            'synchronize_tracking_data': 1  # Mínima prioridad
        }
        
        # Umbrales para activación de operaciones urgentes
        self.urgency_thresholds = {
            'empty_shifts_critical': 10,  # Si hay más de 10 turnos vacíos
            'workload_imbalance_critical': 0.25,  # 25% de desbalance
            'weekend_imbalance_critical': 0.30,  # 30% de desbalance en fines de semana
        }
    
    def prioritize_operations_dynamically(self) -> List[Tuple[str, Callable, int]]:
        """
        Priorizar operaciones basándose en el estado actual del schedule
        
        Returns:
            List[Tuple[str, callable, priority]]: Lista de (nombre, función, prioridad)
        """
        try:
            # Evaluar estado actual
            empty_shifts_count = self.metrics.count_empty_shifts()
            workload_imbalance = self.metrics.calculate_workload_imbalance()
            weekend_imbalance = self.metrics.calculate_weekend_imbalance()
            
            logging.info(f"Estado actual - Turnos vacíos: {empty_shifts_count}, "
                        f"Desbalance carga: {workload_imbalance:.3f}, "
                        f"Desbalance fines de semana: {weekend_imbalance:.3f}")
            
            # Crear lista de operaciones con prioridades ajustadas
            prioritized_operations = []
            
            # Operaciones críticas (máxima prioridad)
            if empty_shifts_count > self.urgency_thresholds['empty_shifts_critical']:
                logging.info("Activando modo crítico: muchos turnos vacíos")
                prioritized_operations.extend([
                    ("fill_empty_shifts_urgent", 
                     self.scheduler.schedule_builder._try_fill_empty_shifts, 15),
                    ("fill_empty_shifts_relaxed", 
                     lambda: self._try_fill_empty_shifts_relaxed(), 14),
                ])
            
            if workload_imbalance > self.urgency_thresholds['workload_imbalance_critical']:
                logging.info("Activando modo crítico: gran desbalance de carga")
                prioritized_operations.append(
                    ("balance_workloads_aggressive", 
                     lambda: self._balance_workloads_aggressive(), 13)
                )
            
            if weekend_imbalance > self.urgency_thresholds['weekend_imbalance_critical']:
                logging.info("Activando modo crítico: gran desbalance de fines de semana")
                prioritized_operations.append(
                    ("improve_weekend_distribution_aggressive", 
                     lambda: self._improve_weekend_distribution_aggressive(), 12)
                )
            
            # Operaciones estándar con prioridades ajustadas
            standard_operations = self._get_standard_operations_with_adjusted_priority(
                empty_shifts_count, workload_imbalance, weekend_imbalance
            )
            
            prioritized_operations.extend(standard_operations)
            
            # Ordenar por prioridad (mayor a menor)
            prioritized_operations.sort(key=lambda x: x[2], reverse=True)
            
            # Log de la priorización final
            logging.info("Orden de operaciones priorizado:")
            for i, (name, _, priority) in enumerate(prioritized_operations[:8], 1):
                logging.info(f"  {i}. {name} (prioridad: {priority})")
            
            return prioritized_operations
            
        except Exception as e:
            logging.error(f"Error en priorización dinámica: {e}")
            return self._get_fallback_operations()
    
    def _get_standard_operations_with_adjusted_priority(self, empty_shifts: int, 
                                                       workload_imbalance: float,
                                                       weekend_imbalance: float) -> List[Tuple[str, Callable, int]]:
        """Obtener operaciones estándar con prioridades ajustadas"""
        
        operations = []
        
        # Ajustar prioridad de llenado de turnos vacíos
        fill_priority = self.base_priorities['fill_empty_shifts']
        if empty_shifts > 5:
            fill_priority += 2
        elif empty_shifts > 2:
            fill_priority += 1
        
        operations.append((
            "fill_empty_shifts", 
            self.scheduler.schedule_builder._try_fill_empty_shifts, 
            fill_priority
        ))
        
        # Ajustar prioridad de balance de carga
        workload_priority = self.base_priorities['balance_workloads']
        if workload_imbalance > 0.15:
            workload_priority += 2
        elif workload_imbalance > 0.08:
            workload_priority += 1
        
        operations.append((
            "balance_workloads", 
            self.scheduler.schedule_builder._balance_workloads, 
            workload_priority
        ))
        
        # Balance de días de semana
        operations.append((
            "balance_weekday_distribution", 
            self.scheduler.schedule_builder._balance_weekday_distribution, 
            self.base_priorities['balance_weekday_distribution']
        ))
        
        # Segunda pasada de llenado si hay turnos vacíos
        if empty_shifts > 0:
            operations.append((
                "fill_empty_shifts_2", 
                self.scheduler.schedule_builder._try_fill_empty_shifts, 
                fill_priority - 1
            ))
        
        # Segunda pasada de balance de carga si hay desbalance
        if workload_imbalance > 0.05:
            operations.append((
                "balance_workloads_2", 
                self.scheduler.schedule_builder._balance_workloads, 
                workload_priority - 1
            ))
        
        # CRITICAL: Balance agresivo de targets si hay desbalance
        if workload_imbalance > 0.08:  # >8% desbalance (reducido de 10%)
            operations.append((
                "balance_target_shifts_aggressively",
                self.scheduler.schedule_builder._balance_target_shifts_aggressively,
                13  # Prioridad muy alta (aumentada de 12)
            ))
            logging.info(f"⚠️ Activado balance AGRESIVO de targets (desbalance: {workload_imbalance:.1%})")
        
        # EXTRA: Segunda pasada de balance agresivo si desbalance muy alto
        if workload_imbalance > 0.15:  # >15% desbalance
            operations.append((
                "balance_target_shifts_aggressively_2",
                self.scheduler.schedule_builder._balance_target_shifts_aggressively,
                14  # Prioridad máxima
            ))
            logging.warning(f"🚨 Activado balance ULTRA-AGRESIVO (desbalance crítico: {workload_imbalance:.1%})")
        
        # Ajustar prioridad de distribución de fines de semana
        weekend_priority = self.base_priorities['improve_weekend_distribution']
        if weekend_imbalance > 0.20:
            weekend_priority += 2
        elif weekend_imbalance > 0.10:
            weekend_priority += 1
        
        operations.extend([
            ("improve_weekend_distribution_1", 
             self.scheduler.schedule_builder._improve_weekend_distribution, 
             weekend_priority),
            
            ("distribute_holiday_shifts_proportionally", 
             self.scheduler.schedule_builder.distribute_holiday_shifts_proportionally, 
             self.base_priorities['distribute_holiday_shifts_proportionally']),
            
            ("rebalance_weekend_distribution", 
             self.scheduler.schedule_builder.rebalance_weekend_distribution, 
             self.base_priorities['rebalance_weekend_distribution']),
            
            ("synchronize_tracking_data", 
             self.scheduler.schedule_builder._synchronize_tracking_data, 
             self.base_priorities['synchronize_tracking_data']),
            
            ("improve_weekend_distribution_2", 
             self.scheduler.schedule_builder._improve_weekend_distribution, 
             weekend_priority - 1),
            
            ("adjust_last_post_distribution", 
             self.scheduler.schedule_builder._adjust_last_post_distribution, 
             self.base_priorities['adjust_last_post_distribution'])
        ])
        
        return operations
    
    def _try_fill_empty_shifts_relaxed(self) -> bool:
        """Versión relajada del llenado de turnos vacíos"""
        try:
            # Esta sería una implementación que usa niveles de relajación más altos
            # Por ahora, llamamos al método estándar
            # En una implementación completa, se podría pasar relaxation_level=2
            logging.info("Ejecutando llenado de turnos con restricciones relajadas")
            return self.scheduler.schedule_builder._try_fill_empty_shifts()
        except Exception as e:
            logging.error(f"Error en fill_empty_shifts_relaxed: {e}")
            return False
    
    def _balance_workloads_aggressive(self) -> bool:
        """Versión agresiva del balance de cargas de trabajo"""
        try:
            # Esta sería una implementación más agresiva del balance
            # Por ahora, llamamos al método estándar
            logging.info("Ejecutando balance de cargas agresivo")
            return self.scheduler.schedule_builder._balance_workloads()
        except Exception as e:
            logging.error(f"Error en balance_workloads_aggressive: {e}")
            return False
    
    def _improve_weekend_distribution_aggressive(self) -> bool:
        """Versión agresiva de mejora de distribución de fines de semana"""
        try:
            logging.info("Ejecutando mejora agresiva de distribución de fines de semana")
            return self.scheduler.schedule_builder._improve_weekend_distribution()
        except Exception as e:
            logging.error(f"Error en improve_weekend_distribution_aggressive: {e}")
            return False
    
    def _get_fallback_operations(self) -> List[Tuple[str, Callable, int]]:
        """Obtener lista de operaciones por defecto en caso de error"""
        return [
            ("fill_empty_shifts", 
             self.scheduler.schedule_builder._try_fill_empty_shifts, 10),
            ("balance_workloads", 
             self.scheduler.schedule_builder._balance_workloads, 8),
            ("balance_weekday_distribution", 
             self.scheduler.schedule_builder._balance_weekday_distribution, 6),
            ("improve_weekend_distribution", 
             self.scheduler.schedule_builder._improve_weekend_distribution, 5),
            ("distribute_holiday_shifts_proportionally", 
             self.scheduler.schedule_builder.distribute_holiday_shifts_proportionally, 4),
            ("rebalance_weekend_distribution", 
             self.scheduler.schedule_builder.rebalance_weekend_distribution, 3),
            ("synchronize_tracking_data", 
             self.scheduler.schedule_builder._synchronize_tracking_data, 2),
            ("adjust_last_post_distribution", 
             self.scheduler.schedule_builder._adjust_last_post_distribution, 1)
        ]
    
    def analyze_operation_effectiveness(self, operation_name: str, 
                                      before_score: float, after_score: float,
                                      execution_time: float) -> Dict[str, Any]:
        """Analizar la efectividad de una operación"""
        try:
            is_improved, improvement_ratio = self.metrics.evaluate_improvement_quality(
                before_score, after_score, operation_name
            )
            
            effectiveness_data = {
                'operation': operation_name,
                'improved': is_improved,
                'improvement_ratio': improvement_ratio,
                'before_score': before_score,
                'after_score': after_score,
                'execution_time_seconds': execution_time,
                'effectiveness_score': self._calculate_effectiveness_score(
                    improvement_ratio, execution_time
                )
            }
            
            return effectiveness_data
            
        except Exception as e:
            logging.error(f"Error analyzing operation effectiveness: {e}")
            return {
                'operation': operation_name,
                'improved': False,
                'improvement_ratio': 0.0,
                'error': str(e)
            }
    
    def _calculate_effectiveness_score(self, improvement_ratio: float, 
                                     execution_time: float) -> float:
        """Calcular un score de efectividad que considera mejora vs tiempo"""
        try:
            if execution_time <= 0:
                execution_time = 0.001  # Evitar división por cero
            
            # Score basado en mejora por segundo
            effectiveness = improvement_ratio / execution_time
            
            # Normalizar a un rango más manejable
            normalized_effectiveness = min(100, max(0, effectiveness * 1000))
            
            return normalized_effectiveness
            
        except Exception as e:
            logging.error(f"Error calculating effectiveness score: {e}")
            return 0.0
    
    def should_skip_operation(self, operation_name: str, current_state: Dict[str, Any]) -> Tuple[bool, str]:
        """Determinar si una operación debe saltarse basándose en el estado actual"""
        try:
            # Si no hay turnos vacíos, saltar las operaciones de llenado secundarias
            if operation_name.startswith('fill_empty_shifts') and operation_name != 'fill_empty_shifts':
                empty_count = current_state.get('empty_shifts_count', 0)
                if empty_count == 0:
                    return True, "No hay turnos vacíos para llenar"
            
            # Si el desbalance es muy bajo, saltar operaciones de balance secundarias
            if operation_name.endswith('_2') and 'balance' in operation_name:
                workload_imbalance = current_state.get('workload_imbalance', 0)
                if workload_imbalance < 0.02:  # 2%
                    return True, "Desbalance ya es muy bajo"
            
            # Si la distribución de fines de semana es buena, saltar segundas pasadas
            if operation_name == 'improve_weekend_distribution_2':
                weekend_imbalance = current_state.get('weekend_imbalance', 0)
                if weekend_imbalance < 0.05:  # 5%
                    return True, "Distribución de fines de semana ya es buena"
            
            return False, "Operación necesaria"
            
        except Exception as e:
            logging.error(f"Error checking if operation should be skipped: {e}")
            return False, "Error en verificación, ejecutando por seguridad"