import logging
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
from functools import lru_cache


class OptimizationMetrics:
    """
    Clase para evaluar la calidad de las mejoras y métricas de optimización
    """
    
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.schedule_hash = None
        self.cached_metrics = {}
        self.iteration_history = []
        self.operation_performance = {}
        
        # Umbrales de mejora por tipo de operación
        self.quality_thresholds = {
            'fill_empty_shifts': 0.05,  # 5% mejora mínima
            'balance_workloads': 0.02,  # 2% mejora mínima  
            'balance_weekday_distribution': 0.015,  # 1.5% mejora mínima
            'improve_weekend_distribution': 0.01,  # 1% mejora mínima
            'distribute_holiday_shifts_proportionally': 0.02,  # 2% mejora mínima
            'rebalance_weekend_distribution': 0.01,  # 1% mejora mínima
            'adjust_last_post_distribution': 0.015,  # 1.5% mejora mínima
            'default': 0.01  # 1% mejora mínima por defecto
        }
    
    def get_schedule_hash(self) -> str:
        """Generar hash del estado actual del schedule"""
        try:
            schedule_items = []
            for date, workers in sorted(self.scheduler.schedule.items()):
                date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
                schedule_items.append((date_str, tuple(workers or [])))
            
            schedule_str = str(schedule_items)
            return hashlib.md5(schedule_str.encode()).hexdigest()
        except Exception as e:
            logging.warning(f"Error generating schedule hash: {e}")
            return datetime.now().isoformat()  # Fallback
    
    def calculate_overall_schedule_score(self) -> float:
        """Calcular un score general del schedule actual"""
        try:
            current_hash = self.get_schedule_hash()
            if current_hash == self.schedule_hash and 'overall_score' in self.cached_metrics:
                return self.cached_metrics['overall_score']
            
            # Componentes del score
            fill_rate_score = self._calculate_fill_rate_score()
            workload_balance_score = self._calculate_workload_balance_score()
            weekend_balance_score = self._calculate_weekend_balance_score()
            post_rotation_score = self._calculate_post_rotation_score()
            constraint_violations_penalty = self._calculate_constraint_violations_penalty()
            
            # Pesos para cada componente
            weights = {
                'fill_rate': 0.35,
                'workload_balance': 0.25,
                'weekend_balance': 0.15,
                'post_rotation': 0.15,
                'constraint_penalty': 0.10
            }
            
            overall_score = (
                fill_rate_score * weights['fill_rate'] +
                workload_balance_score * weights['workload_balance'] +
                weekend_balance_score * weights['weekend_balance'] +
                post_rotation_score * weights['post_rotation'] -
                constraint_violations_penalty * weights['constraint_penalty']
            )
            
            # Cachear resultado
            self.schedule_hash = current_hash
            self.cached_metrics['overall_score'] = max(0, min(100, overall_score))
            
            return self.cached_metrics['overall_score']
            
        except Exception as e:
            logging.error(f"Error calculating overall schedule score: {e}")
            return 0.0
    
    def _calculate_fill_rate_score(self) -> float:
        """Calcular score basado en el fill rate"""
        try:
            total_slots = 0
            filled_slots = 0
            
            for date, workers in self.scheduler.schedule.items():
                total_slots += len(workers)
                filled_slots += sum(1 for worker in workers if worker is not None)
            
            if total_slots == 0:
                return 100.0
            
            fill_rate = filled_slots / total_slots
            return fill_rate * 100
            
        except Exception as e:
            logging.error(f"Error calculating fill rate score: {e}")
            return 0.0
    
    def _calculate_workload_balance_score(self) -> float:
        """Calcular score basado en el balance de carga de trabajo"""
        try:
            if not self.scheduler.workers_data:
                return 100.0
            
            assignment_counts = []
            
            for worker in self.scheduler.workers_data:
                worker_id = worker['id']
                work_percentage = worker.get('work_percentage', 100)
                count = len(self.scheduler.worker_assignments.get(worker_id, set()))
                
                # Normalizar por porcentaje de trabajo
                if work_percentage > 0:
                    normalized_count = count * 100 / work_percentage
                    assignment_counts.append(normalized_count)
            
            if not assignment_counts:
                return 100.0
            
            # Calcular coeficiente de variación
            mean_count = sum(assignment_counts) / len(assignment_counts)
            if mean_count == 0:
                return 100.0
            
            variance = sum((count - mean_count) ** 2 for count in assignment_counts) / len(assignment_counts)
            cv = (variance ** 0.5) / mean_count  # Coeficiente de variación
            
            # Convertir a score (menor CV = mejor score)
            balance_score = max(0, 100 - (cv * 100))
            return balance_score
            
        except Exception as e:
            logging.error(f"Error calculating workload balance score: {e}")
            return 0.0
    
    def _calculate_weekend_balance_score(self) -> float:
        """Calcular score basado en el balance de fines de semana"""
        try:
            if not self.scheduler.workers_data:
                return 100.0
            
            weekend_counts = []
            
            for worker in self.scheduler.workers_data:
                worker_id = worker['id']
                assignments = self.scheduler.worker_assignments.get(worker_id, set())
                
                weekend_count = sum(
                    1 for date in assignments 
                    if hasattr(date, 'weekday') and date.weekday() >= 5  # Sábado y Domingo
                )
                weekend_counts.append(weekend_count)
            
            if not weekend_counts:
                return 100.0
            
            # Calcular variabilidad en asignaciones de fin de semana
            mean_weekend = sum(weekend_counts) / len(weekend_counts)
            if mean_weekend == 0:
                return 100.0
            
            variance = sum((count - mean_weekend) ** 2 for count in weekend_counts) / len(weekend_counts)
            cv = (variance ** 0.5) / mean_weekend
            
            balance_score = max(0, 100 - (cv * 80))  # Factor 80 para weekend balance
            return balance_score
            
        except Exception as e:
            logging.error(f"Error calculating weekend balance score: {e}")
            return 0.0
    
    def _calculate_post_rotation_score(self) -> float:
        """Calcular score basado en la rotación de puestos"""
        try:
            if not self.scheduler.workers_data or self.scheduler.num_shifts <= 1:
                return 100.0
            
            post_balance_scores = []
            
            for worker in self.scheduler.workers_data:
                worker_id = worker['id']
                assignments = self.scheduler.worker_assignments.get(worker_id, set())
                
                if not assignments:
                    continue
                
                # Contar asignaciones por puesto
                post_counts = {}
                for date in assignments:
                    if date in self.scheduler.schedule:
                        workers_in_posts = self.scheduler.schedule[date]
                        for post_idx, assigned_worker in enumerate(workers_in_posts):
                            if assigned_worker == worker_id:
                                post_counts[post_idx] = post_counts.get(post_idx, 0) + 1
                
                if post_counts:
                    expected_per_post = len(assignments) / self.scheduler.num_shifts
                    post_variance = sum(
                        (count - expected_per_post) ** 2 
                        for count in post_counts.values()
                    ) / len(post_counts)
                    
                    # Score basado en varianza (menor = mejor)
                    worker_score = max(0, 100 - (post_variance * 20))
                    post_balance_scores.append(worker_score)
            
            return sum(post_balance_scores) / len(post_balance_scores) if post_balance_scores else 100.0
            
        except Exception as e:
            logging.error(f"Error calculating post rotation score: {e}")
            return 0.0
    
    def _calculate_constraint_violations_penalty(self) -> float:
        """Calcular penalización por violaciones de restricciones"""
        try:
            penalty = 0.0
            
            # Verificar violaciones básicas usando el constraint checker si está disponible
            if hasattr(self.scheduler, 'constraint_checker'):
                # Esta sería una implementación simplificada
                # En la práctica, se podrían verificar violaciones específicas
                pass
            
            # Por ahora, devolver 0 (sin penalizaciones específicas implementadas)
            return penalty
            
        except Exception as e:
            logging.error(f"Error calculating constraint violations penalty: {e}")
            return 0.0
    
    def evaluate_improvement_quality(self, before_score: float, after_score: float, 
                                   operation_type: str) -> Tuple[bool, float]:
        """
        Evaluar la calidad de una mejora
        
        Returns:
            Tuple[bool, float]: (es_mejora_significativa, improvement_ratio)
        """
        try:
            if before_score <= 0:
                # Si el score anterior era 0 o negativo, cualquier mejora es buena
                is_significant = after_score > before_score
                improvement_ratio = float('inf') if before_score == 0 else after_score / before_score - 1
            else:
                improvement_ratio = (after_score - before_score) / before_score
                threshold = self.quality_thresholds.get(operation_type, self.quality_thresholds['default'])
                is_significant = improvement_ratio >= threshold
            
            return is_significant, improvement_ratio
            
        except Exception as e:
            logging.error(f"Error evaluating improvement quality: {e}")
            return False, 0.0
    
    def count_empty_shifts(self) -> int:
        """Contar turnos vacíos en el schedule"""
        try:
            empty_count = 0
            for date, workers in self.scheduler.schedule.items():
                empty_count += sum(1 for worker in workers if worker is None)
            return empty_count
        except Exception as e:
            logging.error(f"Error counting empty shifts: {e}")
            return 0
    
    def calculate_workload_imbalance(self) -> float:
        """Calcular el desbalance de carga de trabajo como ratio"""
        try:
            if not self.scheduler.workers_data:
                return 0.0
            
            assignment_counts = []
            
            for worker in self.scheduler.workers_data:
                worker_id = worker['id']
                work_percentage = worker.get('work_percentage', 100)
                count = len(self.scheduler.worker_assignments.get(worker_id, set()))
                
                if work_percentage > 0:
                    normalized_count = count * 100 / work_percentage
                    assignment_counts.append(normalized_count)
            
            if not assignment_counts:
                return 0.0
            
            max_count = max(assignment_counts)
            min_count = min(assignment_counts)
            
            if max_count == 0:
                return 0.0
            
            return (max_count - min_count) / max_count
            
        except Exception as e:
            logging.error(f"Error calculating workload imbalance: {e}")
            return 0.0
    
    def calculate_weekend_imbalance(self) -> float:
        """Calcular el desbalance de fines de semana"""
        try:
            if not self.scheduler.workers_data:
                return 0.0
            
            weekend_counts = []
            
            for worker in self.scheduler.workers_data:
                worker_id = worker['id']
                assignments = self.scheduler.worker_assignments.get(worker_id, set())
                
                weekend_count = sum(
                    1 for date in assignments 
                    if hasattr(date, 'weekday') and date.weekday() >= 5
                )
                weekend_counts.append(weekend_count)
            
            if not weekend_counts:
                return 0.0
            
            max_weekends = max(weekend_counts)
            min_weekends = min(weekend_counts)
            
            if max_weekends == 0:
                return 0.0
            
            return (max_weekends - min_weekends) / max_weekends
            
        except Exception as e:
            logging.error(f"Error calculating weekend imbalance: {e}")
            return 0.0
    
    def record_iteration_result(self, iteration: int, operation_results: Dict[str, Any], 
                              overall_score: float) -> None:
        """Registrar resultados de una iteración para análisis de tendencias"""
        try:
            iteration_data = {
                'iteration': iteration,
                'timestamp': datetime.now(),
                'overall_score': overall_score,
                'operations': operation_results.copy(),
                'improvements_count': sum(1 for result in operation_results.values() 
                                        if isinstance(result, dict) and result.get('improved', False))
            }
            
            self.iteration_history.append(iteration_data)
            
            # Mantener solo las últimas 10 iteraciones para análisis
            if len(self.iteration_history) > 10:
                self.iteration_history = self.iteration_history[-10:]
                
        except Exception as e:
            logging.error(f"Error recording iteration result: {e}")
    
    def should_continue_optimization(self, current_iteration: int) -> Tuple[bool, str]:
        """
        Determinar si debe continuar la optimización basándose en tendencias
        
        Returns:
            Tuple[bool, str]: (should_continue, reason)
        """
        try:
            if len(self.iteration_history) < 3:
                return True, "Insuficientes datos para análisis de tendencia"
            
            # Analizar últimas 3 iteraciones
            recent_iterations = self.iteration_history[-3:]
            
            # Verificar si hay mejoras en las últimas 3 iteraciones
            recent_improvements = [
                iteration['improvements_count'] 
                for iteration in recent_iterations
            ]
            
            if sum(recent_improvements) == 0:
                return False, "Sin mejoras en las últimas 3 iteraciones"
            
            # Analizar tendencia de scores
            recent_scores = [iteration['overall_score'] for iteration in recent_iterations]
            
            # Calcular mejora promedio
            score_improvements = []
            for i in range(1, len(recent_scores)):
                if recent_scores[i-1] > 0:
                    improvement = (recent_scores[i] - recent_scores[i-1]) / recent_scores[i-1]
                    score_improvements.append(improvement)
            
            if score_improvements:
                avg_improvement = sum(score_improvements) / len(score_improvements)
                
                # Si la mejora promedio es muy pequeña, considerar parar
                if avg_improvement < 0.001:  # 0.1% mejora promedio
                    return False, f"Mejoras marginales detectadas (avg: {avg_improvement:.4f})"
            
            # Verificar si se alcanzó un score excelente
            current_score = recent_scores[-1]
            if current_score >= 95.0:
                return False, f"Score excelente alcanzado: {current_score:.2f}"
            
            return True, f"Continuando optimización (score actual: {current_score:.2f})"
            
        except Exception as e:
            logging.error(f"Error in should_continue_optimization: {e}")
            return True, "Error en análisis, continuando por seguridad"