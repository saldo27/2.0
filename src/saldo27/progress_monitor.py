import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import sys


class ProgressMonitor:
    """
    Sistema de monitoreo visual de progreso para la optimización
    """
    
    def __init__(self, scheduler, total_iterations: int = 120):
        self.scheduler = scheduler
        self.total_iterations = total_iterations
        self.start_time = None
        self.iteration_times = []
        self.best_score = 0.0
        self.iteration_data = []
        self.last_significant_improvement = 0
        
        # Configuración de logging visual
        self.progress_bar_length = 30
        self.update_frequency = 1  # Actualizar cada iteración
    
    def start_monitoring(self):
        """Iniciar el monitoreo de progreso"""
        self.start_time = datetime.now()
        self.iteration_times = []
        self.best_score = 0.0
        self.iteration_data = []
        self.last_significant_improvement = 0
        
        logging.info("=" * 60)
        logging.info("🚀 INICIANDO OPTIMIZACIÓN DE HORARIOS")
        logging.info("=" * 60)
        logging.info(f"📊 Objetivo: {self.total_iterations} iteraciones máximas")
        logging.info(f"🕒 Inicio: {self.start_time.strftime('%H:%M:%S')}")
        logging.info("")
    
    def track_iteration_progress(self, iteration: int, operation_results: Dict[str, Any],
                               current_score: float, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Registrar y mostrar el progreso de una iteración
        
        Returns:
            Dict con datos de progreso para análisis posterior
        """
        try:
            iteration_start_time = datetime.now()
            
            # Calcular tiempo estimado
            if len(self.iteration_times) > 0:
                avg_time_per_iteration = sum(
                    (t.total_seconds() if hasattr(t, 'total_seconds') else t) 
                    for t in self.iteration_times
                ) / len(self.iteration_times)
                remaining_iterations = self.total_iterations - iteration
                estimated_remaining_seconds = avg_time_per_iteration * remaining_iterations
                eta = datetime.now() + timedelta(seconds=estimated_remaining_seconds)
            else:
                eta = None
            
            # Verificar si es una mejora significativa
            is_significant_improvement = current_score > self.best_score + 0.5
            if is_significant_improvement:
                self.best_score = current_score
                self.last_significant_improvement = iteration
            
            # Contar operaciones exitosas
            successful_operations = sum(
                1 for result in operation_results.values() 
                if isinstance(result, dict) and result.get('improved', False)
            ) if isinstance(operation_results, dict) else 0
            
            total_operations = len(operation_results) if isinstance(operation_results, dict) else 0
            
            # Crear datos de progreso
            progress_data = {
                'iteration': iteration,
                'current_score': current_score,
                'best_score': self.best_score,
                'successful_operations': successful_operations,
                'total_operations': total_operations,
                'metrics': metrics_data.copy() if metrics_data else {},
                'is_significant_improvement': is_significant_improvement,
                'iterations_since_improvement': iteration - self.last_significant_improvement,
                'eta': eta
            }
            
            # Mostrar progreso visual
            if iteration % self.update_frequency == 0 or is_significant_improvement:
                self._display_progress_bar(iteration, current_score, progress_data)
                self._display_detailed_metrics(progress_data, operation_results)
            
            # Guardar datos de iteración
            self.iteration_data.append(progress_data)
            
            # Registrar tiempo de iteración
            iteration_time = datetime.now() - iteration_start_time
            self.iteration_times.append(iteration_time)
            
            return progress_data
            
        except Exception as e:
            logging.error(f"Error en track_iteration_progress: {e}")
            return {'error': str(e), 'iteration': iteration}
    
    def _display_progress_bar(self, iteration: int, current_score: float, 
                            progress_data: Dict[str, Any]):
        """Mostrar barra de progreso visual"""
        try:
            # Calcular porcentaje de completitud
            progress_percent = min(100, (iteration / self.total_iterations) * 100)
            
            # Crear barra de progreso
            filled_length = int(self.progress_bar_length * progress_percent / 100)
            bar = "█" * filled_length + "░" * (self.progress_bar_length - filled_length)
            
            # Calcular tiempo transcurrido
            if self.start_time:
                elapsed_time = datetime.now() - self.start_time
                elapsed_str = str(elapsed_time).split('.')[0]  # Remover microsegundos
            else:
                elapsed_str = "00:00:00"
            
            # ETA
            eta_str = ""
            if progress_data.get('eta'):
                eta_str = f" | ETA: {progress_data['eta'].strftime('%H:%M:%S')}"
            
            # Indicador de mejora
            improvement_indicator = "🔥" if progress_data.get('is_significant_improvement') else ""
            
            # Mostrar barra de progreso
            logging.info(f"[{bar}] {progress_percent:5.1f}% | "
                        f"Iter: {iteration:3d}/{self.total_iterations} | "
                        f"Score: {current_score:6.2f} | "
                        f"Mejor: {progress_data['best_score']:6.2f} {improvement_indicator}")
            
            logging.info(f"⏱️  Tiempo: {elapsed_str}{eta_str} | "
                        f"Ops exitosas: {progress_data['successful_operations']}/{progress_data['total_operations']} | "
                        f"Sin mejoras: {progress_data['iterations_since_improvement']}")
            
        except Exception as e:
            logging.error(f"Error displaying progress bar: {e}")
    
    def _display_detailed_metrics(self, progress_data: Dict[str, Any], 
                                operation_results: Dict[str, Any]):
        """Mostrar métricas detalladas cuando hay mejoras significativas"""
        try:
            if not progress_data.get('is_significant_improvement'):
                return
            
            logging.info("🎯 MEJORA SIGNIFICATIVA DETECTADA!")
            
            metrics = progress_data.get('metrics', {})
            
            # Mostrar métricas principales
            if metrics:
                logging.info(f"📈 Métricas actuales:")
                if 'empty_shifts_count' in metrics:
                    logging.info(f"   • Turnos vacíos: {metrics['empty_shifts_count']}")
                if 'workload_imbalance' in metrics:
                    logging.info(f"   • Desbalance carga: {metrics['workload_imbalance']:.3f}")
                if 'weekend_imbalance' in metrics:
                    logging.info(f"   • Desbalance fines de semana: {metrics['weekend_imbalance']:.3f}")
            
            # Mostrar operaciones que tuvieron éxito
            if isinstance(operation_results, dict):
                successful_ops = [
                    name for name, result in operation_results.items()
                    if isinstance(result, dict) and result.get('improved', False)
                ]
                
                if successful_ops:
                    logging.info(f"✅ Operaciones exitosas: {', '.join(successful_ops[:3])}")
            
            logging.info("")  # Línea en blanco para separación
            
        except Exception as e:
            logging.error(f"Error displaying detailed metrics: {e}")
    
    def display_optimization_summary(self, final_iteration: int, final_score: float,
                                   termination_reason: str):
        """Mostrar resumen final de la optimización"""
        try:
            end_time = datetime.now()
            total_time = end_time - self.start_time if self.start_time else timedelta(0)
            
            logging.info("")
            logging.info("=" * 60)
            logging.info("🏁 OPTIMIZACIÓN COMPLETADA")
            logging.info("=" * 60)
            
            # Estadísticas generales
            logging.info(f"📊 Resumen de ejecución:")
            logging.info(f"   • Iteraciones ejecutadas: {final_iteration}/{self.total_iterations}")
            logging.info(f"   • Score final: {final_score:.2f}")
            logging.info(f"   • Mejor score: {self.best_score:.2f}")
            logging.info(f"   • Tiempo total: {str(total_time).split('.')[0]}")
            logging.info(f"   • Razón de terminación: {termination_reason}")
            
            # Estadísticas de rendimiento
            if self.iteration_times:
                avg_time_per_iteration = sum(self.iteration_times, timedelta(0)) / len(self.iteration_times)
                logging.info(f"   • Tiempo promedio por iteración: {avg_time_per_iteration.total_seconds():.2f}s")
            
            # Análisis de mejoras
            significant_improvements = sum(
                1 for data in self.iteration_data 
                if data.get('is_significant_improvement', False)
            )
            
            logging.info(f"   • Mejoras significativas: {significant_improvements}")
            
            if significant_improvements > 0:
                improvement_rate = (self.best_score / max(1, final_iteration)) * 100
                logging.info(f"   • Tasa de mejora: {improvement_rate:.2f} puntos/iteración")
            
            # Evaluación final
            if final_score >= 95:
                logging.info("🌟 EXCELENTE: Score objetivo alcanzado!")
            elif final_score >= 85:
                logging.info("👍 BUENO: Score satisfactorio")
            elif final_score >= 70:
                logging.info("⚠️  REGULAR: Puede requerir ajustes adicionales")
            else:
                logging.info("❌ BAJO: Requiere revisión de restricciones o datos")
            
            logging.info("=" * 60)
            logging.info("")
            
        except Exception as e:
            logging.error(f"Error displaying optimization summary: {e}")
    
    def get_performance_insights(self) -> Dict[str, Any]:
        """Obtener insights de rendimiento para análisis posterior"""
        try:
            if not self.iteration_data:
                return {'error': 'No hay datos de iteraciones disponibles'}
            
            # Análisis de tendencias
            scores = [data['current_score'] for data in self.iteration_data]
            
            insights = {
                'total_iterations': len(self.iteration_data),
                'final_score': scores[-1] if scores else 0,
                'best_score': self.best_score,
                'score_improvement': self.best_score - (scores[0] if scores else 0),
                'significant_improvements': sum(
                    1 for data in self.iteration_data 
                    if data.get('is_significant_improvement', False)
                ),
                'convergence_iteration': self.last_significant_improvement,
                'average_operations_success_rate': 0
            }
            
            # Calcular tasa de éxito promedio de operaciones
            success_rates = []
            for data in self.iteration_data:
                if data.get('total_operations', 0) > 0:
                    rate = data.get('successful_operations', 0) / data['total_operations']
                    success_rates.append(rate)
            
            if success_rates:
                insights['average_operations_success_rate'] = sum(success_rates) / len(success_rates)
            
            # Tiempo de ejecución
            if self.start_time:
                total_time = datetime.now() - self.start_time
                insights['total_execution_time_seconds'] = total_time.total_seconds()
            
            return insights
            
        except Exception as e:
            logging.error(f"Error getting performance insights: {e}")
            return {'error': str(e)}
    
    def should_display_detailed_progress(self, iteration: int) -> bool:
        """Determinar si mostrar progreso detallado en esta iteración"""
        # Mostrar siempre en las primeras 5 iteraciones
        if iteration <= 5:
            return True
        
        # Mostrar cada 10 iteraciones después
        if iteration % 10 == 0:
            return True
        
        # Mostrar siempre que haya mejora significativa
        if self.iteration_data and self.iteration_data[-1].get('is_significant_improvement'):
            return True
        
        return False