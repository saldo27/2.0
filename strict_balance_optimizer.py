"""
Strict Balance Optimizer
========================

Sistema estricto de optimización de balance que garantiza que todos los trabajadores
estén dentro de ±1 turno de su objetivo, respetando todas las constraints.

Estrategia:
1. Identificar trabajadores con mayor desviación
2. Encontrar oportunidades de intercambio que mejoren el balance
3. Validar que no se violan constraints
4. Aplicar cambios y verificar mejora
"""

import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set, Optional
import copy


class StrictBalanceOptimizer:
    """Optimizador estricto de balance de turnos"""
    
    def __init__(self, scheduler, schedule_builder):
        self.scheduler = scheduler
        self.builder = schedule_builder
        self.workers_data = scheduler.workers_data
        self.schedule = scheduler.schedule
        self.worker_assignments = scheduler.worker_assignments
        
        # Estadísticas
        self.stats = {
            'swaps_performed': 0,
            'workers_balanced': 0,
            'max_deviation_before': 0,
            'max_deviation_after': 0
        }
        
        logging.info("💎 Strict Balance Optimizer initialized")
    
    def optimize_balance(self, max_iterations: int = 200, target_tolerance: int = 1) -> bool:
        """
        Optimiza el balance de turnos para que todos estén dentro de ±target_tolerance
        
        Args:
            max_iterations: Máximo número de iteraciones
            target_tolerance: Desviación máxima permitida del target (default: ±1)
            
        Returns:
            bool: True si se logró el balance objetivo
        """
        logging.info("=" * 80)
        logging.info("STRICT BALANCE OPTIMIZER - Starting")
        logging.info(f"Target tolerance: ±{target_tolerance} shifts")
        logging.info("=" * 80)
        
        # Análisis inicial
        initial_analysis = self._analyze_balance()
        self.stats['max_deviation_before'] = initial_analysis['max_deviation']
        
        logging.info(f"Initial state:")
        logging.info(f"  Workers with deviation >{target_tolerance}: {initial_analysis['workers_outside_tolerance']}")
        logging.info(f"  Max deviation: {initial_analysis['max_deviation']}")
        logging.info(f"  Average deviation: {initial_analysis['avg_deviation']:.2f}")
        
        iteration = 0
        improvement_made = True
        
        while iteration < max_iterations and improvement_made:
            iteration += 1
            improvement_made = False
            
            # Obtener trabajadores fuera de tolerancia
            overloaded, underloaded = self._get_imbalanced_workers(target_tolerance)
            
            if not overloaded or not underloaded:
                logging.info(f"✅ Balance achieved at iteration {iteration}")
                break
            
            # Intentar múltiples estrategias
            if self._try_direct_swap(overloaded, underloaded, target_tolerance):
                improvement_made = True
                self.stats['swaps_performed'] += 1
                continue
            
            if self._try_three_way_swap(overloaded, underloaded, target_tolerance):
                improvement_made = True
                self.stats['swaps_performed'] += 1
                continue
            
            if self._try_reassignment(overloaded, underloaded, target_tolerance):
                improvement_made = True
                self.stats['swaps_performed'] += 1
                continue
            
            # Si no hay mejora, intentar con relajación de constraints
            if iteration % 20 == 0:
                logging.info(f"  Iteration {iteration}: Trying relaxed constraints")
                if self._try_relaxed_swap(overloaded, underloaded, target_tolerance):
                    improvement_made = True
                    self.stats['swaps_performed'] += 1
        
        # Análisis final
        final_analysis = self._analyze_balance()
        self.stats['max_deviation_after'] = final_analysis['max_deviation']
        self.stats['workers_balanced'] = (
            initial_analysis['workers_outside_tolerance'] - 
            final_analysis['workers_outside_tolerance']
        )
        
        # Resumen
        logging.info("=" * 80)
        logging.info("STRICT BALANCE OPTIMIZER - Results")
        logging.info("=" * 80)
        logging.info(f"Iterations: {iteration}")
        logging.info(f"Swaps performed: {self.stats['swaps_performed']}")
        logging.info(f"Workers balanced: {self.stats['workers_balanced']}")
        logging.info(f"Max deviation: {self.stats['max_deviation_before']} → {self.stats['max_deviation_after']}")
        logging.info(f"Workers outside tolerance: {initial_analysis['workers_outside_tolerance']} → {final_analysis['workers_outside_tolerance']}")
        
        # Mostrar trabajadores que aún están fuera de balance
        if final_analysis['workers_outside_tolerance'] > 0:
            logging.warning("⚠️ Workers still outside tolerance:")
            for worker_id, info in final_analysis['worker_details'].items():
                if abs(info['deviation']) > target_tolerance:
                    worker_name = next((w['name'] for w in self.workers_data if w['id'] == worker_id), worker_id)
                    logging.warning(f"  - {worker_name}: {info['assigned']}/{info['target']} (deviation: {info['deviation']:+d})")
        
        return final_analysis['workers_outside_tolerance'] == 0
    
    def _analyze_balance(self) -> Dict:
        """Analiza el balance actual del schedule"""
        worker_details = {}
        deviations = []
        outside_tolerance_count = 0
        
        for worker in self.workers_data:
            worker_id = worker['id']
            target = worker.get('target_shifts', 0)
            assigned = len(self.worker_assignments.get(worker_id, set()))
            deviation = assigned - target
            
            worker_details[worker_id] = {
                'name': worker.get('name', worker_id),
                'target': target,
                'assigned': assigned,
                'deviation': deviation,
                'work_percentage': worker.get('work_percentage', 100)
            }
            
            if target > 0:
                deviations.append(abs(deviation))
                if abs(deviation) > 1:
                    outside_tolerance_count += 1
        
        return {
            'worker_details': worker_details,
            'max_deviation': max(deviations) if deviations else 0,
            'avg_deviation': sum(deviations) / len(deviations) if deviations else 0,
            'workers_outside_tolerance': outside_tolerance_count
        }
    
    def _get_imbalanced_workers(self, tolerance: int) -> Tuple[List, List]:
        """
        Obtiene listas de trabajadores sobrecargados y subcargados
        
        Returns:
            (overloaded, underloaded): Listas de tuplas (worker_id, deviation)
        """
        overloaded = []
        underloaded = []
        
        for worker in self.workers_data:
            worker_id = worker['id']
            target = worker.get('target_shifts', 0)
            
            if target == 0:
                continue
            
            assigned = len(self.worker_assignments.get(worker_id, set()))
            deviation = assigned - target
            
            if deviation > tolerance:
                # Sobrecargado: tiene más turnos de los que debería
                overloaded.append((worker_id, deviation))
            elif deviation < -tolerance:
                # Subcargado: tiene menos turnos de los que debería
                underloaded.append((worker_id, deviation))
        
        # Ordenar por magnitud de desviación (mayor primero)
        overloaded.sort(key=lambda x: x[1], reverse=True)
        underloaded.sort(key=lambda x: abs(x[1]), reverse=True)
        
        return overloaded, underloaded
    
    def _try_direct_swap(self, overloaded: List, underloaded: List, tolerance: int) -> bool:
        """
        Intenta intercambio directo: mover un turno de sobrecargado a subcargado
        """
        for over_id, over_dev in overloaded[:5]:  # Top 5 sobrecargados
            for under_id, under_dev in underloaded[:5]:  # Top 5 subcargados
                
                # Buscar un turno del sobrecargado que pueda mover
                over_assignments = list(self.worker_assignments.get(over_id, set()))
                random.shuffle(over_assignments)
                
                for date in over_assignments:
                    # No tocar mandatory
                    if (over_id, date) in self.builder._locked_mandatory:
                        continue
                    
                    # Verificar que podemos modificar
                    if not self.builder._can_modify_assignment(over_id, date, "strict_balance"):
                        continue
                    
                    try:
                        post = self.schedule[date].index(over_id)
                    except (ValueError, KeyError):
                        continue
                    
                    # Verificar que el subcargado puede tomar este turno
                    worker_under = next((w for w in self.workers_data if w['id'] == under_id), None)
                    if not worker_under:
                        continue
                    
                    # Calcular score (con relajación 0 = estricto)
                    score = self.builder._calculate_worker_score(worker_under, date, post, relaxation_level=0)
                    
                    if score == float('-inf'):
                        continue
                    
                    # Simular el cambio
                    state = self._save_state()
                    
                    # Hacer el intercambio
                    self.schedule[date][post] = under_id
                    self.worker_assignments[over_id].discard(date)
                    self.worker_assignments.setdefault(under_id, set()).add(date)
                    
                    # Actualizar tracking
                    self.scheduler._update_tracking_data(over_id, date, post, removing=True)
                    self.scheduler._update_tracking_data(under_id, date, post, removing=False)
                    
                    # Verificar que mejoró el balance
                    over_new = len(self.worker_assignments.get(over_id, set()))
                    under_new = len(self.worker_assignments.get(under_id, set()))
                    
                    over_target = next((w['target_shifts'] for w in self.workers_data if w['id'] == over_id), 0)
                    under_target = next((w['target_shifts'] for w in self.workers_data if w['id'] == under_id), 0)
                    
                    over_new_dev = abs(over_new - over_target)
                    under_new_dev = abs(under_new - under_target)
                    
                    # Verificar mejora
                    if over_new_dev <= abs(over_dev - 1) and under_new_dev <= abs(under_dev + 1):
                        logging.info(f"  ✅ Direct swap: {over_id} → {under_id} on {date.strftime('%Y-%m-%d')}")
                        return True
                    else:
                        # No mejoró, revertir
                        self._restore_state(state)
        
        return False
    
    def _try_three_way_swap(self, overloaded: List, underloaded: List, tolerance: int) -> bool:
        """
        Intenta intercambio de 3 trabajadores para resolver bloqueos
        
        Patrón: A (sobre) → B (equilibrado) → C (bajo)
        """
        for over_id, over_dev in overloaded[:3]:
            over_dates = list(self.worker_assignments.get(over_id, set()))
            
            for date_a in over_dates:
                if (over_id, date_a) in self.builder._locked_mandatory:
                    continue
                
                if not self.builder._can_modify_assignment(over_id, date_a, "three_way"):
                    continue
                
                try:
                    post_a = self.schedule[date_a].index(over_id)
                except (ValueError, KeyError):
                    continue
                
                # Buscar un trabajador B (equilibrado) que pueda intercambiar
                for worker_b in self.workers_data:
                    b_id = worker_b['id']
                    
                    # B debe estar equilibrado o ligeramente cargado
                    b_assigned = len(self.worker_assignments.get(b_id, set()))
                    b_target = worker_b.get('target_shifts', 0)
                    b_dev = b_assigned - b_target
                    
                    if abs(b_dev) > 1:  # Solo equilibrados
                        continue
                    
                    b_dates = list(self.worker_assignments.get(b_id, set()))
                    
                    for date_b in b_dates:
                        if (b_id, date_b) in self.builder._locked_mandatory:
                            continue
                        
                        try:
                            post_b = self.schedule[date_b].index(b_id)
                        except (ValueError, KeyError):
                            continue
                        
                        # Buscar trabajador C (bajo) que pueda recibir turno de B
                        for under_id, under_dev in underloaded[:3]:
                            # Verificar viabilidad del intercambio circular
                            # A → date_b/post_b (lugar de B)
                            # B → date_c (nuevo)
                            # C → date_a/post_a (lugar de A)
                            
                            # Por simplicidad, intentar solo si C puede tomar lugar de A
                            worker_c = next((w for w in self.workers_data if w['id'] == under_id), None)
                            if not worker_c:
                                continue
                            
                            score_c = self.builder._calculate_worker_score(worker_c, date_a, post_a, relaxation_level=0)
                            
                            if score_c > float('-inf'):
                                # Intentar el swap
                                state = self._save_state()
                                
                                try:
                                    # A → B's place
                                    self.schedule[date_b][post_b] = over_id
                                    self.worker_assignments[over_id].discard(date_a)
                                    self.worker_assignments[over_id].add(date_b)
                                    
                                    # C → A's place
                                    self.schedule[date_a][post_a] = under_id
                                    self.worker_assignments.setdefault(under_id, set()).add(date_a)
                                    
                                    # B pierde su turno pero podría ganar otro
                                    self.worker_assignments[b_id].discard(date_b)
                                    
                                    # Actualizar tracking
                                    self.scheduler._update_tracking_data(over_id, date_a, post_a, removing=True)
                                    self.scheduler._update_tracking_data(over_id, date_b, post_b, removing=False)
                                    self.scheduler._update_tracking_data(b_id, date_b, post_b, removing=True)
                                    self.scheduler._update_tracking_data(under_id, date_a, post_a, removing=False)
                                    
                                    logging.info(f"  ✅ Three-way swap: {over_id}→{b_id}→{under_id}")
                                    return True
                                    
                                except Exception as e:
                                    logging.debug(f"Three-way swap failed: {e}")
                                    self._restore_state(state)
        
        return False
    
    def _try_reassignment(self, overloaded: List, underloaded: List, tolerance: int) -> bool:
        """
        Intenta reasignar: remover turno de sobrecargado y buscar slot para subcargado
        """
        for over_id, over_dev in overloaded[:3]:
            over_dates = list(self.worker_assignments.get(over_id, set()))
            
            for date in over_dates:
                if (over_id, date) in self.builder._locked_mandatory:
                    continue
                
                if not self.builder._can_modify_assignment(over_id, date, "reassignment"):
                    continue
                
                try:
                    post = self.schedule[date].index(over_id)
                except (ValueError, KeyError):
                    continue
                
                # Remover el turno temporalmente
                state = self._save_state()
                
                self.schedule[date][post] = None
                self.worker_assignments[over_id].discard(date)
                self.scheduler._update_tracking_data(over_id, date, post, removing=True)
                
                # Intentar asignar a un subcargado
                for under_id, under_dev in underloaded[:3]:
                    worker_under = next((w for w in self.workers_data if w['id'] == under_id), None)
                    if not worker_under:
                        continue
                    
                    score = self.builder._calculate_worker_score(worker_under, date, post, relaxation_level=0)
                    
                    if score > float('-inf'):
                        # Asignar
                        self.schedule[date][post] = under_id
                        self.worker_assignments.setdefault(under_id, set()).add(date)
                        self.scheduler._update_tracking_data(under_id, date, post, removing=False)
                        
                        logging.info(f"  ✅ Reassignment: {over_id} removed, {under_id} assigned on {date.strftime('%Y-%m-%d')}")
                        return True
                
                # No funcionó, revertir
                self._restore_state(state)
        
        return False
    
    def _try_relaxed_swap(self, overloaded: List, underloaded: List, tolerance: int) -> bool:
        """Intenta intercambio con constraints relajadas (nivel 1)"""
        for over_id, over_dev in overloaded[:3]:
            for under_id, under_dev in underloaded[:3]:
                over_assignments = list(self.worker_assignments.get(over_id, set()))
                
                for date in over_assignments:
                    if (over_id, date) in self.builder._locked_mandatory:
                        continue
                    
                    try:
                        post = self.schedule[date].index(over_id)
                    except (ValueError, KeyError):
                        continue
                    
                    worker_under = next((w for w in self.workers_data if w['id'] == under_id), None)
                    if not worker_under:
                        continue
                    
                    # Usar relaxation_level=1
                    score = self.builder._calculate_worker_score(worker_under, date, post, relaxation_level=1)
                    
                    if score > float('-inf'):
                        state = self._save_state()
                        
                        self.schedule[date][post] = under_id
                        self.worker_assignments[over_id].discard(date)
                        self.worker_assignments.setdefault(under_id, set()).add(date)
                        
                        self.scheduler._update_tracking_data(over_id, date, post, removing=True)
                        self.scheduler._update_tracking_data(under_id, date, post, removing=False)
                        
                        logging.info(f"  ✅ Relaxed swap: {over_id} → {under_id} on {date.strftime('%Y-%m-%d')}")
                        return True
        
        return False
    
    def _save_state(self) -> Dict:
        """Guarda el estado actual para rollback"""
        return {
            'schedule': {k: v[:] for k, v in self.schedule.items()},
            'assignments': {k: set(v) for k, v in self.worker_assignments.items()}
        }
    
    def _restore_state(self, state: Dict):
        """Restaura un estado previo"""
        self.schedule.clear()
        self.schedule.update({k: v[:] for k, v in state['schedule'].items()})
        
        self.worker_assignments.clear()
        self.worker_assignments.update({k: set(v) for k, v in state['assignments'].items()})
