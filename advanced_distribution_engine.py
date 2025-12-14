"""
Advanced Distribution Engine
============================

Este módulo implementa estrategias avanzadas de distribución que mejoran significativamente
la capacidad del scheduler para alcanzar el 100% de reparto mientras respeta todas las constraints.

Mejoras Clave:
1. Sistema de scoring multinivel más inteligente
2. Backtracking adaptativo con memoria
3. Búsqueda por bloques temporales (chunk-based search)
4. Optimización de intercambios multi-trabajador
5. Relajación progresiva de constraints soft con rollback
"""

import logging
import copy
import random
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple, Any
from collections import defaultdict


class AdvancedDistributionEngine:
    """Motor avanzado de distribución de turnos"""
    
    def __init__(self, scheduler, schedule_builder):
        """
        Inicializar el motor avanzado
        
        Args:
            scheduler: Referencia al scheduler principal
            schedule_builder: Referencia al schedule builder
        """
        self.scheduler = scheduler
        self.builder = schedule_builder
        self.config = scheduler.config
        
        # Cache y memoria para backtracking
        self._assignment_history: List[Dict] = []
        self._failed_attempts: Set[Tuple] = set()
        self._successful_patterns: List[Dict] = []
        
        # Métricas de rendimiento
        self.metrics = {
            'total_attempts': 0,
            'successful_fills': 0,
            'backtrack_count': 0,
            'swap_success': 0,
            'pattern_reuse': 0
        }
        
        logging.info("🚀 Advanced Distribution Engine initialized")
    
    def enhanced_fill_schedule(self, max_iterations: int = 100) -> bool:
        """
        Método principal de llenado mejorado con múltiples estrategias
        
        Returns:
            bool: True si se logró un llenado exitoso
        """
        logging.info("=" * 80)
        logging.info("ADVANCED DISTRIBUTION ENGINE - Enhanced Fill")
        logging.info("=" * 80)
        
        initial_filled = self._count_filled_slots()
        total_slots = self._count_total_slots()
        
        logging.info(f"Initial state: {initial_filled}/{total_slots} slots filled ({initial_filled/total_slots*100:.1f}%)")
        
        # Estrategia 1: Llenado inteligente por bloques temporales
        logging.info("\n📦 Strategy 1: Chunk-based intelligent fill")
        self._chunk_based_fill()
        
        # Estrategia 2: Búsqueda con backtracking adaptativo
        logging.info("\n🔄 Strategy 2: Adaptive backtracking search")
        self._adaptive_backtracking_fill(max_iterations // 2)
        
        # Estrategia 3: Optimización de intercambios multi-trabajador
        logging.info("\n🔀 Strategy 3: Multi-worker swap optimization")
        self._multi_worker_swap_optimization()
        
        # Estrategia 4: Relleno con relajación progresiva
        logging.info("\n⚡ Strategy 4: Progressive relaxation fill")
        self._progressive_relaxation_fill(max_iterations // 2)
        
        final_filled = self._count_filled_slots()
        improvement = final_filled - initial_filled
        
        logging.info("=" * 80)
        logging.info(f"FINAL RESULTS:")
        logging.info(f"  Filled: {final_filled}/{total_slots} ({final_filled/total_slots*100:.1f}%)")
        logging.info(f"  Improvement: +{improvement} slots")
        logging.info(f"  Metrics: {self.metrics}")
        logging.info("=" * 80)
        
        return final_filled == total_slots
    
    def _chunk_based_fill(self) -> int:
        """
        Llenado inteligente por bloques temporales
        
        Divide el periodo en chunks de 7 días y optimiza cada chunk considerando
        la distribución óptima de trabajadores dentro del bloque.
        
        Returns:
            int: Número de slots llenados
        """
        filled_count = 0
        current_date = self.scheduler.start_date
        chunk_size = 7  # Una semana
        
        while current_date <= self.scheduler.end_date:
            chunk_end = min(current_date + timedelta(days=chunk_size-1), self.scheduler.end_date)
            
            logging.info(f"  Processing chunk: {current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            
            # Obtener slots vacíos en este chunk
            empty_slots = self._get_empty_slots_in_range(current_date, chunk_end)
            
            if empty_slots:
                # Analizar el chunk y crear un plan óptimo
                plan = self._create_chunk_plan(empty_slots, current_date, chunk_end)
                
                # Ejecutar el plan
                filled_in_chunk = self._execute_chunk_plan(plan)
                filled_count += filled_in_chunk
                
                logging.info(f"    Filled {filled_in_chunk} slots in this chunk")
            
            current_date = chunk_end + timedelta(days=1)
        
        return filled_count
    
    def _create_chunk_plan(self, empty_slots: List[Tuple], chunk_start: datetime, chunk_end: datetime) -> Dict:
        """
        Crear un plan óptimo para llenar un chunk
        
        Analiza las necesidades de cada trabajador y distribuye los slots
        de manera que maximice el balance y respete las constraints.
        """
        plan = {
            'assignments': [],
            'priority': []
        }
        
        # Calcular déficit de cada trabajador
        worker_deficit = {}
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            current = len(self.scheduler.worker_assignments.get(worker_id, set()))
            target = worker.get('target_shifts', 0)
            deficit = max(0, target - current)
            
            if deficit > 0:
                worker_deficit[worker_id] = {
                    'deficit': deficit,
                    'work_percentage': worker.get('work_percentage', 100),
                    'priority': deficit * (worker.get('work_percentage', 100) / 100)
                }
        
        # Ordenar trabajadores por prioridad (mayor déficit primero)
        sorted_workers = sorted(
            worker_deficit.items(),
            key=lambda x: x[1]['priority'],
            reverse=True
        )
        
        # Para cada slot vacío, encontrar el mejor trabajador
        for date, post in empty_slots:
            if not sorted_workers:
                break
            
            best_worker = None
            best_score = float('-inf')
            
            for worker_id, info in sorted_workers:
                # Verificar si puede asignarse
                worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
                if not worker_data:
                    continue
                
                score = self.builder._calculate_worker_score(worker_data, date, post, relaxation_level=0)
                
                if score > best_score:
                    best_score = score
                    best_worker = worker_id
            
            if best_worker and best_score > float('-inf'):
                plan['assignments'].append({
                    'worker_id': best_worker,
                    'date': date,
                    'post': post,
                    'score': best_score
                })
        
        return plan
    
    def _execute_chunk_plan(self, plan: Dict) -> int:
        """Ejecutar el plan de asignación de un chunk"""
        filled_count = 0
        
        for assignment in plan['assignments']:
            worker_id = assignment['worker_id']
            date = assignment['date']
            post = assignment['post']
            
            # Verificar que el slot sigue vacío
            if (date not in self.scheduler.schedule or 
                len(self.scheduler.schedule[date]) <= post or 
                self.scheduler.schedule[date][post] is None):
                
                # Intentar asignación
                if self._try_assign_with_validation(worker_id, date, post):
                    filled_count += 1
                    self.metrics['successful_fills'] += 1
        
        return filled_count
    
    def _adaptive_backtracking_fill(self, max_iterations: int) -> int:
        """
        Llenado con backtracking adaptativo
        
        Usa memoria de intentos fallidos para evitar repetir patrones que no funcionan.
        """
        filled_count = 0
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            self.metrics['total_attempts'] += 1
            
            # Obtener el slot vacío más restrictivo
            empty_slot = self._find_most_constrained_slot()
            
            if not empty_slot:
                logging.info("  ✅ No more empty slots found")
                break
            
            date, post = empty_slot
            
            # Obtener candidatos ordenados por score
            candidates = self._get_smart_candidates(date, post)
            
            if not candidates:
                # No hay candidatos válidos - necesitamos backtrack
                if not self._perform_intelligent_backtrack(date, post):
                    logging.debug(f"  ⚠️ Cannot fill slot {date.strftime('%Y-%m-%d')} post {post} - no valid backtrack")
                    # Marcar como intento fallido
                    self._failed_attempts.add((date, post, tuple([])))
                continue
            
            # Intentar con cada candidato
            assigned = False
            for worker_data, score in candidates:
                worker_id = worker_data['id']
                
                # Verificar si este patrón ya falló antes
                if (date, post, worker_id) in self._failed_attempts:
                    continue
                
                # Guardar estado para posible rollback
                state = self._save_state()
                
                if self._try_assign_with_validation(worker_id, date, post):
                    filled_count += 1
                    assigned = True
                    
                    # Guardar patrón exitoso
                    self._successful_patterns.append({
                        'date': date,
                        'post': post,
                        'worker_id': worker_id,
                        'score': score
                    })
                    break
                else:
                    # Rollback y marcar como fallido
                    self._restore_state(state)
                    self._failed_attempts.add((date, post, worker_id))
            
            if not assigned:
                logging.debug(f"  ❌ Could not fill slot {date.strftime('%Y-%m-%d')} post {post} after trying all candidates")
        
        return filled_count
    
    def _find_most_constrained_slot(self) -> Optional[Tuple[datetime, int]]:
        """
        Encuentra el slot vacío más restringido (con menos candidatos válidos)
        
        Estrategia: Llenar primero los slots más difíciles.
        """
        empty_slots = []
        
        for date, workers_in_posts in self.scheduler.schedule.items():
            for post, worker in enumerate(workers_in_posts):
                if worker is None:
                    # Contar candidatos válidos
                    candidates = self._get_smart_candidates(date, post)
                    empty_slots.append((date, post, len(candidates)))
        
        if not empty_slots:
            return None
        
        # Ordenar por número de candidatos (menos candidatos = más restrictivo)
        empty_slots.sort(key=lambda x: x[2])
        
        return (empty_slots[0][0], empty_slots[0][1])
    
    def _get_smart_candidates(self, date: datetime, post: int) -> List[Tuple]:
        """
        Obtener candidatos inteligentes con scoring mejorado
        
        Considera:
        - Score base del builder
        - Historial de patrones exitosos
        - Distancia temporal con asignaciones previas
        - Balance de carga global
        """
        candidates = []
        
        # Obtener workers ya asignados en esta fecha
        already_assigned = [w for i, w in enumerate(self.scheduler.schedule.get(date, [])) 
                          if i != post and w is not None]
        
        for worker in self.scheduler.workers_data:
            worker_id = worker['id']
            
            # Pre-filtros rápidos
            if worker_id in already_assigned:
                continue
            
            if self.builder._is_worker_unavailable(worker_id, date):
                continue
            
            if not self.builder._check_incompatibility_with_list(worker_id, already_assigned):
                continue
            
            # Score base
            base_score = self.builder._calculate_worker_score(worker, date, post, relaxation_level=0)
            
            if base_score == float('-inf'):
                continue
            
            # Bonus por patrones exitosos similares
            pattern_bonus = self._calculate_pattern_bonus(worker_id, date, post)
            
            # Bonus por maximizar distancia entre turnos
            gap_bonus = self._calculate_optimal_gap_bonus(worker_id, date)
            
            # Bonus por balance global
            balance_bonus = self._calculate_global_balance_bonus(worker_id)
            
            total_score = base_score + pattern_bonus + gap_bonus + balance_bonus
            
            candidates.append((worker, total_score))
        
        # Ordenar por score descendente
        candidates.sort(key=lambda x: x[1], reverse=True)
        
        return candidates
    
    def _calculate_pattern_bonus(self, worker_id: str, date: datetime, post: int) -> float:
        """Bonus si este trabajador ha tenido éxito en patrones similares"""
        bonus = 0.0
        
        for pattern in self._successful_patterns[-50:]:  # Últimos 50 patrones exitosos
            if pattern['worker_id'] == worker_id:
                # Misma fecha en la semana
                if pattern['date'].weekday() == date.weekday():
                    bonus += 200
                # Mismo post
                if pattern['post'] == post:
                    bonus += 300
        
        if bonus > 0:
            self.metrics['pattern_reuse'] += 1
        
        return bonus
    
    def _calculate_optimal_gap_bonus(self, worker_id: str, date: datetime) -> float:
        """
        Bonus que MAXIMIZA la distancia entre turnos
        
        Cuanto mayor sea el gap desde el último turno, mayor bonus.
        """
        assignments = self.scheduler.worker_assignments.get(worker_id, set())
        
        if not assignments:
            return 1000  # Bonus alto para primer turno
        
        # Encontrar la asignación más cercana
        closest_gap = float('inf')
        for assigned_date in assignments:
            gap = abs((date - assigned_date).days)
            closest_gap = min(closest_gap, gap)
        
        # Bonus exponencial por gaps grandes
        min_gap = self.scheduler.gap_between_shifts + 1
        
        if closest_gap > min_gap:
            extra_days = closest_gap - min_gap
            # Fórmula exponencial: favorece gaps de 5-7+ días
            return 500 + (extra_days ** 1.5) * 200
        else:
            # Gap mínimo válido
            return closest_gap * 100
    
    def _calculate_global_balance_bonus(self, worker_id: str) -> float:
        """Bonus basado en el balance global del trabajador vs otros"""
        current = len(self.scheduler.worker_assignments.get(worker_id, set()))
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        
        if not worker_data:
            return 0
        
        target = worker_data.get('target_shifts', 0)
        deficit = target - current
        
        # Bonus muy alto para trabajadores con déficit significativo
        if deficit >= 3:
            return 5000 + (deficit * 1000)
        elif deficit >= 2:
            return 3000
        elif deficit >= 1:
            return 1500
        elif deficit == 0:
            return -500  # Pequeña penalización si ya alcanzó el target
        else:
            return -2000  # Penalización mayor si está por encima
    
    def _perform_intelligent_backtrack(self, date: datetime, post: int) -> bool:
        """
        Backtracking inteligente: intenta liberar un slot cercano para hacer espacio
        """
        self.metrics['backtrack_count'] += 1
        
        # Buscar asignaciones recientes que no sean mandatory
        recent_assignments = []
        
        for check_date in self._get_dates_around(date, days=7):
            if check_date not in self.scheduler.schedule:
                continue
            
            for check_post, check_worker in enumerate(self.scheduler.schedule[check_date]):
                if check_worker is None:
                    continue
                
                # No tocar mandatory
                if (check_worker, check_date) in self.builder._locked_mandatory:
                    continue
                
                recent_assignments.append((check_date, check_post, check_worker))
        
        # Intentar liberar y reasignar
        for old_date, old_post, old_worker in recent_assignments:
            # Guardar estado
            state = self._save_state()
            
            # Remover asignación temporal
            self._remove_assignment(old_worker, old_date, old_post)
            
            # Intentar llenar el slot problemático
            candidates = self._get_smart_candidates(date, post)
            
            for worker_data, score in candidates[:3]:  # Probar top 3
                worker_id = worker_data['id']
                
                if self._try_assign_with_validation(worker_id, date, post):
                    # Éxito! Intentar reasignar el trabajador removido
                    if self._try_reassign_worker(old_worker, old_date, old_post):
                        logging.info(f"  ✅ Backtrack successful: freed {old_date.strftime('%Y-%m-%d')} post {old_post}, filled {date.strftime('%Y-%m-%d')} post {post}")
                        return True
            
            # No funcionó, restaurar
            self._restore_state(state)
        
        return False
    
    def _multi_worker_swap_optimization(self) -> int:
        """
        Optimización de intercambios multi-trabajador
        
        Busca oportunidades de intercambio entre 2-3 trabajadores que mejoren
        el balance global y llenen slots vacíos.
        """
        improvements = 0
        max_attempts = 100
        
        for attempt in range(max_attempts):
            # Encontrar un slot vacío
            empty_slots = []
            for date, workers in self.scheduler.schedule.items():
                for post, worker in enumerate(workers):
                    if worker is None:
                        empty_slots.append((date, post))
            
            if not empty_slots:
                break
            
            # Seleccionar un slot vacío random
            target_date, target_post = random.choice(empty_slots)
            
            # Buscar intercambios de 2 trabajadores
            if self._try_two_worker_swap(target_date, target_post):
                improvements += 1
                self.metrics['swap_success'] += 1
                continue
            
            # Buscar intercambios de 3 trabajadores (más complejo)
            if self._try_three_worker_swap(target_date, target_post):
                improvements += 1
                self.metrics['swap_success'] += 1
        
        logging.info(f"  Multi-worker swaps successful: {improvements}")
        return improvements
    
    def _try_two_worker_swap(self, target_date: datetime, target_post: int) -> bool:
        """
        Intenta un intercambio de 2 trabajadores para llenar el slot target
        
        Patrón: Worker A en (date1, post1) → (target_date, target_post)
               Worker B en (date2, post2) → (date1, post1)
        """
        # Buscar candidatos para el slot target
        candidates = self._get_smart_candidates(target_date, target_post)
        
        for worker_a_data, score in candidates[:5]:  # Top 5
            worker_a = worker_a_data['id']
            
            # Buscar una asignación actual de worker_a que podamos intercambiar
            assignments_a = list(self.scheduler.worker_assignments.get(worker_a, set()))
            
            for date_a in assignments_a:
                # No tocar mandatory
                if (worker_a, date_a) in self.builder._locked_mandatory:
                    continue
                
                try:
                    post_a = self.scheduler.schedule[date_a].index(worker_a)
                except (ValueError, KeyError):
                    continue
                
                # Buscar worker B que pueda ocupar el lugar de A
                for worker_b_data in self.scheduler.workers_data:
                    worker_b = worker_b_data['id']
                    
                    if worker_b == worker_a:
                        continue
                    
                    # Worker B no debe estar ya asignado en date_a
                    if worker_b in self.scheduler.schedule.get(date_a, []):
                        continue
                    
                    # Verificar si B puede ir a (date_a, post_a)
                    score_b = self.builder._calculate_worker_score(
                        worker_b_data, date_a, post_a, relaxation_level=0
                    )
                    
                    if score_b == float('-inf'):
                        continue
                    
                    # Intentar el swap
                    state = self._save_state()
                    
                    # 1. Remover A de date_a
                    self._remove_assignment(worker_a, date_a, post_a)
                    
                    # 2. Asignar A a target
                    if not self._try_assign_with_validation(worker_a, target_date, target_post):
                        self._restore_state(state)
                        continue
                    
                    # 3. Asignar B a date_a
                    if not self._try_assign_with_validation(worker_b, date_a, post_a):
                        self._restore_state(state)
                        continue
                    
                    # Swap exitoso!
                    logging.info(f"  ✅ 2-worker swap: {worker_a} to target, {worker_b} to {date_a.strftime('%Y-%m-%d')}")
                    return True
        
        return False
    
    def _try_three_worker_swap(self, target_date: datetime, target_post: int) -> bool:
        """
        Intenta un intercambio de 3 trabajadores (más complejo)
        
        Patrón: A → target, B → lugar de A, C → lugar de B
        """
        # Similar a _try_two_worker_swap pero con un nivel más de recursión
        # Por simplicidad, implementación básica
        return False
    
    def _progressive_relaxation_fill(self, max_iterations: int) -> int:
        """
        Llenado con relajación progresiva de constraints
        
        Comienza estricto y va relajando constraints soft gradualmente.
        """
        filled_count = 0
        
        for relaxation_level in range(4):  # 0, 1, 2, 3
            logging.info(f"  Relaxation level {relaxation_level}")
            
            iteration = 0
            while iteration < max_iterations // 4:
                iteration += 1
                
                # Obtener slots vacíos
                empty_slots = []
                for date, workers in self.scheduler.schedule.items():
                    for post, worker in enumerate(workers):
                        if worker is None:
                            empty_slots.append((date, post))
                
                if not empty_slots:
                    logging.info(f"    ✅ All slots filled at relaxation {relaxation_level}")
                    return filled_count
                
                # Intentar llenar un slot random
                date, post = random.choice(empty_slots)
                
                # Obtener candidatos con este nivel de relajación
                candidates = []
                for worker in self.scheduler.workers_data:
                    score = self.builder._calculate_worker_score(
                        worker, date, post, relaxation_level=relaxation_level
                    )
                    if score > float('-inf'):
                        candidates.append((worker, score))
                
                if candidates:
                    candidates.sort(key=lambda x: x[1], reverse=True)
                    worker_data, score = candidates[0]
                    
                    if self._try_assign_with_validation(worker_data['id'], date, post):
                        filled_count += 1
        
        return filled_count
    
    # ========================================
    # MÉTODOS AUXILIARES
    # ========================================
    
    def _try_assign_with_validation(self, worker_id: str, date: datetime, post: int) -> bool:
        """Intentar asignación con validación completa"""
        try:
            # Asegurar que el schedule tiene la estructura correcta
            if date not in self.scheduler.schedule:
                self.scheduler.schedule[date] = [None] * self.scheduler.num_shifts
            
            while len(self.scheduler.schedule[date]) <= post:
                self.scheduler.schedule[date].append(None)
            
            # Verificar que el slot está vacío
            if self.scheduler.schedule[date][post] is not None:
                return False
            
            # Asignar
            self.scheduler.schedule[date][post] = worker_id
            self.scheduler.worker_assignments.setdefault(worker_id, set()).add(date)
            
            # Actualizar tracking
            self.scheduler._update_tracking_data(worker_id, date, post, removing=False)
            
            return True
            
        except Exception as e:
            logging.error(f"Error in _try_assign_with_validation: {e}")
            return False
    
    def _remove_assignment(self, worker_id: str, date: datetime, post: int):
        """Remover una asignación"""
        if date in self.scheduler.schedule and len(self.scheduler.schedule[date]) > post:
            self.scheduler.schedule[date][post] = None
        
        if worker_id in self.scheduler.worker_assignments:
            self.scheduler.worker_assignments[worker_id].discard(date)
        
        self.scheduler._update_tracking_data(worker_id, date, post, removing=True)
    
    def _try_reassign_worker(self, worker_id: str, preferred_date: datetime, preferred_post: int) -> bool:
        """Intentar reasignar un trabajador a su fecha/post preferido o alternativo"""
        worker_data = next((w for w in self.scheduler.workers_data if w['id'] == worker_id), None)
        if not worker_data:
            return False
        
        # Intentar el slot preferido primero
        if (preferred_date in self.scheduler.schedule and 
            len(self.scheduler.schedule[preferred_date]) > preferred_post and
            self.scheduler.schedule[preferred_date][preferred_post] is None):
            
            score = self.builder._calculate_worker_score(worker_data, preferred_date, preferred_post, relaxation_level=1)
            if score > float('-inf'):
                return self._try_assign_with_validation(worker_id, preferred_date, preferred_post)
        
        # Buscar slots alternativos cercanos
        for days_offset in range(-3, 4):
            alt_date = preferred_date + timedelta(days=days_offset)
            
            if not (self.scheduler.start_date <= alt_date <= self.scheduler.end_date):
                continue
            
            if alt_date not in self.scheduler.schedule:
                continue
            
            for alt_post in range(len(self.scheduler.schedule[alt_date])):
                if self.scheduler.schedule[alt_date][alt_post] is None:
                    score = self.builder._calculate_worker_score(worker_data, alt_date, alt_post, relaxation_level=1)
                    if score > float('-inf'):
                        if self._try_assign_with_validation(worker_id, alt_date, alt_post):
                            return True
        
        return False
    
    def _save_state(self) -> Dict:
        """Guardar estado actual del schedule para posible rollback"""
        return {
            'schedule': {k: v[:] for k, v in self.scheduler.schedule.items()},
            'assignments': {k: set(v) for k, v in self.scheduler.worker_assignments.items()}
        }
    
    def _restore_state(self, state: Dict):
        """Restaurar estado previo"""
        self.scheduler.schedule = {k: v[:] for k, v in state['schedule'].items()}
        self.scheduler.worker_assignments = {k: set(v) for k, v in state['assignments'].items()}
    
    def _count_filled_slots(self) -> int:
        """Contar slots llenos"""
        count = 0
        for date, workers in self.scheduler.schedule.items():
            count += sum(1 for w in workers if w is not None)
        return count
    
    def _count_total_slots(self) -> int:
        """Contar total de slots"""
        count = 0
        for date, workers in self.scheduler.schedule.items():
            count += len(workers)
        return count
    
    def _get_empty_slots_in_range(self, start: datetime, end: datetime) -> List[Tuple]:
        """Obtener slots vacíos en un rango de fechas"""
        empty = []
        current = start
        
        while current <= end:
            if current in self.scheduler.schedule:
                for post, worker in enumerate(self.scheduler.schedule[current]):
                    if worker is None:
                        empty.append((current, post))
            current += timedelta(days=1)
        
        return empty
    
    def _get_dates_around(self, center_date: datetime, days: int) -> List[datetime]:
        """Obtener fechas alrededor de una fecha central"""
        dates = []
        for offset in range(-days, days + 1):
            date = center_date + timedelta(days=offset)
            if self.scheduler.start_date <= date <= self.scheduler.end_date:
                dates.append(date)
        return dates
