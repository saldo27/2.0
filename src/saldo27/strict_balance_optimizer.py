"""
Strict Balance Optimizer
========================

Sistema estricto de optimización de balance que garantiza que todos los trabajadores
estén dentro de ±1 turno de su objetivo, respetando todas las constraints.

Estrategia:
1. Identificar trabajadores con mayor desviación (sobrecargados y subcargados)
2. Encontrar oportunidades de intercambio que mejoren el balance:
   - Intercambio directo: A (sobrecargado) → B (subcargado)
   - Intercambio a 3 bandas: A → C (intermediario equilibrado) → B
     * Cuando A no puede dar turno directamente a B (incompatibilidad, gap, etc.)
     * C toma turno de A y da uno de sus turnos a B
     * Resultado: A -1, C igual, B +1
   - Reasignación: Remover turno y asignar a subcargado
3. Validar que no se violan constraints (gaps, incompatibilidades, balance mensual, etc.)
4. Aplicar cambios y verificar mejora

Intercambio a 3 bandas (three-way swap):
- Útil cuando hay bloqueos entre trabajadores con grandes desviaciones
- Ejemplo: A tiene +20% desviación, B tiene -18%, pero son incompatibles
- C (equilibrado) puede mediar: A→C (en fecha_A), C→B (en fecha_C)
- Se respetan todas las constraints para cada movimiento individual
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

    def _sort_dates_by_monthly_excess(self, worker_id, dates):
        """Sort dates so that months where the worker is ABOVE their monthly
        target come first.  This way, swaps preferentially remove shifts from
        over-target months, preserving monthly balance."""
        worker_config = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker_config or not hasattr(self.builder, '_get_expected_monthly_target'):
            random.shuffle(dates)
            return dates

        # Pre-compute monthly counts
        month_counts = {}
        for d in self.worker_assignments.get(worker_id, set()):
            key = (d.year, d.month)
            month_counts[key] = month_counts.get(key, 0) + 1

        def excess(d):
            key = (d.year, d.month)
            target = self.builder._get_expected_monthly_target(worker_config, d.year, d.month)
            return month_counts.get(key, 0) - target

        # Sort descending by excess (highest excess first = best to give away)
        dates.sort(key=excess, reverse=True)
        return dates

    def _giver_month_ok(self, worker_id, date):
        """Check that removing a shift from this month won't drop the giver
        more than 1 below their monthly target."""
        worker_config = next((w for w in self.workers_data if w['id'] == worker_id), None)
        if not worker_config or not hasattr(self.builder, '_get_expected_monthly_target'):
            return True
        target = self.builder._get_expected_monthly_target(worker_config, date.year, date.month)
        current = sum(1 for d in self.worker_assignments.get(worker_id, set())
                      if d.year == date.year and d.month == date.month)
        # Allow at most 1 below target (since target may be fractional/rounded)
        return (current - 1) >= (target - 1)
    
    def optimize_balance(self, max_iterations: int = 500, target_tolerance: int = 1) -> bool:
        """
        Optimiza el balance de turnos para que todos estén dentro de ±target_tolerance
        
        Args:
            max_iterations: Máximo número de iteraciones (aumentado a 500)
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
        stalled_iterations = 0  # Contador para detectar estancamiento
        
        while iteration < max_iterations and improvement_made:
            iteration += 1
            improvement_made = False
            
            # Obtener trabajadores fuera de tolerancia
            overloaded, underloaded = self._get_imbalanced_workers(target_tolerance)
            
            if not overloaded or not underloaded:
                logging.info(f"✅ Balance achieved at iteration {iteration}")
                break
            
            # Intentar múltiples estrategias en orden de preferencia
            
            # 1. Intercambio directo (más simple y seguro)
            if self._try_direct_swap(overloaded, underloaded, target_tolerance):
                improvement_made = True
                self.stats['swaps_performed'] += 1
                stalled_iterations = 0
                continue
            
            # 2. Intercambio a 3 bandas estricto
            if self._try_three_way_swap(overloaded, underloaded, target_tolerance):
                improvement_made = True
                self.stats['swaps_performed'] += 1
                stalled_iterations = 0
                continue
            
            # 3. Reasignación simple
            if self._try_reassignment(overloaded, underloaded, target_tolerance):
                improvement_made = True
                self.stats['swaps_performed'] += 1
                stalled_iterations = 0
                continue
            
            # 4. Intercambio a 3 bandas agresivo (más combinaciones, constraints relajadas)
            if self._try_aggressive_three_way_swap(overloaded, underloaded, target_tolerance):
                improvement_made = True
                self.stats['swaps_performed'] += 1
                stalled_iterations = 0
                continue
            
            # 5. Cadena de intercambios múltiples (4+ trabajadores)
            if self._try_chain_swap(overloaded, underloaded, target_tolerance):
                improvement_made = True
                self.stats['swaps_performed'] += 1
                stalled_iterations = 0
                continue
            
            # 6. Redistribución forzada (última opción antes de relajar)
            if self._try_forced_redistribution(overloaded, underloaded, target_tolerance):
                improvement_made = True
                self.stats['swaps_performed'] += 1
                stalled_iterations = 0
                continue
            
            # 7. Si no hay mejora, intentar con relajación de constraints cada 10 iteraciones
            stalled_iterations += 1
            if stalled_iterations >= 5 or iteration % 10 == 0:
                logging.info(f"  Iteration {iteration}: Trying relaxed constraints (stalled: {stalled_iterations})")
                if self._try_relaxed_swap(overloaded, underloaded, target_tolerance):
                    improvement_made = True
                    self.stats['swaps_performed'] += 1
                    stalled_iterations = 0
        
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
                    worker_name = next((w.get('name', w['id']) for w in self.workers_data if w['id'] == worker_id), worker_id)
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
            all_assignments = self.worker_assignments.get(worker_id, set())
            total_assigned = len(all_assignments)
            
            # CRITICAL: target_shifts ya tiene mandatory restados
            # Debemos comparar con non-mandatory assigned
            mandatory_dates = set()
            mandatory_str = worker.get('mandatory_days', '')
            if mandatory_str and hasattr(self.builder, 'date_utils'):
                try:
                    mandatory_dates = set(self.builder.date_utils.parse_dates(mandatory_str))
                except Exception:
                    pass
            mandatory_assigned = sum(1 for d in all_assignments if d in mandatory_dates)
            non_mandatory_assigned = total_assigned - mandatory_assigned
            
            deviation = non_mandatory_assigned - target
            
            worker_details[worker_id] = {
                'name': worker.get('name', worker_id),
                'target': target,
                'assigned': non_mandatory_assigned,  # Solo non-mandatory
                'total_assigned': total_assigned,     # Total incluyendo mandatory
                'mandatory': mandatory_assigned,
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
            
            all_assignments = self.worker_assignments.get(worker_id, set())
            total_assigned = len(all_assignments)
            
            # CRITICAL: target_shifts ya tiene mandatory restados
            # Debemos comparar con non-mandatory assigned
            mandatory_dates = set()
            mandatory_str = worker.get('mandatory_days', '')
            if mandatory_str and hasattr(self.builder, 'date_utils'):
                try:
                    mandatory_dates = set(self.builder.date_utils.parse_dates(mandatory_str))
                except Exception:
                    pass
            mandatory_assigned = sum(1 for d in all_assignments if d in mandatory_dates)
            non_mandatory_assigned = total_assigned - mandatory_assigned
            
            deviation = non_mandatory_assigned - target
            
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
                # CRITICAL: Prefer dates from months where giver is ABOVE target
                over_assignments = list(self.worker_assignments.get(over_id, set()))
                over_assignments = self._sort_dates_by_monthly_excess(over_id, over_assignments)
                
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
                    
                    # Check giver's monthly balance: don't take from a month
                    # where giver is already at or below target
                    if not self._giver_month_ok(over_id, date):
                        continue
                    
                    # Verificar que el subcargado puede tomar este turno
                    worker_under = next((w for w in self.workers_data if w['id'] == under_id), None)
                    if not worker_under:
                        continue
                    
                    # Calcular score (con relajación 0 = estricto)
                    score = self.builder._calculate_worker_score(worker_under, date, post, relaxation_level=0)
                    
                    if score == float('-inf'):
                        continue
                    
                    # NEW: Additional validation for monthly balance and weekends
                    # Validate monthly balance
                    if hasattr(self.builder, '_get_expected_monthly_target'):
                        expected_monthly = self.builder._get_expected_monthly_target(worker_under, date.year, date.month)
                        shifts_this_month = sum(
                            1 for d in self.worker_assignments.get(under_id, set())
                            if d.year == date.year and d.month == date.month
                        )
                        work_pct = worker_under.get('work_percentage', 100)
                        monthly_tolerance = 1 if work_pct >= 100 else 0
                        max_monthly = expected_monthly + monthly_tolerance
                        
                        if shifts_this_month + 1 > max_monthly:
                            logging.debug(f"Strict balance: {under_id} blocked by monthly limit")
                            continue
                    
                    # Validate consecutive weekends (including holidays and pre-holidays)
                    # Use builder's method that includes holidays for consistency
                    if hasattr(self.builder, '_is_weekend_or_holiday') and self.builder._is_weekend_or_holiday(date):
                        if hasattr(self.builder, '_would_exceed_weekend_limit_simulated'):
                            if self.builder._would_exceed_weekend_limit_simulated(under_id, date, self.worker_assignments):
                                logging.debug(f"Strict balance: {under_id} blocked by weekend limit")
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
        Intenta intercambio de 3 trabajadores para resolver bloqueos cuando no hay swap directo posible.
        
        Patrón: A (sobrecargado) → C (intermediario equilibrado) → B (subcargado)
        
        Lógica:
        - A tiene turno en fecha_A que no puede dar directamente a B
        - C (equilibrado) puede tomar el turno de A en fecha_A
        - C tiene un turno en fecha_C que puede dar a B
        - Resultado: A pierde turno (-1), C queda igual (gana fecha_A, pierde fecha_C), B gana turno (+1)
        
        Esto permite balancear cuando:
        - A no puede dar turno a B directamente (incompatibilidad, gap, etc.)
        - Pero A puede dar a C, y C tiene un turno que puede dar a B
        """
        for over_id, over_dev in overloaded[:5]:  # Top 5 sobrecargados
            over_dates = list(self.worker_assignments.get(over_id, set()))
            over_dates = self._sort_dates_by_monthly_excess(over_id, over_dates)
            
            for date_a in over_dates[:10]:  # Limitar para rendimiento
                if (over_id, date_a) in self.builder._locked_mandatory:
                    continue
                
                if not self.builder._can_modify_assignment(over_id, date_a, "three_way"):
                    continue
                
                if not self._giver_month_ok(over_id, date_a):
                    continue
                
                try:
                    post_a = self.schedule[date_a].index(over_id)
                except (ValueError, KeyError):
                    continue
                
                # Buscar un trabajador C (intermediario equilibrado) que pueda tomar turno de A
                for worker_c in self.workers_data:
                    c_id = worker_c['id']
                    
                    # C no debe ser A ni estar en overloaded/underloaded extremos
                    if c_id == over_id:
                        continue
                    
                    # C debe estar equilibrado (desviación pequeña)
                    c_assigned = len(self.worker_assignments.get(c_id, set()))
                    c_target = worker_c.get('target_shifts', 0)
                    if c_target == 0:
                        continue
                    c_dev = c_assigned - c_target
                    
                    # Solo trabajadores equilibrados o ligeramente positivos (pueden perder uno)
                    if c_dev < -1 or c_dev > 2:
                        continue
                    
                    # Verificar que C puede tomar el turno de A en date_a
                    score_c_take_a = self.builder._calculate_worker_score(worker_c, date_a, post_a, relaxation_level=0)
                    if score_c_take_a == float('-inf'):
                        continue
                    
                    # C ya trabaja en date_a? No puede tomar el mismo día
                    if c_id in self.schedule.get(date_a, []):
                        continue
                    
                    # Validaciones adicionales para C tomando turno de A
                    skip_c_take = False
                    
                    # Validar balance mensual para C
                    if hasattr(self.builder, '_get_expected_monthly_target'):
                        expected_monthly = self.builder._get_expected_monthly_target(worker_c, date_a.year, date_a.month)
                        shifts_c_month_a = sum(
                            1 for d in self.worker_assignments.get(c_id, set())
                            if d.year == date_a.year and d.month == date_a.month
                        )
                        work_pct = worker_c.get('work_percentage', 100)
                        monthly_tolerance = 1 if work_pct >= 100 else 0
                        max_monthly = expected_monthly + monthly_tolerance
                        
                        # C va a ganar un turno pero también perderá uno, así que solo bloqueamos si está muy alto
                        if shifts_c_month_a + 1 > max_monthly:
                            skip_c_take = True
                    
                    # Validar fines de semana consecutivos para C (incluyendo holidays y pre-holidays)
                    if not skip_c_take and hasattr(self.builder, '_is_weekend_or_holiday') and self.builder._is_weekend_or_holiday(date_a):
                        if hasattr(self.builder, '_would_exceed_weekend_limit_simulated'):
                            if self.builder._would_exceed_weekend_limit_simulated(c_id, date_a, self.worker_assignments):
                                skip_c_take = True
                    
                    if skip_c_take:
                        continue
                    
                    # Ahora buscar un turno de C que pueda dar a B (subcargado)
                    c_dates = list(self.worker_assignments.get(c_id, set()))
                    c_dates = self._sort_dates_by_monthly_excess(c_id, c_dates)
                    
                    for date_c in c_dates[:10]:
                        if date_c == date_a:  # No puede ser el mismo día
                            continue
                        
                        if (c_id, date_c) in self.builder._locked_mandatory:
                            continue
                        
                        # C gives away date_c — check monthly floor
                        # (C also gains date_a, but that could be a different month)
                        if not self._giver_month_ok(c_id, date_c):
                            continue
                        
                        if not self.builder._can_modify_assignment(c_id, date_c, "three_way_intermediary"):
                            continue
                        
                        try:
                            post_c = self.schedule[date_c].index(c_id)
                        except (ValueError, KeyError):
                            continue
                        
                        # Buscar trabajador B (subcargado) que pueda tomar turno de C
                        for under_id, under_dev in underloaded[:5]:
                            if under_id == c_id or under_id == over_id:
                                continue
                            
                            worker_b = next((w for w in self.workers_data if w['id'] == under_id), None)
                            if not worker_b:
                                continue
                            
                            # B ya trabaja en date_c?
                            if under_id in self.schedule.get(date_c, []):
                                continue
                            
                            # Verificar que B puede tomar el turno de C en date_c
                            score_b_take_c = self.builder._calculate_worker_score(worker_b, date_c, post_c, relaxation_level=0)
                            if score_b_take_c == float('-inf'):
                                continue
                            
                            # Validaciones adicionales para B
                            skip_b_take = False
                            
                            # Validar balance mensual para B
                            if hasattr(self.builder, '_get_expected_monthly_target'):
                                expected_monthly_b = self.builder._get_expected_monthly_target(worker_b, date_c.year, date_c.month)
                                shifts_b_month_c = sum(
                                    1 for d in self.worker_assignments.get(under_id, set())
                                    if d.year == date_c.year and d.month == date_c.month
                                )
                                work_pct_b = worker_b.get('work_percentage', 100)
                                monthly_tolerance_b = 1 if work_pct_b >= 100 else 0
                                max_monthly_b = expected_monthly_b + monthly_tolerance_b
                                
                                if shifts_b_month_c + 1 > max_monthly_b:
                                    skip_b_take = True
                            
                            # Validar fines de semana consecutivos para B (incluyendo holidays y pre-holidays)
                            if not skip_b_take and hasattr(self.builder, '_is_weekend_or_holiday') and self.builder._is_weekend_or_holiday(date_c):
                                if hasattr(self.builder, '_would_exceed_weekend_limit_simulated'):
                                    if self.builder._would_exceed_weekend_limit_simulated(under_id, date_c, self.worker_assignments):
                                        skip_b_take = True
                            
                            if skip_b_take:
                                continue
                            
                            # ¡Encontramos un intercambio válido a 3 bandas!
                            state = self._save_state()
                            
                            try:
                                # Paso 1: A pierde turno en date_a
                                self.schedule[date_a][post_a] = c_id
                                self.worker_assignments[over_id].discard(date_a)
                                self.worker_assignments.setdefault(c_id, set()).add(date_a)
                                
                                # Paso 2: C pierde turno en date_c, B lo toma
                                self.schedule[date_c][post_c] = under_id
                                self.worker_assignments[c_id].discard(date_c)
                                self.worker_assignments.setdefault(under_id, set()).add(date_c)
                                
                                # Actualizar tracking
                                self.scheduler._update_tracking_data(over_id, date_a, post_a, removing=True)
                                self.scheduler._update_tracking_data(c_id, date_a, post_a, removing=False)
                                self.scheduler._update_tracking_data(c_id, date_c, post_c, removing=True)
                                self.scheduler._update_tracking_data(under_id, date_c, post_c, removing=False)
                                
                                # Verificar que el resultado mejora el balance global
                                over_new = len(self.worker_assignments.get(over_id, set()))
                                under_new = len(self.worker_assignments.get(under_id, set()))
                                c_new = len(self.worker_assignments.get(c_id, set()))
                                
                                over_target = next((w['target_shifts'] for w in self.workers_data if w['id'] == over_id), 0)
                                under_target = next((w['target_shifts'] for w in self.workers_data if w['id'] == under_id), 0)
                                
                                # Verificar mejora: A debe reducir y B debe aumentar, C debe quedar igual
                                over_improved = (over_new - over_target) < over_dev
                                under_improved = abs(under_new - under_target) < abs(under_dev)
                                c_stable = abs(c_new - c_target) <= 1
                                
                                if over_improved and under_improved and c_stable:
                                    logging.info(f"  ✅ Three-way swap: {over_id}({date_a.strftime('%m-%d')})→{c_id}→{under_id}({date_c.strftime('%m-%d')})")
                                    logging.debug(f"      A:{over_id} {over_dev:+d}→{over_new - over_target:+d}, "
                                                f"C:{c_id} {c_dev:+d}→{c_new - c_target:+d}, "
                                                f"B:{under_id} {under_dev:+d}→{under_new - under_target:+d}")
                                    return True
                                else:
                                    # No mejoró suficiente, revertir
                                    self._restore_state(state)
                                    
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
            over_dates = self._sort_dates_by_monthly_excess(over_id, over_dates)
            
            for date in over_dates:
                if (over_id, date) in self.builder._locked_mandatory:
                    continue
                
                if not self.builder._can_modify_assignment(over_id, date, "reassignment"):
                    continue
                
                # Don't take from a month where giver is at/below target
                if not self._giver_month_ok(over_id, date):
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
                        # NEW: Additional validation for monthly balance and weekends
                        skip_assign = False
                        
                        # Validate monthly balance
                        if hasattr(self.builder, '_get_expected_monthly_target'):
                            expected_monthly = self.builder._get_expected_monthly_target(worker_under, date.year, date.month)
                            shifts_this_month = sum(
                                1 for d in self.worker_assignments.get(under_id, set())
                                if d.year == date.year and d.month == date.month
                            )
                            work_pct = worker_under.get('work_percentage', 100)
                            monthly_tolerance = 1 if work_pct >= 100 else 0
                            max_monthly = expected_monthly + monthly_tolerance
                            
                            if shifts_this_month + 1 > max_monthly:
                                skip_assign = True
                        
                        # Validate consecutive weekends (including holidays and pre-holidays)
                        if not skip_assign and hasattr(self.builder, '_is_weekend_or_holiday') and self.builder._is_weekend_or_holiday(date):
                            if hasattr(self.builder, '_would_exceed_weekend_limit_simulated'):
                                if self.builder._would_exceed_weekend_limit_simulated(under_id, date, self.worker_assignments):
                                    skip_assign = True
                        
                        if skip_assign:
                            continue
                        
                        # Asignar
                        self.schedule[date][post] = under_id
                        self.worker_assignments.setdefault(under_id, set()).add(date)
                        self.scheduler._update_tracking_data(under_id, date, post, removing=False)

                        # Post-check: verify counts match saved state (over -1, under +1)
                        post_over_count  = len(self.worker_assignments.get(over_id, set()))
                        post_under_count = len(self.worker_assignments.get(under_id, set()))
                        orig_over_count  = len(state['assignments'].get(over_id, set()))
                        orig_under_count = len(state['assignments'].get(under_id, set()))
                        if post_over_count != orig_over_count - 1 or post_under_count != orig_under_count + 1:
                            logging.warning(
                                f"  ⚠️ Reassignment count error: "
                                f"{over_id} {orig_over_count}→{post_over_count}, "
                                f"{under_id} {orig_under_count}→{post_under_count} — rolling back"
                            )
                            self._restore_state(state)
                            break  # Skip remaining underloaded for this date; outer loop restores state

                        logging.info(f"  ✅ Reassignment: {over_id} removed, {under_id} assigned on {date.strftime('%Y-%m-%d')}")
                        return True
                
                # No funcionó, revertir
                self._restore_state(state)
        
        return False
    
    def _try_relaxed_swap(self, overloaded: List, underloaded: List, tolerance: int) -> bool:
        """Intenta intercambio con constraints relajadas (nivel 1).

        Igual que _try_direct_swap pero con relaxation_level=1.  Se añade:
        - _giver_month_ok: no ceder turnos de meses donde el cedente ya está al límite.
        - Pre/post count accounting: registra nº de turnos antes y después y revierte
          si los conteos no cambian como se espera o el balance no mejora.
        """
        for over_id, over_dev in overloaded[:3]:
            for under_id, under_dev in underloaded[:3]:
                over_assignments = list(self.worker_assignments.get(over_id, set()))

                for date in over_assignments:
                    if (over_id, date) in self.builder._locked_mandatory:
                        continue

                    # Giver monthly check (same guard as _try_direct_swap)
                    if not self._giver_month_ok(over_id, date):
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

                        # ── Pre-swap counts ──────────────────────────────────────────
                        pre_over_count  = len(self.worker_assignments.get(over_id,  set()))
                        pre_under_count = len(self.worker_assignments.get(under_id, set()))

                        self.schedule[date][post] = under_id
                        self.worker_assignments[over_id].discard(date)
                        self.worker_assignments.setdefault(under_id, set()).add(date)

                        self.scheduler._update_tracking_data(over_id,  date, post, removing=True)
                        self.scheduler._update_tracking_data(under_id, date, post, removing=False)

                        # ── Post-swap counts ─────────────────────────────────────────
                        post_over_count  = len(self.worker_assignments.get(over_id,  set()))
                        post_under_count = len(self.worker_assignments.get(under_id, set()))

                        # Verify counts changed as expected (+1 / -1)
                        counts_ok = (post_over_count  == pre_over_count  - 1 and
                                     post_under_count == pre_under_count + 1)
                        if not counts_ok:
                            logging.error(
                                f"Relaxed swap accounting error: "
                                f"{over_id} {pre_over_count}→{post_over_count} "
                                f"(expected {pre_over_count - 1}), "
                                f"{under_id} {pre_under_count}→{post_under_count} "
                                f"(expected {pre_under_count + 1}) — rolling back"
                            )
                            self._restore_state(state)
                            continue

                        # Verify balance actually improved
                        over_target  = next((w['target_shifts'] for w in self.workers_data if w['id'] == over_id),  0)
                        under_target = next((w['target_shifts'] for w in self.workers_data if w['id'] == under_id), 0)
                        new_over_dev  = abs(post_over_count  - over_target)
                        new_under_dev = abs(post_under_count - under_target)

                        if new_over_dev <= abs(over_dev) and new_under_dev <= abs(under_dev):
                            logging.info(
                                f"  ✅ Relaxed swap: "
                                f"{over_id}({pre_over_count}→{post_over_count}) → "
                                f"{under_id}({pre_under_count}→{post_under_count}) "
                                f"on {date.strftime('%Y-%m-%d')}"
                            )
                            return True
                        else:
                            logging.debug(
                                f"  ↩️ Relaxed swap reverted (no balance gain): "
                                f"{over_id} dev {over_dev}→{post_over_count - over_target}, "
                                f"{under_id} dev {under_dev}→{post_under_count - under_target}"
                            )
                            self._restore_state(state)

        return False
    
    def _try_aggressive_three_way_swap(self, overloaded: List, underloaded: List, tolerance: int) -> bool:
        """
        Intercambio a 3 bandas más agresivo con constraints ligeramente relajadas.
        
        Se usa cuando el _try_three_way_swap normal no encuentra solución.
        Diferencias:
        - Permite intermediarios con desviación -2 a +3
        - Usa relaxation_level=1 para verificaciones
        - Explora más combinaciones
        - No verifica mejora estricta, solo que no empeore significativamente
        
        Patrón: A (sobrecargado) → C (intermediario) → B (subcargado)
        """
        logging.debug("  Trying aggressive three-way swap...")
        
        for over_id, over_dev in overloaded[:8]:  # Más trabajadores
            over_dates = list(self.worker_assignments.get(over_id, set()))
            over_dates = self._sort_dates_by_monthly_excess(over_id, over_dates)
            
            for date_a in over_dates[:15]:  # Más fechas
                if (over_id, date_a) in self.builder._locked_mandatory:
                    continue
                
                if not self.builder._can_modify_assignment(over_id, date_a, "aggressive_three_way"):
                    continue
                
                if not self._giver_month_ok(over_id, date_a):
                    continue
                
                try:
                    post_a = self.schedule[date_a].index(over_id)
                except (ValueError, KeyError):
                    continue
                
                # Buscar intermediarios con criterio más relajado
                potential_intermediaries = []
                for worker_c in self.workers_data:
                    c_id = worker_c['id']
                    if c_id == over_id:
                        continue
                    
                    c_assigned = len(self.worker_assignments.get(c_id, set()))
                    c_target = worker_c.get('target_shifts', 0)
                    if c_target == 0:
                        continue
                    c_dev = c_assigned - c_target
                    
                    # Rango más amplio: -2 a +3
                    if -2 <= c_dev <= 3:
                        potential_intermediaries.append((c_id, worker_c, c_dev, c_target))
                
                # Ordenar por desviación más cercana a 0
                potential_intermediaries.sort(key=lambda x: abs(x[2]))
                
                for c_id, worker_c, c_dev, c_target in potential_intermediaries[:10]:
                    # Verificar que C puede tomar turno de A (con relajación nivel 1)
                    score_c_take_a = self.builder._calculate_worker_score(worker_c, date_a, post_a, relaxation_level=1)
                    if score_c_take_a == float('-inf'):
                        continue
                    
                    if c_id in self.schedule.get(date_a, []):
                        continue

                    # G5: Explicit monthly cap for C taking date_a (mirrors normal three-way)
                    skip_c_take = False
                    if hasattr(self.builder, '_get_expected_monthly_target'):
                        _c_exp_m = self.builder._get_expected_monthly_target(worker_c, date_a.year, date_a.month)
                        _c_month_a = sum(1 for d in self.worker_assignments.get(c_id, set())
                                         if d.year == date_a.year and d.month == date_a.month)
                        _c_work_pct = worker_c.get('work_percentage', 100)
                        _c_monthly_tol = 1 if _c_work_pct >= 100 else 0
                        if _c_month_a + 1 > _c_exp_m + _c_monthly_tol:
                            skip_c_take = True
                    if not skip_c_take and hasattr(self.builder, '_is_weekend_or_holiday') and self.builder._is_weekend_or_holiday(date_a):
                        if hasattr(self.builder, '_would_exceed_weekend_limit_simulated'):
                            if self.builder._would_exceed_weekend_limit_simulated(c_id, date_a, self.worker_assignments):
                                skip_c_take = True
                    if skip_c_take:
                        continue
                    
                    # Buscar turnos de C que pueda ceder
                    c_dates = list(self.worker_assignments.get(c_id, set()))
                    c_dates = self._sort_dates_by_monthly_excess(c_id, c_dates)
                    
                    for date_c in c_dates[:15]:
                        if date_c == date_a:
                            continue
                        
                        if (c_id, date_c) in self.builder._locked_mandatory:
                            continue
                        
                        # C gives away date_c — check monthly floor
                        if not self._giver_month_ok(c_id, date_c):
                            continue
                        
                        if not self.builder._can_modify_assignment(c_id, date_c, "aggressive_intermediary"):
                            continue
                        
                        try:
                            post_c = self.schedule[date_c].index(c_id)
                        except (ValueError, KeyError):
                            continue
                        
                        # Buscar B que pueda tomar turno de C
                        for under_id, under_dev in underloaded[:8]:
                            if under_id in (c_id, over_id):
                                continue
                            
                            worker_b = next((w for w in self.workers_data if w['id'] == under_id), None)
                            if not worker_b:
                                continue
                            
                            if under_id in self.schedule.get(date_c, []):
                                continue
                            
                            # Verificar con relajación nivel 1
                            score_b_take_c = self.builder._calculate_worker_score(worker_b, date_c, post_c, relaxation_level=1)
                            if score_b_take_c == float('-inf'):
                                continue

                            # G5: Explicit monthly cap and weekend check for B (mirrors normal three-way)
                            skip_b_take = False
                            if hasattr(self.builder, '_get_expected_monthly_target'):
                                _b_exp_m = self.builder._get_expected_monthly_target(worker_b, date_c.year, date_c.month)
                                _b_month_c = sum(1 for d in self.worker_assignments.get(under_id, set())
                                                 if d.year == date_c.year and d.month == date_c.month)
                                _b_work_pct = worker_b.get('work_percentage', 100)
                                _b_monthly_tol = 1 if _b_work_pct >= 100 else 0
                                if _b_month_c + 1 > _b_exp_m + _b_monthly_tol:
                                    skip_b_take = True
                            if not skip_b_take and hasattr(self.builder, '_is_weekend_or_holiday') and self.builder._is_weekend_or_holiday(date_c):
                                if hasattr(self.builder, '_would_exceed_weekend_limit_simulated'):
                                    if self.builder._would_exceed_weekend_limit_simulated(under_id, date_c, self.worker_assignments):
                                        skip_b_take = True
                            if skip_b_take:
                                continue
                            
                            # Intentar el intercambio
                            state = self._save_state()
                            
                            try:
                                # A pierde turno en date_a, C lo toma
                                self.schedule[date_a][post_a] = c_id
                                self.worker_assignments[over_id].discard(date_a)
                                self.worker_assignments.setdefault(c_id, set()).add(date_a)
                                
                                # C pierde turno en date_c, B lo toma
                                self.schedule[date_c][post_c] = under_id
                                self.worker_assignments[c_id].discard(date_c)
                                self.worker_assignments.setdefault(under_id, set()).add(date_c)
                                
                                # Actualizar tracking
                                self.scheduler._update_tracking_data(over_id, date_a, post_a, removing=True)
                                self.scheduler._update_tracking_data(c_id, date_a, post_a, removing=False)
                                self.scheduler._update_tracking_data(c_id, date_c, post_c, removing=True)
                                self.scheduler._update_tracking_data(under_id, date_c, post_c, removing=False)
                                
                                # Verificar que mejora globalmente
                                over_new = len(self.worker_assignments.get(over_id, set()))
                                under_new = len(self.worker_assignments.get(under_id, set()))
                                c_new = len(self.worker_assignments.get(c_id, set()))
                                
                                over_target = next((w['target_shifts'] for w in self.workers_data if w['id'] == over_id), 0)
                                under_target = next((w['target_shifts'] for w in self.workers_data if w['id'] == under_id), 0)
                                
                                old_total_dev = abs(over_dev) + abs(under_dev) + abs(c_dev)
                                new_over_dev = over_new - over_target
                                new_under_dev = under_new - under_target
                                new_c_dev = c_new - c_target
                                new_total_dev = abs(new_over_dev) + abs(new_under_dev) + abs(new_c_dev)
                                
                                # Aceptar si mejora la suma de desviaciones
                                if new_total_dev < old_total_dev:
                                    logging.info(f"  ✅ Aggressive three-way: {over_id}({date_a.strftime('%m-%d')})→{c_id}→{under_id}({date_c.strftime('%m-%d')})")
                                    logging.debug(f"      Total deviation: {old_total_dev} → {new_total_dev}")
                                    return True
                                else:
                                    self._restore_state(state)
                                    
                            except Exception as e:
                                logging.debug(f"Aggressive three-way failed: {e}")
                                self._restore_state(state)
        
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
    
    def _try_chain_swap(self, overloaded: List, underloaded: List, tolerance: int) -> bool:
        """
        Intenta una cadena de intercambios entre 4 o más trabajadores.
        
        Patrón: A → B → C → D donde:
        - A está sobrecargado y pierde un turno
        - B, C son intermediarios que pasan turnos
        - D está subcargado y gana un turno
        
        Esto permite resolver bloqueos complejos cuando ni intercambio directo
        ni a 3 bandas funcionan.
        """
        for over_id, over_dev in overloaded[:3]:  # Top 3 más sobrecargados
            over_dates = list(self.worker_assignments.get(over_id, set()))
            over_dates = self._sort_dates_by_monthly_excess(over_id, over_dates)
            
            for date_a in over_dates[:8]:
                if (over_id, date_a) in self.builder._locked_mandatory:
                    continue
                
                if not self.builder._can_modify_assignment(over_id, date_a, "chain_swap"):
                    continue
                
                if not self._giver_month_ok(over_id, date_a):
                    continue
                
                try:
                    post_a = self.schedule[date_a].index(over_id)
                except (ValueError, KeyError):
                    continue
                
                # Buscar cadena: A → B → C → D (o más corta si es posible)
                chain = self._find_swap_chain(over_id, date_a, post_a, underloaded, max_depth=4)
                
                if chain and len(chain) >= 3:
                    # Aplicar la cadena de intercambios
                    if self._apply_chain(chain):
                        logging.info(f"  ✅ Chain swap ({len(chain)} workers): {' → '.join([c[0] for c in chain])}")
                        return True
        
        return False
    
    def _find_swap_chain(self, start_id: str, start_date, start_post: int, 
                         underloaded: List, max_depth: int = 4) -> Optional[List]:
        """
        Encuentra una cadena de intercambios usando BFS limitado.
        
        Returns:
            Lista de tuplas (worker_id, date, post, next_worker_id) o None
        """
        from collections import deque
        
        under_ids = {u[0] for u in underloaded}
        visited = {start_id}
        
        # BFS queue: (current_worker, current_date, current_post, path)
        queue = deque()
        queue.append((start_id, start_date, start_post, []))
        
        while queue:
            curr_id, curr_date, curr_post, path = queue.popleft()
            
            if len(path) >= max_depth:
                continue
            
            # Buscar trabajadores que puedan tomar este turno
            for worker in self.workers_data:
                next_id = worker['id']
                
                if next_id in visited:
                    continue
                
                if next_id in self.schedule.get(curr_date, []):
                    continue
                
                # Verificar que puede tomar el turno
                score = self.builder._calculate_worker_score(worker, curr_date, curr_post, relaxation_level=1)
                if score == float('-inf'):
                    continue
                
                # Crear nuevo paso en el path
                new_step = (curr_id, curr_date, curr_post, next_id)
                new_path = path + [new_step]
                
                # Si llegamos a un trabajador subcargado, tenemos cadena completa
                if next_id in under_ids:
                    return new_path
                
                # Si no, buscar turnos de next_id para continuar la cadena
                next_dates = list(self.worker_assignments.get(next_id, set()))
                random.shuffle(next_dates)
                
                for next_date in next_dates[:5]:
                    if (next_id, next_date) in self.builder._locked_mandatory:
                        continue
                    
                    if not self.builder._can_modify_assignment(next_id, next_date, "chain"):
                        continue
                    
                    # Giver monthly check for intermediary
                    if not self._giver_month_ok(next_id, next_date):
                        continue
                    
                    try:
                        next_post = self.schedule[next_date].index(next_id)
                    except (ValueError, KeyError):
                        continue
                    
                    visited.add(next_id)
                    queue.append((next_id, next_date, next_post, new_path))
        
        return None
    
    def _apply_chain(self, chain: List) -> bool:
        """Aplica una cadena de intercambios."""
        state = self._save_state()
        
        try:
            # Aplicar cada paso de la cadena
            for from_id, date, post, to_id in chain:
                # Actualizar schedule
                self.schedule[date][post] = to_id
                
                # Actualizar assignments
                self.worker_assignments[from_id].discard(date)
                self.worker_assignments.setdefault(to_id, set()).add(date)
                
                # Actualizar tracking
                self.scheduler._update_tracking_data(from_id, date, post, removing=True)
                self.scheduler._update_tracking_data(to_id, date, post, removing=False)
            
            # Verificar que la cadena mejoró el balance
            first_id = chain[0][0]
            last_id = chain[-1][3]
            
            first_new = len(self.worker_assignments.get(first_id, set()))
            last_new = len(self.worker_assignments.get(last_id, set()))
            
            first_target = next((w['target_shifts'] for w in self.workers_data if w['id'] == first_id), 0)
            last_target = next((w['target_shifts'] for w in self.workers_data if w['id'] == last_id), 0)
            
            # El primero debe tener menos desviación positiva
            # El último debe tener menos desviación negativa
            first_improved = (first_new - first_target) < (first_new + 1 - first_target)
            last_improved = abs(last_new - last_target) < abs(last_new - 1 - last_target)
            
            if first_improved and last_improved:
                return True
            else:
                self._restore_state(state)
                return False
                
        except Exception as e:
            logging.debug(f"Chain swap failed: {e}")
            self._restore_state(state)
            return False
    
    def _try_forced_redistribution(self, overloaded: List, underloaded: List, tolerance: int) -> bool:
        """
        Redistribución forzada: encuentra cualquier fecha vacía o con trabajador
        no esencial y redistribuye forzadamente.
        
        Esta es una estrategia de último recurso antes de relajar constraints.
        """
        # Primero intentar usar slots vacíos
        for under_id, under_dev in underloaded[:5]:
            worker_under = next((w for w in self.workers_data if w['id'] == under_id), None)
            if not worker_under:
                continue
            
            # Buscar slots vacíos que pueda tomar
            for date, slots in self.schedule.items():
                for post, assigned in enumerate(slots):
                    if assigned is None:
                        # Verificar si puede tomar este slot
                        score = self.builder._calculate_worker_score(worker_under, date, post, relaxation_level=1)
                        if score > float('-inf'):
                            # Asignar directamente
                            self.schedule[date][post] = under_id
                            self.worker_assignments.setdefault(under_id, set()).add(date)
                            self.scheduler._update_tracking_data(under_id, date, post, removing=False)
                            logging.info(f"  ✅ Forced fill: {under_id} assigned empty slot on {date.strftime('%Y-%m-%d')}")
                            return True
        
        # Si no hay slots vacíos, intentar intercambio forzado con trabajadores
        # que están dentro de tolerancia pero podrían ceder un turno
        for over_id, over_dev in overloaded[:3]:
            for worker in self.workers_data:
                mid_id = worker['id']
                
                # Buscar trabajadores con desviación positiva pero dentro de tolerancia
                mid_assigned = len(self.worker_assignments.get(mid_id, set()))
                mid_target = worker.get('target_shifts', 0)
                mid_dev = mid_assigned - mid_target
                
                # Solo trabajadores con algo de margen positivo
                if mid_dev <= 0 or mid_dev > tolerance:
                    continue
                
                mid_dates = list(self.worker_assignments.get(mid_id, set()))
                random.shuffle(mid_dates)
                
                for date in mid_dates[:10]:
                    if (mid_id, date) in self.builder._locked_mandatory:
                        continue
                    
                    if not self.builder._can_modify_assignment(mid_id, date, "forced"):
                        continue
                    
                    # Giver monthly check
                    if not self._giver_month_ok(mid_id, date):
                        continue
                    
                    try:
                        post = self.schedule[date].index(mid_id)
                    except (ValueError, KeyError):
                        continue
                    
                    # Intentar dar este turno a un subcargado
                    for under_id, under_dev in underloaded[:5]:
                        worker_under = next((w for w in self.workers_data if w['id'] == under_id), None)
                        if not worker_under:
                            continue
                        
                        if under_id in self.schedule.get(date, []):
                            continue
                        
                        score = self.builder._calculate_worker_score(worker_under, date, post, relaxation_level=1)
                        if score > float('-inf'):
                            state = self._save_state()
                            pre_mid_count  = len(self.worker_assignments.get(mid_id, set()))
                            pre_under_count = len(self.worker_assignments.get(under_id, set()))

                            self.schedule[date][post] = under_id
                            self.worker_assignments[mid_id].discard(date)
                            self.worker_assignments.setdefault(under_id, set()).add(date)
                            
                            self.scheduler._update_tracking_data(mid_id, date, post, removing=True)
                            self.scheduler._update_tracking_data(under_id, date, post, removing=False)

                            post_mid_count  = len(self.worker_assignments.get(mid_id, set()))
                            post_under_count = len(self.worker_assignments.get(under_id, set()))
                            if post_mid_count != pre_mid_count - 1 or post_under_count != pre_under_count + 1:
                                logging.warning(
                                    f"  ⚠️ Forced redistribution count error: "
                                    f"{mid_id} {pre_mid_count}→{post_mid_count}, "
                                    f"{under_id} {pre_under_count}→{post_under_count} — rolling back"
                                )
                                self._restore_state(state)
                                continue
                            
                            logging.info(f"  ✅ Forced redistribution: {mid_id}(+{mid_dev}) → {under_id}({under_dev}) on {date.strftime('%Y-%m-%d')}")
                            return True
        
        return False
