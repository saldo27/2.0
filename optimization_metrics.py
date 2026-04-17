import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any

from saldo27.utilities import get_effective_min_gap


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

        # Umbrales de mejora por tipo de operación (calibrados para mejoras incrementales)
        self.quality_thresholds = {
            "fill_empty_shifts": 0.001,  # 0.1% mejora mínima
            "balance_workloads": 0.0005,  # 0.05% mejora mínima
            "balance_weekday_distribution": 0.0005,  # 0.05% mejora mínima
            "improve_weekend_distribution": 0.0005,  # 0.05% mejora mínima
            "distribute_holiday_shifts_proportionally": 0.0005,  # 0.05% mejora mínima
            "rebalance_weekend_distribution": 0.0005,  # 0.05% mejora mínima
            "adjust_last_post_distribution": 0.0005,  # 0.05% mejora mínima
            "default": 0.0005,  # 0.05% mejora mínima por defecto
        }

    def get_schedule_hash(self) -> str:
        """Generar hash del estado actual del schedule"""
        try:
            schedule_items = []
            for date, workers in sorted(self.scheduler.schedule.items()):
                date_str = date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
                schedule_items.append((date_str, tuple(workers or [])))

            schedule_str = str(schedule_items)
            return hashlib.md5(schedule_str.encode()).hexdigest()
        except Exception as e:
            logging.warning(f"Error generating schedule hash: {e}")
            return datetime.now().isoformat()  # Fallback

    def calculate_overall_schedule_score(self, log_components: bool = False) -> float:
        """Calcular un score general del schedule actual"""
        try:
            current_hash = self.get_schedule_hash()
            if not log_components and current_hash == self.schedule_hash and "overall_score" in self.cached_metrics:
                return self.cached_metrics["overall_score"]

            # Componentes del score
            fill_rate_score = self._calculate_fill_rate_score()
            workload_balance_score = self._calculate_workload_balance_score()
            weekend_balance_score = self._calculate_weekend_balance_score()
            post_rotation_score = self._calculate_post_rotation_score()
            constraint_violations_penalty = self._calculate_constraint_violations_penalty()

            # Pesos para cada componente
            weights = {
                "fill_rate": 0.35,
                "workload_balance": 0.25,
                "weekend_balance": 0.15,
                "post_rotation": 0.15,
                "constraint_penalty": 0.10,
            }

            overall_score = (
                fill_rate_score * weights["fill_rate"]
                + workload_balance_score * weights["workload_balance"]
                + weekend_balance_score * weights["weekend_balance"]
                + post_rotation_score * weights["post_rotation"]
                - constraint_violations_penalty * weights["constraint_penalty"]
            )

            if log_components:
                logging.info(
                    f"📊 Score components: "
                    f"fill={fill_rate_score:.1f}(×.35={fill_rate_score * 0.35:.1f}) "
                    f"workload={workload_balance_score:.1f}(×.25={workload_balance_score * 0.25:.1f}) "
                    f"weekend={weekend_balance_score:.1f}(×.15={weekend_balance_score * 0.15:.1f}) "
                    f"post_rot={post_rotation_score:.1f}(×.15={post_rotation_score * 0.15:.1f}) "
                    f"penalty={constraint_violations_penalty:.1f}(×.10={constraint_violations_penalty * 0.10:.1f})"
                )

            # Cachear resultado
            self.schedule_hash = current_hash
            self.cached_metrics["overall_score"] = max(0, min(100, overall_score))

            return self.cached_metrics["overall_score"]

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
        """Calcular score basado en el balance de carga de trabajo.

        Mide la desviación media de cada trabajador respecto a su propio
        target_shifts en lugar del coeficiente de variación global.  El enfoque
        CV es incorrecto cuando existe configuración manual de objetivos distintos
        (p. ej. GABI=60, MÓNICA=24): ese spread es intencionado y no debe
        penalizarse como desequilibrio.
        """
        try:
            if not self.scheduler.workers_data:
                return 100.0

            deviations = []
            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                target = worker.get("target_shifts", 0)
                count = len(self.scheduler.worker_assignments.get(worker_id, set()))
                if target > 0:
                    deviations.append(abs(count - target) / target)

            if not deviations:
                return 100.0

            avg_deviation_pct = sum(deviations) / len(deviations)
            # Escala: 0 % desv → 100, 10 % desv → 60, 25 % desv → 0
            return max(0.0, 100.0 - avg_deviation_pct * 400.0)

        except Exception as e:
            logging.error(f"Error calculating workload balance score: {e}")
            return 0.0

    def _calculate_weekend_balance_score(self) -> float:
        """Calcular score basado en el balance de fines de semana.

        Counts Friday + Saturday + Sunday + Holidays + Pre-holidays to match
        the definition used by the scheduling operations.
        Compares each worker's actual weekend count against their proportional
        expected weekend count (target_shifts × overall_weekend_ratio), so workers
        with intentionally different targets (manual config) are scored fairly.
        """
        try:
            if not self.scheduler.workers_data:
                return 100.0

            holidays = getattr(self.scheduler, "holidays", set())

            # Compute per-worker weekend counts and total shift counts first
            worker_weekend_raw = {}
            worker_total_raw = {}
            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                assignments = self.scheduler.worker_assignments.get(worker_id, set())
                worker_total_raw[worker_id] = len(assignments)
                worker_weekend_raw[worker_id] = sum(
                    1
                    for date in assignments
                    if hasattr(date, "weekday")
                    and (
                        date.weekday() >= 4  # Viernes, Sábado, Domingo
                        or date in holidays  # Festivos
                        or (date + timedelta(days=1)) in holidays  # Prefestivos
                    )
                )

            total_all = sum(worker_total_raw.values())
            total_wknd = sum(worker_weekend_raw.values())
            if total_all == 0:
                return 100.0
            weekend_ratio = total_wknd / total_all

            deviations = []
            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                target = worker.get("target_shifts", 0)
                expected_wknd = target * weekend_ratio
                actual_wknd = worker_weekend_raw[worker_id]
                if expected_wknd > 0:
                    deviations.append(abs(actual_wknd - expected_wknd) / expected_wknd)

            if not deviations:
                return 100.0

            avg_deviation_pct = sum(deviations) / len(deviations)
            # Escala: 0 % desv → 100, 10 % desv → 60, 25 % desv → 0
            return max(0.0, 100.0 - avg_deviation_pct * 400.0)

        except Exception as e:
            logging.error(f"Error calculating weekend balance score: {e}")
            return 0.0

    def _calculate_post_rotation_score(self) -> float:
        """Calcular score basado en la rotación de puestos.

        Compares each worker's actual post distribution against the expected
        EQUAL share across their AVAILABLE posts (not num_shifts, which would
        wrongly penalise workers with no_last_post=True for never filling P4).

        Uses mean absolute relative deviation so the penalty scales with the
        magnitude of imbalance instead of absolute squared differences.
        """
        try:
            if not self.scheduler.workers_data or self.scheduler.num_shifts <= 1:
                return 100.0

            post_balance_scores = []

            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                assignments = self.scheduler.worker_assignments.get(worker_id, set())

                if not assignments:
                    continue

                # Contar asignaciones por puesto
                post_counts: dict[int, int] = {}
                for date in assignments:
                    if date in self.scheduler.schedule:
                        workers_in_posts = self.scheduler.schedule[date]
                        for post_idx, assigned_worker in enumerate(workers_in_posts):
                            if assigned_worker == worker_id:
                                post_counts[post_idx] = post_counts.get(post_idx, 0) + 1

                if not post_counts:
                    continue

                # Expected count per AVAILABLE post (not num_shifts, which would
                # penalise no_last_post workers for never filling P4).
                n_posts_used = len(post_counts)
                expected_per_post = sum(post_counts.values()) / n_posts_used

                if expected_per_post == 0:
                    continue

                # Mean absolute relative deviation: 0 = perfect, 1 = all on one post
                mard = (
                    sum(abs(count - expected_per_post) for count in post_counts.values())
                    / n_posts_used
                    / expected_per_post
                )
                # Scale: 0 % deviation → 100, 25 % → 75, 100 % → 0
                worker_score = max(0.0, 100.0 - mard * 100.0)
                post_balance_scores.append(worker_score)

            return sum(post_balance_scores) / len(post_balance_scores) if post_balance_scores else 100.0

        except Exception as e:
            logging.error(f"Error calculating post rotation score: {e}")
            return 0.0

    def _calculate_constraint_violations_penalty(self) -> float:
        """Calcular penalización por violaciones de restricciones.

        Efficiently counts gap violations by iterating sorted assignments per
        worker, and incompatibility violations by checking each date's assigned
        worker pairs.
        """
        try:
            violation_count = 0
            scheduler = self.scheduler

            # --- Gap violations (fast: O(total_assignments) per worker) ---
            for worker in scheduler.workers_data:
                worker_id = worker["id"]
                assignments = sorted(scheduler.worker_assignments.get(worker_id, set()))
                if len(assignments) < 2:
                    continue
                min_days = get_effective_min_gap(worker, scheduler.gap_between_shifts)

                for i in range(1, len(assignments)):
                    diff = (assignments[i] - assignments[i - 1]).days
                    if diff < min_days or (
                        diff in (7, 14) and assignments[i].weekday() == assignments[i - 1].weekday()
                    ):
                        violation_count += 1

            # --- Incompatibility violations (fast: O(dates * posts^2)) ---
            checker = getattr(scheduler, "constraint_checker", None)
            if checker is not None:
                for date, workers in scheduler.schedule.items():
                    assigned = [w for w in workers if w is not None]
                    for i in range(len(assigned)):
                        for j in range(i + 1, len(assigned)):
                            if checker._are_workers_incompatible(assigned[i], assigned[j]):
                                violation_count += 1

            # Scale: each violation penalises ~1 point (out of 100)
            # Keep penalty light so balance improvements aren't drowned out
            return min(100.0, violation_count * 1.0)

        except Exception as e:
            logging.error(f"Error calculating constraint violations penalty: {e}")
            return 0.0

    def evaluate_improvement_quality(
        self, before_score: float, after_score: float, operation_type: str
    ) -> tuple[bool, float]:
        """
        Evaluar la calidad de una mejora

        Returns:
            Tuple[bool, float]: (es_mejora_significativa, improvement_ratio)
        """
        try:
            if before_score <= 0:
                # Si el score anterior era 0 o negativo, cualquier mejora es buena
                is_significant = after_score > before_score
                improvement_ratio = float("inf") if before_score == 0 else after_score / before_score - 1
            else:
                improvement_ratio = (after_score - before_score) / before_score
                threshold = self.quality_thresholds.get(operation_type, self.quality_thresholds["default"])
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
        """
        Calcular el desbalance de carga de trabajo como ratio.

        CRITICAL: Compara asignaciones non-mandatory vs target_shifts (que ya tiene mandatory restado).
        Esto asegura que workers con mandatory no parezcan sobrecargados incorrectamente.
        """
        try:
            if not self.scheduler.workers_data:
                return 0.0

            deviation_ratios = []

            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                target = worker.get("target_shifts", 0)

                if target <= 0:
                    continue

                all_assignments = self.scheduler.worker_assignments.get(worker_id, set())
                total_count = len(all_assignments)

                # CRITICAL: Exclude mandatory shifts from count since target_shifts already has them subtracted
                mandatory_dates = set()
                mandatory_str = worker.get("mandatory_days", "")
                if mandatory_str and hasattr(self.scheduler, "date_utils"):
                    try:
                        mandatory_dates = set(self.scheduler.date_utils.parse_dates(mandatory_str))
                    except Exception:
                        pass

                mandatory_assigned = sum(1 for d in all_assignments if d in mandatory_dates)
                non_mandatory_count = total_count - mandatory_assigned

                # Calculate deviation ratio: (actual - target) / target
                # Positive = overloaded, Negative = underloaded
                deviation_ratio = (non_mandatory_count - target) / target
                deviation_ratios.append(abs(deviation_ratio))

            if not deviation_ratios:
                return 0.0

            # Return max deviation as the workload imbalance indicator
            return max(deviation_ratios)

        except Exception as e:
            logging.error(f"Error calculating workload imbalance: {e}")
            return 0.0

    def calculate_weekend_imbalance(self) -> float:
        """Calcular el desbalance de fines de semana.

        Uses the same definition as the score component: Fri+Sat+Sun + holidays
        + pre-holidays, normalised by work_percentage.
        """
        try:
            if not self.scheduler.workers_data:
                return 0.0

            holidays = getattr(self.scheduler, "holidays", set())
            weekend_counts: list[float] = []

            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                work_percentage = worker.get("work_percentage", 100)
                assignments = self.scheduler.worker_assignments.get(worker_id, set())

                weekend_count = sum(
                    1
                    for date in assignments
                    if hasattr(date, "weekday")
                    and (
                        date.weekday() >= 4  # Viernes, Sábado, Domingo
                        or date in holidays  # Festivos
                        or (date + timedelta(days=1)) in holidays  # Prefestivos
                    )
                )

                if work_percentage > 0:
                    weekend_counts.append(weekend_count * 100 / work_percentage)

            if not weekend_counts:
                return 0.0

            mean_val = sum(weekend_counts) / len(weekend_counts)
            if mean_val == 0:
                return 0.0

            variance = sum((c - mean_val) ** 2 for c in weekend_counts) / len(weekend_counts)
            cv = (variance**0.5) / mean_val
            return cv

        except Exception as e:
            logging.error(f"Error calculating weekend imbalance: {e}")
            return 0.0

    def record_iteration_result(
        self,
        iteration: int,
        operation_results: dict[str, Any],
        overall_score: float,
        *,
        sa_accepts: int = 0,
        best_score: float | None = None,
    ) -> None:
        """Registrar resultados de una iteración para análisis de tendencias"""
        try:
            iteration_data = {
                "iteration": iteration,
                "timestamp": datetime.now(),
                "overall_score": overall_score,
                "best_score": best_score if best_score is not None else overall_score,
                "sa_accepts": sa_accepts,
                "operations": operation_results.copy(),
                "improvements_count": sum(
                    1
                    for result in operation_results.values()
                    if isinstance(result, dict) and result.get("improved", False)
                ),
            }

            self.iteration_history.append(iteration_data)

            # Mantener solo las últimas 10 iteraciones para análisis
            if len(self.iteration_history) > 10:
                self.iteration_history = self.iteration_history[-10:]

        except Exception as e:
            logging.error(f"Error recording iteration result: {e}")

    def should_continue_optimization(self, current_iteration: int) -> tuple[bool, str]:
        """
        Determinar si debe continuar la optimización basándose en tendencias.

        Usa best_score (checkpoint) en vez de overall_score para evaluar progreso,
        ya que SA puede aceptar caídas temporales que no representan estancamiento real.

        Returns:
            Tuple[bool, str]: (should_continue, reason)
        """
        try:
            if len(self.iteration_history) < 3:
                return True, "Insuficientes datos para análisis de tendencia"

            # Analizar últimas 3 iteraciones
            recent_iterations = self.iteration_history[-3:]

            # Count SA accepts in the window — SA is actively exploring
            sa_accepts_in_window = sum(it.get("sa_accepts", 0) for it in recent_iterations)

            # Verificar si hay mejoras en las últimas 3 iteraciones
            recent_improvements = [iteration["improvements_count"] for iteration in recent_iterations]

            # If SA accepted drops, count those as "exploration activity"
            if sum(recent_improvements) == 0 and sa_accepts_in_window == 0:
                return False, "Sin mejoras en las últimas 3 iteraciones"

            # Use best_score (checkpoint) for trend analysis — immune to SA drops
            recent_best_scores = [it.get("best_score", it["overall_score"]) for it in recent_iterations]

            # Calcular mejora promedio sobre best scores
            score_improvements = []
            for i in range(1, len(recent_best_scores)):
                if recent_best_scores[i - 1] > 0:
                    improvement = (recent_best_scores[i] - recent_best_scores[i - 1]) / recent_best_scores[i - 1]
                    score_improvements.append(improvement)

            if score_improvements:
                avg_improvement = sum(score_improvements) / len(score_improvements)

                # Si la mejora promedio es muy pequeña y SA no está explorando, parar
                if avg_improvement < 0.001 and sa_accepts_in_window == 0:
                    return False, f"Mejoras marginales detectadas (avg: {avg_improvement:.4f})"

                # Even with SA active, stop if best_score is completely flat for 3 iterations
                if avg_improvement < 0.0001:
                    return False, (
                        f"Best score estancado (avg: {avg_improvement:.4f}, SA accepts: {sa_accepts_in_window})"
                    )

            # Verificar si se alcanzó un score excelente
            current_best = recent_best_scores[-1]
            if current_best >= 95.0:
                return False, f"Score excelente alcanzado: {current_best:.2f}"

            return True, f"Continuando optimización (best: {current_best:.2f}, SA accepts: {sa_accepts_in_window})"

        except Exception as e:
            logging.error(f"Error in should_continue_optimization: {e}")
            return True, "Error en análisis, continuando por seguridad"
