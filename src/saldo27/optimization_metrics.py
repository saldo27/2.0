import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, ClassVar

from saldo27.balance_validator import BalanceValidator
from saldo27.constraint_checker import config_forced_dates
from saldo27.structural_score import StructuralScoreBaselines
from saldo27.utilities import get_effective_min_gap, is_fixed_cadence


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
        self._baselines: StructuralScoreBaselines | None = None
        self._baselines_hash: str | None = None

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

    def _structural_baselines(self) -> StructuralScoreBaselines:
        """Fresh baselines when the schedule hash changes; reuse within one score pass."""
        current_hash = self.get_schedule_hash()
        if self._baselines is None or self._baselines_hash != current_hash:
            self._baselines = StructuralScoreBaselines(self.scheduler)
            self._baselines_hash = current_hash
        return self._baselines

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

    # Pesos del score global (compartidos por calculate / breakdown / UI).
    SCORE_WEIGHTS: ClassVar[dict[str, float]] = {
        "fill_rate": 0.35,
        "workload_balance": 0.25,
        "weekend_balance": 0.10,
        "bridge_balance": 0.05,
        "post_rotation": 0.15,
        "constraint_penalty": 0.10,
    }

    def get_score_breakdown(self) -> dict[str, Any]:
        """Return overall score plus raw component scores and weights.

        Used by the Revisión UI and diagnostics. ``overall`` is clamped to 0–100
        (same as :meth:`calculate_overall_schedule_score`).
        """
        fill_rate_score = self._calculate_fill_rate_score()
        workload_balance_score = self._calculate_workload_balance_score()
        weekend_balance_score = self._calculate_weekend_balance_score()
        bridge_balance_score = self._calculate_bridge_balance_score()
        post_rotation_score = self._calculate_post_rotation_score()
        constraint_violations_penalty = self._calculate_constraint_violations_penalty()
        weights = dict(self.SCORE_WEIGHTS)

        overall_score = (
            fill_rate_score * weights["fill_rate"]
            + workload_balance_score * weights["workload_balance"]
            + weekend_balance_score * weights["weekend_balance"]
            + bridge_balance_score * weights["bridge_balance"]
            + post_rotation_score * weights["post_rotation"]
            - constraint_violations_penalty * weights["constraint_penalty"]
        )
        overall = max(0.0, min(100.0, float(overall_score)))

        # Keep the regular score cache coherent for subsequent calls.
        self.schedule_hash = self.get_schedule_hash()
        self.cached_metrics["overall_score"] = overall

        return {
            "overall": overall,
            "components": {
                "fill_rate": float(fill_rate_score),
                "workload_balance": float(workload_balance_score),
                "weekend_balance": float(weekend_balance_score),
                "bridge_balance": float(bridge_balance_score),
                "post_rotation": float(post_rotation_score),
                "constraint_penalty": float(constraint_violations_penalty),
            },
            "weights": weights,
        }

    def calculate_overall_schedule_score(self, log_components: bool = False) -> float:
        """Calcular un score general del schedule actual"""
        try:
            current_hash = self.get_schedule_hash()
            if not log_components and current_hash == self.schedule_hash and "overall_score" in self.cached_metrics:
                return self.cached_metrics["overall_score"]

            breakdown = self.get_score_breakdown()
            components = breakdown["components"]
            weights = breakdown["weights"]

            if log_components:
                logging.info(
                    f"📊 Score components: "
                    f"fill={components['fill_rate']:.1f}"
                    f"(×.{int(weights['fill_rate'] * 100):02d}"
                    f"={components['fill_rate'] * weights['fill_rate']:.1f}) "
                    f"workload={components['workload_balance']:.1f}"
                    f"(×.{int(weights['workload_balance'] * 100):02d}"
                    f"={components['workload_balance'] * weights['workload_balance']:.1f}) "
                    f"weekend={components['weekend_balance']:.1f}"
                    f"(×.{int(weights['weekend_balance'] * 100):02d}"
                    f"={components['weekend_balance'] * weights['weekend_balance']:.1f}) "
                    f"bridge={components['bridge_balance']:.1f}"
                    f"(×.{int(weights['bridge_balance'] * 100):02d}"
                    f"={components['bridge_balance'] * weights['bridge_balance']:.1f}) "
                    f"post_rot={components['post_rotation']:.1f}"
                    f"(×.{int(weights['post_rotation'] * 100):02d}"
                    f"={components['post_rotation'] * weights['post_rotation']:.1f}) "
                    f"penalty={components['constraint_penalty']:.1f}"
                    f"(×.{int(weights['constraint_penalty'] * 100):02d}"
                    f"={components['constraint_penalty'] * weights['constraint_penalty']:.1f})"
                )

            return float(breakdown["overall"])

        except Exception as e:
            logging.error(f"Error calculating overall schedule score: {e}")
            return 0.0

    def _calculate_fill_rate_score(self) -> float:
        """Fill rate treating structurally unfillable holes as filled."""
        try:
            return self._structural_baselines().structural_fill_rate()
        except Exception as e:
            logging.error(f"Error calculating fill rate score: {e}")
            return 0.0

    def _calculate_workload_balance_score(self) -> float:
        """Workload balance vs clique-scaled ``scoring_shift_target`` (±10% deadzone).

        Configuration-driven under-capacity (incompat cliques) is absorbed into
        the scoring target so it does not look like an optimiser failure.
        """
        try:
            if not self.scheduler.workers_data:
                return 100.0

            baselines = self._structural_baselines()
            date_utils = getattr(self.scheduler, "date_utils", None)
            deviations = []
            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                target = baselines.scoring_shift_target(worker)
                assignments = self.scheduler.worker_assignments.get(worker_id, set())
                # target_shifts excludes mandatory shifts, so compare against non-mandatory only.
                _, count = BalanceValidator.compute_non_mandatory_assigned(worker, assignments, date_utils)
                if target > 0:
                    deviations.append(abs(count - target) / target)

            if not deviations:
                return 100.0

            deadzone = 0.10
            effective_deviations = [max(0.0, dev - deadzone) for dev in deviations]
            avg_deviation_pct = sum(effective_deviations) / len(effective_deviations)
            return max(0.0, 100.0 - avg_deviation_pct * 400.0)

        except Exception as e:
            logging.error(f"Error calculating workload balance score: {e}")
            return 0.0

    def _calculate_weekend_balance_score(self) -> float:
        """Weekend balance vs clique-scaled scoring weekend objectives (±15% deadzone)."""
        try:
            if not self.scheduler.workers_data:
                return 100.0

            baselines = self._structural_baselines()
            holidays = getattr(self.scheduler, "holidays", set())

            deviations = []
            for worker in self.scheduler.workers_data:
                assignments = self.scheduler.worker_assignments.get(worker["id"], set())
                actual_wknd = sum(
                    1
                    for date in assignments
                    if hasattr(date, "weekday") and self.scheduler.date_utils.is_weekend_day(date, holidays)
                )
                expected_wknd = baselines.scoring_weekend_objective(worker)
                if expected_wknd > 0:
                    deviations.append(abs(actual_wknd - expected_wknd) / expected_wknd)

            if not deviations:
                return 100.0

            deadzone = 0.15
            effective_deviations = [max(0.0, dev - deadzone) for dev in deviations]
            avg_deviation_pct = sum(effective_deviations) / len(effective_deviations)
            return max(0.0, 100.0 - avg_deviation_pct * 400.0)

        except Exception as e:
            logging.error(f"Error calculating weekend balance score: {e}")
            return 0.0

    def _calculate_bridge_balance_score(self) -> float:
        """Bridge balance vs clique-scaled scoring bridge objectives (±0.5 deadzone)."""
        try:
            bridge_periods = getattr(self.scheduler, "bridge_periods", None)
            if not bridge_periods or not self.scheduler.workers_data:
                return 100.0

            baselines = self._structural_baselines()
            deviations = []
            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                if self.scheduler._shift_objective(worker) <= 0:
                    continue
                target_bridges = baselines.scoring_bridge_objective(worker)
                if target_bridges <= 0:
                    continue
                actual_bridges = self.scheduler.count_bridges_for_worker(worker_id)
                deviations.append(abs(actual_bridges - target_bridges))

            if not deviations:
                return 100.0

            deadzone = 0.5
            effective_deviations = [max(0.0, dev - deadzone) for dev in deviations]
            avg_excess = sum(effective_deviations) / len(effective_deviations)
            return max(0.0, 100.0 - avg_excess * 50.0)

        except Exception as e:
            logging.error(f"Error calculating bridge balance score: {e}")
            return 0.0

    def _calculate_post_rotation_score(self) -> float:
        """Post-rotation score vs structural Rosell expectations (spillover-neutral).

        ``no_last_post`` / ``only_last_post`` / cadence workers are perfect by
        config. Unrestricted workers are compared to ``get_last_post_objective``
        (already structural) plus an even split of the remaining mass on non-last
        posts, so absorbing extra Rosell forced by ``no_last`` peers does not
        penalise them.
        """
        try:
            if not self.scheduler.workers_data or self.scheduler.num_shifts <= 1:
                return 100.0

            baselines = self._structural_baselines()
            post_balance_scores = []
            num_shifts = self.scheduler.num_shifts

            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                assignments = self.scheduler.worker_assignments.get(worker_id, set())
                if not assignments:
                    continue

                post_counts: dict[int, int] = {i: 0 for i in range(num_shifts)}
                for date in assignments:
                    if date in self.scheduler.schedule:
                        workers_in_posts = self.scheduler.schedule[date]
                        for post_idx, assigned_worker in enumerate(workers_in_posts):
                            if assigned_worker == worker_id:
                                post_counts[post_idx] = post_counts.get(post_idx, 0) + 1

                total = sum(post_counts.values())
                if total == 0:
                    continue

                expected = baselines.expected_post_counts(worker, total)
                if expected is None:
                    post_balance_scores.append(100.0)
                    continue

                # Mean absolute relative deviation vs structural expectation.
                mard = (
                    sum(abs(post_counts.get(i, 0) - expected.get(i, 0.0)) for i in range(num_shifts))
                    / num_shifts
                    / (total / num_shifts)
                )
                effective_mard = max(0.0, mard - 0.20)
                worker_score = max(0.0, 100.0 - effective_mard * 100.0)
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
            # Cadence workers, and a pair whose two dates are both mandatory, are
            # configuration: not repaired and not scored. One mandatory date does
            # not excuse the pair; repair moves the other day.
            protected_by_worker = {
                worker["id"]: config_forced_dates(worker, scheduler.date_utils) for worker in scheduler.workers_data
            }
            for worker in scheduler.workers_data:
                if is_fixed_cadence(worker):
                    continue
                worker_id = worker["id"]
                assignments = sorted(scheduler.worker_assignments.get(worker_id, set()))
                if len(assignments) < 2:
                    continue
                protected = protected_by_worker[worker_id]
                gap_periods = getattr(scheduler, "gap_periods", None) or []
                for i in range(1, len(assignments)):
                    if assignments[i] in protected and assignments[i - 1] in protected:
                        continue
                    min_days = get_effective_min_gap(
                        worker,
                        scheduler.gap_between_shifts,
                        date=assignments[i],
                        other_date=assignments[i - 1],
                        gap_periods=gap_periods,
                    )
                    diff = (assignments[i] - assignments[i - 1]).days
                    if diff < min_days or (
                        diff in (7, 14) and assignments[i].weekday() == assignments[i - 1].weekday()
                    ):
                        violation_count += 1

            # --- Incompatibility violations (fast: O(dates * posts^2)) ---
            # Both sides mandatory or cadence: configuration conflict, no penalty.
            checker = getattr(scheduler, "constraint_checker", None)
            if checker is not None:
                for date, workers in scheduler.schedule.items():
                    assigned = [w for w in workers if w is not None]
                    for i in range(len(assigned)):
                        for j in range(i + 1, len(assigned)):
                            if not checker._are_workers_incompatible(assigned[i], assigned[j]):
                                continue
                            if date in protected_by_worker.get(assigned[i], ()) and date in protected_by_worker.get(
                                assigned[j], ()
                            ):
                                continue
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
                    except (TypeError, ValueError) as exc:
                        logging.debug(f"Could not parse mandatory_days for worker {worker_id}: {exc}")

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
        + pre-holidays, normalised by target_shifts (not work_percentage) so that
        workers with intentionally different shift targets (e.g. manual workers)
        are compared fairly.
        """
        try:
            if not self.scheduler.workers_data:
                return 0.0

            holidays = getattr(self.scheduler, "holidays", set())
            weekend_counts: list[float] = []

            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                target_shifts = worker.get("target_shifts", 0)
                assignments = self.scheduler.worker_assignments.get(worker_id, set())

                weekend_count = sum(
                    1
                    for date in assignments
                    if hasattr(date, "weekday") and self.scheduler.date_utils.is_weekend_day(date, holidays)
                )

                if target_shifts > 0:
                    weekend_counts.append(weekend_count / target_shifts)

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

    def calculate_bridge_imbalance(self) -> float:
        """Calcular el desbalance de turnos en días de puente.

        Coeficiente de variación (CV) de bridge_shifts/target_shifts entre
        trabajadores, análogo a ``calculate_weekend_imbalance``. Se usa en
        ``SchedulerCore._finalization_phase`` para decidir si una pasada de
        finalización debe revertirse: antes de este método, el desbalance de
        puentes era completamente invisible en esa comparación pre/post
        (sólo se consideraban workload y weekend), así que una pasada que
        mejorase drásticamente el balance de puentes pero empeorase
        ligeramente otra métrica podía revertirse sin tener en cuenta esa
        pérdida, y viceversa.

        Devuelve 0.0 si no hay periodos de puente definidos.
        """
        try:
            bridge_periods = getattr(self.scheduler, "bridge_periods", None)
            if not bridge_periods or not self.scheduler.workers_data:
                return 0.0

            ratios: list[float] = []
            for worker in self.scheduler.workers_data:
                worker_id = worker["id"]
                target_shifts = worker.get("target_shifts", 0)
                if target_shifts <= 0:
                    continue
                bridge_count = self.scheduler.count_bridges_for_worker(worker_id)
                ratios.append(bridge_count / target_shifts)

            if not ratios:
                return 0.0

            mean_val = sum(ratios) / len(ratios)
            if mean_val == 0:
                return 0.0

            variance = sum((r - mean_val) ** 2 for r in ratios) / len(ratios)
            cv = (variance**0.5) / mean_val
            return cv

        except Exception as e:
            logging.error(f"Error calculating bridge imbalance: {e}")
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
