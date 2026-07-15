"""
Final Adjustment Engine
=======================

Motor de ajuste final post-generación que equilibra las desviaciones de los
trabajadores en tres dimensiones:

1. Turnos/mes (shifts): delegado al StrictBalanceOptimizer existente.
2. Fines de semana (weekends): intercambio de turno de fin-de-semana de un
   trabajador sobrecargado por un turno de día-laborable del trabajador
   subcargado, de forma que el balance global de turnos no se vea afectado.
3. Puentes (bridges): igual que weekends pero aplicado a días de puente.
4. OR-Tools CP-SAT: refinamiento global post-greedy que optimiza turnos,
   weekends y puentes de forma conjunta respetando todas las restricciones
   duras (gap, incompatibilidades, disponibilidad, mandatory).

Estrategia de intercambio "pareado" (paired swap) para weekends/bridges
-----------------------------------------------------------------------
Para no romper el balance de turnos al reequilibrar weekends/bridges:
  - A (excess weekends) cede `date_wknd` a B (deficit weekends).
  - B cede `date_wkday` a A.
  - El total de turnos de A y B no varía; solo cambia qué días trabajan.

Cada intercambio se valida con el ConstraintChecker del scheduler antes de
aplicarlo, y se revierte si no produce mejora neta en la dimensión objetivo.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime

    from saldo27.scheduler import Scheduler


class FinalAdjustmentEngine:
    """Motor de ajuste final que equilibra turnos, fines de semana y puentes."""

    def __init__(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler
        self.schedule = scheduler.schedule
        self.worker_assignments = scheduler.worker_assignments
        self.workers_data = scheduler.workers_data
        self.schedule_builder = getattr(scheduler, "schedule_builder", None)
        self.constraint_checker = getattr(scheduler, "constraint_checker", None)

        self.holidays_set: set[datetime] = set(scheduler.holidays) if scheduler.holidays else set()

        # Pre-compute period-wide totals needed for targets
        self._total_all_slots: int = 0
        self._total_weekend_slots: int = 0
        self._total_bridge_slots: int = 0
        self._precompute_slot_totals()

        # Cache raw targets to avoid repeated O(n) scans inside hot loops
        self._raw_targets: dict[str, int] = {w["id"]: self._get_worker_raw_target(w) for w in self.workers_data}

        # Counters for reporting
        self.stats: dict[str, int] = {
            "shift_swaps": 0,
            "weekend_swaps": 0,
            "bridge_swaps": 0,
            "ortools_reassignments": 0,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, max_iterations: int = 300) -> dict[str, Any]:
        """
        Ejecuta el ciclo completo de ajuste final.

        Args:
            max_iterations: Límite total de iteraciones (distribuido entre las
                            tres fases).

        Returns:
            Dict con métricas antes/después y estadísticas de intercambios.
        """
        logging.info("=" * 70)
        logging.info("FINAL ADJUSTMENT ENGINE - Starting")
        logging.info("=" * 70)

        before = self.compute_metrics()

        iters_per_phase = max(max_iterations // 3, 1)

        # --- Phase 1: Shift balance ----------------------------------------
        logging.info("Phase FA-1: Shift balance")
        self._run_shift_balance(iters_per_phase)

        # --- Phase 2: Weekend balance --------------------------------------
        logging.info("Phase FA-2: Weekend balance")
        for _ in range(iters_per_phase):
            if not self._improve_weekend_balance():
                break
            self.stats["weekend_swaps"] += 1

        # --- Phase 3: Bridge balance ---------------------------------------
        logging.info("Phase FA-3: Bridge balance")
        for _ in range(iters_per_phase):
            if not self._improve_bridge_balance():
                break
            self.stats["bridge_swaps"] += 1

        # --- Phase 4: OR-Tools CP-SAT refinement --------------------------
        logging.info("Phase FA-4: OR-Tools CP-SAT refinement")
        self.stats["ortools_reassignments"] = self._run_ortools_phase()

        after = self.compute_metrics()

        logging.info("=" * 70)
        logging.info("FINAL ADJUSTMENT ENGINE - Results")
        logging.info(f"  Shift swaps:          {self.stats['shift_swaps']}")
        logging.info(f"  Weekend swaps:        {self.stats['weekend_swaps']}")
        logging.info(f"  Bridge swaps:         {self.stats['bridge_swaps']}")
        logging.info(f"  OR-Tools changes:     {self.stats['ortools_reassignments']}")
        logging.info("=" * 70)

        return {
            "before": before,
            "after": after,
            "stats": dict(self.stats),
        }

    def compute_metrics(self) -> dict[str, dict[str, Any]]:
        """
        Devuelve un dict con las métricas actuales de cada trabajador.

        Structure::

            {
                worker_id: {
                    "name": str,
                    "shift_target": int,
                    "shift_assigned": int,
                    "shift_deviation": int,
                    "weekend_target": int,
                    "weekend_assigned": int,
                    "weekend_deviation": int,
                    "bridge_target": int,
                    "bridge_assigned": int,
                    "bridge_deviation": int,
                },
                ...
            }
        """
        metrics: dict[str, dict[str, Any]] = {}
        for worker in self.workers_data:
            wid = worker["id"]
            raw_target = self._raw_targets.get(wid, 0)

            assigned_dates = self.worker_assignments.get(wid, set())
            total_assigned = len(assigned_dates)

            weekend_assigned = sum(
                1 for d in assigned_dates if self.scheduler.date_utils.is_weekend_day(d, self.holidays_set)
            )
            bridge_assigned = len(self.scheduler.worker_bridge_counts.get(wid, set()))

            weekend_target = self._weekend_target_for(raw_target)
            bridge_target = self._bridge_target_for(raw_target)

            metrics[wid] = {
                "name": worker.get("name", wid),
                "shift_target": raw_target,
                "shift_assigned": total_assigned,
                "shift_deviation": total_assigned - raw_target,
                "weekend_target": weekend_target,
                "weekend_assigned": weekend_assigned,
                "weekend_deviation": weekend_assigned - weekend_target,
                "bridge_target": bridge_target,
                "bridge_assigned": bridge_assigned,
                "bridge_deviation": bridge_assigned - bridge_target,
            }
        return metrics

    # ------------------------------------------------------------------
    # Internal: slot pre-computation and target helpers
    # ------------------------------------------------------------------

    def _precompute_slot_totals(self) -> None:
        """Pre-computa totales de slots por tipo de día para calcular targets."""
        from datetime import timedelta

        current = self.scheduler.start_date
        while current <= self.scheduler.end_date:
            n = self.scheduler._get_shifts_for_date(current)
            self._total_all_slots += n
            if self.scheduler.date_utils.is_weekend_day(current, self.holidays_set):
                self._total_weekend_slots += n
            if self.scheduler.date_utils.is_bridge_day(current, self.scheduler.bridge_periods):
                self._total_bridge_slots += n
            current += timedelta(days=1)

    def _weekend_target_for(self, raw_target: int) -> int:
        """Target proporcional de fines de semana para un trabajador."""
        if self._total_all_slots == 0:
            return 0
        ratio = self._total_weekend_slots / self._total_all_slots
        return round(raw_target * ratio)

    def _bridge_target_for(self, raw_target: int) -> int:
        """Target proporcional de puentes para un trabajador."""
        if self._total_all_slots == 0 or self._total_bridge_slots == 0:
            return 0
        ratio = self._total_bridge_slots / self._total_all_slots
        return round(raw_target * ratio)

    @staticmethod
    def _get_worker_raw_target(worker: dict) -> int:
        """
        Devuelve el objetivo bruto de turnos para un trabajador.

        Usa ``_raw_target`` cuando está presente y no es ``None``; en caso
        contrario recurre a ``target_shifts``.  El valor 0 en ``_raw_target``
        es válido (trabajador sin turnos) y no provoca la sustitución.
        """
        raw = worker.get("_raw_target")
        if raw is not None:
            return raw
        return worker.get("target_shifts", 0)

    # ------------------------------------------------------------------
    # Internal: state save/restore (mirrors StrictBalanceOptimizer)
    # ------------------------------------------------------------------

    def _save_state(self) -> dict:
        return {
            "schedule": {k: v[:] for k, v in self.schedule.items()},
            "assignments": {k: set(v) for k, v in self.worker_assignments.items()},
            "shift_counts": dict(self.scheduler.worker_shift_counts),
            "weekdays": {k: dict(v) for k, v in self.scheduler.worker_weekdays.items()},
            "weekends": {k: list(v) for k, v in self.scheduler.worker_weekends.items()},
            "weekend_counts": dict(self.scheduler.worker_weekend_counts),
            "bridge_counts": {k: set(v) for k, v in self.scheduler.worker_bridge_counts.items()},
        }

    def _restore_state(self, state: dict) -> None:
        self.schedule.clear()
        self.schedule.update({k: v[:] for k, v in state["schedule"].items()})

        self.worker_assignments.clear()
        self.worker_assignments.update({k: set(v) for k, v in state["assignments"].items()})

        self.scheduler.worker_shift_counts.clear()
        self.scheduler.worker_shift_counts.update(state["shift_counts"])

        self.scheduler.worker_weekdays.clear()
        self.scheduler.worker_weekdays.update({k: dict(v) for k, v in state["weekdays"].items()})

        self.scheduler.worker_weekends.clear()
        self.scheduler.worker_weekends.update({k: list(v) for k, v in state["weekends"].items()})

        self.scheduler.worker_weekend_counts.clear()
        self.scheduler.worker_weekend_counts.update(state["weekend_counts"])

        self.scheduler.worker_bridge_counts.clear()
        self.scheduler.worker_bridge_counts.update({k: set(v) for k, v in state["bridge_counts"].items()})

    # ------------------------------------------------------------------
    # Internal: constraint validation helpers
    # ------------------------------------------------------------------

    def _is_locked(self, worker_id: str, date: datetime) -> bool:
        """True si la asignación está bloqueada (mandatory)."""
        if self.schedule_builder is None:
            return False
        return (worker_id, date) in self.schedule_builder._locked_mandatory

    def _is_mandatory(self, worker_id: str, date: datetime) -> bool:
        """True si la asignación es un turno obligatorio (config mandatory)."""
        if self.schedule_builder is None:
            return False
        return self.schedule_builder._is_mandatory(worker_id, date)

    def _can_swap_away(self, worker_id: str, date: datetime) -> bool:
        """
        True si una asignación puede cederse en un intercambio.

        A diferencia de _can_modify, NO protege el objetivo mensual porque en
        un intercambio pareado el total de turnos del mes no cambia.
        Solo bloquea asignaciones verdaderamente inmovibles (locked o config mandatory).
        """
        if self._is_locked(worker_id, date):
            return False
        if self._is_mandatory(worker_id, date):
            return False
        return True

    def _can_take_in_swap(
        self,
        worker_id: str,
        date_gain: datetime,
        post_gain: int,
        date_lose: datetime | None,
    ) -> bool:
        """
        True si el trabajador puede recibir el turno (date_gain, post_gain) como
        parte de un intercambio en el que simultáneamente cede date_lose.

        Args:
            worker_id:  ID del trabajador que recibe el nuevo turno.
            date_gain:  Fecha en la que el trabajador pasará a trabajar.
            post_gain:  Puesto (posición de turno) asignado en date_gain.
            date_lose:  Fecha que el trabajador cede simultáneamente (None si no cede ninguna).

        A diferencia de _can_take_shift / _calculate_worker_score, esta función
        NO verifica límites de objetivo ni tolerancia (el total de turnos no cambia
        en un intercambio pareado). Sólo valida:
          1. Disponibilidad del trabajador en date_gain (días libres, periodos de trabajo).
          2. Restricciones no_last_post / only_last_post.
          3. Incompatibilidades con otros trabajadores asignados ese mismo día.
          4. Restricción de gap mínimo (usando asignaciones simuladas: sin date_lose,
             con date_gain aún no incluida para que la comprobación sea correcta).
        """
        sb = self.schedule_builder

        # Without schedule_builder, allow all swaps
        if sb is None:
            return True

        # 1. Basic availability
        if sb._is_worker_unavailable(worker_id, date_gain):
            return False

        # 2. Post constraints
        worker_config = next((w for w in self.workers_data if w["id"] == worker_id), None)
        if worker_config:
            num_shifts = getattr(sb, "num_shifts", None)
            if num_shifts is not None:
                is_last_post = post_gain == num_shifts - 1
                if is_last_post and worker_config.get("no_last_post", False):
                    return False
                if not is_last_post and worker_config.get("only_last_post", False):
                    return False

        # 3. Incompatibility: check against workers already on date_gain (excluding the
        #    slot being freed, if any, to avoid false conflicts with the outgoing worker)
        others_on_date = [
            w
            for idx, w in enumerate(self.schedule.get(date_gain, []))
            if w is not None and w != worker_id and idx != post_gain
        ]
        if not sb._check_incompatibility_with_list(worker_id, others_on_date):
            return False

        # 4. Gap constraint — simulate the post-swap assignment set:
        #    remove date_lose (the date this worker gives up), do NOT yet add date_gain
        #    (_check_gap_constraint_simulated checks date_gain against the remaining dates).
        current_dates = self.worker_assignments.get(worker_id, set())
        simulated_worker_dates = current_dates - {date_lose} if date_lose is not None else current_dates
        # _check_gap_constraint_simulated only reads simulated[worker_id]; a single-entry
        # dict is sufficient and avoids copying the entire worker_assignments mapping.
        if not sb._check_gap_constraint_simulated(worker_id, date_gain, {worker_id: simulated_worker_dates}):
            return False

        return True

    # ------------------------------------------------------------------
    # Internal: shift balance
    # ------------------------------------------------------------------

    def _run_shift_balance(self, max_iters: int) -> None:
        """Delega el balance de turnos al StrictBalanceOptimizer."""
        if self.schedule_builder is None:
            logging.warning("FinalAdjustmentEngine: schedule_builder not available, skipping shift balance")
            return
        try:
            from saldo27.strict_balance_optimizer import StrictBalanceOptimizer

            sbo = StrictBalanceOptimizer(self.scheduler, self.schedule_builder)
            sbo.optimize_balance(max_iterations=max_iters, target_tolerance=1)
            self.stats["shift_swaps"] = sbo.stats["swaps_performed"]
        except Exception as exc:
            logging.error(f"FinalAdjustmentEngine shift balance error: {exc}", exc_info=True)

    # ------------------------------------------------------------------
    # Internal: weekend balance
    # ------------------------------------------------------------------

    def _sorted_by_weekend_deviation(self) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
        """
        Devuelve (over_weekend, under_weekend) ordenados por desviación.
        Sólo incluye trabajadores con target > 0 y desviación != 0.
        """
        over: list[tuple[str, int]] = []
        under: list[tuple[str, int]] = []

        for worker in self.workers_data:
            wid = worker["id"]
            raw_target = self._raw_targets.get(wid, 0)
            if raw_target == 0:
                continue
            wknd_target = self._weekend_target_for(raw_target)
            wknd_assigned = sum(
                1
                for d in self.worker_assignments.get(wid, set())
                if self.scheduler.date_utils.is_weekend_day(d, self.holidays_set)
            )
            dev = wknd_assigned - wknd_target
            if dev > 0:
                over.append((wid, dev))
            elif dev < 0:
                under.append((wid, dev))

        over.sort(key=lambda x: x[1], reverse=True)
        under.sort(key=lambda x: abs(x[1]), reverse=True)
        return over, under

    def _improve_weekend_balance(self) -> bool:
        """
        Intenta un intercambio pareado que mejore el balance de weekends.

        A (excess weekends) cede date_wknd → B (deficit weekends).
        B cede date_wkday → A.
        Net: shift counts unchanged; A -1 weekend, B +1 weekend.
        """
        over, under = self._sorted_by_weekend_deviation()
        if not over or not under:
            return False

        for over_id, over_dev in over[:5]:
            # Weekend dates of A that are not locked/mandatory
            over_wknd_dates = [
                d for d in sorted(self.scheduler.worker_weekends.get(over_id, [])) if self._can_swap_away(over_id, d)
            ]

            for under_id, under_dev in under[:5]:
                if over_id == under_id:
                    continue

                # Weekday dates of B (non-weekend, non-bridge) that are not locked/mandatory
                under_wkday_dates = [
                    d
                    for d in sorted(self.worker_assignments.get(under_id, set()))
                    if not self.scheduler.date_utils.is_weekend_day(d, self.holidays_set)
                    and not self.scheduler.date_utils.is_bridge_day(d, self.scheduler.bridge_periods)
                    and self._can_swap_away(under_id, d)
                ]

                if not over_wknd_dates or not under_wkday_dates:
                    continue

                for date_wknd in over_wknd_dates:
                    try:
                        post_w = self.schedule[date_wknd].index(over_id)
                    except (ValueError, KeyError):
                        continue

                    # B takes A's weekend slot; B simultaneously loses date_wkday
                    # (checked below when we know which date_wkday we're using)
                    for date_wkday in under_wkday_dates:
                        if date_wkday == date_wknd:
                            continue
                        try:
                            post_d = self.schedule[date_wkday].index(under_id)
                        except (ValueError, KeyError):
                            continue

                        # Swap-aware constraint check (no target/tolerance gate)
                        if not self._can_take_in_swap(under_id, date_wknd, post_w, date_wkday):
                            continue
                        if not self._can_take_in_swap(over_id, date_wkday, post_d, date_wknd):
                            continue

                        # --- Attempt paired swap ---
                        state = self._save_state()

                        # A loses date_wknd, gains date_wkday
                        self.schedule[date_wknd][post_w] = under_id
                        self.worker_assignments[over_id].discard(date_wknd)
                        self.worker_assignments.setdefault(under_id, set()).add(date_wknd)
                        self.scheduler._update_tracking_data(over_id, date_wknd, post_w, removing=True)
                        self.scheduler._update_tracking_data(under_id, date_wknd, post_w, removing=False)

                        self.schedule[date_wkday][post_d] = over_id
                        self.worker_assignments[under_id].discard(date_wkday)
                        self.worker_assignments.setdefault(over_id, set()).add(date_wkday)
                        self.scheduler._update_tracking_data(under_id, date_wkday, post_d, removing=True)
                        self.scheduler._update_tracking_data(over_id, date_wkday, post_d, removing=False)

                        # Evaluate: did weekend deviations improve?
                        new_over_wknd = self.scheduler.worker_weekend_counts.get(over_id, 0)
                        new_under_wknd = self.scheduler.worker_weekend_counts.get(under_id, 0)
                        new_over_dev = new_over_wknd - self._weekend_target_for(self._raw_targets.get(over_id, 0))
                        new_under_dev = new_under_wknd - self._weekend_target_for(self._raw_targets.get(under_id, 0))

                        improved = abs(new_over_dev) < abs(over_dev) and abs(new_under_dev) < abs(under_dev)
                        if not improved:
                            self._restore_state(state)
                            continue

                        logging.info(
                            f"  ✅ Weekend swap: {over_id} ↔ {under_id} | "
                            f"{date_wknd.strftime('%Y-%m-%d')} (wknd) ↔ "
                            f"{date_wkday.strftime('%Y-%m-%d')} (wkday)"
                        )
                        return True

        return False

    # ------------------------------------------------------------------
    # Internal: bridge balance
    # ------------------------------------------------------------------

    def _sorted_by_bridge_deviation(self) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
        """Devuelve (over_bridge, under_bridge) ordenados por desviación."""
        over: list[tuple[str, int]] = []
        under: list[tuple[str, int]] = []

        for worker in self.workers_data:
            wid = worker["id"]
            raw_target = self._raw_targets.get(wid, 0)
            if raw_target == 0 or self._bridge_target_for(raw_target) == 0:
                continue
            bridge_target = self._bridge_target_for(raw_target)
            bridge_assigned = len(self.scheduler.worker_bridge_counts.get(wid, set()))
            dev = bridge_assigned - bridge_target
            if dev > 0:
                over.append((wid, dev))
            elif dev < 0:
                under.append((wid, dev))

        over.sort(key=lambda x: x[1], reverse=True)
        under.sort(key=lambda x: abs(x[1]), reverse=True)
        return over, under

    def _improve_bridge_balance(self) -> bool:
        """
        Intenta un intercambio pareado que mejore el balance de puentes.

        A (excess bridges) cede date_bridge → B (deficit bridges).
        B cede date_nonbridge → A.
        Net: shift counts unchanged; A -1 bridge, B +1 bridge.
        """
        if self._total_bridge_slots == 0:
            return False  # No bridge periods in this schedule

        over, under = self._sorted_by_bridge_deviation()
        if not over or not under:
            return False

        for over_id, over_dev in over[:5]:
            # Bridge dates of A that are not locked/mandatory
            over_bridge_dates = [
                d
                for d in sorted(self.worker_assignments.get(over_id, set()))
                if self.scheduler.date_utils.is_bridge_day(d, self.scheduler.bridge_periods)
                and self._can_swap_away(over_id, d)
            ]

            for under_id, under_dev in under[:5]:
                if over_id == under_id:
                    continue

                # Non-bridge dates of B that are not locked/mandatory
                under_nonbridge_dates = [
                    d
                    for d in sorted(self.worker_assignments.get(under_id, set()))
                    if not self.scheduler.date_utils.is_bridge_day(d, self.scheduler.bridge_periods)
                    and self._can_swap_away(under_id, d)
                ]

                if not over_bridge_dates or not under_nonbridge_dates:
                    continue

                for date_bridge in over_bridge_dates:
                    try:
                        post_b = self.schedule[date_bridge].index(over_id)
                    except (ValueError, KeyError):
                        continue

                    for date_nonbridge in under_nonbridge_dates:
                        if date_nonbridge == date_bridge:
                            continue
                        try:
                            post_n = self.schedule[date_nonbridge].index(under_id)
                        except (ValueError, KeyError):
                            continue

                        # Swap-aware constraint check (no target/tolerance gate)
                        if not self._can_take_in_swap(under_id, date_bridge, post_b, date_nonbridge):
                            continue
                        if not self._can_take_in_swap(over_id, date_nonbridge, post_n, date_bridge):
                            continue

                        # --- Attempt paired swap ---
                        state = self._save_state()

                        # A loses date_bridge, gains date_nonbridge
                        self.schedule[date_bridge][post_b] = under_id
                        self.worker_assignments[over_id].discard(date_bridge)
                        self.worker_assignments.setdefault(under_id, set()).add(date_bridge)
                        self.scheduler._update_tracking_data(over_id, date_bridge, post_b, removing=True)
                        self.scheduler._update_tracking_data(under_id, date_bridge, post_b, removing=False)

                        self.schedule[date_nonbridge][post_n] = over_id
                        self.worker_assignments[under_id].discard(date_nonbridge)
                        self.worker_assignments.setdefault(over_id, set()).add(date_nonbridge)
                        self.scheduler._update_tracking_data(under_id, date_nonbridge, post_n, removing=True)
                        self.scheduler._update_tracking_data(over_id, date_nonbridge, post_n, removing=False)

                        # Evaluate: did bridge deviations improve?
                        new_over_bridge = len(self.scheduler.worker_bridge_counts.get(over_id, set()))
                        new_under_bridge = len(self.scheduler.worker_bridge_counts.get(under_id, set()))
                        new_over_dev = new_over_bridge - self._bridge_target_for(self._raw_targets.get(over_id, 0))
                        new_under_dev = new_under_bridge - self._bridge_target_for(self._raw_targets.get(under_id, 0))

                        improved = abs(new_over_dev) < abs(over_dev) and abs(new_under_dev) < abs(under_dev)
                        if not improved:
                            self._restore_state(state)
                            continue

                        logging.info(
                            f"  ✅ Bridge swap: {over_id} ↔ {under_id} | "
                            f"{date_bridge.strftime('%Y-%m-%d')} (bridge) ↔ "
                            f"{date_nonbridge.strftime('%Y-%m-%d')} (non-bridge)"
                        )
                        return True

        return False

    # ------------------------------------------------------------------
    # Internal: OR-Tools CP-SAT phase
    # ------------------------------------------------------------------

    def _run_ortools_phase(self, time_limit_seconds: int = 30) -> int:
        """
        Ejecuta la Fase 4 de refinamiento CP-SAT con OR-Tools.

        Si OR-Tools no está disponible se omite sin error.  Si el solver
        no mejora las métricas actuales se descarta la solución y se
        restaura el estado previo.

        Returns:
            Número de slots reasignados por el solver (0 si no aplica).
        """
        try:
            from ortools.sat.python import cp_model
        except ImportError:
            logging.warning("FinalAdjustmentEngine: ortools no disponible, se omite la Fase 4 CP-SAT.")
            return 0

        state_before = self._save_state()
        metrics_before = self.compute_metrics()

        try:
            phase = ORToolsPhase(self)
            result = phase.solve(time_limit_seconds=time_limit_seconds)
        except Exception as exc:
            logging.error(f"FinalAdjustmentEngine OR-Tools error: {exc}", exc_info=True)
            self._restore_state(state_before)
            return 0

        if result is None:
            # No feasible/improved solution found
            return 0

        # Apply the solution: iterate over slots that changed
        changes = 0
        for (date, post), (old_worker, new_worker) in result.items():
            if old_worker == new_worker:
                continue
            self.schedule[date][post] = new_worker
            if old_worker is not None:
                self.worker_assignments.setdefault(old_worker, set()).discard(date)
                self.scheduler._update_tracking_data(old_worker, date, post, removing=True)
            if new_worker is not None:
                self.worker_assignments.setdefault(new_worker, set()).add(date)
                self.scheduler._update_tracking_data(new_worker, date, post, removing=False)
            changes += 1

        # Verify improvement: total weighted deviation must not increase
        metrics_after = self.compute_metrics()
        score_before = _deviation_score(metrics_before)
        score_after = _deviation_score(metrics_after)

        if score_after > score_before:
            logging.info(
                f"  OR-Tools solution did not improve metrics ({score_after} > {score_before}), reverting."
            )
            self._restore_state(state_before)
            return 0

        if changes:
            logging.info(
                f"  ✅ OR-Tools CP-SAT: {changes} reasignación(es), score {score_before} → {score_after}"
            )
        return changes


def _deviation_score(metrics: dict[str, dict[str, Any]]) -> int:
    """Weighted total deviation used to compare before/after OR-Tools."""
    total = 0
    for m in metrics.values():
        total += 1000 * abs(m.get("shift_deviation", 0))
        total += 100 * abs(m.get("weekend_deviation", 0))
        total += 10 * abs(m.get("bridge_deviation", 0))
    return total


class ORToolsPhase:
    """
    Modelado CP-SAT con Google OR-Tools para refinar el schedule.

    El modelo toma los slots *ya ocupados* como dominio fijo: no crea ni
    elimina slots, sólo decide qué worker cubre cada uno.  Se parte de la
    solución greedy como warm-start y se busca minimizar la desviación
    ponderada (turnos > weekends > puentes).
    """

    # Pesos del objetivo multi-dimensión
    W_SHIFT = 1000
    W_WEEKEND = 100
    W_BRIDGE = 10

    def __init__(self, engine: FinalAdjustmentEngine) -> None:
        self.engine = engine
        self.scheduler = engine.scheduler
        self.workers_data = engine.workers_data
        self.schedule = engine.schedule
        self.worker_assignments = engine.worker_assignments
        self.holidays_set = engine.holidays_set

    def solve(self, time_limit_seconds: int = 30) -> dict | None:
        """
        Construye y resuelve el modelo CP-SAT.

        Returns:
            Dict ``{(date, post): (old_worker, new_worker)}`` con los cambios
            propuestos, o ``None`` si no se encontró solución factible o no se
            produjo mejora.
        """
        from datetime import timedelta

        from ortools.sat.python import cp_model

        from saldo27.utilities import get_effective_min_gap

        model = cp_model.CpModel()

        # -------------------------------------------------------------------
        # 1. Collect occupied slots: [(date, post, current_worker)]
        # -------------------------------------------------------------------
        slots: list[tuple] = []
        for date, slot_list in sorted(self.schedule.items()):
            for post, worker in enumerate(slot_list):
                if worker is not None:
                    slots.append((date, post, worker))

        if not slots:
            return None

        worker_ids = [w["id"] for w in self.workers_data]
        worker_data_by_id = {w["id"]: w for w in self.workers_data}
        n_workers = len(worker_ids)
        n_slots = len(slots)

        # -------------------------------------------------------------------
        # 2. Decision variables: x[w_idx, s_idx] ∈ {0, 1}
        # -------------------------------------------------------------------
        x: dict[tuple[int, int], Any] = {}
        for wi in range(n_workers):
            for si in range(n_slots):
                x[wi, si] = model.new_bool_var(f"x_{wi}_{si}")

        # -------------------------------------------------------------------
        # 3. Hard constraints
        # -------------------------------------------------------------------

        # 3a. Each slot has exactly one worker
        for si in range(n_slots):
            model.add_exactly_one(x[wi, si] for wi in range(n_workers))

        # 3b. Each worker assigned at most once per date
        date_to_slots: dict = {}
        for si, (date, post, _) in enumerate(slots):
            date_to_slots.setdefault(date, []).append(si)

        for wi in range(n_workers):
            for date, slot_indices in date_to_slots.items():
                if len(slot_indices) > 1:
                    model.add(sum(x[wi, si] for si in slot_indices) <= 1)

        # 3c. Mandatory / locked assignments are fixed
        for wi, wid in enumerate(worker_ids):
            for si, (date, post, current_worker) in enumerate(slots):
                if self.engine._is_locked(wid, date) or self.engine._is_mandatory(wid, date):
                    model.add(x[wi, si] == (1 if current_worker == wid else 0))

        # 3d. Worker unavailability (days_off / work_periods)
        sb = self.engine.schedule_builder
        if sb is not None:
            for wi, wid in enumerate(worker_ids):
                for si, (date, post, _) in enumerate(slots):
                    if sb._is_worker_unavailable(wid, date):
                        model.add(x[wi, si] == 0)

        # 3e. Post constraints (no_last_post / only_last_post)
        num_shifts = self.scheduler.num_shifts
        for wi, wid in enumerate(worker_ids):
            wd = worker_data_by_id[wid]
            for si, (date, post, _) in enumerate(slots):
                is_last = post == num_shifts - 1
                if is_last and wd.get("no_last_post", False):
                    model.add(x[wi, si] == 0)
                if not is_last and wd.get("only_last_post", False):
                    model.add(x[wi, si] == 0)

        # 3f. Incompatibilities: incompatible pair cannot share the same date
        incompat_pairs: set[tuple[str, str]] = set()
        for wd in self.workers_data:
            wid = wd["id"]
            for other in wd.get("incompatible_with", []):
                pair = (min(wid, other), max(wid, other))
                incompat_pairs.add(pair)

        worker_idx = {wid: wi for wi, wid in enumerate(worker_ids)}
        for a_id, b_id in incompat_pairs:
            if a_id not in worker_idx or b_id not in worker_idx:
                continue
            a_wi = worker_idx[a_id]
            b_wi = worker_idx[b_id]
            for date, slot_indices in date_to_slots.items():
                a_vars = [x[a_wi, si] for si in slot_indices]
                b_vars = [x[b_wi, si] for si in slot_indices]
                model.add(sum(a_vars) + sum(b_vars) <= 1)

        # 3g. Gap and 7/14-day pattern constraints
        gap = self.scheduler.gap_between_shifts
        for wi, wid in enumerate(worker_ids):
            wd = worker_data_by_id[wid]
            min_gap = get_effective_min_gap(wd, gap)
            # slots is already sorted by date (step 1 iterates sorted(self.schedule.items()))
            for idx_a in range(n_slots):
                date_a = slots[idx_a][0]
                for idx_b in range(idx_a + 1, n_slots):
                    date_b = slots[idx_b][0]
                    delta = (date_b - date_a).days  # positive because sorted
                    # Break once slots are beyond both the gap window and the 7/14-day window
                    if delta > 14 and delta >= min_gap:
                        break
                    if 0 < delta < min_gap or (delta in (7, 14) and date_a.weekday() == date_b.weekday()):
                        model.add(x[wi, idx_a] + x[wi, idx_b] <= 1)

            # Enforce gap against prior-period assignments
            cutoff = self.scheduler.start_date - timedelta(days=90)
            prior_dates = sorted(
                d
                for d in getattr(self.scheduler, "prior_assignments", {}).get(wid, set())
                if cutoff <= d < self.scheduler.start_date
            )
            for prior_date in prior_dates:
                for si in range(n_slots):
                    date_s = slots[si][0]
                    delta = abs((date_s - prior_date).days)
                    if delta == 0:
                        continue
                    if delta < min_gap or (delta in (7, 14) and date_s.weekday() == prior_date.weekday()):
                        model.add(x[wi, si] == 0)

        # 3h. Max shifts per worker
        max_shifts = getattr(self.scheduler, "max_shifts_per_worker", None)
        if max_shifts:
            for wi in range(n_workers):
                model.add(sum(x[wi, si] for si in range(n_slots)) <= max_shifts)

        # -------------------------------------------------------------------
        # 4. Objective: minimize weighted deviations
        # -------------------------------------------------------------------
        raw_targets = self.engine._raw_targets

        slot_is_weekend = [
            self.scheduler.date_utils.is_weekend_day(slots[si][0], self.holidays_set)
            for si in range(n_slots)
        ]
        slot_is_bridge = [
            self.scheduler.date_utils.is_bridge_day(slots[si][0], self.scheduler.bridge_periods)
            for si in range(n_slots)
        ]

        obj_terms = []
        for wi, wid in enumerate(worker_ids):
            raw_tgt = raw_targets.get(wid, 0)

            # Shift deviation
            actual_shifts = sum(x[wi, si] for si in range(n_slots))
            dplus_s = model.new_int_var(0, n_slots, f"dps_{wi}")
            dminus_s = model.new_int_var(0, n_slots, f"dms_{wi}")
            model.add(actual_shifts - raw_tgt == dplus_s - dminus_s)
            obj_terms.append(self.W_SHIFT * (dplus_s + dminus_s))

            # Weekend deviation
            wknd_tgt = self.engine._weekend_target_for(raw_tgt)
            actual_wknd = sum(x[wi, si] for si in range(n_slots) if slot_is_weekend[si])
            dplus_w = model.new_int_var(0, n_slots, f"dpw_{wi}")
            dminus_w = model.new_int_var(0, n_slots, f"dmw_{wi}")
            model.add(actual_wknd - wknd_tgt == dplus_w - dminus_w)
            obj_terms.append(self.W_WEEKEND * (dplus_w + dminus_w))

            # Bridge deviation
            bridge_tgt = self.engine._bridge_target_for(raw_tgt)
            actual_bridge = sum(x[wi, si] for si in range(n_slots) if slot_is_bridge[si])
            dplus_b = model.new_int_var(0, n_slots, f"dpb_{wi}")
            dminus_b = model.new_int_var(0, n_slots, f"dmb_{wi}")
            model.add(actual_bridge - bridge_tgt == dplus_b - dminus_b)
            obj_terms.append(self.W_BRIDGE * (dplus_b + dminus_b))

        model.minimize(sum(obj_terms))

        # -------------------------------------------------------------------
        # 5. Warm-start hint from current schedule
        # -------------------------------------------------------------------
        for wi, wid in enumerate(worker_ids):
            for si, (date, post, current_worker) in enumerate(slots):
                model.add_hint(x[wi, si], 1 if current_worker == wid else 0)

        # -------------------------------------------------------------------
        # 6. Solve
        # -------------------------------------------------------------------
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = time_limit_seconds
        solver.parameters.num_workers = 4
        solver.parameters.log_search_progress = False

        status = solver.solve(model)

        if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            logging.info("  OR-Tools CP-SAT: no feasible solution found within time limit.")
            return None

        # -------------------------------------------------------------------
        # 7. Extract changes: {(date, post): (old_worker, new_worker)}
        # -------------------------------------------------------------------
        changes: dict[tuple, tuple] = {}
        for si, (date, post, old_worker) in enumerate(slots):
            new_worker = None
            for wi, wid in enumerate(worker_ids):
                if solver.value(x[wi, si]) == 1:
                    new_worker = wid
                    break
            if new_worker != old_worker:
                changes[(date, post)] = (old_worker, new_worker)

        return changes if changes else None
