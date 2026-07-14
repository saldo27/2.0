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
        self._raw_targets: dict[str, int] = {
            w["id"]: (
                w["_raw_target"] if w.get("_raw_target") is not None else w.get("target_shifts", 0)
            )
            for w in self.workers_data
        }

        # Counters for reporting
        self.stats: dict[str, int] = {
            "shift_swaps": 0,
            "weekend_swaps": 0,
            "bridge_swaps": 0,
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

        after = self.compute_metrics()

        logging.info("=" * 70)
        logging.info("FINAL ADJUSTMENT ENGINE - Results")
        logging.info(f"  Shift swaps:   {self.stats['shift_swaps']}")
        logging.info(f"  Weekend swaps: {self.stats['weekend_swaps']}")
        logging.info(f"  Bridge swaps:  {self.stats['bridge_swaps']}")
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
                1
                for d in assigned_dates
                if self.scheduler.date_utils.is_weekend_day(d, self.holidays_set)
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
        self.scheduler.worker_bridge_counts.update(
            {k: set(v) for k, v in state["bridge_counts"].items()}
        )

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
        simulated_worker_dates = (
            current_dates - {date_lose} if date_lose is not None else current_dates
        )
        # _check_gap_constraint_simulated only reads simulated[worker_id]; a single-entry
        # dict is sufficient and avoids copying the entire worker_assignments mapping.
        if not sb._check_gap_constraint_simulated(
            worker_id, date_gain, {worker_id: simulated_worker_dates}
        ):
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
                d
                for d in sorted(self.scheduler.worker_weekends.get(over_id, []))
                if self._can_swap_away(over_id, d)
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
