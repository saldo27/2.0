"""
target_calculator.py
====================
Responsible for computing per-worker shift targets (monthly and total).

Extracted from the ``Scheduler`` God Object so that target-calculation logic
is testable in isolation and does not pollute the main scheduling orchestrator.

Public entry point
------------------
``TargetCalculator(scheduler).calculate()`` — mirrors the old
``Scheduler._calculate_target_shifts()`` call and returns True/False.
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Any


class TargetCalculator:
    """
    Computes ``target_shifts``, ``_raw_target``, ``_mandatory_count``,
    ``monthly_targets`` and ``monthly_targets_ceil`` for every worker in
    ``scheduler.workers_data``.

    All data is written back to the worker dicts in-place, exactly as the
    original ``Scheduler`` methods did.
    """

    def __init__(self, scheduler: Any) -> None:
        self.scheduler = scheduler

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def calculate(self) -> bool:
        """
        Recalculate each worker's target_shifts by:
          1) For workers with auto_calculate_shifts=False (manual):
             - Use target_shifts as "guardias/mes" and multiply by number of months in period
             - Calculated FIRST so their slots are reserved before auto distribution

          2) For workers with auto_calculate_shifts=True:
             - Count slots they can work (based on work_periods & days_off)
             - Weight those slots by their work_percentage
             - Allocate REMAINING slots (total - manual) proportionally
               (largest-remainder rounding)
        """
        s = self.scheduler
        try:
            logging.info("Calculating target shifts based on availability and percentage")

            cadence_workers = [w for w in s.workers_data if w.get("has_cadence")]
            auto_calc_workers = [
                w for w in s.workers_data if w.get("auto_calculate_shifts", True) and not w.get("has_cadence")
            ]
            manual_workers = [
                w for w in s.workers_data if not w.get("auto_calculate_shifts", True) and not w.get("has_cadence")
            ]

            # 0) CADENCE WORKERS FIRST — their shifts are fixed by the cadence
            #    pattern (see scheduler_initializer._apply_cadence_assignments)
            #    and must be excluded entirely from the pool distributed among
            #    the rest of the workers.
            cadence_slots_reserved = 0
            if cadence_workers:
                cadence_slots_reserved = self._calculate_cadence_targets(cadence_workers)

            # 1) MANUAL CALCULATION — reserve their slots before auto distribution
            manual_slots_reserved = 0
            if manual_workers:
                manual_slots_reserved = self._calculate_manual_targets(manual_workers)

            # 2) AUTOMATIC CALCULATION for auto_calc_workers (using remaining slots)
            if auto_calc_workers:
                total_slots = sum(len(slots) for slots in s.schedule.values())
                if total_slots <= 0:
                    logging.warning("No slots in schedule; skipping allocation")
                else:
                    slots_for_auto = total_slots - manual_slots_reserved - cadence_slots_reserved
                    if slots_for_auto < 0:
                        logging.error(
                            f"Manual+cadence targets ({manual_slots_reserved + cadence_slots_reserved}) "
                            f"exceed total slots ({total_slots})! Clamping to 0 for auto workers."
                        )
                        slots_for_auto = 0
                    elif manual_slots_reserved > 0 or cadence_slots_reserved > 0:
                        logging.info(
                            f"Reserving {manual_slots_reserved} slots for manual workers and "
                            f"{cadence_slots_reserved} slots for cadence workers. "
                            f"Auto workers share {slots_for_auto}/{total_slots} slots."
                        )

                    # Compute available_slots per worker
                    available_slots: dict[str, int] = {}
                    for w in auto_calc_workers:
                        wid = w["id"]
                        wp = w.get("work_periods", "").strip()
                        dp = w.get("days_off", "").strip()
                        work_ranges = s.date_utils.parse_date_ranges(wp) if wp else [(s.start_date, s.end_date)]
                        off_ranges = s.date_utils.parse_date_ranges(dp) if dp else []
                        count = 0
                        for date, slots in s.schedule.items():
                            in_work = any(rs <= date <= re for rs, re in work_ranges)
                            in_off = any(rs <= date <= re for rs, re in off_ranges)
                            if in_work and not in_off:
                                count += len(slots)
                        available_slots[wid] = count
                        logging.debug(f"Worker {wid}: available_slots={count}")

                    # Weight = available_slots * (work_percentage / 100)
                    weights = []
                    for w in auto_calc_workers:
                        wid = w["id"]
                        pct = 1.0
                        try:
                            pct = float(str(w.get("work_percentage", 100)).strip()) / 100.0
                        except (TypeError, ValueError):
                            logging.warning(f"Worker {wid} invalid work_percentage; defaulting to 100%")
                        pct = max(0.0, pct)
                        weights.append(available_slots.get(wid, 0) * pct)

                    total_weight = sum(weights) or 1.0

                    exact_targets = [wgt / total_weight * slots_for_auto for wgt in weights]

                    # Largest-remainder rounding
                    floors = [int(x) for x in exact_targets]
                    remainder = int(slots_for_auto - sum(floors))
                    fracs = sorted(
                        enumerate(exact_targets),
                        key=lambda ix: exact_targets[ix[0]] - floors[ix[0]],
                        reverse=True,
                    )
                    targets = floors[:]
                    for idx, _ in fracs[:remainder]:
                        targets[idx] += 1

                    for i, w in enumerate(auto_calc_workers):
                        raw_target = targets[i]
                        mand_count = 0
                        mand_str = w.get("mandatory_days", "").strip()
                        if mand_str:
                            try:
                                mand_dates = s.date_utils.parse_dates(mand_str)
                                mand_count = sum(1 for d in mand_dates if s.start_date <= d <= s.end_date)
                            except Exception as e:
                                logging.error(f"Failed to parse mandatory_days for {w['id']}: {e}")
                        adjusted = max(0, raw_target - mand_count)
                        w["target_shifts"] = adjusted
                        w["_raw_target"] = raw_target
                        w["_mandatory_count"] = mand_count

                        if mand_count > 0:
                            logging.info(
                                f"Worker {w['id']} (AUTO): RAW target={raw_target}, "
                                f"Mandatory={mand_count}, Adjusted target_shifts={adjusted}"
                            )
                        else:
                            logging.info(f"Worker {w['id']} (AUTO): target_shifts={adjusted}")

            # Once all targets are set, compute work_periods-aware monthly targets
            self._calculate_monthly_targets()
            return True

        except Exception as e:
            logging.error(f"Error calculating target shifts: {e}")
            return False

    # ------------------------------------------------------------------
    # Cadence ("cadencia") targets
    # ------------------------------------------------------------------

    def _calculate_cadence_targets(self, cadence_workers: list) -> int:
        """
        Calculate targets for fixed-cadence workers.

        Their ``mandatory_days`` was already overwritten with the computed
        cadence dates (see ``SchedulerInitializer._apply_cadence_assignments``),
        so all of their assigned shifts are mandatory. ``target_shifts`` (the
        non-mandatory budget used by the assignment algorithms to hand out
        *additional* shifts) is therefore always 0 — a cadence worker must
        never receive more or fewer shifts than dictated by their cadence.

        Returns:
            Total number of cadence-mandated slots reserved (to subtract from
            the pool distributed among the rest of the workers).
        """
        s = self.scheduler
        total_cadence_slots = 0
        for w in cadence_workers:
            wid = w["id"]
            mand_count = 0
            mand_str = w.get("mandatory_days", "").strip()
            if mand_str:
                try:
                    mand_dates = s.date_utils.parse_dates(mand_str)
                    mand_count = sum(1 for d in mand_dates if s.start_date <= d <= s.end_date)
                except Exception as e:
                    logging.error(f"Failed to parse cadence mandatory_days for {wid}: {e}")

            w["target_shifts"] = 0
            w["_raw_target"] = mand_count
            w["_mandatory_count"] = mand_count
            total_cadence_slots += mand_count

            logging.info(f"Worker {wid} (CADENCIA): {mand_count} shift(s) fixed by cadence pattern, target_shifts=0")

        return total_cadence_slots

    # ------------------------------------------------------------------
    # Manual targets
    # ------------------------------------------------------------------

    def _calculate_manual_targets(self, manual_workers: list) -> int:
        """
        Calculate targets for manual workers (guardias/mes) and return the
        total number of slots they need (raw targets including mandatory).
        """
        import calendar

        s = self.scheduler
        proportional_months = 0.0
        cur_year, cur_month = s.start_date.year, s.start_date.month
        end_year, end_month = s.end_date.year, s.end_date.month
        while (cur_year, cur_month) <= (end_year, end_month):
            days_in_month = calendar.monthrange(cur_year, cur_month)[1]
            month_start = datetime(cur_year, cur_month, 1)
            month_end = datetime(cur_year, cur_month, days_in_month)
            effective_start = max(month_start, s.start_date)
            effective_end = min(month_end, s.end_date)
            days_covered = (effective_end - effective_start).days + 1
            fraction = days_covered / days_in_month
            proportional_months += fraction
            logging.debug(
                f"Manual month calc: {cur_year}-{cur_month:02d} → "
                f"{days_covered}/{days_in_month} days = {fraction:.3f} months"
            )
            cur_month += 1
            if cur_month > 12:
                cur_month = 1
                cur_year += 1

        logging.info(f"Manual target calculation: {proportional_months:.2f} proportional months in period")

        total_manual_slots = 0
        for w in manual_workers:
            wid = w["id"]
            if "_original_target_shifts" not in w:
                w["_original_target_shifts"] = w.get("target_shifts", 0)
            guardias_per_mes = w["_original_target_shifts"]

            # Effective availability considers BOTH work_periods (if configured)
            # AND days_off (vacations/permissions) — a manual worker on vacation
            # for half a month must have their guardias/mes prorated down for
            # that month, exactly as if they were outside their work_period.
            month_avail = self._manual_worker_month_availability(w)
            if month_avail:
                worker_months = 0.0
                for month_key, avail_days in month_avail.items():
                    year_m, month_m = int(month_key[:4]), int(month_key[5:])
                    days_in_month = calendar.monthrange(year_m, month_m)[1]
                    worker_months += avail_days / days_in_month
                logging.debug(f"Worker {wid}: work_periods/days_off → {worker_months:.2f} effective months")
            else:
                worker_months = proportional_months

            raw_target = round(guardias_per_mes * worker_months)

            mand_count = 0
            mand_str = w.get("mandatory_days", "").strip()
            if mand_str:
                try:
                    mand_dates = s.date_utils.parse_dates(mand_str)
                    mand_count = sum(1 for d in mand_dates if s.start_date <= d <= s.end_date)
                except Exception as e:
                    logging.error(f"Failed to parse mandatory_days for {wid}: {e}")

            adjusted = max(0, raw_target - mand_count)
            w["target_shifts"] = adjusted
            w["_raw_target"] = raw_target
            w["_mandatory_count"] = mand_count
            total_manual_slots += raw_target

            logging.info(
                f"Worker {wid} (MANUAL): {guardias_per_mes} guardias/mes * {worker_months:.2f} meses = {raw_target}, "
                f"Mandatory={mand_count}, Adjusted target_shifts={adjusted}"
            )

        return total_manual_slots

    def _manual_worker_month_availability(self, worker: dict) -> dict[str, int]:
        """
        Days available to work per month for a manual worker, considering
        BOTH ``work_periods`` (if configured, only days inside these ranges
        count; otherwise the whole schedule period counts) AND ``days_off``
        (vacations/permissions, always subtracted). This lets a manual
        worker's guardias/mes be prorated for a partial month exactly as if
        the vacation days were outside their work_period.

        Returns a ``{"YYYY-MM": available_days}`` dict, or ``{}`` if the
        worker has no schedule-period availability at all restrictions
        cannot be resolved (callers should fall back to full-period targets).
        """
        s = self.scheduler
        import calendar as cal_mod

        work_periods_str = worker.get("work_periods", "").strip()
        work_ranges: list[tuple[datetime, datetime]] = [(s.start_date, s.end_date)]
        if work_periods_str:
            try:
                parsed_ranges = s.date_utils.parse_date_ranges(work_periods_str)
                if parsed_ranges:
                    work_ranges = parsed_ranges
            except (TypeError, ValueError) as exc:
                logging.warning(f"Worker {worker.get('id')} invalid work_periods for availability calc: {exc}")

        days_off_str = worker.get("days_off", "").strip()
        off_ranges: list[tuple[datetime, datetime]] = []
        if days_off_str:
            try:
                off_ranges = s.date_utils.parse_date_ranges(days_off_str)
            except (TypeError, ValueError) as exc:
                logging.warning(f"Worker {worker.get('id')} invalid days_off for availability calc: {exc}")

        month_avail: dict[str, int] = {}
        cur_year, cur_month = s.start_date.year, s.start_date.month
        end_year, end_month = s.end_date.year, s.end_date.month
        while (cur_year, cur_month) <= (end_year, end_month):
            month_key = f"{cur_year}-{cur_month:02d}"
            days_in_month = cal_mod.monthrange(cur_year, cur_month)[1]
            month_start = datetime(cur_year, cur_month, 1)
            month_end = datetime(cur_year, cur_month, days_in_month)
            period_start = max(month_start, s.start_date)
            period_end = min(month_end, s.end_date)

            count = 0
            if period_end >= period_start:
                cur = period_start
                while cur <= period_end:
                    in_work = any(rs <= cur <= re for rs, re in work_ranges)
                    in_off = any(rs <= cur <= re for rs, re in off_ranges)
                    if in_work and not in_off:
                        count += 1
                    cur += timedelta(days=1)
            month_avail[month_key] = count

            cur_month += 1
            if cur_month > 12:
                cur_month = 1
                cur_year += 1

        return month_avail

    # ------------------------------------------------------------------
    # Monthly targets
    # ------------------------------------------------------------------

    def _calculate_monthly_targets(self) -> bool:
        """
        Calculate monthly target shifts for each worker based on their overall
        targets and their individual work_periods availability per month.
        """
        import calendar as cal_mod

        s = self.scheduler
        logging.info("Calculating monthly target distribution...")

        month_days = self._get_schedule_months()

        for worker in s.workers_data:
            worker_id = worker["id"]
            overall_target = worker.get("target_shifts", 0)

            worker["monthly_targets"] = {}
            worker["monthly_targets_ceil"] = {}

            # Always use target_shifts (non-mandatory budget) for monthly proportional
            # distribution — see Scheduler._calculate_monthly_targets docstring.
            overall_target = worker.get("target_shifts", 0)

            work_periods_str = worker.get("work_periods", "").strip()
            if work_periods_str:
                try:
                    work_ranges = s.date_utils.parse_date_ranges(work_periods_str)
                except (TypeError, ValueError) as exc:
                    logging.warning(f"Worker {worker_id} invalid work_periods for monthly target distribution: {exc}")
                    work_ranges = []
            else:
                work_ranges = []

            worker_month_avail: dict[str, int] = {}
            for month_key in month_days:
                year_m, month_m = int(month_key[:4]), int(month_key[5:])
                days_in_month = cal_mod.monthrange(year_m, month_m)[1]
                month_start = datetime(year_m, month_m, 1)
                month_end = datetime(year_m, month_m, days_in_month)

                if work_ranges:
                    avail = 0
                    for rng_start, rng_end in work_ranges:
                        overlap_start = max(month_start, rng_start, s.start_date)
                        overlap_end = min(month_end, rng_end, s.end_date)
                        if overlap_end >= overlap_start:
                            avail += (overlap_end - overlap_start).days + 1
                else:
                    overlap_start = max(month_start, s.start_date)
                    overlap_end = min(month_end, s.end_date)
                    avail = max(0, (overlap_end - overlap_start).days + 1)

                worker_month_avail[month_key] = avail

            # MANUAL WORKERS: never distribute the overall total proportionally
            # by available days (days-in-month varies 28-31, so a days-based
            # split of e.g. 6 shifts over 2 months could yield 4/2 instead of
            # the required 3/3). Manual workers have an explicit guardias/mes
            # figure (_original_target_shifts) that must be honoured verbatim
            # for every fully-available month, with zero tolerance.
            is_manual = not worker.get("auto_calculate_shifts", True)
            if is_manual:
                guardias_mes = worker.get("_original_target_shifts", 0)
                # Availability here considers BOTH work_periods and days_off
                # (vacations/permissions) so a manual worker on vacation for
                # part of a month gets their guardias/mes prorated down for
                # that month instead of the full quota.
                manual_month_avail = self._manual_worker_month_availability(worker)
                if guardias_mes > 0 and any(manual_month_avail.values()):
                    mand_str = worker.get("mandatory_days", "").strip()
                    mand_dates_by_month: dict[str, int] = {}
                    if mand_str:
                        try:
                            for d in s.date_utils.parse_dates(mand_str):
                                if s.start_date <= d <= s.end_date:
                                    mk = f"{d.year}-{d.month:02d}"
                                    mand_dates_by_month[mk] = mand_dates_by_month.get(mk, 0) + 1
                        except (TypeError, ValueError) as exc:
                            logging.debug(f"Worker {worker_id} error parsing mandatory_days for monthly split: {exc}")

                    remaining_target = overall_target
                    fully_available_months = []
                    for month_key in month_days:
                        avail = manual_month_avail.get(month_key, 0)
                        year_m, month_m = int(month_key[:4]), int(month_key[5:])
                        days_in_month = cal_mod.monthrange(year_m, month_m)[1]

                        if avail == 0:
                            worker["monthly_targets"][month_key] = 0
                            worker["monthly_targets_ceil"][month_key] = 0
                            continue

                        month_fraction = min(1.0, avail / days_in_month)
                        raw_month_target = round(guardias_mes * month_fraction)
                        mand_in_month = mand_dates_by_month.get(month_key, 0)
                        month_target = max(0, raw_month_target - mand_in_month)
                        month_target = min(month_target, remaining_target)
                        worker["monthly_targets"][month_key] = month_target
                        worker["monthly_targets_ceil"][month_key] = math.ceil(guardias_mes * month_fraction)
                        remaining_target -= month_target
                        if month_fraction >= 0.999:
                            fully_available_months.append(month_key)

                    # Reconcile rounding drift against the overall (mandatory-adjusted)
                    # total, preferring fully-available months so partial months
                    # (period edges) keep their smaller, proportionally-correct share.
                    if remaining_target > 0:
                        candidates = fully_available_months or [k for k, v in manual_month_avail.items() if v > 0]
                        for month_key in candidates:
                            if remaining_target <= 0:
                                break
                            worker["monthly_targets"][month_key] += 1
                            remaining_target -= 1
                    elif remaining_target < 0:
                        candidates = sorted(
                            [k for k, v in manual_month_avail.items() if v > 0],
                            key=lambda k: worker["monthly_targets"].get(k, 0),
                            reverse=True,
                        )
                        for month_key in candidates:
                            if remaining_target >= 0:
                                break
                            if worker["monthly_targets"].get(month_key, 0) > 0:
                                worker["monthly_targets"][month_key] -= 1
                                remaining_target += 1

                    logging.debug(
                        f"Worker {worker_id} (MANUAL): monthly targets from guardias/mes={guardias_mes} → "
                        f"{worker['monthly_targets']}"
                    )
                    continue
                else:
                    for month_key in month_days:
                        worker["monthly_targets"][month_key] = 0
                        worker["monthly_targets_ceil"][month_key] = 0
                    continue

            total_avail_days = sum(worker_month_avail.values())
            remaining_target = overall_target

            if total_avail_days > 0:
                for month_key in month_days:
                    avail = worker_month_avail.get(month_key, 0)
                    if avail == 0:
                        worker["monthly_targets"][month_key] = 0
                        worker["monthly_targets_ceil"][month_key] = 0
                    else:
                        raw_fraction = overall_target * avail / total_avail_days
                        month_target = round(raw_fraction)
                        month_target = min(month_target, remaining_target)
                        worker["monthly_targets"][month_key] = month_target
                        worker["monthly_targets_ceil"][month_key] = math.ceil(raw_fraction)
                        remaining_target -= month_target
                        logging.debug(
                            f"Worker {worker_id}: {month_key} → {month_target} shifts "
                            f"({avail}/{total_avail_days} avail days)"
                        )

                if remaining_target > 0:
                    sorted_months = sorted(
                        [(k, v) for k, v in worker_month_avail.items() if v > 0],
                        key=lambda x: x[1],
                        reverse=True,
                    )
                    for month_key, _ in sorted_months:
                        if remaining_target <= 0:
                            break
                        worker["monthly_targets"][month_key] += 1
                        remaining_target -= 1
            else:
                for month_key in month_days:
                    worker["monthly_targets"][month_key] = 0
                    worker["monthly_targets_ceil"][month_key] = 0

        logging.info("Monthly targets calculated (work_periods-aware)")
        return True

    def _get_schedule_months(self) -> dict[str, int]:
        """
        Return a dict mapping ``"YYYY-MM"`` keys to the number of available
        days for each month within the schedule period.
        """
        s = self.scheduler
        month_days: dict[str, int] = {}
        current = s.start_date
        while current <= s.end_date:
            month_key = f"{current.year}-{current.month:02d}"

            month_start = max(current.replace(day=1), s.start_date)
            month_end = min(
                (current.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1),
                s.end_date,
            )

            days_in_month = (month_end - month_start).days + 1
            month_days[month_key] = days_in_month

            current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)

        return month_days
