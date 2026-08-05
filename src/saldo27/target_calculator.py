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

            auto_calc_workers = [w for w in s.workers_data if w.get("auto_calculate_shifts", True)]
            manual_workers = [w for w in s.workers_data if not w.get("auto_calculate_shifts", True)]

            # 1) MANUAL CALCULATION FIRST — reserve their slots before auto distribution
            manual_slots_reserved = 0
            if manual_workers:
                manual_slots_reserved = self._calculate_manual_targets(manual_workers)

            # 2) AUTOMATIC CALCULATION for auto_calc_workers (using remaining slots)
            if auto_calc_workers:
                total_slots = sum(len(slots) for slots in s.schedule.values())
                if total_slots <= 0:
                    logging.warning("No slots in schedule; skipping allocation")
                else:
                    slots_for_auto = total_slots - manual_slots_reserved
                    if slots_for_auto < 0:
                        logging.error(
                            f"Manual targets ({manual_slots_reserved}) exceed total slots ({total_slots})! "
                            f"Clamping to 0 for auto workers."
                        )
                        slots_for_auto = 0
                    elif manual_slots_reserved > 0:
                        logging.info(
                            f"Reserving {manual_slots_reserved} slots for manual workers. "
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

            work_periods_str = w.get("work_periods", "").strip()
            if work_periods_str:
                try:
                    work_ranges = s.date_utils.parse_date_ranges(work_periods_str)
                except (TypeError, ValueError) as exc:
                    logging.warning(f"Worker {wid} invalid work_periods; using default availability: {exc}")
                    work_ranges = []

                if work_ranges:
                    worker_months = 0.0
                    cur_year, cur_month = s.start_date.year, s.start_date.month
                    end_year, end_month = s.end_date.year, s.end_date.month
                    while (cur_year, cur_month) <= (end_year, end_month):
                        days_in_month = calendar.monthrange(cur_year, cur_month)[1]
                        month_start = datetime(cur_year, cur_month, 1)
                        month_end = datetime(cur_year, cur_month, days_in_month)
                        covered = 0
                        for rng_start, rng_end in work_ranges:
                            overlap_start = max(month_start, rng_start, s.start_date)
                            overlap_end = min(month_end, rng_end, s.end_date)
                            if overlap_end >= overlap_start:
                                covered += (overlap_end - overlap_start).days + 1
                        worker_months += covered / days_in_month
                        cur_month += 1
                        if cur_month > 12:
                            cur_month = 1
                            cur_year += 1
                    logging.debug(f"Worker {wid}: work_periods → {worker_months:.2f} effective months")
                else:
                    worker_months = proportional_months
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
