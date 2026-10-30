"""CP-SAT pass that fills leftover empty slots without undoing protected dates.

The generation greedy fills and the Final Adjustment OR-Tools phase never treat
empty slots as decision variables (FA only reassigns occupied ones, and reverts
if balance worsens). This pass includes the empties, maximises coverage, and
keeps mandatory, cadence and current manual dates fixed. Auto workers may move
within monthly_cap and the relaxed shift ceiling. A long calendar is re-solved
in a window around each cluster of holes, with the rest of the schedule frozen.
Skipped when OR-Tools is missing.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from saldo27 import tolerance_policy
from saldo27.constraint_checker import MAX_CONSECUTIVE_LAST_POSTS, candidate_spacing_rejection
from saldo27.infrastructure.optional_engines import ortools_available
from saldo27.prior_schedule_handler import get_effective_assignments
from saldo27.utilities import get_effective_min_gap, is_fixed_cadence, widest_base_gap

if TYPE_CHECKING:
    from saldo27.scheduler import Scheduler

FILL_WEIGHT = 1_000
# A full-period model on a long calendar does not come back with a feasible
# rearrangement in the time limit. Above this size, only the hole windows move.
FULL_SOLVE_SLOT_LIMIT = 480
WINDOW_RADIUS_DAYS = 14


class HoleFillEngine:
    """Fill empty slots with CP-SAT; no-op when there are none or OR-Tools is absent."""

    def __init__(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler
        self.schedule = scheduler.schedule
        self.workers_data = scheduler.workers_data
        self.builder = getattr(scheduler, "schedule_builder", None)

    def fill(self, time_limit_seconds: int | None = None) -> int:
        """Apply a coverage-improving reassignment. Returns how many empty slots were filled."""
        empty_before = self._empty_count()
        if empty_before == 0:
            return 0
        config = getattr(self.scheduler, "config", None) or {}
        if not config.get("enable_hole_fill", True):
            return 0
        if not ortools_available():
            logging.info("HoleFillEngine: OR-Tools no disponible, se omite el relleno de huecos.")
            return 0

        if time_limit_seconds is None:
            time_limit_seconds = int(config.get("ortools_time_limit_seconds", 20))

        filled = 0
        slot_count = sum(len(posts) for posts in self.schedule.values())
        if slot_count <= FULL_SOLVE_SLOT_LIMIT:
            filled += self._attempt(time_limit_seconds, active_dates=None)
        for start, end in self._hole_clusters():
            if self._empty_count() == 0:
                break
            window = {
                day
                for day in self.schedule
                if start - timedelta(days=WINDOW_RADIUS_DAYS) <= day <= end + timedelta(days=WINDOW_RADIUS_DAYS)
            }
            logging.info(f"HoleFillEngine: reajuste {start:%Y-%m-%d}..{end:%Y-%m-%d} ({len(window)} días).")
            filled += self._attempt(time_limit_seconds, active_dates=window)
        if filled:
            logging.info(f"HoleFillEngine: cubiertos {filled} hueco(s) ({empty_before} → {self._empty_count()}).")
        return filled

    def _attempt(self, time_limit_seconds: int, active_dates: set[datetime] | None) -> int:
        """Solve one model and keep it only when coverage rises without new violations."""
        empty_before = self._empty_count()
        if active_dates is not None and not any(
            worker is None and day in active_dates for day, posts in self.schedule.items() for worker in posts
        ):
            return 0
        snapshot = {day: posts[:] for day, posts in self.schedule.items()}
        violations_before = self._violation_count()
        t0 = time.monotonic()
        try:
            assignment = self._solve(time_limit_seconds, active_dates)
        except Exception as exc:
            logging.error(f"HoleFillEngine: error CP-SAT: {exc}", exc_info=True)
            return 0
        finally:
            logging.info(f"HoleFillEngine: CP-SAT completado en {time.monotonic() - t0:.2f}s")

        if assignment is None:
            return 0

        self._apply(assignment)
        empty_after = self._empty_count()
        filled = empty_before - empty_after
        violations_after = self._violation_count()
        if filled <= 0 or violations_after > violations_before:
            logging.info(
                f"HoleFillEngine: se restaura "
                f"(huecos {empty_before}→{empty_after}, violaciones {violations_before}→{violations_after})."
            )
            self._apply_posts(snapshot)
            return 0
        return filled

    def _hole_clusters(self) -> list[tuple[datetime, datetime]]:
        """Groups of hole days at most ``WINDOW_RADIUS_DAYS`` apart."""
        hole_days = sorted(day for day, posts in self.schedule.items() if any(worker is None for worker in posts))
        if not hole_days:
            return []
        clusters: list[tuple[datetime, datetime]] = []
        start = previous = hole_days[0]
        for day in hole_days[1:]:
            if (day - previous).days > WINDOW_RADIUS_DAYS:
                clusters.append((start, previous))
                start = day
            previous = day
        clusters.append((start, previous))
        return clusters

    def _violation_count(self) -> int:
        checker = getattr(self.scheduler, "constraint_checker", None)
        if checker is None:
            return 0
        try:
            return len(checker.check_schedule_violations(self.scheduler.worker_assignments))
        except Exception:
            return 0

    def _empty_count(self) -> int:
        return sum(1 for posts in self.schedule.values() for worker in posts if worker is None)

    def _apply_posts(self, posts_by_date: dict[datetime, list]) -> None:
        for day, posts in posts_by_date.items():
            self.schedule[day][:] = posts[:]
        self.scheduler._synchronize_tracking_data()

    def _apply(self, assignment: dict[tuple[datetime, int], str | None]) -> None:
        touched = {day for day, _post in assignment}
        for day in touched:
            posts = self.schedule[day]
            for post in range(len(posts)):
                key = (day, post)
                if key in assignment:
                    posts[post] = assignment[key]
        self.scheduler._synchronize_tracking_data()

    def _mandatory_dates(self, worker: dict) -> set[datetime]:
        raw = worker.get("mandatory_days") or ""
        if not raw:
            return set()
        try:
            return {d for d in self.scheduler.date_utils.parse_dates(raw) if d in self.schedule}
        except (TypeError, ValueError):
            return set()

    def _is_unavailable(self, worker_id: str, date: datetime) -> bool:
        if self.builder is not None:
            return bool(self.builder._is_worker_unavailable(worker_id, date))
        return False

    def _link_frozen_rosell(
        self,
        model: Any,
        x: dict[tuple[int, int], Any],
        wi: int,
        wid: str,
        slots: list[tuple[datetime, int, str | None]],
        slot_is_last: list[bool],
        last_sis: list[int],
        frozen_days: set[datetime],
        protected: set[datetime],
    ) -> None:
        """A frozen last post still forbids a window last post with no non-last of this worker between them."""
        if not frozen_days or not last_sis:
            return

        def _is_frozen_last(day: datetime) -> bool:
            posts = self.schedule.get(day) or []
            if wid not in posts:
                return False
            return self.scheduler.is_last_post(day, posts.index(wid))

        frozen_sorted = sorted(frozen_days)
        for si in last_sis:
            day = slots[si][0]
            if day in protected:
                continue
            earlier = [d for d in frozen_sorted if d < day]
            if earlier and _is_frozen_last(earlier[-1]):
                resets = [x[wi, sk] for sk in range(si) if not slot_is_last[sk] and earlier[-1] < slots[sk][0] < day]
                model.add(x[wi, si] <= (sum(resets) if resets else 0))
            later = [d for d in frozen_sorted if d > day]
            if later and _is_frozen_last(later[0]):
                resets = [
                    x[wi, sk]
                    for sk in range(si + 1, len(slots))
                    if not slot_is_last[sk] and day < slots[sk][0] < later[0]
                ]
                model.add(x[wi, si] <= (sum(resets) if resets else 0))

    def _solve(
        self, time_limit_seconds: int, active_dates: set[datetime] | None = None
    ) -> dict[tuple[datetime, int], str | None] | None:
        from ortools.sat.python import cp_model

        slots: list[tuple[datetime, int, str | None]] = []
        for date, posts in sorted(self.schedule.items()):
            if active_dates is not None and date not in active_dates:
                continue
            for post, worker in enumerate(posts):
                slots.append((date, post, worker))
        if not slots:
            return None

        worker_ids = [w["id"] for w in self.workers_data]
        worker_by_id = {w["id"]: w for w in self.workers_data}
        n_workers = len(worker_ids)
        n_slots = len(slots)
        worker_idx = {wid: i for i, wid in enumerate(worker_ids)}
        mandatory = {wid: self._mandatory_dates(w) for wid, w in worker_by_id.items()}

        model = cp_model.CpModel()
        x: dict[tuple[int, int], Any] = {}
        for wi in range(n_workers):
            for si in range(n_slots):
                x[wi, si] = model.new_bool_var(f"h_{wi}_{si}")

        filled_terms = []
        for si in range(n_slots):
            slot_vars = [x[wi, si] for wi in range(n_workers)]
            model.add_at_most_one(slot_vars)
            filled_terms.extend(slot_vars)

        date_to_slots: dict[datetime, list[int]] = {}
        for si, (date, _post, _cur) in enumerate(slots):
            date_to_slots.setdefault(date, []).append(si)
        for wi in range(n_workers):
            for slot_indices in date_to_slots.values():
                if len(slot_indices) > 1:
                    model.add(sum(x[wi, si] for si in slot_indices) <= 1)

        pinned_by_worker: dict[str, set[datetime]] = {}
        for wi, wid in enumerate(worker_ids):
            worker = worker_by_id[wid]
            pinned_dates = set(mandatory[wid])
            if is_fixed_cadence(worker) or tolerance_policy.is_manual(worker):
                pinned_dates.update(d for d, posts in self.schedule.items() if wid in posts)
            pinned_by_worker[wid] = pinned_dates
            if is_fixed_cadence(worker):
                for date, slot_indices in date_to_slots.items():
                    if date in pinned_dates:
                        model.add(sum(x[wi, si] for si in slot_indices) == 1)
                    else:
                        for si in slot_indices:
                            model.add(x[wi, si] == 0)
                continue
            for date, slot_indices in date_to_slots.items():
                if date in pinned_dates:
                    model.add(sum(x[wi, si] for si in slot_indices) == 1)
            for si, (date, _post, _cur) in enumerate(slots):
                if self._is_unavailable(wid, date) and date not in pinned_dates:
                    model.add(x[wi, si] == 0)

        for wi, wid in enumerate(worker_ids):
            worker = worker_by_id[wid]
            for si, (date, post, _cur) in enumerate(slots):
                is_last = self.scheduler.is_last_post(date, post)
                if is_last and worker.get("no_last_post", False):
                    model.add(x[wi, si] == 0)
                if not is_last and worker.get("only_last_post", False):
                    model.add(x[wi, si] == 0)

        incompat_pairs: set[tuple[str, str]] = set()
        for worker in self.workers_data:
            for other in worker.get("incompatible_with") or []:
                incompat_pairs.add((min(worker["id"], other), max(worker["id"], other)))
        for a_id, b_id in incompat_pairs:
            if a_id not in worker_idx or b_id not in worker_idx:
                continue
            a_wi, b_wi = worker_idx[a_id], worker_idx[b_id]
            for date, slot_indices in date_to_slots.items():
                if date in pinned_by_worker[a_id] and date in pinned_by_worker[b_id]:
                    continue
                model.add(sum(x[a_wi, si] for si in slot_indices) + sum(x[b_wi, si] for si in slot_indices) <= 1)

        gap = self.scheduler.gap_between_shifts
        gap_periods = getattr(self.scheduler, "gap_periods", None) or []
        max_window = max(14, widest_base_gap(gap, gap_periods) + 1)
        candidate_pairs: list[tuple[int, int]] = []
        for idx_a in range(n_slots):
            date_a = slots[idx_a][0]
            for idx_b in range(idx_a + 1, n_slots):
                date_b = slots[idx_b][0]
                if (date_b - date_a).days > max_window:
                    break
                candidate_pairs.append((idx_a, idx_b))

        for wi, wid in enumerate(worker_ids):
            worker = worker_by_id[wid]
            if is_fixed_cadence(worker):
                continue
            pinned = mandatory[wid]
            for idx_a, idx_b in candidate_pairs:
                date_a, date_b = slots[idx_a][0], slots[idx_b][0]
                if date_a in pinned and date_b in pinned:
                    continue
                if slots[idx_a][2] == wid and slots[idx_b][2] == wid:
                    continue
                pair_gap = get_effective_min_gap(worker, gap, date=date_b, other_date=date_a, gap_periods=gap_periods)
                if (
                    candidate_spacing_rejection(
                        date_b,
                        (date_a,),
                        min_gap=pair_gap,
                        date_is_mandatory=date_b in pinned,
                        block_gap2_weekend_pairs=(pair_gap == 2),
                    )
                    is not None
                ):
                    model.add(x[wi, idx_a] + x[wi, idx_b] <= 1)

        import bisect

        slot_dates = [s[0] for s in slots]
        cutoff = self.scheduler.start_date - timedelta(days=90)
        frozen: dict[str, set[datetime]] = {wid: set() for wid in worker_ids}
        if active_dates is not None:
            for date, posts in self.schedule.items():
                if date in active_dates:
                    continue
                for worker in posts:
                    if worker in frozen:
                        frozen[worker].add(date)
        for wi, wid in enumerate(worker_ids):
            if is_fixed_cadence(worker_by_id[wid]):
                continue
            prior_dates = sorted(
                d
                for d in getattr(self.scheduler, "prior_assignments", {}).get(wid, set())
                if cutoff <= d < self.scheduler.start_date
            )
            prior_dates.extend(sorted(frozen[wid]))
            for prior_date in prior_dates:
                lo = bisect.bisect_left(slot_dates, prior_date - timedelta(days=max_window))
                hi = bisect.bisect_right(slot_dates, prior_date + timedelta(days=max_window))
                for si in range(lo, hi):
                    date_s = slots[si][0]
                    if date_s in mandatory[wid] or (slots[si][2] == wid and prior_date not in frozen[wid]):
                        continue
                    pair_gap = get_effective_min_gap(
                        worker_by_id[wid], gap, date=date_s, other_date=prior_date, gap_periods=gap_periods
                    )
                    if (
                        candidate_spacing_rejection(
                            date_s,
                            (prior_date,),
                            min_gap=pair_gap,
                            date_is_mandatory=False,
                            block_gap2_weekend_pairs=(pair_gap == 2),
                        )
                        is not None
                    ):
                        model.add(x[wi, si] == 0)

        months = sorted({(d.year, d.month) for d, _p, _c in slots})
        for wi, wid in enumerate(worker_ids):
            worker = worker_by_id[wid]
            if is_fixed_cadence(worker):
                continue
            mand = mandatory[wid]
            held = self.scheduler.worker_assignments.get(wid, set()) if active_dates is not None else set()
            outside = [day for day in held if active_dates is not None and day not in active_dates]
            outside_free = sum(1 for day in outside if day not in mand)
            free_slots = [si for si, (date, _p, _c) in enumerate(slots) if date not in mand]
            if tolerance_policy.is_manual(worker):
                model.add(sum(x[wi, si] for si in free_slots) + outside_free <= int(worker.get("target_shifts") or 0))
            else:
                high = tolerance_policy.max_shifts(worker, worker.get("target_shifts") or 0, relaxed=True)
                model.add(sum(x[wi, si] for si in free_slots) + outside_free <= high)
            for year, month in months:
                cap = tolerance_policy.monthly_cap(worker, year, month)
                if cap is None:
                    continue
                month_free = [
                    si
                    for si, (date, _p, _c) in enumerate(slots)
                    if date.year == year and date.month == month and date not in mand
                ]
                outside_month = sum(1 for day in outside if day.year == year and day.month == month and day not in mand)
                if month_free or outside_month:
                    model.add(sum(x[wi, si] for si in month_free) + outside_month <= int(cap))

        # Rosell follows the worker's own sequence: any two last posts are
        # forbidden unless one of their non-last shifts sits strictly between
        # them. Both dates mandatory is a configuration clash and stays.
        slot_is_last = [self.scheduler.is_last_post(slots[si][0], slots[si][1]) for si in range(n_slots)]
        if getattr(self.scheduler, "num_shifts", 1) > 1 and n_slots > 0 and MAX_CONSECUTIVE_LAST_POSTS == 1:
            last_sis = [si for si in range(n_slots) if slot_is_last[si]]
            for wi, wid in enumerate(worker_ids):
                worker = worker_by_id[wid]
                if worker.get("only_last_post", False) or is_fixed_cadence(worker) or not last_sis:
                    continue
                protected = mandatory[wid]
                for i, si in enumerate(last_sis):
                    for prev_si in last_sis[:i]:
                        if slots[prev_si][0] in protected and slots[si][0] in protected:
                            continue
                        between = [x[wi, sk] for sk in range(prev_si + 1, si) if not slot_is_last[sk]]
                        model.add(x[wi, prev_si] + x[wi, si] <= 1 + sum(between))
                self._link_frozen_rosell(model, x, wi, wid, slots, slot_is_last, last_sis, frozen[wid], protected)

        holidays = set(self.scheduler.holidays or [])
        slot_is_weekend = [self.scheduler.date_utils.is_weekend_day(slots[si][0], holidays) for si in range(n_slots)]
        max_consecutive_weekends = int(getattr(self.scheduler, "max_consecutive_weekends", 3) or 3)
        weekend_slots_by_week: dict[datetime, list[int]] = {}
        for si in range(n_slots):
            if slot_is_weekend[si]:
                week_start = slots[si][0] - timedelta(days=slots[si][0].weekday())
                weekend_slots_by_week.setdefault(week_start, []).append(si)
        prior_assignments = getattr(self.scheduler, "prior_assignments", {}) or {}
        prior_weekend_weeks: dict[str, set[datetime]] = {wid: set() for wid in worker_ids}
        for wid, frozen_days in frozen.items():
            prior_weekend_weeks[wid].update(
                day - timedelta(days=day.weekday())
                for day in frozen_days
                if self.scheduler.date_utils.is_weekend_day(day, holidays)
            )
        if prior_assignments and self.scheduler.start_date is not None:
            for wid in worker_ids:
                prior_dates = get_effective_assignments(wid, {}, prior_assignments, self.scheduler.start_date)
                prior_weekend_weeks.setdefault(wid, set()).update(
                    d - timedelta(days=d.weekday())
                    for d in prior_dates
                    if d < self.scheduler.start_date and self.scheduler.date_utils.is_weekend_day(d, holidays)
                )
        sorted_weeks = sorted(
            set(weekend_slots_by_week).union(*prior_weekend_weeks.values())
            if prior_weekend_weeks
            else weekend_slots_by_week
        )
        if sorted_weeks and max_consecutive_weekends >= 1:
            for wi, wid in enumerate(worker_ids):
                if is_fixed_cadence(worker_by_id[wid]):
                    continue
                worker_prior = prior_weekend_weeks.get(wid, set())
                week_flags: list = []
                for week_start in sorted_weeks:
                    if week_start in worker_prior:
                        week_flags.append(1)
                        continue
                    week_slot_indices = weekend_slots_by_week.get(week_start)
                    if not week_slot_indices:
                        week_flags.append(0)
                        continue
                    week_sum = sum(x[wi, si] for si in week_slot_indices)
                    flag = model.new_bool_var(f"hw_{wi}_{week_start.strftime('%Y%m%d')}")
                    model.add(week_sum >= 1).only_enforce_if(flag)
                    model.add(week_sum == 0).only_enforce_if(flag.Not())
                    week_flags.append(flag)
                window = max_consecutive_weekends + 1
                for start_idx in range(len(sorted_weeks) - max_consecutive_weekends):
                    consecutive = all(
                        (sorted_weeks[start_idx + offset + 1] - sorted_weeks[start_idx + offset]).days == 7
                        for offset in range(max_consecutive_weekends)
                    )
                    window_flags = [week_flags[start_idx + offset] for offset in range(window)]
                    if consecutive and any(not isinstance(f, int) for f in window_flags):
                        model.add(sum(window_flags) <= max_consecutive_weekends)

        changes = []
        for si, (_date, _post, current) in enumerate(slots):
            if current is None:
                continue
            wi = worker_idx.get(current)
            if wi is None:
                continue
            stay = model.new_bool_var(f"stay_{si}")
            model.add(x[wi, si] == 1).only_enforce_if(stay)
            model.add(x[wi, si] == 0).only_enforce_if(stay.Not())
            changes.append(stay.Not())

        model.maximize(FILL_WEIGHT * sum(filled_terms) - sum(changes))

        for wi, wid in enumerate(worker_ids):
            for si, (_date, _post, current) in enumerate(slots):
                model.add_hint(x[wi, si], 1 if current == wid else 0)

        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = float(time_limit_seconds)
        solver.parameters.num_search_workers = 1
        solver.parameters.log_search_progress = False
        status = solver.solve(model)
        if status == cp_model.FEASIBLE:
            logging.info("HoleFillEngine: solución factible (tiempo agotado).")
        if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            logging.info(f"HoleFillEngine: sin solución ({solver.status_name(status)}).")
            return None

        result: dict[tuple[datetime, int], str | None] = {}
        for si, (date, post, _current) in enumerate(slots):
            chosen = None
            for wi, wid in enumerate(worker_ids):
                if solver.value(x[wi, si]) == 1:
                    chosen = wid
                    break
            result[(date, post)] = chosen
        return result
