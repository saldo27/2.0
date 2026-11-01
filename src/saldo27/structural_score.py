"""Constraint-aware baselines for schedule quality scoring.

Operational objectives (``get_bridge_objective_for_worker``, etc.) stay unchanged.
These helpers only adjust *scoring* targets so configuration-driven imbalance
(no_last / only_last / incompat cliques / cadence / unfillable holes) does not
penalise the engine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from saldo27.utilities import is_fixed_cadence

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime


def _incompat_list(worker: dict[str, Any]) -> list[str]:
    raw = worker.get("incompatible_with", []) or []
    if isinstance(raw, str):
        return [raw] if raw.strip() else []
    return [str(x) for x in raw]


def incompat_adjacency(workers_data: list[dict[str, Any]]) -> dict[str, set[str]]:
    """Undirected incompatibility graph (edge if either side lists the other)."""
    ids = {w["id"] for w in workers_data}
    adj: dict[str, set[str]] = {wid: set() for wid in ids}
    for worker in workers_data:
        wid = worker["id"]
        for other in _incompat_list(worker):
            if other in ids and other != wid:
                adj[wid].add(other)
                adj[other].add(wid)
    return adj


def maximal_incompat_cliques(workers_data: list[dict[str, Any]]) -> list[frozenset[str]]:
    """Maximal cliques in the incompat graph (Bron–Kerbosch with pivoting)."""
    adj = incompat_adjacency(workers_data)
    if not any(adj.values()):
        return []

    cliques: list[frozenset[str]] = []

    def bron_kerbosch(r: set[str], p: set[str], x: set[str]) -> None:
        if not p and not x:
            if len(r) >= 2:
                cliques.append(frozenset(r))
            return
        u = next(iter(p | x))
        for v in list(p - adj[u]):
            bron_kerbosch(r | {v}, p & adj[v], x & adj[v])
            p.remove(v)
            x.add(v)

    bron_kerbosch(set(), set(adj), set())
    return cliques


def _worker_available_on_date(scheduler, worker: dict[str, Any], date: datetime) -> bool:
    """Lightweight availability for capacity estimates (no live schedule conflicts)."""
    builder = getattr(scheduler, "schedule_builder", None)
    if builder is not None and hasattr(builder, "_is_worker_unavailable"):
        return not builder._is_worker_unavailable(worker["id"], date)

    # Fallback without builder: work_periods / days_off / cadence dates only.
    date_utils = scheduler.date_utils
    if is_fixed_cadence(worker) or worker.get("has_cadence"):
        try:
            cadence = set(date_utils.parse_dates(worker.get("mandatory_days", "") or ""))
        except (TypeError, ValueError):
            cadence = set()
        if date not in cadence:
            return False

    work_periods = worker.get("work_periods", "") or ""
    if work_periods:
        try:
            ranges = date_utils.parse_date_ranges(work_periods)
            if not any(start <= date <= end for start, end in ranges):
                return False
        except Exception:
            return False

    days_off = worker.get("days_off", "") or ""
    if days_off:
        try:
            if date in set(date_utils.parse_dates(days_off)):
                return False
        except Exception:
            pass
    return True


def clique_day_capacity(
    scheduler,
    clique: frozenset[str],
    workers_by_id: dict[str, dict[str, Any]],
    date_predicate: Callable[[datetime], bool] | None = None,
) -> float:
    """Days where at least one clique member is available (1 assignment/day max)."""
    capacity = 0
    for date in scheduler._get_date_range(scheduler.start_date, scheduler.end_date):
        if date_predicate is not None and not date_predicate(date):
            continue
        if scheduler._get_shifts_for_date(date) <= 0:
            continue
        if any(_worker_available_on_date(scheduler, workers_by_id[wid], date) for wid in clique if wid in workers_by_id):
            capacity += 1
    return float(capacity)


def scale_objectives_for_cliques(
    scheduler,
    nominal_by_id: dict[str, float],
    date_predicate: Callable[[datetime], bool] | None = None,
) -> dict[str, float]:
    """Cap nominal objectives by incompat-clique day capacity; take min across cliques."""
    workers_by_id = {w["id"]: w for w in scheduler.workers_data}
    scaled = dict(nominal_by_id)
    for clique in maximal_incompat_cliques(scheduler.workers_data):
        members = [wid for wid in clique if wid in scaled]
        if len(members) < 2:
            continue
        total = sum(max(0.0, nominal_by_id.get(wid, 0.0)) for wid in members)
        if total <= 0:
            continue
        capacity = clique_day_capacity(scheduler, clique, workers_by_id, date_predicate)
        if total <= capacity + 1e-9:
            continue
        factor = capacity / total
        for wid in members:
            scaled[wid] = min(scaled[wid], max(0.0, nominal_by_id.get(wid, 0.0) * factor))
    return scaled


class StructuralScoreBaselines:
    """Cached constraint-aware scoring baselines for one scheduler snapshot."""

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self._shift_targets: dict[str, float] | None = None
        self._weekend_objectives: dict[str, float] | None = None
        self._bridge_objectives: dict[str, float] | None = None

    def scoring_shift_target(self, worker: dict[str, Any]) -> float:
        """Non-mandatory target capped by incompat-clique capacity."""
        if self._shift_targets is None:
            nominal = {
                w["id"]: float(w.get("target_shifts", 0) or 0) for w in self.scheduler.workers_data
            }
            self._shift_targets = scale_objectives_for_cliques(self.scheduler, nominal)
        return float(self._shift_targets.get(worker["id"], 0.0))

    def scoring_weekend_objective(self, worker: dict[str, Any]) -> float:
        if self._weekend_objectives is None:
            holidays = set(getattr(self.scheduler, "holidays", ()) or ())
            nominal = {
                w["id"]: float(self.scheduler.get_weekend_objective(w)) for w in self.scheduler.workers_data
            }
            self._weekend_objectives = scale_objectives_for_cliques(
                self.scheduler,
                nominal,
                date_predicate=lambda d: self.scheduler.date_utils.is_weekend_day(d, holidays),
            )
        return float(self._weekend_objectives.get(worker["id"], 0.0))

    def scoring_bridge_objective(self, worker: dict[str, Any]) -> float:
        if self._bridge_objectives is None:
            nominal = {
                w["id"]: float(self.scheduler.get_bridge_objective_for_worker(w["id"]))
                for w in self.scheduler.workers_data
            }

            def _is_bridge(date: datetime) -> bool:
                return bool(self.scheduler.date_utils.get_bridge_period_for_date(date, self.scheduler.bridge_periods))

            self._bridge_objectives = scale_objectives_for_cliques(
                self.scheduler, nominal, date_predicate=_is_bridge
            )
        return float(self._bridge_objectives.get(worker["id"], 0.0))

    def is_structural_empty(self, date: datetime, post: int) -> bool:
        """True when the empty slot has no hard-feasible candidate under current config."""
        slots = self.scheduler.schedule.get(date)
        if not slots or post >= len(slots) or slots[post] is not None:
            return False

        builder = getattr(self.scheduler, "schedule_builder", None)
        if builder is None or not hasattr(builder, "_check_hard_constraints"):
            # Without a builder, treat empties as non-structural (still penalise).
            return False

        for worker in self.scheduler.workers_data:
            wid = worker["id"]
            if date in self.scheduler.worker_assignments.get(wid, ()):
                continue
            if builder._check_hard_constraints(wid, date, post):
                return False
        return True

    def structural_fill_rate(self) -> float:
        """Fill rate counting structurally unfillable holes as filled."""
        total = 0
        effective_filled = 0
        for date, slots in self.scheduler.schedule.items():
            for post, wid in enumerate(slots):
                total += 1
                if wid is not None or self.is_structural_empty(date, post):
                    effective_filled += 1
        if total == 0:
            return 100.0
        return 100.0 * effective_filled / total

    def expected_post_counts(self, worker: dict[str, Any], total_assignments: int) -> dict[int, float] | None:
        """Expected post histogram for scoring.

        Restricted / cadence workers return ``None`` (caller should treat as perfect).

        When the pool has no ``no_last_post`` / ``only_last_post`` workers, expect
        a uniform split across posts (legacy behaviour). When post restrictions
        exist, unrestricted workers are scored against the structural Rosell
        share from ``get_last_post_objective`` so absorbing spillover Rosell is
        not penalised.
        """
        if total_assignments <= 0:
            return None
        if worker.get("no_last_post", False) or worker.get("only_last_post", False) or is_fixed_cadence(worker):
            return None

        num_shifts = max(1, int(getattr(self.scheduler, "num_shifts", 1)))
        if num_shifts <= 1:
            return {0: float(total_assignments)}

        has_post_restrictions = any(
            w.get("no_last_post", False) or w.get("only_last_post", False) for w in self.scheduler.workers_data
        )
        if not has_post_restrictions:
            even = float(total_assignments) / num_shifts
            return {i: even for i in range(num_shifts)}

        last_idx = num_shifts - 1
        last_obj = float(self.scheduler.get_last_post_objective(worker))
        shift_obj = float(self.scheduler._shift_objective(worker) or 0.0)
        if shift_obj > 0:
            last_rate = min(1.0, max(0.0, last_obj / shift_obj))
        else:
            last_rate = 1.0 / num_shifts
        expected_last = float(total_assignments) * last_rate
        remaining = float(total_assignments) - expected_last
        non_last = num_shifts - 1
        per_other = remaining / non_last if non_last else 0.0

        expected = {i: per_other for i in range(non_last)}
        expected[last_idx] = expected_last
        return expected
