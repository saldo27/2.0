"""
prior_schedule_handler.py
=========================
Parses a previously-exported schedule JSON and extracts per-worker
statistics that the scheduler needs to honour cross-period constraints:

  • prior_assignments   – set of datetime dates worked in the prior period
  • prior_weekends      – count of weekend/holiday/pre-holiday dates worked
  • prior_shift_counts  – total shifts worked (for target adjustment)
  • prior_last_date     – last date worked (for gap constraints at boundary)

The exported JSON format (produced by scheduler.export_schedule_json) is:
{
  "metadata": { "period_start": "...", "period_end": "...", ... },
  "schedule":  { "YYYY-MM-DD": [worker_id, ...], ... },
  "worker_assignments": { worker_id: ["YYYY-MM-DD", ...], ... },
  "workers_data": [ ... ],
  "config": { ... }
}
"""

import json
import logging
from datetime import datetime
from typing import Any

from saldo27.utilities import DateTimeUtils

_DATE_UTILS = DateTimeUtils()


def _is_weekend_or_holiday(date: datetime, holidays: set[datetime]) -> bool:
    """Return True if date is Fri/Sat/Sun, a holiday, or the day before a holiday."""
    return _DATE_UTILS.is_weekend_day(date, holidays)


def load_prior_schedule(
    json_source,
    new_period_start: datetime,
    new_period_holidays: set[datetime] | None = None,
) -> dict[str, Any]:
    """
    Parse a prior-period schedule JSON and return a dict with per-worker stats.

    Parameters
    ----------
    json_source : file-like object, str path, or dict
        The prior schedule JSON.  Accepted forms:
          • a dict (already parsed)
          • a str file path
          • a file-like object (e.g. from st.file_uploader)
    new_period_start : datetime
        First day of the NEW scheduling period (used to decide which prior
        assignments are "recent enough" to matter for gap/consecutive checks).
    new_period_holidays : set of datetime, optional
        Holidays of the *new* period.  We also need the prior-period holidays
        that appear in the schedule itself; those are read from the JSON.

    Returns
    -------
    dict with keys:
      "prior_assignments"  : {worker_id: set of datetime}
      "prior_weekends"     : {worker_id: int}
      "prior_shift_counts" : {worker_id: int}
      "prior_last_date"    : {worker_id: datetime or None}
      "prior_period_start" : datetime or None
      "prior_period_end"   : datetime or None
      "holidays"           : set of datetime (prior-period holidays from JSON)
      "error"              : str or None  — set if parsing failed
    """
    result: dict[str, Any] = {
        "prior_assignments": {},
        "prior_weekends": {},
        "prior_shift_counts": {},
        "prior_target_shifts": {},  # configured/computed target per worker in prior period
        "prior_last_date": {},
        "prior_period_start": None,
        "prior_period_end": None,
        "holidays": set(),
        "error": None,
    }

    # ── 1. Parse JSON ──────────────────────────────────────────────────────────
    try:
        if isinstance(json_source, dict):
            data = json_source
        elif isinstance(json_source, str):
            with open(json_source, encoding="utf-8") as fh:
                data = json.load(fh)
        else:
            # file-like (e.g. BytesIO from Streamlit uploader)
            content = json_source.read()
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            data = json.loads(content)
    except Exception as exc:
        result["error"] = f"No se pudo leer el JSON: {exc}"
        return result

    # ── 2. Basic structure validation ─────────────────────────────────────────
    if not isinstance(data, dict):
        result["error"] = "El archivo JSON no tiene el formato esperado (debe ser un objeto, no una lista)."
        return result

    # Accept both the full export format and the simpler config+schedule format
    worker_assignments_raw: dict[str, list] = {}

    if "worker_assignments" in data:
        # Full export format: worker_assignments is already per-worker list of dates
        worker_assignments_raw = data["worker_assignments"]
    elif "schedule" in data:
        # Reconstruct from schedule dict
        sched = data["schedule"]
        for date_str, workers_in_posts in sched.items():
            if not workers_in_posts:
                continue
            for w in workers_in_posts:
                if w is None:
                    continue
                worker_assignments_raw.setdefault(str(w), []).append(date_str)
    else:
        result["error"] = (
            "El JSON no contiene 'worker_assignments' ni 'schedule'. "
            "Exporta el calendario anterior con el botón 'Descargar JSON Completo'."
        )
        return result

    # ── 3. Extract prior period dates ─────────────────────────────────────────
    try:
        meta = data.get("metadata") or data.get("config") or {}
        ps = meta.get("period_start") or meta.get("start_date") or data.get("start_date")
        pe = meta.get("period_end") or meta.get("end_date") or data.get("end_date")
        if ps:
            result["prior_period_start"] = datetime.fromisoformat(str(ps).split("T")[0])
        if pe:
            result["prior_period_end"] = datetime.fromisoformat(str(pe).split("T")[0])
        if not ps or not pe:
            logging.warning("[PriorSchedule] Could not extract period dates from imported JSON")
    except Exception as exc:
        logging.warning(f"[PriorSchedule] Error parsing period dates: {exc}")

    # ── 4. Extract prior holidays ─────────────────────────────────────────────
    prior_holidays: set[datetime] = set()
    raw_holidays = data.get("holidays", [])
    for h in raw_holidays:
        try:
            prior_holidays.add(datetime.fromisoformat(str(h).split("T")[0]))
        except (TypeError, ValueError) as exc:
            logging.debug(f"[PriorSchedule] Skipping invalid holiday date {h!r}: {exc}")
    result["holidays"] = prior_holidays

    # Merge with new-period holidays for weekend detection near boundary
    all_holidays = prior_holidays | (new_period_holidays or set())

    # ── 5. Build per-worker stats ──────────────────────────────────────────────
    for worker_id, date_list in worker_assignments_raw.items():
        dates: set[datetime] = set()
        for d_raw in date_list:
            try:
                dates.add(datetime.fromisoformat(str(d_raw).split("T")[0]))
            except (TypeError, ValueError) as exc:
                logging.debug(f"[PriorSchedule] Skipping invalid assignment date {d_raw!r} for {worker_id}: {exc}")
                continue

        result["prior_assignments"][worker_id] = dates
        result["prior_shift_counts"][worker_id] = len(dates)
        result["prior_last_date"][worker_id] = max(dates) if dates else None
        result["prior_weekends"][worker_id] = sum(1 for d in dates if _is_weekend_or_holiday(d, all_holidays))

    # ── 6. Extract prior targets per worker from workers_data ──────────────────
    #   Use _raw_target (pre-mandatory-adjustment) so that delta calculation
    #   compares total_actual vs total_target and mandatory shifts cancel out.
    #   Falls back to target_shifts for exports created before _raw_target existed.
    for w in data.get("workers_data", []):
        wid = str(w.get("id", ""))
        t = w.get("_raw_target", w.get("target_shifts"))
        if wid and t is not None:
            try:
                result["prior_target_shifts"][wid] = float(t)
            except (TypeError, ValueError):
                pass

    logging.info(
        f"[PriorSchedule] Loaded {len(result['prior_assignments'])} workers "
        f"from prior period "
        f"{result['prior_period_start']} → {result['prior_period_end']}"
    )
    return result


def summarize_prior_schedule(prior_data: dict[str, Any]) -> dict[str, dict]:
    """
    Return a human-readable summary dict: {worker_id: {shifts, weekends, last_date}}.
    Used by the UI to show what was loaded.
    """
    summary = {}
    for wid in sorted(prior_data.get("prior_assignments", {}).keys()):
        summary[wid] = {
            "shifts": prior_data["prior_shift_counts"].get(wid, 0),
            "weekends": prior_data["prior_weekends"].get(wid, 0),
            "last_date": prior_data["prior_last_date"].get(wid),
        }
    return summary


def apply_prior_period_balance(
    workers_data: list[dict],
    prior_shift_counts: dict[str, int],
    prior_target_shifts: dict[str, float],
    base_target_shifts: dict[str, float],
) -> None:
    """
    Adjust each worker's ``target_shifts`` for the new period so that
    over/under-delivery in the prior period is compensated.

    Logic:
      delta = prior_actual_shifts - prior_target_shifts
      new_target = base_target - delta

    Workers who worked MORE than their prior target get a smaller new-period
    target (and vice-versa).  The adjustment is always relative to
    ``base_target_shifts`` so that repeated calls (e.g. user loads a
    different prior file) are idempotent.

    Mutates ``workers_data`` in-place.
    """
    if not prior_shift_counts or not prior_target_shifts:
        return

    for worker in workers_data:
        wid = worker["id"]
        prior_actual = prior_shift_counts.get(wid)
        prior_target = prior_target_shifts.get(wid)
        if prior_actual is None or prior_target is None or prior_target == 0:
            continue

        delta = prior_actual - prior_target  # positive: worked extra; negative: worked less
        if delta == 0:
            continue

        base_target = base_target_shifts.get(wid) or float(worker.get("target_shifts", 1))
        adjusted = max(1, round(base_target - delta))
        worker["target_shifts"] = adjusted
        logging.info(
            f"[PriorBalance] {wid}: base_target={base_target} → adjusted={adjusted} "
            f"(prior_actual={prior_actual}, prior_target={prior_target}, delta={delta:+.0f})"
        )


def validate_target_capacity(
    workers_data: list[dict],
    schedule: dict,
    base_target_shifts: dict[str, float],
) -> None:
    """
    Check that the sum of all adjusted ``target_shifts`` does not exceed the
    total available slots.  If it does, proportionally scale down the
    adjustments that *increased* targets (workers who were under-assigned
    in the prior period) so the total fits within capacity.

    Mutates ``workers_data`` in-place.
    """
    total_slots = sum(len(slots) for slots in schedule.values())
    if total_slots <= 0:
        return

    target_sum = sum(w.get("target_shifts", 0) for w in workers_data)
    logging.info(f"[TargetValidation] Sum of targets={target_sum}, available slots={total_slots}")

    if target_sum <= total_slots:
        return

    overflow = target_sum - total_slots
    logging.warning(
        f"[TargetValidation] Adjusted targets exceed capacity by {overflow} "
        f"({target_sum} targets vs {total_slots} slots). "
        f"Scaling down inflated targets to fit."
    )

    # Identify workers whose targets were inflated by prior-balance (delta < 0)
    inflated = []
    for w in workers_data:
        wid = w["id"]
        base = base_target_shifts.get(wid, 0)
        current = w.get("target_shifts", 0)
        if current > base:
            inflated.append((w, current - base))

    if not inflated:
        logging.warning(
            "[TargetValidation] No inflated targets to reduce; deficit may be unavoidable with current constraints."
        )
        return

    total_inflation = sum(inc for _, inc in inflated)
    remaining_overflow = overflow

    # Proportionally reduce inflated targets
    for w, increment in inflated:
        reduction = min(increment, round(increment / total_inflation * overflow))
        reduction = min(reduction, remaining_overflow)
        if reduction > 0:
            old = w["target_shifts"]
            w["target_shifts"] = max(1, old - reduction)
            remaining_overflow -= reduction
            logging.info(
                f"[TargetValidation] {w['id']}: target {old} → {w['target_shifts']} "
                f"(reduced by {reduction} to fit capacity)"
            )

    # If rounding left leftover, remove one more from the largest remaining inflation
    if remaining_overflow > 0:
        inflated.sort(key=lambda x: x[0].get("target_shifts", 0), reverse=True)
        for w, _ in inflated:
            if remaining_overflow <= 0:
                break
            if w["target_shifts"] > 1:
                w["target_shifts"] -= 1
                remaining_overflow -= 1
                logging.info(
                    f"[TargetValidation] {w['id']}: further reduced to {w['target_shifts']} (residual rounding)"
                )

    new_sum = sum(w.get("target_shifts", 0) for w in workers_data)
    logging.info(f"[TargetValidation] After scaling: sum of targets={new_sum}, available slots={total_slots}")


def get_effective_assignments(
    worker_id: str,
    worker_assignments: dict[str, set],
    prior_assignments: dict[str, set],
    start_date: "datetime",
) -> set:
    """
    Return the merged set of prior-period dates AND current-period dates for a
    worker.  Used by constraint checkers that need cross-period visibility (gap
    constraint, consecutive-weekend constraint).

    Only prior assignments within the lookback window (90 days before the new
    period start) are included to avoid stale data from very old periods
    disturbing constraints.
    """
    from datetime import timedelta

    current = worker_assignments.get(worker_id, set())
    prior = prior_assignments.get(worker_id, set())
    if not prior:
        return current
    lookback_cutoff = start_date - timedelta(days=90)
    relevant_prior = {d for d in prior if d >= lookback_cutoff}
    return current | relevant_prior


def get_prior_weekend_count(
    worker_id: str,
    prior_weekend_counts: dict[str, int],
) -> int:
    """Return the prior-period weekend count for a worker."""
    return prior_weekend_counts.get(worker_id, 0)


def get_effective_weekend_count(
    worker_id: str,
    prior_weekend_counts: dict[str, int],
    worker_weekend_counts: dict[str, int],
) -> int:
    """Return prior-period weekend count + current-period weekend count."""
    return prior_weekend_counts.get(worker_id, 0) + worker_weekend_counts.get(worker_id, 0)
