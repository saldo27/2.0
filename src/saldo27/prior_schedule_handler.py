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
from datetime import datetime, timedelta
from typing import Any


def _is_weekend_or_holiday(date: datetime, holidays: set[datetime]) -> bool:
    """Return True if date is Fri/Sat/Sun, a holiday, or the day before a holiday."""
    return date.weekday() >= 4 or date in holidays or (date + timedelta(days=1)) in holidays


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
        except Exception:
            pass
    result["holidays"] = prior_holidays

    # Merge with new-period holidays for weekend detection near boundary
    all_holidays = prior_holidays | (new_period_holidays or set())

    # ── 5. Build per-worker stats ──────────────────────────────────────────────
    for worker_id, date_list in worker_assignments_raw.items():
        dates: set[datetime] = set()
        for d_raw in date_list:
            try:
                dates.add(datetime.fromisoformat(str(d_raw).split("T")[0]))
            except Exception:
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
