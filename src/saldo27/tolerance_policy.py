"""Shift-count tolerance policy shared by every builder, optimizer and validator.

Two levels, both measured on non-mandatory shifts against ``target_shifts``:

- **Hard limit**: a percentage of the target (10 % strict, 13 % relaxed), scaled
  down for part-time workers to a minimum of 5 %. Builders never assign past it
  and validators flag anything outside it.
- **Balance band**: the ±1 shift the balance optimizers aim for, never wider than
  the hard limit allows for that target.

Manual workers (``auto_calculate_shifts=False``) have zero tolerance at both levels.

Monthly: the non-mandatory shifts of a month may not pass ``monthly_cap`` (month
target + 1 for auto workers, exact for manual ones), and manual workers are
paced through the month with ``monthly_pace_cap``.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable
    from datetime import datetime

STRICT_TOLERANCE = 0.10
RELAXED_TOLERANCE = 0.13
MIN_PART_TIME_TOLERANCE = 0.05
BALANCE_BAND = 1
MONTHLY_MARGIN = 1


def is_manual(worker: dict[str, Any]) -> bool:
    return not worker.get("auto_calculate_shifts", True)


def tolerance_fraction(worker: dict[str, Any], *, relaxed: bool = True) -> float:
    """Hard-limit tolerance as a fraction of the target."""
    if is_manual(worker):
        return 0.0
    fraction = RELAXED_TOLERANCE if relaxed else STRICT_TOLERANCE
    try:
        work_percentage = float(worker.get("work_percentage", 100))
    except (TypeError, ValueError):
        work_percentage = 100.0
    if work_percentage < 100:
        fraction = max(MIN_PART_TIME_TOLERANCE, fraction * work_percentage / 100.0)
    return fraction


def shift_bounds(worker: dict[str, Any], target: float, *, relaxed: bool = True) -> tuple[int, int]:
    """Hard (min, max) non-mandatory shifts allowed for ``target``."""
    if target <= 0:
        return (0, 0)
    fraction = tolerance_fraction(worker, relaxed=relaxed)
    # round() absorbs float noise such as 30 * 0.9 == 27.000000000000004
    low = max(0, math.ceil(round(target * (1 - fraction), 9)))
    high = int(round(target * (1 + fraction), 9))
    return (low, high)


def max_shifts(worker: dict[str, Any], target: float, *, relaxed: bool = True) -> int:
    return shift_bounds(worker, target, relaxed=relaxed)[1]


def balance_band(worker: dict[str, Any], target: float, band: int = BALANCE_BAND) -> int:
    """Deviation (in shifts) the balance optimizers accept around ``target``."""
    if is_manual(worker) or target <= 0:
        return 0
    low, high = shift_bounds(worker, target, relaxed=True)
    room = min(high - target, target - low)
    return max(0, min(band, math.floor(room)))


def monthly_ceiling(worker: dict[str, Any], monthly_target: float) -> float:
    """Most non-mandatory shifts a worker may hold in one month."""
    if is_manual(worker):
        return monthly_target
    return monthly_target + MONTHLY_MARGIN


def monthly_cap(worker: dict[str, Any], year: int, month: int) -> float | None:
    """Monthly ceiling from the worker's ``monthly_targets`` (non-mandatory budget).

    Auto workers get the month target + ``MONTHLY_MARGIN`` whatever their
    percentage; manual workers exactly the month target. None when there is no
    month target, and for fixed-cadence workers, who have no monthly limit.
    """
    if worker.get("has_cadence"):
        return None
    target = (worker.get("monthly_targets") or {}).get(f"{year}-{month:02d}")
    if target is None:
        return None
    return monthly_ceiling(worker, target)


def exceeds_monthly_cap(
    worker: dict[str, Any], date: datetime, assignments: Iterable[datetime], mandatory_dates: Collection[datetime]
) -> bool:
    """True if holding a non-mandatory shift on ``date`` puts that month over its cap.

    Mandatory shifts never count. ``assignments`` may already include ``date``.
    """
    if date in mandatory_dates:
        return False
    cap = monthly_cap(worker, date.year, date.month)
    if cap is None:
        return False
    held = sum(
        1
        for d in assignments
        if d != date and d.year == date.year and d.month == date.month and d not in mandatory_dates
    )
    return held + 1 > cap


def monthly_pace_cap(monthly_target: float, day: int, last_available_day: int) -> int:
    """Most non-mandatory shifts allowed by ``day`` when pacing ``monthly_target`` evenly.

    The month is spread up to the worker's last available day, so someone who
    leaves mid-month can still reach the whole target. At least 1 when the
    target is positive.
    """
    if monthly_target <= 0:
        return 0
    if last_available_day <= 0:
        return math.ceil(monthly_target)
    return max(1, math.ceil(monthly_target * min(day, last_available_day) / last_available_day))
