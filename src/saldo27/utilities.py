# Imports
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

from saldo27.performance_cache import cached


def get_effective_min_gap(worker_data: dict | None, gap_between_shifts: int) -> int:
    """Return the effective minimum gap (calendar days) between shifts for a worker.

    Three tiers based on worker type:
    - Auto workers at 100%+ work: gap_between_shifts - 1 (relaxed)
    - Manual workers with ≤3 guardias/mes OR any worker with <60% work: gap_between_shifts + 1 (strict)
    - Everyone else: gap_between_shifts (standard)

    Returns at least 1 (minimum 1 calendar day between shifts).
    """
    if not worker_data:
        return gap_between_shifts

    work_percentage = worker_data.get("work_percentage", 100)
    is_auto = worker_data.get("auto_calculate_shifts", True)

    # Rule B (strict): work <60% — applies to ALL workers
    if work_percentage < 60:
        return gap_between_shifts + 1

    # Rule B (strict): manual workers with ≤3 guardias/mes
    if not is_auto:
        guardias_mes = worker_data.get("_original_target_shifts", worker_data.get("target_shifts", 0))
        if guardias_mes <= 3:
            return gap_between_shifts + 1

    # Rule A (relaxed): auto workers at 100%+
    if is_auto and work_percentage >= 100:
        return max(1, gap_between_shifts - 1)

    # Standard
    return gap_between_shifts


def numeric_sort_key(item):
    """
    Attempts to convert the first element of a tuple (the key) to an integer
    for sorting. Returns a tuple to prioritize numeric keys and handle errors.
    item[0] is assumed to be the worker ID (key).
    """
    try:
        return (0, int(item[0]))  # (0, numeric_value) - sorts numbers first
    except (ValueError, TypeError):
        return (1, item[0])  # (1, original_string) - sorts non-numbers after numbers


class DateTimeUtils:
    """Date and time utility functions"""

    # Methods
    def __init__(self):
        """Initialize the date/time utilities with performance optimizations"""
        # Cache for frequently computed values
        self._weekend_cache = {}
        self._holiday_cache = {}
        self._month_cache = {}

        logging.info("DateTimeUtils initialized with performance optimizations")

    @cached(ttl=86400)  # Cache for 24 hours
    def get_spain_time(self):
        """Get current time in Spain timezone with fallback options (cached)"""
        try:
            response = requests.get("http://worldtimeapi.org/api/timezone/Europe/Madrid", timeout=5, verify=True)

            if response.status_code == 200:
                time_data = response.json()
                return datetime.fromisoformat(time_data["datetime"]).replace(tzinfo=None)

        except (requests.RequestException, ValueError) as e:
            logging.warning(f"Error getting time from API: {e!s}")

        try:
            spain_tz = ZoneInfo("Europe/Madrid")
            return datetime.now(spain_tz).replace(tzinfo=None)
        except Exception as e:
            logging.error(f"Fallback time error: {e!s}")
            return datetime.utcnow()

    def parse_dates(self, date_str: str) -> list[datetime]:
        """
        Parse semicolon-separated individual dates only.
        Use parse_date_ranges() for date ranges.

        Args:
            date_str: String with individual dates separated by semicolons
                     Example: '05-03-2026;10-03-2026;15-03-2026'

        Returns:
            List of datetime objects
        """
        if not date_str:
            return []

        dates = []
        # Split once and process efficiently
        date_parts = [part.strip() for part in date_str.split(";") if part.strip()]

        for date_text in date_parts:
            try:
                dates.append(datetime.strptime(date_text, "%d-%m-%Y"))
            except ValueError as e:
                logging.warning(f"Invalid date format '{date_text}' - {e!s}")
        return dates

    def parse_date_ranges(self, date_ranges_str: str) -> list[tuple[datetime, datetime]]:
        """Parse semicolon-separated date ranges (optimized)"""
        if not date_ranges_str:
            return []

        ranges = []
        # Process all ranges efficiently
        range_parts = [part.strip() for part in date_ranges_str.split(";") if part.strip()]

        for date_range in range_parts:
            try:
                if " - " in date_range:
                    start_str, end_str = date_range.split(" - ", 1)  # Split only once
                    start = datetime.strptime(start_str.strip(), "%d-%m-%Y")
                    end = datetime.strptime(end_str.strip(), "%d-%m-%Y")
                    ranges.append((start, end))
                else:
                    date = datetime.strptime(date_range, "%d-%m-%Y")
                    ranges.append((date, date))
            except ValueError as e:
                logging.warning(f"Invalid date range format '{date_range}' - {e!s}")
        return ranges

    def is_holiday(self, date: datetime, holidays_list: list[datetime] | None = None) -> bool:
        """
        Check if a date is a holiday

        Args:
            date: Date to check
            holidays_list: Optional list of holiday dates to check against

        Returns:
            bool: True if date is a holiday
        """
        if holidays_list is None:
            holidays_list = []  # Default to empty list if not provided

        # Use set for O(1) lookup instead of list
        holidays_set = set(holidays_list) if not isinstance(holidays_list, set) else holidays_list

        return date in holidays_set

    def is_pre_holiday(self, date: datetime, holidays_list: list[datetime] | None = None) -> bool:
        """
        Check if a date is the day before a holiday

        Args:
            date: Date to check
            holidays_list: Optional list of holiday dates to check against

        Returns:
            bool: True if date is the day before a holiday
        """
        if holidays_list is None:
            holidays_list = []  # Default to empty list if not provided

        # Use set for O(1) lookup instead of list
        holidays_set = set(holidays_list) if not isinstance(holidays_list, set) else holidays_list

        # Check if the next day is a holiday
        next_day = date + timedelta(days=1)
        return next_day in holidays_set

    def is_weekend_day(self, date: datetime, holidays_list: list[datetime] | None = None) -> bool:
        """
        Check if a date is a weekend day or holiday (optimized with caching)

        Args:
            date: Date to check
            holidays_list: Optional list of holiday dates to check against

        Returns:
            bool: True if date is a weekend day (Fri, Sat, Sun) or holiday
        """
        if holidays_list is None:
            holidays_list = []  # Default to empty list if not provided

        # Use set for O(1) lookup instead of list
        holidays_set = set(holidays_list) if not isinstance(holidays_list, set) else holidays_list

        # Check if it's Friday, Saturday or Sunday
        if date.weekday() >= 4:  # 4=Friday, 5=Saturday, 6=Sunday
            return True

        # Check if it's a holiday
        if date in holidays_set:
            return True

        # Check if it's a day before holiday (treated as special in some parts of the code)
        next_day = date + timedelta(days=1)
        if next_day in holidays_set:
            return True

        return False

    def get_weekend_start(self, date, holidays=None):
        """
        Get the start date (Friday) of the weekend containing this date

        Args:
            date: datetime object
            holidays: optional list of holidays
        Returns:
            datetime: Friday date of the weekend (or holiday start)
        """
        if self.is_pre_holiday(date, holidays):
            return date
        elif self.is_holiday(date, holidays):
            return date - timedelta(days=1)
        else:
            # Regular weekend - get to Friday
            weekday = date.weekday()
            if weekday < 4:  # Monday-Thursday
                return date + timedelta(days=4 - weekday)  # Move forward to Friday
            else:  # Friday-Sunday
                return date - timedelta(days=weekday - 4)  # Move back to Friday

    def get_effective_weekday(self, date, holidays=None):
        """
        Get the effective weekday, treating holidays as Sundays and pre-holidays as Fridays

        Args:
            date: datetime object
            holidays: optional list of holidays
        Returns:
            int: 0-6 representing Monday-Sunday, with holidays as 6 and pre-holidays as 4
        """
        if self.is_holiday(date, holidays):
            return 6  # Sunday
        if self.is_pre_holiday(date, holidays):
            return 4  # Friday
        return date.weekday()

    def identify_bridge_periods(self, holidays: list[datetime], year: int) -> list[dict]:
        """
        Identify bridge periods (puentes) where a holiday is adjacent to a weekend.

        A bridge period is defined as:
        - Holiday Thursday: Thu-Fri-Sat-Sun
        - Holiday Friday: Fri-Sat-Sun
        - Holiday Monday: Fri-Sat-Sun-Mon
        - Holiday Tuesday: Fri-Sat-Sun-Mon-Tue

        Args:
            holidays: List of holiday dates
            year: Year to process

        Returns:
            List of dictionaries with bridge period information:
            [{
                'id': 'bridge_YYYY-MM-DD',
                'start_date': datetime,
                'end_date': datetime,
                'holiday': datetime,
                'type': 'thursday'|'friday'|'monday'|'tuesday'
            }]
        """
        if not holidays:
            return []

        bridge_periods = []
        holidays_set = set(holidays)

        for holiday in holidays:
            # Only process holidays in the specified year
            if holiday.year != year:
                continue

            weekday = holiday.weekday()  # 0=Monday, 1=Tuesday, ..., 6=Sunday

            # Holiday on Thursday (3) -> Bridge: Thu-Fri-Sat-Sun
            if weekday == 3:
                start_date = holiday
                end_date = holiday + timedelta(days=3)  # Sunday
                bridge_periods.append(
                    {
                        "id": f"bridge_{holiday.strftime('%Y-%m-%d')}",
                        "start_date": start_date,
                        "end_date": end_date,
                        "holiday": holiday,
                        "type": "thursday",
                    }
                )

            # Holiday on Friday (4) -> Bridge: Fri-Sat-Sun
            elif weekday == 4:
                start_date = holiday
                end_date = holiday + timedelta(days=2)  # Sunday
                bridge_periods.append(
                    {
                        "id": f"bridge_{holiday.strftime('%Y-%m-%d')}",
                        "start_date": start_date,
                        "end_date": end_date,
                        "holiday": holiday,
                        "type": "friday",
                    }
                )

            # Holiday on Monday (0) -> Bridge: Fri-Sat-Sun-Mon
            elif weekday == 0:
                start_date = holiday - timedelta(days=3)  # Friday
                end_date = holiday
                bridge_periods.append(
                    {
                        "id": f"bridge_{holiday.strftime('%Y-%m-%d')}",
                        "start_date": start_date,
                        "end_date": end_date,
                        "holiday": holiday,
                        "type": "monday",
                    }
                )

            # Holiday on Tuesday (1) -> Bridge: Fri-Sat-Sun-Mon-Tue
            elif weekday == 1:
                start_date = holiday - timedelta(days=4)  # Friday
                end_date = holiday
                bridge_periods.append(
                    {
                        "id": f"bridge_{holiday.strftime('%Y-%m-%d')}",
                        "start_date": start_date,
                        "end_date": end_date,
                        "holiday": holiday,
                        "type": "tuesday",
                    }
                )

        logging.info(f"Identified {len(bridge_periods)} bridge periods for year {year}")
        for bp in bridge_periods:
            logging.debug(
                f"  Bridge {bp['type']}: {bp['start_date'].strftime('%Y-%m-%d')} to {bp['end_date'].strftime('%Y-%m-%d')}"
            )

        return bridge_periods

    def get_bridge_period_for_date(self, date: datetime, bridge_periods: list[dict]) -> dict | None:
        """
        Get the bridge period that contains the given date.

        Args:
            date: Date to check
            bridge_periods: List of bridge period dictionaries from identify_bridge_periods()

        Returns:
            Bridge period dict if date is within a bridge period, None otherwise
        """
        if not bridge_periods:
            return None

        for bridge in bridge_periods:
            if bridge["start_date"] <= date <= bridge["end_date"]:
                return bridge

        return None

    def is_bridge_day(self, date: datetime, bridge_periods: list[dict]) -> bool:
        """
        Check if a date is part of any bridge period.

        Args:
            date: Date to check
            bridge_periods: List of bridge period dictionaries from identify_bridge_periods()

        Returns:
            True if date is within a bridge period, False otherwise
        """
        return self.get_bridge_period_for_date(date, bridge_periods) is not None
