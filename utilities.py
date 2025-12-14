# Imports
from datetime import datetime, timedelta
import calendar
import logging
import requests
from zoneinfo import ZoneInfo
from typing import List, Tuple, Optional, Set
from functools import lru_cache
from performance_cache import cached, memoize

def numeric_sort_key(item):
    """
    Attempts to convert the first element of a tuple (the key) to an integer
    for sorting. Returns a tuple to prioritize numeric keys and handle errors.
    item[0] is assumed to be the worker ID (key).
    """
    try:
        return (0, int(item[0])) # (0, numeric_value) - sorts numbers first
    except (ValueError, TypeError):
        return (1, item[0]) # (1, original_string) - sorts non-numbers after numbers

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
            response = requests.get(
                'http://worldtimeapi.org/api/timezone/Europe/Madrid',
                timeout=5,
                verify=True
            )
            
            if response.status_code == 200:
                time_data = response.json()
                return datetime.fromisoformat(time_data['datetime']).replace(tzinfo=None)
                
        except (requests.RequestException, ValueError) as e:
            logging.warning(f"Error getting time from API: {str(e)}")

        try:
            spain_tz = ZoneInfo('Europe/Madrid')
            return datetime.now(spain_tz).replace(tzinfo=None)
        except Exception as e:
            logging.error(f"Fallback time error: {str(e)}")
            return datetime.utcnow()
        
    def parse_dates(self, date_str: str) -> List[datetime]:
        """Parse semicolon-separated dates (optimized)"""
        if not date_str:
            return []

        dates = []
        # Split once and process efficiently
        date_parts = [part.strip() for part in date_str.split(';') if part.strip()]
        
        for date_text in date_parts:
            try:
                dates.append(datetime.strptime(date_text, '%d-%m-%Y'))
            except ValueError as e:
                logging.warning(f"Invalid date format '{date_text}' - {str(e)}")
        return dates

    def parse_date_ranges(self, date_ranges_str: str) -> List[Tuple[datetime, datetime]]:
        """Parse semicolon-separated date ranges (optimized)"""
        if not date_ranges_str:
            return []

        ranges = []
        # Process all ranges efficiently
        range_parts = [part.strip() for part in date_ranges_str.split(';') if part.strip()]
        
        for date_range in range_parts:
            try:
                if ' - ' in date_range:
                    start_str, end_str = date_range.split(' - ', 1)  # Split only once
                    start = datetime.strptime(start_str.strip(), '%d-%m-%Y')
                    end = datetime.strptime(end_str.strip(), '%d-%m-%Y')
                    ranges.append((start, end))
                else:
                    date = datetime.strptime(date_range, '%d-%m-%Y')
                    ranges.append((date, date))
            except ValueError as e:
                logging.warning(f"Invalid date range format '{date_range}' - {str(e)}")
        return ranges
    
    def is_holiday(self, date: datetime, holidays_list: Optional[List[datetime]] = None) -> bool:
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
    
    def is_pre_holiday(self, date: datetime, holidays_list: Optional[List[datetime]] = None) -> bool:
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
    
    def is_weekend_day(self, date: datetime, holidays_list: Optional[List[datetime]] = None) -> bool:
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
