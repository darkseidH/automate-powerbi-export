# utils/date_manager.py
"""Date range calculation utilities."""

import calendar
from typing import List, Tuple


class DateManager:
    """Manages date calculations for monthly data extraction."""

    @staticmethod
    def get_month_range(year: int, month: int) -> Tuple[int, int, int, int]:
        """
        Calculate start and end dates for a given month.
        
        Args:
            year: Target year
            month: Target month (1-12)
            
        Returns:
            Tuple of (year, month, day_start, day_end)
        """
        day_start = 1
        day_end = calendar.monthrange(year, month)[1]
        return year, month, day_start, day_end

    @staticmethod
    def calculate_12_month_range(
            end_year: int,
            end_month: int
    ) -> List[Tuple[int, int]]:
        """
        Calculate 12 months backward from given end date.
        
        Args:
            end_year: Ending year
            end_month: Ending month (1-12)
            
        Returns:
            List of (year, month) tuples in chronological order
        """
        months = []

        # Generate 12 months going backwards
        for i in range(12):
            year = end_year
            month = end_month - i

            # Handle year boundary
            while month <= 0:
                month += 12
                year -= 1

            months.append((year, month))

        # Return in chronological order
        months.reverse()
        return months

    @staticmethod
    def get_month_name(year: int, month: int) -> str:
        """
        Get formatted month name.
        
        Args:
            year: Year
            month: Month (1-12)
            
        Returns:
            Formatted string like "January 2025"
        """
        return f"{calendar.month_name[month]} {year}"

    @staticmethod
    def format_filename_date(
            year: int,
            month: int,
            day_start: int,
            day_end: int
    ) -> str:
        """
        Format date components for filename.
        
        Args:
            year: Year
            month: Month
            day_start: Start day
            day_end: End day
            
        Returns:
            Formatted string like "2025_01_01_31"
        """
        return f"{year}_{month:02d}_{day_start:02d}_{day_end:02d}"
