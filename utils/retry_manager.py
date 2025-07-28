# utils/retry_manager.py
"""Retry management for failed month extractions."""

from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from typing import List, Dict, Tuple


class ErrorType(Enum):
    """Categorization of error types for retry strategy."""

    SESSION_EXPIRED = "session_expired"
    CONNECTION_TIMEOUT = "connection_timeout"
    MEMORY_ERROR = "memory_error"
    DATA_ERROR = "data_error"
    UNKNOWN = "unknown"


@dataclass
class FailedMonth:
    """Tracks detailed information about failed month extractions."""

    year: int
    month: int
    error_message: str
    error_type: ErrorType
    attempt_count: int = 1
    last_attempt_time: datetime = None
    first_error_time: datetime = None

    def __post_init__(self):
        """Initialize timestamps if not provided."""
        if self.last_attempt_time is None:
            self.last_attempt_time = datetime.now()
        if self.first_error_time is None:
            self.first_error_time = datetime.now()

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data["error_type"] = self.error_type.value
        data["last_attempt_time"] = self.last_attempt_time.isoformat()
        data["first_error_time"] = self.first_error_time.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict) -> "FailedMonth":
        """Create instance from dictionary."""
        data["error_type"] = ErrorType(data["error_type"])
        data["last_attempt_time"] = datetime.fromisoformat(data["last_attempt_time"])
        data["first_error_time"] = datetime.fromisoformat(data["first_error_time"])
        return cls(**data)


class RetryManager:
    """Manages retry logic for failed month extractions."""

    def __init__(self, max_retry_attempts: int = 5, retry_delay_seconds: int = 30):
        """
        Initialize retry manager.

        Args:
            max_retry_attempts: Maximum retry attempts per month
            retry_delay_seconds: Base delay between retry attempts
        """
        self.max_retry_attempts = max_retry_attempts
        self.retry_delay_seconds = retry_delay_seconds
        self.failed_months: Dict[Tuple[int, int], FailedMonth] = {}

    def add_failure(self, year: int, month: int, error_message: str):
        """
        Add or update a failed month entry.

        Args:
            year: Failed year
            month: Failed month
            error_message: Error message from exception
        """
        error_type = self._categorize_error(error_message)
        key = (year, month)

        if key in self.failed_months:
            # Update existing failure
            failed = self.failed_months[key]
            failed.attempt_count += 1
            failed.last_attempt_time = datetime.now()
            failed.error_message = error_message
            failed.error_type = error_type
        else:
            # Create new failure entry
            self.failed_months[key] = FailedMonth(
                year=year,
                month=month,
                error_message=error_message,
                error_type=error_type,
            )

    def _categorize_error(self, error_message: str) -> ErrorType:
        """
        Categorize error type based on error message.

        Args:
            error_message: Error message string

        Returns:
            ErrorType enum value
        """
        error_lower = error_message.lower()

        # Session errors
        if any(
                keyword in error_lower
                for keyword in [
                    "session",
                    "expired",
                    "session id cannot be found",
                    "session does not exist",
                ]
        ):
            return ErrorType.SESSION_EXPIRED

        # Connection/Timeout errors
        elif any(
                keyword in error_lower
                for keyword in [
                    "timeout",
                    "timed out",
                    "connection lost",
                    "connection either timed out",
                ]
        ):
            return ErrorType.CONNECTION_TIMEOUT

        # Memory errors
        elif any(
                keyword in error_lower
                for keyword in [
                    "memory",
                    "out of memory",
                    "memoryerror",
                    "insufficient memory",
                ]
        ):
            return ErrorType.MEMORY_ERROR

        # Data errors
        elif any(
                keyword in error_lower for keyword in ["data", "dataset", "query execution"]
        ):
            return ErrorType.DATA_ERROR

        else:
            return ErrorType.UNKNOWN

    def get_retry_delay(self, failed_month: FailedMonth) -> int:
        """
        Calculate retry delay based on error type and attempt count.

        Args:
            failed_month: FailedMonth instance

        Returns:
            Delay in seconds before next retry
        """
        base_delay = self.retry_delay_seconds

        # Different delays for different error types
        if failed_month.error_type == ErrorType.SESSION_EXPIRED:
            # Quick retry for session errors
            return min(base_delay, 10)

        elif failed_month.error_type == ErrorType.CONNECTION_TIMEOUT:
            # Longer delay for timeout issues
            return base_delay * failed_month.attempt_count

        elif failed_month.error_type == ErrorType.MEMORY_ERROR:
            # Much longer delay for memory issues to allow cleanup
            return base_delay * 2 * failed_month.attempt_count

        else:
            # Standard delay for other errors
            return base_delay

    def should_retry(self, year: int, month: int) -> bool:
        """
        Check if a month should be retried.

        Args:
            year: Year to check
            month: Month to check

        Returns:
            True if should retry, False otherwise
        """
        key = (year, month)
        if key not in self.failed_months:
            return False

        failed = self.failed_months[key]
        return failed.attempt_count < self.max_retry_attempts

    def get_retry_strategy(self, failed_month: FailedMonth) -> Dict[str, any]:
        """
        Get retry strategy parameters based on error type.

        Args:
            failed_month: FailedMonth instance

        Returns:
            Dictionary with retry parameters
        """
        strategy = {
            "connect_timeout": 30,
            "command_timeout": 600,
            "wait_before_retry": True,
            "clear_memory": False,
        }

        if failed_month.error_type == ErrorType.SESSION_EXPIRED:
            # Fresh connection with standard timeouts
            strategy["wait_before_retry"] = False

        elif failed_month.error_type == ErrorType.CONNECTION_TIMEOUT:
            # Increase timeouts significantly
            strategy["connect_timeout"] = 60 * (failed_month.attempt_count + 1)
            strategy["command_timeout"] = 1800 * (failed_month.attempt_count + 1)

        elif failed_month.error_type == ErrorType.MEMORY_ERROR:
            # Clear memory and use smaller batch processing
            strategy["clear_memory"] = True
            strategy["command_timeout"] = 300  # Shorter timeout to fail fast

        return strategy

    def get_failed_months_for_retry(self) -> List[FailedMonth]:
        """
        Get list of failed months that should be retried.

        Returns:
            List of FailedMonth instances eligible for retry
        """
        return [
            failed
            for failed in self.failed_months.values()
            if self.should_retry(failed.year, failed.month)
        ]

    def get_permanent_failures(self) -> List[FailedMonth]:
        """
        Get list of months that exceeded max retry attempts.

        Returns:
            List of FailedMonth instances that won't be retried
        """
        return [
            failed
            for failed in self.failed_months.values()
            if failed.attempt_count >= self.max_retry_attempts
        ]

    def remove_success(self, year: int, month: int):
        """
        Remove a month from failed list after successful retry.

        Args:
            year: Successful year
            month: Successful month
        """
        key = (year, month)
        if key in self.failed_months:
            del self.failed_months[key]

    def clear(self):
        """Clear all failed months."""
        self.failed_months.clear()

    def get_summary(self) -> Dict[str, any]:
        """
        Get summary statistics of failures.

        Returns:
            Dictionary with failure statistics
        """
        if not self.failed_months:
            return {"total_failures": 0}

        error_counts = {}
        for failed in self.failed_months.values():
            error_type = failed.error_type.value
            error_counts[error_type] = error_counts.get(error_type, 0) + 1

        return {
            "total_failures": len(self.failed_months),
            "permanent_failures": len(self.get_permanent_failures()),
            "retry_eligible": len(self.get_failed_months_for_retry()),
            "error_breakdown": error_counts,
            "max_attempts_reached": [
                f"{f.year}-{f.month:02d}" for f in self.get_permanent_failures()
            ],
        }
