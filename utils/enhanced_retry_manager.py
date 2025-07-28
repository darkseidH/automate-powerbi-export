# utils/enhanced_retry_manager.py
"""Enhanced retry manager that understands processing stages."""

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from core.types import ProcessingStage, MonthResult
from utils.retry_manager import ErrorType, RetryManager


@dataclass
class EnhancedFailedMonth:
    """Enhanced tracking of failed months with stage information."""
    year: int
    month: int
    error_message: str
    error_type: ErrorType
    failed_stage: ProcessingStage
    attempt_count: int = 1
    last_attempt_time: datetime = field(default_factory=datetime.now)
    first_error_time: datetime = field(default_factory=datetime.now)
    export_success: bool = False
    validation_success: bool = False
    exported_file_path: Optional[str] = None  # Path to exported data if available

    def should_retry_full(self) -> bool:
        """Determine if full retry is needed."""
        return self.failed_stage == ProcessingStage.EXPORT or not self.export_success

    def should_retry_validation_only(self) -> bool:
        """Determine if only validation retry is needed."""
        return (
                self.failed_stage == ProcessingStage.VALIDATION
                and self.export_success
                and self.exported_file_path is not None
        )


class EnhancedRetryManager(RetryManager):
    """Enhanced retry manager with stage-aware retry logic."""

    def __init__(self, max_retry_attempts: int = 5, retry_delay_seconds: int = 30):
        """Initialize enhanced retry manager."""
        super().__init__(max_retry_attempts, retry_delay_seconds)
        self.enhanced_failed_months: Dict[Tuple[int, int], EnhancedFailedMonth] = {}

    def add_failure_from_result(self, result: MonthResult, exported_path: Optional[str] = None):
        """Add failure from MonthResult."""
        key = (result.year, result.month)
        error_type = self._categorize_error(result.error_message or "Unknown error")

        if key in self.enhanced_failed_months:
            # Update existing failure
            failed = self.enhanced_failed_months[key]
            failed.attempt_count += 1
            failed.last_attempt_time = datetime.now()
            failed.error_message = result.error_message or failed.error_message
            failed.error_type = error_type
            failed.failed_stage = result.last_stage
            failed.export_success = result.export_success
            failed.validation_success = result.validation_success
            if exported_path:
                failed.exported_file_path = exported_path
        else:
            # Create new failure entry
            self.enhanced_failed_months[key] = EnhancedFailedMonth(
                year=result.year,
                month=result.month,
                error_message=result.error_message or "Unknown error",
                error_type=error_type,
                failed_stage=result.last_stage,
                export_success=result.export_success,
                validation_success=result.validation_success,
                exported_file_path=exported_path
            )

    def get_validation_only_retries(self) -> List[EnhancedFailedMonth]:
        """Get months that only need validation retry."""
        return [
            failed for failed in self.enhanced_failed_months.values()
            if failed.should_retry_validation_only()
               and failed.attempt_count < self.max_retry_attempts
        ]

    def get_full_retries(self) -> List[EnhancedFailedMonth]:
        """Get months that need full retry."""
        return [
            failed for failed in self.enhanced_failed_months.values()
            if failed.should_retry_full()
               and failed.attempt_count < self.max_retry_attempts
        ]

    def remove_success_enhanced(self, year: int, month: int):
        """Remove a successfully processed month."""
        key = (year, month)
        if key in self.enhanced_failed_months:
            del self.enhanced_failed_months[key]
        # Also remove from base class
        self.remove_success(year, month)

    def get_exported_file_path(self, year: int, month: int) -> Optional[str]:
        """Get the exported file path for a failed month."""
        key = (year, month)
        failed = self.enhanced_failed_months.get(key)
        if failed and failed.exported_file_path and os.path.exists(failed.exported_file_path):
            return failed.exported_file_path
        return None

    def get_enhanced_summary(self) -> Dict:
        """Get enhanced summary with stage information."""
        base_summary = self.get_summary()

        validation_only_failures = len(self.get_validation_only_retries())
        full_retry_needed = len(self.get_full_retries())

        base_summary.update({
            'validation_only_failures': validation_only_failures,
            'full_retry_needed': full_retry_needed,
            'stage_breakdown': {
                ProcessingStage.EXPORT.value: len([
                    f for f in self.enhanced_failed_months.values()
                    if f.failed_stage == ProcessingStage.EXPORT
                ]),
                ProcessingStage.VALIDATION.value: len([
                    f for f in self.enhanced_failed_months.values()
                    if f.failed_stage == ProcessingStage.VALIDATION
                ])
            }
        })

        return base_summary
