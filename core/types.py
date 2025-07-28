# core/types.py
"""Shared type definitions to avoid circular imports."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ProcessingStage(Enum):
    """Stages of month processing for retry logic."""
    EXPORT = "export"
    VALIDATION = "validation"
    COMPLETE = "complete"


@dataclass
class MonthResult:
    """Result of processing a month."""
    year: int
    month: int
    rows: int = 0
    memory_mb: float = 0.0
    export_success: bool = False
    validation_success: bool = False
    last_stage: ProcessingStage = ProcessingStage.EXPORT
    error_message: Optional[str] = None
