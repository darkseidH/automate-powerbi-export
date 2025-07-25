# exporters/base.py
"""Base exporter interface."""

import os
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional
from rich.progress import Progress


class BaseExporter(ABC):
    """Abstract base class for data exporters."""
    
    def __init__(self, output_dir: str):
        """
        Initialize exporter with output directory.
        
        Args:
            output_dir: Base directory for exports
        """
        self.output_dir = output_dir
        self._ensure_directory()
    
    def _ensure_directory(self):
        """Create output directory if it doesn't exist."""
        os.makedirs(self.output_dir, exist_ok=True)
    
    @abstractmethod
    def export(
        self, 
        df: pd.DataFrame, 
        filename: str,
        progress: Optional[Progress] = None
    ) -> str:
        """
        Export DataFrame to file.
        
        Args:
            df: DataFrame to export
            filename: Base filename (without extension)
            progress: Optional progress tracker
            
        Returns:
            Full path to exported file
        """
        pass
    
    @property
    @abstractmethod
    def file_extension(self) -> str:
        """Get file extension for this exporter."""
        pass
    
    def get_full_path(self, filename: str) -> str:
        """
        Get full file path with extension.
        
        Args:
            filename: Base filename
            
        Returns:
            Complete file path
        """
        return os.path.join(
            self.output_dir, 
            f"{filename}.{self.file_extension}"
        )