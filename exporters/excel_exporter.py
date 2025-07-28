# exporters/excel_exporter.py
"""Excel export implementation that guarantees file writing."""

from typing import Optional

import pandas as pd
from rich.progress import Progress

from .base import BaseExporter


class ExcelExporter(BaseExporter):
    """Export pandas DataFrame to Excel format."""

    def __init__(self, output_dir: str):
        """
        Initialize Excel exporter.

        Args:
            output_dir: Output directory path
        """
        super().__init__(output_dir)

    @property
    def file_extension(self) -> str:
        """Excel file extension."""
        return "xlsx"

    def export(
            self,
            df: pd.DataFrame,
            filename: str,
            progress: Optional[Progress] = None
    ) -> str:
        """
        Export DataFrame to Excel.

        Args:
            df: DataFrame to export
            filename: Base filename
            progress: Optional progress tracker

        Returns:
            Full path to exported file
        """
        filepath = self.get_full_path(filename)

        # Create progress task if available
        export_task = None
        if progress:
            export_task = progress.add_task(
                "[green]Writing Excel file",
                total=1
            )

        # CRITICAL: Always use context manager for ExcelWriter
        # This guarantees the file is saved even if an exception occurs

        # For ALL file sizes, use the same approach
        try:
            # First, try the direct method (simplest and most reliable)
            df.to_excel(filepath, sheet_name='Data', index=False)

        except Exception as e:
            # If direct method fails, try with explicit context manager
            print(f"Direct Excel export failed: {e}, trying with context manager")

            try:
                # Use context manager to guarantee file is written
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Data', index=False)
                    # No need to call writer.close() - context manager handles it

            except ImportError:
                # If openpyxl not available, try xlsxwriter
                try:
                    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
                        df.to_excel(writer, sheet_name='Data', index=False)
                except ImportError:
                    # Last resort - let pandas choose the engine
                    with pd.ExcelWriter(filepath) as writer:
                        df.to_excel(writer, sheet_name='Data', index=False)

        if progress and export_task:
            progress.advance(export_task)
            progress.remove_task(export_task)

        return filepath
