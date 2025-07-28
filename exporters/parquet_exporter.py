# exporters/parquet_exporter.py
"""Parquet export implementation."""

from typing import Optional

import pandas as pd
from rich.progress import Progress

from .base import BaseExporter


class ParquetExporter(BaseExporter):
    """Exports DataFrames to Parquet format with compression."""

    def __init__(
            self,
            output_dir: str,
            compression: str = 'snappy',
            engine: str = 'pyarrow'
    ):
        """
        Initialize Parquet exporter.
        
        Args:
            output_dir: Output directory path
            compression: Compression algorithm ('snappy', 'gzip', 'brotli')
            engine: Parquet engine ('pyarrow' or 'fastparquet')
        """
        super().__init__(output_dir)
        self.compression = compression
        self.engine = engine

    @property
    def file_extension(self) -> str:
        """Parquet file extension."""
        return "parquet"

    def export(
            self,
            df: pd.DataFrame,
            filename: str,
            progress: Optional[Progress] = None
    ) -> str:
        """
        Export DataFrame to Parquet format.
        
        Args:
            df: DataFrame to export
            filename: Base filename
            progress: Optional progress tracker
            
        Returns:
            Full path to exported file
        """
        filepath = self.get_full_path(filename)

        df.to_parquet(
            filepath,
            index=False,
            compression=self.compression,
            engine=self.engine
        )

        return filepath
