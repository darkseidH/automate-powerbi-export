# exporters/csv_exporter.py
"""CSV export implementation."""

import pandas as pd
from typing import Optional
from rich.progress import Progress
from .base import BaseExporter


class CSVExporter(BaseExporter):
    """Exports DataFrames to CSV format with chunking support."""
    
    def __init__(self, output_dir: str, chunk_size: int = 100_000):
        """
        Initialize CSV exporter.
        
        Args:
            output_dir: Output directory path
            chunk_size: Rows per chunk for large files
        """
        super().__init__(output_dir)
        self.chunk_size = chunk_size
    
    @property
    def file_extension(self) -> str:
        """CSV file extension."""
        return "csv"
    
    def export(
        self, 
        df: pd.DataFrame, 
        filename: str,
        progress: Optional[Progress] = None
    ) -> str:
        """
        Export DataFrame to CSV with optional chunking.
        
        Args:
            df: DataFrame to export
            filename: Base filename
            progress: Optional progress tracker
            
        Returns:
            Full path to exported file
        """
        filepath = self.get_full_path(filename)
        
        # Check if chunking is needed
        if len(df) > self.chunk_size:
            self._export_chunked(df, filepath, progress)
        else:
            df.to_csv(filepath, index=False)
        
        return filepath
    
    def _export_chunked(
        self, 
        df: pd.DataFrame, 
        filepath: str, 
        progress: Optional[Progress]
    ):
        """Export large DataFrame in chunks."""
        chunks = [
            df[i:i + self.chunk_size] 
            for i in range(0, len(df), self.chunk_size)
        ]
        
        # Create progress task if available
        chunk_task = None
        if progress:
            chunk_task = progress.add_task(
                "[yellow]Writing CSV chunks", 
                total=len(chunks)
            )
        
        # Write chunks
        for i, chunk in enumerate(chunks):
            mode = 'w' if i == 0 else 'a'
            header = i == 0
            chunk.to_csv(filepath, mode=mode, header=header, index=False)
            
            if progress and chunk_task:
                progress.advance(chunk_task)
        
        # Cleanup progress task
        if progress and chunk_task:
            progress.remove_task(chunk_task)