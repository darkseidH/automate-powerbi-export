# core/data_processor.py
"""Data processing and conversion utilities."""

from typing import Optional

import numpy as np
import pandas as pd
from rich.progress import Progress


class DataProcessor:
    """Handles .NET DataTable to Pandas DataFrame conversion."""

    @staticmethod
    def convert_to_dataframe(
            datatable,
            progress: Optional[Progress] = None
    ) -> pd.DataFrame:
        """
        Convert .NET DataTable to Pandas DataFrame.
        
        Args:
            datatable: .NET DataTable instance
            progress: Optional progress tracker
            
        Returns:
            Pandas DataFrame
        """
        from System import DBNull, DateTime
        import datetime

        # Extract column names
        cols = [col.ColumnName for col in datatable.Columns]
        row_count = datatable.Rows.Count

        # Initialize numpy array
        data = np.empty((row_count, len(cols)), dtype=object)

        # Type references for performance
        dbnull_type = type(DBNull.Value)
        datetime_type = type(DateTime.Now)

        # Progress tracking setup
        conversion_task = None
        if progress and row_count > 0:
            conversion_task = progress.add_task(
                "[magenta]Converting .NET DataTable to NumPy array",
                total=row_count
            )

        # Batch size for progress updates
        batch_size = max(1, row_count // 1000)

        # Convert rows
        for i, dr in enumerate(datatable.Rows):
            for j, item in enumerate(dr.ItemArray):
                if isinstance(item, dbnull_type):
                    data[i, j] = None
                elif isinstance(item, datetime_type):
                    data[i, j] = datetime.datetime(
                        item.Year,
                        item.Month,
                        item.Day,
                        item.Hour,
                        item.Minute,
                        item.Second,
                        item.Millisecond * 1000
                    )
                else:
                    data[i, j] = item

            # Update progress
            if progress and conversion_task:
                if (i + 1) % batch_size == 0 or i == row_count - 1:
                    progress.update(conversion_task, completed=i + 1)

        # Cleanup progress task
        if progress and conversion_task:
            progress.remove_task(conversion_task)

        return pd.DataFrame(data, columns=cols)
