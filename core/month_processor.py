# core/month_processor.py
"""Handles processing of individual months with smart retry logic."""

import gc
from typing import Optional

import pandas as pd

from config import Config
from core import PowerBIConnection, QueryExecutor, DataProcessor
from core.types import ProcessingStage, MonthResult
from exporters import CSVExporter, ParquetExporter
from exporters.excel_exporter import ExcelExporter
from logger.logger import Logger
from utils import DateManager, ProgressTracker
from utils.validation_manager import ValidationManager


class MonthProcessor:
    """Processes individual months with export and validation."""

    def __init__(
            self,
            config: Config,
            query_executor: QueryExecutor,
            data_processor: DataProcessor,
            validation_manager: ValidationManager,
            tracker: ProgressTracker
    ):
        """Initialize month processor with dependencies."""
        self.config = config
        self.query_executor = query_executor
        self.data_processor = data_processor
        self.validation_manager = validation_manager
        self.tracker = tracker
        self.date_manager = DateManager()

        # Initialize exporters
        self.csv_exporter = CSVExporter(config.csv_dir)
        self.parquet_exporter = ParquetExporter(config.parquet_dir)
        self.excel_exporter = ExcelExporter(config.excel_dir)

        # Load validation query
        self.validation_query_template = self._load_validation_query()

    def _load_validation_query(self) -> str:
        """Load validation query template."""
        import os
        query_path = os.path.join(
            self.config.base_dir, "queries", "total_per_month_query.dax"
        )
        try:
            with open(query_path, 'r', encoding='utf-8') as f:
                return f.read()
        except:
            # Fallback to inline query
            return """DEFINE
    VAR __DS0FilterTable =
        FILTER (
            KEEPFILTERS ( VALUES ( 'Billing Cases'[ProcessingDate] ) ),
            AND (
                'Billing Cases'[ProcessingDate] >= DATE ( {year}, {month}, {day_start} ),
                'Billing Cases'[ProcessingDate]
                    <= ( DATE ( {year}, {month}, {day_end} ) + TIME ( 0, 0, 1 ) )
            )
        )

EVALUATE
SUMMARIZECOLUMNS (
    __DS0FilterTable,
    "SumAmountInEuro", IGNORE ( CALCULATE ( SUM ( 'Billing Cases - Costs'[AmountInEuro] ) ) )
)"""

    def process_month(
            self,
            year: int,
            month: int,
            progress,
            retry_stage: Optional[ProcessingStage] = None,
            existing_data_path: Optional[str] = None
    ) -> MonthResult:
        """
        Process a single month with smart retry logic.

        Args:
            year: Year to process
            month: Month to process
            progress: Progress tracker
            retry_stage: If retrying, which stage to retry from
            existing_data_path: Path to existing data if only retrying validation

        Returns:
            MonthResult with processing details
        """
        month_name = self.date_manager.get_month_name(year, month)
        result = MonthResult(year=year, month=month)

        try:
            with PowerBIConnection(self.config) as conn:
                # Determine what needs to be done
                if retry_stage == ProcessingStage.VALIDATION and existing_data_path:
                    # Only retry validation
                    self.tracker.print_info(f"🔄 Retrying validation only for {month_name}")
                    df = self._load_existing_data(existing_data_path)
                    result.export_success = True
                    result.rows = len(df)
                    result.memory_mb = df.memory_usage(deep=True).sum() / 1024 ** 2
                else:
                    # Full processing (export + validation)
                    self.tracker.print_processing(month_name)
                    df = self._extract_and_export(conn, year, month, result, progress)
                    if df is None:
                        return result

                # Validation stage
                result.last_stage = ProcessingStage.VALIDATION
                self._validate_data(conn.connection, df, year, month, month_name)
                result.validation_success = True
                result.last_stage = ProcessingStage.COMPLETE

                # Cleanup
                del df
                gc.collect()

                return result

        except Exception as e:
            result.error_message = str(e)
            self.tracker.print_error(f"Error in {result.last_stage.value} for {month_name}: {str(e)}")
            Logger.error(f"Failed {result.last_stage.value} for {month_name}: {str(e)}")
            return result

    def _extract_and_export(
            self,
            conn: PowerBIConnection,
            year: int,
            month: int,
            result: MonthResult,
            progress
    ) -> Optional[pd.DataFrame]:
        """Extract data and export to files."""
        # Get date range
        year, month, day_start, day_end = self.date_manager.get_month_range(year, month)
        month_name = self.date_manager.get_month_name(year, month)

        query_params = {
            "year": year,
            "month": month,
            "day_start": day_start,
            "day_end": day_end,
        }

        # Execute query
        dax_query = self.query_executor.format_query(query_params)
        datatable = self.query_executor.execute(conn.connection, dax_query)
        df = self.data_processor.convert_to_dataframe(datatable, progress)

        # Check for empty results
        if len(df) == 0:
            self.tracker.print_warning(f"No data found for {month_name}")
            result.export_success = True  # Not a failure, just no data
            return None

        # Update metrics
        result.rows = len(df)
        result.memory_mb = df.memory_usage(deep=True).sum() / 1024 ** 2

        self.tracker.print_success(
            f"Retrieved: [green]{result.rows:,} rows[/green] ({result.memory_mb:.1f} MB)"
        )

        # Export data
        self._export_data(df, year, month, day_start, day_end, progress)
        result.export_success = True

        return df

    def _export_data(
            self,
            df: pd.DataFrame,
            year: int,
            month: int,
            day_start: int,
            day_end: int,
            progress
    ):
        """Export DataFrame to multiple formats."""
        date_suffix = self.date_manager.format_filename_date(
            year, month, day_start, day_end
        )
        filename = f"billing_cases_{date_suffix}"

        export_task = progress.add_task("[green]File Export Operations", total=3)

        # CSV
        progress.update(export_task, description="[green]Exporting to CSV")
        csv_path = self.csv_exporter.export(df, filename, progress)
        self.tracker.print_info(f"📄 CSV exported: [bold]{csv_path}[/bold]")
        progress.advance(export_task)

        # Parquet
        progress.update(export_task, description="[green]Exporting to Parquet")
        parquet_path = self.parquet_exporter.export(df, filename, progress)
        self.tracker.print_info(f"📦 Parquet exported: [bold]{parquet_path}[/bold]")
        progress.advance(export_task)

        # Excel
        progress.update(export_task, description="[green]Exporting to Excel")
        excel_path = self.excel_exporter.export(df, filename, progress)
        self.tracker.print_info(f"📊 Excel exported: [bold]{excel_path}[/bold]")
        progress.advance(export_task)

        progress.remove_task(export_task)

    def _validate_data(
            self,
            connection,
            df: pd.DataFrame,
            year: int,
            month: int,
            month_name: str
    ):
        """Validate data against DAX query."""
        self.tracker.print_info(f"\n🔍 Running validation for {month_name}...")

        # Get date range
        _, _, day_start, day_end = self.date_manager.get_month_range(year, month)

        # Format and execute validation query
        validation_query = self.validation_query_template.format(
            year=year, month=month,
            day_start=day_start, day_end=day_end
        )

        validation_datatable = self.query_executor.execute(connection, validation_query)
        validation_df = self.data_processor.convert_to_dataframe(validation_datatable)

        # Extract sums
        dax_sum = 0.0
        if len(validation_df) > 0:
            # Try different column name formats
            for col in ['[SumAmountInEuro]', 'SumAmountInEuro']:
                if col in validation_df.columns:
                    dax_sum = float(validation_df[col].iloc[0])
                    break

        # Calculate DataFrame sum
        dataframe_sum = self._calculate_dataframe_sum(df)

        # Perform validation
        self.validation_manager.validate_month(
            year, month, dax_sum, dataframe_sum, len(df), month_name
        )

    def _calculate_dataframe_sum(self, df: pd.DataFrame) -> float:
        """Calculate sum from DataFrame with multiple column name attempts."""
        possible_columns = [
            '[SumAmountInEuro]', 'SumAmountInEuro',
            'AmountInEuro', '[AmountInEuro]',
            'Amount', 'amount'
        ]

        for col in possible_columns:
            if col in df.columns:
                return float(df[col].sum())

        # Try to find any column with 'amount' in the name
        amount_cols = [col for col in df.columns if 'amount' in col.lower()]
        if amount_cols:
            return float(df[amount_cols[0]].sum())

        self.tracker.print_warning("No amount column found for validation")
        return 0.0

    def _load_existing_data(self, file_path: str) -> pd.DataFrame:
        """Load existing data for validation retry."""
        if file_path.endswith('.parquet'):
            return pd.read_parquet(file_path)
        elif file_path.endswith('.csv'):
            return pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
