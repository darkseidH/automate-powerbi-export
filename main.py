# main.py
"""Main orchestrator for PowerBI 12-month data export pipeline."""

import gc
import logging
from datetime import datetime
from typing import List, Tuple, Optional

from config import Config
from core import PowerBIConnection, QueryExecutor, DataProcessor
from exporters import CSVExporter, ParquetExporter
from utils import DateManager, ProgressTracker
from logger.logger import Logger


class PowerBIExportPipeline:
    """Orchestrates the complete export pipeline."""
    
    def __init__(self):
        """Initialize pipeline components."""
        self.config = Config()
        self.tracker = ProgressTracker()
        self.date_manager = DateManager()
        self.query_executor = QueryExecutor(self.config)
        self.data_processor = DataProcessor()
        
        # Initialize exporters
        self.csv_exporter = CSVExporter(self.config.csv_dir)
        self.parquet_exporter = ParquetExporter(self.config.parquet_dir)
        
        # Setup logging
        Logger.setup(name="PowerBI_Export", level=logging.INFO)
        
        # Track failed months for retry
        self.failed_months: List[Tuple[int, int, str]] = []
        
    def run(self, end_year: Optional[int] = None, end_month: Optional[int] = None):
        """
        Execute the complete 12-month export pipeline.
        
        Args:
            end_year: Optional ending year (defaults to current)
            end_month: Optional ending month (defaults to current)
        """
        self.tracker.print_header("PowerBI 12-Month Rolling Data Export Pipeline")
        
        # Determine date range
        if not end_year or not end_month:
            current_date = datetime.now()
            end_year = current_date.year
            end_month = current_date.month
        
        # Calculate months to process
        months_to_process = self.date_manager.calculate_12_month_range(
            end_year, end_month
        )
        
        self._print_configuration(months_to_process)
        
        # Execute pipeline with progress tracking
        with self.tracker.create_progress_bar() as progress:
            total_rows, total_memory = self._execute_pipeline(
                months_to_process, progress
            )
        
        self._print_summary(months_to_process, total_rows, total_memory)
        
        # Handle retries if needed
        if self.failed_months:
            self._retry_failed_months()
    
    def _execute_pipeline(
        self, 
        months: List[Tuple[int, int]], 
        progress
    ) -> Tuple[int, float]:
        """
        Execute main pipeline processing.
        
        Args:
            months: List of (year, month) tuples to process
            progress: Progress bar instance
            
        Returns:
            Tuple of (total_rows, total_memory_mb)
        """
        # Main progress task
        main_task = progress.add_task(
            "[bold blue]PowerBI 12-Month Export Process",
            total=len(months)
        )
        
        total_rows = 0
        total_memory = 0.0
        
        # Process each month with separate connection
        for year, month in months:
            month_name = self.date_manager.get_month_name(year, month)
            
            progress.update(
                main_task,
                description=f"[blue]Processing {month_name}"
            )
            
            rows, memory = self._process_month(
                year, month, month_name, progress
            )
            
            total_rows += rows
            total_memory += memory
            progress.advance(main_task)
        
        return total_rows, total_memory
    
    def _process_month(
        self,
        year: int,
        month: int,
        month_name: str,
        progress
    ) -> Tuple[int, float]:
        """
        Process a single month's data extraction.
        
        Args:
            year: Year to process
            month: Month to process
            month_name: Formatted month name
            progress: Progress instance
            
        Returns:
            Tuple of (row_count, memory_mb)
        """
        try:
            # Create fresh connection for this month
            with PowerBIConnection(self.config) as conn:
                self.tracker.print_processing(month_name)
                
                # Get date range and format query
                year, month, day_start, day_end = self.date_manager.get_month_range(
                    year, month
                )
                
                query_params = {
                    'year': year,
                    'month': month,
                    'day_start': day_start,
                    'day_end': day_end
                }
                
                dax_query = self.query_executor.format_query(query_params)
                
                # Execute query
                datatable = self.query_executor.execute(
                    conn.connection, dax_query
                )
                
                # Convert to DataFrame
                df = self.data_processor.convert_to_dataframe(
                    datatable, progress
                )
                
                # Check for empty results
                if len(df) == 0:
                    self.tracker.print_warning(f"No data found for {month_name}")
                    return 0, 0.0
                
                # Calculate metrics
                row_count = len(df)
                memory_mb = df.memory_usage(deep=True).sum() / 1024**2
                
                self.tracker.print_success(
                    f"Retrieved: [green]{row_count:,} rows[/green] ({memory_mb:.1f} MB)"
                )
                
                # Export data
                self._export_data(df, year, month, day_start, day_end, progress)
                
                # Cleanup
                del df
                gc.collect()
                
                return row_count, memory_mb
                
        except Exception as e:
            error_msg = str(e)
            self.tracker.print_error(f"Error processing {month_name}: {error_msg}")
            Logger.error(f"Failed to process {month_name}: {error_msg}")
            self.failed_months.append((year, month, error_msg))
            return 0, 0.0
    
    def _export_data(
        self,
        df,
        year: int,
        month: int,
        day_start: int,
        day_end: int,
        progress
    ):
        """Export DataFrame to multiple formats."""
        # Generate filename
        date_suffix = self.date_manager.format_filename_date(
            year, month, day_start, day_end
        )
        filename = f"billing_cases_{date_suffix}"
        
        # Create export task
        export_task = progress.add_task(
            "[green]File Export Operations",
            total=2
        )
        
        # Export to CSV
        progress.update(export_task, description="[green]Exporting to CSV")
        csv_path = self.csv_exporter.export(df, filename, progress)
        self.tracker.print_info(f"📄 CSV exported: [bold]{csv_path}[/bold]")
        progress.advance(export_task)
        
        # Export to Parquet
        progress.update(export_task, description="[green]Exporting to Parquet")
        parquet_path = self.parquet_exporter.export(df, filename, progress)
        self.tracker.print_info(f"📦 Parquet exported: [bold]{parquet_path}[/bold]")
        progress.advance(export_task)
        
        progress.remove_task(export_task)
    
    def _retry_failed_months(self):
        """Retry processing for failed months."""
        self.tracker.print_header("Retrying Failed Months")
        
        retry_count = len(self.failed_months)
        self.tracker.print_info(f"Attempting to retry {retry_count} failed months...")
        
        # Clear failed months list and retry
        failed_months_copy = self.failed_months.copy()
        self.failed_months.clear()
        
        with self.tracker.create_progress_bar() as progress:
            retry_task = progress.add_task(
                "[yellow]Retrying failed months",
                total=retry_count
            )
            
            for year, month, _ in failed_months_copy:
                month_name = self.date_manager.get_month_name(year, month)
                self._process_month(year, month, month_name, progress)
                progress.advance(retry_task)
    
    def _print_configuration(self, months: List[Tuple[int, int]]):
        """Print export configuration details."""
        self.tracker.print_info("📊 [bold]Export Configuration:[/bold]")
        self.tracker.print_info(
            f"   • Period: {months[0][0]}-{months[0][1]:02d} to "
            f"{months[-1][0]}-{months[-1][1]:02d}"
        )
        self.tracker.print_info(f"   • Total months: {len(months)}")
        self.tracker.print_info("")
    
    def _print_summary(
        self,
        months: List[Tuple[int, int]],
        total_rows: int,
        total_memory: float
    ):
        """Print final execution summary."""
        successful = len(months) - len(self.failed_months)
        
        self.tracker.print_info("\n📊 [bold]Export Summary:[/bold]")
        self.tracker.print_info(
            f"   • Successful months: [green]{successful}/{len(months)}[/green]"
        )
        self.tracker.print_info(
            f"   • Total rows exported: [green]{total_rows:,}[/green]"
        )
        self.tracker.print_info(
            f"   • Total data processed: [yellow]{total_memory:.1f} MB[/yellow]"
        )
        self.tracker.print_info(
            f"   • Output location: [blue]{self.config.output_dir}[/blue]"
        )
        
        if self.failed_months:
            self.tracker.print_warning(
                f"\n   • Failed months: {len(self.failed_months)}"
            )
            for year, month, error in self.failed_months:
                month_name = self.date_manager.get_month_name(year, month)
                self.tracker.print_error(f"     - {month_name}: {error[:50]}...")


def main():
    """Entry point for the application."""
    pipeline = PowerBIExportPipeline()
    
    try:
        # Run with default settings (current month as endpoint)
        pipeline.run()
        
        # Or specify custom endpoint:
        # pipeline.run(end_year=2025, end_month=7)
        
    except Exception as e:
        pipeline.tracker.print_error(f"[bold red]Pipeline error: {str(e)}[/bold red]")
        Logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        pipeline.tracker.print_info(
            "\n🎉 [bold green]PowerBI export pipeline completed![/bold green]"
        )


if __name__ == "__main__":
    main()