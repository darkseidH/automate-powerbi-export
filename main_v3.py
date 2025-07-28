# main_v3.py
"""Main orchestrator for PowerBI 12-month data export pipeline."""

import gc
import logging
import os
import time
from datetime import datetime
from typing import List, Tuple, Optional

from config import Config
from core import PowerBIConnection, QueryExecutor, DataProcessor
from exporters import CSVExporter, ParquetExporter
from exporters.excel_exporter import ExcelExporter
from logger.logger import Logger
from utils import DateManager, ProgressTracker, RetryManager, StateManager


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
        self.excel_exporter = ExcelExporter(self.config.excel_dir)

        # Setup logging
        Logger.setup(name="PowerBI_Export", level=logging.INFO)

        # Initialize retry and state management
        self.retry_manager = RetryManager(max_retry_attempts=5, retry_delay_seconds=30)
        self.state_manager = StateManager(
            state_file=os.path.join(self.config.base_dir, "retry_state.json")
        )

        # Load any previous retry state
        if self.state_manager.load_retry_state(self.retry_manager):
            self.tracker.print_warning(
                f"Loaded {len(self.retry_manager.failed_months)} failed months from previous run"
            )

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

        # Save state after initial pass
        self.state_manager.save_retry_state(self.retry_manager)

        # Execute continuous retry loop
        self._execute_retry_loop()

        # Final summary
        self._print_summary(months_to_process, total_rows, total_memory)

        # Save execution log
        summary = self.retry_manager.get_summary()
        self.state_manager.save_execution_log(
            {
                "end_date": f"{end_year}-{end_month:02d}",
                "total_months": len(months_to_process),
                "summary": summary,
            }
        )

        # Clear state if all successful
        if not self.retry_manager.failed_months:
            self.state_manager.clear_state()
            self.tracker.print_success("All months processed successfully!")

    def _execute_pipeline(
            self, months: List[Tuple[int, int]], progress
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
            "[bold blue]PowerBI 12-Month Export Process", total=len(months)
        )

        total_rows = 0
        total_memory = 0.0

        # Process each month with separate connection
        for year, month in months:
            month_name = self.date_manager.get_month_name(year, month)

            progress.update(main_task, description=f"[blue]Processing {month_name}")

            rows, memory = self._process_month(year, month, month_name, progress)

            total_rows += rows
            total_memory += memory
            progress.advance(main_task)

        return total_rows, total_memory

    def _process_month(
            self, year: int, month: int, month_name: str, progress
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
                    "year": year,
                    "month": month,
                    "day_start": day_start,
                    "day_end": day_end,
                }

                dax_query = self.query_executor.format_query(query_params)

                # Execute query
                datatable = self.query_executor.execute(conn.connection, dax_query)

                # Convert to DataFrame
                df = self.data_processor.convert_to_dataframe(datatable, progress)

                # Check for empty results
                if len(df) == 0:
                    self.tracker.print_warning(f"No data found for {month_name}")
                    return 0, 0.0

                # Calculate metrics
                row_count = len(df)
                memory_mb = df.memory_usage(deep=True).sum() / 1024 ** 2

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
            self.retry_manager.add_failure(year, month, error_msg)
            return 0, 0.0

    def _export_data(
            self, df, year: int, month: int, day_start: int, day_end: int, progress
    ):
        """Export DataFrame to multiple formats."""
        # Generate filename
        date_suffix = self.date_manager.format_filename_date(
            year, month, day_start, day_end
        )
        filename = f"billing_cases_{date_suffix}"

        # Create export task
        export_task = progress.add_task("[green]File Export Operations", total=2)

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

        # Export to Excel
        progress.update(export_task, description="[green]Exporting to Excel")
        excel_path = self.excel_exporter.export(df, filename, progress)
        self.tracker.print_info(f"📊 Excel exported: [bold]{excel_path}[/bold]")
        progress.advance(export_task)

        progress.remove_task(export_task)

    def _execute_retry_loop(self):
        """Execute continuous retry loop for failed months."""
        retry_count = 0
        max_global_retries = 10  # Prevent infinite loops

        while retry_count < max_global_retries:
            # Get months that need retry
            retry_candidates = self.retry_manager.get_failed_months_for_retry()

            if not retry_candidates:
                # No more months to retry
                break

            retry_count += 1
            self.tracker.print_header(f"Retry Attempt #{retry_count}")
            self.tracker.print_info(
                f"Retrying {len(retry_candidates)} failed months..."
            )

            # Save current state before retry
            self.state_manager.save_retry_state(self.retry_manager)

            with self.tracker.create_progress_bar() as progress:
                retry_task = progress.add_task(
                    "[yellow]Retrying failed months", total=len(retry_candidates)
                )

                for failed_month in retry_candidates:
                    month_name = self.date_manager.get_month_name(
                        failed_month.year, failed_month.month
                    )

                    # Get retry strategy
                    strategy = self.retry_manager.get_retry_strategy(failed_month)

                    # Wait if needed
                    if strategy["wait_before_retry"]:
                        delay = self.retry_manager.get_retry_delay(failed_month)
                        self.tracker.print_info(
                            f"Waiting {delay}s before retrying {month_name}..."
                        )
                        time.sleep(delay)

                    # Clear memory if needed
                    if strategy["clear_memory"]:
                        gc.collect()
                        time.sleep(5)  # Give system time to release memory

                    # Retry with custom strategy
                    rows, memory = self._process_month_with_strategy(
                        failed_month.year,
                        failed_month.month,
                        month_name,
                        progress,
                        strategy,
                    )

                    # Check if successful
                    if rows > 0:
                        self.retry_manager.remove_success(
                            failed_month.year, failed_month.month
                        )
                        self.tracker.print_success(
                            f"Successfully recovered {month_name}!"
                        )

                    progress.advance(retry_task)

            # Check if all succeeded
            if not self.retry_manager.failed_months:
                self.tracker.print_success("All months successfully processed!")
                break

            # Print interim summary
            summary = self.retry_manager.get_summary()
            self.tracker.print_info(f"\nRetry Summary:")
            self.tracker.print_info(f"  • Still failed: {summary['total_failures']}")
            self.tracker.print_info(f"  • Retry eligible: {summary['retry_eligible']}")

            # If no more retry eligible, stop
            if summary["retry_eligible"] == 0:
                self.tracker.print_warning(
                    "No more months eligible for retry (max attempts reached)"
                )
                break

    def _process_month_with_strategy(
            self, year: int, month: int, month_name: str, progress, strategy: dict
    ) -> Tuple[int, float]:
        """
        Process month with custom retry strategy.

        Args:
            year: Year to process
            month: Month to process
            month_name: Formatted month name
            progress: Progress instance
            strategy: Retry strategy parameters

        Returns:
            Tuple of (row_count, memory_mb)
        """
        try:
            # Create connection with custom timeouts
            custom_config = Config()
            custom_config._connect_timeout = strategy["connect_timeout"]
            custom_config._command_timeout = strategy["command_timeout"]

            with PowerBIConnection(custom_config) as conn:
                self.tracker.print_processing(f"{month_name} (Retry)")

                # Get date range and format query
                year, month, day_start, day_end = self.date_manager.get_month_range(
                    year, month
                )

                query_params = {
                    "year": year,
                    "month": month,
                    "day_start": day_start,
                    "day_end": day_end,
                }

                dax_query = self.query_executor.format_query(query_params)

                # Execute query
                datatable = self.query_executor.execute(conn.connection, dax_query)

                # Convert to DataFrame
                df = self.data_processor.convert_to_dataframe(datatable, progress)

                # Check for empty results
                if len(df) == 0:
                    self.tracker.print_warning(f"No data found for {month_name}")
                    return 0, 0.0

                # Calculate metrics
                row_count = len(df)
                memory_mb = df.memory_usage(deep=True).sum() / 1024 ** 2

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
            self.tracker.print_error(f"Retry failed for {month_name}: {error_msg}")
            Logger.error(f"Retry failed for {month_name}: {error_msg}")
            self.retry_manager.add_failure(year, month, error_msg)
            return 0, 0.0

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
            self, months: List[Tuple[int, int]], total_rows: int, total_memory: float
    ):
        """Print final execution summary."""
        summary = self.retry_manager.get_summary()
        successful = len(months) - summary["total_failures"]

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

        if summary["total_failures"] > 0:
            self.tracker.print_warning(
                f"\n   • Failed months: {summary['total_failures']}"
            )

            # Show error breakdown
            if "error_breakdown" in summary:
                self.tracker.print_info("   • Error types:")
                for error_type, count in summary["error_breakdown"].items():
                    self.tracker.print_info(f"     - {error_type}: {count}")

            # Show permanent failures
            if summary["permanent_failures"] > 0:
                self.tracker.print_error(
                    f"   • Permanent failures (max retries exceeded): "
                    f"{summary['permanent_failures']}"
                )
                for month_str in summary.get("max_attempts_reached", []):
                    self.tracker.print_error(f"     - {month_str}")


def main():
    """Entry point for the application."""
    pipeline = PowerBIExportPipeline()
    end_year = input("Enter end year (default current year): ")
    end_year = int(end_year) if end_year else None
    end_month = input("Enter end month (default current month): ")
    end_month = int(end_month) if end_month else None

    try:
        # Run with default settings (current month as endpoint)
        pipeline.run(end_month=end_month, end_year=end_year)

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
