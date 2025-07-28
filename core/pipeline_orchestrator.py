# core/pipeline_orchestrator.py
"""Main pipeline orchestrator - simplified and focused."""

import os
import time
from typing import List, Tuple, Optional

from config import Config
from core import QueryExecutor, DataProcessor
from core.month_processor import MonthProcessor
from core.types import ProcessingStage
from utils import DateManager, ProgressTracker, StateManager
from utils.enhanced_retry_manager import EnhancedRetryManager
from utils.validation_manager import ValidationManager


class PipelineOrchestrator:
    """Orchestrates the complete export pipeline."""

    def __init__(self):
        """Initialize pipeline components."""
        # Core components
        self.config = Config()
        self.tracker = ProgressTracker()
        self.date_manager = DateManager()

        # Processing components
        self.query_executor = QueryExecutor(self.config)
        self.data_processor = DataProcessor()
        self.validation_manager = ValidationManager(self.config.output_dir)

        # Month processor
        self.month_processor = MonthProcessor(
            config=self.config,
            query_executor=self.query_executor,
            data_processor=self.data_processor,
            validation_manager=self.validation_manager,
            tracker=self.tracker
        )

        # Retry and state management
        self.retry_manager = EnhancedRetryManager(
            max_retry_attempts=5,
            retry_delay_seconds=30
        )
        self.state_manager = StateManager(
            state_file=os.path.join(self.config.base_dir, "retry_state.json")
        )

        # Statistics
        self.total_rows = 0
        self.total_memory = 0.0

    def run(self, end_year: Optional[int] = None, end_month: Optional[int] = None):
        """Execute the pipeline."""
        self.tracker.print_header("PowerBI 12-Month Rolling Data Export Pipeline")

        # Determine date range
        months_to_process = self._get_months_to_process(end_year, end_month)
        self._print_configuration(months_to_process)

        # Main processing
        self._process_all_months(months_to_process)

        # Smart retry loop
        self._execute_smart_retry_loop()

        # Save reports
        self._save_reports()

        # Final summary
        self._print_final_summary(months_to_process)

    def _get_months_to_process(
            self,
            end_year: Optional[int],
            end_month: Optional[int]
    ) -> List[Tuple[int, int]]:
        """Get the list of months to process."""
        if not end_year or not end_month:
            from datetime import datetime
            current_date = datetime.now()
            end_year = current_date.year
            end_month = current_date.month

        return self.date_manager.calculate_12_month_range(end_year, end_month)

    def _process_all_months(self, months: List[Tuple[int, int]]):
        """Process all months with progress tracking."""
        with self.tracker.create_progress_bar() as progress:
            main_task = progress.add_task(
                "[bold blue]PowerBI 12-Month Export Process",
                total=len(months)
            )

            for year, month in months:
                month_name = self.date_manager.get_month_name(year, month)
                progress.update(main_task, description=f"[blue]Processing {month_name}")

                # Process month
                result = self.month_processor.process_month(year, month, progress)

                # Handle result
                self._handle_month_result(result)

                progress.advance(main_task)

    def _handle_month_result(self, result):
        """Handle the result of processing a month."""
        if result.last_stage == ProcessingStage.COMPLETE:
            # Success
            self.total_rows += result.rows
            self.total_memory += result.memory_mb
            self.tracker.print_success(
                f"✅ {self.date_manager.get_month_name(result.year, result.month)} completed"
            )
        else:
            # Failure - determine exported file path
            exported_path = self._get_exported_file_path(result.year, result.month)
            self.retry_manager.add_failure_from_result(result, exported_path)

    def _get_exported_file_path(self, year: int, month: int) -> Optional[str]:
        """Get the path to exported parquet file if it exists."""
        # Build expected filename
        _, _, day_start, day_end = self.date_manager.get_month_range(year, month)
        date_suffix = self.date_manager.format_filename_date(
            year, month, day_start, day_end
        )
        filename = f"billing_cases_{date_suffix}.parquet"
        filepath = os.path.join(self.config.parquet_dir, filename)

        return filepath if os.path.exists(filepath) else None

    def _execute_smart_retry_loop(self):
        """Execute smart retry loop with stage awareness."""
        retry_count = 0
        max_global_retries = 10

        while retry_count < max_global_retries:
            # Get failures by type
            validation_only = self.retry_manager.get_validation_only_retries()
            full_retries = self.retry_manager.get_full_retries()

            if not validation_only and not full_retries:
                break

            retry_count += 1
            self.tracker.print_header(f"Smart Retry Attempt #{retry_count}")

            # Retry validation-only failures first (faster)
            if validation_only:
                self._retry_validation_only(validation_only)

            # Then retry full failures
            if full_retries:
                self._retry_full_months(full_retries)

            # Check if all succeeded
            if not self.retry_manager.enhanced_failed_months:
                self.tracker.print_success("All months successfully processed!")
                break

    def _retry_validation_only(self, failures):
        """Retry only validation for months where export succeeded."""
        self.tracker.print_info(
            f"🔄 Retrying validation only for {len(failures)} months..."
        )

        with self.tracker.create_progress_bar() as progress:
            task = progress.add_task(
                "[yellow]Validation-only retries",
                total=len(failures)
            )

            for failed in failures:
                month_name = self.date_manager.get_month_name(
                    failed.year, failed.month
                )

                # Get exported file path
                file_path = failed.exported_file_path
                if not file_path or not os.path.exists(file_path):
                    # Fall back to full retry if file not found
                    self.tracker.print_warning(
                        f"Exported file not found for {month_name}, will retry full"
                    )
                    continue

                # Retry validation only
                result = self.month_processor.process_month(
                    year=failed.year,
                    month=failed.month,
                    progress=progress,
                    retry_stage=ProcessingStage.VALIDATION,
                    existing_data_path=file_path
                )

                if result.validation_success:
                    self.retry_manager.remove_success_enhanced(
                        failed.year, failed.month
                    )
                    self.tracker.print_success(
                        f"✅ Validation succeeded for {month_name}"
                    )

                progress.advance(task)

    def _retry_full_months(self, failures):
        """Retry complete processing for failed months."""
        self.tracker.print_info(
            f"🔄 Retrying full processing for {len(failures)} months..."
        )

        with self.tracker.create_progress_bar() as progress:
            task = progress.add_task(
                "[yellow]Full month retries",
                total=len(failures)
            )

            for failed in failures:
                # Apply retry strategy
                strategy = self.retry_manager.get_retry_strategy(failed)

                if strategy['wait_before_retry']:
                    delay = self.retry_manager.get_retry_delay(failed)
                    self.tracker.print_info(f"Waiting {delay}s before retry...")
                    time.sleep(delay)

                # Retry full month
                result = self.month_processor.process_month(
                    year=failed.year,
                    month=failed.month,
                    progress=progress
                )

                if result.last_stage == ProcessingStage.COMPLETE:
                    self.retry_manager.remove_success_enhanced(
                        failed.year, failed.month
                    )
                    self.total_rows += result.rows
                    self.total_memory += result.memory_mb
                else:
                    # Update failure
                    self._handle_month_result(result)

                progress.advance(task)

    def _save_reports(self):
        """Save validation and execution reports."""
        # Save validation report
        if self.validation_manager.validation_results:
            self.tracker.print_header("Saving Validation Report")
            self.validation_manager.save_validation_report()

        # Save retry state
        self.state_manager.save_retry_state(self.retry_manager)

        # Save execution log
        summary = self.retry_manager.get_enhanced_summary()
        self.state_manager.save_execution_log({
            'summary': summary,
            'total_rows': self.total_rows,
            'total_memory_mb': self.total_memory
        })

    def _print_configuration(self, months: List[Tuple[int, int]]):
        """Print pipeline configuration."""
        self.tracker.print_info("📊 [bold]Export Configuration:[/bold]")
        self.tracker.print_info(
            f"   • Period: {months[0][0]}-{months[0][1]:02d} to "
            f"{months[-1][0]}-{months[-1][1]:02d}"
        )
        self.tracker.print_info(f"   • Total months: {len(months)}")
        self.tracker.print_info("")

    def _print_final_summary(self, months: List[Tuple[int, int]]):
        """Print final execution summary."""
        summary = self.retry_manager.get_enhanced_summary()
        successful = len(months) - summary['total_failures']

        self.tracker.print_info("\n📊 [bold]Export Summary:[/bold]")
        self.tracker.print_info(
            f"   • Successful months: [green]{successful}/{len(months)}[/green]"
        )
        self.tracker.print_info(
            f"   • Total rows exported: [green]{self.total_rows:,}[/green]"
        )
        self.tracker.print_info(
            f"   • Total data processed: [yellow]{self.total_memory:.1f} MB[/yellow]"
        )

        if summary['total_failures'] > 0:
            self.tracker.print_warning(
                f"\n   • Failed months: {summary['total_failures']}"
            )
            if summary['validation_only_failures'] > 0:
                self.tracker.print_info(
                    f"   • Validation-only failures: {summary['validation_only_failures']}"
                )
            if summary['full_retry_needed'] > 0:
                self.tracker.print_info(
                    f"   • Full retry needed: {summary['full_retry_needed']}"
                )
