# main.py
"""Simplified main entry point for PowerBI export pipeline."""

import logging
from datetime import datetime
from typing import Optional

from core.pipeline_orchestrator import PipelineOrchestrator
from logger.logger import Logger
from utils import ProgressTracker


def get_user_input() -> tuple[Optional[int], Optional[int]]:
    """Get date range from user input."""
    tracker = ProgressTracker()

    tracker.print_info("\n📅 Date Range Selection")
    tracker.print_info("Press Enter to use current month, or specify custom end date")

    # Year input
    year_input = input("\nEnter end year (e.g., 2025): ").strip()
    end_year = None
    if year_input:
        try:
            end_year = int(year_input)
            if end_year < 2020 or end_year > 2030:
                tracker.print_warning("Year seems unusual, but continuing...")
        except ValueError:
            tracker.print_error("Invalid year, using current year")
            end_year = None

    # Month input
    end_month = None
    if end_year:
        month_input = input("Enter end month (1-12): ").strip()
        if month_input:
            try:
                end_month = int(month_input)
                if end_month < 1 or end_month > 12:
                    tracker.print_error("Invalid month, using current month")
                    end_month = None
            except ValueError:
                tracker.print_error("Invalid month, using current month")
                end_month = None

    # Show what will be processed
    if end_year and end_month:
        tracker.print_info(f"\n✅ Will process 12 months ending in {end_year}-{end_month:02d}")
    else:
        current = datetime.now()
        tracker.print_info(f"\n✅ Will process 12 months ending in {current.year}-{current.month:02d} (current)")

    return end_year, end_month


def main():
    """Main entry point for the application."""
    # Setup logging
    Logger.setup(name="PowerBI_Export", level=logging.INFO)

    # Create tracker for user interaction
    tracker = ProgressTracker()

    try:
        # Welcome message
        tracker.print_header("PowerBI Export Tool v2.0")
        tracker.print_info("This tool exports 12 months of PowerBI data with validation\n")

        # Get user input
        end_year, end_month = get_user_input()

        # Confirm before starting
        input("\nPress Enter to start extraction...")

        # Create and run pipeline
        pipeline = PipelineOrchestrator()
        pipeline.run(end_year=end_year, end_month=end_month)

        tracker.print_info("\n🎉 [bold green]Export pipeline completed![/bold green]")

    except KeyboardInterrupt:
        tracker.print_warning("\n\n⚠️  Process interrupted by user")
        Logger.warning("Process interrupted by user")
    except Exception as e:
        tracker.print_error(f"\n❌ [bold red]Pipeline error: {str(e)}[/bold red]")
        Logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        # Always show goodbye message
        tracker.print_info("\nThank you for using PowerBI Export Tool!")
        input("\nPress Enter to exit...")


def run_batch_mode(end_year: int, end_month: int):
    """Run in batch mode without user interaction."""
    Logger.setup(name="PowerBI_Export", level=logging.INFO)

    pipeline = PipelineOrchestrator()
    pipeline.run(end_year=end_year, end_month=end_month)


if __name__ == "__main__":
    import sys

    # Check for command line arguments for batch mode
    if len(sys.argv) == 3:
        try:
            year = int(sys.argv[1])
            month = int(sys.argv[2])
            run_batch_mode(year, month)
        except ValueError:
            print("Usage: python main.py [year] [month]")
            sys.exit(1)
    else:
        # Interactive mode
        main()
