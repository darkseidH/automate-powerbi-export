# utils/progress_tracker.py
"""Progress tracking utilities."""

from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)


class ProgressTracker:
    """Manages progress bar configurations and creation."""

    def __init__(self):
        """Initialize progress tracker with console."""
        self.console = Console()

    def create_progress_bar(self) -> Progress:
        """
        Create configured Progress instance.
        
        Returns:
            Configured Progress instance for context manager use
        """
        return Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            MofNCompleteColumn(),
            "•",
            TimeRemainingColumn(),
            console=self.console,
            expand=True
        )

    def print_header(self, message: str):
        """Print formatted header message."""
        self.console.print(f"🚀 [bold magenta]{message}[/bold magenta]\n")

    def print_info(self, message: str):
        """Print information message."""
        self.console.print(message)

    def print_success(self, message: str):
        """Print success message."""
        self.console.print(f"✅ {message}")

    def print_error(self, message: str):
        """Print error message."""
        self.console.print(f"❌ {message}")

    def print_warning(self, message: str):
        """Print warning message."""
        self.console.print(f"⚠️  {message}")

    def print_processing(self, message: str):
        """Print processing status message."""
        self.console.print(f"📅 Processing: [bold blue]{message}[/bold blue]")
