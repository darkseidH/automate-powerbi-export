# utils/state_manager.py
"""State persistence for retry management."""

import json
import os
from typing import Dict
from datetime import datetime
from .retry_manager import FailedMonth, RetryManager


class StateManager:
    """Manages persistent state for retry operations."""

    def __init__(self, state_file: str = "retry_state.json"):
        """
        Initialize state manager.

        Args:
            state_file: Path to state persistence file
        """
        self.state_file = state_file
        self._ensure_state_directory()

    def _ensure_state_directory(self):
        """Ensure the directory for state file exists."""
        state_dir = os.path.dirname(self.state_file)
        if state_dir and not os.path.exists(state_dir):
            os.makedirs(state_dir, exist_ok=True)

    def save_retry_state(self, retry_manager: RetryManager):
        """
        Save current retry state to file.

        Args:
            retry_manager: RetryManager instance to persist
        """
        state_data = {
            "last_saved": datetime.now().isoformat(),
            "failed_months": [
                failed.to_dict() for failed in retry_manager.failed_months.values()
            ],
            "settings": {
                "max_retry_attempts": retry_manager.max_retry_attempts,
                "retry_delay_seconds": retry_manager.retry_delay_seconds,
            },
        }

        try:
            with open(self.state_file, "w") as f:
                json.dump(state_data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save retry state: {e}")

    def load_retry_state(self, retry_manager: RetryManager) -> bool:
        """
        Load retry state from file into retry manager.

        Args:
            retry_manager: RetryManager instance to populate

        Returns:
            True if state was loaded, False otherwise
        """
        if not os.path.exists(self.state_file):
            return False

        try:
            with open(self.state_file, "r") as f:
                state_data = json.load(f)

            # Clear existing state
            retry_manager.clear()

            # Load failed months
            for failed_data in state_data.get("failed_months", []):
                failed_month = FailedMonth.from_dict(failed_data)
                key = (failed_month.year, failed_month.month)
                retry_manager.failed_months[key] = failed_month

            return True

        except Exception as e:
            print(f"Warning: Could not load retry state: {e}")
            return False

    def clear_state(self):
        """Remove the state file."""
        if os.path.exists(self.state_file):
            try:
                os.remove(self.state_file)
            except Exception as e:
                print(f"Warning: Could not clear state file: {e}")

    def save_execution_log(self, log_entry: Dict, log_file: str = "execution_log.json"):
        """
        Append execution results to log file.

        Args:
            log_entry: Dictionary with execution details
            log_file: Path to log file
        """
        logs = []

        # Load existing logs
        if os.path.exists(log_file):
            try:
                with open(log_file, "r") as f:
                    logs = json.load(f)
            except:
                logs = []

        # Add timestamp to entry
        log_entry["timestamp"] = datetime.now().isoformat()

        # Append new entry
        logs.append(log_entry)

        # Keep only last 100 entries
        logs = logs[-100:]

        # Save updated logs
        try:
            with open(log_file, "w") as f:
                json.dump(logs, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save execution log: {e}")
