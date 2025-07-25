# config/settings.py
"""Configuration management for PowerBI export system."""

import os
from typing import Tuple
from dotenv import load_dotenv


class Config:
    """Centralized configuration management."""

    def __init__(self):
        """Initialize configuration by loading environment variables."""
        load_dotenv()
        self._validate_environment()

    @property
    def powerbi_server(self) -> str:
        """PowerBI server URL."""
        return os.getenv("POWERBI_SERVER")

    @property
    def powerbi_database(self) -> str:
        """PowerBI database/dataset name."""
        return os.getenv("POWERBI_DATABASE")

    @property
    def connect_timeout(self) -> int:
        """Connection timeout in seconds."""
        return int(os.getenv("CONNECT_TIMEOUT", "30"))

    @property
    def command_timeout(self) -> int:
        """Command execution timeout in seconds."""
        return int(os.getenv("COMMAND_TIMEOUT", "600"))

    @property
    def base_dir(self) -> str:
        """Base directory path."""
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    @property
    def assembly_path(self) -> str:
        """ADOMD.NET assembly path."""
        return os.path.join(
            self.base_dir, "lib", "Microsoft.AnalysisServices.AdomdClient"
        )

    @property
    def query_path(self) -> str:
        """DAX query template path."""
        return os.path.join(self.base_dir, "queries", "billing_cases_query.dax")

    @property
    def output_dir(self) -> str:
        """Output directory for exported files."""
        return os.path.join(self.base_dir, "exported_data")

    @property
    def csv_dir(self) -> str:
        """CSV output directory."""
        return os.path.join(self.output_dir, "csv")

    @property
    def parquet_dir(self) -> str:
        """Parquet output directory."""
        return os.path.join(self.output_dir, "parquet")

    def get_connection_params(self) -> Tuple[str, str]:
        """Get PowerBI connection parameters."""
        return self.powerbi_server, self.powerbi_database

    def _validate_environment(self):
        """Validate required environment variables."""
        required = ["POWERBI_SERVER", "POWERBI_DATABASE"]
        missing = [var for var in required if not os.getenv(var)]

        if missing:
            raise RuntimeError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
