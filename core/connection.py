# core/connection.py
"""PowerBI connection management."""

import clr
from typing import Optional
from config import Config


class PowerBIConnection:
    """Manages ADOMD.NET connections to PowerBI."""
    
    def __init__(self, config: Config):
        """
        Initialize connection manager.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        self._connection = None
        self._load_assembly()
        
    def _load_assembly(self):
        """Load ADOMD.NET assembly."""
        clr.AddReference(self.config.assembly_path)
        
    def _build_connection_string(self) -> str:
        """Build MSOLAP connection string."""
        server, database = self.config.get_connection_params()
        
        return (
            f"Provider=MSOLAP;"
            f"Data Source={server};"
            f"Initial Catalog={database};"
            f"Connect Timeout={self.config.connect_timeout};"
            f"Timeout={self.config.command_timeout};"
        )
    
    def connect(self):
        """Establish connection to PowerBI."""
        from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
        
        conn_string = self._build_connection_string()
        self._connection = AdomdConnection(conn_string)
        self._connection.Open()
        
    def disconnect(self):
        """Close PowerBI connection."""
        if self._connection:
            self._connection.Close()
            self._connection = None
            
    @property
    def connection(self):
        """Get active connection instance."""
        if not self._connection:
            raise RuntimeError("Not connected to PowerBI")
        return self._connection
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()