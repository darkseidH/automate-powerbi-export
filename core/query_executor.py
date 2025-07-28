# core/query_executor.py
"""DAX query execution engine."""

from typing import Dict, Any

from config import Config


class QueryExecutor:
    """Executes DAX queries against PowerBI datasets."""

    def __init__(self, config: Config):
        """
        Initialize query executor.
        
        Args:
            config: Configuration instance
        """
        self.config = config
        self._query_template = None

    def load_query_template(self) -> str:
        """Load DAX query template from file."""
        if not self._query_template:
            with open(self.config.query_path, 'r', encoding='utf-8') as f:
                self._query_template = f.read()
        return self._query_template

    def format_query(self, parameters: Dict[str, Any]) -> str:
        """
        Format query template with parameters.
        
        Args:
            parameters: Dictionary containing year, month, day_start, day_end
            
        Returns:
            Formatted DAX query string
        """
        template = self.load_query_template()
        return template.format(**parameters)

    def execute(self, connection, query: str) -> 'DataTable':
        """
        Execute DAX query and return DataTable.
        
        Args:
            connection: Active ADOMD connection
            query: Formatted DAX query
            
        Returns:
            .NET DataTable with query results
        """
        from Microsoft.AnalysisServices.AdomdClient import AdomdDataAdapter
        from System.Data import DataTable

        adapter = AdomdDataAdapter(query, connection)
        dt = DataTable()
        adapter.Fill(dt)

        return dt
