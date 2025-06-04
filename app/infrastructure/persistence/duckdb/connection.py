# app/infrastructure/persistence/duckdb/connection.py
import duckdb
from app.config.settings import settings

class DuckDBConnection:
    """Manages the DuckDB database connection."""
    def __init__(self):
        # Connect to the DuckDB file specified in settings
        # ensure that the directory for the duckdb file exists
        import os
        db_dir = os.path.dirname(settings.DUCKDB_PATH)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        self.conn = duckdb.connect(database=settings.DUCKDB_PATH, read_only=False)

    def get_connection(self):
        """Returns the active DuckDB connection object."""
        return self.conn

    def close(self):
        """Closes the DuckDB connection."""
        self.conn.close()

    def __enter__(self):
        """Context manager entry point."""
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point, ensures connection is closed."""
        self.conn.close()