from abc import ABC, abstractmethod
from typing import Optional, List, Any, Dict
import polars as pl
import pyarrow as pa

class IDataRepository(ABC):
    """
    Interface for a repository that handles data persistence and retrieval.
    """

    @abstractmethod
    async def query(self, sql: str, params: Optional[List[Any]] = None) -> pl.DataFrame:
        """Executes a query and returns results as a Polars DataFrame."""
        pass

    @abstractmethod
    async def query_json(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Executes a query and returns results as a list of dictionaries."""
        pass

    @abstractmethod
    async def query_arrow(self, sql: str, params: Optional[List[Any]] = None) -> pa.Table:
        """Executes a query and returns results as a PyArrow Table."""
        pass

    @abstractmethod
    async def execute(self, sql: str, params: Optional[List[Any]] = None) -> int:
        """Executes a command (INSERT, UPDATE, DELETE) and returns the number of affected rows."""
        pass

    @abstractmethod
    async def bulk_insert_arrow(self, table_name: str, arrow_table: pa.Table) -> int:
        """Performs a bulk insert into a table from a PyArrow Table."""
        pass 