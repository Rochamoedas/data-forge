import asyncio
from typing import Optional, List, Any, Dict
import polars as pl
import pyarrow as pa
from app.domain.repositories.data_repository import IDataRepository
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.config.logging_config import logger

class DuckDBDataRepository(IDataRepository):
    """
    Concrete implementation of IDataRepository using DuckDB.
    """
    def __init__(self, connection_pool: AsyncDuckDBPool):
        self.connection_pool = connection_pool

    def _query_sync(self, conn, sql: str, params: Optional[List[Any]] = None) -> pl.DataFrame:
        """Synchronous part of query."""
        return conn.execute(sql, params).pl()

    async def query(self, sql: str, params: Optional[List[Any]] = None) -> pl.DataFrame:
        """Executes a query and returns results as a Polars DataFrame."""
        async with self.connection_pool.acquire() as conn:
            return await asyncio.to_thread(self._query_sync, conn, sql, params)

    def _query_json_sync(self, conn, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Synchronous part of query_json."""
        df = conn.execute(sql, params).fetchdf()
        return df.to_dict(orient="records")

    async def query_json(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Executes a query and returns results as a list of dictionaries."""
        async with self.connection_pool.acquire() as conn:
            return await asyncio.to_thread(self._query_json_sync, conn, sql, params)

    def _query_arrow_sync(self, conn, sql: str, params: Optional[List[Any]] = None) -> pa.Table:
        """Synchronous part of query_arrow."""
        return conn.execute(sql, params).fetch_arrow_table()

    async def query_arrow(self, sql: str, params: Optional[List[Any]] = None) -> pa.Table:
        """Executes a query and returns results as a PyArrow Table."""
        async with self.connection_pool.acquire() as conn:
            return await asyncio.to_thread(self._query_arrow_sync, conn, sql, params)

    def _execute_sync(self, conn, sql: str, params: Optional[List[Any]] = None) -> int:
        """Synchronous part of execute."""
        result = conn.execute(sql, params)
        return result.rowcount

    async def execute(self, sql: str, params: Optional[List[Any]] = None) -> int:
        """Executes a command (INSERT, UPDATE, DELETE) and returns the number of affected rows."""
        async with self.connection_pool.acquire() as conn:
            return await asyncio.to_thread(self._execute_sync, conn, sql, params)

    async def bulk_insert_polars(self, table_name: str, df: pl.DataFrame) -> int:
        """Helper to convert Polars DF to Arrow and use bulk_insert_arrow."""
        arrow_table = df.to_arrow()
        return await self.bulk_insert_arrow(table_name, arrow_table)

    def _bulk_insert_arrow_sync(self, conn, table_name: str, arrow_table: pa.Table) -> int:
        """Synchronous part of bulk insert."""
        # Register the Arrow table as a temporary view in DuckDB
        conn.register('temp_arrow_table', arrow_table)
        try:
            # Insert data from the temporary view into the target table
            result = conn.execute(f'INSERT OR IGNORE INTO "{table_name}" SELECT * FROM temp_arrow_table')
            return result.rowcount
        finally:
            # Unregister the temporary view to clean up
            conn.unregister('temp_arrow_table')
            logger.info(f"Bulk insert of {arrow_table.num_rows} rows into '{table_name}' completed.")

    async def bulk_insert_arrow(self, table_name: str, arrow_table: pa.Table) -> int:
        """Performs a high-performance bulk insert into a table from a PyArrow Table."""
        async with self.connection_pool.acquire() as conn:
            return await asyncio.to_thread(
                self._bulk_insert_arrow_sync, conn, table_name, arrow_table
            ) 