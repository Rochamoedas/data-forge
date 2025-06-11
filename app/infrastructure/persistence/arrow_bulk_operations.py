"""
Arrow-Based Bulk Operations
"""

import duckdb
import pandas as pd
import pyarrow as pa
from typing import List, Dict, Any
from pathlib import Path
from typing import TYPE_CHECKING

from app.domain.entities.schema import Schema
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.domain.repositories.interfaces import IArrowBulkOperations
from app.config.logging_config import logger

if TYPE_CHECKING:
    import pandas as pd


class ArrowBulkOperations(IArrowBulkOperations):
    """
    Service for high-performance bulk operations using Arrow and DuckDB.
    """
    
    def __init__(self, connection_pool: AsyncDuckDBPool):
        self.connection_pool = connection_pool
    
    async def bulk_insert_from_dataframe(self, schema: Schema, df: pd.DataFrame) -> None:
        """Insert data from pandas DataFrame via Arrow - optimized path"""
        arrow_table = pa.Table.from_pandas(df)
        await self.bulk_insert_from_arrow_table(schema, arrow_table)
    
    async def bulk_insert_from_arrow_table(self, schema: Schema, arrow_table: pa.Table) -> None:
        """Insert data from Arrow Table directly, ignoring duplicates."""
        async with self.connection_pool.acquire() as conn:
            conn.begin()
            try:
                conn.register("arrow_table", arrow_table)
                # Use INSERT OR IGNORE to skip duplicates without raising an error.
                # This is a high-performance way to handle conflicts in DuckDB.
                conn.execute(f'INSERT OR IGNORE INTO "{schema.table_name}" SELECT * FROM arrow_table')
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
    
    async def bulk_read_to_arrow_table(self, schema: Schema) -> pa.Table:
        """Read data as Arrow Table - zero-copy when possible"""
        async with self.connection_pool.acquire() as conn:
            result = conn.execute(f'SELECT * FROM "{schema.table_name}"')
            return result.fetch_arrow_table()
    
    async def bulk_read_to_dataframe(self, schema: Schema) -> pd.DataFrame:
        """Read data as pandas DataFrame"""
        async with self.connection_pool.acquire() as conn:
            return conn.execute(f'SELECT * FROM "{schema.table_name}"').fetchdf() 