import duckdb
import pandas as pd
import pyarrow as pa
from typing import List, Dict, Any
from pathlib import Path
import logging

from app.domain.entities.schema import Schema
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool


class IArrowBulkOperations:
    """Interface for Arrow-based bulk operations"""
    
    async def bulk_insert_from_dataframe(self, schema: Schema, df: pd.DataFrame) -> None:
        """Insert data from pandas DataFrame via Arrow"""
        raise NotImplementedError
    
    async def bulk_insert_from_arrow_table(self, schema: Schema, arrow_table: pa.Table) -> None:
        """Insert data from Arrow Table directly"""
        raise NotImplementedError
    
    async def bulk_read_to_arrow_table(self, schema: Schema) -> pa.Table:
        """Read data as Arrow Table"""
        raise NotImplementedError
    
    async def bulk_read_to_dataframe(self, schema: Schema) -> pd.DataFrame:
        """Read data as pandas DataFrame"""
        raise NotImplementedError


class ArrowBulkOperations(IArrowBulkOperations):
    """Arrow-based bulk operations for DuckDB"""
    
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
                conn.execute(f'INSERT OR IGNORE INTO "{schema.table_name}" SELECT * FROM arrow_table')
                conn.commit()
            except Exception as e:
                conn.rollback()
                logging.error(f"Bulk insert failed for table {schema.table_name}: {e}")
                raise
    
    async def bulk_read_to_arrow_table(self, schema: Schema) -> pa.Table:
        """Read data as Arrow Table - zero-copy when possible"""
        async with self.connection_pool.acquire() as conn:
            result = conn.execute(f'SELECT * FROM "{schema.table_name}"')
            return result.fetch_arrow_table()
    
    async def bulk_read_to_dataframe(self, schema: Schema) -> pd.DataFrame:
        """Read data as pandas DataFrame"""
        async with self.connection_pool.acquire() as conn:
            return conn.execute(f'SELECT * FROM "{schema.table_name}"').fetchdf()