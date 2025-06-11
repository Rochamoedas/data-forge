"""
High-Performance Data Processor
"""

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import asyncio
import time
import gc
from typing import Dict, Any, List, Optional, AsyncIterator, Union
from pathlib import Path
import tempfile
import os
from concurrent.futures import ThreadPoolExecutor
import connectorx as cx
import json

from app.domain.entities.schema import Schema
from app.domain.entities.data_record import DataRecord
from app.config.logging_config import logger
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool


async def run_cpu_bound_task(func, *args, **kwargs):
    """
    Run CPU-bound tasks in a thread pool.
    """
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        return await loop.run_in_executor(executor, func, *args, **kwargs)


class HighPerformanceDataProcessor:
    """
    Data processor combining Polars, PyArrow, and DuckDB.
    """
    
    def __init__(self, connection_pool: AsyncDuckDBPool, max_workers: int = 8):
        self.connection_pool = connection_pool
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Performance settings optimized for high-end hardware (16GB RAM, i7 10th gen)
        self.chunk_size = 500000  # Much larger chunk size for better performance
        self.arrow_batch_size = 250000  # Aggressive Arrow batch size for high-end systems
        self.csv_batch_size = 100000  # Optimal CSV batch size for bulk operations
        
    async def bulk_insert_ultra_fast(
        self, 
        schema: Schema, 
        data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Bulk insert using Polars -> Arrow -> DuckDB pipeline.
        """
        total_records = len(data)
        
        if not data:
            return {"success": True, "records_processed": 0}
        
        try:
            # Step 1: Convert to Polars DataFrame with schema validation
            df = await self._create_polars_dataframe(data, schema)
            
            # Step 2: Apply data transformations and validations using Polars
            df_processed = await self._apply_polars_transformations(df, schema)
            
            # Step 3: Convert to Arrow format (zero-copy when possible)
            arrow_table = await self._convert_to_arrow(df_processed)
            
            # Step 4: Insert via DuckDB Arrow integration
            await self._insert_via_arrow(schema, arrow_table)
            
            logger.info(
                f"[HIGH-PERF-PROCESSOR] Bulk insert completed: {total_records:,} records"
            )
            
            return {
                "success": True,
                "records_processed": total_records,
                "method": "polars_arrow_duckdb"
            }
            
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            raise
    
    async def _create_polars_dataframe(
        self, 
        data: List[Dict[str, Any]], 
        schema: Schema
    ) -> pl.DataFrame:
        """Create Polars DataFrame with proper schema and data types"""
        
        def create_df():
            # Create DataFrame
            df = pl.DataFrame(data)
            
            # Apply schema-based type casting for optimal performance
            cast_expressions = []
            for prop in schema.properties:
                if prop.name in df.columns:
                    if prop.type == "string":
                        cast_expressions.append(pl.col(prop.name).cast(pl.Utf8))
                    elif prop.type == "integer":
                        cast_expressions.append(pl.col(prop.name).cast(pl.Int64))
                    elif prop.type == "number":
                        cast_expressions.append(pl.col(prop.name).cast(pl.Float64))
                    elif prop.type == "boolean":
                        cast_expressions.append(pl.col(prop.name).cast(pl.Boolean))
                    elif prop.type == "date":
                        cast_expressions.append(pl.col(prop.name).cast(pl.Date))
                    elif prop.type == "datetime":
                        cast_expressions.append(pl.col(prop.name).cast(pl.Datetime))
            
            # Apply all casts at once (efficient)
            if cast_expressions:
                df = df.with_columns(cast_expressions)
            
            return df
        
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, create_df)
    
    async def _apply_polars_transformations(
        self, 
        df: pl.DataFrame, 
        schema: Schema
    ) -> pl.DataFrame:
        """Apply data transformations using Polars' lazy evaluation"""
        
        def transform():
            # Use lazy evaluation for better performance
            lazy_df = df.lazy()
            
            # Add system columns
            lazy_df = lazy_df.with_columns([
                pl.lit(None).cast(pl.Utf8).alias("id"),  # Will be generated in DuckDB
                pl.lit(None).cast(pl.Datetime).alias("created_at"),  # Will be set in DuckDB
                pl.lit(1).alias("version")
            ])
            
            # Apply deduplication if schema has primary key
            if schema.primary_key:
                lazy_df = lazy_df.unique(subset=schema.primary_key, keep="first")
            
            # Collect the lazy DataFrame (execute all operations)
            return lazy_df.collect()
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, transform)
    
    async def _convert_to_arrow(self, df: pl.DataFrame) -> pa.Table:
        """Convert Polars DataFrame to Arrow Table (zero-copy when possible)"""
        
        def convert():
            # Polars → Arrow conversion (optimized)
            return df.to_arrow()
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, convert)
    
    async def _insert_via_arrow(self, schema: Schema, arrow_table: pa.Table) -> Dict[str, Any]:
        """Insert Arrow table directly into DuckDB using optimized batch insert"""
        
        async with self.connection_pool.acquire() as conn:
            try:
                # Convert Arrow to Polars DataFrame
                df = pl.from_arrow(arrow_table)
                
                # Create temporary CSV file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, 
                                                      newline='', encoding='utf-8') as temp_file:
                    
                    try:
                        # Define columns in correct order
                        columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
                        
                        # Write CSV data efficiently
                        writer = csv.writer(temp_file, quoting=csv.QUOTE_MINIMAL)
                        writer.writerow(columns)
                        
                        # Convert DataFrame to records and write
                        records_data = df.to_dicts()
                        for record_data in records_data:
                            import uuid
                            from datetime import datetime
                            
                            row_data = [
                                str(uuid.uuid4()),
                                datetime.now().isoformat(),
                                record_data.get("version", 1)
                            ]
                            
                            # Add property values
                            for prop in schema.properties:
                                value = record_data.get(prop.name, '')
                                if value is None:
                                    row_data.append('')
                                else:
                                    row_data.append(str(value))
                            
                            writer.writerow(row_data)
                        
                        temp_file_name = temp_file.name
                    
                    finally:
                        pass # temp_file is closed by with statement

                try:
                    # Use DuckDB COPY FROM for ultra-fast bulk insert
                    conn.execute("BEGIN TRANSACTION")
                    
                    # Create temporary table for COPY operation to handle duplicates
                    temp_table = f"temp_copy_{schema.table_name}_{int(time.time())}"
                    create_temp_sql = f'CREATE TEMPORARY TABLE "{temp_table}" AS SELECT * FROM "{schema.table_name}" LIMIT 0'
                    conn.execute(create_temp_sql)
                    
                    # Ultra-fast COPY FROM CSV to temporary table
                    copy_sql = f"""
                        COPY "{temp_table}" FROM '{temp_file_name}' 
                        (FORMAT CSV, HEADER true, DELIMITER ',', QUOTE '"', ENCODING 'utf-8', IGNORE_ERRORS false)
                    """
                    conn.execute(copy_sql)
                    
                    # Insert from temp table with duplicate handling (like traditional method)
                    insert_sql = f'INSERT OR IGNORE INTO "{schema.table_name}" SELECT * FROM "{temp_table}"'
                    conn.execute(insert_sql)
                    
                    # Clean up temp table
                    conn.execute(f'DROP TABLE "{temp_table}"')
                    
                    conn.execute("COMMIT")
                    
                    return {"rows_inserted": len(arrow_table)}
                    
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_file_name):
                        try:
                            os.unlink(temp_file_name)
                        except Exception as e:
                            logger.warning(f"Failed to cleanup temp file: {e}")
                
            except Exception as e:
                conn.execute("ROLLBACK")
                logger.error(f"Ultra-fast bulk insert failed: {e}")
                raise
    
    async def query_with_polars_optimization(
        self, 
        schema: Schema, 
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> pl.DataFrame:
        """
        Query data with Polars optimization.
        """
        async with self.connection_pool.acquire() as conn:
            
            use_arrow = False
            def setup_read_optimizations():
                nonlocal use_arrow
                try:
                    conn.execute("LOAD arrow;")
                    use_arrow = True
                except Exception:
                    use_arrow = False

            await run_cpu_bound_task(setup_read_optimizations)

            def execute_optimized_query():
                query_parts = [f'SELECT * FROM "{schema.table_name}"']
                params = []
                
                if filters:
                    where_conditions = []
                    for field, value in filters.items():
                        where_conditions.append(f'"{field}" = ?')
                        params.append(value)
                    query_parts.append(f"WHERE {' AND '.join(where_conditions)}")
                
                if limit:
                    query_parts.append(f"LIMIT {limit}")
                
                query = " ".join(query_parts)

                if use_arrow:
                    try:
                        arrow_result = conn.execute(query, params).arrow()
                        df = pl.from_arrow(arrow_result)
                        return df
                    except Exception as arrow_error:
                        logger.warning(f"Arrow optimization failed: {arrow_error}, falling back.")

                # Fallback to df
                result_relation = conn.execute(query, params)
                df = result_relation.pl() # to_polars()
                return df

            df = await run_cpu_bound_task(execute_optimized_query)
            
            logger.info(
                f"[HIGH-PERF-PROCESSOR] Query executed: {len(df):,} records"
            )
            
            return df
    
    async def stream_with_arrow_batches(
        self, 
        schema: Schema, 
        batch_size: int = 50000
    ) -> AsyncIterator[pl.DataFrame]:
        """
        Stream data in Arrow batches.
        """
        async with self.connection_pool.acquire() as conn:
            use_arrow = False
            def setup_streaming_optimizations():
                nonlocal use_arrow
                try:
                    conn.execute("LOAD arrow;")
                    use_arrow = True
                except Exception:
                    use_arrow = False
                    
            await run_cpu_bound_task(setup_streaming_optimizations)

            def get_total_count():
                count_result = conn.execute(f'SELECT COUNT(*) FROM "{schema.table_name}"').fetchone()
                return count_result[0] if count_result else 0
            
            total_records = await asyncio.to_thread(get_total_count)
            
            if not use_arrow:
                logger.warning("Arrow extension not available, using traditional streaming with optimizations")

            for offset in range(0, total_records, batch_size):
                query = f'SELECT * FROM "{schema.table_name}" LIMIT {batch_size} OFFSET {offset}'

                def execute_batch():
                    if use_arrow:
                        arrow_batch = conn.execute(query).arrow()
                        return pl.from_arrow(arrow_batch)
                    else:
                        # Fallback
                        return conn.execute(query).pl()

                df_batch = await run_cpu_bound_task(execute_batch)
                yield df_batch
    
    async def export_to_parquet_optimized(
        self, 
        schema: Schema, 
        output_path: Path,
        compression: str = "snappy"
    ) -> Dict[str, Any]:
        """
        Fast Parquet export using DuckDB COPY TO.
        """
        async with self.connection_pool.acquire() as conn:
            try:
                # Direct DuckDB to Parquet export (fastest method)
                export_sql = f"""
                COPY (SELECT * FROM "{schema.table_name}") 
                TO '{output_path}' 
                (FORMAT PARQUET, COMPRESSION '{compression}')
                """
                
                conn.execute(export_sql)
                
                # Get file stats
                file_size = output_path.stat().st_size if output_path.exists() else 0
                
                logger.info(
                    f"[HIGH-PERF-PROCESSOR] Parquet export completed: {file_size / (1024*1024):.2f}MB"
                )
                
                return {
                    "success": True,
                    "file_path": str(output_path),
                    "file_size_mb": file_size / (1024*1024),
                    "compression": compression
                }
                
            except Exception as e:
                logger.error(f"Parquet export failed: {e}")
                raise
    
    async def analyze_with_polars(
        self, 
        schema: Schema,
        analysis_type: str = "summary"
    ) -> Dict[str, Any]:
        """
        Perform data analysis using Polars.
        """
        # For summary analysis, use SQL aggregations instead of loading all data
        if analysis_type == "summary":
            async with self.connection_pool.acquire() as conn:
                def get_summary_stats():
                    # Get basic stats using SQL (much faster than loading all data)
                    count_query = f'SELECT COUNT(*) FROM "{schema.table_name}"'
                    count_result = conn.execute(count_query).fetchone()
                    total_records = count_result[0] if count_result else 0
                    
                    # Get column info
                    columns_query = f'DESCRIBE "{schema.table_name}"'
                    columns_result = conn.execute(columns_query).fetchall()
                    
                    columns = []
                    dtypes = {}
                    for col_info in columns_result:
                        col_name = col_info[0]
                        col_type = col_info[1]
                        columns.append(col_name)
                        dtypes[col_name] = col_type
                    
                    # Get null counts for each column
                    null_counts = {}
                    for col in columns:
                        null_query = f'SELECT COUNT(*) FROM "{schema.table_name}" WHERE "{col}" IS NULL'
                        null_result = conn.execute(null_query).fetchone()
                        null_counts[col] = null_result[0] if null_result else 0
                    
                    return {
                        "shape": (total_records, len(columns)),
                        "columns": columns,
                        "dtypes": dtypes,
                        "null_counts": null_counts,
                        "total_records": total_records
                    }
                
                analysis_result = await asyncio.to_thread(get_summary_stats)
        else:
            # For detailed analysis, we need the full DataFrame
            df = await self.query_with_polars_optimization(schema, limit=10000)  # Limit for analysis
            
            def analyze():
                if analysis_type == "profile":
                    return {
                        "describe": df.describe().to_dicts(),
                        "unique_counts": {col: df[col].n_unique() for col in df.columns},
                        "sample_data": df.head(5).to_dicts()
                    }
                elif analysis_type == "quality":
                    return {
                        "completeness": {col: 1 - (df[col].null_count() / len(df)) for col in df.columns},
                        "duplicates": df.is_duplicated().sum(),
                        "total_records": len(df),
                        "note": "Analysis based on sample of 10,000 records"
                    }
            
            # Run analysis in thread pool
            analysis_result = await asyncio.to_thread(analyze)
        
        logger.info(f"[HIGH-PERF-PROCESSOR] Data analysis completed")
        
        return {
            "analysis_type": analysis_type,
            "results": analysis_result
        } 