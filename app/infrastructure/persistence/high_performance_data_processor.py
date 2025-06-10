"""
🚀 Ultra High-Performance Data Processor
Combines Polars, PyArrow, and DuckDB for maximum performance:

- Polars: Lightning-fast DataFrame operations with lazy evaluation
- PyArrow: Zero-copy data interchange and columnar memory format
- DuckDB: Vectorized analytical queries with Arrow integration
- ConnectorX: Ultra-fast data loading from various sources
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
import uuid # Added import
from datetime import datetime, timezone # Added import
from concurrent.futures import ThreadPoolExecutor
import connectorx as cx
import json

from app.domain.entities.schema import Schema
from app.config.logging_config import logger
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.config.settings import settings


async def run_cpu_bound_task(func, *args, **kwargs):
    """
    ✅ Run CPU-bound tasks in thread pool to avoid blocking the event loop.
    
    This is the recommended pattern for handling CPU-intensive operations
    in async FastAPI applications.
    """
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        return await loop.run_in_executor(executor, func, *args, **kwargs)


class HighPerformanceDataProcessor:
    """
    🚀 Ultra-fast data processor combining Polars + PyArrow + DuckDB
    
    Performance Features:
    - Zero-copy data transfers via Arrow
    - Lazy evaluation with Polars
    - Vectorized operations in DuckDB
    - Parallel processing with async/await
    - Memory-efficient streaming
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
        🚀 Ultra-fast bulk insert using Polars → Arrow → DuckDB pipeline
        
        Performance optimizations:
        1. Convert to Polars DataFrame (fast)
        2. Transform to Arrow format (zero-copy)
        3. Insert via DuckDB Arrow integration (vectorized)
        """
        start_time = time.perf_counter()
        total_records = len(data)
        
        if not data:
            return {"success": True, "records_processed": 0, "duration_ms": 0}
        
        try:
            # Step 1: Convert to Polars DataFrame with schema validation
            logger.info(f"🔄 Converting {total_records:,} records to Polars DataFrame...")
            df = await self._create_polars_dataframe(data, schema)
            
            # Step 2: Apply data transformations and validations using Polars
            df_processed = await self._apply_polars_transformations(df, schema)
            
            # Step 3: Convert to Arrow format (zero-copy when possible)
            arrow_table = await self._convert_to_arrow(df_processed)
            
            # Step 4: Insert via DuckDB Arrow integration
            result = await self._insert_via_arrow(schema, arrow_table)
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            throughput = total_records / (duration_ms / 1000) if duration_ms > 0 else 0
            
            # Enhanced logging for large datasets
            if total_records >= 50000:
                logger.info(
                    f"[HIGH-PERF-PROCESSOR] 🚀 LARGE DATASET: Ultra-fast bulk insert completed: {total_records:,} records "
                    f"in {duration_ms:.2f}ms ({int(throughput):,} records/sec) "
                    f"- Polars+DuckDB optimization"
                )
            else:
                logger.info(
                    f"[HIGH-PERF-PROCESSOR] 🚀 Ultra-fast bulk insert completed: {total_records:,} records "
                    f"in {duration_ms:.2f}ms ({int(throughput):,} records/sec)"
                )
            

            
            return {
                "success": True,
                "records_processed": total_records,
                "duration_ms": duration_ms,
                "throughput_rps": int(throughput),
                "method": "polars_arrow_duckdb"
            }
            
        except Exception as e:
            logger.error(f"❌ Ultra-fast bulk insert failed: {e}")
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
                pl.col(schema.primary_key[0] if schema.primary_key and schema.primary_key[0] in df.columns else df.columns[0]).map_elements(lambda _: str(uuid.uuid4()), return_dtype=pl.Utf8).alias("id"),
                pl.lit(datetime.now(timezone.utc)).cast(pl.Datetime(time_unit='us', time_zone='UTC')).alias("created_at"),
                pl.lit(1).cast(pl.Int64).alias("version")
            ])
            
            # Apply deduplication if schema has primary key
            if schema.primary_key:
                # Ensure primary key columns exist before attempting unique
                valid_primary_keys = [pk_col for pk_col in schema.primary_key if pk_col in lazy_df.columns]
                if valid_primary_keys:
                    lazy_df = lazy_df.unique(subset=valid_primary_keys, keep="first")
            
            # Collect the lazy DataFrame (execute all operations)
            df_transformed = lazy_df.collect()

            # Ensure final column order
            final_columns_order = ["id", "created_at", "version"] + [p.name for p in schema.properties if p.name in df_transformed.columns]
            return df_transformed.select(final_columns_order)
        
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
        """Insert Arrow table directly into DuckDB using its native Arrow support."""

        async with self.connection_pool.acquire() as conn:
            try:
                temp_table_name = f"arrow_insert_{schema.table_name}_{int(time.time())}"
                conn.register(temp_table_name, arrow_table)
                
                arrow_columns = arrow_table.column_names
                
                # Define the order of columns for insertion into the database table
                # This order must match the target table structure.
                db_target_columns_ordered = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
                
                # Create the string for column names in the INSERT INTO clause (e.g., "id", "created_at", ...)
                db_columns_str = ", ".join([f'"{col}"' for col in db_target_columns_ordered])
                
                # Create the string for selecting columns from the Arrow table.
                # These columns MUST be selected in the same order as db_target_columns_ordered.
                # We rely on _apply_polars_transformations to have produced an Arrow table
                # with columns named appropriately and available.
                select_cols_from_arrow_str = ", ".join([f'"{col}"' for col in db_target_columns_ordered])

                logger.info(f"[HIGH-PERF-PROCESSOR] Inserting {len(arrow_table)} records via direct Arrow to DuckDB method for schema {schema.name}")

                conn.execute("BEGIN TRANSACTION")

                conn.execute("PRAGMA enable_progress_bar=false")
                conn.execute(f"PRAGMA threads={self.max_workers}")
                conn.execute(f"PRAGMA memory_limit='{settings.DUCKDB_MEMORY_LIMIT_HIGH_PERF_WRITE}'")

                insert_sql = f"""
                    INSERT OR IGNORE INTO "{schema.table_name}" ({db_columns_str})
                    SELECT {select_cols_from_arrow_str} FROM {temp_table_name}
                """
                conn.execute(insert_sql)

                conn.execute("COMMIT")

                # conn.unregister(temp_table_name) # Optional: good practice but often auto-cleaned.

                return {"rows_inserted": len(arrow_table)}
                
            except Exception as e:
                if conn:
                    conn.execute("ROLLBACK")
                logger.error(f"Direct Arrow insert failed for schema {schema.name}: {e}")
                raise
    
    async def query_with_polars_optimization(
        self, 
        schema: Schema, 
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> pl.DataFrame:
        """
        🚀 Ultra-fast querying using optimized DuckDB → Arrow → Polars pipeline
        
        Performance benefits:
        - Vectorized query execution in DuckDB
        - Direct Arrow integration (zero-copy when possible)
        - Fast post-processing with Polars
        - Optimized for large datasets
        """
        start_time = time.perf_counter()
        
        async with self.connection_pool.acquire() as conn:
            try:
                # Setup DuckDB optimizations for read performance
                def setup_read_optimizations():
                    try:
                        # Optimize DuckDB for read performance
                        conn.execute("PRAGMA enable_progress_bar=false")
                        conn.execute(f"PRAGMA threads={self.max_workers}")
                        conn.execute(f"PRAGMA memory_limit='{settings.DUCKDB_MEMORY_LIMIT_HIGH_PERF_READ}'")
                        conn.execute(f"PRAGMA max_memory='{settings.DUCKDB_MEMORY_LIMIT_HIGH_PERF_READ}'")
                        conn.execute("PRAGMA temp_directory='/tmp'")
                        
                        # Enable query optimizations - FIXED: Use disabled_optimizers instead of enable_optimizer
                        conn.execute("PRAGMA disabled_optimizers=''")  # Enable all optimizers
                        conn.execute("PRAGMA enable_profiling=false")  # Disable profiling for speed
                        conn.execute("PRAGMA enable_progress_bar=false")
                        
                        # Try to install and load Arrow extension for zero-copy transfers
                        try:
                            conn.execute("INSTALL arrow")
                            conn.execute("LOAD arrow")
                            return True
                        except Exception:
                            logger.warning("[HIGH-PERF-PROCESSOR] Arrow extension not available, using optimized fallback")
                            return False
                    except Exception as e:
                        logger.warning(f"[HIGH-PERF-PROCESSOR] Setup optimizations failed: {e}")
                        return False
                
                use_arrow = await asyncio.to_thread(setup_read_optimizations)
                
                # Build optimized query with proper indexing hints
                query_parts = [f'SELECT * FROM "{schema.table_name}"']
                params = []
                
                if filters:
                    where_conditions = []
                    for field, value in filters.items():
                        # Use proper parameterized queries for security and performance
                        where_conditions.append(f'"{field}" = ?')
                        params.append(value)
                    query_parts.append(f"WHERE {' AND '.join(where_conditions)}")
                
                # Add ORDER BY for consistent results and better performance with indexes
                query_parts.append("ORDER BY id")
                
                if limit:
                    query_parts.append(f"LIMIT {limit}")
                
                query = " ".join(query_parts)
                
                # Execute query with the best available method
                def execute_optimized_query():
                    if use_arrow:  # Attempt Arrow path if the extension is loaded
                        try:
                            logger.info("[HIGH-PERF-PROCESSOR] Using DuckDB → Arrow → Polars pipeline")
                            arrow_result = conn.execute(query, params).arrow()
                            # Convert to Polars DataFrame directly from Arrow result
                            df = pl.from_arrow(arrow_result)
                            return df, "arrow_to_polars_optimized"
                        except Exception as arrow_error:
                            logger.warning(f"[HIGH-PERF-PROCESSOR] DuckDB → Arrow → Polars pipeline failed: {arrow_error}. Falling back.")
                            # Fall through to the next best optimized fallback
                    
                    # Fallback 1: DuckDB's native DataFrame conversion (to Pandas, then Polars)
                    logger.info("[HIGH-PERF-PROCESSOR] Using fallback: DuckDB → Pandas DataFrame → Polars pipeline")
                    try:
                        result_relation = conn.execute(query, params)
                        pd_df = result_relation.df()  # DuckDB's optimized pandas DataFrame
                        df = pl.from_pandas(pd_df) # Convert pandas to Polars
                        return df, "duckdb_pandas_to_polars_fallback"
                    except Exception as pandas_fallback_error:
                        logger.warning(f"[HIGH-PERF-PROCESSOR] DuckDB → Pandas DataFrame → Polars fallback failed: {pandas_fallback_error}. Falling back to manual conversion.")
                        # Fall through to the final manual fallback

                    # Fallback 2: Manual conversion (fetchall and create Polars DataFrame)
                    logger.info("[HIGH-PERF-PROCESSOR] Using fallback: Manual dict conversion → Polars pipeline")
                    # Re-execute the query for fetchall() as the relation might have been consumed or in a different state
                    result_for_manual = conn.execute(query, params)
                    description = result_for_manual.description # Ensure description is fetched from the correct execution
                    rows = result_for_manual.fetchall()
                    column_names = [desc[0] for desc in description]

                    records = [dict(zip(column_names, row)) for row in rows]
                    df = pl.DataFrame(records)
                    return df, "manual_conversion_fallback"

                df, method = await run_cpu_bound_task(execute_optimized_query)
                
                duration_ms = (time.perf_counter() - start_time) * 1000
                throughput = len(df) / (duration_ms / 1000) if duration_ms > 0 else 0
                
                logger.info(
                    f"[HIGH-PERF-PROCESSOR] 🚀 ULTRA-FAST Query executed: {len(df):,} records in {duration_ms:.2f}ms "
                    f"({int(throughput):,} records/sec) using {method} pipeline"
                )
                
                return df
                
            except Exception as e:
                logger.error(f"❌ Ultra-fast query failed: {e}")
                raise
    
    async def stream_with_arrow_batches(
        self, 
        schema: Schema, 
        batch_size: int = 50000  # Increased default batch size for better performance
    ) -> AsyncIterator[pl.DataFrame]:
        """
        🚀 Ultra-fast streaming using OFFSET-based pagination
        
        Memory efficient streaming with:
        - OFFSET-based pagination (much faster than cursor-based)
        - Larger batch sizes for better throughput
        - Arrow batch processing when available
        - Async iteration
        """
        async with self.connection_pool.acquire() as conn:
            try:
                # Optimize DuckDB for streaming performance
                def setup_streaming_optimizations():
                    try:
                        # Memory and performance optimizations for streaming
                        conn.execute("PRAGMA enable_progress_bar=false")
                        conn.execute(f"PRAGMA threads={self.max_workers}")
                        conn.execute(f"PRAGMA memory_limit='{settings.DUCKDB_MEMORY_LIMIT_HIGH_PERF_STREAM}'")
                        conn.execute(f"PRAGMA max_memory='{settings.DUCKDB_MEMORY_LIMIT_HIGH_PERF_STREAM}'")
                        conn.execute("PRAGMA temp_directory='/tmp'")
                        
                        # Try to install and load Arrow extension
                        conn.execute("INSTALL arrow; LOAD arrow;")
                        return True
                    except Exception:
                        return False
                
                use_arrow = await asyncio.to_thread(setup_streaming_optimizations)
                if not use_arrow:
                    logger.warning("Arrow extension not available, using traditional streaming with optimizations")
                
                # Get total count efficiently
                def get_total_count():
                    count_result = conn.execute(f'SELECT COUNT(*) FROM "{schema.table_name}"').fetchone()
                    return count_result[0] if count_result else 0
                
                total_records = await asyncio.to_thread(get_total_count)
                logger.info(f"[HIGH-PERF-PROCESSOR] 🔄 Starting OFFSET-based streaming: {total_records:,} total records with batch size {batch_size:,}")
                
                processed = 0
                stream_start_time = time.perf_counter()
                batch_number = 0
                
                while processed < total_records:
                    batch_number += 1
                    
                    # Use OFFSET-based pagination (much faster and more predictable)
                    query = f'''
                    SELECT * FROM "{schema.table_name}" 
                    ORDER BY id
                    LIMIT {batch_size} OFFSET {processed}
                    '''
                    
                    # Execute batch query with optimizations
                    def execute_batch():
                        if use_arrow:
                            try:
                                # Try Arrow-optimized path first
                                arrow_batch = conn.execute(query).arrow()
                                df_batch = pl.from_arrow(arrow_batch)
                                return df_batch, "arrow_optimized"
                            except Exception:
                                # Fallback to traditional method
                                result = conn.execute(query).fetchall()
                                description = conn.description
                                column_names = [desc[0] for desc in description]
                                records = [dict(zip(column_names, row)) for row in result]
                                df_batch = pl.DataFrame(records)
                                return df_batch, "traditional_fallback"
                        else:
                            # Traditional method with optimizations
                            result = conn.execute(query).fetchall()
                            description = conn.description
                            column_names = [desc[0] for desc in description]
                            records = [dict(zip(column_names, row)) for row in result]
                            df_batch = pl.DataFrame(records)
                            return df_batch, "traditional_optimized"
                    
                    df_batch, method = await asyncio.to_thread(execute_batch)
                    
                    if len(df_batch) == 0:
                        logger.info(f"📦 No more records found, ending stream at batch {batch_number}")
                        break
                    
                    batch_length = len(df_batch)
                    processed += batch_length
                    
                    # Calculate streaming performance
                    elapsed_time = time.perf_counter() - stream_start_time
                    current_throughput = processed / elapsed_time if elapsed_time > 0 else 0
                    
                    # Calculate progress percentage
                    progress_pct = (processed / total_records) * 100 if total_records > 0 else 0
                    
                    logger.info(f"[HIGH-PERF-PROCESSOR] 🚀 Streaming batch {batch_number}: {batch_length:,} records ({processed:,}/{total_records:,} = {progress_pct:.1f}%) - {int(current_throughput):,} records/sec using {method}")
                    
                    yield df_batch
                    
                    # Memory cleanup after each batch
                    del df_batch
                    gc.collect()
                    
                    # Small delay to allow other tasks and prevent overwhelming the system
                    await asyncio.sleep(0.001)  # Minimal delay
                    
                    # Break if we got fewer records than requested (end of data)
                    if batch_length < batch_size:
                        logger.info(f"📦 Reached end of data at batch {batch_number}")
                        break
                
                final_elapsed = time.perf_counter() - stream_start_time
                final_throughput = processed / final_elapsed if final_elapsed > 0 else 0
                logger.info(f"[HIGH-PERF-PROCESSOR] ✅ OFFSET-based streaming completed: {processed:,} records in {final_elapsed:.2f}s ({int(final_throughput):,} records/sec)")
                
            except Exception as e:
                logger.error(f"❌ Streaming failed: {e}")
                raise
    
    async def export_to_parquet_optimized(
        self, 
        schema: Schema, 
        output_path: Path,
        compression: str = "snappy"
    ) -> Dict[str, Any]:
        """
        🚀 Ultra-fast Parquet export using DuckDB → Arrow → Parquet pipeline
        """
        start_time = time.perf_counter()
        
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
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                logger.info(
                    f"[HIGH-PERF-PROCESSOR] 🚀 Parquet export completed: {file_size / (1024*1024):.2f}MB "
                    f"in {duration_ms:.2f}ms"
                )
                
                return {
                    "success": True,
                    "file_path": str(output_path),
                    "file_size_mb": file_size / (1024*1024),
                    "duration_ms": duration_ms,
                    "compression": compression
                }
                
            except Exception as e:
                logger.error(f"❌ Parquet export failed: {e}")
                raise
    
    async def analyze_with_polars(
        self, 
        schema: Schema,
        analysis_type: str = "summary"
    ) -> Dict[str, Any]:
        """
        🚀 Ultra-fast data analysis using Polars
        
        Analysis types:
        - summary: Basic statistics
        - profile: Data profiling
        - quality: Data quality metrics
        """
        start_time = time.perf_counter()
        
        # ✅ FIX: For summary analysis, use SQL aggregations instead of loading all data
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
            # ✅ FIX: Limit data for analysis to avoid memory issues
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
            
            # ✅ FIX: Run analysis in thread pool
            analysis_result = await asyncio.to_thread(analyze)
        
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        logger.info(f"[HIGH-PERF-PROCESSOR] 🚀 Data analysis completed in {duration_ms:.2f}ms using optimized method")
        
        return {
            "analysis_type": analysis_type,
            "duration_ms": duration_ms,
            "results": analysis_result
        } 