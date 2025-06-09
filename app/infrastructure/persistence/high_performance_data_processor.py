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
        
        # Performance settings optimized for large datasets
        self.chunk_size = 250000  # Larger chunk size for better performance
        self.arrow_batch_size = 100000  # Larger Arrow batch size
        self.csv_batch_size = 50000  # Optimal CSV batch size
        
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
                    f"🚀 LARGE DATASET: Ultra-fast bulk insert completed: {total_records:,} records "
                    f"in {duration_ms:.2f}ms ({int(throughput):,} records/sec) "
                    f"- Polars+DuckDB optimization"
                )
            else:
                logger.info(
                    f"🚀 Ultra-fast bulk insert completed: {total_records:,} records "
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
                # Skip Arrow extension entirely - use optimized Polars → DuckDB approach
                logger.info("Using optimized Polars → DuckDB pipeline (no Arrow extension needed)")
                
                # Convert Arrow to Polars DataFrame
                df = pl.from_arrow(arrow_table)
                
                # Use DuckDB's COPY FROM for maximum performance
                import tempfile
                import csv
                import os
                
                # Create temporary CSV file
                temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, 
                                                      newline='', encoding='utf-8')
                
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
                    
                    temp_file.close()
                    
                    # Use DuckDB COPY FROM for ultra-fast bulk insert
                    conn.execute("BEGIN TRANSACTION")
                    
                    # Optimize DuckDB for maximum performance
                    conn.execute("PRAGMA enable_progress_bar=false")
                    conn.execute("PRAGMA threads=8")
                    conn.execute("PRAGMA memory_limit='8GB'")
                    
                    # Create temporary table for COPY operation to handle duplicates
                    temp_table = f"temp_copy_{schema.table_name}_{int(time.time())}"
                    create_temp_sql = f'CREATE TEMPORARY TABLE "{temp_table}" AS SELECT * FROM "{schema.table_name}" LIMIT 0'
                    conn.execute(create_temp_sql)
                    
                    # Ultra-fast COPY FROM CSV to temporary table
                    copy_sql = f"""
                        COPY "{temp_table}" FROM '{temp_file.name}' 
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
                    if os.path.exists(temp_file.name):
                        try:
                            os.unlink(temp_file.name)
                        except Exception as e:
                            logger.warning(f"Failed to cleanup temp file: {e}")
                
            except Exception as e:
                conn.execute("ROLLBACK")
                raise e
    
    async def query_with_polars_optimization(
        self, 
        schema: Schema, 
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> pl.DataFrame:
        """
        🚀 Ultra-fast querying using optimized DuckDB → Polars pipeline
        
        Performance benefits:
        - Vectorized query execution in DuckDB
        - Efficient data transfer
        - Fast post-processing with Polars
        """
        start_time = time.perf_counter()
        
        async with self.connection_pool.acquire() as conn:
            try:
                # Build optimized query
                query = f'SELECT * FROM "{schema.table_name}"'
                params = []
                
                if filters:
                    where_conditions = []
                    for field, value in filters.items():
                        where_conditions.append(f'"{field}" = ?')
                        params.append(value)
                    query += f" WHERE {' AND '.join(where_conditions)}"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                # Use optimized DuckDB → dict → Polars pipeline (no Arrow extension needed)
                logger.info("Using optimized DuckDB → Polars pipeline (no Arrow extension needed)")
                
                # ✅ FIX: Execute query in thread pool to avoid blocking
                def execute_query():
                    result = conn.execute(query, params).fetchall()
                    description = conn.description
                    return result, description
                
                result, description = await run_cpu_bound_task(execute_query)
                
                # ✅ FIX: Convert to list of dicts in thread pool (CPU-intensive)
                def convert_to_records():
                    column_names = [desc[0] for desc in description]
                    return [dict(zip(column_names, row)) for row in result]
                
                records = await run_cpu_bound_task(convert_to_records)
                
                # ✅ FIX: Create Polars DataFrame in thread pool (CPU-intensive)
                def create_dataframe():
                    return pl.DataFrame(records)
                
                df = await run_cpu_bound_task(create_dataframe)
                method = "duckdb_optimized_polars"
                
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(
                    f"🚀 Query executed: {len(df):,} records in {duration_ms:.2f}ms "
                    f"using {method} pipeline"
                )
                
                return df
                
            except Exception as e:
                logger.error(f"❌ Query with Polars optimization failed: {e}")
                raise
    
    async def stream_with_arrow_batches(
        self, 
        schema: Schema, 
        batch_size: int = 50000
    ) -> AsyncIterator[pl.DataFrame]:
        """
        🚀 Ultra-fast streaming using Arrow batches
        
        Memory efficient streaming with:
        - Arrow batch processing
        - Polars DataFrame chunks
        - Async iteration
        """
        async with self.connection_pool.acquire() as conn:
            try:
                # Check if Arrow is available
                def check_arrow():
                    try:
                        conn.execute("INSTALL arrow; LOAD arrow;")
                        return True
                    except Exception:
                        return False
                
                use_arrow = await asyncio.to_thread(check_arrow)
                if not use_arrow:
                    logger.warning("Arrow extension not available, using traditional streaming")
                
                # ✅ FIX: Get total count in thread pool
                def get_total_count():
                    count_result = conn.execute(f'SELECT COUNT(*) FROM "{schema.table_name}"').fetchone()
                    return count_result[0] if count_result else 0
                
                total_records = await asyncio.to_thread(get_total_count)
                logger.info(f"🔄 Starting batch streaming: {total_records:,} total records")
                
                offset = 0
                processed = 0
                
                while offset < total_records:
                    # Query batch
                    query = f'''
                    SELECT * FROM "{schema.table_name}" 
                    ORDER BY created_at 
                    LIMIT {batch_size} OFFSET {offset}
                    '''
                    
                    # ✅ FIX: Execute batch query in thread pool
                    def execute_batch():
                        if use_arrow:
                            try:
                                arrow_batch = conn.execute(query).arrow()
                                df_batch = pl.from_arrow(arrow_batch)
                                return df_batch, "arrow_batch"
                            except Exception:
                                # Fallback to traditional method
                                result = conn.execute(query).fetchall()
                                description = conn.description
                                column_names = [desc[0] for desc in description]
                                records = [dict(zip(column_names, row)) for row in result]
                                df_batch = pl.DataFrame(records)
                                return df_batch, "traditional_batch"
                        else:
                            # Traditional method
                            result = conn.execute(query).fetchall()
                            description = conn.description
                            column_names = [desc[0] for desc in description]
                            records = [dict(zip(column_names, row)) for row in result]
                            df_batch = pl.DataFrame(records)
                            return df_batch, "traditional_batch"
                    
                    df_batch, method = await asyncio.to_thread(execute_batch)
                    
                    if len(df_batch) == 0:
                        break
                    
                    processed += len(df_batch)
                    
                    logger.debug(f"📦 Streaming batch: {len(df_batch):,} records ({processed:,}/{total_records:,}) using {method}")
                    
                    yield df_batch
                    
                    offset += batch_size
                    
                    # Allow other tasks to run
                    await asyncio.sleep(0)
                
                logger.info(f"✅ Batch streaming completed: {processed:,} records processed")
                
            except Exception as e:
                logger.error(f"❌ Batch streaming failed: {e}")
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
                    f"🚀 Parquet export completed: {file_size / (1024*1024):.2f}MB "
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
        
        logger.info(f"🚀 Data analysis completed in {duration_ms:.2f}ms using optimized method")
        
        return {
            "analysis_type": analysis_type,
            "duration_ms": duration_ms,
            "results": analysis_result
        } 