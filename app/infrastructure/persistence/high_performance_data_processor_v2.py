"""
🚀 ULTRA High-Performance Data Processor V2
Optimized for i7 10th Gen + 16GB RAM + SSD

Key Optimizations:
1. Direct DuckDB operations (no intermediate formats)
2. Bulk Arrow/Parquet operations
3. Zero-copy transfers where possible  
4. Proper async/await patterns
5. Memory-efficient streaming
6. Hardware-optimized configurations
"""

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import asyncio
import time
import gc
from typing import Dict, Any, List, Optional, AsyncIterator, Union
from pathlib import Path
import tempfile
import os
import io
from concurrent.futures import ThreadPoolExecutor
import json

from app.domain.entities.schema import Schema
from app.config.logging_config import logger
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool


class UltraHighPerformanceProcessor:
    """
    🚀 ULTRA-OPTIMIZED data processor for maximum local performance
    
    Designed for: i7 10th Gen (6 cores, 12 threads) + 16GB RAM + SSD
    Expected performance: 500K-2M rows/second for reads, 200K-1M rows/second for writes
    """
    
    def __init__(self, connection_pool: AsyncDuckDBPool, max_workers: int = 6):
        self.connection_pool = connection_pool
        self.max_workers = max_workers
        
        # Hardware-optimized settings for i7 10th Gen + 16GB RAM
        self.memory_limit = "12GB"  # Use 75% of RAM
        self.threads = 12  # Use all logical cores
        self.arrow_batch_size = 500000  # Optimized for high-end hardware
        self.parquet_chunk_size = 1000000  # Large chunks for SSD
        
    async def ultra_fast_bulk_insert(
        self, 
        schema: Schema, 
        data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        🚀 ULTRA-FAST bulk insert - Direct DuckDB operations only
        
        Performance target: 500K-1M records/second for local hardware
        """
        start_time = time.perf_counter()
        total_records = len(data)
        
        if not data:
            return {"success": True, "records_processed": 0, "duration_ms": 0}
        
        async with self.connection_pool.acquire() as conn:
            try:
                # Apply hardware-optimized DuckDB settings
                await self._optimize_connection_for_writes(conn)
                
                # Method 1: Direct Arrow insertion (fastest for large datasets)
                if total_records >= 50000:
                    result = await self._bulk_insert_via_arrow_direct(conn, schema, data)
                    method = "arrow_direct"
                    
                # Method 2: DuckDB VALUES (fastest for medium datasets)  
                elif total_records >= 1000:
                    result = await self._bulk_insert_via_values_batch(conn, schema, data)
                    method = "values_batch"
                    
                # Method 3: Individual inserts (only for small datasets)
                else:
                    result = await self._bulk_insert_individual(conn, schema, data)
                    method = "individual"
                
                duration_ms = (time.perf_counter() - start_time) * 1000
                throughput = total_records / (duration_ms / 1000) if duration_ms > 0 else 0
                
                logger.info(
                    f"[ULTRA-PERF] 🚀 Bulk insert: {total_records:,} records in {duration_ms:.2f}ms "
                    f"({int(throughput):,} records/sec) using {method}"
                )
                
                return {
                    "success": True,
                    "records_processed": total_records,
                    "duration_ms": duration_ms,
                    "throughput_rps": int(throughput),
                    "method": method
                }
                
            except Exception as e:
                logger.error(f"❌ Ultra-fast bulk insert failed: {e}")
                raise
    
    async def _optimize_connection_for_writes(self, conn):
        """Apply write-optimized DuckDB settings"""
        def optimize():
            # Hardware-optimized settings for i7 10th Gen + 16GB RAM
            conn.execute(f"SET memory_limit = '{self.memory_limit}'")
            conn.execute(f"SET threads = {self.threads}")
            conn.execute("SET enable_progress_bar = false")
            conn.execute("SET enable_profiling = false")
            
            # Write optimizations
            conn.execute("SET checkpoint_threshold = '1GB'")  # Less frequent checkpoints
            conn.execute("SET wal_autocheckpoint = 0")  # Disable auto-checkpoint during bulk writes
            conn.execute("SET synchronous = OFF")  # Faster writes (safe for local use)
            conn.execute("SET temp_directory = '/tmp'")  # Fast temp storage
            
            # Enable all optimizers
            conn.execute("SET disabled_optimizers = ''")
            
        await asyncio.to_thread(optimize)
    
    async def _bulk_insert_via_arrow_direct(
        self, 
        conn, 
        schema: Schema, 
        data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        🚀 FASTEST: Direct Arrow → DuckDB insertion (zero intermediate files)
        """
        def insert_arrow():
            import uuid
            from datetime import datetime
            
            # Prepare data with system columns
            enhanced_data = []
            for record in data:
                enhanced_record = {
                    "id": str(uuid.uuid4()),
                    "created_at": datetime.now(),
                    "version": 1,
                    **record
                }
                enhanced_data.append(enhanced_record)
            
            # Create Polars DataFrame (fastest for this size)
            df = pl.DataFrame(enhanced_data)
            
            # Convert to PyArrow (zero-copy when possible)
            arrow_table = df.to_arrow()
            
            # Direct Arrow → DuckDB insertion (fastest method)
            conn.execute("BEGIN TRANSACTION")
            
            # Register Arrow table as virtual table
            conn.register("temp_arrow_data", arrow_table)
            
            # Insert from Arrow table with conflict resolution
            insert_sql = f"""
            INSERT OR IGNORE INTO "{schema.table_name}" 
            SELECT * FROM temp_arrow_data
            """
            
            result = conn.execute(insert_sql)
            conn.execute("COMMIT")
            
            # Clean up
            conn.unregister("temp_arrow_data")
            
            return {"rows_inserted": len(data)}
        
        return await asyncio.to_thread(insert_arrow)
    
    async def _bulk_insert_via_values_batch(
        self, 
        conn, 
        schema: Schema, 
        data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        🚀 FAST: Batched VALUES insertion (good for medium datasets)
        """
        def insert_values():
            import uuid
            from datetime import datetime
            
            # Prepare column names
            system_columns = ["id", "created_at", "version"]
            property_columns = [prop.name for prop in schema.properties]
            all_columns = system_columns + property_columns
            columns_str = ", ".join(f'"{col}"' for col in all_columns)
            
            # Process in batches of 1000 for optimal performance
            batch_size = 1000
            total_inserted = 0
            
            conn.execute("BEGIN TRANSACTION")
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                # Build VALUES clause
                values_parts = []
                for record in batch:
                    # System values
                    values = [
                        f"'{uuid.uuid4()}'",
                        f"'{datetime.now().isoformat()}'",
                        "1"
                    ]
                    
                    # Property values
                    for prop in schema.properties:
                        value = record.get(prop.name)
                        if value is None:
                            values.append("NULL")
                        elif prop.type in ["string", "date", "datetime"]:
                            # Escape single quotes
                            escaped_value = str(value).replace("'", "''")
                            values.append(f"'{escaped_value}'")
                        else:
                            values.append(str(value))
                    
                    values_parts.append(f"({', '.join(values)})")
                
                # Execute batch insert
                values_clause = ", ".join(values_parts)
                insert_sql = f"""
                INSERT OR IGNORE INTO "{schema.table_name}" ({columns_str})
                VALUES {values_clause}
                """
                
                conn.execute(insert_sql)
                total_inserted += len(batch)
            
            conn.execute("COMMIT")
            
            return {"rows_inserted": total_inserted}
        
        return await asyncio.to_thread(insert_values)
    
    async def _bulk_insert_individual(
        self, 
        conn, 
        schema: Schema, 
        data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Individual inserts for small datasets"""
        def insert_individual():
            import uuid
            from datetime import datetime
            
            system_columns = ["id", "created_at", "version"]
            property_columns = [prop.name for prop in schema.properties]
            all_columns = system_columns + property_columns
            
            columns_str = ", ".join(f'"{col}"' for col in all_columns)
            placeholders = ", ".join(["?"] * len(all_columns))
            
            insert_sql = f"""
            INSERT OR IGNORE INTO "{schema.table_name}" ({columns_str})
            VALUES ({placeholders})
            """
            
            conn.execute("BEGIN TRANSACTION")
            
            for record in data:
                # Prepare values
                values = [
                    str(uuid.uuid4()),
                    datetime.now().isoformat(),
                    1
                ]
                
                for prop in schema.properties:
                    values.append(record.get(prop.name))
                
                conn.execute(insert_sql, values)
            
            conn.execute("COMMIT")
            
            return {"rows_inserted": len(data)}
        
        return await asyncio.to_thread(insert_individual)
    
    async def ultra_fast_query(
        self, 
        schema: Schema, 
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> pl.DataFrame:
        """
        🚀 ULTRA-FAST querying with direct DuckDB → Arrow → Polars pipeline
        
        Performance target: 1-2M records/second for local hardware
        """
        start_time = time.perf_counter()
        
        async with self.connection_pool.acquire() as conn:
            try:
                # Apply read optimizations
                await self._optimize_connection_for_reads(conn)
                
                # Build optimized query
                query, params = self._build_optimized_query(schema, filters, limit, offset)
                
                # Execute with best method based on result size
                def execute_query():
                    try:
                        # Try direct Arrow method (fastest for large results)
                        arrow_result = conn.execute(query, params).arrow()
                        df = pl.from_arrow(arrow_result)
                        return df, "arrow_direct"
                    except Exception:
                        # Fallback to DuckDB DataFrame method
                        try:
                            pandas_df = conn.execute(query, params).df()
                            df = pl.from_pandas(pandas_df)
                            return df, "pandas_conversion"
                        except Exception:
                            # Final fallback
                            result = conn.execute(query, params).fetchall()
                            columns = [desc[0] for desc in conn.description]
                            records = [dict(zip(columns, row)) for row in result]
                            df = pl.DataFrame(records)
                            return df, "manual_conversion"
                
                df, method = await asyncio.to_thread(execute_query)
                
                duration_ms = (time.perf_counter() - start_time) * 1000
                throughput = len(df) / (duration_ms / 1000) if duration_ms > 0 else 0
                
                logger.info(
                    f"[ULTRA-PERF] 🚀 Query: {len(df):,} records in {duration_ms:.2f}ms "
                    f"({int(throughput):,} records/sec) using {method}"
                )
                
                return df
                
            except Exception as e:
                logger.error(f"❌ Ultra-fast query failed: {e}")
                raise
    
    async def _optimize_connection_for_reads(self, conn):
        """Apply read-optimized DuckDB settings"""
        def optimize():
            # Hardware-optimized settings
            conn.execute(f"SET memory_limit = '{self.memory_limit}'")
            conn.execute(f"SET threads = {self.threads}")
            conn.execute("SET enable_progress_bar = false")
            conn.execute("SET enable_profiling = false")
            
            # Read optimizations
            conn.execute("SET enable_object_cache = true")
            conn.execute("SET disabled_optimizers = ''")  # Enable all optimizers
            conn.execute("SET temp_directory = '/tmp'")
            
        await asyncio.to_thread(optimize)
    
    def _build_optimized_query(
        self, 
        schema: Schema, 
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> tuple[str, list]:
        """Build optimized query with proper parameter binding"""
        query_parts = [f'SELECT * FROM "{schema.table_name}"']
        params = []
        
        # Add filters
        if filters:
            where_conditions = []
            for field, value in filters.items():
                where_conditions.append(f'"{field}" = ?')
                params.append(value)
            
            if where_conditions:
                query_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        # Add ordering for consistent results
        query_parts.append("ORDER BY id")
        
        # Add pagination
        if limit:
            query_parts.append(f"LIMIT {limit}")
            
        if offset:
            query_parts.append(f"OFFSET {offset}")
        
        return " ".join(query_parts), params
    
    async def ultra_fast_stream(
        self, 
        schema: Schema,
        batch_size: int = 500000,  # Large batches for high-end hardware
        filters: Optional[Dict[str, Any]] = None
    ) -> AsyncIterator[pl.DataFrame]:
        """
        🚀 ULTRA-FAST streaming with optimized batching
        
        Uses cursor-based pagination for consistent performance
        """
        async with self.connection_pool.acquire() as conn:
            try:
                await self._optimize_connection_for_reads(conn)
                
                # Get total count
                count_query, count_params = self._build_optimized_query(schema, filters)
                count_query = count_query.replace("SELECT *", "SELECT COUNT(*)")
                
                def get_count():
                    result = conn.execute(count_query, count_params).fetchone()
                    return result[0] if result else 0
                
                total_records = await asyncio.to_thread(get_count)
                
                logger.info(f"[ULTRA-PERF] 🔄 Starting stream: {total_records:,} records, batch size {batch_size:,}")
                
                # Stream using LIMIT/OFFSET (optimized for DuckDB)
                processed = 0
                start_time = time.perf_counter()
                
                while processed < total_records:
                    # Get batch
                    query, params = self._build_optimized_query(
                        schema, filters, limit=batch_size, offset=processed
                    )
                    
                    def get_batch():
                        try:
                            arrow_result = conn.execute(query, params).arrow()
                            return pl.from_arrow(arrow_result), "arrow_direct"
                        except Exception:
                            result = conn.execute(query, params).fetchall()
                            columns = [desc[0] for desc in conn.description]
                            records = [dict(zip(columns, row)) for row in result]
                            return pl.DataFrame(records), "manual"
                    
                    df_batch, method = await asyncio.to_thread(get_batch)
                    
                    if len(df_batch) == 0:
                        break
                    
                    processed += len(df_batch)
                    elapsed = time.perf_counter() - start_time
                    throughput = processed / elapsed if elapsed > 0 else 0
                    
                    logger.info(
                        f"[ULTRA-PERF] 📦 Batch: {len(df_batch):,} records "
                        f"({processed:,}/{total_records:,}) - {int(throughput):,} records/sec"
                    )
                    
                    yield df_batch
                    
                    # Memory cleanup
                    del df_batch
                    gc.collect()
                    
                    # Small yield to event loop
                    await asyncio.sleep(0.001)
                    
                    if len(df_batch) < batch_size:
                        break
                
            except Exception as e:
                logger.error(f"❌ Ultra-fast streaming failed: {e}")
                raise
    
    async def export_to_parquet_optimized(
        self, 
        schema: Schema, 
        output_path: Path,
        compression: str = "snappy"
    ) -> Dict[str, Any]:
        """
        🚀 ULTRA-FAST Parquet export using direct DuckDB COPY
        """
        start_time = time.perf_counter()
        
        async with self.connection_pool.acquire() as conn:
            try:
                await self._optimize_connection_for_reads(conn)
                
                def export():
                    # Direct DuckDB to Parquet (fastest possible method)
                    export_sql = f"""
                    COPY (SELECT * FROM "{schema.table_name}") 
                    TO '{output_path}' 
                    (FORMAT PARQUET, COMPRESSION '{compression}')
                    """
                    
                    conn.execute(export_sql)
                
                await asyncio.to_thread(export)
                
                # Get file stats
                file_size = output_path.stat().st_size if output_path.exists() else 0
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                logger.info(
                    f"[ULTRA-PERF] 🚀 Parquet export: {file_size / (1024*1024):.2f}MB "
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