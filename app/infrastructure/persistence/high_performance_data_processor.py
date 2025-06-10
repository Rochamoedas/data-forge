"""
🚀 Ultra High-Performance Data Processor
Combines Polars, PyArrow, and DuckDB for maximum performance:
# - Polars: Lightning-fast DataFrame operations with lazy evaluation.
# - PyArrow: Zero-copy data interchange and columnar memory format.
# - DuckDB: Vectorized analytical queries with Arrow integration.
# - ConnectorX: Ultra-fast data loading from various sources (if integrated).
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
from app.config.duckdb_config import get_duckdb_config_string, ARROW_EXTENSION_CONFIG, DEFAULT_STREAMING_STRATEGY, OPERATION_PROFILES
from app.domain.exceptions import DataProcessingError, DatabaseError
from app.infrastructure.persistence.duckdb.duckdb_utils import create_csv_from_dicts, execute_duckdb_copy_from_csv


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
    HighPerformanceDataProcessor (HPDP)
    
    This class orchestrates high-performance data processing tasks by leveraging
    Polars for DataFrame manipulation, PyArrow for efficient data interchange (especially
    with DuckDB), and DuckDB for fast analytical queries and data storage.

    Key Performance Features:
    - Zero-copy data transfers between Polars and PyArrow, and PyArrow and DuckDB where possible.
    - Lazy evaluation capabilities of Polars for optimizing complex transformations.
    - Vectorized query execution engine of DuckDB.
    - Asynchronous operations using asyncio and ThreadPoolExecutor for CPU-bound tasks
      to prevent blocking the event loop, crucial for responsive applications (e.g., FastAPI).
    - Memory-efficient streaming for large datasets using configurable strategies (offset or keyset).
    - Optimized bulk data ingestion using DuckDB's COPY statement via temporary CSV files.
    """
    
    def __init__(self, connection_pool: AsyncDuckDBPool, max_workers: int = 8):
        """
        Initializes the HighPerformanceDataProcessor.

        Args:
            connection_pool: An instance of AsyncDuckDBPool for acquiring DuckDB connections.
            max_workers: The maximum number of worker threads for the ThreadPoolExecutor.
                         This is used for running synchronous, CPU-bound tasks off the main event loop.
        """
        self.connection_pool = connection_pool
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers) # For CPU-bound sync tasks
        
        # These seem to be instance-specific settings, potentially for tuning.
        # Consider if these should be configurable or derived from schema/operation profiles.
        self.chunk_size = 500000  # General purpose chunk size, usage context might need review.
        self.arrow_batch_size = 250000  # Batch size for Arrow operations.
        self.csv_batch_size = 100000  # Batch size for CSV writing, if done in chunks (currently not).
        logger.info(f"[HPDP] Initialized with max_workers: {max_workers}. Connection Pool: {connection_pool}")
        
    async def bulk_insert_ultra_fast(
        self, 
        schema: Schema, 
        data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Performs an ultra-fast bulk insert of data into the specified schema's table.

        This method uses a highly optimized pipeline:
        1. Raw data (List of Dictionaries) is converted to a Polars DataFrame.
        2. Schema-based type casting and transformations are applied using Polars.
        3. The Polars DataFrame is converted to a PyArrow Table.
        4. The PyArrow Table data is written to a temporary CSV file.
        5. DuckDB's `COPY FROM` command loads data from the CSV into the target table,
           often using an intermediate temporary table for `INSERT OR IGNORE` semantics.

        Args:
            schema: The domain Schema object defining the target table and data structure.
            data: A list of dictionaries, where each dictionary represents a record.

        Returns:
            A dictionary containing metrics of the bulk insert operation, including
            success status, records processed, duration, and throughput.

        Raises:
            DataProcessingError: If an error occurs during Polars, PyArrow, or CSV operations.
            DatabaseError: If a DuckDB specific error occurs during the COPY operation.
        """
        start_time = time.perf_counter()
        total_records = len(data)
        
        if not data:
            logger.info(f"[HPDP] Bulk insert for schema '{schema.name}': No data provided. Skipping operation.")
            return {"success": True, "records_processed": 0, "duration_ms": 0}
        
        logger.info(f"[HPDP] Starting ultra-fast bulk insert for {total_records:,} records into schema '{schema.name}'.")

        try:
            # Step 1: Convert to Polars DataFrame with schema validation
            logger.debug(f"[HPDP] Step 1/4: Converting {total_records:,} records to Polars DataFrame for schema '{schema.name}'.")
            df = await self._create_polars_dataframe(data, schema)
            logger.debug(f"[HPDP] Polars DataFrame created with shape: {df.shape} for schema '{schema.name}'.")
            
            # Step 2: Apply data transformations and validations using Polars
            logger.debug(f"[HPDP] Step 2/4: Applying Polars transformations for schema '{schema.name}'.")
            df_processed = await self._apply_polars_transformations(df, schema)
            logger.debug(f"[HPDP] Polars transformations applied. Processed DataFrame shape: {df_processed.shape} for schema '{schema.name}'.")
            
            # Step 3: Convert to Arrow format (zero-copy when possible)
            logger.debug(f"[HPDP] Step 3/4: Converting Polars DataFrame to Arrow Table for schema '{schema.name}'.")
            arrow_table = await self._convert_to_arrow(df_processed)
            logger.debug(f"[HPDP] Arrow Table created with {arrow_table.num_rows} rows for schema '{schema.name}'.")
            
            # Step 4: Insert via DuckDB utility (CSV COPY based)
            logger.debug(f"[HPDP] Step 4/4: Inserting Arrow Table data via DuckDB COPY for schema '{schema.name}'.")
            # _insert_via_arrow now uses the duckdb_utils for CSV COPY
            insert_result = await self._insert_via_arrow(schema, arrow_table)
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            throughput = total_records / (duration_ms / 1000) if duration_ms > 0 else float('inf')
            
            logger.info(
                f"[HPDP] ✅ Ultra-fast bulk insert for schema '{schema.name}' COMPLETED. "
                f"Processed: {total_records:,} records. Duration: {duration_ms:.2f}ms. "
                f"Throughput: {throughput:,.0f} records/sec. Method: {insert_result.get('method', 'polars_arrow_duckdb_csv_copy')}."
            )
            
            return {
                "success": True,
                "records_processed": total_records,
                "duration_ms": duration_ms,
                "throughput_rps": int(throughput),
                # Method detail from _insert_via_arrow might be more specific now
                "method": insert_result.get('method', 'polars_arrow_duckdb_csv_copy')
            }
            
        except pl.PolarsError as e:
            logger.error(f"❌ [HPDP] Polars DataFrame operation FAILED during bulk insert for schema '{schema.name}': {e}")
            raise DataProcessingError(message=f"Polars DataFrame error for schema '{schema.name}' during bulk insert", underlying_exception=e)
        except pa.ArrowException as e:
            logger.error(f"❌ [HPDP] PyArrow operation FAILED during bulk insert for schema '{schema.name}': {e}")
            raise DataProcessingError(message=f"PyArrow error for schema '{schema.name}' during bulk insert", underlying_exception=e)
        except duckdb.Error as e: # Should be caught by DatabaseError from utils, but as a fallback
            logger.error(f"❌ [HPDP] DuckDB FAILED during bulk insert for schema '{schema.name}': {e}")
            raise DatabaseError(message=f"DuckDB error for schema '{schema.name}' during bulk insert", underlying_exception=e)
        except DataProcessingError as e: # Catch specific custom errors if raised by sub-methods or utils
            logger.error(f"❌ [HPDP] Data processing FAILED during bulk insert for schema '{schema.name}': {e.message}")
            raise # Re-raise to propagate the specific error
        except DatabaseError as e: # Catch specific custom errors if raised by sub-methods or utils
            logger.error(f"❌ [HPDP] Database FAILED during bulk insert for schema '{schema.name}': {e.message}")
            raise # Re-raise
        except Exception as e:
            logger.error(f"❌ [HPDP] Unexpected FAILED during ultra-fast bulk insert for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
            raise DataProcessingError(message=f"Unexpected error during bulk insert for schema '{schema.name}'", underlying_exception=e)
    
    async def _create_polars_dataframe(
        self, 
        data: List[Dict[str, Any]], 
        schema: Schema
    ) -> pl.DataFrame:
        """
        Creates a Polars DataFrame from a list of dictionaries, applying schema-based type casting.
        This method is run in a ThreadPoolExecutor to avoid blocking the asyncio event loop.
        """
        
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
        """
        Inserts data from a PyArrow Table into DuckDB using the CSV COPY method.
        This is an internal method typically called by `bulk_insert_ultra_fast`.
        It converts Arrow data to dictionaries, writes to a temporary CSV, then uses
        `execute_duckdb_copy_from_csv` utility for the actual database insertion.

        Args:
            schema: The domain Schema object.
            arrow_table: The PyArrow Table containing data to be inserted.

        Returns:
            A dictionary with "rows_inserted" (reflecting rows in input Arrow table)
            and "method" indicating the pathway.

        Raises:
            DataProcessingError: For errors during Polars/Arrow conversion or CSV writing.
            DatabaseError: For errors during the DuckDB COPY operation from the utility.
        """
        async with self.connection_pool.acquire() as conn:
            temp_file_path = None # Initialize for ensure cleanup in finally block
            try:
                logger.info(f"[HPDP] _insert_via_arrow: Using Polars → CSV → DuckDB COPY pipeline for schema '{schema.name}'. Input Arrow rows: {arrow_table.num_rows}.")
                
                # Convert Arrow to Polars DataFrame, then to list of dictionaries
                logger.debug(f"[HPDP] _insert_via_arrow: Converting Arrow Table to Polars DataFrame for schema '{schema.name}'.")
                df = pl.from_arrow(arrow_table) # Can raise pa.ArrowException or pl.PolarsError
                logger.debug(f"[HPDP] _insert_via_arrow: Converting Polars DataFrame to dictionaries for schema '{schema.name}'. Shape: {df.shape}.")
                records_data = df.to_dicts() # Can raise pl.PolarsError

                # Create a temporary file for CSV data. It's critical this file is cleaned up.
                # Using delete=False and manual unlink in `finally` is one way to manage this across async calls.
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False,
                                                 newline='', encoding='utf-8') as temp_file_obj:
                    temp_file_path = temp_file_obj.name
                logger.debug(f"[HPDP] _insert_via_arrow: Created temporary CSV file: '{temp_file_path}' for schema '{schema.name}'.")
                
                # Use utility to create CSV from dictionaries.
                # `include_generated_fields=True` as the original logic for this path generated id, created_at, version.
                await create_csv_from_dicts(records_data, schema, temp_file_path, include_generated_fields=True)

                # Retrieve DuckDB configuration string for "bulk_insert" profile
                config_str = get_duckdb_config_string("bulk_insert")
                logger.debug(f"[HPDP] _insert_via_arrow: Using DuckDB config for 'bulk_insert' profile for schema '{schema.name}'.")
                
                # The execute_duckdb_copy_from_csv utility handles transactions.
                rows_copied_estimate = await execute_duckdb_copy_from_csv(
                    db_conn=conn,
                    schema_name=schema.name,
                    table_name=schema.table_name,
                    temp_csv_path=temp_file_path,
                    use_temp_table=True, # Original logic used a temp table for INSERT OR IGNORE
                    config_string=config_str
                )
                
                logger.info(f"[HPDP] _insert_via_arrow: COPY (via util) for schema '{schema.name}' processed an estimated {rows_copied_estimate} records from CSV.")
                # Return value reflects the number of rows in the input Arrow table, as per original method's intent.
                return {"rows_inserted": len(arrow_table), "method": "duckdb_csv_copy_util"}

            except (pl.PolarsError, pa.ArrowException) as e:
                logger.error(f"❌ [HPDP] _insert_via_arrow: Data conversion error (Polars/Arrow) for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise DataProcessingError(message=f"Data conversion error for schema '{schema.name}' during insert", underlying_exception=e)
            # Errors from duckdb_utils (DatabaseError, DataProcessingError for IO)
            except (DataProcessingError, DatabaseError) as e:
                logger.error(f"❌ [HPDP] _insert_via_arrow: Error from utility function for schema '{schema.name}': {type(e).__name__} - {e.message}", exc_info=True)
                # Re-raise, potentially wrapping if more context is needed, but utils already provide good messages.
                raise
            except Exception as e:
                logger.error(f"❌ [HPDP] _insert_via_arrow: Unexpected error for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise DataProcessingError(message=f"Unexpected error during CSV insert process for schema '{schema.name}'", underlying_exception=e)
            finally:
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.unlink(temp_file_path)
                        logger.debug(f"[HPDP] _insert_via_arrow: Cleaned up temporary CSV file: '{temp_file_path}' for schema '{schema.name}'.")
                    except Exception as e_unlink:
                        logger.warning(f"[HPDP] _insert_via_arrow: Failed to cleanup temp CSV file '{temp_file_path}' for schema '{schema.name}': {e_unlink}")
    
    async def query_with_polars_optimization(
        self, 
        schema: Schema, 
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> pl.DataFrame:
        """
        Executes a query with optimizations for Polars DataFrame output.

        This method attempts to use the most performant path for fetching data from
        DuckDB and converting it to a Polars DataFrame:
        1. Sets up DuckDB with a "query_optimized" profile.
        2. Tries to load the Arrow extension for zero-copy transfer if configured.
        3. Builds a SQL query based on provided filters and limit.
        4. Executes the query:
            - If Arrow is available and the dataset is large enough (>=10k records, heuristic),
              it fetches data directly as an Arrow Table and converts to Polars (`DuckDB -> Arrow -> Polars`).
            - Otherwise, it falls back to DuckDB's native `df()` method (optimized Pandas DataFrame)
              and then converts to Polars (`DuckDB -> Pandas -> Polars`).
            - A final fallback to manual fetching and Polars DataFrame creation exists if needed.
        
        Args:
            schema: The domain Schema object for the table being queried.
            filters: Optional dictionary of filters to apply (field: value).
            limit: Optional limit for the number of records to return.

        Returns:
            A Polars DataFrame containing the query results.

        Raises:
            DatabaseError: If a DuckDB error occurs during query execution.
            DataProcessingError: If an error occurs during Polars or Arrow processing.
        """
        start_time = time.perf_counter()
        logger.info(f"[HPDP] Starting Polars optimized query for schema '{schema.name}'. Filters: {filters}, Limit: {limit}.")
        
        async with self.connection_pool.acquire() as conn:
            try:
                # Setup DuckDB optimizations for read performance
                def setup_read_optimizations_sync(): # Renamed for clarity
                    try:
                        db_config_str = get_duckdb_config_string("query_optimized")
                        logger.debug(f"[HPDP] Applying query_optimized DuckDB settings for schema '{schema.name}': \"{db_config_str}\"")
                        conn.execute(db_config_str)
                        
                        if ARROW_EXTENSION_CONFIG["load_by_default"]:
                            logger.debug(f"[HPDP] Attempting to load Arrow extension for schema '{schema.name}'. Install if not found: {ARROW_EXTENSION_CONFIG['install_if_not_found']}.")
                            try:
                                if ARROW_EXTENSION_CONFIG["install_if_not_found"]:
                                    conn.execute("INSTALL arrow")
                                conn.execute("LOAD arrow")
                                logger.info(f"[HPDP] Arrow extension loaded successfully for schema '{schema.name}'.")
                                return True
                            except Exception as arrow_ex:
                                logger.warning(f"[HPDP] Arrow extension failed to load for schema '{schema.name}': {arrow_ex}. Will use non-Arrow fallback.")
                                return False
                        else:
                            logger.info("[HPDP] Arrow extension loading is disabled by default in config.")
                            return False
                    except duckdb.Error as db_ex:
                        logger.warning(f"[HPDP] DuckDB error during read optimizations setup for schema '{schema.name}': {db_ex}. Proceeding without custom setup.")
                        return False
                    except Exception as e:
                        logger.warning(f"[HPDP] Generic error during read optimizations setup for schema '{schema.name}': {e}. Proceeding with defaults.")
                        return False
                
                use_arrow = await asyncio.to_thread(setup_read_optimizations_sync)
                
                # Build optimized query
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
                logger.debug(f"[HPDP] Executing query for schema '{schema.name}': {query} with params: {params}")
                
                # Execute query with the best available method (run in thread pool)
                def execute_optimized_query_sync(): # Renamed for clarity
                    # Heuristic: Use Arrow direct scan for potentially large results if limit is high or not set
                    # and Arrow extension is successfully loaded.
                    # limit can be None, so handle that.
                    use_arrow_path = use_arrow and (limit is None or limit >= 10000)

                    if use_arrow_path:
                        try:
                            logger.info(f"[HPDP] Attempting DuckDB → Arrow → Polars pipeline for schema '{schema.name}'. Query: {query}, Params: {params}")
                            arrow_result = conn.execute(query, params).arrow()
                            df = pl.from_arrow(arrow_result) # Can raise pa.ArrowException or pl.PolarsError
                            logger.debug(f"[HPDP] Successfully fetched {df.shape[0]} records via Arrow for schema '{schema.name}'.")
                            return df, "duckdb_arrow_polars"
                        except (duckdb.Error, pa.ArrowException, pl.PolarsError) as arrow_pipeline_error:
                            logger.warning(f"[HPDP] DuckDB → Arrow → Polars pipeline FAILED for schema '{schema.name}': {arrow_pipeline_error}. Falling back.")
                            # Fall through to the next best method
                    
                    logger.info(f"[HPDP] Using DuckDB → Pandas → Polars pipeline (fallback or default) for schema '{schema.name}'. Query: {query}, Params: {params}")
                    try:
                        result_relation = conn.execute(query, params) # Can raise duckdb.Error
                        df_pandas = result_relation.df() # DuckDB's optimized pandas DataFrame conversion
                        df = pl.from_pandas(df_pandas) # Can raise pl.PolarsError
                        logger.debug(f"[HPDP] Successfully fetched {df.shape[0]} records via Pandas for schema '{schema.name}'.")
                        return df, "duckdb_pandas_polars"
                    except (duckdb.Error, pl.PolarsError) as pandas_pipeline_error:
                        logger.warning(f"[HPDP] DuckDB → Pandas → Polars pipeline FAILED for schema '{schema.name}': {pandas_pipeline_error}. Falling back to manual conversion.")
                        # Final fallback: manual conversion from fetched records
                        result = conn.execute(query, params).fetchall() # Can raise duckdb.Error
                        description = conn.description # Must be called after execute
                        column_names = [desc[0] for desc in description]
                        records = [dict(zip(column_names, row)) for row in result]
                        df = pl.DataFrame(records) # Can raise pl.PolarsError
                        logger.debug(f"[HPDP] Successfully fetched {df.shape[0]} records via manual conversion for schema '{schema.name}'.")
                        return df, "duckdb_manual_polars"

                df, method = await run_cpu_bound_task(execute_optimized_query_sync)
                
                duration_ms = (time.perf_counter() - start_time) * 1000
                throughput = len(df) / (duration_ms / 1000) if duration_ms > 0 else float('inf')
                
                log_query = query if 'query' in locals() else "Query not yet constructed"
                logger.info(
                    f"[HPDP] ✅ Query for schema '{schema.name}' EXECUTED. Method: {method}. "
                    f"Fetched: {len(df):,} records. Duration: {duration_ms:.2f}ms. "
                    f"Throughput: {throughput:,.0f} records/sec."
                )
                return df
                
            except duckdb.Error as e:
                logger.error(f"❌ [HPDP] DuckDB FAILED during query for schema '{schema.name}': {e}. Query: {log_query if 'log_query' in locals() else 'N/A'}", exc_info=True)
                raise DatabaseError(message=f"DuckDB query failed for schema '{schema.name}'", underlying_exception=e)
            except pl.PolarsError as e:
                logger.error(f"❌ [HPDP] Polars FAILED during query processing for schema '{schema.name}': {e}", exc_info=True)
                raise DataProcessingError(message=f"Polars processing failed after query for schema '{schema.name}'", underlying_exception=e)
            except pa.ArrowException as e:
                logger.error(f"❌ [HPDP] Arrow FAILED during query processing for schema '{schema.name}': {e}", exc_info=True)
                raise DataProcessingError(message=f"Arrow processing failed after query for schema '{schema.name}'", underlying_exception=e)
            except Exception as e: # Catch any other unexpected error
                logger.error(f"❌ [HPDP] Unexpected FAILED during query for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise DataProcessingError(message=f"Unexpected error during query for schema '{schema.name}'", underlying_exception=e)
    
    async def stream_with_arrow_batches(
        self, 
        schema: Schema, 
        batch_size: int = 50000,
        streaming_strategy: Optional[str] = None
    ) -> AsyncIterator[pl.DataFrame]:
        """
        Streams data from DuckDB in batches, using either offset or keyset pagination.

        This method is optimized for handling large datasets by processing them in
        chunks, which helps in managing memory usage. It supports two pagination strategies:
        - "offset": Traditional SQL OFFSET-based pagination. Can be less performant on very large tables.
        - "keyset": Uses a continuously updated "last value" of a sort key (e.g., "id > last_id")
                    for pagination. Generally more performant for large, indexed tables.

        The choice of strategy can be passed as a parameter or defaults to the application's
        configured default (`DEFAULT_STREAMING_STRATEGY`). Arrow extension is used if available
        and configured for faster data transfer.

        Args:
            schema: The domain Schema object for the table being streamed.
            batch_size: The number of records to fetch in each batch.
            streaming_strategy: Optional. The pagination strategy ("offset" or "keyset").
                                If None, uses the configured default.

        Yields:
            Polars DataFrame for each batch of records.

        Raises:
            ValueError: If an unknown streaming strategy is specified.
            DatabaseError: If a DuckDB error occurs during query execution or setup.
            DataProcessingError: If an error occurs during Polars/Arrow processing or
                                 if keyset pagination fails due to missing sort column.
        """
        # Determine the streaming strategy
        if streaming_strategy is None:
            streaming_profile_config = OPERATION_PROFILES.get("streaming", {})
            strategy = streaming_profile_config.get("strategy", DEFAULT_STREAMING_STRATEGY)
            logger.debug(f"[HPDP] No streaming strategy passed for schema '{schema.name}', using default from config: '{strategy}'.")
        else:
            strategy = streaming_strategy
            logger.debug(f"[HPDP] Explicit streaming strategy for schema '{schema.name}': '{strategy}'.")

        logger.info(f"[HPDP] Starting data stream for schema '{schema.name}'. Batch size: {batch_size:,}, Strategy: '{strategy}'.")

        async with self.connection_pool.acquire() as conn:
            try:
                # Setup DuckDB connection for streaming (applies "streaming" profile)
                def setup_streaming_optimizations_sync(): # Renamed for clarity
                    try:
                        db_config_str = get_duckdb_config_string("streaming")
                        logger.debug(f"[HPDP] Applying 'streaming' DuckDB profile for schema '{schema.name}': \"{db_config_str}\"")
                        conn.execute(db_config_str)
                        
                        if ARROW_EXTENSION_CONFIG["load_by_default"]:
                            logger.debug(f"[HPDP] Attempting to load Arrow extension for streaming schema '{schema.name}'. Install if not found: {ARROW_EXTENSION_CONFIG['install_if_not_found']}.")
                            try:
                                if ARROW_EXTENSION_CONFIG["install_if_not_found"]:
                                    conn.execute("INSTALL arrow")
                                conn.execute("LOAD arrow")
                                logger.info(f"[HPDP] Arrow extension loaded successfully for streaming schema '{schema.name}'.")
                                return True
                            except Exception as arrow_ex:
                                logger.warning(f"[HPDP] Arrow extension failed to load for streaming schema '{schema.name}': {arrow_ex}. Will use non-Arrow fallback.")
                                return False
                        else:
                            logger.info("[HPDP] Arrow extension loading is disabled by default in config for streaming.")
                            return False
                    except duckdb.Error as db_ex:
                        logger.warning(f"[HPDP] DuckDB error during streaming optimizations setup for schema '{schema.name}': {db_ex}. Proceeding with defaults.")
                        return False
                    except Exception as e:
                        logger.warning(f"[HPDP] Generic error during streaming optimizations setup for schema '{schema.name}': {e}. Proceeding with defaults.")
                        return False
                
                use_arrow = await asyncio.to_thread(setup_streaming_optimizations_sync)
                log_arrow_status = "Arrow-optimized" if use_arrow else "Traditional (non-Arrow)"
                logger.info(f"[HPDP] Streaming data fetch method for schema '{schema.name}': {log_arrow_status}.")

                stream_start_time = time.perf_counter()
                total_records_streamed_session = 0 # Tracks records for the entire stream session
                batch_number = 0
                
                # Defines how a batch is fetched and converted to Polars DataFrame
                async def fetch_and_convert_batch(current_query: str, current_params: List[Any]):
                    logger.debug(f"[HPDP] Batch {batch_number}: Executing query for schema '{schema.name}': {current_query} with params {current_params}")
                    if use_arrow:
                        try:
                            arrow_batch = conn.execute(current_query, current_params).arrow()
                            df_batch = pl.from_arrow(arrow_batch)
                            logger.debug(f"[HPDP] Batch {batch_number}: Fetched {len(df_batch)} records via Arrow for schema '{schema.name}'.")
                            return df_batch, "arrow"
                        except (duckdb.Error, pa.ArrowException, pl.PolarsError) as arrow_fetch_error:
                            logger.warning(f"[HPDP] Batch {batch_number}: Arrow fetch failed for schema '{schema.name}' ({arrow_fetch_error}), falling back to traditional fetch.")
                    
                    # Fallback or default traditional fetch
                    result = conn.execute(current_query, current_params).fetchall()
                    description = conn.description
                    column_names = [desc[0] for desc in description]
                    records = [dict(zip(column_names, row)) for row in result]
                    df_batch = pl.DataFrame(records)
                    logger.debug(f"[HPDP] Batch {batch_number}: Fetched {len(df_batch)} records via traditional method for schema '{schema.name}'.")
                    return df_batch, "traditional"

                # --- Keyset Pagination Strategy ---
                if strategy == "keyset":
                    keyset_sort_column = "id"  # Configurable: Assume 'id' for now. Best if indexed and unique.
                    last_keyset_value = None   # Stores the last value of the sort key from the previous batch.
                    logger.info(f"[HPDP] Initiating KEYSET pagination strategy for schema '{schema.name}' on sort key '{keyset_sort_column}'.")

                    while True:
                        batch_number += 1
                        query_parts = [f'SELECT * FROM "{schema.table_name}"']
                        query_params = []

                        if last_keyset_value is not None:
                            query_parts.append(f'WHERE "{keyset_sort_column}" > ?')
                            query_params.append(last_keyset_value)

                        query_parts.append(f'ORDER BY "{keyset_sort_column}" ASC') # Ensure consistent ordering
                        query_parts.append(f'LIMIT {batch_size}')
                        current_query = " ".join(query_parts)

                        try:
                            df_batch, fetch_method = await asyncio.to_thread(fetch_and_convert_batch, current_query, query_params)
                        except duckdb.Error as e:
                            logger.error(f"❌ [HPDP] Keyset Batch {batch_number}: DuckDB FAILED for schema '{schema.name}': {e}. Query: {current_query}", exc_info=True)
                            raise DatabaseError(f"DuckDB error during keyset streaming for schema '{schema.name}' (Batch {batch_number})", underlying_exception=e)
                        except (pl.PolarsError, pa.ArrowException) as e:
                            logger.error(f"❌ [HPDP] Keyset Batch {batch_number}: Data processing (Polars/Arrow) FAILED for schema '{schema.name}': {e}", exc_info=True)
                            raise DataProcessingError(f"Data processing error during keyset streaming for schema '{schema.name}' (Batch {batch_number})", underlying_exception=e)
                        except Exception as e: # Catch any other unexpected error during batch processing
                            logger.error(f"❌ [HPDP] Keyset Batch {batch_number}: Unexpected FAILED for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                            raise DataProcessingError(f"Unexpected error during keyset streaming for schema '{schema.name}' (Batch {batch_number})", underlying_exception=e)

                        if len(df_batch) == 0: # No more records found
                            logger.info(f"[HPDP] Keyset pagination for schema '{schema.name}': No more records found. Ending stream at batch {batch_number}.")
                            break

                        # Update last_keyset_value for the next iteration
                        if keyset_sort_column in df_batch.columns: # Ensure sort column is in DataFrame
                            raw_val = df_batch[keyset_sort_column][-1] # Get last value of sort key
                            last_keyset_value = raw_val.as_py() if hasattr(raw_val, 'as_py') else raw_val # Convert if Polars type
                        else:
                            logger.error(f"❌ [HPDP] Keyset sort column '{keyset_sort_column}' NOT FOUND in results from schema '{schema.name}'. Keyset pagination cannot proceed.")
                            raise DataProcessingError(f"Keyset sort column '{keyset_sort_column}' missing in results for schema '{schema.name}'.")

                        total_records_streamed_session += len(df_batch)
                        elapsed_time_session = time.perf_counter() - stream_start_time
                        current_throughput_session = total_records_streamed_session / elapsed_time_session if elapsed_time_session > 0 else float('inf')
                        logger.info(f"[HPDP] Keyset Batch {batch_number} for schema '{schema.name}': Fetched {len(df_batch):,} records (Total streamed: {total_records_streamed_session:,}). Method: {fetch_method}. Throughput: {current_throughput_session:,.0f} recs/sec.")
                        yield df_batch
                        del df_batch; gc.collect(); await asyncio.sleep(0.001) # Memory management and cooperative multitasking

                        if len(df_batch) < batch_size: # Reached the end of the dataset
                            logger.info(f"[HPDP] Keyset pagination for schema '{schema.name}': Fetched last batch (size {len(df_batch)} < requested {batch_size}). Ending stream at batch {batch_number}.")
                            break
                
                # --- Offset Pagination Strategy ---
                elif strategy == "offset":
                    logger.info(f"[HPDP] Initiating OFFSET pagination strategy for schema '{schema.name}'.")
                    # Get total record count for progress calculation with offset strategy
                    def get_total_count_sync(): # Renamed for clarity
                        count_result = conn.execute(f'SELECT COUNT(*) FROM "{schema.table_name}"').fetchone()
                        return count_result[0] if count_result else 0
                    total_records_for_offset = await asyncio.to_thread(get_total_count_sync)
                    logger.info(f"[HPDP] Offset streaming for schema '{schema.name}': Total records to fetch: {total_records_for_offset:,}.")

                    current_offset = 0
                    while current_offset < total_records_for_offset:
                        batch_number += 1
                        current_query = f'SELECT * FROM "{schema.table_name}" ORDER BY id LIMIT {batch_size} OFFSET {current_offset}' # Assuming 'id' for ORDER BY for consistent offset

                        try:
                            df_batch, fetch_method = await asyncio.to_thread(fetch_and_convert_batch, current_query, [])
                        except duckdb.Error as e:
                            logger.error(f"❌ [HPDP] Offset Batch {batch_number}: DuckDB FAILED for schema '{schema.name}': {e}. Query: {current_query}", exc_info=True)
                            raise DatabaseError(f"DuckDB error during offset streaming for schema '{schema.name}' (Batch {batch_number})", underlying_exception=e)
                        except (pl.PolarsError, pa.ArrowException) as e:
                            logger.error(f"❌ [HPDP] Offset Batch {batch_number}: Data processing (Polars/Arrow) FAILED for schema '{schema.name}': {e}", exc_info=True)
                            raise DataProcessingError(f"Data processing error during offset streaming for schema '{schema.name}' (Batch {batch_number})", underlying_exception=e)
                        except Exception as e:
                            logger.error(f"❌ [HPDP] Offset Batch {batch_number}: Unexpected FAILED for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                            raise DataProcessingError(f"Unexpected error during offset streaming for schema '{schema.name}' (Batch {batch_number})", underlying_exception=e)

                        if len(df_batch) == 0: # Should not happen if current_offset < total_records_for_offset, but as safeguard
                            logger.warning(f"[HPDP] Offset pagination for schema '{schema.name}': No records found at offset {current_offset}, though expected {total_records_for_offset - current_offset}. Ending stream early at batch {batch_number}.")
                            break

                        batch_length = len(df_batch)
                        current_offset += batch_length
                        total_records_streamed_session += batch_length
                        elapsed_time_session = time.perf_counter() - stream_start_time
                        current_throughput_session = total_records_streamed_session / elapsed_time_session if elapsed_time_session > 0 else float('inf')
                        progress_pct = (current_offset / total_records_for_offset) * 100 if total_records_for_offset > 0 else 0
                        logger.info(f"[HPDP] Offset Batch {batch_number} for schema '{schema.name}': Fetched {batch_length:,} records (Processed: {current_offset:,}/{total_records_for_offset:,} - {progress_pct:.1f}%). Method: {fetch_method}. Throughput: {current_throughput_session:,.0f} recs/sec.")
                        yield df_batch
                        del df_batch; gc.collect(); await asyncio.sleep(0.001)

                        # No explicit check for `batch_length < batch_size` to break, as `while current_offset < total_records_for_offset` handles termination.
                else:
                    logger.error(f"❌ [HPDP] Unknown streaming strategy '{strategy}' specified for schema '{schema.name}'. Cannot proceed.")
                    raise ValueError(f"Unknown streaming strategy: {strategy}")

                final_elapsed_session = time.perf_counter() - stream_start_time
                final_throughput_session = total_records_streamed_session / final_elapsed_session if final_elapsed_session > 0 else float('inf')
                logger.info(f"[HPDP] ✅ Streaming session (Strategy: '{strategy}') COMPLETED for schema '{schema.name}'. "
                            f"Total records streamed: {total_records_streamed_session:,}. Duration: {final_elapsed_session:.2f}s. "
                            f"Average throughput: {final_throughput_session:,.0f} recs/sec.")

            except duckdb.Error as e: # Catch DuckDB errors during initial setup (e.g. total count for offset)
                logger.error(f"❌ [HPDP] DuckDB FAILED during streaming setup for schema '{schema.name}': {e}", exc_info=True)
                raise DatabaseError(message=f"DuckDB setup error for streaming on schema '{schema.name}'", underlying_exception=e)
            except ValueError as e: # Catch unknown strategy error
                logger.error(f"❌ [HPDP] Streaming FAILED for schema '{schema.name}' due to configuration error: {e}", exc_info=True)
                raise e # Re-raise ValueError
            except Exception as e: # Catch any other unexpected errors during stream setup phase
                logger.error(f"❌ [HPDP] Unexpected FAILED during streaming setup for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise DataProcessingError(message=f"Unexpected error setting up stream for schema '{schema.name}'", underlying_exception=e)
    
    async def export_to_parquet_optimized(
        self, 
        schema: Schema, 
        output_path: Path,
        compression: str = "snappy"
    ) -> Dict[str, Any]:
        """
        Exports data from the specified schema's table to a Parquet file.

        This method uses DuckDB's efficient `COPY TO` statement for direct Parquet export,
        which is generally very performant. It applies default DuckDB settings during the operation.

        Args:
            schema: The domain Schema object of the table to export.
            output_path: The Path object representing the desired output Parquet file path.
            compression: The compression Codec to use for Parquet (e.g., "snappy", "gzip", "zstd").

        Returns:
            A dictionary containing information about the export, including success status,
            file path, file size, duration, and compression used.

        Raises:
            DatabaseError: If a DuckDB error occurs during the export.
            DataProcessingError: If a file system (IOError) or other unexpected error occurs.
        """
        start_time = time.perf_counter()
        logger.info(f"[HPDP] Starting Parquet export for schema '{schema.name}' to '{output_path}'. Compression: {compression}.")
        
        async with self.connection_pool.acquire() as conn:
            try:
                # Apply general performance settings (default profile)
                # A specific "export" profile could be created if needed.
                db_config_str = get_duckdb_config_string()
                logger.debug(f"[HPDP] Applying default DuckDB settings for Parquet export of schema '{schema.name}': \"{db_config_str}\"")
                if db_config_str: conn.execute(db_config_str)

                export_sql = f"""
                COPY (SELECT * FROM "{schema.table_name}") 
                TO ?
                (FORMAT PARQUET, COMPRESSION ?)
                """
                logger.debug(f"[HPDP] Executing Parquet export for schema '{schema.name}'. SQL: COPY ... TO '{output_path}' (Parquet, Comp: {compression})")
                
                # Parameters used for path and compression to prevent SQL injection, though less critical for fixed strings.
                conn.execute(export_sql, [str(output_path), compression])
                
                file_size_bytes = 0
                if output_path.exists():
                    file_size_bytes = output_path.stat().st_size
                    logger.debug(f"[HPDP] Parquet file '{output_path}' created for schema '{schema.name}'. Size: {file_size_bytes} bytes.")
                else:
                    # This case should ideally not happen if COPY succeeds without error.
                    logger.warning(f"[HPDP] Parquet export for schema '{schema.name}' reported success, but output file '{output_path}' was NOT FOUND.")

                duration_ms = (time.perf_counter() - start_time) * 1000
                file_size_mb = file_size_bytes / (1024*1024)
                
                logger.info(
                    f"[HPDP] ✅ Parquet export for schema '{schema.name}' COMPLETED. "
                    f"File: '{output_path}', Size: {file_size_mb:.2f}MB, Compression: {compression}. "
                    f"Duration: {duration_ms:.2f}ms."
                )
                
                return {
                    "success": True,
                    "file_path": str(output_path),
                    "file_size_mb": file_size_mb,
                    "duration_ms": duration_ms,
                    "compression": compression
                }
            except duckdb.Error as e:
                logger.error(f"❌ [HPDP] DuckDB FAILED during Parquet export for schema '{schema.name}' to '{output_path}': {e}", exc_info=True)
                raise DatabaseError(message=f"Parquet export failed for schema '{schema.name}' due to DuckDB error", underlying_exception=e)
            except IOError as e:
                logger.error(f"❌ [HPDP] File system FAILED during Parquet export for schema '{schema.name}' to '{output_path}': {e}", exc_info=True)
                raise DataProcessingError(message=f"Parquet export failed for schema '{schema.name}' due to file system error", underlying_exception=e)
            except Exception as e:
                logger.error(f"❌ [HPDP] Unexpected FAILED during Parquet export for schema '{schema.name}' to '{output_path}': {type(e).__name__} - {e}", exc_info=True)
                raise DataProcessingError(message=f"Unexpected error during Parquet export for schema '{schema.name}'", underlying_exception=e)
    
    async def analyze_with_polars(
        self, 
        schema: Schema,
        analysis_type: str = "summary"
    ) -> Dict[str, Any]:
        """
        Performs data analysis on the specified schema's table using Polars.

        Supports different analysis types:
        - "summary": Provides basic statistics like record count, column names, data types,
                     and null counts. This is typically done using efficient SQL aggregations.
        - "profile": Generates a more detailed data profile including descriptive statistics
                     (mean, min, max, etc.), unique value counts, and sample data.
                     Requires loading a sample of the data (limited to 10,000 records).
        - "quality": Calculates data quality metrics like completeness (non-null percentage)
                     and duplicate count, also based on a sample of 10,000 records.

        Args:
            schema: The domain Schema object for the table to be analyzed.
            analysis_type: The type of analysis to perform ("summary", "profile", "quality").

        Returns:
            A dictionary containing the analysis results, type, and duration.

        Raises:
            DatabaseError: If a DuckDB error occurs, especially for "summary" analysis.
            DataProcessingError: If an error occurs during Polars processing or if data fetching
                                 for "profile"/"quality" analysis fails.
            ValueError: If an unknown analysis_type is provided.
        """
        start_time = time.perf_counter()
        logger.info(f"[HPDP] Starting '{analysis_type}' analysis for schema '{schema.name}'.")
        
        analysis_result = {}
        try:
            if analysis_type == "summary":
                logger.debug(f"[HPDP] Performing 'summary' analysis using SQL aggregations for schema '{schema.name}'.")
                async with self.connection_pool.acquire() as conn:
                    db_config_str = get_duckdb_config_string("query_optimized") # Use query optimized profile for summary
                    logger.debug(f"[HPDP] Applying 'query_optimized' DuckDB profile for summary analysis of schema '{schema.name}': \"{db_config_str}\"")
                    if db_config_str: conn.execute(db_config_str)

                    def get_summary_stats_sync_local(): # Renamed to avoid conflict
                        logger.debug(f"[HPDP] Fetching COUNT(*) for summary of schema '{schema.name}'.")
                        count_query = f'SELECT COUNT(*) FROM "{schema.table_name}"'
                        count_result_val = conn.execute(count_query).fetchone()
                        total_records = count_result_val[0] if count_result_val else 0
                        logger.debug(f"[HPDP] Total records for summary of schema '{schema.name}': {total_records}.")

                        logger.debug(f"[HPDP] Fetching table description (DESCRIBE) for summary of schema '{schema.name}'.")
                        columns_query = f'DESCRIBE "{schema.table_name}"'
                        columns_result_val = conn.execute(columns_query).fetchall()

                        columns = [col_info[0] for col_info in columns_result_val]
                        dtypes = {col_info[0]: col_info[1] for col_info in columns_result_val}
                        logger.debug(f"[HPDP] Columns and DTypes for summary of schema '{schema.name}': {dtypes}.")

                        null_counts = {}
                        logger.debug(f"[HPDP] Fetching NULL counts for each column for summary of schema '{schema.name}'.")
                        for col_name_iter_local in columns: # Renamed to avoid conflict
                            null_query = f'SELECT COUNT(*) FROM "{schema.table_name}" WHERE "{col_name_iter_local}" IS NULL'
                            null_result_val = conn.execute(null_query).fetchone()
                            null_counts[col_name_iter_local] = null_result_val[0] if null_result_val else 0
                        logger.debug(f"[HPDP] NULL counts for summary of schema '{schema.name}': {null_counts}.")

                        return {
                            "shape": (total_records, len(columns)), "columns": columns,
                            "dtypes": dtypes, "null_counts": null_counts, "total_records": total_records
                        }
                    analysis_result = await asyncio.to_thread(get_summary_stats_sync_local)

            elif analysis_type in ["profile", "quality"]:
                sample_limit = 10000 # Define sample limit
                logger.info(f"[HPDP] Performing '{analysis_type}' analysis for schema '{schema.name}'. Fetching sample data (limit {sample_limit:,} records).")
                # query_with_polars_optimization already logs its operations and errors.
                df = await self.query_with_polars_optimization(schema, limit=sample_limit)
                
                logger.debug(f"[HPDP] Sample data fetched for '{analysis_type}' analysis of schema '{schema.name}'. Shape: {df.shape}. Now performing Polars analysis.")
                def analyze_df_sync_local(): # Renamed for clarity
                    if analysis_type == "profile":
                        profile_res = {
                            "describe": df.describe().to_dicts(), # Polars describe() output
                            "unique_counts": {col: df[col].n_unique() for col in df.columns},
                            "sample_data": df.head(5).to_dicts() # First 5 rows as sample
                        }
                        logger.debug(f"[HPDP] 'profile' analysis details generated for schema '{schema.name}'.")
                        return profile_res
                    elif analysis_type == "quality":
                        quality_res = {
                            "completeness_pct": { # Percentage of non-null values
                                col: (1 - (df[col].null_count() / len(df))) * 100 if len(df) > 0 else 0
                                for col in df.columns
                            },
                            "duplicate_row_count": df.is_duplicated().sum(),
                            "total_rows_in_sample": len(df),
                            "note": f"Analysis based on a sample of up to {sample_limit:,} records."
                        }
                        logger.debug(f"[HPDP] 'quality' analysis details generated for schema '{schema.name}'.")
                        return quality_res
                    return {} # Should not be reached if type is validated
                analysis_result = await asyncio.to_thread(analyze_df_sync_local)
            else:
                logger.warning(f"[HPDP] Unknown analysis type '{analysis_type}' requested for schema '{schema.name}'.")
                raise ValueError(f"Unknown analysis type: {analysis_type}")
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.info(f"[HPDP] ✅ Analysis '{analysis_type}' for schema '{schema.name}' COMPLETED. Duration: {duration_ms:.2f}ms.")
            
            return {
                "analysis_type": analysis_type,
                "duration_ms": duration_ms,
                "results": analysis_result
            }
        except duckdb.Error as e: # Catch DuckDB errors, mainly for "summary" or if query_with_polars_optimization fails at DB level
            logger.error(f"❌ [HPDP] DuckDB FAILED during '{analysis_type}' analysis for schema '{schema.name}': {e}", exc_info=True)
            raise DatabaseError(message=f"Database error during '{analysis_type}' analysis for schema '{schema.name}'", underlying_exception=e)
        except pl.PolarsError as e: # Catch Polars errors during "profile" or "quality"
            logger.error(f"❌ [HPDP] Polars FAILED during '{analysis_type}' analysis for schema '{schema.name}': {e}", exc_info=True)
            raise DataProcessingError(message=f"Polars error during '{analysis_type}' analysis for schema '{schema.name}'", underlying_exception=e)
        except DataProcessingError as e: # Re-raise if query_with_polars_optimization or other util throws it
             logger.error(f"❌ [HPDP] Data processing FAILED during '{analysis_type}' analysis setup for schema '{schema.name}': {e.message}", exc_info=True)
             raise # Keep the original exception type and message
        except ValueError as e: # Catch unknown analysis type error
            logger.error(f"❌ [HPDP] Configuration FAILED for '{analysis_type}' analysis of schema '{schema.name}': {e}", exc_info=True)
            raise e
        except Exception as e: # Catch any other unexpected errors
            logger.error(f"❌ [HPDP] Unexpected FAILED during '{analysis_type}' analysis for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
            raise DataProcessingError(message=f"Unexpected error during '{analysis_type}' analysis for schema '{schema.name}'", underlying_exception=e)