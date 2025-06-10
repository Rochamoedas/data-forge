import duckdb
from typing import Dict, Any, List, Optional, AsyncIterator
from uuid import UUID
from datetime import datetime
import time
import psutil
import os
from app.domain.entities.schema import Schema
from app.domain.entities.data_record import DataRecord
from app.domain.repositories.data_repository import IDataRepository
from app.application.dto.data_dto import PaginatedResponse
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.infrastructure.persistence.duckdb.query_builder import DuckDBQueryBuilder
from app.application.dto.query_dto import DataQueryRequest
from app.config.logging_config import logger
from app.config.duckdb_config import get_duckdb_config_string # Used for applying DB profiles
from app.domain.exceptions import RepositoryException, DuplicateRecordException, RecordNotFoundException, DatabaseError, DataProcessingError
from app.infrastructure.persistence.duckdb.duckdb_utils import create_csv_from_records, execute_duckdb_copy_from_csv

import asyncio
import duckdb # For specific DuckDB exception types like duckdb.ConstraintException
import tempfile
import os

class DuckDBDataRepository(IDataRepository):
    """
    DuckDB-specific implementation of the IDataRepository interface.

    This repository handles data persistence operations (CRUD, batch, streaming)
    for DuckDB, leveraging its performance features like COPY FROM for bulk inserts
    and optimized query execution. It uses an asynchronous connection pool.
    """
    def __init__(self, connection_pool: AsyncDuckDBPool):
        """
        Initializes the DuckDBDataRepository.

        Args:
            connection_pool: An instance of AsyncDuckDBPool for managing DuckDB connections.
        """
        self.connection_pool = connection_pool
        # These batch/chunk sizes are specific to this repository's older methods or general use.
        # Consider if these are still relevant or should be harmonized with HPDP or schema-driven.
        self._ultra_batch_size = 250000  # Potentially for non-HPDP batching, or if there's a direct call.
        self._stream_chunk_size = 100000 # Default chunk size for streaming methods if not overridden.

        # psutil.Process is imported but not used. Removing for now unless a specific need arises.
        # self._process = psutil.Process(os.getpid())
        logger.info(f"[REPO] DuckDBDataRepository initialized. Connection Pool: {connection_pool}")

    def _get_performance_metrics(self, start_time: float) -> Dict[str, float]:
        """
        Calculates performance metrics for an operation.
        Note: psutil was removed; CPU/memory metrics here would require re-adding it
              and careful consideration of its overhead and relevance per operation.
              Currently, only execution time is reported.
        """
        end_time = time.perf_counter()
        execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
        memory_info = self._process.memory_info()
        memory_usage = memory_info.rss / (1024 * 1024)  # Convert to MB
        cpu_percent = self._process.cpu_percent(interval=None)
        
        return {
            "execution_time_ms": execution_time,
            "memory_usage_mb": memory_usage,
            "cpu_percent": cpu_percent
        }

    async def create(self, schema: Schema, data: Dict[str, Any]) -> DataRecord:
        """
        Creates a single data record in the specified schema's table.

        Generates a DataRecord object, including a unique ID and timestamps.
        Handles potential database constraint violations (e.g., duplicate records)
        by raising specific domain exceptions.

        Args:
            schema: The domain Schema object defining the target table.
            data: A dictionary containing the data for the new record.

        Returns:
            The created DataRecord object.

        Raises:
            DuplicateRecordException: If a record with the same key already exists.
            RepositoryException: For other database or unexpected errors.
        """
        start_time = time.perf_counter()
        
        # Extract composite key if schema defines one
        composite_key_str = "" # For logging
        try:
            composite_key = schema.get_composite_key_from_data(data)
            if composite_key: # Convert dict to string for logging if it's a dict
                composite_key_str = str(composite_key) if isinstance(composite_key, dict) else composite_key
        except Exception as key_ex: # Catch errors during key generation
            logger.warning(f"[REPO] Error generating composite key for schema '{schema.name}': {key_ex}. Data: {data}")
            # Decide if this should be a hard error or proceed without composite key if possible
            composite_key = None

        record = DataRecord(schema_name=schema.name, data=data, composite_key=composite_key)
        logger.debug(f"[REPO] Attempting to create single record '{record.id}' in schema '{schema.name}'. CompositeKey: '{composite_key_str}'.")
        
        columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
        placeholders = ", ".join(["?"] * len(columns))
        column_names_sql = ", ".join(f'"{c}"' for c in columns) # Ensure column names are quoted
        insert_sql = f'INSERT OR IGNORE INTO "{schema.table_name}" ({column_names_sql}) VALUES ({placeholders})'

        values = [str(record.id), record.created_at.isoformat(), record.version] + \
                 [record.data.get(prop.name) for prop in schema.properties]

        async with self.connection_pool.acquire() as conn:
            try:
                conn.execute(insert_sql, values)
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(f"[REPO] ✅ Record '{record.id}' created successfully in schema '{schema.name}'. Duration: {duration_ms:.2f}ms.")
                return record
            except duckdb.ConstraintException as e:
                logger.error(f"❌ [REPO] Constraint violation (likely duplicate) creating record '{record.id}' (Key: '{composite_key_str}') in schema '{schema.name}'. SQL: {insert_sql}. Error: {e}", exc_info=True)
                raise DuplicateRecordException(message=f"Record with key '{composite_key_str}' already exists in schema '{schema.name}'.", record_id=str(record.id), underlying_exception=e)
            except duckdb.IOException as e:
                logger.error(f"❌ [REPO] IO error creating record '{record.id}' in schema '{schema.name}': {e}", exc_info=True)
                raise RepositoryException(message=f"File system error during record creation in schema '{schema.name}'.", underlying_exception=e)
            except duckdb.Error as e:
                logger.error(f"❌ [REPO] DuckDB error creating record '{record.id}' in schema '{schema.name}': {e}. SQL: {insert_sql}", exc_info=True)
                raise RepositoryException(message=f"Database error creating record in schema '{schema.name}'.", underlying_exception=e)
            except Exception as e:
                logger.error(f"❌ [REPO] Unexpected error creating record '{record.id}' in schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise RepositoryException(message=f"Unexpected error creating record in schema '{schema.name}'.", underlying_exception=e)

    async def create_batch(self, schema: Schema, records: List[DataRecord]) -> None:
        """
        Creates a batch of data records using an ultra-fast COPY FROM CSV approach.

        This method leverages `_ultra_fast_copy_insert` which uses the `duckdb_utils`
        for CSV creation and DuckDB's COPY command.

        Args:
            schema: The domain Schema object for the target table.
            records: A list of DataRecord objects to be inserted.
        """
        if not records:
            logger.info(f"[REPO] Create batch for schema '{schema.name}': No records provided. Skipping.")
            return

        start_time_batch = time.perf_counter() # Renamed to avoid conflict with _get_performance_metrics internal start_time
        total_records = len(records)
        logger.info(f"[REPO] Starting batch insert for {total_records:,} records into schema '{schema.name}'.")
        
        await self._ultra_fast_copy_insert(schema, records)

        # Performance metrics for the batch operation
        # Note: _get_performance_metrics currently only provides duration.
        metrics = self._get_performance_metrics(start_time_batch)
        duration_ms = metrics["execution_time_ms"]
        throughput = total_records / (duration_ms / 1000) if duration_ms > 0 else float('inf')
        
        logger.info(f"[REPO] ✅ Batch insert COMPLETED for schema '{schema.name}'. "
                    f"Inserted: {total_records:,} records. Duration: {duration_ms:.2f}ms. "
                    f"Throughput: {throughput:,.0f} records/sec.")
        

    async def _ultra_fast_copy_insert(self, schema: Schema, records: List[DataRecord]) -> None:
        """
        Internal method for ultra-fast bulk insertion using DuckDB's COPY FROM CSV.

        This method utilizes utility functions from `duckdb_utils.py` to:
        1. Create a temporary CSV file from the list of DataRecord objects.
        2. Execute DuckDB's `COPY FROM` command to load data from the CSV into the target table.
           This typically involves using a temporary DuckDB table for staging to handle
           `INSERT OR IGNORE` semantics for duplicate prevention.

        Args:
            schema: The domain Schema object.
            records: A list of DataRecord objects for insertion.

        Raises:
            RepositoryException: Wraps errors from utility functions (DataProcessingError, DatabaseError)
                                 or catches other unexpected errors during the process.
        """
        import tempfile
        import csv
        import os
        
        start_time_copy = time.perf_counter() # Renamed to avoid conflict
        temp_file_path = None # Ensure defined for finally block
        
        logger.info(f"[REPO] Starting _ultra_fast_copy_insert for {len(records)} records into schema '{schema.name}'.")
        try:
            # Create a named temporary file; it's explicitly deleted in the `finally` block.
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False,
                                             newline='', encoding='utf-8') as temp_file_obj:
                temp_file_path = temp_file_obj.name
            logger.debug(f"[REPO] Created temporary CSV file for COPY: '{temp_file_path}' for schema '{schema.name}'.")
            
            # Ensure composite keys are set on records before writing to CSV
            # This step is crucial if composite keys are derived and not part of the initial record data.
            for record_idx, record_item in enumerate(records): # Renamed to avoid conflict
                if not record_item.composite_key and schema.primary_key:
                    try:
                        record_item.composite_key = schema.get_composite_key_from_data(record_item.data)
                    except Exception as key_ex:
                        logger.warning(f"[REPO] Error generating composite key for record index {record_idx} in schema '{schema.name}': {key_ex}. Record data: {record_item.data}")
                        # Depending on policy, this could raise an error or allow record to proceed without a key.
                        # For now, it proceeds, potentially leading to issues if key is vital.

            # Use utility to create the CSV file from DataRecord objects
            await create_csv_from_records(records, schema, temp_file_path)
            
            async with self.connection_pool.acquire() as conn:
                # Retrieve DuckDB configuration string for "bulk_insert" profile
                config_str = get_duckdb_config_string("bulk_insert")
                logger.debug(f"[REPO] Applying 'bulk_insert' DuckDB profile for COPY operation on schema '{schema.name}'.")

                # Use utility to execute the COPY FROM CSV operation.
                # This utility handles DB transactions and specific DuckDB errors.
                await execute_duckdb_copy_from_csv(
                    db_conn=conn,
                    schema_name=schema.name,
                    table_name=schema.table_name,
                    temp_csv_path=temp_file_path,
                    use_temp_table=True, # Retain original logic of using a temp table for INSERT OR IGNORE
                    config_string=config_str
                )

            duration_ms_copy = (time.perf_counter() - start_time_copy) * 1000
            throughput_copy = len(records) / (duration_ms_copy / 1000) if duration_ms_copy > 0 else float('inf')
            logger.info(f"[REPO] ✅ _ultra_fast_copy_insert for schema '{schema.name}' COMPLETED. "
                       f"Processed: {len(records):,} records. Duration: {duration_ms_copy:.2f}ms. "
                       f"Throughput: {throughput_copy:,.0f} records/sec.")

        except (DataProcessingError, DatabaseError) as e:
            # Catch errors raised by create_csv_from_records or execute_duckdb_copy_from_csv
            logger.error(f"❌ [REPO] Error during _ultra_fast_copy_insert for schema '{schema.name}': {type(e).__name__} - {e.message}", exc_info=True)
            # Re-raise as RepositoryException to abstract specific util errors from the caller
            raise RepositoryException(message=f"Bulk insert sub-operation failed for schema '{schema.name}': {e.message}", underlying_exception=e.underlying_exception or e)
        except Exception as e: # Catch any other unexpected errors
            logger.error(f"❌ [REPO] Unexpected error in _ultra_fast_copy_insert for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
            raise RepositoryException(message=f"Unexpected error during bulk insert for schema '{schema.name}'", underlying_exception=e)
        finally:
            # Cleanup: Ensure the temporary CSV file is deleted
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.debug(f"[REPO] Successfully cleaned up temporary CSV file: '{temp_file_path}' for schema '{schema.name}'.")
                except Exception as e_unlink:
                    logger.warning(f"[REPO] Failed to cleanup temporary CSV file '{temp_file_path}' for schema '{schema.name}': {e_unlink}")

    async def get_by_id(self, schema: Schema, record_id: UUID) -> Optional[DataRecord]:
        """
        Retrieves a single data record by its UUID from the specified schema's table.
        """
        start_time = time.perf_counter()
        select_sql = f'SELECT * FROM "{schema.table_name}" WHERE id = ?'
        logger.debug(f"[REPO] Attempting to get record by ID '{record_id}' from schema '{schema.name}'. SQL: {select_sql}")
        async with self.connection_pool.acquire() as conn:
            try:
                # Consider applying a "query_optimized" profile if one is suitable for point lookups
                # conn.execute(get_duckdb_config_string("query_optimized"))
                result = conn.execute(select_sql, [str(record_id)]).fetchone()
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                if result:
                    logger.info(f"[REPO] ✅ Record '{record_id}' found in schema '{schema.name}'. Duration: {duration_ms:.2f}ms.")
                    return self._map_row_to_data_record(schema, result, conn.description)
                else:
                    logger.info(f"[REPO] Record '{record_id}' NOT FOUND in schema '{schema.name}'. Duration: {duration_ms:.2f}ms.")
                    return None
            except duckdb.Error as e:
                logger.error(f"❌ [REPO] DuckDB error retrieving record '{record_id}' from schema '{schema.name}': {e}. SQL: {select_sql}", exc_info=True)
                raise RepositoryException(message=f"Database error retrieving record by ID from schema '{schema.name}'", underlying_exception=e)
            except Exception as e:
                logger.error(f"❌ [REPO] Unexpected error retrieving record '{record_id}' from schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise RepositoryException(message=f"Unexpected error retrieving record by ID from schema '{schema.name}'", underlying_exception=e)


    async def get_all(self, schema: Schema, query_request: DataQueryRequest) -> PaginatedResponse[DataRecord]:
        """
        Retrieves a paginated list of data records from the schema's table,
        applying specified filters and sorting.
        """
        start_time = time.perf_counter()
        pagination_details = query_request.pagination
        logger.info(f"[REPO] Getting all records for schema '{schema.name}'. Page: {pagination_details.page}, Size: {pagination_details.size}. Filters: {query_request.filters}, Sort: {query_request.sort}")

        query_builder = DuckDBQueryBuilder(schema) # QueryBuilder handles quoting of table/column names
        query_builder.add_filters(query_request.filters)
        query_builder.add_sorts(query_request.sort)
        
        offset = (pagination_details.page - 1) * pagination_details.size
        query_builder.add_pagination(pagination_details.size, offset)
        
        count_sql = query_builder.build_count_query()
        count_params = query_builder.get_params().copy()
        
        select_sql = query_builder.build_select_query()
        select_params = query_builder.get_params()

        logger.debug(f"[REPO] Get All - Count SQL for '{schema.name}': {count_sql}, Params: {count_params}")
        logger.debug(f"[REPO] Get All - Select SQL for '{schema.name}': {select_sql}, Params: {select_params}")

        async with self.connection_pool.acquire() as conn:
            try:
                db_config_str = get_duckdb_config_string("query_optimized")
                logger.debug(f"[REPO] Applying 'query_optimized' DuckDB profile for get_all on schema '{schema.name}'.")
                if db_config_str: conn.execute(db_config_str)

                count_result = conn.execute(count_sql, count_params).fetchone()
                total = count_result[0] if count_result else 0
                logger.debug(f"[REPO] Total records count for schema '{schema.name}' (filters applied): {total}.")

                result_relation = conn.execute(select_sql, select_params)
                rows = result_relation.fetchall()
                description = result_relation.description # Get column descriptions for mapping
                logger.debug(f"[REPO] Fetched {len(rows)} records for current page from schema '{schema.name}'.")
                
                records = [self._map_row_to_data_record_optimized(schema, row, description) for row in rows]
                
                has_next = (pagination_details.page * pagination_details.size) < total
                has_previous = pagination_details.page > 1
                
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(f"[REPO] ✅ get_all for schema '{schema.name}' COMPLETED. Fetched: {len(records)} records. Total: {total}. Duration: {duration_ms:.2f}ms.")
                return PaginatedResponse(
                    items=records, total=total, page=pagination_details.page, size=pagination_details.size,
                    has_next=has_next, has_previous=has_previous
                )
            except duckdb.Error as e:
                logger.error(f"❌ [REPO] DuckDB error retrieving records from schema '{schema.name}': {e}. CountSQL: {count_sql}, SelectSQL: {select_sql}", exc_info=True)
                raise RepositoryException(message=f"Database error retrieving records from schema '{schema.name}'", underlying_exception=e)
            except Exception as e:
                logger.error(f"❌ [REPO] Unexpected error retrieving records from schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise RepositoryException(message=f"Unexpected error retrieving records from schema '{schema.name}'", underlying_exception=e)

    async def stream_query_results(self, schema: Schema, query_request: DataQueryRequest) -> AsyncIterator[DataRecord]:
        """
        Streams data records from the schema's table, applying filters and sorting.
        Uses a fixed-size chunk for fetching data iteratively.
        """
        start_time_stream = time.perf_counter() # Renamed for clarity
        filter_details = query_request.filters
        sort_details = query_request.sort
        pagination_details = query_request.pagination

        logger.info(f"[REPO] Starting stream_query_results for schema '{schema.name}'. Filters: {filter_details}, Sort: {sort_details}, Pagination: {pagination_details}")

        query_builder = DuckDBQueryBuilder(schema)
        query_builder.add_filters(filter_details)
        query_builder.add_sorts(sort_details)
        
        # Determine if there's a specific limit requested by pagination, or stream all (effectively infinite limit for the loop)
        requested_limit = float('inf')
        if pagination_details:
            offset = (pagination_details.page - 1) * pagination_details.size
            query_builder.add_pagination(pagination_details.size, offset)
            select_sql = query_builder.build_select_query() # Query with limit and offset
            requested_limit = pagination_details.size # User wants only this many records for this "page" of stream
            logger.debug(f"[REPO] Stream for '{schema.name}' is paginated. Page: {pagination_details.page}, Size: {pagination_details.size}, Offset: {offset}.")
        else:
            select_sql = query_builder.build_select_query_without_pagination() # Query without LIMIT/OFFSET for full stream
            logger.debug(f"[REPO] Stream for '{schema.name}' is not paginated (full stream requested).")
        
        params = query_builder.get_params()
        logger.debug(f"[REPO] Streaming SQL for schema '{schema.name}': {select_sql}, Params: {params}")

        async with self.connection_pool.acquire() as conn:
            try:
                db_config_str = get_duckdb_config_string("streaming") # Apply "streaming" profile
                logger.debug(f"[REPO] Applying 'streaming' DuckDB profile for stream on schema '{schema.name}'.")
                if db_config_str: conn.execute(db_config_str)

                result_relation = conn.execute(select_sql, params)
                description = result_relation.description # Get column info for mapping
                
                records_yielded_total = 0
                batch_num = 0
                while True:
                    batch_num +=1
                    logger.debug(f"[REPO] Fetching chunk {batch_num} (size {self._stream_chunk_size}) for stream from schema '{schema.name}'.")
                    try:
                        rows = result_relation.fetchmany(self._stream_chunk_size)
                    except duckdb.Error as fetch_err:
                        logger.error(f"❌ [REPO] DuckDB error fetching chunk {batch_num} for stream from schema '{schema.name}': {fetch_err}", exc_info=True)
                        raise RepositoryException(message=f"Database error fetching stream chunk from schema '{schema.name}'", underlying_exception=fetch_err)

                    if not rows: # End of stream
                        logger.info(f"[REPO] Stream for schema '{schema.name}' ended after {records_yielded_total} records (Chunk {batch_num} was empty).")
                        break
                    
                    logger.debug(f"[REPO] Fetched {len(rows)} rows in chunk {batch_num} for stream from schema '{schema.name}'.")
                    for row_idx, row_data_item in enumerate(rows): # Renamed to avoid conflict
                        yield self._map_row_to_data_record_optimized(schema, row_data_item, description)
                        records_yielded_total += 1
                        if records_yielded_total >= requested_limit: # Check against user-defined limit for this stream call
                            logger.info(f"[REPO] Stream limit ({requested_limit}) reached for schema '{schema.name}' after {records_yielded_total} records.")
                            # Log final stream duration before returning
                            duration_ms_stream = (time.perf_counter() - start_time_stream) * 1000
                            logger.info(f"[REPO] ✅ stream_query_results for schema '{schema.name}' COMPLETED (limit reached). Yielded: {records_yielded_total}. Duration: {duration_ms_stream:.2f}ms.")
                            return
                
                # Log final stream duration if loop finishes naturally
                duration_ms_stream_natural_end = (time.perf_counter() - start_time_stream) * 1000
                logger.info(f"[REPO] ✅ stream_query_results for schema '{schema.name}' COMPLETED (all records streamed). Yielded: {records_yielded_total}. Duration: {duration_ms_stream_natural_end:.2f}ms.")
                
            except duckdb.Error as e:
                logger.error(f"❌ [REPO] DuckDB error during stream_query_results for schema '{schema.name}': {e}. SQL: {select_sql}", exc_info=True)
                raise RepositoryException(message=f"Database error streaming records from schema '{schema.name}'", underlying_exception=e)
            except Exception as e:
                logger.error(f"❌ [REPO] Unexpected error during stream_query_results for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise RepositoryException(message=f"Unexpected error streaming records from schema '{schema.name}'", underlying_exception=e)

    async def count_all(self, schema: Schema, query_request: DataQueryRequest) -> int:
        """
        Counts all records in the schema's table, optionally applying filters.
        """
        start_time = time.perf_counter()
        filter_details_count = query_request.filters # Renamed for clarity
        logger.info(f"[REPO] Counting all records for schema '{schema.name}'. Filters: {filter_details_count}")

        query_builder = DuckDBQueryBuilder(schema)
        query_builder.add_filters(filter_details_count)
        count_sql = query_builder.build_count_query()
        params = query_builder.get_params()
        logger.debug(f"[REPO] Count SQL for schema '{schema.name}': {count_sql}, Params: {params}")

        async with self.connection_pool.acquire() as conn:
            try:
                db_config_str = get_duckdb_config_string("query_optimized")
                logger.debug(f"[REPO] Applying 'query_optimized' DuckDB profile for count_all on schema '{schema.name}'.")
                if db_config_str: conn.execute(db_config_str)

                result = conn.execute(count_sql, params).fetchone()
                count = result[0] if result else 0
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(f"[REPO] ✅ Count for schema '{schema.name}' is {count}. Duration: {duration_ms:.2f}ms.")
                return count
            except duckdb.Error as e:
                logger.error(f"❌ [REPO] DuckDB error counting records for schema '{schema.name}': {e}. SQL: {count_sql}", exc_info=True)
                raise RepositoryException(message=f"Database error counting records in schema '{schema.name}'", underlying_exception=e)
            except Exception as e:
                logger.error(f"❌ [REPO] Unexpected error counting records for schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise RepositoryException(message=f"Unexpected error counting records in schema '{schema.name}'", underlying_exception=e)


    async def get_by_composite_key(self, schema: Schema, composite_key: Dict[str, Any]) -> Optional[DataRecord]:
        """
        Retrieves a single data record by its composite key values.
        The composite key fields are defined in the Schema.
        """
        if not schema.primary_key: # A composite key is a type of primary key.
            logger.warning(f"[REPO] Attempted get_by_composite_key for schema '{schema.name}', but no primary_key (composite key fields) defined in schema.")
            # Consider raising an InvalidOperationException or similar if this is an invalid state.
            return None
            
        start_time = time.perf_counter()
        # Log the composite key structure for clarity, avoid logging actual sensitive values if present.
        logger.info(f"[REPO] Attempting to get record by composite key for schema '{schema.name}'. Key fields: {schema.primary_key}.")
        
        where_conditions = []
        values = []
        # Ensure all parts of the composite key defined in the schema are present in the input `composite_key` dict.
        for key_field in schema.primary_key:
            if key_field not in composite_key:
                logger.error(f"❌ [REPO] Missing part '{key_field}' of composite key for schema '{schema.name}'. Provided key data: {composite_key}")
                raise RepositoryException(f"Incomplete composite key for schema '{schema.name}'. Missing field: '{key_field}'.")
            where_conditions.append(f'"{key_field}" = ?') # Column names quoted
            values.append(composite_key[key_field])

        # This check is now redundant due to the loop above, but kept for safety.
        if not where_conditions:
            logger.error(f"❌ [REPO] No valid conditions derived for composite key query in schema '{schema.name}'. Provided key: {composite_key}")
            # This implies schema.primary_key was empty, which is caught earlier.
            return None
            
        where_clause = " AND ".join(where_conditions)
        select_sql = f'SELECT * FROM "{schema.table_name}" WHERE {where_clause}' # Table name quoted
        logger.debug(f"[REPO] Composite key query for schema '{schema.name}': {select_sql}, Values: {values}")
        
        async with self.connection_pool.acquire() as conn:
            try:
                db_config_str = get_duckdb_config_string("query_optimized")
                logger.debug(f"[REPO] Applying 'query_optimized' DuckDB profile for get_by_composite_key on schema '{schema.name}'.")
                if db_config_str: conn.execute(db_config_str)

                result = conn.execute(select_sql, values).fetchone()
                duration_ms = (time.perf_counter() - start_time) * 1000
                
                if result:
                    logger.info(f"[REPO] ✅ Record found by composite key in schema '{schema.name}'. Duration: {duration_ms:.2f}ms.")
                    return self._map_row_to_data_record_optimized(schema, result, conn.description)
                else:
                    logger.info(f"[REPO] Record NOT FOUND by composite key in schema '{schema.name}'. Duration: {duration_ms:.2f}ms.")
                    # Depending on requirements, RecordNotFoundException could be raised here.
                    return None
            except duckdb.Error as e:
                logger.error(f"❌ [REPO] DuckDB error retrieving record by composite key from schema '{schema.name}': {e}. SQL: {select_sql}", exc_info=True)
                raise RepositoryException(message=f"Database error retrieving record by composite key from schema '{schema.name}'", underlying_exception=e)
            except Exception as e:
                logger.error(f"❌ [REPO] Unexpected error retrieving record by composite key from schema '{schema.name}': {type(e).__name__} - {e}", exc_info=True)
                raise RepositoryException(message=f"Unexpected error retrieving record by composite key from schema '{schema.name}'", underlying_exception=e)

    def _map_row_to_data_record(self, schema: Schema, row: tuple, description: list) -> DataRecord:
        """
        Maps a raw database row (tuple) to a DataRecord object.
        This is a standard mapping function; `_map_row_to_data_record_optimized` is preferred for performance.
        """
        column_names = [desc[0] for desc in description]
        row_dict = dict(zip(column_names, row))
        
        # Convert data types according to schema definition
        data_payload = {}
        for prop in schema.properties:
            raw_value = row_dict.get(prop.name)
            if raw_value is not None:
                # Convert datetime objects to strings for fields that should be strings
                if prop.type == "string" and isinstance(raw_value, datetime):
                    data_payload[prop.name] = raw_value.strftime('%Y-%m-%d') if prop.db_type == "TIMESTAMP" else str(raw_value)
                else:
                    data_payload[prop.name] = raw_value
            else:
                data_payload[prop.name] = raw_value
        
        # Extract composite key if schema defines one - now with properly converted data
        composite_key = schema.get_composite_key_from_data(data_payload) if schema.primary_key else None
        
        return DataRecord(
            id=UUID(row_dict['id']),
            schema_name=schema.name,
            data=data_payload,
            created_at=row_dict['created_at'],
            version=row_dict['version'],
            composite_key=composite_key
        )

    def _map_row_to_data_record_optimized(self, schema: Schema, row: tuple, description: list) -> DataRecord:
        """🚀 High-performance optimized record mapping with minimal overhead for millions of rows"""
        # Pre-compute column indices for faster access (cached per schema)
        cache_key = f"{schema.name}_{len(description)}"
        if not hasattr(self, '_column_cache') or cache_key not in getattr(self, '_column_cache', {}):
            if not hasattr(self, '_column_cache'):
                self._column_cache = {}
            
            column_names = [desc[0] for desc in description]
            self._column_cache[cache_key] = {
                'id_idx': column_names.index('id'),
                'created_at_idx': column_names.index('created_at'), 
                'version_idx': column_names.index('version'),
                'prop_indices': {prop.name: column_names.index(prop.name) 
                               for prop in schema.properties if prop.name in column_names}
            }
        
        cache = self._column_cache[cache_key]
        
        # Fast data extraction using pre-computed indices
        data_payload = {}
        for prop_name, idx in cache['prop_indices'].items():
            raw_value = row[idx]
            # Minimal type conversion for performance
            if raw_value is not None:
                prop = next(p for p in schema.properties if p.name == prop_name)
                if prop.type == "string" and isinstance(raw_value, datetime):
                    data_payload[prop_name] = raw_value.strftime('%Y-%m-%d') if prop.db_type == "TIMESTAMP" else str(raw_value)
                else:
                    data_payload[prop_name] = raw_value
            else:
                data_payload[prop_name] = raw_value
        
        # Extract composite key if schema defines one
        composite_key = schema.get_composite_key_from_data(data_payload) if schema.primary_key else None
        
        return DataRecord(
            id=UUID(row[cache['id_idx']]),
            schema_name=schema.name,
            data=data_payload,
            created_at=row[cache['created_at_idx']],
            version=row[cache['version_idx']],
            composite_key=composite_key
        )
