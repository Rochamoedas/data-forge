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
from app.infrastructure.web.dependencies.profiling import log_repository_performance
import asyncio

class DuckDBDataRepository(IDataRepository):
    def __init__(self, connection_pool: AsyncDuckDBPool):
        self.connection_pool = connection_pool
        self._batch_size = 10000  # Increased batch size for better performance
        self._process = psutil.Process(os.getpid())

    def _get_performance_metrics(self, start_time: float) -> Dict[str, float]:
        """Get accurate performance metrics."""
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
        start_time = time.perf_counter()
        
        # Extract composite key if schema defines one
        composite_key = schema.get_composite_key_from_data(data)
        record = DataRecord(schema_name=schema.name, data=data, composite_key=composite_key)
        
        columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(f'"{c}"' for c in columns)
        insert_sql = f'INSERT INTO "{schema.table_name}" ({column_names}) VALUES ({placeholders})'
        values = [str(record.id), record.created_at, record.version] + [record.data.get(prop.name) for prop in schema.properties]

        async with self.connection_pool.acquire() as conn:
            try:
                conn.execute(insert_sql, values)
                
                # Log performance metrics
                metrics = self._get_performance_metrics(start_time)
                log_repository_performance("CREATE_RECORD", schema.name, metrics, record_id=str(record.id))
                
                return record
            except Exception as e:
                logger.error(f"Error creating record in schema {schema.name}: {e}")
                raise

    async def create_batch(self, schema: Schema, records: List[DataRecord]) -> None:
        if not records:
            return

        start_time = time.perf_counter()
        total_records = len(records)
        processed_records = 0

        # Process records in larger batches
        for i in range(0, total_records, self._batch_size):
            batch = records[i:i + self._batch_size]
            await self._process_batch(schema, batch)
            processed_records += len(batch)
            
            # Log progress for large operations only
            if total_records > 1000:  # Only log progress for large batches
                progress_pct = (processed_records/total_records)*100
                if progress_pct % 25 == 0 or processed_records == total_records:  # Log at 25%, 50%, 75%, 100%
                    metrics = self._get_performance_metrics(start_time)
                    log_repository_performance(
                        "BATCH_PROGRESS", 
                        schema.name, 
                        metrics, 
                        progress=f"{processed_records}/{total_records}",
                        percent=f"{progress_pct:.1f}%"
                    )

        # Final metrics
        metrics = self._get_performance_metrics(start_time)
        log_repository_performance("CREATE_BATCH", schema.name, metrics, total_records=total_records)

    async def _process_batch(self, schema: Schema, records: List[DataRecord]) -> None:
        columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(f'"{c}"' for c in columns)
        
        # Create a temporary table for bulk insert
        temp_table = f"temp_bulk_insert_{schema.table_name}"
        create_temp_sql = f'CREATE TEMPORARY TABLE "{temp_table}" AS SELECT * FROM "{schema.table_name}" LIMIT 0'
        
        # Prepare values for bulk insert - ensure composite keys are set
        values_to_insert = []
        for record in records:
            # Ensure composite key is set if not already
            if not record.composite_key and schema.primary_key:
                record.composite_key = schema.get_composite_key_from_data(record.data)
            
            values_to_insert.append(
                (str(record.id), record.created_at, record.version) + 
                tuple(record.data.get(prop.name) for prop in schema.properties)
            )

        async with self.connection_pool.acquire() as conn:
            try:
                # Start transaction
                conn.execute("BEGIN TRANSACTION")
                
                # Create temporary table
                conn.execute(create_temp_sql)
                
                # Bulk insert into temporary table
                insert_sql = f'INSERT INTO "{temp_table}" ({column_names}) VALUES ({placeholders})'
                conn.executemany(insert_sql, values_to_insert)
                
                # Use INSERT OR IGNORE to handle any remaining duplicates gracefully
                copy_sql = f'INSERT OR IGNORE INTO "{schema.table_name}" SELECT * FROM "{temp_table}"'
                result = conn.execute(copy_sql)
                
                # Get the number of actually inserted records
                inserted_count = result.rowcount if hasattr(result, 'rowcount') else len(records)
                
                # Drop temporary table
                conn.execute(f'DROP TABLE "{temp_table}"')
                
                # Commit transaction
                conn.execute("COMMIT")
                
                if inserted_count < len(records):
                    skipped_count = len(records) - inserted_count
                    logger.info(f"Batch inserted {inserted_count} records into schema {schema.name}, "
                               f"skipped {skipped_count} duplicates")
                else:
                    logger.info(f"Batch inserted {len(records)} records into schema {schema.name}")
                    
            except Exception as e:
                conn.execute("ROLLBACK")
                # Provide more specific error information
                error_msg = str(e)
                if "Constraint Error" in error_msg and "Duplicate key" in error_msg:
                    logger.warning(f"Bulk operation - Duplicate key constraint violation in schema {schema.name}: {error_msg}")
                    # Extract the duplicate key information for better debugging
                    if "field_code:" in error_msg:
                        logger.info(f"Suggestion: Consider implementing deduplication logic or using INSERT OR IGNORE for bulk operations")
                else:
                    logger.error(f"Internal error batch creating records in schema {schema.name}: {e}")
                raise

    async def get_by_id(self, schema: Schema, record_id: UUID) -> Optional[DataRecord]:
        start_time = time.perf_counter()
        select_sql = f'SELECT * FROM "{schema.table_name}" WHERE id = ?'
        async with self.connection_pool.acquire() as conn:
            try:
                result = conn.execute(select_sql, [str(record_id)]).fetchone()
                
                # Log performance metrics
                metrics = self._get_performance_metrics(start_time)
                log_repository_performance("GET_BY_ID", schema.name, metrics, record_id=str(record_id))
                
                if result:
                    return self._map_row_to_data_record(schema, result, conn.description)
                return None
            except Exception as e:
                logger.error(f"Error retrieving record {record_id} from schema {schema.name}: {e}")
                raise

    async def get_all(self, schema: Schema, query_request: DataQueryRequest) -> PaginatedResponse[DataRecord]:
        start_time = time.perf_counter()
        query_builder = DuckDBQueryBuilder(schema)
        query_builder.add_filters(query_request.filters)
        query_builder.add_sorts(query_request.sort)
        
        # Calculate offset from page and size
        offset = (query_request.pagination.page - 1) * query_request.pagination.size
        query_builder.add_pagination(query_request.pagination.size, offset)
        
        # Build count query first and get its parameters
        count_sql = query_builder.build_count_query()
        count_params = query_builder.get_params().copy()
        
        # Build select query and get its parameters
        select_sql = query_builder.build_select_query()
        select_params = query_builder.get_params().copy()

        async with self.connection_pool.acquire() as conn:
            try:
                # Get total count
                count_result = conn.execute(count_sql, count_params).fetchone()
                total = count_result[0] if count_result else 0
                
                # Get paginated results
                result_relation = conn.execute(select_sql, select_params)
                rows = result_relation.fetchall()
                description = result_relation.description
                
                records = [self._map_row_to_data_record(schema, row, description) for row in rows]
                
                # Log performance metrics
                metrics = self._get_performance_metrics(start_time)
                log_repository_performance(
                    "GET_PAGINATED", 
                    schema.name, 
                    metrics, 
                    page=query_request.pagination.page,
                    size=query_request.pagination.size,
                    total_results=total,
                    returned_records=len(records)
                )
                
                # Calculate pagination flags
                current_page = query_request.pagination.page
                page_size = query_request.pagination.size
                has_next = (current_page * page_size) < total
                has_previous = current_page > 1
                
                return PaginatedResponse(
                    items=records,
                    total=total,
                    page=current_page,
                    size=page_size,
                    has_next=has_next,
                    has_previous=has_previous
                )
            except Exception as e:
                logger.error(f"Error retrieving records from schema {schema.name}: {e}")
                raise

    async def stream_query_results(self, schema: Schema, query_request: DataQueryRequest) -> AsyncIterator[DataRecord]:
        start_time = time.perf_counter()
        query_builder = DuckDBQueryBuilder(schema)
        query_builder.add_filters(query_request.filters)
        query_builder.add_sorts(query_request.sort)
        
        select_sql = query_builder.build_select_query_without_pagination()
        params = query_builder.get_params()

        async with self.connection_pool.acquire() as conn:
            try:
                result_relation = conn.execute(select_sql, params)
                rows = result_relation.fetchall()
                description = result_relation.description
                
                # Log performance metrics
                metrics = self._get_performance_metrics(start_time)
                log_repository_performance("STREAM_QUERY", schema.name, metrics, streamed_records=len(rows))
                
                for row in rows:
                    yield self._map_row_to_data_record(schema, row, description)
                    
            except Exception as e:
                logger.error(f"Error streaming records from schema {schema.name}: {e}")
                raise

    async def count_all(self, schema: Schema, query_request: DataQueryRequest) -> int:
        start_time = time.perf_counter()
        query_builder = DuckDBQueryBuilder(schema)
        query_builder.add_filters(query_request.filters)
        count_sql = query_builder.build_count_query()
        params = query_builder.get_params()

        async with self.connection_pool.acquire() as conn:
            try:
                result = conn.execute(count_sql, params).fetchone()
                count = result[0] if result else 0
                
                # Log performance metrics
                metrics = self._get_performance_metrics(start_time)
                log_repository_performance("COUNT_RECORDS", schema.name, metrics, count=count)
                
                return count
            except Exception as e:
                logger.error(f"Error counting records for schema {schema.name}: {e}")
                raise

    async def get_by_composite_key(self, schema: Schema, composite_key: Dict[str, Any]) -> Optional[DataRecord]:
        """Get record by composite key values"""
        if not schema.primary_key:
            return None
            
        start_time = time.perf_counter()
        
        # Build WHERE clause for composite key
        where_conditions = []
        values = []
        for key_field in schema.primary_key:
            if key_field in composite_key:
                where_conditions.append(f'"{key_field}" = ?')
                values.append(composite_key[key_field])
        
        if not where_conditions:
            return None
            
        where_clause = " AND ".join(where_conditions)
        select_sql = f'SELECT * FROM "{schema.table_name}" WHERE {where_clause}'
        
        async with self.connection_pool.acquire() as conn:
            try:
                result = conn.execute(select_sql, values).fetchone()
                
                # Log performance metrics
                metrics = self._get_performance_metrics(start_time)
                log_repository_performance("GET_BY_COMPOSITE_KEY", schema.name, metrics, composite_key=str(composite_key))
                
                if result:
                    return self._map_row_to_data_record(schema, result, conn.description)
                return None
            except Exception as e:
                logger.error(f"Error retrieving record by composite key {composite_key} from schema {schema.name}: {e}")
                raise

    def _map_row_to_data_record(self, schema: Schema, row: tuple, description: list) -> DataRecord:
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
