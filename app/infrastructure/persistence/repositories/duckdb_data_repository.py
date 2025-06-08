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
import asyncio

class DuckDBDataRepository(IDataRepository):
    def __init__(self, connection_pool: AsyncDuckDBPool):
        self.connection_pool = connection_pool
        self._batch_size = 10000  # Increased batch size for better performance
        self._process = psutil.Process(os.getpid())

    def _get_performance_metrics(self, start_time: float) -> Dict[str, float]:
        """Get accurate performance metrics."""
        end_time = time.time()
        execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
        memory_info = self._process.memory_info()
        memory_usage = memory_info.rss / (1024 * 1024)  # Convert to MB
        cpu_percent = self._process.cpu_percent()
        
        return {
            "execution_time_ms": execution_time,
            "memory_usage_mb": memory_usage,
            "cpu_percent": cpu_percent
        }

    async def create(self, schema: Schema, data: Dict[str, Any]) -> DataRecord:
        record = DataRecord(schema_name=schema.name, data=data)
        columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(f'"{c}"' for c in columns)
        insert_sql = f'INSERT INTO "{schema.table_name}" ({column_names}) VALUES ({placeholders})'
        values = [str(record.id), record.created_at, record.version] + [record.data.get(prop.name) for prop in schema.properties]

        async with self.connection_pool.acquire() as conn:
            try:
                conn.execute(insert_sql, values)
                logger.info(f"Created record {record.id} in schema {schema.name}")
                return record
            except Exception as e:
                logger.error(f"Error creating record in schema {schema.name}: {e}")
                raise

    async def create_batch(self, schema: Schema, records: List[DataRecord]) -> None:
        if not records:
            return

        start_time = time.time()
        total_records = len(records)
        processed_records = 0

        # Process records in larger batches
        for i in range(0, total_records, self._batch_size):
            batch = records[i:i + self._batch_size]
            await self._process_batch(schema, batch)
            processed_records += len(batch)
            
            # Log progress and metrics
            metrics = self._get_performance_metrics(start_time)
            logger.info(
                f"Progress: {processed_records}/{total_records} records "
                f"({(processed_records/total_records)*100:.1f}%) - "
                f"Time: {metrics['execution_time_ms']:.2f}ms, "
                f"Memory: {metrics['memory_usage_mb']:.2f}MB, "
                f"CPU: {metrics['cpu_percent']:.1f}%"
            )

    async def _process_batch(self, schema: Schema, records: List[DataRecord]) -> None:
        columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(f'"{c}"' for c in columns)
        
        # Create a temporary table for bulk insert
        temp_table = f"temp_bulk_insert_{schema.table_name}"
        create_temp_sql = f'CREATE TEMPORARY TABLE "{temp_table}" AS SELECT * FROM "{schema.table_name}" LIMIT 0'
        
        # Prepare values for bulk insert
        values_to_insert = [
            (str(record.id), record.created_at, record.version) + 
            tuple(record.data.get(prop.name) for prop in schema.properties)
            for record in records
        ]

        async with self.connection_pool.acquire() as conn:
            try:
                # Start transaction
                conn.execute("BEGIN TRANSACTION")
                
                # Create temporary table
                conn.execute(create_temp_sql)
                
                # Bulk insert into temporary table
                insert_sql = f'INSERT INTO "{temp_table}" ({column_names}) VALUES ({placeholders})'
                conn.executemany(insert_sql, values_to_insert)
                
                # Copy from temporary to main table
                copy_sql = f'INSERT INTO "{schema.table_name}" SELECT * FROM "{temp_table}"'
                conn.execute(copy_sql)
                
                # Drop temporary table
                conn.execute(f'DROP TABLE "{temp_table}"')
                
                # Commit transaction
                conn.execute("COMMIT")
                
                logger.info(f"Batch inserted {len(records)} records into schema {schema.name}")
            except Exception as e:
                conn.execute("ROLLBACK")
                logger.error(f"Error batch creating records in schema {schema.name}: {e}")
                raise

    async def get_by_id(self, schema: Schema, record_id: UUID) -> Optional[DataRecord]:
        start_time = time.time()
        select_sql = f'SELECT * FROM "{schema.table_name}" WHERE id = ?'
        async with self.connection_pool.acquire() as conn:
            try:
                result = conn.execute(select_sql, [str(record_id)]).fetchone()
                metrics = self._get_performance_metrics(start_time)
                logger.info(
                    f"Retrieved record {record_id} from schema {schema.name} - "
                    f"Time: {metrics['execution_time_ms']:.2f}ms, "
                    f"Memory: {metrics['memory_usage_mb']:.2f}MB, "
                    f"CPU: {metrics['cpu_percent']:.1f}%"
                )
                if result:
                    return self._map_row_to_data_record(schema, result, conn.description)
                return None
            except Exception as e:
                logger.error(f"Error retrieving record {record_id} from schema {schema.name}: {e}")
                raise

    async def get_all(self, schema: Schema, query_request: DataQueryRequest) -> PaginatedResponse[DataRecord]:
        query_builder = DuckDBQueryBuilder(schema)
        query_builder.add_filters(query_request.filters)
        query_builder.add_sorts(query_request.sort)
        query_builder.add_pagination(query_request.pagination.size, (query_request.pagination.page - 1) * query_request.pagination.size)
        
        select_sql = query_builder.build_select_query()
        params = query_builder.get_params()

        async with self.connection_pool.acquire() as conn:
            try:
                result_relation = conn.execute(select_sql, params)
                rows = result_relation.fetchall()
                description = result_relation.description
                total = await self.count_all(schema, query_request)
                records = [self._map_row_to_data_record(schema, row, description) for row in rows]
                return PaginatedResponse(
                    items=records,
                    total=total,
                    page=query_request.pagination.page,
                    size=query_request.pagination.size,
                    has_next=total > (query_request.pagination.page * query_request.pagination.size),
                    has_previous=query_request.pagination.page > 1
                )
            except Exception as e:
                logger.error(f"Error getting all records from schema {schema.name}: {e}")
                raise

    async def stream_query_results(self, schema: Schema, query_request: DataQueryRequest) -> AsyncIterator[DataRecord]:
        start_time = time.time()
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
                
                metrics = self._get_performance_metrics(start_time)
                logger.info(
                    f"Streaming {len(rows)} records from schema {schema.name} - "
                    f"Time: {metrics['execution_time_ms']:.2f}ms, "
                    f"Memory: {metrics['memory_usage_mb']:.2f}MB, "
                    f"CPU: {metrics['cpu_percent']:.1f}%"
                )
                
                for row in rows:
                    yield self._map_row_to_data_record(schema, row, description)
                    
            except Exception as e:
                logger.error(f"Error streaming records from schema {schema.name}: {e}")
                raise

    async def count_all(self, schema: Schema, query_request: DataQueryRequest) -> int:
        start_time = time.time()
        query_builder = DuckDBQueryBuilder(schema)
        query_builder.add_filters(query_request.filters)
        count_sql = query_builder.build_count_query()
        params = query_builder.get_params()

        async with self.connection_pool.acquire() as conn:
            try:
                result = conn.execute(count_sql, params).fetchone()
                metrics = self._get_performance_metrics(start_time)
                logger.info(
                    f"Counted records in schema {schema.name} - "
                    f"Time: {metrics['execution_time_ms']:.2f}ms, "
                    f"Memory: {metrics['memory_usage_mb']:.2f}MB, "
                    f"CPU: {metrics['cpu_percent']:.1f}%"
                )
                return result[0] if result else 0
            except Exception as e:
                logger.error(f"Error counting records for schema {schema.name}: {e}")
                raise

    def _map_row_to_data_record(self, schema: Schema, row: tuple, description: list) -> DataRecord:
        column_names = [desc[0] for desc in description]
        row_dict = dict(zip(column_names, row))
        
        data_payload = {prop.name: row_dict.get(prop.name) for prop in schema.properties}
        
        return DataRecord(
            id=UUID(row_dict['id']),
            schema_name=schema.name,
            data=data_payload,
            created_at=row_dict['created_at'],
            version=row_dict['version']
        )
