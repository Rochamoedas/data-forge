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
        # Optimized for millions of rows - single high-performance configuration
        self._ultra_batch_size = 50000  # Large batches for maximum throughput
        self._stream_chunk_size = 10000  # Optimal streaming chunk size
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
        """Single record creation - optimized for high performance"""
        start_time = time.perf_counter()
        
        # Extract composite key if schema defines one
        composite_key = schema.get_composite_key_from_data(data)
        record = DataRecord(schema_name=schema.name, data=data, composite_key=composite_key)
        
        columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
        placeholders = ", ".join(["?"] * len(columns))
        column_names = ", ".join(f'"{c}"' for c in columns)
        insert_sql = f'INSERT OR IGNORE INTO "{schema.table_name}" ({column_names}) VALUES ({placeholders})'
        values = [str(record.id), record.created_at, record.version] + [record.data.get(prop.name) for prop in schema.properties]

        async with self.connection_pool.acquire() as conn:
            try:
                conn.execute(insert_sql, values)
                
                return record
            except Exception as e:
                logger.error(f"Error creating record in schema {schema.name}: {e}")
                raise

    async def create_batch(self, schema: Schema, records: List[DataRecord]) -> None:
        """🚀 ULTRA HIGH-PERFORMANCE batch creation optimized for millions of rows"""
        if not records:
            return

        start_time = time.perf_counter()
        total_records = len(records)
        
        # Always use ultra-fast COPY FROM for maximum performance
        await self._ultra_fast_copy_insert(schema, records)

        # Final metrics
        metrics = self._get_performance_metrics(start_time)
        throughput = total_records / (metrics["execution_time_ms"] / 1000) if metrics["execution_time_ms"] > 0 else 0
        
        logger.info(f"🚀 ULTRA-FAST batch inserted {total_records} records into {schema.name} "
                   f"in {metrics['execution_time_ms']:.2f}ms ({int(throughput)} records/second)")
        


    async def _ultra_fast_copy_insert(self, schema: Schema, records: List[DataRecord]) -> None:
        """🚀 MAXIMUM PERFORMANCE: Use DuckDB COPY FROM for massive bulk inserts with proper encoding"""
        import tempfile
        import csv
        import os
        
        start_time = time.perf_counter()
        temp_file = None
        
        try:
            # Create temporary CSV file with explicit UTF-8 encoding
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, 
                                                  newline='', encoding='utf-8')
            
            # Define columns in correct order
            columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
            
            # Write CSV data with proper encoding handling
            writer = csv.writer(temp_file, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(columns)  # Header
            
            for record in records:
                # Ensure composite key is set if not already
                if not record.composite_key and schema.primary_key:
                    record.composite_key = schema.get_composite_key_from_data(record.data)
                
                # Handle data encoding properly
                row_data = []
                row_data.append(str(record.id))
                row_data.append(record.created_at.isoformat())
                row_data.append(str(record.version))
                
                # Process schema properties with proper encoding
                for prop in schema.properties:
                    value = record.data.get(prop.name, '')
                    if value is None:
                        row_data.append('')
                    elif isinstance(value, str):
                        # Ensure proper UTF-8 encoding for strings
                        row_data.append(value.encode('utf-8', errors='replace').decode('utf-8'))
                    else:
                        row_data.append(str(value))
                
                writer.writerow(row_data)
            
            temp_file.close()
            
            # Use DuckDB COPY FROM for ultra-fast bulk insert
            async with self.connection_pool.acquire() as conn:
                try:
                    conn.execute("BEGIN TRANSACTION")
                    
                    # Optimize DuckDB for maximum performance with millions of rows
                    conn.execute("PRAGMA enable_progress_bar=false")
                    conn.execute("PRAGMA threads=8")
                    conn.execute("PRAGMA memory_limit='8GB'")
                    conn.execute("PRAGMA max_memory='8GB'")
                    
                    # Create temporary table for COPY operation
                    temp_table = f"temp_copy_{schema.table_name}_{int(time.time())}"
                    create_temp_sql = f'CREATE TEMPORARY TABLE "{temp_table}" AS SELECT * FROM "{schema.table_name}" LIMIT 0'
                    conn.execute(create_temp_sql)
                    
                    # Ultra-fast COPY FROM CSV with proper encoding settings
                    copy_sql = f"""
                        COPY "{temp_table}" FROM '{temp_file.name}' 
                        (FORMAT CSV, HEADER true, DELIMITER ',', QUOTE '"', ENCODING 'utf-8', IGNORE_ERRORS true)
                    """
                    conn.execute(copy_sql)
                    
                    # Insert with duplicate handling for maximum performance
                    insert_sql = f'INSERT OR IGNORE INTO "{schema.table_name}" SELECT * FROM "{temp_table}"'
                    conn.execute(insert_sql)
                    
                    # Clean up temp table
                    conn.execute(f'DROP TABLE "{temp_table}"')
                    
                    conn.execute("COMMIT")
                    
                    # Performance metrics
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    throughput = len(records) / (duration_ms / 1000) if duration_ms > 0 else 0
                    
                    logger.info(f"🚀 ULTRA-FAST COPY inserted {len(records)} records into schema {schema.name} "
                               f"in {duration_ms:.2f}ms ({int(throughput)} records/second)")
                    
                except Exception as e:
                    conn.execute("ROLLBACK")
                    logger.error(f"COPY FROM operation failed: {e}")
                    raise
                    
        finally:
            # Clean up temporary file
            if temp_file and os.path.exists(temp_file.name):
                try:
                    os.unlink(temp_file.name)
                except Exception as e:
                    logger.warning(f"Failed to cleanup temp file {temp_file.name}: {e}")

    async def get_by_id(self, schema: Schema, record_id: UUID) -> Optional[DataRecord]:
        start_time = time.perf_counter()
        select_sql = f'SELECT * FROM "{schema.table_name}" WHERE id = ?'
        async with self.connection_pool.acquire() as conn:
            try:
                result = conn.execute(select_sql, [str(record_id)]).fetchone()
                
                if result:
                    return self._map_row_to_data_record(schema, result, conn.description)
                return None
            except Exception as e:
                logger.error(f"Error retrieving record {record_id} from schema {schema.name}: {e}")
                raise

    async def get_all(self, schema: Schema, query_request: DataQueryRequest) -> PaginatedResponse[DataRecord]:
        """High-performance paginated queries optimized for millions of rows"""
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
                
                records = [self._map_row_to_data_record_optimized(schema, row, description) for row in rows]
                
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
        """🚀 ULTRA HIGH-PERFORMANCE streaming optimized for millions of rows"""
        start_time = time.perf_counter()
        query_builder = DuckDBQueryBuilder(schema)
        query_builder.add_filters(query_request.filters)
        query_builder.add_sorts(query_request.sort)
        
        # Build query based on pagination
        if query_request.pagination:
            offset = (query_request.pagination.page - 1) * query_request.pagination.size
            query_builder.add_pagination(query_request.pagination.size, offset)
            select_sql = query_builder.build_select_query()
            requested_limit = query_request.pagination.size
        else:
            select_sql = query_builder.build_select_query_without_pagination()
            requested_limit = 1000000  # Default limit for unlimited queries
        
        params = query_builder.get_params()

        async with self.connection_pool.acquire() as conn:
            try:
                # Execute query and get results
                result_relation = conn.execute(select_sql, params)
                description = result_relation.description
                
                records_processed = 0
                total_fetch_time = 0
                
                # Stream results in optimized chunks
                while True:
                    fetch_start = time.perf_counter()
                    rows = result_relation.fetchmany(self._stream_chunk_size)
                    fetch_time = time.perf_counter() - fetch_start
                    total_fetch_time += fetch_time
                    
                    if not rows:
                        break
                    
                    # Process and yield records in optimized batches
                    for row in rows:
                        yield self._map_row_to_data_record_optimized(schema, row, description)
                        records_processed += 1
                        
                        # Respect limit if specified
                        if records_processed >= requested_limit:
                            return
                
                # Enhanced performance metrics
                total_time = time.perf_counter() - start_time
                query_metrics = self._get_performance_metrics(start_time)
                throughput = records_processed / total_time if total_time > 0 else 0
                

                
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
                
                if result:
                    return self._map_row_to_data_record_optimized(schema, result, conn.description)
                return None
            except Exception as e:
                logger.error(f"Error retrieving record by composite key {composite_key} from schema {schema.name}: {e}")
                raise

    def _map_row_to_data_record(self, schema: Schema, row: tuple, description: list) -> DataRecord:
        """Standard record mapping for compatibility"""
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
