# app/infrastructure/persistence/partitioning/partitioned_data_repository.py

import time
import tempfile
import csv
import os
from typing import Dict, Any, List, Optional, AsyncIterator, Union
from uuid import UUID
from datetime import datetime
from app.domain.entities.schema import Schema
from app.domain.entities.data_record import DataRecord
from app.domain.repositories.data_repository import IDataRepository
from app.application.dto.data_dto import PaginatedResponse
from app.application.dto.query_dto import DataQueryRequest, QueryFilter, FilterOperator
from app.infrastructure.persistence.partitioning.partition_manager import PartitionManager
from app.infrastructure.persistence.partitioning.partition_config import PartitionConfig, DEFAULT_PARTITION_CONFIG
from app.infrastructure.persistence.duckdb.query_builder import DuckDBQueryBuilder
from app.config.logging_config import logger
import asyncio


class PartitionedDataRepository(IDataRepository):
    """
    High-performance partitioned data repository for handling billions of records.
    
    Features:
    - Time-based partitioning for massive datasets
    - Automatic partition routing based on timestamp
    - Cross-partition queries
    - Optimized bulk operations
    - Seamless integration with existing codebase
    """
    
    def __init__(self, partition_config: PartitionConfig = DEFAULT_PARTITION_CONFIG):
        self.partition_manager = PartitionManager(partition_config)
        self.config = partition_config
        
        # Performance settings optimized for billions of records
        self._ultra_batch_size = 500000  # Even larger batches for partitioned approach
        self._stream_chunk_size = 100000
        
    async def initialize(self):
        """Initialize the partitioned repository."""
        await self.partition_manager.initialize()
        logger.info("Partitioned data repository initialized")
    
    def _get_performance_metrics(self, start_time: float) -> Dict[str, float]:
        """Get accurate performance metrics."""
        duration = time.perf_counter() - start_time
        return {
            "duration_ms": duration * 1000,
            "duration_seconds": duration
        }
    
    async def create(self, schema: Schema, data: Dict[str, Any]) -> DataRecord:
        """Single record creation - routes to appropriate partition."""
        start_time = time.perf_counter()
        
        try:
            # Create DataRecord
            record = DataRecord.create(data)
            
            # Determine target partition based on timestamp column
            partition_name = self._get_partition_for_data(data)
            
            # Ensure partition exists
            await self.partition_manager.ensure_partition_exists(partition_name, schema)
            
            # Insert into partition
            async with self.partition_manager.acquire_partition_connection(partition_name) as conn:
                # Set composite key if needed
                if not record.composite_key and schema.primary_key:
                    record.composite_key = schema.get_composite_key_from_data(record.data)
                
                # Build insert SQL
                columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
                placeholders = ", ".join(["?" for _ in columns])
                insert_sql = f'INSERT INTO "{schema.table_name}" ({", ".join([f'"{col}"' for col in columns])}) VALUES ({placeholders})'
                
                # Prepare values
                values = [str(record.id), record.created_at.isoformat(), record.version]
                values.extend([record.data.get(prop.name) for prop in schema.properties])
                
                conn.execute(insert_sql, values)
                
                metrics = self._get_performance_metrics(start_time)
                logger.info(f"[PARTITIONED-REPO] Created record in partition {partition_name} "
                           f"in {metrics['duration_ms']:.2f}ms")
                
                return record
                
        except Exception as e:
            logger.error(f"Error creating record in partitioned repository: {e}")
            raise
    
    async def create_batch(self, schema: Schema, records: List[DataRecord]) -> None:
        """Ultra high-performance partitioned batch creation."""
        if not records:
            return
        
        start_time = time.perf_counter()
        
        try:
            # Group records by partition
            partition_groups = self._group_records_by_partition(records)
            
            logger.info(f"[PARTITIONED-REPO] Batch creating {len(records)} records across {len(partition_groups)} partitions")
            
            # Process each partition group
            tasks = []
            for partition_name, partition_records in partition_groups.items():
                task = self._create_batch_in_partition(schema, partition_name, partition_records)
                tasks.append(task)
            
            # Execute all partition inserts concurrently
            await asyncio.gather(*tasks)
            
            metrics = self._get_performance_metrics(start_time)
            throughput = len(records) / metrics['duration_seconds'] if metrics['duration_seconds'] > 0 else 0
            
            logger.info(f"[PARTITIONED-REPO] 🚀 ULTRA-FAST batch created {len(records)} records "
                       f"across {len(partition_groups)} partitions in {metrics['duration_ms']:.2f}ms "
                       f"({int(throughput)} records/second)")
            
        except Exception as e:
            logger.error(f"Error in partitioned batch creation: {e}")
            raise
    
    async def _create_batch_in_partition(self, schema: Schema, partition_name: str, records: List[DataRecord]) -> None:
        """Create a batch of records in a specific partition using ultra-fast COPY FROM."""
        start_time = time.perf_counter()
        temp_file = None
        
        try:
            # Ensure partition exists
            await self.partition_manager.ensure_partition_exists(partition_name, schema)
            
            # Create temporary CSV file
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, 
                                                  newline='', encoding='utf-8')
            
            # Define columns
            columns = ["id", "created_at", "version"] + [prop.name for prop in schema.properties]
            
            # Write CSV data
            writer = csv.writer(temp_file, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(columns)  # Header
            
            for record in records:
                # Ensure composite key is set
                if not record.composite_key and schema.primary_key:
                    record.composite_key = schema.get_composite_key_from_data(record.data)
                
                row_data = [str(record.id), record.created_at.isoformat(), str(record.version)]
                
                # Process schema properties with proper encoding
                for prop in schema.properties:
                    value = record.data.get(prop.name, '')
                    if value is None:
                        row_data.append('')
                    elif isinstance(value, str):
                        row_data.append(value.encode('utf-8', errors='replace').decode('utf-8'))
                    else:
                        row_data.append(str(value))
                
                writer.writerow(row_data)
            
            temp_file.close()
            
            # Ultra-fast COPY FROM operation
            async with self.partition_manager.acquire_partition_connection(partition_name) as conn:
                try:
                    conn.execute("BEGIN TRANSACTION")
                    
                    # Optimize for bulk insert
                    conn.execute("PRAGMA enable_progress_bar=false")
                    conn.execute("PRAGMA threads=8")
                    
                    # Create temporary table
                    temp_table = f"temp_copy_{schema.table_name}_{int(time.time())}"
                    create_temp_sql = f'CREATE TEMPORARY TABLE "{temp_table}" AS SELECT * FROM "{schema.table_name}" LIMIT 0'
                    conn.execute(create_temp_sql)
                    
                    # COPY FROM CSV
                    copy_sql = f"""
                        COPY "{temp_table}" FROM '{temp_file.name}' 
                        (FORMAT CSV, HEADER true, DELIMITER ',', QUOTE '"', ENCODING 'utf-8', IGNORE_ERRORS true)
                    """
                    conn.execute(copy_sql)
                    
                    # Insert with duplicate handling
                    insert_sql = f'INSERT OR IGNORE INTO "{schema.table_name}" SELECT * FROM "{temp_table}"'
                    conn.execute(insert_sql)
                    
                    # Clean up temp table
                    conn.execute(f'DROP TABLE "{temp_table}"')
                    
                    conn.execute("COMMIT")
                    
                    metrics = self._get_performance_metrics(start_time)
                    throughput = len(records) / metrics['duration_seconds'] if metrics['duration_seconds'] > 0 else 0
                    
                    logger.info(f"[PARTITIONED-REPO] Partition {partition_name}: {len(records)} records "
                               f"in {metrics['duration_ms']:.2f}ms ({int(throughput)} records/second)")
                    
                except Exception as e:
                    conn.execute("ROLLBACK")
                    logger.error(f"COPY FROM operation failed in partition {partition_name}: {e}")
                    raise
                    
        finally:
            # Clean up temporary file
            if temp_file and os.path.exists(temp_file.name):
                try:
                    os.unlink(temp_file.name)
                except Exception as e:
                    logger.warning(f"Failed to cleanup temp file {temp_file.name}: {e}")
    
    def _get_partition_for_data(self, data: Dict[str, Any]) -> str:
        """Determine which partition should contain this data."""
        timestamp_value = data.get(self.config.partition_column)
        if timestamp_value:
            return self.partition_manager.get_partition_for_timestamp(str(timestamp_value))
        else:
            # Fallback to current date if no timestamp
            return self.partition_manager.get_partition_for_timestamp(datetime.now().isoformat())
    
    def _group_records_by_partition(self, records: List[DataRecord]) -> Dict[str, List[DataRecord]]:
        """Group records by their target partition."""
        partition_groups = {}
        
        for record in records:
            partition_name = self._get_partition_for_data(record.data)
            
            if partition_name not in partition_groups:
                partition_groups[partition_name] = []
            
            partition_groups[partition_name].append(record)
        
        return partition_groups
    
    async def get_by_id(self, schema: Schema, record_id: UUID) -> Optional[DataRecord]:
        """Get a record by ID - searches across all partitions if needed."""
        start_time = time.perf_counter()
        
        # First try to find in main database
        try:
            async with self.partition_manager.acquire_main_connection() as conn:
                select_sql = f'SELECT * FROM "{schema.table_name}" WHERE id = ?'
                result = conn.execute(select_sql, [str(record_id)]).fetchone()
                
                if result:
                    return self._map_row_to_data_record(schema, result, conn.description)
        except Exception as e:
            logger.warning(f"Error searching main database: {e}")
        
        # If not found in main, search partitions (this could be expensive)
        existing_partitions = self.config.list_existing_partitions()
        
        for partition_name in existing_partitions:
            try:
                async with self.partition_manager.acquire_partition_connection(partition_name) as conn:
                    select_sql = f'SELECT * FROM "{schema.table_name}" WHERE id = ?'
                    result = conn.execute(select_sql, [str(record_id)]).fetchone()
                    
                    if result:
                        metrics = self._get_performance_metrics(start_time)
                        logger.info(f"[PARTITIONED-REPO] Found record in partition {partition_name} "
                                   f"in {metrics['duration_ms']:.2f}ms")
                        return self._map_row_to_data_record(schema, result, conn.description)
                        
            except Exception as e:
                logger.warning(f"Error searching partition {partition_name}: {e}")
                continue
        
        return None
    
    async def get_all(self, schema: Schema, query_request: DataQueryRequest) -> PaginatedResponse[DataRecord]:
        """High-performance paginated queries across partitions."""
        start_time = time.perf_counter()
        
        # Determine which partitions to query based on filters
        target_partitions = self._determine_target_partitions(query_request)
        
        if not target_partitions:
            # If no specific partitions identified, query main database
            target_partitions = ["main"]
        
        logger.info(f"[PARTITIONED-REPO] Querying {len(target_partitions)} partitions")
        
        # For simplicity, query each partition and aggregate results
        # In production, you might want to implement more sophisticated query planning
        all_records = []
        total_count = 0
        
        for partition_name in target_partitions:
            try:
                if partition_name == "main":
                    connection_context = self.partition_manager.acquire_main_connection()
                else:
                    connection_context = self.partition_manager.acquire_partition_connection(partition_name)
                
                async with connection_context as conn:
                    # Build query
                    query_builder = DuckDBQueryBuilder(schema)
                    query_builder.add_filters(query_request.filters)
                    query_builder.add_sorts(query_request.sort)
                    
                    # Get count
                    count_sql = query_builder.build_count_query()
                    count_params = query_builder.get_params()
                    count_result = conn.execute(count_sql, count_params).fetchone()
                    partition_count = count_result[0] if count_result else 0
                    total_count += partition_count
                    
                    # Get records (simplified - you might want to implement distributed pagination)
                    if query_request.pagination:
                        offset = (query_request.pagination.page - 1) * query_request.pagination.size
                        query_builder.add_pagination(query_request.pagination.size, offset)
                    
                    select_sql = query_builder.build_select_query()
                    select_params = query_builder.get_params()
                    
                    result_relation = conn.execute(select_sql, select_params)
                    rows = result_relation.fetchall()
                    description = result_relation.description
                    
                    partition_records = [self._map_row_to_data_record_optimized(schema, row, description) for row in rows]
                    all_records.extend(partition_records)
                    
            except Exception as e:
                logger.warning(f"Error querying partition {partition_name}: {e}")
                continue
        
        # Calculate pagination (simplified)
        current_page = query_request.pagination.page if query_request.pagination else 1
        page_size = query_request.pagination.size if query_request.pagination else len(all_records)
        has_next = len(all_records) >= page_size
        has_previous = current_page > 1
        
        metrics = self._get_performance_metrics(start_time)
        logger.info(f"[PARTITIONED-REPO] Retrieved {len(all_records)} records from {len(target_partitions)} partitions "
                   f"in {metrics['duration_ms']:.2f}ms")
        
        return PaginatedResponse(
            items=all_records,
            total=total_count,
            page=current_page,
            size=page_size,
            has_next=has_next,
            has_previous=has_previous
        )
    
    def _determine_target_partitions(self, query_request: DataQueryRequest) -> List[str]:
        """Determine which partitions need to be queried based on filters."""
        target_partitions = []
        
        if not query_request.filters:
            # No filters - need to query all partitions
            return self.config.list_existing_partitions()
        
        # Look for date range filters on the partition column
        date_filters = [f for f in query_request.filters if f.field == self.config.partition_column]
        
        if date_filters:
            # Extract date ranges from filters
            start_date = None
            end_date = None
            
            for date_filter in date_filters:
                if date_filter.operator in [FilterOperator.GTE, FilterOperator.GT]:
                    try:
                        start_date = datetime.fromisoformat(str(date_filter.value).replace('Z', '+00:00'))
                    except:
                        pass
                elif date_filter.operator in [FilterOperator.LTE, FilterOperator.LT]:
                    try:
                        end_date = datetime.fromisoformat(str(date_filter.value).replace('Z', '+00:00'))
                    except:
                        pass
                elif date_filter.operator == FilterOperator.EQ:
                    try:
                        filter_date = datetime.fromisoformat(str(date_filter.value).replace('Z', '+00:00'))
                        start_date = filter_date
                        end_date = filter_date
                    except:
                        pass
            
            if start_date or end_date:
                # Use a reasonable default range if only one bound is specified
                if not start_date:
                    start_date = datetime(2020, 1, 1)  # Reasonable default
                if not end_date:
                    end_date = datetime.now()
                
                target_partitions = self.partition_manager.get_partitions_for_date_range(start_date, end_date)
                
                # Filter to only existing partitions
                existing_partitions = set(self.config.list_existing_partitions())
                target_partitions = [p for p in target_partitions if p in existing_partitions]
        
        if not target_partitions:
            # Fallback to all existing partitions
            target_partitions = self.config.list_existing_partitions()
        
        return target_partitions
    
    async def stream_query_results(self, schema: Schema, query_request: DataQueryRequest) -> AsyncIterator[DataRecord]:
        """High-performance streaming across partitions."""
        start_time = time.perf_counter()
        records_processed = 0
        
        # Determine target partitions
        target_partitions = self._determine_target_partitions(query_request)
        logger.info(f"[PARTITIONED-REPO] Streaming from {len(target_partitions)} partitions")
        
        for partition_name in target_partitions:
            try:
                if partition_name == "main":
                    connection_context = self.partition_manager.acquire_main_connection()
                else:
                    connection_context = self.partition_manager.acquire_partition_connection(partition_name)
                
                async with connection_context as conn:
                    query_builder = DuckDBQueryBuilder(schema)
                    query_builder.add_filters(query_request.filters)
                    query_builder.add_sorts(query_request.sort)
                    
                    select_sql = query_builder.build_select_query_without_pagination()
                    params = query_builder.get_params()
                    
                    result_relation = conn.execute(select_sql, params)
                    description = result_relation.description
                    
                    # Stream results in chunks
                    while True:
                        rows = result_relation.fetchmany(self._stream_chunk_size)
                        if not rows:
                            break
                        
                        for row in rows:
                            yield self._map_row_to_data_record_optimized(schema, row, description)
                            records_processed += 1
                            
                            # Respect limit if specified
                            if query_request.pagination and records_processed >= query_request.pagination.size:
                                return
                
            except Exception as e:
                logger.warning(f"Error streaming from partition {partition_name}: {e}")
                continue
        
        metrics = self._get_performance_metrics(start_time)
        throughput = records_processed / metrics['duration_seconds'] if metrics['duration_seconds'] > 0 else 0
        logger.info(f"[PARTITIONED-REPO] Streamed {records_processed} records from {len(target_partitions)} partitions "
                   f"in {metrics['duration_ms']:.2f}ms ({int(throughput)} records/second)")
    
    async def count_all(self, schema: Schema, query_request: DataQueryRequest) -> int:
        """Count records across partitions."""
        target_partitions = self._determine_target_partitions(query_request)
        total_count = 0
        
        for partition_name in target_partitions:
            try:
                if partition_name == "main":
                    connection_context = self.partition_manager.acquire_main_connection()
                else:
                    connection_context = self.partition_manager.acquire_partition_connection(partition_name)
                
                async with connection_context as conn:
                    query_builder = DuckDBQueryBuilder(schema)
                    query_builder.add_filters(query_request.filters)
                    count_sql = query_builder.build_count_query()
                    params = query_builder.get_params()
                    
                    result = conn.execute(count_sql, params).fetchone()
                    partition_count = result[0] if result else 0
                    total_count += partition_count
                    
            except Exception as e:
                logger.warning(f"Error counting in partition {partition_name}: {e}")
                continue
        
        return total_count
    
    def _map_row_to_data_record(self, schema: Schema, row: tuple, description) -> DataRecord:
        """Map database row to DataRecord."""
        column_names = [desc[0] for desc in description]
        row_dict = dict(zip(column_names, row))
        
        # Extract system fields
        record_id = UUID(row_dict['id'])
        created_at = row_dict['created_at']
        version = row_dict['version']
        
        # Extract data fields
        data = {}
        for prop in schema.properties:
            if prop.name in row_dict:
                data[prop.name] = row_dict[prop.name]
        
        # Create record
        record = DataRecord(
            id=record_id,
            data=data,
            created_at=created_at,
            version=version
        )
        
        # Set composite key if schema has primary key
        if schema.primary_key:
            record.composite_key = schema.get_composite_key_from_data(data)
        
        return record
    
    def _map_row_to_data_record_optimized(self, schema: Schema, row: tuple, description) -> DataRecord:
        """Optimized version of row mapping for high performance."""
        # Same implementation as above, but could be optimized further if needed
        return self._map_row_to_data_record(schema, row, description)
    
    async def close(self):
        """Close all connections and clean up resources."""
        await self.partition_manager.close_all_connections()
        logger.info("Partitioned data repository closed")
    
    async def get_partition_statistics(self) -> Dict[str, Any]:
        """Get statistics about all partitions."""
        return await self.partition_manager.get_partition_statistics()
