import asyncio
from typing import Dict, Any, Optional, List, AsyncIterator
from uuid import UUID, uuid4
from contextlib import asynccontextmanager
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor
import time
import os
import psutil

from app.domain.entities.schema import Schema
from app.domain.entities.data_record import DataRecord
from app.domain.repositories.data_repository import IDataRepository
from app.infrastructure.persistence.duckdb.connection_pool import DuckDBConnectionPool
from app.infrastructure.persistence.duckdb.schema_manager import DuckDBSchemaManager
from app.infrastructure.persistence.mappers.generic_mapper import map_dict_to_data_record
from app.config.logging_config import logger
from app.domain.exceptions import InvalidDataException, RepositoryException

class SimplifiedDuckDBDataRepository(IDataRepository):
    """Simplified DuckDB repository for read and write operations."""
    
    def __init__(
        self, 
        connection_pool: DuckDBConnectionPool, 
        schema_manager: DuckDBSchemaManager,
        default_batch_size: int = 5000
    ):
        self.connection_pool = connection_pool
        self.schema_manager = schema_manager
        self.default_batch_size = default_batch_size
        num_cpu_cores = os.cpu_count() or 4
        self.executor = ThreadPoolExecutor(max_workers=min(32, max(4, num_cpu_cores * 2)))

    async def create(self, schema: Schema, data: Dict[str, Any]) -> DataRecord:
        """Create a single record."""
        try:
            result = await self.create_batch(schema, [data])
            if result["success_count"] == 0:
                raise InvalidDataException(f"Failed to create record: {result.get('error', 'Unknown error')}")
            record_id = UUID(data.get("id", str(uuid4())))
            return map_dict_to_data_record(data, schema.name, record_id)
        except Exception as e:
            logger.error(f"Single record creation failed for schema {schema.name}: {e}")
            raise RepositoryException(f"Failed to create record: {str(e)}") from e

    async def create_batch(self, schema: Schema, data_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Batch insert records."""
        if not data_batch:
            return {"success_count": 0, "failure_count": 0, "total_time_seconds": 0.0}
        
        batch_size = self._calculate_optimal_batch_size(len(data_batch))
        start_time = time.perf_counter()
        
        total_success = 0
        total_failures = 0
        error = None
        
        try:
            await self._ensure_schema_exists(schema)
            for i in range(0, len(data_batch), batch_size):
                chunk = data_batch[i:i + batch_size]
                chunk_result = await self._process_batch_chunk(schema, chunk)
                total_success += chunk_result["success_count"]
                total_failures += chunk_result["failure_count"]
                if chunk_result.get("error"):
                    error = chunk_result["error"]
        
        except Exception as e:
            logger.error(f"Batch create failed: {e}")
            total_failures = len(data_batch)
            error = str(e)
        
        total_time = time.perf_counter() - start_time
        return {
            "success_count": total_success,
            "failure_count": total_failures,
            "total_time_seconds": total_time,
            "error": error
        }

    async def _process_batch_chunk(self, schema: Schema, chunk: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch chunk with transaction."""
        def _execute_transaction():
            success_count = 0
            error = None
            try:
                for record in chunk:
                    if "id" not in record or not record["id"]:
                        record["id"] = str(uuid4())
                arrow_table = pa.Table.from_pylist(chunk)
                with self.connection_pool.get_connection() as conn:
                    conn.execute("BEGIN TRANSACTION")
                    try:
                        temp_table_name = f"temp_{schema.name}_{int(time.time() * 1000000)}"
                        conn.register(temp_table_name, arrow_table)
                        conn.execute(f"INSERT INTO {schema.name} SELECT * FROM {temp_table_name}")
                        conn.execute("COMMIT")
                        success_count = len(chunk)
                        conn.unregister(temp_table_name)
                    except Exception as e:
                        conn.execute("ROLLBACK")
                        raise e
            except Exception as e:
                logger.error(f"Batch chunk failed: {e}")
                error = str(e)
                return {"success_count": 0, "failure_count": len(chunk), "error": error}
            return {"success_count": success_count, "failure_count": 0, "error": None}
        
        return await asyncio.get_event_loop().run_in_executor(self.executor, _execute_transaction)

    async def get_by_id(self, schema: Schema, record_id: UUID) -> Optional[DataRecord]:
        """Retrieve a single record by ID."""
        def _fetch_record():
            with self.connection_pool.get_connection() as conn:
                try:
                    query = f"SELECT * FROM {schema.name} WHERE id = ?"
                    result = conn.execute(query, [str(record_id)]).fetchone()
                    if result:
                        column_names = [desc[0] for desc in conn.description]
                        return dict(zip(column_names, result))
                    return None
                except Exception as e:
                    logger.error(f"Failed to fetch record {record_id} from {schema.name}: {e}")
                    return None
        
        row_dict = await asyncio.get_event_loop().run_in_executor(self.executor, _fetch_record)
        if row_dict:
            return map_dict_to_data_record(row_dict, schema.name, UUID(row_dict["id"]))
        return None

    async def stream_all(self, schema: Schema, batch_size: int = 1000) -> AsyncIterator[List[DataRecord]]:
        """Stream all records in batches."""
        offset = 0
        consecutive_empty_batches = 0
        max_empty_batches = 3
        
        while consecutive_empty_batches < max_empty_batches:
            def _fetch_batch():
                with self.connection_pool.get_connection() as conn:
                    try:
                        query = f"SELECT * FROM {schema.name} LIMIT {batch_size} OFFSET {offset}"
                        results = conn.execute(query).fetchall()
                        if not results:
                            return []
                        column_names = [desc[0] for desc in conn.description]
                        return [map_dict_to_data_record(
                            dict(zip(column_names, row)), schema.name, UUID(row[0])
                        ) for row in results]
                    except Exception as e:
                        logger.error(f"Stream batch fetch failed at offset {offset}: {e}")
                        return []
            
            batch = await asyncio.get_event_loop().run_in_executor(self.executor, _fetch_batch)
            if not batch:
                consecutive_empty_batches += 1
            else:
                consecutive_empty_batches = 0
                yield batch
            offset += batch_size

    def _calculate_optimal_batch_size(self, total_records: int) -> int:
        """Calculate optimal batch size based on memory."""
        available_memory = psutil.virtual_memory().available / (1024**2)
        if total_records > 100000 or available_memory < 1000:
            return min(self.default_batch_size, 1000)
        return self.default_batch_size

    async def _ensure_schema_exists(self, schema: Schema):
        """Ensure schema table exists."""
        def _create_table():
            with self.connection_pool.get_connection() as conn:
                create_table_sql = self.schema_manager.get_create_table_statement(schema)
                conn.execute(create_table_sql)
        
        await asyncio.get_event_loop().run_in_executor(self.executor, _create_table)

    async def close(self):
        """Clean up resources."""
        logger.info("Shutting down SimplifiedDuckDBDataRepository...")
        self.executor.shutdown(wait=True)
        logger.info("Repository shutdown complete")
