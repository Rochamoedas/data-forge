# app/infrastructure/persistence/partitioning/partition_manager.py

import asyncio
import duckdb
import os
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from app.infrastructure.persistence.partitioning.partition_config import PartitionConfig, DEFAULT_PARTITION_CONFIG
from app.domain.entities.schema import Schema
from app.config.logging_config import logger
from app.config.settings import settings


class PartitionManager:
    """
    Manages partitioned DuckDB databases for handling billions of records.
    
    Features:
    - Time-based partitioning (yearly, monthly, weekly, daily)
    - Automatic partition creation and management
    - Cross-partition queries
    - Performance optimization for massive datasets
    """
    
    def __init__(self, partition_config: PartitionConfig = DEFAULT_PARTITION_CONFIG):
        self.config = partition_config
        self._partition_connections: Dict[str, duckdb.DuckDBPyConnection] = {}
        self._connection_lock = asyncio.Lock()
        self._main_connection: Optional[duckdb.DuckDBPyConnection] = None
        
    async def initialize(self):
        """Initialize the partition manager and main database connection."""
        async with self._connection_lock:
            if self._main_connection is None:
                logger.info(f"Initializing partition manager with config: {self.config.strategy.value}")
                
                # Initialize main database connection
                self._main_connection = duckdb.connect(
                    database=self.config.main_database_path,
                    read_only=False,
                    config=settings.DUCKDB_PERFORMANCE_CONFIG
                )
                
                # Apply runtime settings
                await self._apply_runtime_settings(self._main_connection)
                
                # Ensure partition directory exists
                os.makedirs(self.config.partition_directory, exist_ok=True)
                
                logger.info(f"Partition manager initialized. Partition directory: {self.config.partition_directory}")
    
    async def _apply_runtime_settings(self, connection: duckdb.DuckDBPyConnection):
        """Apply DuckDB runtime settings for optimal performance."""
        runtime_config = {
            'memory_limit': '8GB',
            'threads': 8,
            'enable_object_cache': True,
            'disabled_optimizers': '',
        }
        
        for key, value in runtime_config.items():
            if value is not None:
                if isinstance(value, str) and value:
                    connection.execute(f"SET {key} = '{value}'")
                elif not isinstance(value, str):
                    connection.execute(f"SET {key} = {str(value).lower()}")
    
    async def get_partition_connection(self, partition_name: str) -> duckdb.DuckDBPyConnection:
        """Get or create a connection to a specific partition."""
        async with self._connection_lock:
            if partition_name not in self._partition_connections:
                partition_path = self.config.get_partition_path(partition_name)
                
                logger.info(f"Creating new partition connection: {partition_name} at {partition_path}")
                
                connection = duckdb.connect(
                    database=partition_path,
                    read_only=False,
                    config=settings.DUCKDB_PERFORMANCE_CONFIG
                )
                
                await self._apply_runtime_settings(connection)
                
                # Install arrow extension if needed
                if settings.DUCKDB_ARROW_EXTENSION_ENABLED:
                    try:
                        connection.execute("INSTALL arrow")
                        connection.execute("LOAD arrow")
                    except Exception as e:
                        logger.warning(f"Could not install arrow extension in partition {partition_name}: {e}")
                
                self._partition_connections[partition_name] = connection
                
                # Manage connection pool size
                await self._manage_connection_pool()
            
            return self._partition_connections[partition_name]
    
    async def _manage_connection_pool(self):
        """Manage the size of the connection pool to prevent memory issues."""
        if len(self._partition_connections) > self.config.max_partitions_in_memory:
            # Close oldest connections
            oldest_partitions = list(self._partition_connections.keys())[:-self.config.max_partitions_in_memory]
            for partition_name in oldest_partitions:
                try:
                    self._partition_connections[partition_name].close()
                    del self._partition_connections[partition_name]
                    logger.info(f"Closed connection to partition: {partition_name}")
                except Exception as e:
                    logger.warning(f"Error closing partition connection {partition_name}: {e}")
    
    async def ensure_partition_exists(self, partition_name: str, schema: Schema):
        """Ensure a partition exists and has the correct schema."""
        partition_path = self.config.get_partition_path(partition_name)
        
        if not os.path.exists(partition_path):
            logger.info(f"Creating new partition: {partition_name}")
            
            # Create the partition database file
            connection = await self.get_partition_connection(partition_name)
            
            # Create the schema table in the partition
            await self._create_table_in_partition(connection, schema)
            
            logger.info(f"Partition created successfully: {partition_name}")
        else:
            # Verify the schema exists
            connection = await self.get_partition_connection(partition_name)
            await self._ensure_table_schema(connection, schema)
    
    async def _create_table_in_partition(self, connection: duckdb.DuckDBPyConnection, schema: Schema):
        """Create the table schema in a partition."""
        try:
            # Build column definitions
            column_defs = ", ".join([f'"{prop.name}" {prop.db_type}' for prop in schema.properties])
            
            # Build composite primary key constraint if defined
            composite_pk_constraint = ""
            if schema.primary_key:
                pk_columns = ", ".join([f'"{col}"' for col in schema.primary_key])
                composite_pk_constraint = f", UNIQUE({pk_columns})"
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS "{schema.table_name}" (
                id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP,
                version INTEGER,
                {column_defs}{composite_pk_constraint}
            );
            """
            
            connection.execute(create_table_sql)
            
            # Create indexes for performance
            connection.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_id" ON "{schema.table_name}"(id);')
            connection.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_created_at" ON "{schema.table_name}"(created_at);')
            connection.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_{self.config.partition_column}" ON "{schema.table_name}"({self.config.partition_column});')
            
            # Create composite key index for performance
            if schema.primary_key:
                pk_columns = ", ".join([f'"{col}"' for col in schema.primary_key])
                connection.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_composite_key" ON "{schema.table_name}"({pk_columns});')
            
            logger.info(f"Table {schema.table_name} created in partition with indexes")
            
        except Exception as e:
            logger.error(f"Error creating table in partition: {e}")
            raise
    
    async def _ensure_table_schema(self, connection: duckdb.DuckDBPyConnection, schema: Schema):
        """Ensure the table schema exists in the partition."""
        try:
            # Check if table exists
            result = connection.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{schema.table_name}'
            """).fetchone()
            
            if result[0] == 0:
                await self._create_table_in_partition(connection, schema)
            
        except Exception as e:
            logger.warning(f"Error checking table schema: {e}")
            # Create table if check fails
            await self._create_table_in_partition(connection, schema)
    
    def get_partitions_for_date_range(self, start_date: datetime, end_date: datetime) -> List[str]:
        """Get all partitions that overlap with the given date range."""
        partitions = []
        current_date = start_date
        
        while current_date <= end_date:
            partition_name = self.config.get_partition_for_date(current_date)
            if partition_name not in partitions:
                partitions.append(partition_name)
            
            # Move to next partition period
            if self.config.strategy.value == "yearly":
                current_date = datetime(current_date.year + 1, 1, 1)
            elif self.config.strategy.value == "monthly":
                if current_date.month == 12:
                    current_date = datetime(current_date.year + 1, 1, 1)
                else:
                    current_date = datetime(current_date.year, current_date.month + 1, 1)
            elif self.config.strategy.value == "weekly":
                current_date += timedelta(weeks=1)
            elif self.config.strategy.value == "daily":
                current_date += timedelta(days=1)
        
        return partitions
    
    def get_partition_for_timestamp(self, timestamp_str: str) -> str:
        """Get the partition name for a given timestamp string."""
        try:
            # Parse timestamp string
            if isinstance(timestamp_str, str):
                # Handle various timestamp formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]:
                    try:
                        date = datetime.strptime(timestamp_str, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    # Fallback: try parsing as ISO format
                    date = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                date = timestamp_str
            
            return self.config.get_partition_for_date(date)
        except Exception as e:
            logger.warning(f"Error parsing timestamp {timestamp_str}: {e}")
            # Fallback to current date partition
            return self.config.get_partition_for_date(datetime.now())
    
    async def get_partition_statistics(self) -> Dict[str, Any]:
        """Get statistics about all partitions."""
        stats = {
            "total_partitions": 0,
            "partition_sizes": {},
            "partition_row_counts": {},
            "total_size_mb": 0,
            "total_rows": 0
        }
        
        existing_partitions = self.config.list_existing_partitions()
        stats["total_partitions"] = len(existing_partitions)
        
        for partition_name in existing_partitions:
            try:
                partition_path = self.config.get_partition_path(partition_name)
                
                # Get file size
                if os.path.exists(partition_path):
                    size_bytes = os.path.getsize(partition_path)
                    size_mb = size_bytes / (1024 * 1024)
                    stats["partition_sizes"][partition_name] = size_mb
                    stats["total_size_mb"] += size_mb
                
                # Get row count (optional - can be expensive for large partitions)
                # Uncomment if you need row counts
                # connection = await self.get_partition_connection(partition_name)
                # result = connection.execute("SELECT COUNT(*) FROM well_production").fetchone()
                # row_count = result[0] if result else 0
                # stats["partition_row_counts"][partition_name] = row_count
                # stats["total_rows"] += row_count
                
            except Exception as e:
                logger.warning(f"Error getting statistics for partition {partition_name}: {e}")
        
        return stats
    
    async def close_all_connections(self):
        """Close all partition connections."""
        async with self._connection_lock:
            for partition_name, connection in self._partition_connections.items():
                try:
                    connection.close()
                    logger.info(f"Closed connection to partition: {partition_name}")
                except Exception as e:
                    logger.warning(f"Error closing connection to partition {partition_name}: {e}")
            
            self._partition_connections.clear()
            
            if self._main_connection:
                try:
                    self._main_connection.close()
                    self._main_connection = None
                    logger.info("Closed main database connection")
                except Exception as e:
                    logger.warning(f"Error closing main connection: {e}")
    
    @asynccontextmanager
    async def acquire_partition_connection(self, partition_name: str):
        """Context manager for acquiring a partition connection."""
        connection = await self.get_partition_connection(partition_name)
        try:
            yield connection
        finally:
            # Connection is managed by the pool, no need to close here
            pass
    
    @asynccontextmanager 
    async def acquire_main_connection(self):
        """Context manager for acquiring the main database connection."""
        if self._main_connection is None:
            await self.initialize()
        
        try:
            yield self._main_connection
        finally:
            # Connection is managed by the manager, no need to close here
            pass
