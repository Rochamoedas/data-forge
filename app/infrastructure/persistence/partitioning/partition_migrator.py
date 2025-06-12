# app/infrastructure/persistence/partitioning/partition_migrator.py

import asyncio
import time
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from app.infrastructure.persistence.partitioning.partition_manager import PartitionManager
from app.infrastructure.persistence.partitioning.partition_config import PartitionConfig, DEFAULT_PARTITION_CONFIG
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.domain.entities.schema import Schema
from app.config.logging_config import logger


class PartitionMigrator:
    """
    Utility for migrating data from the main database to partitioned databases.
    
    This tool helps transition from a single large database to a partitioned approach
    without disrupting existing functionality.
    """
    
    def __init__(self, 
                 main_connection_pool: AsyncDuckDBPool,
                 partition_config: PartitionConfig = DEFAULT_PARTITION_CONFIG):
        self.main_connection_pool = main_connection_pool
        self.partition_manager = PartitionManager(partition_config)
        self.config = partition_config
        
    async def initialize(self):
        """Initialize the migrator."""
        await self.partition_manager.initialize()
        logger.info("Partition migrator initialized")
    
    async def migrate_table_to_partitions(self, 
                                        schema: Schema, 
                                        batch_size: int = 100000,
                                        dry_run: bool = False) -> Dict[str, Any]:
        """
        Migrate data from the main table to partitioned tables.
        
        Args:
            schema: The schema definition for the table
            batch_size: Number of records to process in each batch
            dry_run: If True, only analyze data without actual migration
            
        Returns:
            Migration statistics and results
        """
        start_time = time.perf_counter()
        
        stats = {
            "total_records": 0,
            "migrated_records": 0,
            "partitions_created": 0,
            "partition_distribution": {},
            "errors": [],
            "duration_seconds": 0,
            "throughput_records_per_second": 0,
            "dry_run": dry_run
        }
        
        try:
            logger.info(f"Starting {'DRY RUN' if dry_run else 'MIGRATION'} for table {schema.table_name}")
            
            # Step 1: Analyze existing data
            await self._analyze_existing_data(schema, stats)
            
            if stats["total_records"] == 0:
                logger.info(f"No data found in table {schema.table_name}")
                return stats
            
            # Step 2: Get data distribution by partition
            partition_distribution = await self._analyze_partition_distribution(schema)
            stats["partition_distribution"] = partition_distribution
            
            logger.info(f"Data distribution across partitions: {partition_distribution}")
            
            if dry_run:
                logger.info("DRY RUN completed - no data was migrated")
                return stats
            
            # Step 3: Create partitions and migrate data
            await self._perform_migration(schema, batch_size, stats)
            
            # Step 4: Verify migration
            await self._verify_migration(schema, stats)
            
            # Calculate final stats
            duration = time.perf_counter() - start_time
            stats["duration_seconds"] = duration
            stats["throughput_records_per_second"] = stats["migrated_records"] / duration if duration > 0 else 0
            
            logger.info(f"Migration completed successfully!")
            logger.info(f"Migrated {stats['migrated_records']} records to {stats['partitions_created']} partitions "
                       f"in {duration:.2f} seconds ({stats['throughput_records_per_second']:.0f} records/second)")
            
        except Exception as e:
            error_msg = f"Migration failed: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
            raise
        
        return stats
    
    async def _analyze_existing_data(self, schema: Schema, stats: Dict[str, Any]):
        """Analyze the existing data in the main database."""
        async with self.main_connection_pool.acquire() as conn:
            # Check if table exists
            table_exists_sql = f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{schema.table_name}'
            """
            result = conn.execute(table_exists_sql).fetchone()
            
            if result[0] == 0:
                logger.warning(f"Table {schema.table_name} does not exist in main database")
                return
            
            # Get total record count
            count_sql = f'SELECT COUNT(*) FROM "{schema.table_name}"'
            result = conn.execute(count_sql).fetchone()
            stats["total_records"] = result[0] if result else 0
            
            logger.info(f"Found {stats['total_records']} records in main table {schema.table_name}")
    
    async def _analyze_partition_distribution(self, schema: Schema) -> Dict[str, int]:
        """Analyze how data would be distributed across partitions."""
        distribution = {}
        
        async with self.main_connection_pool.acquire() as conn:
            # Query to get partition distribution
            if self.config.partition_column:
                # Group data by partition based on the partition column
                partition_sql = f"""
                    SELECT 
                        EXTRACT(YEAR FROM "{self.config.partition_column}") as year,
                        EXTRACT(MONTH FROM "{self.config.partition_column}") as month,
                        COUNT(*) as record_count
                    FROM "{schema.table_name}"
                    WHERE "{self.config.partition_column}" IS NOT NULL
                    GROUP BY year, month
                    ORDER BY year, month
                """
                
                try:
                    result = conn.execute(partition_sql).fetchall()
                    
                    for row in result:
                        year, month, count = row
                        # Create a sample date to get partition name
                        sample_date = datetime(int(year), int(month), 1)
                        partition_name = self.config.get_partition_name(sample_date)
                        distribution[partition_name] = int(count)
                        
                except Exception as e:
                    logger.warning(f"Could not analyze partition distribution: {e}")
                    # Fallback: assume all data goes to current partition
                    partition_name = self.config.get_partition_name(datetime.now())
                    async with self.main_connection_pool.acquire() as conn:
                        count_result = conn.execute(f'SELECT COUNT(*) FROM "{schema.table_name}"').fetchone()
                        distribution[partition_name] = count_result[0] if count_result else 0
        
        return distribution
    
    async def _perform_migration(self, schema: Schema, batch_size: int, stats: Dict[str, Any]):
        """Perform the actual data migration."""
        partition_counts = {}
        
        async with self.main_connection_pool.acquire() as conn:
            # Get all data ordered by partition column for efficient processing
            if self.config.partition_column:
                select_sql = f'SELECT * FROM "{schema.table_name}" ORDER BY "{self.config.partition_column}"'
            else:
                select_sql = f'SELECT * FROM "{schema.table_name}" ORDER BY created_at'
            
            # Process data in batches
            offset = 0
            while True:
                batch_sql = f"{select_sql} LIMIT {batch_size} OFFSET {offset}"
                result = conn.execute(batch_sql)
                rows = result.fetchall()
                
                if not rows:
                    break
                
                # Process this batch
                await self._migrate_batch(schema, rows, result.description, partition_counts)
                
                offset += len(rows)
                stats["migrated_records"] += len(rows)
                
                if offset % (batch_size * 10) == 0:  # Log progress every 10 batches
                    logger.info(f"Migrated {offset} records so far...")
        
        stats["partitions_created"] = len(partition_counts)
        logger.info(f"Created partitions: {list(partition_counts.keys())}")
    
    async def _migrate_batch(self, schema: Schema, rows: List, description, partition_counts: Dict[str, int]):
        """Migrate a batch of rows to appropriate partitions."""
        # Group rows by partition
        partition_groups = {}
        
        column_names = [desc[0] for desc in description]
        
        for row in rows:
            row_dict = dict(zip(column_names, row))
            
            # Determine target partition
            partition_name = self._get_partition_for_row(row_dict)
            
            if partition_name not in partition_groups:
                partition_groups[partition_name] = []
            
            partition_groups[partition_name].append(row_dict)
        
        # Insert into each partition
        for partition_name, partition_rows in partition_groups.items():
            await self._insert_into_partition(schema, partition_name, partition_rows)
            
            # Update statistics
            if partition_name not in partition_counts:
                partition_counts[partition_name] = 0
            partition_counts[partition_name] += len(partition_rows)
    
    def _get_partition_for_row(self, row_dict: Dict[str, Any]) -> str:
        """Determine which partition a row should go to."""
        if self.config.partition_column and self.config.partition_column in row_dict:
            timestamp_value = row_dict[self.config.partition_column]
            if timestamp_value:
                return self.partition_manager.get_partition_for_timestamp(str(timestamp_value))
        
        # Fallback to created_at or current time
        if 'created_at' in row_dict and row_dict['created_at']:
            return self.partition_manager.get_partition_for_timestamp(str(row_dict['created_at']))
        
        return self.config.get_partition_name(datetime.now())
    
    async def _insert_into_partition(self, schema: Schema, partition_name: str, rows: List[Dict[str, Any]]):
        """Insert rows into a specific partition."""
        # Ensure partition exists
        await self.partition_manager.ensure_partition_exists(partition_name, schema)
          # Insert data
        async with self.partition_manager.acquire_partition_connection(partition_name) as conn:
            # Build insert SQL
            columns = list(rows[0].keys())
            placeholders = ", ".join(["?" for _ in columns])
            quoted_columns = ", ".join([f'"{col}"' for col in columns])
            insert_sql = f'INSERT OR IGNORE INTO "{schema.table_name}" ({quoted_columns}) VALUES ({placeholders})'
            
            # Prepare data for batch insert
            values_list = []
            for row in rows:
                values = [row.get(col) for col in columns]
                values_list.append(values)
            
            # Batch insert
            conn.executemany(insert_sql, values_list)
    
    async def _verify_migration(self, schema: Schema, stats: Dict[str, Any]):
        """Verify that migration was successful."""
        logger.info("Verifying migration...")
        
        # Count records in all partitions
        total_partition_records = 0
        
        existing_partitions = self.config.list_existing_partitions()
        for partition_name in existing_partitions:
            try:
                async with self.partition_manager.acquire_partition_connection(partition_name) as conn:
                    count_sql = f'SELECT COUNT(*) FROM "{schema.table_name}"'
                    result = conn.execute(count_sql).fetchone()
                    partition_count = result[0] if result else 0
                    total_partition_records += partition_count
                    logger.info(f"Partition {partition_name}: {partition_count} records")
            except Exception as e:
                error_msg = f"Error verifying partition {partition_name}: {e}"
                logger.warning(error_msg)
                stats["errors"].append(error_msg)
        
        logger.info(f"Verification: {total_partition_records} total records in partitions vs {stats['total_records']} in main table")
        
        if total_partition_records != stats["total_records"]:
            error_msg = f"Migration verification failed: partition records ({total_partition_records}) != main table records ({stats['total_records']})"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
        else:
            logger.info("✅ Migration verification successful!")
    
    async def create_partition_summary_report(self) -> Dict[str, Any]:
        """Create a summary report of all partitions."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "partition_strategy": self.config.strategy.value,
            "partition_column": self.config.partition_column,
            "partitions": {}
        }
        
        existing_partitions = self.config.list_existing_partitions()
        
        for partition_name in existing_partitions:
            try:
                partition_info = {
                    "name": partition_name,
                    "path": self.config.get_partition_path(partition_name),
                    "date_range": self.config.get_date_range_for_partition(partition_name),
                    "file_size_mb": 0,
                    "record_count": 0,
                    "status": "active"
                }
                
                # Get file size
                partition_path = self.config.get_partition_path(partition_name)
                if os.path.exists(partition_path):
                    size_bytes = os.path.getsize(partition_path)
                    partition_info["file_size_mb"] = size_bytes / (1024 * 1024)
                
                report["partitions"][partition_name] = partition_info
                
            except Exception as e:
                logger.warning(f"Error getting info for partition {partition_name}: {e}")
                report["partitions"][partition_name] = {
                    "name": partition_name,
                    "status": "error",
                    "error": str(e)
                }
        
        return report
    
    async def close(self):
        """Close all connections."""
        await self.partition_manager.close_all_connections()
        logger.info("Partition migrator closed")
