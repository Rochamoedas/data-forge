# app/infrastructure/persistence/partitioning/partition_config.py

from typing import Dict, Any, List, Optional
from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timedelta
import os

class PartitionStrategy(Enum):
    """Partition strategies for handling massive datasets."""
    YEARLY = "yearly"
    MONTHLY = "monthly" 
    WEEKLY = "weekly"
    DAILY = "daily"

class PartitionType(Enum):
    """Types of partitioning supported."""
    TIME_BASED = "time_based"
    HASH_BASED = "hash_based"
    RANGE_BASED = "range_based"

@dataclass
class PartitionConfig:
    """Configuration for database partitioning."""
    
    # Partition strategy
    strategy: PartitionStrategy = PartitionStrategy.MONTHLY
    partition_type: PartitionType = PartitionType.TIME_BASED
    
    # Time-based partitioning settings
    partition_column: str = "production_period"  # The timestamp column to partition by
    
    # Storage settings
    base_partition_path: str = "./data/partitions"
    main_database_path: str = "./data/data.duckdb"
    
    # Performance settings
    max_partitions_in_memory: int = 12  # Max number of partition databases to keep open
    partition_retention_days: Optional[int] = None  # None = keep all partitions
    
    # Auto-partitioning settings
    auto_partition_enabled: bool = True
    partition_size_threshold_mb: int = 1000  # Auto-create new partition when current exceeds this
    
    # Query optimization settings
    enable_cross_partition_queries: bool = True
    query_cache_enabled: bool = True
    
    @property
    def partition_directory(self) -> str:
        """Get the full partition directory path."""
        os.makedirs(self.base_partition_path, exist_ok=True)
        return self.base_partition_path
    
    def get_partition_name(self, date: datetime) -> str:
        """Generate partition name based on date and strategy."""
        if self.strategy == PartitionStrategy.YEARLY:
            return f"partition_{date.year}"
        elif self.strategy == PartitionStrategy.MONTHLY:
            return f"partition_{date.year}_{date.month:02d}"
        elif self.strategy == PartitionStrategy.WEEKLY:
            year, week, _ = date.isocalendar()
            return f"partition_{year}_w{week:02d}"
        elif self.strategy == PartitionStrategy.DAILY:
            return f"partition_{date.year}_{date.month:02d}_{date.day:02d}"
        else:
            raise ValueError(f"Unsupported partition strategy: {self.strategy}")
    
    def get_partition_path(self, partition_name: str) -> str:
        """Get the full path to a partition database file."""
        return os.path.join(self.partition_directory, f"{partition_name}.duckdb")
    
    def get_date_range_for_partition(self, partition_name: str) -> tuple[datetime, datetime]:
        """Get the date range covered by a partition."""
        # Parse partition name to extract date components
        if not partition_name.startswith("partition_"):
            raise ValueError(f"Invalid partition name format: {partition_name}")
        
        parts = partition_name.replace("partition_", "").split("_")
        
        if self.strategy == PartitionStrategy.YEARLY:
            year = int(parts[0])
            start_date = datetime(year, 1, 1)
            end_date = datetime(year + 1, 1, 1) - timedelta(microseconds=1)
        elif self.strategy == PartitionStrategy.MONTHLY:
            year, month = int(parts[0]), int(parts[1])
            start_date = datetime(year, month, 1)
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(microseconds=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(microseconds=1)
        elif self.strategy == PartitionStrategy.WEEKLY:
            year = int(parts[0])
            week = int(parts[1].replace("w", ""))
            start_date = datetime.strptime(f'{year} {week} 1', '%Y %W %w')
            end_date = start_date + timedelta(days=7) - timedelta(microseconds=1)
        elif self.strategy == PartitionStrategy.DAILY:
            year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
            start_date = datetime(year, month, day)
            end_date = start_date + timedelta(days=1) - timedelta(microseconds=1)
        else:
            raise ValueError(f"Unsupported partition strategy: {self.strategy}")
        
        return start_date, end_date
    
    def get_partition_for_date(self, date: datetime) -> str:
        """Get the partition name that should contain data for the given date."""
        return self.get_partition_name(date)
    
    def list_existing_partitions(self) -> List[str]:
        """List all existing partition files."""
        if not os.path.exists(self.partition_directory):
            return []
        
        partitions = []
        for file in os.listdir(self.partition_directory):
            if file.endswith('.duckdb') and file.startswith('partition_'):
                partition_name = file.replace('.duckdb', '')
                partitions.append(partition_name)
        
        return sorted(partitions)

# Default configuration instance
DEFAULT_PARTITION_CONFIG = PartitionConfig()
