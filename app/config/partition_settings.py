# app/config/partition_settings.py

"""
Partition configuration settings for the ReactFastAPI application.

This module provides configuration for the partitioned database system without
modifying the existing codebase structure.
"""

import os
from app.infrastructure.persistence.partitioning.partition_config import (
    PartitionConfig, PartitionStrategy, PartitionType
)


def get_production_partition_config() -> PartitionConfig:
    """
    Get the production partition configuration for handling billions of records.
    
    This configuration is optimized for the well_production schema with
    time-based partitioning on the production_period column.
    """
    return PartitionConfig(
        # Partitioning strategy - monthly partitions for production data
        strategy=PartitionStrategy.MONTHLY,
        partition_type=PartitionType.TIME_BASED,
        
        # The timestamp column to partition by (from your schema)
        partition_column="production_period",
        
        # Storage paths
        base_partition_path=os.path.join(".", "data", "partitions"),
        main_database_path=os.path.join(".", "data", "data.duckdb"),
        
        # Performance settings for billions of records
        max_partitions_in_memory=24,  # Keep 2 years of monthly partitions in memory
        partition_retention_days=None,  # Keep all partitions by default
        
        # Auto-partitioning settings
        auto_partition_enabled=True,
        partition_size_threshold_mb=2000,  # 2GB per partition before creating new one
        
        # Query optimization
        enable_cross_partition_queries=True,
        query_cache_enabled=True,
    )


def get_yearly_partition_config() -> PartitionConfig:
    """
    Alternative configuration for yearly partitioning.
    Use this for even larger datasets or when monthly partitions become too small.
    """
    return PartitionConfig(
        strategy=PartitionStrategy.YEARLY,
        partition_type=PartitionType.TIME_BASED,
        partition_column="production_period",
        base_partition_path=os.path.join(".", "data", "partitions_yearly"),
        main_database_path=os.path.join(".", "data", "data.duckdb"),
        max_partitions_in_memory=10,  # Keep 10 years in memory
        partition_size_threshold_mb=10000,  # 10GB per partition
    )


def get_daily_partition_config() -> PartitionConfig:
    """
    Configuration for daily partitioning.
    Use this for extremely high-volume data ingestion scenarios.
    """
    return PartitionConfig(
        strategy=PartitionStrategy.DAILY,
        partition_type=PartitionType.TIME_BASED,
        partition_column="production_period",
        base_partition_path=os.path.join(".", "data", "partitions_daily"),
        main_database_path=os.path.join(".", "data", "data.duckdb"),
        max_partitions_in_memory=60,  # Keep 2 months of daily partitions
        partition_size_threshold_mb=500,  # 500MB per partition
    )


# Default configuration to use
DEFAULT_PARTITION_CONFIG = get_production_partition_config()


def get_partition_config_by_strategy(strategy: str) -> PartitionConfig:
    """Get partition configuration by strategy name."""
    configs = {
        "monthly": get_production_partition_config(),
        "yearly": get_yearly_partition_config(),
        "daily": get_daily_partition_config(),
    }
    return configs.get(strategy, DEFAULT_PARTITION_CONFIG)
