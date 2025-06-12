# manage_partitions.py

"""
Partition Management Script for ReactFastAPI

This script provides command-line tools for managing the partitioned database system.
It demonstrates how to use the partitioning modules without modifying existing code.

Usage examples:
    python manage_partitions.py analyze
    python manage_partitions.py migrate --dry-run
    python manage_partitions.py migrate --schema well_production
    python manage_partitions.py health-report
    python manage_partitions.py cleanup --retention-days 365 --dry-run
    python manage_partitions.py stats
"""

import asyncio
import argparse
import json
from datetime import datetime
from typing import Dict, Any

# Import existing modules
from app.infrastructure.metadata.schemas_description import SCHEMAS_METADATA
from app.domain.entities.schema import Schema
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.config.settings import settings

# Import new partitioning modules
from app.infrastructure.persistence.partitioning.partition_config import (
    PartitionConfig, PartitionStrategy, DEFAULT_PARTITION_CONFIG
)
from app.infrastructure.persistence.partitioning.partition_manager import PartitionManager
from app.infrastructure.persistence.partitioning.partitioned_data_repository import PartitionedDataRepository
from app.infrastructure.persistence.partitioning.partition_migrator import PartitionMigrator
from app.infrastructure.persistence.partitioning.partition_utilities import PartitionUtilities

from app.config.logging_config import logger


def get_schema_by_name(schema_name: str) -> Schema:
    """Get a schema object by name from SCHEMAS_METADATA."""
    for schema_dict in SCHEMAS_METADATA:
        if schema_dict["name"] == schema_name:
            return Schema(
                name=schema_dict["name"],
                description=schema_dict["description"],
                table_name=schema_dict["table_name"],
                primary_key=schema_dict.get("primary_key", []),
                properties=[
                    Schema.Property(
                        name=prop["name"],
                        type=prop["type"],
                        db_type=prop["db_type"],
                        required=prop.get("required", False),
                        primary_key=prop.get("primary_key", False)
                    )
                    for prop in schema_dict["properties"]
                ]
            )
    raise ValueError(f"Schema '{schema_name}' not found")


async def cmd_analyze_partitions(args):
    """Analyze partition distribution without migrating data."""
    print(f"🔍 Analyzing partition distribution for schema: {args.schema}")
    
    # Create partition config
    config = PartitionConfig(
        strategy=PartitionStrategy(args.strategy),
        partition_column=args.partition_column
    )
    
    # Initialize components
    main_pool = AsyncDuckDBPool()
    await main_pool.initialize()
    
    migrator = PartitionMigrator(main_pool, config)
    await migrator.initialize()
    
    try:
        schema = get_schema_by_name(args.schema)
        
        # Perform dry run migration to get analysis
        results = await migrator.migrate_table_to_partitions(
            schema=schema,
            batch_size=args.batch_size,
            dry_run=True
        )
        
        print("\n📊 PARTITION ANALYSIS RESULTS")
        print("=" * 50)
        print(f"Total records in main database: {results['total_records']:,}")
        print(f"Estimated partitions needed: {len(results['partition_distribution'])}")
        print("\nPartition distribution:")
        
        for partition_name, count in results['partition_distribution'].items():
            print(f"  📁 {partition_name}: {count:,} records")
        
        if results['errors']:
            print("\n⚠️  Errors encountered:")
            for error in results['errors']:
                print(f"  ❌ {error}")
        
        print(f"\nPartition strategy: {config.strategy.value}")
        print(f"Partition column: {config.partition_column}")
        print(f"Partition directory: {config.partition_directory}")
        
    finally:
        await migrator.close()
        await main_pool.close()


async def cmd_migrate_data(args):
    """Migrate data from main database to partitions."""
    if args.dry_run:
        print(f"🧪 DRY RUN: Analyzing migration for schema: {args.schema}")
    else:
        print(f"🚀 MIGRATING data for schema: {args.schema}")
        print("⚠️  This will create partition databases and copy data!")
    
    # Create partition config
    config = PartitionConfig(
        strategy=PartitionStrategy(args.strategy),
        partition_column=args.partition_column
    )
    
    # Initialize components
    main_pool = AsyncDuckDBPool()
    await main_pool.initialize()
    
    migrator = PartitionMigrator(main_pool, config)
    await migrator.initialize()
    
    try:
        schema = get_schema_by_name(args.schema)
        
        # Perform migration
        results = await migrator.migrate_table_to_partitions(
            schema=schema,
            batch_size=args.batch_size,
            dry_run=args.dry_run
        )
        
        print("\n📊 MIGRATION RESULTS")
        print("=" * 50)
        print(f"Total records: {results['total_records']:,}")
        print(f"Migrated records: {results['migrated_records']:,}")
        print(f"Partitions created: {results['partitions_created']}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")
        print(f"Throughput: {results['throughput_records_per_second']:,.0f} records/second")
        
        if results['partition_distribution']:
            print("\nPartition distribution:")
            for partition_name, count in results['partition_distribution'].items():
                print(f"  📁 {partition_name}: {count:,} records")
        
        if results['errors']:
            print("\n⚠️  Errors encountered:")
            for error in results['errors']:
                print(f"  ❌ {error}")
        
        if args.dry_run:
            print("\n✅ DRY RUN completed - no data was actually migrated")
        else:
            print("\n🎉 Migration completed successfully!")
            
    finally:
        await migrator.close()
        await main_pool.close()


async def cmd_health_report(args):
    """Generate a comprehensive health report for all partitions."""
    print("🏥 Generating partition health report...")
    
    config = PartitionConfig(
        strategy=PartitionStrategy(args.strategy),
        partition_column=args.partition_column
    )
    
    utilities = PartitionUtilities(config)
    await utilities.initialize()
    
    try:
        schema = get_schema_by_name(args.schema)
        report = await utilities.create_partition_health_report(schema)
        
        print("\n🏥 PARTITION HEALTH REPORT")
        print("=" * 50)
        print(f"Overall Health: {report['overall_health'].upper()}")
        print(f"Total Partitions: {report['partition_count']}")
        print(f"Total Size: {report['total_size_gb']:.2f} GB")
        
        print("\nHealth Checks:")
        for check, passed in report['health_checks'].items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {status} {check.replace('_', ' ').title()}")
        
        if report['recommendations']:
            print("\n💡 Recommendations:")
            for rec in report['recommendations']:
                print(f"  💡 {rec}")
        
        if report['errors']:
            print("\n⚠️  Errors:")
            for error in report['errors']:
                print(f"  ❌ {error}")
        
        # Save detailed report if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"\n📄 Detailed report saved to: {args.output}")
            
    finally:
        await utilities.close()


async def cmd_partition_stats(args):
    """Show partition statistics and performance metrics."""
    print("📈 Gathering partition statistics...")
    
    config = PartitionConfig(
        strategy=PartitionStrategy(args.strategy),
        partition_column=args.partition_column
    )
    
    manager = PartitionManager(config)
    await manager.initialize()
    
    try:
        stats = await manager.get_partition_statistics()
        
        print("\n📈 PARTITION STATISTICS")
        print("=" * 50)
        print(f"Total Partitions: {stats['total_partitions']}")
        print(f"Total Size: {stats['total_size_mb']:.2f} MB ({stats['total_size_mb']/1024:.2f} GB)")
        print(f"Total Rows: {stats['total_rows']:,}" if 'total_rows' in stats else "Total Rows: Not calculated")
        
        if stats['partition_sizes']:
            print("\nPartition sizes:")
            sorted_partitions = sorted(stats['partition_sizes'].items(), 
                                     key=lambda x: x[1], reverse=True)
            
            for partition_name, size_mb in sorted_partitions:
                print(f"  📁 {partition_name}: {size_mb:.2f} MB")
        
    finally:
        await manager.close_all_connections()


async def cmd_cleanup_partitions(args):
    """Clean up old partitions based on retention policy."""
    if args.dry_run:
        print(f"🧪 DRY RUN: Analyzing partitions for cleanup (retention: {args.retention_days} days)")
    else:
        print(f"🧹 CLEANING UP partitions older than {args.retention_days} days")
        print("⚠️  This will permanently delete partition files!")
    
    config = PartitionConfig(
        strategy=PartitionStrategy(args.strategy),
        partition_column=args.partition_column
    )
    
    utilities = PartitionUtilities(config)
    await utilities.initialize()
    
    try:
        results = await utilities.cleanup_old_partitions(
            retention_days=args.retention_days,
            dry_run=args.dry_run
        )
        
        print("\n🧹 CLEANUP RESULTS")
        print("=" * 50)
        print(f"Partitions analyzed: {results['partitions_analyzed']}")
        print(f"Partitions to delete: {len(results['partitions_to_delete'])}")
        print(f"Space to be freed: {results['total_space_freed_mb']:.2f} MB")
        
        if results['partitions_to_delete']:
            print("\nPartitions to delete:")
            for partition in results['partitions_to_delete']:
                print(f"  📁 {partition['name']} ({partition['size_mb']:.2f} MB) - ends {partition['end_date']}")
        
        if not args.dry_run and results['partitions_deleted']:
            print(f"\n✅ Successfully deleted {len(results['partitions_deleted'])} partitions")
        
        if results['errors']:
            print("\n⚠️  Errors:")
            for error in results['errors']:
                print(f"  ❌ {error}")
        
    finally:
        await utilities.close()


async def cmd_test_partitioned_repo(args):
    """Test the partitioned repository functionality."""
    print("🧪 Testing partitioned repository...")
    
    config = PartitionConfig(
        strategy=PartitionStrategy(args.strategy),
        partition_column=args.partition_column
    )
    
    # Initialize partitioned repository
    repo = PartitionedDataRepository(config)
    await repo.initialize()
    
    try:
        schema = get_schema_by_name(args.schema)
        
        print(f"✅ Partitioned repository initialized")
        print(f"📊 Getting partition statistics...")
        
        stats = await repo.get_partition_statistics()
        print(f"   Total partitions: {stats['total_partitions']}")
        print(f"   Total size: {stats['total_size_mb']:.2f} MB")
        
        # You could add more tests here, like creating test records
        print("✅ Partitioned repository test completed")
        
    finally:
        await repo.close()


def main():
    parser = argparse.ArgumentParser(description="Partition Management Tool")
    
    # Global arguments
    parser.add_argument("--schema", default="well_production", 
                       help="Schema name to work with (default: well_production)")
    parser.add_argument("--strategy", default="monthly", 
                       choices=["yearly", "monthly", "weekly", "daily"],
                       help="Partitioning strategy (default: monthly)")
    parser.add_argument("--partition-column", default="production_period",
                       help="Column to partition by (default: production_period)")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze partition distribution")
    analyze_parser.add_argument("--batch-size", type=int, default=100000,
                               help="Batch size for analysis (default: 100000)")
    
    # Migrate command
    migrate_parser = subparsers.add_parser("migrate", help="Migrate data to partitions")
    migrate_parser.add_argument("--dry-run", action="store_true",
                               help="Perform a dry run without actual migration")
    migrate_parser.add_argument("--batch-size", type=int, default=100000,
                               help="Batch size for migration (default: 100000)")
    
    # Health report command
    health_parser = subparsers.add_parser("health-report", help="Generate partition health report")
    health_parser.add_argument("--output", 
                              help="Save detailed report to JSON file")
    
    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show partition statistics")
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Clean up old partitions")
    cleanup_parser.add_argument("--retention-days", type=int, default=365,
                               help="Retention period in days (default: 365)")
    cleanup_parser.add_argument("--dry-run", action="store_true",
                               help="Perform a dry run without actual deletion")
    
    # Test command
    test_parser = subparsers.add_parser("test", help="Test partitioned repository")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Set up logging
    logger.info(f"Starting partition management command: {args.command}")
    
    # Execute command
    if args.command == "analyze":
        asyncio.run(cmd_analyze_partitions(args))
    elif args.command == "migrate":
        asyncio.run(cmd_migrate_data(args))
    elif args.command == "health-report":
        asyncio.run(cmd_health_report(args))
    elif args.command == "stats":
        asyncio.run(cmd_partition_stats(args))
    elif args.command == "cleanup":
        asyncio.run(cmd_cleanup_partitions(args))
    elif args.command == "test":
        asyncio.run(cmd_test_partitioned_repo(args))


if __name__ == "__main__":
    main()
