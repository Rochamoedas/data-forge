# example_partitioned_usage.py

"""
Example script demonstrating how to use the partitioned database system
alongside your existing ReactFastAPI application.

This shows how to:
1. Initialize the partitioned repository
2. Migrate existing data to partitions
3. Use the partitioned repository for high-performance operations
4. Switch between normal and partitioned repositories
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import List

# Existing imports
from app.infrastructure.metadata.schemas_description import SCHEMAS_METADATA
from app.domain.entities.schema import Schema
from app.domain.entities.data_record import DataRecord
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool

# New partitioning imports
from app.infrastructure.persistence.partitioning.partitioned_data_repository import PartitionedDataRepository
from app.infrastructure.persistence.partitioning.partition_migrator import PartitionMigrator
from app.config.partition_settings import get_production_partition_config

from app.config.logging_config import logger


def get_well_production_schema() -> Schema:
    """Get the well_production schema object."""
    for schema_dict in SCHEMAS_METADATA:
        if schema_dict["name"] == "well_production":
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
    raise ValueError("well_production schema not found")


def create_sample_well_production_data(count: int = 1000) -> List[DataRecord]:
    """Create sample well production data for testing."""
    records = []
    base_date = datetime(2023, 1, 1)
    
    for i in range(count):
        # Create data that spans multiple partitions
        date_offset = timedelta(days=i * 30)  # One record per month
        production_date = base_date + date_offset
        
        data = {
            "field_code": i % 10,  # 10 different fields
            "field_name": f"Field_{i % 10}",
            "well_code": i,
            "well_reference": f"WELL_{i:06d}",
            "well_name": f"Well {i}",
            "production_period": production_date.isoformat(),
            "days_on_production": 30,
            "oil_production_kbd": round(100 + (i % 50), 2),
            "gas_production_mmcfd": round(50 + (i % 25), 2),
            "liquids_production_kbd": round(120 + (i % 60), 2),
            "water_production_kbd": round(10 + (i % 20), 2),
            "data_source": "TEST_DATA",
            "source_data": "EXAMPLE",
            "partition_0": f"partition_{i % 5}",
        }
        
        records.append(DataRecord.create(data))
    
    return records


async def example_1_basic_partitioned_operations():
    """Example 1: Basic partitioned repository operations."""
    print("\n🚀 EXAMPLE 1: Basic Partitioned Operations")
    print("=" * 60)
    
    # Get configuration and schema
    config = get_production_partition_config()
    schema = get_well_production_schema()
    
    # Initialize partitioned repository
    repo = PartitionedDataRepository(config)
    await repo.initialize()
    
    try:
        # Create sample data
        print("📝 Creating sample well production data...")
        sample_records = create_sample_well_production_data(100)  # 100 records across different months
        
        # Batch insert into partitioned database
        print("💾 Inserting data into partitioned database...")
        start_time = time.perf_counter()
        
        await repo.create_batch(schema, sample_records)
        
        duration = time.perf_counter() - start_time
        throughput = len(sample_records) / duration
        
        print(f"✅ Inserted {len(sample_records)} records in {duration:.2f}s ({throughput:.0f} records/sec)")
        
        # Get partition statistics
        print("\n📊 Partition statistics:")
        stats = await repo.get_partition_statistics()
        print(f"   Total partitions: {stats['total_partitions']}")
        print(f"   Total size: {stats['total_size_mb']:.2f} MB")
        
        # Test queries
        print("\n🔍 Testing partitioned queries...")
        from app.application.dto.query_dto import DataQueryRequest, QueryFilter, FilterOperator, QueryPagination
        
        # Query with date filter (should use partition pruning)
        query_request = DataQueryRequest(
            filters=[
                QueryFilter(
                    field="production_period",
                    operator=FilterOperator.GTE,
                    value="2023-06-01"
                )
            ],
            pagination=QueryPagination(page=1, size=10)
        )
        
        start_time = time.perf_counter()
        results = await repo.get_all(schema, query_request)
        query_duration = time.perf_counter() - start_time
        
        print(f"✅ Query returned {len(results.items)} records in {query_duration*1000:.2f}ms")
        print(f"   Total matching records: {results.total}")
        
    finally:
        await repo.close()


async def example_2_migration_from_main_database():
    """Example 2: Migrate existing data from main database to partitions."""
    print("\n🔄 EXAMPLE 2: Migration from Main Database")
    print("=" * 60)
    
    config = get_production_partition_config()
    schema = get_well_production_schema()
    
    # Initialize main database connection
    main_pool = AsyncDuckDBPool()
    await main_pool.initialize()
    
    # Initialize migrator
    migrator = PartitionMigrator(main_pool, config)
    await migrator.initialize()
    
    try:
        print("🔍 Analyzing existing data for partition migration...")
        
        # Perform dry run analysis
        analysis_results = await migrator.migrate_table_to_partitions(
            schema=schema,
            batch_size=50000,
            dry_run=True  # Just analyze, don't migrate
        )
        
        print(f"📊 Analysis results:")
        print(f"   Total records in main database: {analysis_results['total_records']:,}")
        print(f"   Estimated partitions needed: {len(analysis_results['partition_distribution'])}")
        
        if analysis_results['partition_distribution']:
            print("   Partition distribution:")
            for partition_name, count in analysis_results['partition_distribution'].items():
                print(f"     📁 {partition_name}: {count:,} records")
        
        # If you want to actually perform the migration, uncomment this:
        # print("\n🚀 Performing actual migration...")
        # migration_results = await migrator.migrate_table_to_partitions(
        #     schema=schema,
        #     batch_size=50000,
        #     dry_run=False  # Actually migrate the data
        # )
        # print(f"✅ Migration completed: {migration_results['migrated_records']:,} records migrated")
        
    finally:
        await migrator.close()
        await main_pool.close()


async def example_3_performance_comparison():
    """Example 3: Performance comparison between normal and partitioned repositories."""
    print("\n⚡ EXAMPLE 3: Performance Comparison")
    print("=" * 60)
    
    # This example shows how you could compare performance
    # In a real scenario, you'd have data in both systems
    
    config = get_production_partition_config()
    schema = get_well_production_schema()
    
    # Initialize partitioned repository
    partitioned_repo = PartitionedDataRepository(config)
    await partitioned_repo.initialize()
    
    try:
        # Create test data for performance testing
        print("📝 Creating performance test data...")
        test_records = create_sample_well_production_data(1000)
        
        # Test partitioned repository performance
        print("\n⚡ Testing partitioned repository performance...")
        start_time = time.perf_counter()
        
        await partitioned_repo.create_batch(schema, test_records)
        
        partitioned_duration = time.perf_counter() - start_time
        partitioned_throughput = len(test_records) / partitioned_duration
        
        print(f"✅ Partitioned repository: {len(test_records)} records in {partitioned_duration:.2f}s")
        print(f"   Throughput: {partitioned_throughput:.0f} records/second")
        
        # Test query performance with date range (partition pruning advantage)
        from app.application.dto.query_dto import DataQueryRequest, QueryFilter, FilterOperator
        
        query_request = DataQueryRequest(
            filters=[
                QueryFilter(
                    field="production_period",
                    operator=FilterOperator.GTE,
                    value="2023-12-01"
                ),
                QueryFilter(
                    field="production_period",
                    operator=FilterOperator.LT,
                    value="2024-01-01"
                )
            ]
        )
        
        start_time = time.perf_counter()
        results = await partitioned_repo.get_all(schema, query_request)
        query_duration = time.perf_counter() - start_time
        
        print(f"✅ Partitioned query: {len(results.items)} records in {query_duration*1000:.2f}ms")
        print(f"   (Query targeted specific partition - should be very fast)")
        
    finally:
        await partitioned_repo.close()


async def example_4_partition_management():
    """Example 4: Partition management and utilities."""
    print("\n🔧 EXAMPLE 4: Partition Management")
    print("=" * 60)
    
    config = get_production_partition_config()
    schema = get_well_production_schema()
    
    # Import partition utilities
    from app.infrastructure.persistence.partitioning.partition_utilities import PartitionUtilities
    
    utilities = PartitionUtilities(config)
    await utilities.initialize()
    
    try:
        print("🏥 Generating partition health report...")
        health_report = await utilities.create_partition_health_report(schema)
        
        print(f"📊 Health Report Summary:")
        print(f"   Overall health: {health_report['overall_health']}")
        print(f"   Total partitions: {health_report['partition_count']}")
        print(f"   Total size: {health_report['total_size_gb']:.2f} GB")
        
        print("\n🔍 Health checks:")
        for check, passed in health_report['health_checks'].items():
            status = "✅" if passed else "❌"
            print(f"   {status} {check.replace('_', ' ').title()}")
        
        if health_report['recommendations']:
            print("\n💡 Recommendations:")
            for rec in health_report['recommendations']:
                print(f"   💡 {rec}")
        
        # Performance analysis
        print("\n📈 Analyzing partition performance...")
        perf_analysis = await utilities.analyze_partition_performance(schema)
        
        print(f"📊 Performance Analysis:")
        print(f"   Total partitions analyzed: {perf_analysis['total_partitions']}")
        print(f"   Total size: {perf_analysis['summary']['total_size_mb']:.2f} MB")
        
        if perf_analysis['summary']['largest_partition']:
            print(f"   Largest partition: {perf_analysis['summary']['largest_partition']}")
            print(f"   Smallest partition: {perf_analysis['summary']['smallest_partition']}")
        
        if perf_analysis['summary']['performance_recommendations']:
            print("\n🎯 Performance recommendations:")
            for rec in perf_analysis['summary']['performance_recommendations']:
                print(f"   🎯 {rec}")
        
    finally:
        await utilities.close()


async def main():
    """Run all examples."""
    print("🎯 PARTITIONED DATABASE EXAMPLES")
    print("This demonstrates the new partitioning system for handling billions of records")
    print("=" * 80)
    
    try:
        # Run examples
        await example_1_basic_partitioned_operations()
        await example_2_migration_from_main_database()
        await example_3_performance_comparison()
        await example_4_partition_management()
        
        print("\n🎉 ALL EXAMPLES COMPLETED SUCCESSFULLY!")
        print("\nNext steps:")
        print("1. Use 'python manage_partitions.py analyze' to analyze your real data")
        print("2. Use 'python manage_partitions.py migrate --dry-run' to plan migration")
        print("3. Use 'python manage_partitions.py migrate' to perform actual migration")
        print("4. Integrate PartitionedDataRepository into your application container")
        
    except Exception as e:
        logger.error(f"Error in examples: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
