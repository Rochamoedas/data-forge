# app/infrastructure/persistence/partitioning/partition_utilities.py

import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from app.infrastructure.persistence.partitioning.partition_config import PartitionConfig, DEFAULT_PARTITION_CONFIG
from app.infrastructure.persistence.partitioning.partition_manager import PartitionManager
from app.infrastructure.persistence.partitioning.partition_migrator import PartitionMigrator
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.domain.entities.schema import Schema
from app.config.logging_config import logger


class PartitionUtilities:
    """
    Utility functions for managing partitioned databases.
    
    Provides tools for:
    - Partition maintenance and cleanup
    - Performance monitoring
    - Data analysis across partitions
    - Backup and recovery operations
    """
    
    def __init__(self, partition_config: PartitionConfig = DEFAULT_PARTITION_CONFIG):
        self.config = partition_config
        self.partition_manager = PartitionManager(partition_config)
    
    async def initialize(self):
        """Initialize the utilities."""
        await self.partition_manager.initialize()
    
    async def analyze_partition_performance(self, schema: Schema) -> Dict[str, Any]:
        """Analyze performance characteristics of all partitions."""
        analysis = {
            "timestamp": datetime.now().isoformat(),
            "total_partitions": 0,
            "partition_analysis": {},
            "summary": {
                "total_size_mb": 0,
                "average_size_mb": 0,
                "largest_partition": None,
                "smallest_partition": None,
                "performance_recommendations": []
            }
        }
        
        existing_partitions = self.config.list_existing_partitions()
        analysis["total_partitions"] = len(existing_partitions)
        
        partition_sizes = {}
        
        for partition_name in existing_partitions:
            try:
                partition_analysis = await self._analyze_single_partition(partition_name, schema)
                analysis["partition_analysis"][partition_name] = partition_analysis
                
                size_mb = partition_analysis.get("size_mb", 0)
                partition_sizes[partition_name] = size_mb
                analysis["summary"]["total_size_mb"] += size_mb
                
            except Exception as e:
                logger.warning(f"Error analyzing partition {partition_name}: {e}")
                analysis["partition_analysis"][partition_name] = {"error": str(e)}
        
        # Calculate summary statistics
        if partition_sizes:
            analysis["summary"]["average_size_mb"] = analysis["summary"]["total_size_mb"] / len(partition_sizes)
            analysis["summary"]["largest_partition"] = max(partition_sizes, key=partition_sizes.get)
            analysis["summary"]["smallest_partition"] = min(partition_sizes, key=partition_sizes.get)
            
            # Generate recommendations
            analysis["summary"]["performance_recommendations"] = self._generate_performance_recommendations(partition_sizes)
        
        return analysis
    
    async def _analyze_single_partition(self, partition_name: str, schema: Schema) -> Dict[str, Any]:
        """Analyze a single partition."""
        partition_path = self.config.get_partition_path(partition_name)
        
        analysis = {
            "name": partition_name,
            "path": partition_path,
            "exists": os.path.exists(partition_path),
            "size_mb": 0,
            "record_count": None,
            "date_range": None,
            "last_modified": None
        }
        
        if analysis["exists"]:
            # File size
            size_bytes = os.path.getsize(partition_path)
            analysis["size_mb"] = size_bytes / (1024 * 1024)
            
            # Last modified
            mtime = os.path.getmtime(partition_path)
            analysis["last_modified"] = datetime.fromtimestamp(mtime).isoformat()
            
            # Date range from partition name
            try:
                start_date, end_date = self.config.get_date_range_for_partition(partition_name)
                analysis["date_range"] = {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                }
            except Exception as e:
                logger.warning(f"Could not parse date range for {partition_name}: {e}")
            
            # Record count (optional - can be expensive)
            # Uncomment if you need record counts
            # try:
            #     async with self.partition_manager.acquire_partition_connection(partition_name) as conn:
            #         result = conn.execute(f'SELECT COUNT(*) FROM "{schema.table_name}"').fetchone()
            #         analysis["record_count"] = result[0] if result else 0
            # except Exception as e:
            #     logger.warning(f"Could not get record count for {partition_name}: {e}")
        
        return analysis
    
    def _generate_performance_recommendations(self, partition_sizes: Dict[str, float]) -> List[str]:
        """Generate performance recommendations based on partition analysis."""
        recommendations = []
        
        if not partition_sizes:
            return recommendations
        
        # Check for very large partitions
        max_size = max(partition_sizes.values())
        if max_size > 2000:  # 2GB
            largest_partition = max(partition_sizes, key=partition_sizes.get)
            recommendations.append(f"Partition {largest_partition} is very large ({max_size:.1f}MB). Consider using a finer partitioning strategy.")
        
        # Check for very small partitions
        min_size = min(partition_sizes.values())
        if min_size < 10:  # 10MB
            smallest_partition = min(partition_sizes, key=partition_sizes.get)
            recommendations.append(f"Partition {smallest_partition} is very small ({min_size:.1f}MB). Consider using a coarser partitioning strategy.")
        
        # Check for uneven distribution
        avg_size = sum(partition_sizes.values()) / len(partition_sizes)
        size_variance = sum((size - avg_size) ** 2 for size in partition_sizes.values()) / len(partition_sizes)
        size_std = size_variance ** 0.5
        
        if size_std > avg_size * 0.5:  # High variance
            recommendations.append("Partition sizes vary significantly. Consider reviewing your partitioning strategy for better balance.")
        
        # Check total number of partitions
        if len(partition_sizes) > 100:
            recommendations.append(f"You have {len(partition_sizes)} partitions. Consider archiving old partitions to improve query performance.")
        
        return recommendations
    
    async def cleanup_old_partitions(self, retention_days: int, dry_run: bool = True) -> Dict[str, Any]:
        """Clean up old partitions based on retention policy."""
        cleanup_report = {
            "timestamp": datetime.now().isoformat(),
            "retention_days": retention_days,
            "dry_run": dry_run,
            "partitions_analyzed": 0,
            "partitions_to_delete": [],
            "partitions_deleted": [],
            "total_space_freed_mb": 0,
            "errors": []
        }
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        existing_partitions = self.config.list_existing_partitions()
        cleanup_report["partitions_analyzed"] = len(existing_partitions)
        
        logger.info(f"Analyzing {len(existing_partitions)} partitions for cleanup (retention: {retention_days} days, cutoff: {cutoff_date})")
        
        for partition_name in existing_partitions:
            try:
                # Get partition date range
                start_date, end_date = self.config.get_date_range_for_partition(partition_name)
                
                # Check if partition is older than retention period
                if end_date < cutoff_date:
                    partition_path = self.config.get_partition_path(partition_name)
                    size_mb = 0
                    
                    if os.path.exists(partition_path):
                        size_bytes = os.path.getsize(partition_path)
                        size_mb = size_bytes / (1024 * 1024)
                    
                    partition_info = {
                        "name": partition_name,
                        "path": partition_path,
                        "end_date": end_date.isoformat(),
                        "size_mb": size_mb
                    }
                    
                    cleanup_report["partitions_to_delete"].append(partition_info)
                    cleanup_report["total_space_freed_mb"] += size_mb
                    
                    if not dry_run:
                        # Actually delete the partition
                        try:
                            # Close any open connections to this partition first
                            if partition_name in self.partition_manager._partition_connections:
                                self.partition_manager._partition_connections[partition_name].close()
                                del self.partition_manager._partition_connections[partition_name]
                            
                            os.remove(partition_path)
                            cleanup_report["partitions_deleted"].append(partition_info)
                            logger.info(f"Deleted partition {partition_name} ({size_mb:.1f}MB)")
                            
                        except Exception as e:
                            error_msg = f"Failed to delete partition {partition_name}: {e}"
                            logger.error(error_msg)
                            cleanup_report["errors"].append(error_msg)
                
            except Exception as e:
                error_msg = f"Error analyzing partition {partition_name}: {e}"
                logger.warning(error_msg)
                cleanup_report["errors"].append(error_msg)
        
        if dry_run:
            logger.info(f"DRY RUN: Would delete {len(cleanup_report['partitions_to_delete'])} partitions, "
                       f"freeing {cleanup_report['total_space_freed_mb']:.1f}MB")
        else:
            logger.info(f"Cleanup completed: Deleted {len(cleanup_report['partitions_deleted'])} partitions, "
                       f"freed {cleanup_report['total_space_freed_mb']:.1f}MB")
        
        return cleanup_report
    
    async def backup_partition(self, partition_name: str, backup_directory: str) -> Dict[str, Any]:
        """Backup a specific partition to a backup directory."""
        backup_info = {
            "timestamp": datetime.now().isoformat(),
            "partition_name": partition_name,
            "backup_directory": backup_directory,
            "success": False,
            "backup_path": None,
            "original_size_mb": 0,
            "backup_size_mb": 0,
            "error": None
        }
        
        try:
            # Ensure backup directory exists
            os.makedirs(backup_directory, exist_ok=True)
            
            # Get source and destination paths
            source_path = self.config.get_partition_path(partition_name)
            backup_filename = f"{partition_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
            backup_path = os.path.join(backup_directory, backup_filename)
            
            if not os.path.exists(source_path):
                raise FileNotFoundError(f"Partition file not found: {source_path}")
            
            # Get original size
            backup_info["original_size_mb"] = os.path.getsize(source_path) / (1024 * 1024)
            
            # Copy the file (simple file copy - you might want to use a more sophisticated backup method)
            import shutil
            shutil.copy2(source_path, backup_path)
            
            # Verify backup
            if os.path.exists(backup_path):
                backup_info["backup_path"] = backup_path
                backup_info["backup_size_mb"] = os.path.getsize(backup_path) / (1024 * 1024)
                backup_info["success"] = True
                logger.info(f"Successfully backed up partition {partition_name} to {backup_path}")
            else:
                raise Exception("Backup file was not created")
            
        except Exception as e:
            backup_info["error"] = str(e)
            logger.error(f"Failed to backup partition {partition_name}: {e}")
        
        return backup_info
    
    async def create_partition_health_report(self, schema: Schema) -> Dict[str, Any]:
        """Create a comprehensive health report for all partitions."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "partition_config": {
                "strategy": self.config.strategy.value,
                "partition_column": self.config.partition_column,
                "base_path": self.config.base_partition_path
            },
            "overall_health": "unknown",
            "partition_count": 0,
            "total_size_gb": 0,
            "health_checks": {
                "all_partitions_accessible": True,
                "reasonable_size_distribution": True,
                "no_corrupted_files": True,
                "date_ranges_valid": True
            },
            "partitions": {},
            "recommendations": [],
            "errors": []
        }
        
        try:
            existing_partitions = self.config.list_existing_partitions()
            report["partition_count"] = len(existing_partitions)
            
            total_size = 0
            size_list = []
            
            for partition_name in existing_partitions:
                partition_health = await self._check_partition_health(partition_name, schema)
                report["partitions"][partition_name] = partition_health
                
                # Update overall health checks
                if not partition_health.get("accessible", False):
                    report["health_checks"]["all_partitions_accessible"] = False
                
                if partition_health.get("date_range_error"):
                    report["health_checks"]["date_ranges_valid"] = False
                
                size_mb = partition_health.get("size_mb", 0)
                total_size += size_mb
                size_list.append(size_mb)
            
            report["total_size_gb"] = total_size / 1024
            
            # Check size distribution
            if size_list:
                avg_size = sum(size_list) / len(size_list)
                max_size = max(size_list)
                min_size = min(size_list)
                
                # Flag if there's extreme size variation
                if max_size > avg_size * 10 or min_size < avg_size * 0.1:
                    report["health_checks"]["reasonable_size_distribution"] = False
            
            # Determine overall health
            health_score = sum(report["health_checks"].values())
            if health_score == len(report["health_checks"]):
                report["overall_health"] = "excellent"
            elif health_score >= len(report["health_checks"]) * 0.75:
                report["overall_health"] = "good"
            elif health_score >= len(report["health_checks"]) * 0.5:
                report["overall_health"] = "fair"
            else:
                report["overall_health"] = "poor"
            
            # Generate recommendations
            report["recommendations"] = self._generate_health_recommendations(report)
            
        except Exception as e:
            error_msg = f"Error creating health report: {e}"
            logger.error(error_msg)
            report["errors"].append(error_msg)
            report["overall_health"] = "error"
        
        return report
    
    async def _check_partition_health(self, partition_name: str, schema: Schema) -> Dict[str, Any]:
        """Check the health of a single partition."""
        health = {
            "name": partition_name,
            "accessible": False,
            "size_mb": 0,
            "date_range_error": None,
            "connection_test": False,
            "table_exists": False,
            "last_modified": None
        }
        
        try:
            partition_path = self.config.get_partition_path(partition_name)
            
            # Check file existence and size
            if os.path.exists(partition_path):
                health["size_mb"] = os.path.getsize(partition_path) / (1024 * 1024)
                health["last_modified"] = datetime.fromtimestamp(os.path.getmtime(partition_path)).isoformat()
                health["accessible"] = True
                
                # Test database connection
                try:
                    async with self.partition_manager.acquire_partition_connection(partition_name) as conn:
                        health["connection_test"] = True
                        
                        # Check if table exists
                        table_check_sql = f"""
                            SELECT COUNT(*) FROM information_schema.tables 
                            WHERE table_name = '{schema.table_name}'
                        """
                        result = conn.execute(table_check_sql).fetchone()
                        health["table_exists"] = result[0] > 0 if result else False
                        
                except Exception as e:
                    health["connection_error"] = str(e)
            
            # Validate date range
            try:
                start_date, end_date = self.config.get_date_range_for_partition(partition_name)
                health["date_range"] = {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                }
            except Exception as e:
                health["date_range_error"] = str(e)
        
        except Exception as e:
            health["error"] = str(e)
        
        return health
    
    def _generate_health_recommendations(self, report: Dict[str, Any]) -> List[str]:
        """Generate health recommendations based on the report."""
        recommendations = []
        
        if not report["health_checks"]["all_partitions_accessible"]:
            recommendations.append("Some partitions are not accessible. Check file permissions and disk space.")
        
        if not report["health_checks"]["reasonable_size_distribution"]:
            recommendations.append("Partition sizes vary significantly. Consider reviewing your partitioning strategy.")
        
        if not report["health_checks"]["date_ranges_valid"]:
            recommendations.append("Some partitions have invalid date ranges. Check partition naming convention.")
        
        if report["partition_count"] == 0:
            recommendations.append("No partitions found. Consider migrating data from main database.")
        elif report["partition_count"] > 200:
            recommendations.append("Very large number of partitions. Consider archiving old data.")
        
        if report["total_size_gb"] > 100:
            recommendations.append("Large total size. Consider implementing data retention policies.")
        
        return recommendations
    
    async def export_partition_metadata(self, output_file: str) -> Dict[str, Any]:
        """Export partition metadata to a JSON file."""
        try:
            existing_partitions = self.config.list_existing_partitions()
            
            metadata = {
                "export_timestamp": datetime.now().isoformat(),
                "partition_config": {
                    "strategy": self.config.strategy.value,
                    "partition_column": self.config.partition_column,
                    "base_path": self.config.base_partition_path
                },
                "partitions": []
            }
            
            for partition_name in existing_partitions:
                try:
                    partition_path = self.config.get_partition_path(partition_name)
                    
                    partition_info = {
                        "name": partition_name,
                        "path": partition_path,
                        "exists": os.path.exists(partition_path)
                    }
                    
                    if partition_info["exists"]:
                        partition_info["size_mb"] = os.path.getsize(partition_path) / (1024 * 1024)
                        partition_info["last_modified"] = datetime.fromtimestamp(os.path.getmtime(partition_path)).isoformat()
                        
                        # Add date range
                        try:
                            start_date, end_date = self.config.get_date_range_for_partition(partition_name)
                            partition_info["date_range"] = {
                                "start": start_date.isoformat(),
                                "end": end_date.isoformat()
                            }
                        except Exception as e:
                            partition_info["date_range_error"] = str(e)
                    
                    metadata["partitions"].append(partition_info)
                    
                except Exception as e:
                    logger.warning(f"Error processing partition {partition_name}: {e}")
            
            # Write to file
            with open(output_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Exported partition metadata to {output_file}")
            return {"success": True, "file": output_file, "partitions_exported": len(metadata["partitions"])}
            
        except Exception as e:
            error_msg = f"Error exporting partition metadata: {e}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
    
    async def close(self):
        """Close all connections."""
        await self.partition_manager.close_all_connections()
