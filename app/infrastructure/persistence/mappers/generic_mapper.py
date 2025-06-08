# app/infrastructure/persistence/mappers/generic_mapper.py
from typing import Dict, Any, List, Tuple, Optional, Iterator, Union
from uuid import UUID, uuid4
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import logging
from app.domain.entities.data_record import DataRecord
from app.domain.exceptions import InvalidDataException

logger = logging.getLogger(__name__)

@dataclass
class MappingResult:
    """Result container for batch mapping operations."""
    success_count: int
    error_count: int
    errors: List[str]

class PerformantGenericMapper:
    """
    High-performance generic mapper optimized for million-row operations.
    Provides both single-record and batch processing capabilities.
    """
    
    def __init__(self, max_workers: int = 4, batch_size: int = 1000):
        self.max_workers = max_workers
        self.batch_size = batch_size
        self._uuid_cache = {}  # Cache for UUID parsing to avoid repeated conversions

    def map_dict_to_data_record(
        self, 
        data: Dict[str, Any], 
        schema_name: str, 
        record_id: Optional[Any] = None
    ) -> DataRecord:
        """
        Maps a dictionary of data to a DataRecord entity.
        Optimized with caching and fast UUID handling.
        """
        try:
            # Fast path for record_id extraction
            if record_id is None:
                record_id = data.get("id")
            
            # Optimized UUID conversion with caching
            if record_id is not None:
                record_id = self._convert_to_uuid(record_id)
            
            # Create clean data copy (exclude id from data if present)
            clean_data = {k: v for k, v in data.items() if k != "id"}
            
            return DataRecord(id=record_id, schema_name=schema_name, data=clean_data)
            
        except Exception as e:
            logger.error(f"Error mapping dict to DataRecord for schema {schema_name}: {e}")
            raise InvalidDataException(f"Failed to map data: {str(e)}")

    def map_data_record_to_dict(self, record: DataRecord) -> Dict[str, Any]:
        """
        Maps a DataRecord entity back to a dictionary.
        Optimized for API responses and persistence.
        """
        try:
            # Pre-allocate dictionary with known size for performance
            result = {"id": str(record.id)}
            result.update(record.data)
            return result
        except Exception as e:
            logger.error(f"Error mapping DataRecord to dict: {e}")
            raise InvalidDataException(f"Failed to serialize record: {str(e)}")

    def map_duckdb_row_to_dict(self, row: Tuple, column_names: List[str]) -> Dict[str, Any]:
        """
        Maps a single DuckDB row to dictionary.
        Optimized with fast zip operation.
        """
        if len(row) != len(column_names):
            raise InvalidDataException(
                f"Row length ({len(row)}) doesn't match column count ({len(column_names)})"
            )
        
        return dict(zip(column_names, row))

    def batch_map_duckdb_rows_to_dicts(
        self, 
        rows: List[Tuple], 
        column_names: List[str]
    ) -> List[Dict[str, Any]]:
        """
        High-performance batch mapping of DuckDB rows to dictionaries.
        Uses list comprehension for optimal performance.
        """
        if not rows:
            return []
        
        # Validate first row for early error detection
        if rows and len(rows[0]) != len(column_names):
            raise InvalidDataException(
                f"Row length doesn't match column count. Expected {len(column_names)}, got {len(rows[0])}"
            )
        
        try:
            # Use list comprehension for maximum performance
            return [dict(zip(column_names, row)) for row in rows]
        except Exception as e:
            logger.error(f"Error in batch mapping: {e}")
            raise InvalidDataException(f"Batch mapping failed: {str(e)}")

    def batch_map_dicts_to_data_records(
        self, 
        data_list: List[Dict[str, Any]], 
        schema_name: str
    ) -> Tuple[List[DataRecord], MappingResult]:
        """
        Batch mapping with error handling and performance monitoring.
        Returns both successful records and mapping statistics.
        """
        if not data_list:
            return [], MappingResult(0, 0, [])
        
        records = []
        errors = []
        
        try:
            # Process in batches for memory efficiency
            for i in range(0, len(data_list), self.batch_size):
                batch = data_list[i:i + self.batch_size]
                
                for idx, data in enumerate(batch):
                    try:
                        record = self.map_dict_to_data_record(data, schema_name)
                        records.append(record)
                    except Exception as e:
                        error_msg = f"Row {i + idx}: {str(e)}"
                        errors.append(error_msg)
                        logger.warning(error_msg)
            
            result = MappingResult(
                success_count=len(records),
                error_count=len(errors),
                errors=errors
            )
            
            logger.info(f"Batch mapping completed: {result.success_count} success, {result.error_count} errors")
            return records, result
            
        except Exception as e:
            logger.error(f"Critical error in batch mapping: {e}")
            raise InvalidDataException(f"Batch mapping failed: {str(e)}")

    def stream_map_duckdb_rows(
        self, 
        rows_iterator: Iterator[Tuple], 
        column_names: List[str]
    ) -> Iterator[Dict[str, Any]]:
        """
        Memory-efficient streaming mapper for very large datasets.
        Yields dictionaries one at a time to minimize memory usage.
        """
        column_count = len(column_names)
        
        for row_idx, row in enumerate(rows_iterator):
            try:
                if len(row) != column_count:
                    raise InvalidDataException(
                        f"Row {row_idx}: length mismatch. Expected {column_count}, got {len(row)}"
                    )
                
                yield dict(zip(column_names, row))
                
            except Exception as e:
                logger.warning(f"Skipping row {row_idx} due to error: {e}")
                continue

    def _convert_to_uuid(self, value: Any) -> UUID:
        """
        Optimized UUID conversion with caching.
        Handles string UUIDs, UUID objects, and None values.
        """
        if value is None:
            return uuid4()
        
        if isinstance(value, UUID):
            return value
        
        if isinstance(value, str):
            # Use cache for repeated UUID strings
            if value in self._uuid_cache:
                return self._uuid_cache[value]
            
            try:
                uuid_obj = UUID(value)
                # Cache only if cache isn't too large (prevent memory bloat)
                if len(self._uuid_cache) < 10000:
                    self._uuid_cache[value] = uuid_obj
                return uuid_obj
            except ValueError:
                raise ValueError(f"Invalid UUID format: {value}")
        
        # Try to convert other types to string first
        try:
            return UUID(str(value))
        except ValueError:
            raise ValueError(f"Cannot convert {type(value).__name__} to UUID: {value}")

# Global mapper instance for dependency injection
def get_mapper_instance() -> PerformantGenericMapper:
    """Factory function for creating mapper instances."""
    return PerformantGenericMapper(max_workers=4, batch_size=1000)

# Convenience functions for backward compatibility
def map_dict_to_data_record(data: Dict[str, Any], schema_name: str, record_id: Optional[Any] = None) -> DataRecord:
    """Convenience function using the optimized mapper."""
    mapper = get_mapper_instance()
    return mapper.map_dict_to_data_record(data, schema_name, record_id)

def map_data_record_to_dict(record: DataRecord) -> Dict[str, Any]:
    """Convenience function using the optimized mapper."""
    mapper = get_mapper_instance()
    return mapper.map_data_record_to_dict(record)

def map_duckdb_row_to_dict(row: Tuple, column_names: List[str]) -> Dict[str, Any]:
    """Convenience function using the optimized mapper."""
    mapper = get_mapper_instance()
    return mapper.map_duckdb_row_to_dict(row, column_names)

def batch_map_duckdb_rows_to_dicts(rows: List[Tuple], column_names: List[str]) -> List[Dict[str, Any]]:
    """Convenience function for batch mapping."""
    mapper = get_mapper_instance()
    return mapper.batch_map_duckdb_rows_to_dicts(rows, column_names)