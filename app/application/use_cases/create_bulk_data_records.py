from typing import Dict, Any, List
from app.domain.entities.data_record import DataRecord
from app.domain.repositories.data_repository import IDataRepository
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.exceptions import SchemaNotFoundException, InvalidDataException
from app.config.logging_config import logger
from app.infrastructure.web.dependencies.profiling import log_use_case_performance
import time

class CreateBulkDataRecordsUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, data_list: List[Dict[str, Any]]) -> List[DataRecord]:
        start_time = time.perf_counter()
        original_count = len(data_list)
        
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            
            # Phase 1: Deduplication based on composite keys
            unique_data = []
            seen_keys = set()
            duplicates_removed = 0
            
            for i, data in enumerate(data_list):
                try:
                    schema.validate_data(data)
                    
                    # Create composite key for deduplication if schema has primary key
                    if schema.primary_key:
                        composite_key = schema.get_composite_key_from_data(data)
                        if composite_key:
                            # Create a hashable key from the composite key
                            key_tuple = tuple(sorted(composite_key.items()))
                            if key_tuple in seen_keys:
                                duplicates_removed += 1
                                logger.debug(f"Duplicate record found at index {i}, composite key: {composite_key}")
                                continue
                            seen_keys.add(key_tuple)
                    
                    unique_data.append(data)
                    
                except Exception as e:
                    raise InvalidDataException(f"Invalid data at index {i}: {str(e)}")
            
            # Phase 2: Create DataRecord objects for unique data
            records = []
            for data in unique_data:
                record = DataRecord(schema_name=schema.name, data=data)
                records.append(record)
            
            # Phase 3: Batch create all unique records
            if records:
                await self.data_repository.create_batch(schema, records)
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            # Log detailed performance metrics including deduplication stats
            logger.info(f"Bulk insert completed: {len(records)} records inserted, "
                       f"{duplicates_removed} duplicates removed from {original_count} total records")
            
            log_use_case_performance(
                "CreateBulkDataRecordsUseCase", 
                schema_name, 
                duration_ms,
                records_count=len(records),
                original_count=original_count,
                duplicates_removed=duplicates_removed
            )
            
            return records
            
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            # Check if it's a constraint violation (common in bulk operations)
            if "Constraint Error" in str(e) and "Duplicate key" in str(e):
                logger.warning(f"use_case_constraint_violation: CreateBulkDataRecordsUseCase, error={str(e)}, "
                            f"schema_name={schema_name}, records_count={len(data_list) if data_list else 0}, "
                            f"duration_ms={duration_ms:.2f}")
            else:
                logger.error(f"use_case_failed: CreateBulkDataRecordsUseCase, error={str(e)}, "
                            f"schema_name={schema_name}, records_count={len(data_list) if data_list else 0}, "
                            f"duration_ms={duration_ms:.2f}")
            raise 