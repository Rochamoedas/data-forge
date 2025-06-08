from typing import Dict, Any, List
from app.domain.entities.data_record import DataRecord
from app.domain.repositories.data_repository import IDataRepository
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.exceptions import SchemaNotFoundException, InvalidDataException
from app.config.logging_config import logger
import time

class CreateBulkDataRecordsUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, data_list: List[Dict[str, Any]]) -> List[DataRecord]:
        start_time = time.perf_counter()
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            
            # Validate all records before creating any
            records = []
            for i, data in enumerate(data_list):
                try:
                    schema.validate_data(data)
                    record = DataRecord(schema_name=schema.name, data=data)
                    records.append(record)
                except Exception as e:
                    raise InvalidDataException(f"Invalid data at index {i}: {str(e)}")
            
            # Batch create all records
            await self.data_repository.create_batch(schema, records)
            
            duration = time.perf_counter() - start_time
            logger.info(f"use_case_completed: CreateBulkDataRecordsUseCase, schema_name={schema_name}, records_count={len(records)}, duration_ms={duration * 1000}")
            return records
        except Exception as e:
            duration = time.perf_counter() - start_time
            logger.error(f"use_case_failed: CreateBulkDataRecordsUseCase, error={str(e)}, schema_name={schema_name}, records_count={len(data_list) if data_list else 0}, duration_ms={duration * 1000}")
            raise 