from typing import Dict, Any
from app.domain.entities.data_record import DataRecord
from app.domain.repositories.data_repository import IDataRepository
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.exceptions import SchemaNotFoundException, InvalidDataException
from app.config.logging_config import logger
from app.infrastructure.web.dependencies.profiling import log_use_case_performance
import time

class CreateDataRecordUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, data: Dict[str, Any]) -> DataRecord:
        start_time = time.perf_counter()
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            schema.validate_data(data)
            record = await self.data_repository.create(schema, data)
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            log_use_case_performance(
                "CreateDataRecordUseCase", 
                schema_name, 
                duration_ms,
                record_id=str(record.id)
            )
            return record
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.error(f"use_case_failed: CreateDataRecordUseCase, error={str(e)}, "
                        f"schema_name={schema_name}, duration_ms={duration_ms:.2f}")
            raise
