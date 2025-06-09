from typing import Dict, Any
from app.domain.entities.data_record import DataRecord
from app.domain.repositories.data_repository import IDataRepository
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.exceptions import SchemaNotFoundException, InvalidDataException
from app.config.logging_config import logger

class CreateDataRecordUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, data: Dict[str, Any]) -> DataRecord:
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            schema.validate_data(data)
            record = await self.data_repository.create(schema, data)
            
            return record
        except Exception as e:
            logger.error(f"use_case_failed: CreateDataRecordUseCase, error={str(e)}, "
                        f"schema_name={schema_name}")
            raise
