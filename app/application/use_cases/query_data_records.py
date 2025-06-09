from typing import Optional, AsyncIterator
from app.domain.entities.data_record import DataRecord
from app.domain.repositories.data_repository import IDataRepository
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.exceptions import SchemaNotFoundException
from app.application.dto.query_dto import DataQueryRequest
from app.application.dto.data_dto import PaginatedResponse
from app.config.logging_config import logger


class QueryDataRecordsUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, query_request: DataQueryRequest) -> PaginatedResponse[DataRecord]:
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            
            result = await self.data_repository.get_all(schema, query_request)
            
            return result
        except Exception as e:
            logger.error(f"use_case_failed: QueryDataRecordsUseCase, error={str(e)}, "
                        f"schema_name={schema_name}")
            raise

class StreamDataRecordsUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, query_request: DataQueryRequest) -> AsyncIterator[DataRecord]:
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            
            async for record in self.data_repository.stream_query_results(schema, query_request):
                yield record
            
        except Exception as e:
            logger.error(f"use_case_failed: StreamDataRecordsUseCase, error={str(e)}, "
                        f"schema_name={schema_name}")
            raise

class CountDataRecordsUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, query_request: DataQueryRequest) -> int:
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            
            count = await self.data_repository.count_all(schema, query_request)
            
            return count
        except Exception as e:
            logger.error(f"use_case_failed: CountDataRecordsUseCase, error={str(e)}, "
                        f"schema_name={schema_name}")
            raise 