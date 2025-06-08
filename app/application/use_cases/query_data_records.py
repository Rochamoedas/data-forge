from typing import Optional, AsyncIterator
from app.domain.entities.data_record import DataRecord
from app.domain.repositories.data_repository import IDataRepository
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.exceptions import SchemaNotFoundException
from app.application.dto.query_dto import DataQueryRequest
from app.application.dto.data_dto import PaginatedResponse
from app.config.logging_config import logger
from app.infrastructure.web.dependencies.profiling import log_use_case_performance
import time

class QueryDataRecordsUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, query_request: DataQueryRequest) -> PaginatedResponse[DataRecord]:
        start_time = time.perf_counter()
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            
            result = await self.data_repository.get_all(schema, query_request)
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            log_use_case_performance(
                "QueryDataRecordsUseCase", 
                schema_name, 
                duration_ms,
                page=query_request.pagination.page,
                size=query_request.pagination.size,
                filters=len(query_request.filters or []),
                total_results=result.total
            )
            return result
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.error(f"use_case_failed: QueryDataRecordsUseCase, error={str(e)}, "
                        f"schema_name={schema_name}, duration_ms={duration_ms:.2f}")
            raise

class StreamDataRecordsUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, query_request: DataQueryRequest) -> AsyncIterator[DataRecord]:
        start_time = time.perf_counter()
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            
            record_count = 0
            async for record in self.data_repository.stream_query_results(schema, query_request):
                record_count += 1
                yield record
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            log_use_case_performance(
                "StreamDataRecordsUseCase", 
                schema_name, 
                duration_ms,
                streamed_records=record_count
            )
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.error(f"use_case_failed: StreamDataRecordsUseCase, error={str(e)}, "
                        f"schema_name={schema_name}, duration_ms={duration_ms:.2f}")
            raise

class CountDataRecordsUseCase:
    def __init__(self, data_repository: IDataRepository, schema_repository: ISchemaRepository):
        self.data_repository = data_repository
        self.schema_repository = schema_repository

    async def execute(self, schema_name: str, query_request: DataQueryRequest) -> int:
        start_time = time.perf_counter()
        try:
            schema = await self.schema_repository.get_schema_by_name(schema_name)
            if not schema:
                raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
            
            count = await self.data_repository.count_all(schema, query_request)
            
            duration_ms = (time.perf_counter() - start_time) * 1000
            log_use_case_performance(
                "CountDataRecordsUseCase", 
                schema_name, 
                duration_ms,
                count=count
            )
            return count
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.error(f"use_case_failed: CountDataRecordsUseCase, error={str(e)}, "
                        f"schema_name={schema_name}, duration_ms={duration_ms:.2f}")
            raise 