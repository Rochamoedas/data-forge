# app/container/container.py
from app.config.settings import settings
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.infrastructure.persistence.duckdb.schema_manager import DuckDBSchemaManager
from app.infrastructure.persistence.repositories.file_schema_repository import FileSchemaRepository
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.repositories.data_repository import IDataRepository
from app.infrastructure.persistence.repositories.duckdb_data_repository import DuckDBDataRepository
from app.infrastructure.persistence.high_performance_data_processor import HighPerformanceDataProcessor
from app.infrastructure.persistence.arrow_bulk_operations import ArrowBulkOperations
from app.application.command_handlers.bulk_data_command_handlers import BulkDataCommandHandler
from app.application.use_cases.create_data_record import CreateDataRecordUseCase
from app.application.use_cases.create_bulk_data_records import CreateBulkDataRecordsUseCase
from app.application.use_cases.create_ultra_fast_bulk_data import CreateUltraFastBulkDataUseCase
from app.application.use_cases.get_data_record import GetDataRecordUseCase
from app.application.use_cases.query_data_records import (
    QueryDataRecordsUseCase, 
    StreamDataRecordsUseCase, 
    CountDataRecordsUseCase
)

class Container:
    def __init__(self):
        # Core infrastructure
        self.connection_pool = AsyncDuckDBPool()
        self.schema_manager = DuckDBSchemaManager(connection_pool=self.connection_pool)

        # Repositories
        self.schema_repository: ISchemaRepository = FileSchemaRepository(schema_manager=self.schema_manager)
        self.data_repository: IDataRepository = DuckDBDataRepository(connection_pool=self.connection_pool)
        
        # High-Performance Data Processor
        self.high_performance_processor = HighPerformanceDataProcessor(
            connection_pool=self.connection_pool,
            max_workers=8  # Optimize for your system
        )
        
        # Arrow-based bulk operations
        self.arrow_bulk_operations = ArrowBulkOperations(
            connection_pool=self.connection_pool
        )
        
        # CQRS Command Handler for bulk operations
        self.bulk_data_command_handler = BulkDataCommandHandler(
            schema_repository=self.schema_repository,
            arrow_operations=self.arrow_bulk_operations
        )

        # Use Cases
        self.create_data_record_use_case = CreateDataRecordUseCase(
            data_repository=self.data_repository,
            schema_repository=self.schema_repository
        )
        self.create_bulk_data_records_use_case = CreateBulkDataRecordsUseCase(
            data_repository=self.data_repository,
            schema_repository=self.schema_repository
        )
        self.create_ultra_fast_bulk_data_use_case = CreateUltraFastBulkDataUseCase(
            command_handler=self.bulk_data_command_handler
        )
        self.get_data_record_use_case = GetDataRecordUseCase(
            data_repository=self.data_repository,
            schema_repository=self.schema_repository
        )
        self.query_data_records_use_case = QueryDataRecordsUseCase(
            data_repository=self.data_repository,
            schema_repository=self.schema_repository
        )
        self.stream_data_records_use_case = StreamDataRecordsUseCase(
            data_repository=self.data_repository,
            schema_repository=self.schema_repository
        )
        self.count_data_records_use_case = CountDataRecordsUseCase(
            data_repository=self.data_repository,
            schema_repository=self.schema_repository
        )

    async def startup(self):
        await self.connection_pool.initialize()
        await self.schema_repository.initialize()

    async def shutdown(self):
        await self.connection_pool.close()

# Create a single instance of our container
container = Container()