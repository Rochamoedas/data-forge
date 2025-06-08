# app/container/container.py
from app.config.settings import settings
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.infrastructure.persistence.duckdb.schema_manager import DuckDBSchemaManager
from app.infrastructure.persistence.repositories.file_schema_repository import FileSchemaRepository
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.repositories.data_repository import IDataRepository
from app.infrastructure.persistence.repositories.duckdb_data_repository import DuckDBDataRepository
from app.application.use_cases.create_data_record import CreateDataRecordUseCase
from app.application.use_cases.create_bulk_data_records import CreateBulkDataRecordsUseCase
from app.application.use_cases.get_data_record import GetDataRecordUseCase
from app.application.use_cases.query_data_records import (
    QueryDataRecordsUseCase, 
    StreamDataRecordsUseCase, 
    CountDataRecordsUseCase
)

class Container:
    def __init__(self):
        # Core infrastructure
        self.connection_pool = AsyncDuckDBPool(database_path=settings.DATABASE_PATH)
        self.schema_manager = DuckDBSchemaManager(connection_pool=self.connection_pool)

        # Repositories
        self.schema_repository: ISchemaRepository = FileSchemaRepository(schema_manager=self.schema_manager)
        self.data_repository: IDataRepository = DuckDBDataRepository(connection_pool=self.connection_pool)

        # Use Cases
        self.create_data_record_use_case = CreateDataRecordUseCase(
            data_repository=self.data_repository,
            schema_repository=self.schema_repository
        )
        self.create_bulk_data_records_use_case = CreateBulkDataRecordsUseCase(
            data_repository=self.data_repository,
            schema_repository=self.schema_repository
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