# app/container/container.py
from app.config.settings import settings
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.infrastructure.persistence.file_schema_repository import FileSchemaRepository
from app.infrastructure.persistence.duckdb_schema_manager import DuckDBSchemaManager
from app.domain.repositories.interfaces import ISchemaRepository
from app.infrastructure.persistence.arrow_bulk_operations import ArrowBulkOperations
from app.application.command_handlers.bulk_data_command_handlers import BulkDataCommandHandler
from app.application.use_cases.create_ultra_fast_bulk_data import CreateUltraFastBulkDataUseCase

class Container:
    def __init__(self):
        # Core infrastructure
        self.connection_pool = AsyncDuckDBPool()
        # We need a schema manager for the file schema repository.
        self.schema_manager = DuckDBSchemaManager(connection_pool=self.connection_pool)
        self.schema_repository: ISchemaRepository = FileSchemaRepository(schema_manager=self.schema_manager)
        
        # Arrow-based bulk operations (Kept for high-performance endpoints)
        self.arrow_bulk_operations = ArrowBulkOperations(
            connection_pool=self.connection_pool
        )
        
        # CQRS Command Handler for bulk operations
        self.bulk_data_command_handler = BulkDataCommandHandler(
            schema_repository=self.schema_repository,
            arrow_operations=self.arrow_bulk_operations
        )

        # Use Case for ultra-fast bulk data
        self.create_ultra_fast_bulk_data_use_case = CreateUltraFastBulkDataUseCase(
            command_handler=self.bulk_data_command_handler
        )

    async def startup(self):
        # The new DataService handles its own initialization.
        # The container's startup now only needs to initialize what's necessary
        # for the arrow-performance endpoints.
        await self.connection_pool.initialize()
        await self.schema_repository.initialize()

    async def shutdown(self):
        await self.connection_pool.close()

# Create a single instance of our container
container = Container()