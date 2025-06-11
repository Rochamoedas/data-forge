# app/container/container.py
from app.config.settings import settings
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.infrastructure.metadata.file_schema_repository import FileSchemaRepository
from app.infrastructure.persistence.duckdb.schema_manager import DuckDBSchemaManager
from app.infrastructure.persistence.repositories.duckdb_data_repository import DuckDBDataRepository
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.repositories.data_repository import IDataRepository
# Command Handlers and Use Cases would be imported here
from app.application.use_cases.create_ultra_fast_bulk_data import CreateUltraFastBulkDataUseCase
from app.application.command_handlers.bulk_data_command_handlers import BulkDataCommandHandler

class Container:
    def __init__(self):
        # Core Infrastructure Providers
        self.connection_pool = AsyncDuckDBPool()
        
        self.schema_repository: ISchemaRepository = FileSchemaRepository()
        
        self.data_repository: IDataRepository = DuckDBDataRepository(
            connection_pool=self.connection_pool
        )
        
        self.schema_manager = DuckDBSchemaManager(
            connection_pool=self.connection_pool
        )
        
        # Application Layer Providers (Use Cases and Handlers)
        # These would be instantiated here, injected with repositories
        # For example:
        self.bulk_data_command_handler = BulkDataCommandHandler(
            schema_repository=self.schema_repository,
            data_repository=self.data_repository
        )
        self.create_ultra_fast_bulk_data_use_case = CreateUltraFastBulkDataUseCase(
            command_handler=self.bulk_data_command_handler
        )

    async def startup(self):
        """
        Initializes core services and prepares the database.
        """
        # Initialize the database connection pool
        await self.connection_pool.initialize()
        
        # Load all schemas from the repository
        schemas = self.schema_repository.get_all_schemas()
        
        # Ensure all tables and indexes exist in the database
        await self.schema_manager.create_tables_and_indexes(schemas)

    async def shutdown(self):
        """
        Gracefully closes resources.
        """
        await self.connection_pool.close()

# Create a single, shared instance of the container
container = Container()