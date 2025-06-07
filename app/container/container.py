# app/container/container.py
# We'll use a simple custom container for clarity, but a library like `python-inject`
# or `fastapi-injector` would be used in a real project.

from app.infrastructure.persistence.duckdb.connection import DuckDBConnection
from app.infrastructure.persistence.duckdb.schema_manager import DuckDBSchemaManager
from app.infrastructure.persistence.repositories.file_schema_repository import FileSchemaRepository
from app.domain.repositories.schema_repository import ISchemaRepository
# Future: In Sprint 2, import and register IDataRepository and use cases
# from app.domain.repositories.data_repository import IDataRepository
# from app.infrastructure.persistence.repositories.duckdb_data_repository import DuckDBDataRepository
# from app.application.use_cases.create_data_record import CreateDataRecordUseCase
# from app.application.use_cases.get_data_record import GetDataRecordUseCase

class Container:
    """
    Our Dependency Injection Container.
    It knows how to create and provide all the necessary objects (dependencies).
    """
    def __init__(self):
        # 1. Initialize core infrastructure components
        self._db_connection = DuckDBConnection()
        self._schema_manager = DuckDBSchemaManager(db_connection=self._db_connection)

        # 2. Register concrete implementations for domain interfaces (Ports)
        # For ISchemaRepository, we use FileSchemaRepository
        self._schema_repository = FileSchemaRepository()

        # TODO: In Sprint 2, we'll register DuckDBDataRepository for IDataRepository
        # self._data_repository = DuckDBDataRepository(
        #     db_connection=self._db_connection,
        #     schema_manager=self._schema_manager # SchemaManager is needed for table creation on first write
        # )



    # Properties to access the registered instances
    @property
    def db_connection(self) -> DuckDBConnection:
        return self._db_connection

    @property
    def schema_manager(self) -> DuckDBSchemaManager:
        return self._schema_manager

    @property
    def schema_repository(self) -> ISchemaRepository:
        return self._schema_repository

    # Future: In Sprint 2, add properties for data_repository and use cases
    # @property
    # def data_repository(self) -> IDataRepository:
    #     return self._data_repository

    # @property
    # def create_data_record_use_case(self) -> CreateDataRecordUseCase:
    #     return self._create_data_record_use_case

    # @property
    # def get_data_record_use_case(self) -> GetDataRecordUseCase:
    #     return self._get_data_record_use_case

# Create a single instance of our container
container = Container()