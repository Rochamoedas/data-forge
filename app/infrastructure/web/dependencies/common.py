# app/infrastructure/web/dependencies/common.py
from typing import Annotated
from fastapi import Depends
from app.domain.repositories.schema_repository import ISchemaRepository
# Import the container (will be created in step 4.1)
from app.container.container import container

def get_schema_repository() -> ISchemaRepository:
    """
    FastAPI dependency that provides the ISchemaRepository instance
    from our Dependency Injection Container.
    """
    return container.schema_repository # Access the registered instance

# Annotated type for easy dependency injection in FastAPI routes
SchemaRepositoryDep = Annotated[ISchemaRepository, Depends(get_schema_repository)]

# Future: In Sprint 2, we'll add dependencies for IDataRepository and use cases
# from src.domain.repositories.data_repository import IDataRepository
# def get_data_repository() -> IDataRepository:
#     return container.data_repository
# DataRepositoryDep = Annotated[IDataRepository, Depends(get_data_repository)]

# from src.application.use_cases.create_data_record import CreateDataRecordUseCase
# def get_create_data_record_use_case() -> CreateDataRecordUseCase:
#     return container.create_data_record_use_case
# CreateDataRecordUseCaseDep = Annotated[CreateDataRecordUseCase, Depends(get_create_data_record_use_case)]

# from src.application.use_cases.get_data_record import GetDataRecordUseCase
# def get_get_data_record_use_case() -> GetDataRecordUseCase:
#     return container.get_data_record_use_case
# GetDataRecordUseCaseDep = Annotated[GetDataRecordUseCase, Depends(get_get_data_record_use_case)]