# app/infrastructure/web/dependencies/common.py
from typing import Annotated
from fastapi import Depends
from app.domain.repositories.schema_repository import ISchemaRepository
from app.container.container import container

def get_schema_repository() -> ISchemaRepository:
    """
    FastAPI dependency that provides the ISchemaRepository instance
    from our Dependency Injection Container.
    """
    return container.schema_repository # Access the registered instance

# Annotated type for easy dependency injection in FastAPI routes
SchemaRepositoryDep = Annotated[ISchemaRepository, Depends(get_schema_repository)]
