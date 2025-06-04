# app/infrastructure/persistence/repositories/file_schema_repository.py
from typing import Optional
from app.domain.entities.schema import Schema
from app.domain.repositories.schema_repository import ISchemaRepository
from app.infrastructure.metadata.schemas_description import ALL_SCHEMAS

class FileSchemaRepository(ISchemaRepository):
    """
    An adapter that implements ISchemaRepository, providing schema definitions
    loaded from a static Python file (schemas_description.py).
    """
    def __init__(self):
        # Load all schemas into an in-memory dictionary for quick access
        self._schemas = ALL_SCHEMAS
        print(f"Loaded {len(self._schemas)} schemas from file.")

    def get_schema_by_name(self, schema_name: str) -> Optional[Schema]:
        """
        Retrieves a schema definition by its name from the in-memory cache.
        """
        return self._schemas.get(schema_name)