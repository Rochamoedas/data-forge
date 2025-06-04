# app/infrastructure/persistence/repositories/file_schema_repository.py
from typing import List, Optional
from app.domain.entities.schema import Schema
from app.domain.repositories.schema_repository import ISchemaRepository
from app.infrastructure.metadata.schemas_description import ALL_SCHEMAS, get_schema_by_name, list_available_schemas

class FileSchemaRepository(ISchemaRepository):
    """
    A file-based implementation of ISchemaRepository.

    For the MVP, schemas are defined in Python files rather than stored in a database.
    This simplifies early development while maintaining the repository pattern.
    """
    
    def get_by_name(self, name: str) -> Optional[Schema]:
        """Get a schema by its name."""
        try:
            return get_schema_by_name(name)
        except ValueError:
            return None
    
    def get_all(self) -> List[Schema]:
        """Get all available schemas."""
        return list(ALL_SCHEMAS.values())
    
    def list_schema_names(self) -> List[str]:
        """Get a list of all available schema names."""
        return list_available_schemas()
    
    def schema_exists(self, name: str) -> bool:
        """Check if a schema exists."""
        return name in ALL_SCHEMAS