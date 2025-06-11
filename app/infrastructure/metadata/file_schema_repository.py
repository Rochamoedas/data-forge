from typing import List, Optional, Dict
from app.domain.entities.schema import Schema
from app.domain.repositories.schema_repository import ISchemaRepository
from app.infrastructure.metadata.schemas_description import SCHEMAS_METADATA
from app.config.logging_config import logger

class FileSchemaRepository(ISchemaRepository):
    """
    A repository that loads schema metadata from a Python file.
    This implementation acts as an in-memory cache for schemas.
    """
    def __init__(self):
        self._schemas: Dict[str, Schema] = {}
        self._load_schemas()

    def _load_schemas(self):
        """Loads schemas from the central metadata definition."""
        for schema_data in SCHEMAS_METADATA:
            try:
                schema = Schema(**schema_data)
                self._schemas[schema.name] = schema
            except Exception as e:
                logger.error(f"Error loading schema: {schema_data.get('name', 'N/A')}. Error: {e}")
        logger.info(f"Successfully loaded {len(self._schemas)} schemas into FileSchemaRepository.")

    def get_schema_by_name(self, schema_name: str) -> Optional[Schema]:
        """Retrieves a schema by its name."""
        return self._schemas.get(schema_name)

    def get_all_schemas(self) -> List[Schema]:
        """Retrieves all available schemas."""
        return list(self._schemas.values()) 