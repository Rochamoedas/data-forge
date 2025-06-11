from typing import Optional, List, Dict
from app.domain.entities.schema import Schema
from app.domain.repositories.interfaces import ISchemaRepository
from app.infrastructure.metadata.schemas_description import SCHEMAS_METADATA
from app.infrastructure.persistence.duckdb_schema_manager import DuckDBSchemaManager
from app.config.logging_config import logger

class FileSchemaRepository(ISchemaRepository):
    def __init__(self, schema_manager: DuckDBSchemaManager):
        self._schemas: Dict[str, Schema] = {}
        self.schema_manager = schema_manager

    async def initialize(self):
        # Create all schemas first
        schemas = []
        for schema_data in SCHEMAS_METADATA:
            schema = Schema(**schema_data)
            self._schemas[schema.name] = schema
            schemas.append(schema)
        
        # Create all tables and indexes in a single transaction
        await self.schema_manager.ensure_tables_exist(schemas)
        logger.info(f"Loaded {len(self._schemas)} schemas and ensured tables in DuckDB.")

    async def get_schema_by_name(self, name: str) -> Optional[Schema]:
        return self._schemas.get(name)

    async def get_all_schemas(self) -> List[Schema]:
        return list(self._schemas.values()) 