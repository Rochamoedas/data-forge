# app/infrastructure/persistence/repositories/file_schema_repository.py
from typing import Optional, List, Dict
from app.domain.entities.schema import Schema
from app.domain.repositories.schema_repository import ISchemaRepository
from app.infrastructure.metadata.schemas_description import SCHEMAS_METADATA
from app.infrastructure.persistence.duckdb.schema_manager import DuckDBSchemaManager
from app.config.logging_config import logger

class FileSchemaRepository(ISchemaRepository):
    def __init__(self, schema_manager: DuckDBSchemaManager):
        self._schemas: Dict[str, Schema] = {}
        self.schema_manager = schema_manager

    async def initialize(self):
        for schema_data in SCHEMAS_METADATA:
            schema = Schema(**schema_data)
            self._schemas[schema.name] = schema
            await self.schema_manager.ensure_table_exists(schema)
        logger.info(f"Loaded {len(self._schemas)} schemas and ensured tables in DuckDB.")

    async def get_schema_by_name(self, name: str) -> Optional[Schema]:
        return self._schemas.get(name)

    async def get_all_schemas(self) -> List[Schema]:
        return list(self._schemas.values())