"""
CQRS Command Handlers for Bulk Data Operations
"""

import pyarrow as pa

from app.application.commands.bulk_data_commands import (
    BulkInsertFromArrowTableCommand,
    BulkUpdateFromArrowTableCommand,
    BulkReadToArrowCommand
)
from app.domain.entities.schema import Schema
from app.domain.repositories.schema_repository import ISchemaRepository
from app.infrastructure.persistence.arrow_bulk_operations import IArrowBulkOperations
from app.domain.exceptions import SchemaNotFoundException
from app.config.logging_config import logger


class BulkDataCommandHandler:
    """Command handler for bulk data operations"""
    
    def __init__(
        self,
        schema_repository: ISchemaRepository,
        arrow_operations: IArrowBulkOperations
    ):
        self.schema_repository = schema_repository
        self.arrow_operations = arrow_operations
    
    async def handle_bulk_insert_from_arrow_table(
        self, 
        command: BulkInsertFromArrowTableCommand
    ) -> None:
        """Handle bulk insert from Arrow Table command"""
        
        schema = await self._get_schema(command.schema_name)
        await self.arrow_operations.bulk_insert_from_arrow_table(schema, command.arrow_table)
        logger.info(f"Bulk insert from Arrow Table completed: {command.arrow_table.num_rows} records")
    
    async def handle_bulk_read_to_arrow(
        self, 
        command: BulkReadToArrowCommand
    ) -> pa.Table:
        """Handle bulk read to Arrow Table command"""
        
        schema = await self._get_schema(command.schema_name)
        result = await self.arrow_operations.bulk_read_to_arrow_table(schema)
        logger.info(f"Bulk read to Arrow completed: {result.num_rows} records")
        return result
    
    async def _get_schema(self, schema_name: str) -> Schema:
        """Get schema from repository with proper error handling"""
        schema = await self.schema_repository.get_schema_by_name(schema_name)
        if not schema:
            raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
        return schema