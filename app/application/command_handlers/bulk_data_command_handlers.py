"""
CQRS Command Handlers for Bulk Data Operations
"""

import polars as pl
import pyarrow as pa
from typing import List, Dict, Any

from app.application.commands.bulk_data_commands import (
    BulkInsertFromArrowTableCommand,
    BulkReadToArrowCommand
)
from app.domain.entities.schema import Schema
from app.domain.repositories.schema_repository import ISchemaRepository
from app.domain.repositories.data_repository import IDataRepository
from app.domain.exceptions import SchemaNotFoundException
from app.config.logging_config import logger


class BulkDataCommandHandler:
    """Command handler for bulk data operations, optimized for performance."""
    
    def __init__(
        self,
        schema_repository: ISchemaRepository,
        data_repository: IDataRepository
    ):
        self.schema_repository = schema_repository
        self.data_repository = data_repository
    
    async def handle_bulk_insert_from_arrow_table(
        self, 
        command: BulkInsertFromArrowTableCommand
    ) -> None:
        """Handle bulk insert from an Arrow Table."""
        schema = self._get_schema(command.schema_name)
        
        # The data repository has a method to handle this directly
        rows_affected = await self.data_repository.bulk_insert_arrow(
            table_name=schema.table_name,
            arrow_table=command.arrow_table
        )
        logger.info(f"Bulk insert from Arrow Table completed: {rows_affected} records into '{schema.table_name}'.")
    
    async def handle_bulk_read_to_arrow(
        self, 
        command: BulkReadToArrowCommand
    ) -> pa.Table:
        """Handle bulk read to an Arrow Table."""
        schema = self._get_schema(command.schema_name)
        
        # Build a simple SELECT * query and use the data repository
        sql = f'SELECT * FROM "{schema.table_name}";'
        result = await self.data_repository.query_arrow(sql)
        
        logger.info(f"Bulk read to Arrow completed: {result.num_rows} records from '{schema.table_name}'.")
        return result
    
    def _get_schema(self, schema_name: str) -> Schema:
        """Get schema from repository with proper error handling (synchronous)."""
        schema = self.schema_repository.get_schema_by_name(schema_name)
        if not schema:
            raise SchemaNotFoundException(f"Schema '{schema_name}' not found")
        return schema 