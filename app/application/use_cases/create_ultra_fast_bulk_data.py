"""
🚀 Ultra-Fast Bulk Data Creation Use Case

Following Hexagonal Architecture and DDD principles:
- Pure business logic in application layer
- No infrastructure concerns
- No test data generation (belongs in tests)
- Uses CQRS commands and handlers
- Performance monitoring is optional
"""

from typing import List, Dict, Any, Optional
import pandas as pd
import pyarrow as pa

from app.application.commands.bulk_data_commands import (
    BulkInsertFromArrowTableCommand,
    BulkReadToArrowCommand,
)
from app.application.command_handlers.bulk_data_command_handlers import BulkDataCommandHandler


class CreateUltraFastBulkDataUseCase:
    """Use case for ultra-fast bulk data operations."""

    def __init__(self, command_handler: BulkDataCommandHandler):
        self.command_handler = command_handler

    async def execute_from_arrow_table(
        self, schema_name: str, arrow_table: pa.Table
    ) -> None:
        """Execute bulk insert from an Arrow Table."""
        command = BulkInsertFromArrowTableCommand(
            schema_name=schema_name, arrow_table=arrow_table
        )
        await self.command_handler.handle_bulk_insert_from_arrow_table(command)

    async def read_to_arrow_table(self, schema_name: str) -> pa.Table:
        """Read data into an Arrow Table."""
        command = BulkReadToArrowCommand(schema_name=schema_name)
        return await self.command_handler.handle_bulk_read_to_arrow(command) 