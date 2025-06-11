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
    BulkInsertFromDataFrameCommand,
    BulkInsertFromArrowTableCommand,
    BulkInsertFromDictListCommand,
    BulkReadToArrowCommand,
    BulkReadToDataFrameCommand,
)
from app.application.command_handlers.bulk_data_command_handlers import BulkDataCommandHandler


class CreateUltraFastBulkDataUseCase:
    """Use case for ultra-fast bulk data operations."""

    def __init__(self, command_handler: BulkDataCommandHandler):
        self.command_handler = command_handler

    async def execute_from_dataframe(
        self, schema_name: str, dataframe: pd.DataFrame
    ) -> None:
        """Execute bulk insert from a pandas DataFrame."""
        command = BulkInsertFromDataFrameCommand(
            schema_name=schema_name, dataframe=dataframe
        )
        await self.command_handler.handle_bulk_insert_from_dataframe(command)

    async def execute_from_arrow_table(
        self, schema_name: str, arrow_table: pa.Table
    ) -> None:
        """Execute bulk insert from an Arrow Table."""
        command = BulkInsertFromArrowTableCommand(
            schema_name=schema_name, arrow_table=arrow_table
        )
        await self.command_handler.handle_bulk_insert_from_arrow_table(command)

    async def execute_from_dict_list(
        self, schema_name: str, data: List[Dict[str, Any]]
    ) -> None:
        """Execute bulk insert from a list of dictionaries."""
        command = BulkInsertFromDictListCommand(schema_name=schema_name, data=data)
        await self.command_handler.handle_bulk_insert_from_dict_list(command)

    async def read_to_arrow_table(self, schema_name: str) -> pa.Table:
        """Read data into an Arrow Table."""
        command = BulkReadToArrowCommand(schema_name=schema_name)
        return await self.command_handler.handle_bulk_read_to_arrow(command)

    async def read_to_dataframe(self, schema_name: str) -> pd.DataFrame:
        """Read data into a pandas DataFrame."""
        command = BulkReadToDataFrameCommand(schema_name=schema_name)
        return await self.command_handler.handle_bulk_read_to_dataframe(command) 