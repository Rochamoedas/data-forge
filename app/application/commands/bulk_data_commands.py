"""
CQRS Commands for Bulk Data Operations

Following CQRS and DDD principles:
- Commands represent intentions to change state
- Immutable command objects
- Clear separation of concerns
- Schema-driven validation
"""

from dataclasses import dataclass
import pyarrow as pa


@dataclass(frozen=True)
class BulkInsertFromArrowTableCommand:
    """Command to insert bulk data from Arrow Table"""
    schema_name: str
    arrow_table: pa.Table
    
    def __post_init__(self):
        if not self.schema_name:
            raise ValueError("Schema name is required")


@dataclass(frozen=True)
class BulkReadToArrowCommand:
    """Command to read bulk data as Arrow Table"""
    schema_name: str
    
    def __post_init__(self):
        if not self.schema_name:
            raise ValueError("Schema name is required")


@dataclass(frozen=True)
class BulkReadToDataFrameCommand:
    """Command to read bulk data as pandas DataFrame"""
    schema_name: str
    
    def __post_init__(self):
        if not self.schema_name:
            raise ValueError("Schema name is required") 