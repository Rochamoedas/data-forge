"""
CQRS Commands for Bulk Data Operations

Following CQRS and DDD principles:
- Commands represent intentions to change state
- Immutable command objects
- Clear separation of concerns
- Schema-driven validation
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd
import pyarrow as pa


@dataclass(frozen=True)
class BulkInsertFromDataFrameCommand:
    """Command to insert bulk data from pandas DataFrame"""
    schema_name: str
    dataframe: pd.DataFrame
    
    def __post_init__(self):
        if self.dataframe.empty:
            raise ValueError("DataFrame cannot be empty")
        if not self.schema_name:
            raise ValueError("Schema name is required")


@dataclass(frozen=True)
class BulkInsertFromArrowTableCommand:
    """Command to insert bulk data from Arrow Table"""
    schema_name: str
    arrow_table: pa.Table
    
    def __post_init__(self):
        if self.arrow_table.num_rows == 0:
            raise ValueError("Arrow table cannot be empty")
        if not self.schema_name:
            raise ValueError("Schema name is required")


@dataclass(frozen=True)
class BulkInsertFromDictListCommand:
    """Command to insert bulk data from list of dictionaries"""
    schema_name: str
    data: List[Dict[str, Any]]
    
    def __post_init__(self):
        if not self.data:
            raise ValueError("Data list cannot be empty")
        if not self.schema_name:
            raise ValueError("Schema name is required")


@dataclass(frozen=True)
class BulkExportToArrowCommand:
    """Command to export data to Arrow format"""
    schema_name: str
    output_format: str = "arrow"  # "arrow" or "parquet"
    
    def __post_init__(self):
        if not self.schema_name:
            raise ValueError("Schema name is required")
        if self.output_format not in ["arrow", "parquet"]:
            raise ValueError("Output format must be 'arrow' or 'parquet'")


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