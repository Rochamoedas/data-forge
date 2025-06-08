from typing import Optional, List, Any, Dict
from pydantic import BaseModel, Field, validator
from enum import Enum
from app.application.dto.query_dto import FilterOperator, QueryFilter, QuerySort, QueryPagination

class QueryRequestFilter(BaseModel):
    """Filter for query requests - simplified for URL parameters"""
    field: str = Field(..., description="Field name to filter on")
    operator: FilterOperator = Field(..., description="Filter operator")
    value: Optional[str] = Field(None, description="Filter value (will be parsed based on field type)")

class QueryRequestSort(BaseModel):
    """Sort specification for query requests"""
    field: str = Field(..., description="Field name to sort by")
    order: str = Field("asc", pattern="^(asc|desc)$", description="Sort order: asc or desc")

class QueryDataRecordsRequest(BaseModel):
    """Request model for querying data records with filters, sorting, and pagination"""
    schema_name: str = Field(..., description="Name of the schema/table to query")
    
    # Pagination
    page: int = Field(1, ge=1, description="Page number (1-based)")
    size: int = Field(10, ge=1, le=1000, description="Number of records per page")
    
    # Filtering
    filters: Optional[List[QueryRequestFilter]] = Field(None, description="List of filters to apply")
    
    # Sorting
    sort: Optional[List[QueryRequestSort]] = Field(None, description="List of sort specifications")
    
    # Full-text search (optional)
    search: Optional[str] = Field(None, description="Full-text search query")

class StreamDataRecordsRequest(BaseModel):
    """Request model for streaming data records - no pagination"""
    schema_name: str = Field(..., description="Name of the schema/table to stream")
    
    # Filtering
    filters: Optional[List[QueryRequestFilter]] = Field(None, description="List of filters to apply")
    
    # Sorting
    sort: Optional[List[QueryRequestSort]] = Field(None, description="List of sort specifications")
    
    # Limit for streaming (optional safety)
    limit: Optional[int] = Field(None, ge=1, le=10000, description="Maximum number of records to stream")

class CountDataRecordsRequest(BaseModel):
    """Request model for counting data records"""
    schema_name: str = Field(..., description="Name of the schema/table to count")
    
    # Filtering
    filters: Optional[List[QueryRequestFilter]] = Field(None, description="List of filters to apply")

# Response DTOs
class QueryDataRecordsResponse(BaseModel):
    """Response model for paginated query results"""
    success: bool = True
    message: str
    schema_name: str
    data: Dict[str, Any]  # This will contain the PaginatedResponse data
    execution_time_ms: float

class CountDataRecordsResponse(BaseModel):
    """Response model for count results"""
    success: bool = True
    message: str
    schema_name: str
    count: int
    execution_time_ms: float

class DataRecordStreamResponse(BaseModel):
    """Response model for individual streamed records"""
    id: str
    schema_name: str
    data: Dict[str, Any]
    created_at: str
    version: int 