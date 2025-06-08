from typing import Optional, List, Any, Dict
from pydantic import BaseModel, Field, validator
from enum import Enum
from app.application.dto.query_dto import FilterOperator, QueryFilter, QuerySort, QueryPagination
from app.config.api_limits import api_limits

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
    size: int = Field(
        api_limits.DEFAULT_PAGE_SIZE,
        ge=api_limits.MIN_PAGE_SIZE,
        le=api_limits.MAX_PAGE_SIZE,
        description="Number of records per page"
    )
    
    # Filtering
    filters: Optional[List[QueryRequestFilter]] = Field(
        None,
        description="List of filters to apply",
        max_length=api_limits.MAX_FILTER_CONDITIONS
    )
    
    # Sorting
    sort: Optional[List[QueryRequestSort]] = Field(
        None,
        description="List of sort specifications",
        max_length=api_limits.MAX_SORT_FIELDS
    )
    
    # Full-text search (optional)
    search: Optional[str] = Field(None, description="Full-text search query")

class StreamDataRecordsRequest(BaseModel):
    """Request model for streaming data records - no pagination"""
    schema_name: str = Field(..., description="Name of the schema/table to stream")
    
    # Filtering
    filters: Optional[List[QueryRequestFilter]] = Field(
        None,
        description="List of filters to apply",
        max_length=api_limits.MAX_FILTER_CONDITIONS
    )
    
    # Sorting
    sort: Optional[List[QueryRequestSort]] = Field(
        None,
        description="List of sort specifications",
        max_length=api_limits.MAX_SORT_FIELDS
    )
    
    # Limit for streaming (optional safety)
    limit: Optional[int] = Field(
        None,
        ge=api_limits.MIN_STREAM_LIMIT,
        le=api_limits.MAX_STREAM_LIMIT,
        description="Maximum number of records to stream"
    )

class CountDataRecordsRequest(BaseModel):
    """Request model for counting data records"""
    schema_name: str = Field(..., description="Name of the schema/table to count")
    
    # Filtering
    filters: Optional[List[QueryRequestFilter]] = Field(
        None,
        description="List of filters to apply",
        max_length=api_limits.MAX_FILTER_CONDITIONS
    )

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