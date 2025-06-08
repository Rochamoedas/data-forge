from pydantic import BaseModel, Field
from typing import Optional, List, Any
from enum import Enum
from app.config.api_limits import api_limits

class FilterOperator(str, Enum):
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    LIKE = "like"
    ILIKE = "ilike"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"

class QueryFilter(BaseModel):
    field: str
    operator: FilterOperator
    value: Any = None

class QuerySort(BaseModel):
    field: str
    order: str = "asc"

class QueryPagination(BaseModel):
    page: int = Field(1, ge=1)
    size: int = Field(
        api_limits.DEFAULT_PAGE_SIZE,
        ge=api_limits.MIN_PAGE_SIZE,
        le=api_limits.MAX_PAGE_SIZE
    )

class DataQueryRequest(BaseModel):
    filters: Optional[List[QueryFilter]] = None
    sort: Optional[List[QuerySort]] = None
    pagination: QueryPagination = Field(default_factory=QueryPagination) 