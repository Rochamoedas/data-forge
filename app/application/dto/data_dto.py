from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional, Generic, TypeVar
from uuid import UUID
from datetime import datetime

T = TypeVar('T')

class CreateDataRecordRequest(BaseModel):
    data: Dict[str, Any]

class DataRecordResponse(BaseModel):
    id: UUID
    schema_name: str
    data: Dict[str, Any]
    created_at: datetime
    version: int

class ErrorResponse(BaseModel):
    detail: str

class PaginatedResponse(BaseModel, Generic[T]):
    items: List[T]
    total: int
    page: int
    size: int
    has_next: bool
    has_previous: bool 