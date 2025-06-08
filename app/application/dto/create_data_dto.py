from typing import Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime
from uuid import UUID

class CreateDataRequest(BaseModel):
    """Request model for creating a single data record"""
    schema_name: str = Field(..., description="Name of the schema/table")
    data: Dict[str, Any] = Field(..., description="Data payload to be stored")

class CreateBulkDataRequest(BaseModel):
    """Request model for creating multiple data records in bulk"""
    schema_name: str = Field(..., description="Name of the schema/table")
    data: List[Dict[str, Any]] = Field(..., description="List of data payloads to be stored", min_items=1)

class DataRecordResponse(BaseModel):
    """Response model for a single data record"""
    id: UUID
    schema_name: str
    data: Dict[str, Any]
    created_at: datetime
    version: int

class CreateDataResponse(BaseModel):
    """Response model for single data creation"""
    success: bool
    message: str
    record: DataRecordResponse

class CreateBulkDataResponse(BaseModel):
    """Response model for bulk data creation"""
    success: bool
    message: str
    records_created: int
    records: List[DataRecordResponse] 