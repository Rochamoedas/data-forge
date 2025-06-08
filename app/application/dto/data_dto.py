# app/application/dto/_module_marker.py
from typing import Dict, Any, List, Optional
from uuid import UUID
from pydantic import BaseModel, Field

class CreateDataRecordRequest(BaseModel):
    """DTO for creating a new data record."""
    data: Dict[str, Any] = Field(
        ..., 
        description="The actual data of the record as a key-value dictionary",
        example={"field_name": "example_value", "field_code": 123}
    )
    # Optional ID for cases where client pre-generates UUIDs, though system can generate it.
    id: Optional[UUID] = Field(None, description="Optional: Unique identifier for the data record. If not provided, one will be generated.")


class CreateManyDataRecordsRequest(BaseModel):
    """DTO for creating multiple data records in a single batch."""
    records: List[CreateDataRecordRequest] = Field(
        ...,
        description="A list of data records to be created in a batch."
    )

class DataRecordResponse(BaseModel):
    """DTO for responding with a single data record."""
    id: UUID = Field(..., description="Unique identifier of the data record")
    schema_name: str = Field(..., description="The name of the schema this record conforms to")
    data: Dict[str, Any] = Field(..., description="The actual data of the record")
    
    model_config = {"from_attributes": True}

class DataRecordsResponse(BaseModel):
    """DTO for responding with multiple data records (e.g., from a batch read)."""
    records: List[DataRecordResponse] = Field(..., description="A list of data records.")

class BulkOperationResult(BaseModel):
    """DTO for summarizing the result of a bulk data operation."""
    success_count: int = Field(..., description="Number of records successfully processed.")
    error_count: int = Field(..., description="Number of records that failed processing.")
    errors: List[str] = Field(..., description="List of error messages for failed records.")

class ErrorResponse(BaseModel):
    """Standard error response format."""
    detail: str = Field(..., description="Error message")
    error_type: str = Field(..., description="Type of error")
