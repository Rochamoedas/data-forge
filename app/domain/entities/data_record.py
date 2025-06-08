from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from uuid import UUID, uuid4
from datetime import datetime

class BaseEntity(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    version: int = Field(default=1)

    class Config:
        validate_assignment = True
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }

class DataRecord(BaseEntity):
    schema_name: str
    data: Dict[str, Any] 