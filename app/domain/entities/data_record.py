# app/domain/entities/data_record.py
from typing import Dict, Any
from uuid import UUID, uuid4
from pydantic import BaseModel, Field

class DataRecord(BaseModel):
    """Represents a single generic data record (row) conforming to a specific schema."""
    id: UUID = Field(default_factory=uuid4, description="Unique identifier for the data record.")
    schema_name: str = Field(..., description="The name of the schema this record conforms to.")
    data: Dict[str, Any] = Field(..., description="The actual data of the record, as a dictionary of field_name: value.")

    def get_value(self, field_name: str) -> Any:
        """Retrieves the value of a specific field from the record's data."""
        return self.data.get(field_name)