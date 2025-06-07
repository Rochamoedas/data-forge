# app/domain/entities/schema.py
from typing import List
from pydantic import BaseModel, Field

class SchemaField(BaseModel):
    """Represents a single field (column) within a schema."""
    name: str = Field(..., description="The name of the field (e.g., 'user_id', 'product_name').")
    type: str = Field(..., description="The data type of the field (e.g., 'STRING', 'INTEGER', 'BOOLEAN', 'TIMESTAMP').")
    # Future: Add more attributes like 'is_nullable', 'default_value', 'is_primary_key'

class Schema(BaseModel):
    """Represents the definition of a generic data table or dataset."""
    name: str = Field(..., description="The unique name of the schema (e.g., 'users', 'products').")
    fields: List[SchemaField] = Field(..., description="A list of fields that define the schema's structure.")

    def get_field_names(self) -> List[str]:
        """Returns a list of all field names in the schema."""
        return [field.name for field in self.fields]