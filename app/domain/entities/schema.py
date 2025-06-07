# app/domain/entities/schema.py
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from app.domain.exceptions import InvalidDataException

class SchemaField(BaseModel):
    """Represents a single field (column) within a schema."""
    name: str = Field(..., description="The name of the field")
    type: str = Field(..., description="The data type of the field")
    is_required: bool = Field(default=False, description="Whether this field is required")
    
    # Enhanced validation attributes for Sprint 2
    min_length: int = Field(default=None, description="Minimum length for string fields")
    max_length: int = Field(default=None, description="Maximum length for string fields")
    pattern: str = Field(default=None, description="Regex pattern for string validation")

class Schema(BaseModel):
    """Represents the definition of a generic data table or dataset."""
    name: str = Field(..., description="The unique name of the schema")
    fields: List[SchemaField] = Field(..., description="List of fields defining schema structure")

    def get_field_names(self) -> List[str]:
        """Returns a list of all field names in the schema."""
        return [field.name for field in self.fields]
    
    def validate_data(self, data: Dict[str, Any]) -> None:
        """
        Validates a dictionary of data against the schema's defined fields.
        Raises InvalidDataException if validation fails.
        """
        required_fields = {field.name for field in self.fields if field.is_required}
        defined_fields_map = {field.name: field for field in self.fields}

        # Check for missing required fields
        missing_fields = required_fields - set(data.keys())
        if missing_fields:
            raise InvalidDataException(f"Missing required fields: {', '.join(missing_fields)}")

        # Check for unknown fields and perform type validation
        for key, value in data.items():
            if key == 'id':  # Skip validation for auto-generated ID field
                continue
                
            if key not in defined_fields_map:
                raise InvalidDataException(f"Unknown field: '{key}'")

            field_def = defined_fields_map[key]
            self._validate_field_value(key, value, field_def)

    def _validate_field_value(self, field_name: str, value: Any, field_def: SchemaField) -> None:
        """Internal method to validate a single field value against its definition."""
        field_type = field_def.type.upper()
        
        # Basic type validation
        if field_type == "STRING" and not isinstance(value, str):
            raise InvalidDataException(f"Field '{field_name}' expects string, got {type(value).__name__}")
        elif field_type == "INTEGER" and not isinstance(value, int):
            raise InvalidDataException(f"Field '{field_name}' expects integer, got {type(value).__name__}")
        elif field_type == "BOOLEAN" and not isinstance(value, bool):
            raise InvalidDataException(f"Field '{field_name}' expects boolean, got {type(value).__name__}")
        elif field_type == "DOUBLE" and not isinstance(value, (int, float)):
            raise InvalidDataException(f"Field '{field_name}' expects number, got {type(value).__name__}")
        
        # String-specific validation
        if field_type == "STRING" and isinstance(value, str):
            if field_def.min_length and len(value) < field_def.min_length:
                raise InvalidDataException(f"Field '{field_name}' minimum length is {field_def.min_length}")
            if field_def.max_length and len(value) > field_def.max_length:
                raise InvalidDataException(f"Field '{field_name}' maximum length is {field_def.max_length}")