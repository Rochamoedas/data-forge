# app/domain/entities/schema.py
from typing import List, Dict, Any, Literal, Optional
from pydantic import BaseModel, Field
from app.domain.exceptions import InvalidDataException

class SchemaProperty(BaseModel):
    name: str
    type: Literal["string", "integer", "number", "boolean", "array", "object"]
    db_type: str
    required: bool = False
    default: Any = None
    primary_key: bool = False  # New field to mark primary key components

class Schema(BaseModel):
    name: str
    description: str
    table_name: str
    properties: List[SchemaProperty]
    primary_key: Optional[List[str]] = None  # List of field names that form the composite key

    def validate_data(self, data: Dict[str, Any]):
        missing_required = [prop.name for prop in self.properties if prop.required and prop.name not in data]
        if missing_required:
            raise InvalidDataException(f"Missing required fields: {', '.join(missing_required)}")

        for prop in self.properties:
            if prop.name in data:
                value = data[prop.name]
                if prop.type == "string" and not isinstance(value, str):
                    raise InvalidDataException(f"Field '{prop.name}' expected string, got {type(value).__name__}")
                if prop.type == "integer" and not isinstance(value, int):
                    raise InvalidDataException(f"Field '{prop.name}' expected integer, got {type(value).__name__}")
                if prop.type == "number" and not isinstance(value, (int, float)):
                    raise InvalidDataException(f"Field '{prop.name}' expected number, got {type(value).__name__}")
                if prop.type == "boolean" and not isinstance(value, bool):
                    raise InvalidDataException(f"Field '{prop.name}' expected boolean, got {type(value).__name__}")
                if prop.type == "array" and not isinstance(value, list):
                    raise InvalidDataException(f"Field '{prop.name}' expected array, got {type(value).__name__}")
                if prop.type == "object" and not isinstance(value, dict):
                    raise InvalidDataException(f"Field '{prop.name}' expected object, got {type(value).__name__}")
    
    def get_composite_key_from_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract composite key values from data based on schema definition"""
        if not self.primary_key:
            return None
        
        composite_key = {}
        missing_pk_fields = []
        
        for key_field in self.primary_key:
            if key_field in data:
                composite_key[key_field] = data[key_field]
            else:
                # Check if it's a required primary key field
                prop = next((p for p in self.properties if p.name == key_field), None)
                if prop and prop.primary_key:
                    missing_pk_fields.append(key_field)
        
        if missing_pk_fields:
            raise InvalidDataException(f"Primary key field(s) {', '.join(missing_pk_fields)} are required but missing")
        
        return composite_key if composite_key else None