# app/domain/entities/schema.py
from typing import List, Dict, Any, Literal
from pydantic import BaseModel, Field
from app.domain.exceptions import InvalidDataException

class SchemaProperty(BaseModel):
    name: str
    type: Literal["string", "integer", "number", "boolean", "array", "object"]
    db_type: str
    required: bool = False
    default: Any = None

class Schema(BaseModel):
    name: str
    description: str
    table_name: str
    properties: List[SchemaProperty]

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