from pydantic import BaseModel, Field, ConfigDict
from typing import Dict, Any, Optional, Union
from uuid import UUID, uuid4
from datetime import datetime

class BaseEntity(BaseModel):
    model_config = ConfigDict(
        validate_assignment=True,
        use_enum_values=True,
        # Remove json_encoders as it's deprecated - Pydantic v2 handles UUID and datetime automatically
    )
    
    id: UUID = Field(default_factory=uuid4)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    version: int = Field(default=1)

class DataRecord(BaseEntity):
    schema_name: str
    data: Dict[str, Any]
    # Optional composite key for business logic optimization
    composite_key: Optional[Dict[str, Union[str, int]]] = None
    
    @property
    def composite_id(self) -> Optional[str]:
        """Generate a string representation of the composite key for API responses"""
        if not self.composite_key:
            return None
        key_parts = [f"{k}={v}" for k, v in sorted(self.composite_key.items())]
        return "&".join(key_parts)
    
    @classmethod
    def parse_composite_id(cls, composite_id: str) -> Dict[str, str]:
        """Parse composite_id string back to dictionary"""
        return dict(param.split('=', 1) for param in composite_id.split('&')) 