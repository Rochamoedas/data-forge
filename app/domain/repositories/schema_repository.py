# app/domain/repositories/schema_repository.py
from abc import ABC, abstractmethod
from typing import Optional
from app.domain.entities.schema import Schema

class ISchemaRepository(ABC):
    """
    Abstract Base Class (ABC) defining the contract for retrieving schema definitions.
    This is a 'Port' in Hexagonal Architecture.
    """
    @abstractmethod
    def get_schema_by_name(self, schema_name: str) -> Optional[Schema]:
        """
        Retrieves a schema definition by its unique name.
        Returns None if the schema is not found.
        """
        pass