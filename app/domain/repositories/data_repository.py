# app/domain/repositories/data_repository.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from uuid import UUID
from app.domain.entities.schema import Schema
from app.domain.entities.data_record import DataRecord

class IDataRepository(ABC):
    """
    Abstract Base Class (ABC) defining the contract for generic data operations.
    This is a 'Port' in Hexagonal Architecture.
    """
    @abstractmethod
    def create(self, schema: Schema, data: Dict[str, Any]) -> DataRecord:
        """
        Creates a new data record for a given schema.
        The 'data' dictionary must conform to the schema's fields.
        """
        pass

    @abstractmethod
    def get_by_id(self, schema: Schema, record_id: UUID) -> Optional[DataRecord]:
        """
        Retrieves a single data record by its ID for a given schema.
        Returns None if the record is not found.
        """
        pass
    # Future: Add update and delete methods here in Sprint 2