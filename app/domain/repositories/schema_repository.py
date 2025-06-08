# app/domain/repositories/schema_repository.py
from abc import ABC, abstractmethod
from typing import Optional, List
from app.domain.entities.schema import Schema

class ISchemaRepository(ABC):
    @abstractmethod
    async def get_schema_by_name(self, name: str) -> Optional[Schema]:
        pass

    @abstractmethod
    async def get_all_schemas(self) -> List[Schema]:
        pass