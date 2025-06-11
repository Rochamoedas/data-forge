from abc import ABC, abstractmethod
from typing import List, Optional
from app.domain.entities.schema import Schema

class ISchemaRepository(ABC):
    """
    Interface for a repository that handles schema metadata.
    """

    @abstractmethod
    def get_schema_by_name(self, schema_name: str) -> Optional[Schema]:
        """
        Retrieves a schema by its name.
        """
        pass

    @abstractmethod
    def get_all_schemas(self) -> List[Schema]:
        """
        Retrieves all available schemas.
        """
        pass 