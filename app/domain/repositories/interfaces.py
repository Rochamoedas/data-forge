from abc import ABC, abstractmethod
from typing import Optional, List
from app.domain.entities.schema import Schema

class ISchemaRepository(ABC):
    @abstractmethod
    async def initialize(self):
        pass

    @abstractmethod
    async def get_schema_by_name(self, name: str) -> Optional[Schema]:
        pass

    @abstractmethod
    async def get_all_schemas(self) -> List[Schema]:
        pass

class IArrowBulkOperations(ABC):
    
    @abstractmethod
    async def bulk_insert_from_dataframe(self, schema: Schema, dataframe: 'pd.DataFrame') -> None:
        """Bulk insert from a pandas DataFrame"""
        pass
    
    @abstractmethod
    async def bulk_insert_from_arrow_table(self, schema: Schema, arrow_table: 'pa.Table') -> None:
        """Bulk insert from a PyArrow Table"""
        pass
        
    @abstractmethod
    async def bulk_read_to_arrow_table(self, schema: Schema) -> 'pa.Table':
        """Bulk read to a PyArrow Table"""
        pass
        
    @abstractmethod
    async def bulk_read_to_dataframe(self, schema: Schema) -> 'pd.DataFrame':
        """Bulk read to a pandas DataFrame"""
        pass 