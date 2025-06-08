# app/domain/repositories/data_repository.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, AsyncIterator
from uuid import UUID
from app.domain.entities.data_record import DataRecord
from app.domain.entities.schema import Schema
from app.application.dto.query_dto import DataQueryRequest
from app.application.dto.data_dto import PaginatedResponse

class IDataRepository(ABC):
    @abstractmethod
    async def create(self, schema: Schema, data: Dict[str, Any]) -> DataRecord:
        pass

    @abstractmethod
    async def create_batch(self, schema: Schema, records: List[DataRecord]) -> None:
        pass

    @abstractmethod
    async def get_by_id(self, schema: Schema, record_id: UUID) -> Optional[DataRecord]:
        pass

    @abstractmethod
    async def get_all(self, schema: Schema, query_request: DataQueryRequest) -> PaginatedResponse[DataRecord]:
        pass

    @abstractmethod
    async def stream_query_results(self, schema: Schema, query_request: DataQueryRequest) -> AsyncIterator[DataRecord]:
        pass

    @abstractmethod
    async def count_all(self, schema: Schema, query_request: DataQueryRequest) -> int:
        pass