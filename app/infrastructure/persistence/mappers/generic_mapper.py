# app/infrastructure/persistence/mappers/generic_mapper.py
from typing import Dict, Any, Optional
from uuid import UUID
from app.domain.entities.data_record import DataRecord

def map_dict_to_data_record(data: Dict[str, Any], schema_name: str, record_id: Optional[UUID] = None) -> DataRecord:
    """
    Maps a dictionary of data to a DataRecord entity.
    """
    return DataRecord(id=record_id, schema_name=schema_name, data=data)

def map_data_record_to_dict(record: DataRecord) -> Dict[str, Any]:
    """
    Maps a DataRecord entity back to a dictionary for persistence or API response.
    Includes the 'id' of the record in the dictionary.
    """
    return {"id": str(record.id), **record.data}