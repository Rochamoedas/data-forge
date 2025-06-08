from fastapi import APIRouter, HTTPException, status, Query, Depends
from fastapi.responses import StreamingResponse
from typing import List, Dict, Optional
from uuid import UUID
import json
import time

from app.application.dto.create_data_dto import (
    CreateDataRequest, 
    CreateBulkDataRequest, 
    CreateDataResponse, 
    CreateBulkDataResponse,
    DataRecordResponse
)
from app.application.dto.query_request_dto import (
    QueryDataRecordsResponse,
    CountDataRecordsResponse,
    DataRecordStreamResponse
)
from app.application.dto.query_dto import (
    DataQueryRequest,
    QueryFilter,
    QuerySort,
    QueryPagination,
    FilterOperator
)
from app.container.container import container
from app.domain.exceptions import SchemaNotFoundException, InvalidDataException
from app.config.logging_config import logger

router = APIRouter()

@router.post("/records", response_model=CreateDataResponse, status_code=status.HTTP_201_CREATED)
async def create_data_record(request: CreateDataRequest) -> CreateDataResponse:
    """
    Create a single data record in the specified schema/table
    """
    try:
        record = await container.create_data_record_use_case.execute(
            schema_name=request.schema_name,
            data=request.data
        )
        
        return CreateDataResponse(
            success=True,
            message=f"Record created successfully in {request.schema_name}",
            record=DataRecordResponse(
                id=record.id,
                schema_name=record.schema_name,
                data=record.data,
                created_at=record.created_at,
                version=record.version
            )
        )
    except SchemaNotFoundException as e:
        logger.error(f"Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidDataException as e:
        logger.error(f"Invalid data: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating record: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/records/bulk", response_model=CreateBulkDataResponse, status_code=status.HTTP_201_CREATED)
async def create_bulk_data_records(request: CreateBulkDataRequest) -> CreateBulkDataResponse:
    """
    Create multiple data records in bulk for fast operations
    """
    try:
        records = await container.create_bulk_data_records_use_case.execute(
            schema_name=request.schema_name,
            data_list=request.data
        )
        
        response_records = [
            DataRecordResponse(
                id=record.id,
                schema_name=record.schema_name,
                data=record.data,
                created_at=record.created_at,
                version=record.version
            )
            for record in records
        ]
        
        return CreateBulkDataResponse(
            success=True,
            message=f"Successfully created {len(records)} records in {request.schema_name}",
            records_created=len(records),
            records=response_records
        )
    except SchemaNotFoundException as e:
        logger.error(f"Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidDataException as e:
        logger.error(f"Invalid data: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating bulk records: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/{schema_name}", response_model=QueryDataRecordsResponse)
async def get_records_by_schema(
    schema_name: str,
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    size: int = Query(10, ge=1, le=1000, description="Number of records per page"),
    filters: Optional[str] = Query(None, description="JSON string of filters array"),
    sort: Optional[str] = Query(None, description="JSON string of sort array"),
) -> QueryDataRecordsResponse:
    """
    Get paginated records from a specific schema/table with filtering and sorting
    
    Example filters: [{"field": "field_name", "operator": "eq", "value": "test"}]
    Example sort: [{"field": "created_at", "order": "desc"}]
    """
    start_time = time.perf_counter()
    try:
        # Parse filters and sort from JSON strings
        parsed_filters = []
        if filters:
            filter_data = json.loads(filters)
            for f in filter_data:
                parsed_filters.append(QueryFilter(
                    field=f["field"],
                    operator=FilterOperator(f["operator"]),
                    value=f.get("value")
                ))
        
        parsed_sort = []
        if sort:
            sort_data = json.loads(sort)
            for s in sort_data:
                parsed_sort.append(QuerySort(
                    field=s["field"],
                    order=s.get("order", "asc")
                ))
        
        query_request = DataQueryRequest(
            filters=parsed_filters if parsed_filters else None,
            sort=parsed_sort if parsed_sort else None,
            pagination=QueryPagination(page=page, size=size)
        )
        
        result = await container.query_data_records_use_case.execute(schema_name, query_request)
        
        # Convert result to response format
        response_data = {
            "items": [
                {
                    "id": str(record.id),
                    "schema_name": record.schema_name,
                    "data": record.data,
                    "created_at": record.created_at.isoformat(),
                    "version": record.version
                }
                for record in result.items
            ],
            "total": result.total,
            "page": result.page,
            "size": result.size,
            "has_next": result.has_next,
            "has_previous": result.has_previous
        }
        
        duration = time.perf_counter() - start_time
        return QueryDataRecordsResponse(
            message=f"Successfully retrieved {len(result.items)} records from {schema_name}",
            schema_name=schema_name,
            data=response_data,
            execution_time_ms=duration * 1000
        )
    except SchemaNotFoundException as e:
        logger.error(f"Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in query parameters: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON in filters or sort parameters")
    except Exception as e:
        logger.error(f"Error retrieving records: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/{schema_name}/stream")
async def stream_records_by_schema(
    schema_name: str,
    filters: Optional[str] = Query(None, description="JSON string of filters array"),
    sort: Optional[str] = Query(None, description="JSON string of sort array"),
    limit: Optional[int] = Query(None, ge=1, le=10000, description="Maximum number of records to stream"),
):
    """
    Stream records from a specific schema/table for high-performance data access
    Returns NDJSON (newline-delimited JSON) for efficient streaming
    """
    try:
        # Parse filters and sort from JSON strings
        parsed_filters = []
        if filters:
            filter_data = json.loads(filters)
            for f in filter_data:
                parsed_filters.append(QueryFilter(
                    field=f["field"],
                    operator=FilterOperator(f["operator"]),
                    value=f.get("value")
                ))
        
        parsed_sort = []
        if sort:
            sort_data = json.loads(sort)
            for s in sort_data:
                parsed_sort.append(QuerySort(
                    field=s["field"],
                    order=s.get("order", "asc")
                ))
        
        # Use a high page size for streaming, with optional limit
        pagination = QueryPagination(page=1, size=limit or 1000)
        
        query_request = DataQueryRequest(
            filters=parsed_filters if parsed_filters else None,
            sort=parsed_sort if parsed_sort else None,
            pagination=pagination
        )
        
        async def generate_stream():
            record_count = 0
            async for record in container.stream_data_records_use_case.execute(schema_name, query_request):
                if limit and record_count >= limit:
                    break
                
                record_data = {
                    "id": str(record.id),
                    "schema_name": record.schema_name,
                    "data": record.data,
                    "created_at": record.created_at.isoformat(),
                    "version": record.version
                }
                yield json.dumps(record_data) + "\n"
                record_count += 1
        
        return StreamingResponse(
            generate_stream(),
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": f"attachment; filename={schema_name}_data.ndjson"
            }
        )
    except SchemaNotFoundException as e:
        logger.error(f"Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in query parameters: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON in filters or sort parameters")
    except Exception as e:
        logger.error(f"Error streaming records: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/{schema_name}/count", response_model=CountDataRecordsResponse)
async def count_records_by_schema(
    schema_name: str,
    filters: Optional[str] = Query(None, description="JSON string of filters array"),
) -> CountDataRecordsResponse:
    """
    Get the count of records in a specific schema/table with optional filtering
    """
    start_time = time.perf_counter()
    try:
        # Parse filters from JSON string
        parsed_filters = []
        if filters:
            filter_data = json.loads(filters)
            for f in filter_data:
                parsed_filters.append(QueryFilter(
                    field=f["field"],
                    operator=FilterOperator(f["operator"]),
                    value=f.get("value")
                ))
        
        query_request = DataQueryRequest(
            filters=parsed_filters if parsed_filters else None,
            sort=None,
            pagination=QueryPagination(page=1, size=1)  # Minimal pagination for count
        )
        
        count = await container.count_data_records_use_case.execute(schema_name, query_request)
        
        duration = time.perf_counter() - start_time
        return CountDataRecordsResponse(
            message=f"Successfully counted records in {schema_name}",
            schema_name=schema_name,
            count=count,
            execution_time_ms=duration * 1000
        )
    except SchemaNotFoundException as e:
        logger.error(f"Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in query parameters: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON in filters parameters")
    except Exception as e:
        logger.error(f"Error counting records: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/{schema_name}/{record_id}", response_model=DataRecordResponse)
async def get_record_by_id(schema_name: str, record_id: UUID) -> DataRecordResponse:
    """
    Get a specific record by ID from a schema/table
    """
    try:
        record = await container.get_data_record_use_case.execute(
            schema_name=schema_name,
            record_id=record_id
        )
        
        return DataRecordResponse(
            id=record.id,
            schema_name=record.schema_name,
            data=record.data,
            created_at=record.created_at,
            version=record.version
        )
    except SchemaNotFoundException as e:
        logger.error(f"Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error retrieving record: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/schemas")
async def get_available_schemas() -> List[Dict]:
    """
    Get list of available schemas/tables
    """
    try:
        schemas = await container.schema_repository.get_all_schemas()
        return [
            {
                "name": schema.name,
                "description": schema.description,
                "table_name": schema.table_name,
                "properties": [
                    {
                        "name": prop.name, 
                        "type": prop.type, 
                        "required": prop.required
                    } 
                    for prop in schema.properties
                ]
            }
            for schema in schemas
        ]
    except Exception as e:
        logger.error(f"Error retrieving schemas: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
