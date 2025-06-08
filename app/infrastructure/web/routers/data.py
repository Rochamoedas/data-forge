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
from app.domain.exceptions import SchemaNotFoundException, InvalidDataException, RecordNotFoundException
from app.config.logging_config import logger
from app.config.api_limits import api_limits
from app.infrastructure.web.dependencies.profiling import profiling_decorator

router = APIRouter()

@router.post("/records", response_model=CreateDataResponse, status_code=status.HTTP_201_CREATED)
@profiling_decorator
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
                version=record.version,
                composite_id=record.composite_id
            )
        )
    except SchemaNotFoundException as e:
        logger.warning(f"Client error - Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidDataException as e:
        logger.warning(f"Client error - Invalid data: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Internal server error creating record: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/records/bulk", response_model=CreateBulkDataResponse, status_code=status.HTTP_201_CREATED)
@profiling_decorator
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
                version=record.version,
                composite_id=record.composite_id
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
        logger.warning(f"Client error - Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except InvalidDataException as e:
        logger.warning(f"Client error - Invalid data: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Check if it's a constraint violation (duplicate key) - common in bulk operations
        if "Constraint Error" in str(e) and "Duplicate key" in str(e):
            logger.warning(f"Bulk operation constraint violation: {e}")
            raise HTTPException(status_code=409, detail=f"Duplicate key constraint violation: {str(e)}")
        else:
            logger.error(f"Internal server error creating bulk records: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/{schema_name}", response_model=QueryDataRecordsResponse)
@profiling_decorator
async def get_records_by_schema(
    schema_name: str,
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    size: int = Query(api_limits.DEFAULT_PAGE_SIZE, ge=api_limits.MIN_PAGE_SIZE, le=api_limits.MAX_PAGE_SIZE, description="Number of records per page"),
    filters: Optional[str] = Query(None, description="JSON string of filters array"),
    sort: Optional[str] = Query(None, description="JSON string of sort array"),
) -> QueryDataRecordsResponse:
    """
    Get paginated records from a specific schema/table with filtering and sorting
    
    Example filters: [{"field": "field_name", "operator": "eq", "value": "test"}]
    Example sort: [{"field": "created_at", "order": "desc"}]
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
                    "version": record.version,
                    "composite_id": record.composite_id
                }
                for record in result.items
            ],
            "total": result.total,
            "page": result.page,
            "size": result.size,
            "has_next": result.has_next,
            "has_previous": result.has_previous
        }
        
        return QueryDataRecordsResponse(
            message=f"Successfully retrieved {len(result.items)} records from {schema_name}",
            schema_name=schema_name,
            data=response_data,
            execution_time_ms=0.0  # This will be set by the profiling decorator
        )
    except SchemaNotFoundException as e:
        logger.warning(f"Client error - Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except json.JSONDecodeError as e:
        logger.warning(f"Client error - Invalid JSON in query parameters: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON in filters or sort parameters")
    except Exception as e:
        logger.error(f"Internal server error retrieving records: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/{schema_name}/stream")
@profiling_decorator
async def stream_records_by_schema(
    schema_name: str,
    filters: Optional[str] = Query(None, description="JSON string of filters array"),
    sort: Optional[str] = Query(None, description="JSON string of sort array"),
    limit: Optional[int] = Query(None, ge=api_limits.MIN_STREAM_LIMIT, le=api_limits.MAX_STREAM_LIMIT, description="Maximum number of records to stream"),
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
            try:
                record_count = 0
                async for record in container.stream_data_records_use_case.execute(schema_name, query_request):
                    if limit and record_count >= limit:
                        break
                    
                    record_data = {
                        "id": str(record.id),
                        "schema_name": record.schema_name,
                        "data": record.data,
                        "created_at": record.created_at.isoformat() if hasattr(record.created_at, 'isoformat') else str(record.created_at),
                        "version": record.version,
                        "composite_id": record.composite_id
                    }
                    yield json.dumps(record_data, default=str) + "\n"
                    record_count += 1
                # Ensure proper stream ending
                logger.info(f"Stream completed for {schema_name}, {record_count} records streamed")
            except Exception as e:
                logger.error(f"Error in stream generator: {e}")
                # Yield error as final message
                error_data = {"error": str(e), "schema_name": schema_name}
                yield json.dumps(error_data, default=str) + "\n"
        
        return StreamingResponse(
            generate_stream(),
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": f"attachment; filename={schema_name}_data.ndjson"
            }
        )
    except SchemaNotFoundException as e:
        logger.warning(f"Client error - Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except json.JSONDecodeError as e:
        logger.warning(f"Client error - Invalid JSON in query parameters: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON in filters or sort parameters")
    except Exception as e:
        logger.error(f"Internal server error streaming records: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/{schema_name}/count", response_model=CountDataRecordsResponse)
@profiling_decorator
async def count_records_by_schema(
    schema_name: str,
    filters: Optional[str] = Query(None, description="JSON string of filters array"),
) -> CountDataRecordsResponse:
    """
    Get the count of records in a specific schema/table with optional filtering
    """
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
        
        return CountDataRecordsResponse(
            message=f"Successfully counted records in {schema_name}",
            schema_name=schema_name,
            count=count,
            execution_time_ms=0.0  # This will be set by the profiling decorator
        )
    except SchemaNotFoundException as e:
        logger.warning(f"Client error - Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except json.JSONDecodeError as e:
        logger.warning(f"Client error - Invalid JSON in query parameters: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON in filters parameters")
    except Exception as e:
        logger.error(f"Internal server error counting records: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/{schema_name}/{record_id}", response_model=DataRecordResponse)
@profiling_decorator
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
            version=record.version,
            composite_id=record.composite_id
        )
    except SchemaNotFoundException as e:
        logger.warning(f"Client error - Schema not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except RecordNotFoundException as e:
        logger.warning(f"Client error - Record not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Internal server error retrieving record: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/records/{schema_name}/by-key/{composite_id}", response_model=DataRecordResponse)
@profiling_decorator
async def get_record_by_composite_key(schema_name: str, composite_id: str) -> DataRecordResponse:
    """
    Get a specific record by composite key from a schema/table
    Example: /records/production_data/by-key/field_code=123&well_code=456&production_period=2024-01
    """
    try:
        # Parse composite_id back to dict
        composite_key = DataRecord.parse_composite_id(composite_id)
        
        # Get schema to validate composite key
        schema = await container.schema_repository.get_schema_by_name(schema_name)
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found")
        
        if not schema.primary_key:
            raise HTTPException(status_code=400, detail=f"Schema '{schema_name}' does not support composite keys")
        
        # Get record by composite key
        record = await container.data_repository.get_by_composite_key(schema, composite_key)
        
        if not record:
            raise HTTPException(status_code=404, detail=f"Record not found with composite key: {composite_id}")
        
        return DataRecordResponse(
            id=record.id,
            schema_name=record.schema_name,
            data=record.data,
            created_at=record.created_at,
            version=record.version,
            composite_id=record.composite_id
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Internal server error retrieving record by composite key: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/schemas")
@profiling_decorator
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
                "primary_key": schema.primary_key,
                "properties": [
                    {
                        "name": prop.name, 
                        "type": prop.type, 
                        "required": prop.required,
                        "primary_key": prop.primary_key
                    } 
                    for prop in schema.properties
                ]
            }
            for schema in schemas
        ]
    except Exception as e:
        logger.error(f"Internal server error retrieving schemas: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
