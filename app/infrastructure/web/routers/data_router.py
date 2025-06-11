# app/infrastructure/web/routers/data_router.py
from fastapi import APIRouter, Depends, HTTPException
import time
import polars as pl

from app.container.container import container
from app.domain.repositories.data_repository import IDataRepository
from app.application.dto.api_dto import QueryRequest, BulkInsertRequest, QueryResponse, ExecuteResponse
from app.config.logging_config import logger

router = APIRouter()

def get_data_repository() -> IDataRepository:
    return container.data_repository

@router.post("/query", response_model=QueryResponse, tags=["Generic Data Operations"])
async def query_data(
    request: QueryRequest,
    repo: IDataRepository = Depends(get_data_repository)
):
    """Execute SELECT queries and return results in the specified format."""
    start_time = time.perf_counter()
    try:
        if request.format == "arrow":
            arrow_table = await repo.query_arrow(request.sql, request.params)
            data = arrow_table.to_pydict()
            rows = arrow_table.num_rows
        else:  # Default to JSON
            data = await repo.query_json(request.sql, request.params)
            rows = len(data)

        duration_ms = (time.perf_counter() - start_time) * 1000
        return QueryResponse(success=True, data=data, rows=rows, duration_ms=duration_ms)
    
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/execute", response_model=ExecuteResponse, tags=["Generic Data Operations"])
async def execute_sql(
    request: QueryRequest,
    repo: IDataRepository = Depends(get_data_repository)
):
    """Execute INSERT, UPDATE, DELETE, or other non-SELECT SQL statements."""
    start_time = time.perf_counter()
    try:
        rows_affected = await repo.execute(request.sql, request.params)
        duration_ms = (time.perf_counter() - start_time) * 1000
        return ExecuteResponse(success=True, rows_affected=rows_affected, duration_ms=duration_ms)
    except Exception as e:
        logger.error(f"Execute failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk-insert", response_model=ExecuteResponse, tags=["Generic Data Operations"])
async def bulk_insert(
    request: BulkInsertRequest,
    repo: IDataRepository = Depends(get_data_repository)
):
    """High-performance bulk data insertion from a list of dictionaries."""
    start_time = time.perf_counter()
    try:
        # The repository expects a Polars DataFrame for this specific method
        # which is a helper around the arrow implementation.
        df = pl.DataFrame(request.data)
        rows_affected = await repo.bulk_insert_polars(request.table, df)
        duration_ms = (time.perf_counter() - start_time) * 1000
        return ExecuteResponse(success=True, rows_affected=rows_affected, duration_ms=duration_ms)
    except Exception as e:
        logger.error(f"Bulk insert failed: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 