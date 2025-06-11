# app/main.py
from fastapi import FastAPI, HTTPException, Depends
from contextlib import asynccontextmanager
import time

from app.config.settings import settings
from app.data_service import DataService
from app.models import QueryRequest, BulkInsertRequest, QueryResponse, ExecuteResponse
from app.infrastructure.web.routers import arrow_performance_data
from app.container.container import container
from app.config.logging_config import logger

# Initialize DataService
data_service = DataService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    await container.startup()
    await data_service.initialize()
    logger.info("Application startup complete.")
    yield
    # Shutdown logic
    await container.shutdown()
    await data_service.close()
    logger.info("Application shutdown complete.")

app = FastAPI(
    title="Data Forge",
    version="1.0.0",
    lifespan=lifespan
)

# Mount the high-performance Arrow router
app.include_router(arrow_performance_data.router, prefix="/api/v1")

# --- Simplified API Endpoints ---
@app.post("/query", response_model=QueryResponse)
async def query_data(request: QueryRequest, service: DataService = Depends(lambda: data_service)):
    """Execute SELECT queries and return results in the specified format."""
    start_time = time.perf_counter()
    
    try:
        if request.format == "dataframe":
            # Not directly returnable via JSON, special handling would be needed
            # For now, let's treat it as JSON for API purposes
            data = await service.query_json(request.sql, request.params)
            rows = len(data) if data else 0
        elif request.format == "arrow":
             # API cannot directly return Arrow, so we convert to JSON
            arrow_table = await service.query_arrow(request.sql, request.params)
            data = arrow_table.to_pydict()
            rows = arrow_table.num_rows
        else: # JSON format
            data = await service.query_json(request.sql, request.params)
            rows = len(data) if data else 0

        duration_ms = (time.perf_counter() - start_time) * 1000
        return QueryResponse(success=True, data=data, rows=rows, duration_ms=duration_ms)
    
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/execute", response_model=ExecuteResponse)
async def execute_sql(request: QueryRequest, service: DataService = Depends(lambda: data_service)):
    """Execute INSERT, UPDATE, DELETE, or other non-SELECT SQL statements."""
    start_time = time.perf_counter()
    try:
        result = await service.execute(request.sql, request.params)
        duration_ms = (time.perf_counter() - start_time) * 1000
        return ExecuteResponse(
            success=True, 
            rows_affected=result.get("rows_affected", 0),
            duration_ms=duration_ms
        )
    except Exception as e:
        logger.error(f"Execute failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/bulk-insert", response_model=ExecuteResponse)
async def bulk_insert(request: BulkInsertRequest, service: DataService = Depends(lambda: data_service)):
    """High-performance bulk data insertion from a list of dictionaries."""
    start_time = time.perf_counter()
    try:
        result = await service.bulk_insert_polars(request.table, request.data)
        duration_ms = (time.perf_counter() - start_time) * 1000
        return ExecuteResponse(
            success=True, 
            rows_affected=result.get("rows_affected", 0),
            duration_ms=duration_ms
        )
    except Exception as e:
        logger.error(f"Bulk insert failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def read_root():
    return {"message": "Welcome to Data Forge API"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)