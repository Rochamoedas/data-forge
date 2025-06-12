"""
Arrow-Based Data API Endpoints

Clean API layer following Hexagonal Architecture:
- Controllers delegate to use cases
- No business logic in controllers
- Clean separation of concerns
"""

from fastapi import APIRouter, HTTPException, status, Request
from typing import Dict, Any
import pyarrow as pa
import pyarrow.ipc as ipc

from app.container.container import container
from app.domain.exceptions import SchemaNotFoundException
from app.config.logging_config import logger
from app.infrastructure.web.arrow import ArrowResponse


router = APIRouter()


@router.post("/arrow/bulk-insert/{schema_name}", tags=["Arrow"])
async def ultra_fast_bulk_insert(
    schema_name: str,
    request: Request
) -> Dict[str, Any]:
    """
    Bulk insert using Arrow IPC stream format.
    
    Delegates to use case which handles:
    - Schema validation
    - Data conversion to optimal format
    - Arrow → DuckDB insertion
    """
    try:
        arrow_bytes = await request.body()
        if not arrow_bytes:
            raise HTTPException(status_code=400, detail="No Arrow data provided in request body")
        
        # Add logging for incoming data size and validation
        logger.info(f"[ARROW-API] Received bulk-insert request for schema '{schema_name}' with data size: {len(arrow_bytes)} bytes")
        
        with ipc.open_stream(arrow_bytes) as reader:
            arrow_table = reader.read_all()
        
        # Validate Arrow table before processing
        if arrow_table is None or arrow_table.num_rows == 0:
            raise HTTPException(status_code=400, detail="Invalid or empty Arrow table provided")
        
        await container.create_ultra_fast_bulk_data_use_case.execute_from_arrow_table(
            schema_name=schema_name,
            arrow_table=arrow_table
        )

        return {
            "success": True,
            "message": f"Bulk insert completed for {schema_name}",
            "records_processed": arrow_table.num_rows,
            "optimization": "arrow_ipc_stream"
        }

    except SchemaNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except pa.ArrowInvalid as e:  # Specific exception for Arrow IPC errors
        logger.error(f"[ARROW-API] Arrow IPC deserialization failed: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid Arrow IPC data: {e}")
    except HTTPException as e:
        raise
    except Exception as e:
        logger.error(f"[ARROW-API] Bulk insert failed: {e}")
        raise HTTPException(status_code=500, detail="Bulk insert operation failed")


@router.get("/arrow/bulk-read/{schema_name}", response_class=ArrowResponse, tags=["Arrow"])
async def ultra_fast_bulk_read(
    schema_name: str
) -> ArrowResponse:
    """
    Bulk read using Arrow IPC stream format.
    """
    try:
        arrow_table = await container.create_ultra_fast_bulk_data_use_case.read_to_arrow_table(
            schema_name=schema_name
        )
        return ArrowResponse(arrow_table)
    except SchemaNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"[ARROW-API] Bulk read failed: {e}")
        raise HTTPException(status_code=500, detail="Bulk read operation failed")
