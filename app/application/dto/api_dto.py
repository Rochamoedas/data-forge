from pydantic import BaseModel
from typing import Optional, List, Any, Literal, Dict

# Request Models
class QueryRequest(BaseModel):
    sql: str
    params: Optional[List[Any]] = None
    format: Literal["json", "arrow", "parquet"] = "json"

class BulkInsertRequest(BaseModel):
    table: str
    data: List[Dict[str, Any]]
    format: Literal["polars", "arrow"] = "polars"

# Response Models  
class QueryResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    rows: int
    duration_ms: float

class ExecuteResponse(BaseModel):
    success: bool
    rows_affected: int
    duration_ms: float 