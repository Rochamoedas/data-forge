\
This document outlines the changes to achieve the simplified architecture described in `simplify_rev2.md`.
Apply these changes using a diff/patch tool or manually.

# File Deletions

diff --git a/app/application/dto/create_data_dto.py b/app/application/dto/create_data_dto.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/application/dto/create_data_dto.py
+++ /dev/null

diff --git a/app/application/dto/data_dto.py b/app/application/dto/data_dto.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/application/dto/data_dto.py
+++ /dev/null

diff --git a/app/application/dto/query_dto.py b/app/application/dto/query_dto.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/application/dto/query_dto.py
+++ /dev/null

diff --git a/app/application/dto/query_request_dto.py b/app/application/dto/query_request_dto.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/application/dto/query_request_dto.py
+++ /dev/null

diff --git a/app/application/dto/schema_dto.py b/app/application/dto/schema_dto.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/application/dto/schema_dto.py
+++ /dev/null

diff --git a/app/application/use_cases/create_data_record.py b/app/application/use_cases/create_data_record.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/application/use_cases/create_data_record.py
+++ /dev/null

diff --git a/app/application/use_cases/create_bulk_data_records.py b/app/application/use_cases/create_bulk_data_records.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/application/use_cases/create_bulk_data_records.py
+++ /dev/null

diff --git a/app/application/use_cases/get_data_record.py b/app/application/use_cases/get_data_record.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/application/use_cases/get_data_record.py
+++ /dev/null

diff --git a/app/application/use_cases/query_data_records.py b/app/application/use_cases/query_data_records.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/application/use_cases/query_data_records.py
+++ /dev/null

diff --git a/app/infrastructure/web/dependencies/common.py b/app/infrastructure/web/dependencies/common.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/infrastructure/web/dependencies/common.py
+++ /dev/null

diff --git a/app/infrastructure/web/routers/data.py b/app/infrastructure/web/routers/data.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/infrastructure/web/routers/data.py
+++ /dev/null

diff --git a/app/infrastructure/web/routers/high_performance_data.py b/app/infrastructure/web/routers/high_performance_data.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/infrastructure/web/routers/high_performance_data.py
+++ /dev/null

diff --git a/app/infrastructure/persistence/high_performance_data_processor.py b/app/infrastructure/persistence/high_performance_data_processor.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/infrastructure/persistence/high_performance_data_processor.py
+++ /dev/null

diff --git a/app/domain/services/data_management.py b/app/domain/services/data_management.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/domain/services/data_management.py
+++ /dev/null

diff --git a/app/infrastructure/persistence/repositories/duckdb_data_repository.py b/app/infrastructure/persistence/repositories/duckdb_data_repository.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/infrastructure/persistence/repositories/duckdb_data_repository.py
+++ /dev/null
+# Note: Logic from this file should be merged into app/data_service.py

diff --git a/app/infrastructure/persistence/duckdb/schema_manager.py b/app/infrastructure/persistence/duckdb/schema_manager.py
deleted file mode 100644
index xxxxxxx..0000000
--- a/app/infrastructure/persistence/duckdb/schema_manager.py
+++ /dev/null
+# Note: Logic from this file should be merged into app/data_service.py

# File Creations / Overwrites

diff --git a/app/data_service.py b/app/data_service.py
new file mode 100644
index 0000000..xxxxxxx
--- /dev/null
+++ b/app/data_service.py
@@ -0,0 +1,45 @@
+from typing import Dict, Any, List
+import polars as pl
+import pyarrow as pa
+
+# Placeholder for AsyncDuckDBPool, replace with actual import or definition
+class AsyncDuckDBPool:
+    def __init__(self, db_path: str):
+        self.db_path = db_path
+        # Initialize connection pool logic here
+        pass
+    async def execute(self, query: str, params: list = None): # Example method
+        # Actual execution logic
+        pass
+    async def fetchdf(self, query: str, params: list = None): # Example method for Polars
+        # Actual execution logic
+        pass
+    async def fetch_arrow_table(self, query: str, params: list = None): # Example method for Arrow
+        # Actual execution logic
+        pass
+
+# Placeholder for Schema, replace with actual import or definition
+class Schema:
+    pass
+
+class DataService:
+    """
+    🚀 Unified high-performance data service
+    Combines Polars + PyArrow + DuckDB for all operations
+    """
+    def __init__(self, db_path: str):
+        self.connection_pool = AsyncDuckDBPool(db_path)
+        self.schemas = self._load_schemas()
+
+    async def query(self, sql: str, params: list = None) -> pl.DataFrame: # type: ignore
+        # return await self.connection_pool.fetchdf(sql, params) # Example
+        pass
+    async def query_json(self, sql: str, params: list = None) -> Dict[str, Any]:
+        # df = await self.query(sql, params) # Example
+        # return {"data": df.to_dicts()} # Example
+        pass
+    async def query_arrow(self, sql: str, params: list = None) -> pa.Table: # type: ignore
+        # return await self.connection_pool.fetch_arrow_table(sql, params) # Example
+        pass
+    async def execute(self, sql: str, params: list = None) -> Dict[str, Any]:
+        # await self.connection_pool.execute(sql, params) # Example
+        # return {"rows_affected": 0} # Example: get actual rows affected
+        pass
+    async def bulk_insert_polars(self, table_name: str, df: pl.DataFrame) -> Dict[str, Any]:
+        # conn.register(f'{table_name}_pl', df) # Example with DuckDB direct Polars scan
+        # await self.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_pl") # Example
+        pass
+    async def bulk_insert_arrow(self, table_name: str, arrow_table: pa.Table) -> Dict[str, Any]:
+        # conn.register(f'{table_name}_arrow', arrow_table) # Example
+        # await self.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_arrow") # Example
+        pass
+    def get_schema(self, name: str) -> Schema: # type: ignore
+        # Logic to retrieve a schema by name
+        pass
+    def list_schemas(self) -> List[Schema]: # type: ignore
+        # Logic to list all available schemas
+        return []
+    def _load_schemas(self):
+        # Logic to load schemas, possibly from app.infrastructure.persistence.repositories.file_schema_repository.py
+        return {}

diff --git a/app/models.py b/app/models.py
new file mode 100644
index 0000000..xxxxxxx
--- /dev/null
+++ b/app/models.py
@@ -0,0 +1,26 @@
+from typing import Optional, List, Any, Literal, Dict
+from pydantic import BaseModel
+
+# Request Models
+class QueryRequest(BaseModel):
+    sql: str
+    params: Optional[List[Any]] = None
+    format: Literal["json", "arrow", "parquet"] = "json"
+
+class BulkInsertRequest(BaseModel):
+    table: str
+    data: List[Dict[str, Any]]
+    format: Literal["polars", "arrow"] = "polars"
+
+# Response Models
+class QueryResponse(BaseModel):
+    success: bool
+    data: Optional[Any] = None
+    rows: int
+    duration_ms: float
+
+class ExecuteResponse(BaseModel):
+    success: bool
+    rows_affected: int
+    duration_ms: float

diff --git a/app/main.py b/app/main.py
index xxxxxxx..xxxxxxx 100644 # Assuming app/main.py is overwritten
--- a/app/main.py
+++ b/app/main.py
@@ -1,X +1,Y @@ # Exact line numbers depend on original app/main.py
+from fastapi import FastAPI
+from .models import QueryRequest, QueryResponse, BulkInsertRequest, ExecuteResponse
+from .data_service import DataService
+
+# from app.config.settings import settings # Assuming settings are here
+# from app.infrastructure.web.routers import arrow_performance_data # For existing Arrow endpoints
+
+app = FastAPI(title="Data Forge", version="1.0.0")
+
+# Single global service instance
+# data_service = DataService(settings.DATABASE_PATH) # Use actual path from settings
+data_service = DataService("data/data.duckdb") # Placeholder DB path
+
+@app.post("/query", response_model=QueryResponse)
+async def query_data(request: QueryRequest) -> QueryResponse:
+    """Execute SELECT queries and return results"""
+    # data = None
+    # rows = 0
+    # duration_ms = 0.0 # Calculate actual duration
+    # # Example:
+    # # if request.format == "polars":
+    # #     df = await data_service.query(request.sql, request.params)
+    # #     data = df.to_dicts() # Or appropriate format
+    # #     rows = len(df)
+    # # elif request.format == "arrow":
+    # #     arrow_table = await data_service.query_arrow(request.sql, request.params)
+    # #     # Convert arrow_table to JSON-serializable format if returning directly
+    # #     data = "Arrow data (implement serialization)"
+    # #     rows = len(arrow_table)
+    # # else: # json
+    # #     json_data_dict = await data_service.query_json(request.sql, request.params)
+    # #     data = json_data_dict.get("data")
+    # #     rows = len(data) if isinstance(data, list) else 0
+    # return QueryResponse(success=True, data=data, rows=rows, duration_ms=duration_ms)
+    pass
+
+@app.post("/execute", response_model=ExecuteResponse)
+async def execute_sql(request: QueryRequest) -> ExecuteResponse: # Consider a more specific request model if 'format' isn't needed
+    """Execute INSERT/UPDATE/DELETE statements"""
+    # result = await data_service.execute(request.sql, request.params)
+    # return ExecuteResponse(success=True, rows_affected=result.get("rows_affected", 0), duration_ms=0.0)
+    pass
+
+@app.post("/bulk-insert", response_model=ExecuteResponse)
+async def bulk_insert(request: BulkInsertRequest) -> ExecuteResponse:
+    """High-performance bulk data insertion"""
+    # import polars as pl
+    # import pyarrow as pa
+    # duration_ms = 0.0 # Calculate actual duration
+    # rows_affected = 0
+    # # Example:
+    # # if request.format == "polars":
+    # #     df = pl.DataFrame(request.data)
+    # #     result = await data_service.bulk_insert_polars(request.table, df)
+    # #     rows_affected = result.get("rows_affected", len(df))
+    # # elif request.format == "arrow":
+    # #     # This conversion needs care based on schema
+    # #     # Example: arrow_table = pa.Table.from_pylist(request.data)
+    # #     # result = await data_service.bulk_insert_arrow(request.table, arrow_table)
+    # #     # rows_affected = result.get("rows_affected", len(arrow_table))
+    # return ExecuteResponse(success=True, rows_affected=rows_affected, duration_ms=duration_ms)
+    pass
+
+# IMPORTANT: Include the existing Arrow performance router
+# app.include_router(arrow_performance_data.router, prefix="/api/v1/arrow-performance", tags=["Arrow Performance"])

# Manual Simplification / Merging Notes
# The following files and tasks require manual intervention as per simplify_rev2.md:

# 1. app/container/container.py
#    Action: Simplify this file for Dependency Injection specifically for the arrow-performance endpoints.
#    The existing complex DI system should be largely removed or streamlined.

# 2. app/infrastructure/persistence/duckdb/query_builder.py
#    Action: Simplify this query builder. Its role might be reduced given the direct SQL capabilities
#    exposed via DataService.

# 3. Merge Logic into app/data_service.py:
#    - The functionality of `app/infrastructure/persistence/repositories/duckdb_data_repository.py`
#      (traditional CRUD operations) should be incorporated into `app/data_service.py`.
#    - The functionality of `app/infrastructure/persistence/duckdb/schema_manager.py`
#      (schema management related to DuckDB) should be incorporated into `app/data_service.py`,
#      particularly within methods like `_load_schemas` or schema-related operations.

# 4. Review `app/infrastructure/persistence/repositories/file_schema_repository.py`:
#    Action: While `simplify_rev2.md` mentions merging this into `DataService` in one section,
#    the "New Folder Structure" lists it as a separate file (`app/infrastructure/persistence/repositories/file_schema_repository.py # Schema loading`).
#    Clarify its role. If kept, `DataService._load_schemas()` would likely utilize this repository.

# 5. Create New Directory Structure (if not existing):
#    - `app/domain/entities/`
#    - `app/domain/repositories/` (for repository interfaces if needed by the arrow-performance module)

# 6. Verify Arrow Performance Endpoints:
#    Ensure that the changes do not break the existing arrow-performance endpoints.
#    The simplified `app/main.py` must correctly include the `arrow_performance_data.py` router.
#    The simplified `app/container/container.py` must correctly provide dependencies for these endpoints.
