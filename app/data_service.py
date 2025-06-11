import duckdb
import polars as pl
import pyarrow as pa
from typing import Dict, Any, List, Optional
import time
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.domain.entities.schema import Schema
from app.infrastructure.metadata.schemas_description import SCHEMAS_METADATA
from app.config.logging_config import logger

class DataService:
    """
    🚀 Unified high-performance data service
    Combines Polars + PyArrow + DuckDB for all operations
    """
    
    def __init__(self):
        self.connection_pool = AsyncDuckDBPool()
        self.schemas: Dict[str, Schema] = {}

    async def initialize(self):
        """Initializes the service, including the connection pool and schemas."""
        await self.connection_pool.initialize()
        
        # Load schemas from metadata
        schemas_list = []
        for schema_data in SCHEMAS_METADATA:
            schema = Schema(**schema_data)
            self.schemas[schema.name] = schema
            schemas_list.append(schema)
        
        # Ensure tables and indexes exist
        await self._ensure_tables_exist(schemas_list)
        logger.info(f"Loaded {len(self.schemas)} schemas and ensured tables in DuckDB.")
        logger.info("DataService initialized.")

    async def close(self):
        """Closes the connection pool."""
        await self.connection_pool.close()
        logger.info("DataService closed.")

    async def _ensure_tables_exist(self, schemas: List[Schema]):
        """Create all tables and indexes in a single transaction for better performance."""
        async with self.connection_pool.acquire() as conn:
            try:
                conn.execute("BEGIN TRANSACTION")
                
                for schema in schemas:
                    column_defs = ", ".join([f'"{prop.name}" {prop.db_type}' for prop in schema.properties])
                    
                    composite_pk_constraint = ""
                    if schema.primary_key:
                        pk_columns = ", ".join([f'"{col}"' for col in schema.primary_key])
                        composite_pk_constraint = f", UNIQUE({pk_columns})"
                    
                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS "{schema.table_name}" (
                        id VARCHAR PRIMARY KEY,
                        created_at TIMESTAMP,
                        version INTEGER,
                        {column_defs}{composite_pk_constraint}
                    );
                    """
                    conn.execute(create_table_sql)
                
                for schema in schemas:
                    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_id" ON "{schema.table_name}"(id);')
                    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_created_at" ON "{schema.table_name}"(created_at);')
                    
                    if schema.primary_key:
                        pk_columns = ", ".join([f'"{col}"' for col in schema.primary_key])
                        conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_composite_key" ON "{schema.table_name}"({pk_columns});')
                    
                    for prop in schema.properties:
                        if prop.required:
                            conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_{prop.name}" ON "{schema.table_name}"({prop.name});')
                
                conn.execute("COMMIT")
                logger.info(f"Created/verified {len(schemas)} tables with composite keys and their indexes in a single transaction")
                
            except Exception as e:
                conn.execute("ROLLBACK")
                logger.error(f"Error creating tables and indexes: {e}")
                raise

    # Read Operations
    async def query(self, sql: str, params: Optional[List[Any]] = None) -> pl.DataFrame:
        async with self.connection_pool.acquire() as conn:
            result = conn.execute(sql, params).pl()
            return result

    async def query_json(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        async with self.connection_pool.acquire() as conn:
            result = conn.execute(sql, params).fetchdf().to_dict(orient="records")
            return result

    async def query_arrow(self, sql: str, params: Optional[List[Any]] = None) -> pa.Table:
        async with self.connection_pool.acquire() as conn:
            result = conn.execute(sql, params).fetch_arrow_table()
            return result

    # Write Operations
    async def execute(self, sql: str, params: Optional[List[Any]] = None) -> int:
        async with self.connection_pool.acquire() as conn:
            result = conn.execute(sql, params)
            return result.rowcount

    async def bulk_insert_polars(self, table_name: str, df: pl.DataFrame) -> int:
        arrow_table = df.to_arrow()
        return await self.bulk_insert_arrow(table_name, arrow_table)

    async def bulk_insert_arrow(self, table_name: str, arrow_table: pa.Table) -> int:
        async with self.connection_pool.acquire() as conn:
            conn.register('temp_arrow_table', arrow_table)
            result = conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM temp_arrow_table')
            conn.unregister('temp_arrow_table')
            return result.rowcount

    # Schema Operations
    def get_schema(self, name: str) -> Optional[Schema]:
        return self.schemas.get(name)

    def list_schemas(self) -> List[Schema]:
        return list(self.schemas.values()) 