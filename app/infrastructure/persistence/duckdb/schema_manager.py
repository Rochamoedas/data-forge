# app/infrastructure/persistence/duckdb/schema_manager.py
import duckdb
from app.domain.entities.schema import Schema
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.config.logging_config import logger

class DuckDBSchemaManager:
    def __init__(self, connection_pool: AsyncDuckDBPool):
        self.connection_pool = connection_pool

    async def ensure_table_exists(self, schema: Schema):
        column_defs = ", ".join([f'"{prop.name}" {prop.db_type}' for prop in schema.properties])
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema.table_name}" (
            id VARCHAR PRIMARY KEY,
            created_at TIMESTAMP,
            version INTEGER,
            {column_defs}
        );
        """
        async with self.connection_pool.acquire() as conn:
            try:
                conn.execute(create_table_sql)
                logger.info(f"Ensured table '{schema.table_name}' exists in DuckDB.")
                conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_id" ON "{schema.table_name}"(id);')
                conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_created_at" ON "{schema.table_name}"(created_at);')
                logger.info(f"Ensured indexes on '{schema.table_name}' for 'id' and 'created_at'.")
            except Exception as e:
                logger.error(f"Error ensuring table '{schema.table_name}' exists: {e}")
                raise