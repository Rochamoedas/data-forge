import asyncio
from typing import List
from app.domain.entities.schema import Schema
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.config.logging_config import logger

class DuckDBSchemaManager:
    """
    Manages the database schema in DuckDB, creating tables and indexes.
    """
    def __init__(self, connection_pool: AsyncDuckDBPool):
        self.connection_pool = connection_pool

    def _create_tables_and_indexes_sync(self, conn, schemas: List[Schema]):
        """
        The synchronous part of creating tables and indexes.
        """
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
                        conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_{prop.name}" ON "{schema.table_name}"("{prop.name}");')
            
            conn.execute("COMMIT")
            logger.info(f"Created/verified {len(schemas)} tables with composite keys and their indexes in a single transaction.")
            
        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Error creating tables and indexes: {e}")
            raise

    async def create_tables_and_indexes(self, schemas: List[Schema]):
        """
        Creates all tables and indexes in a single transaction for better performance.
        This method is designed to be idempotent and runs sync code in a thread pool.
        """
        async with self.connection_pool.acquire() as conn:
            await asyncio.to_thread(self._create_tables_and_indexes_sync, conn, schemas) 