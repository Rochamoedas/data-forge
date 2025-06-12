# app/infrastructure/persistence/duckdb/schema_manager.py
import duckdb
from app.domain.entities.schema import Schema
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.config.logging_config import logger
from typing import List

class DuckDBSchemaManager:
    def __init__(self, connection_pool: AsyncDuckDBPool):
        self.connection_pool = connection_pool

    async def ensure_tables_exist(self, schemas: List[Schema]):
        """Create all tables and indexes in a single transaction for better performance."""
        async with self.connection_pool.acquire() as conn:
            try:
                # Start transaction
                conn.execute("BEGIN TRANSACTION")
                
                # Create all tables
                for schema in schemas:
                    column_defs = ", ".join([f'"{prop.name}" {prop.db_type}' for prop in schema.properties])
                    
                    # Build composite primary key constraint if defined
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
                
                # Create all indexes in parallel
                for schema in schemas:
                    # Create indexes concurrently
                    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_id" ON "{schema.table_name}"(id);')
                    conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_created_at" ON "{schema.table_name}"(created_at);')
                    
                    # Create composite key index for performance
                    if schema.primary_key:
                        pk_columns = ", ".join([f'"{col}"' for col in schema.primary_key])
                        conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_composite_key" ON "{schema.table_name}"({pk_columns});')
                    
                    # Create indexes for required fields
                    for prop in schema.properties:
                        if prop.required:
                            conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{schema.table_name}_{prop.name}" ON "{schema.table_name}"({prop.name});')
                
                # Commit transaction
                conn.execute("COMMIT")
                logger.info(f"Created/verified {len(schemas)} tables with composite keys and their indexes in a single transaction")
                
            except Exception as e:
                conn.execute("ROLLBACK")
                logger.error(f"Error creating tables and indexes: {e}")
                raise

    async def ensure_table_exists(self, schema: Schema):
        """Legacy method for backward compatibility."""
        await self.ensure_tables_exist([schema])

    async def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        async with self.connection_pool.acquire() as conn:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
                return result[0] > 0
            except Exception as e:
                logger.error(f"Error checking if table exists: {e}")
                return False