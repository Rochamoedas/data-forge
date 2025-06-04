# app/infrastructure/persistence/duckdb/schema_manager.py
from app.domain.entities.schema import Schema, SchemaField
from app.infrastructure.persistence.duckdb.connection import DuckDBConnection
from textwrap import dedent

class DuckDBSchemaManager:
    """
    Manages the creation and verification of database tables in DuckDB
    based on defined Schema entities.
    """
    def __init__(self, db_connection: DuckDBConnection):
        self.db_connection = db_connection

    def _map_field_type_to_duckdb(self, field_type: str) -> str:
        """Maps our generic field types to DuckDB specific types."""
        type_map = {
            "STRING": "VARCHAR",
            "INTEGER": "BIGINT", # Use BIGINT for more flexibility
            "BOOLEAN": "BOOLEAN",
            "DOUBLE": "DOUBLE",
            "TIMESTAMP": "TIMESTAMP",
            "UUID": "UUID", # DuckDB supports UUID
            # Add more mappings as needed
        }
        return type_map.get(field_type.upper(), "VARCHAR") # Default to VARCHAR

    def ensure_table_exists(self, schema: Schema):
        """
        Ensures that a table corresponding to the given schema exists in DuckDB.
        Creates it if it doesn't exist.
        """
        conn = self.db_connection.get_connection()

        # Build column definitions for the CREATE TABLE statement
        column_defs = []
        # Always add a primary key 'id' column for DataRecord
        column_defs.append("id UUID PRIMARY KEY") # We'll manage UUID generation from DataRecord

        for field in schema.fields:
            # Skip 'id' field if it's already added as primary key
            if field.name.lower() == 'id' and field.type.upper() == 'UUID':
                continue
            duckdb_type = self._map_field_type_to_duckdb(field.type)
            column_defs.append(f"{field.name} {duckdb_type}")

        # Join column definitions to form the SQL statement
        columns_sql = ", ".join(column_defs)

        create_table_sql = dedent(f"""
        CREATE TABLE IF NOT EXISTS {schema.name} (
            {columns_sql}
        );
        """)

        try:
            conn.execute(create_table_sql)
            print(f"Table '{schema.name}' ensured/created successfully.")
        except Exception as e:
            print(f"Error ensuring table '{schema.name}': {e}")
            raise # Re-raise the exception to propagate it