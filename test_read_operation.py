import asyncio
import duckdb
import pandas as pd
import pyarrow as pa
from typing import List, Dict, Any

# Mock objects and simplified implementations for testing
class MockSchema:
    def __init__(self, table_name, properties):
        self.table_name = table_name
        self.properties = properties

class MockProperty:
    def __init__(self, name, type):
        self.name = name
        self.type = type

class MockAsyncDuckDBPool:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def acquire(self):
        return self

class ArrowBulkOperations:
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool

    async def bulk_insert_from_arrow_table(self, schema: MockSchema, arrow_table: pa.Table) -> None:
        async with self.connection_pool.acquire() as conn:
            # Separate table creation from insertion
            columns_def = ", ".join([f"{prop.name} {prop.type}" for prop in schema.properties])
            conn.execute(f'CREATE TABLE IF NOT EXISTS {schema.table_name} ({columns_def})')
            
            conn.register("arrow_table", arrow_table)
            conn.execute(f'INSERT INTO {schema.table_name} SELECT * FROM arrow_table')
            conn.execute('CHECKPOINT')

    async def bulk_read_to_arrow_table(self, schema: MockSchema) -> pa.Table:
        async with self.connection_pool.acquire() as conn:
            result = conn.execute(f'SELECT * FROM "{schema.table_name}"')
            return result.fetch_arrow_table()

async def run_test():
    """Isolated test for Arrow bulk operations."""
    print("--- Running Isolated Read Operation Test ---")
    
    # 1. Use an in-memory database for isolation
    conn = duckdb.connect(':memory:')
    pool = MockAsyncDuckDBPool(conn)
    
    # 2. Define a simple schema
    schema = MockSchema(
        table_name="test_table",
        properties=[
            MockProperty(name="id", type="INTEGER"),
            MockProperty(name="value", type="VARCHAR"),
        ]
    )
    
    # 3. Create test data
    data = [{'id': 1, 'value': 'A'}, {'id': 2, 'value': 'B'}]
    arrow_table = pa.Table.from_pylist(data)
    
    # 4. Instantiate and use the operations class
    operations = ArrowBulkOperations(connection_pool=pool)
    
    try:
        # Insert data
        print("Inserting test data...")
        await operations.bulk_insert_from_arrow_table(schema, arrow_table)
        print("Insert successful.")
        
        # Read data back
        print("Reading data back...")
        read_table = await operations.bulk_read_to_arrow_table(schema)
        print("Read successful.")
        
        # 5. Verify the results
        print(f"Original records: {len(arrow_table)}")
        print(f"Read records: {len(read_table)}")
        
        if len(read_table) == len(arrow_table):
            print("✅ TEST PASSED: Read operation returned the correct number of records.")
        else:
            print("❌ TEST FAILED: Read operation did not return the expected number of records.")
            
    except Exception as e:
        print(f"❌ TEST FAILED: An error occurred - {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(run_test()) 