import duckdb
import pandas as pd
from app.config.settings import settings

# Configure pandas display options
pd.set_option('display.max_rows', settings.DISPLAY_MAX_ROWS)
pd.set_option('display.width', settings.DISPLAY_WIDTH)

DB_PATH = settings.DATABASE_PATH
TABLE_NAME = "well_production"

def verify_data():
    """Verify data in the DuckDB database."""
    # Connect to DuckDB
    conn = duckdb.connect(settings.DATABASE_PATH)
    
    # Check field_code = 0
    query_fc_0 = f"SELECT * FROM {TABLE_NAME} WHERE field_code = 0 LIMIT 1"
    print("\nField Code 0:")
    print(conn.execute(query_fc_0).df())
    
    # Check field_code = 3
    query_fc_3 = f"SELECT * FROM {TABLE_NAME} WHERE field_code = 3 LIMIT 1"
    print("\nField Code 3:")
    print(conn.execute(query_fc_3).df())
    
    # Check highest field_code
    query_fc_max = f"SELECT * FROM {TABLE_NAME} ORDER BY field_code DESC LIMIT 1"
    print("\nHighest Field Code:")
    print(conn.execute(query_fc_max).df())
    
    # Close connection
    conn.close()

if __name__ == "__main__":
    verify_data() 