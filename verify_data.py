import duckdb
import pandas as pd
from app.config.settings import settings

# Set pandas display options for better output
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 200)

DB_PATH = settings.DATABASE_PATH
TABLE_NAME = "well_production"

def verify_data():
    """Connects to the database and prints specific rows for verification."""
    print(f"Connecting to database at: {DB_PATH}")
    try:
        con = duckdb.connect(database=DB_PATH, read_only=True)

        # Get total number of rows
        total_rows_query = f"SELECT COUNT(*) FROM {TABLE_NAME}"
        total_rows = con.execute(total_rows_query).fetchone()[0]
        print(f"Total rows in '{TABLE_NAME}': {total_rows}")

        if total_rows == 0:
            print("Table is empty. No data to display.")
            return

        # Define offsets
        first_offset = 0
        third_offset = total_rows // 3
        last_offset = total_rows - 1

        print("\n--- Fetching first, one-third, and last rows ordered by 'field_code' ---")

        # Queries
        query_first = f"SELECT * FROM {TABLE_NAME} ORDER BY field_code LIMIT 1 OFFSET {first_offset}"
        query_third = f"SELECT * FROM {TABLE_NAME} ORDER BY field_code LIMIT 1 OFFSET {third_offset}"
        query_last = f"SELECT * FROM {TABLE_NAME} ORDER BY field_code LIMIT 1 OFFSET {last_offset}"
        
        # Fetch data as pandas DataFrames
        df_first = con.execute(query_first).fetchdf()
        df_third = con.execute(query_third).fetchdf()
        df_last = con.execute(query_last).fetchdf()

        # Combine and print
        result_df = pd.concat([df_first, df_third, df_last]).reset_index(drop=True)
        result_df.index = ['First Row', 'One-Third Row', 'Last Row']
        
        print("\n--- Query Results ---")
        print(result_df)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    verify_data() 