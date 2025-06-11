import duckdb
import pandas as pd
import json
import time
import tempfile
import os
from pathlib import Path
from typing import List, Dict, Any
import psutil
import uuid
from datetime import datetime, timedelta

from app.config.settings import settings
from app.config.api_limits import api_limits

# --- CONFIGURABLE PARAMETERS ---
# Path to the mocked response file (change for different test sizes)
MOCKED_JSON_PATH = "external/mocked_response_100K-4.json"  # e.g. "external/mocked_response_100K-4.json"

# DuckDB table name
DUCKDB_TABLE_NAME = "well_production"

# DuckDB file suffix
DUCKDB_FILE_SUFFIX = ".duckdb"

# Number of test records to generate (set to 0 to use file)
TEST_DATA_SIZE = 900_000  # Set to 0 to use file, >0 to use generated data

# --- END CONFIGURABLE PARAMETERS ---

# --- Load schema from schemas_description.py ---
from app.infrastructure.metadata.schemas_description import SCHEMAS_METADATA

# Find the well_production schema
def get_well_production_schema():
    for schema in SCHEMAS_METADATA:
        if schema["name"] == "well_production":
            return schema
    raise ValueError("well_production schema not found")


schema = get_well_production_schema()

# --- Prepare DuckDB table DDL from schema ---
def duckdb_type(py_type, db_type):
    # Map schema types to DuckDB types
    if db_type == "BIGINT":
        return "BIGINT"
    if db_type == "DOUBLE":
        return "DOUBLE"
    if db_type == "TIMESTAMP":
        return "TIMESTAMP"
    if db_type == "VARCHAR":
        return "VARCHAR"
    return "VARCHAR"  # fallback


def make_ddl(schema):
    cols = []
    for prop in schema["properties"]:
        col = f'"{prop["name"]}" {duckdb_type(prop["type"], prop["db_type"])}'
        cols.append(col)
    pk = schema.get("primary_key", [])
    pk_clause = f', PRIMARY KEY ({", ".join(pk)})' if pk else ''
    return f'CREATE TABLE {DUCKDB_TABLE_NAME} ({", ".join(cols)}{pk_clause});'


table_ddl = make_ddl(schema)

# --- Test Data Generation ---
def generate_test_data(size: int = api_limits.BENCHMARK_IO_TEST_SIZE) -> List[Dict[str, Any]]:
    """Generate test data with unique composite primary keys."""
    data = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(size):
        prod_date = base_date + timedelta(seconds=i)
        record = {
            "id": str(uuid.uuid4()),
            "created_at": datetime.now(),
            "version": 1,
            "field_code": i % 1000,
            "_field_name": f"Field_{i % 1000}",
            "well_code": i % 100,
            "_well_reference": f"WELL_REF_{i % 100:03d}",
            "well_name": f"Well_{i % 100}",
            "production_period": prod_date.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "days_on_production": 30,
            "oil_production_kbd": round(100.0 + (i * 0.1), 2),
            "gas_production_mmcfd": round(50.0 + (i * 0.05), 2),
            "liquids_production_kbd": round(25.0 + (i * 0.025), 2),
            "water_production_kbd": round(75.0 + (i * 0.075), 2),
            "data_source": "performance_test",
            "source_data": json.dumps({"test": f"data_{i}"}),
            "partition_0": f"partition_{i % 10}"
        }
        data.append(record)
    return data

# --- Resource Monitoring ---
def get_process_metrics() -> Dict[str, float]:
    """Get current process CPU and memory usage."""
    process = psutil.Process()
    return {
        "cpu_percent": process.cpu_percent(),
        "memory_mb": process.memory_info().rss / (1024 * 1024)
    }

# --- Benchmark Functions ---
def benchmark_duckdb_write(data: List[Dict[str, Any]], db_path: str) -> Dict[str, Any]:
    """Benchmark writing data to DuckDB."""
    print("\n--- Starting DuckDB Write Benchmark ---")
    metrics_start = get_process_metrics()
    start_time = time.perf_counter()
    
    # Convert to DataFrame and write to DuckDB
    df = pd.DataFrame(data)
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS well_production AS SELECT * FROM df")
    conn.close()
    
    end_time = time.perf_counter()
    metrics_end = get_process_metrics()
    
    duration = end_time - start_time
    return {
        "operation": "DuckDB Write",
        "duration_s": duration,
        "records_processed": len(data),
        "throughput_rps": len(data) / duration if duration > 0 else 0,
        "cpu_usage": metrics_end["cpu_percent"] - metrics_start["cpu_percent"],
        "memory_usage_mb": metrics_end["memory_mb"] - metrics_start["memory_mb"]
    }

def benchmark_duckdb_read(db_path: str) -> Dict[str, Any]:
    """Benchmark reading data from DuckDB."""
    print("\n--- Starting DuckDB Read Benchmark ---")
    metrics_start = get_process_metrics()
    start_time = time.perf_counter()
    
    # Read from DuckDB
    conn = duckdb.connect(db_path)
    df = conn.execute("SELECT * FROM well_production").df()
    conn.close()
    
    end_time = time.perf_counter()
    metrics_end = get_process_metrics()
    
    duration = end_time - start_time
    return {
        "operation": "DuckDB Read",
        "duration_s": duration,
        "records_processed": len(df),
        "throughput_rps": len(df) / duration if duration > 0 else 0,
        "cpu_usage": metrics_end["cpu_percent"] - metrics_start["cpu_percent"],
        "memory_usage_mb": metrics_end["memory_mb"] - metrics_start["memory_mb"]
    }

def print_results_table(results: List[Dict[str, Any]]):
    """Print benchmark results in a formatted table."""
    headers = ["Operation", "Duration (s)", "Records", "Throughput (rps)", "CPU %", "Memory (MB)"]
    rows = []
    
    for result in results:
        rows.append([
            result.get("operation", "N/A"),
            f"{result['duration_s']:.2f}",
            result.get("records_processed", 0),
            f"{result['throughput_rps']:.2f}",
            f"{result['cpu_usage']:.1f}",
            f"{result['memory_usage_mb']:.1f}"
        ])
    
    # Calculate column widths
    col_widths = [max(len(str(cell)) for cell in col) for col in zip(headers, *rows)]
    
    # Print header
    print("\nBenchmark Results:")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    print("| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")
    
    # Print rows
    for row in rows:
        print("| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |")
    
    print("+" + "+".join("-" * (w + 2) for w in col_widths) + "+")

def main():
    """Run the benchmark suite."""
    print("Starting IO Benchmark Suite...")
    
    # Create temporary database file
    temp_dir = tempfile.gettempdir()
    db_path = os.path.join(temp_dir, "benchmark.duckdb")
    
    # Generate test data
    test_data = generate_test_data()
    print(f"Generated {len(test_data)} test records.")
    
    results = []
    
    # Run write benchmark
    write_result = benchmark_duckdb_write(test_data, db_path)
    results.append(write_result)
    
    # Run read benchmark
    read_result = benchmark_duckdb_read(db_path)
    results.append(read_result)
    
    # Print results
    print_results_table(results)
    
    # Cleanup
    try:
        os.remove(db_path)
    except:
        pass

if __name__ == "__main__":
    main()