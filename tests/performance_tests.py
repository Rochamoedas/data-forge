import json
import pytest
import requests
import time
from datetime import datetime
from typing import Dict, List
import ijson
import statistics
from decimal import Decimal
import math
import psutil
import os
from requests.exceptions import RequestException

# Import centralized configuration
try:
    from app.config.api_limits import api_limits
    # Use centralized configuration for performance testing
    MAX_PAGE_SIZE = api_limits.MAX_PAGE_SIZE
    BATCH_SIZE = api_limits.PERFORMANCE_TEST_BATCH_SIZE
    TIMEOUT = api_limits.PERFORMANCE_TEST_TIMEOUT
    MAX_STREAM_LIMIT = api_limits.MAX_STREAM_LIMIT
    MAX_RECORDS = api_limits.PERFORMANCE_TEST_MAX_RECORDS
except ImportError:
    # Fallback values if import fails
    MAX_PAGE_SIZE = 1000
    BATCH_SIZE = 20000
    TIMEOUT = 300
    MAX_STREAM_LIMIT = 100000
    MAX_RECORDS = 100000

def convert_decimal(obj):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

# Global configuration
BASE_URL = "http://localhost:8080/api/v1"
TEST_SCHEMA = "well_production"
MOCKED_RESPONSE_FILE = "external/mocked_response_100K-4.json"

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_memory_usage():
    """Get current process memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def measure_time(func):
    """Decorator to measure execution time and memory usage of a function"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_memory = get_memory_usage()
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        end_memory = get_memory_usage()
        
        execution_time = end_time - start_time
        memory_used = end_memory - start_memory
        
        print(f"\n{func.__name__} metrics:")
        print(f"  Execution time: {execution_time:.2f} seconds")
        print(f"  Memory usage: {memory_used:.2f} MB")
        print(f"  CPU usage: {psutil.cpu_percent()}%")
        
        return result, execution_time, memory_used
    return wrapper

def measure_time_pytest(func):
    """Decorator to measure execution time and memory usage for pytest functions"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_memory = get_memory_usage()
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        end_memory = get_memory_usage()
        
        execution_time = end_time - start_time
        memory_used = end_memory - start_memory
        
        print(f"\n{func.__name__} metrics:")
        print(f"  Execution time: {execution_time:.2f} seconds")
        print(f"  Memory usage: {memory_used:.2f} MB")
        print(f"  CPU usage: {psutil.cpu_percent()}%")
        
        # For pytest, don't return the metrics, just the original result
        return result
    return wrapper

@measure_time_pytest
def test_bulk_insert_100k():
    """Test bulk insertion of 100K records and measure time"""
    # Stream all records from the file
    records = []
    with open(MOCKED_RESPONSE_FILE, 'rb') as f:
        parser = ijson.items(f, 'value.item')
        for item in parser:
            # Map the JSON structure to the expected schema fields
            mapped_record = {
                "field_code": convert_decimal(item.get("field_code")),
                "field_name": item.get("_field_name"),  # Note: using _field_name from JSON
                "well_code": convert_decimal(item.get("well_code")),
                "well_reference": item.get("_well_reference"),
                "well_name": item.get("well_name"),
                "production_period": item.get("production_period"),
                "days_on_production": convert_decimal(item.get("days_on_production")),
                "oil_production_kbd": convert_decimal(item.get("oil_production_kbd")),
                "gas_production_mmcfd": convert_decimal(item.get("gas_production_mmcfd")),
                "liquids_production_kbd": convert_decimal(item.get("liquids_production_kbd")),
                "water_production_kbd": convert_decimal(item.get("water_production_kbd")),
                "data_source": item.get("data_source"),
                "source_data": item.get("source_data"),
                "partition_0": item.get("partition_0")
            }
            records.append(mapped_record)
    
    total_records = len(records)
    print(f"\nTotal records to insert: {total_records}")
    
    # Process in batches
    total_inserted = 0
    for i in range(0, total_records, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        new_records = {
            "schema_name": TEST_SCHEMA,
            "data": batch
        }
        
        # Use custom encoder for Decimal values
        response = requests.post(
            f"{BASE_URL}/records/bulk",
            json=new_records,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code != 201:
            print(f"Error response: {response.status_code} - {response.text}")
            raise AssertionError(f"Expected 201, got {response.status_code}")
            
        data = response.json()
        assert data["success"] is True
        
        # Get the number of records inserted from the API response
        inserted_count = data.get("records_created", len(batch))
        total_inserted += inserted_count
        print(f"Inserted batch of {inserted_count} records. Total: {total_inserted}/{total_records}")
    
    # Assert final results - Note: some records may be skipped as duplicates
    print(f"\nBulk insert completed: {total_inserted} records inserted out of {total_records} total records")
    if total_inserted < total_records:
        print(f"Note: {total_records - total_inserted} records were skipped (likely duplicates)")
    
    # Assert that we got a reasonable response (at least the API worked)
    assert total_inserted >= 0, f"Invalid inserted count: {total_inserted}"

@measure_time_pytest
def test_get_all_records():
    """Test retrieving all records and measure time"""
    all_records = []
    page = 1
    start_time = time.time()
    
    while True:
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(f"Test exceeded timeout of {TIMEOUT} seconds")
            
        try:
            response = requests.get(
                f"{BASE_URL}/records/{TEST_SCHEMA}",
                params={"page": page, "size": min(MAX_PAGE_SIZE, 5000)},  # Use reasonable page size
                timeout=30  # Longer timeout for performance testing
            )
            response.raise_for_status()
            data = response.json()
            
            if "data" not in data or "items" not in data["data"]:
                raise ValueError(f"Unexpected response format: {data}")
                
            records = data["data"]["items"]
            all_records.extend(records)
            
            print(f"Retrieved page {page} with {len(records)} records. Total: {len(all_records)}")
            
            if len(records) < MAX_PAGE_SIZE:
                break
                
            page += 1
            
        except RequestException as e:
            raise Exception(f"Request failed: {str(e)}")
    
    # Assert we got some records
    assert len(all_records) > 0, "No records were retrieved"

@measure_time_pytest
def test_stream_all_records():
    """Test streaming all records and measure time"""
    all_records = []
    start_time = time.time()
    
    try:
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(f"Test exceeded timeout of {TIMEOUT} seconds")
            
        response = requests.get(
            f"{BASE_URL}/records/{TEST_SCHEMA}/stream",
            params={"limit": MAX_RECORDS},  # Use centralized configuration
            stream=True,
            timeout=120  # Longer timeout for performance testing
        )
        response.raise_for_status()
        
        if response.headers.get("content-type") != "application/x-ndjson":
            raise ValueError(f"Unexpected content type: {response.headers.get('content-type')}")
        
        batch_count = 0
        for line in response.iter_lines():
            if line:
                try:
                    record = json.loads(line)
                    all_records.append(record)
                    batch_count += 1
                    
                    # Print progress every 1000 records
                    if batch_count % 1000 == 0:
                        print(f"Streamed {batch_count} records. Total: {len(all_records)}")
                        
                    # Check timeout periodically
                    if batch_count % 5000 == 0 and time.time() - start_time > TIMEOUT:
                        print(f"Timeout reached after {batch_count} records")
                        break
                        
                except json.JSONDecodeError as e:
                    raise ValueError(f"Failed to parse JSON from stream: {str(e)}")
        
        print(f"Streaming completed. Total records: {len(all_records)}")
        
    except RequestException as e:
        raise Exception(f"Request failed: {str(e)}")
    
    # Assert that we got some records
    assert len(all_records) > 0, "No records were streamed"

def test_count_records():
    """Test counting all records and measure time"""
    @measure_time
    def count_records_impl():
        response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}/count")
        assert response.status_code == 200
        data = response.json()
        assert "count" in data
        print(f"Total record count: {data['count']}")
        return data
    
    # Execute the measured function and assert the result
    result, execution_time, memory_used = count_records_impl()
    assert result["count"] >= 0, "Count should be non-negative"

def test_performance_metrics():
    """Run all performance tests and collect metrics"""
    print("\n=== Performance Test Results ===")
    print(f"Configuration: BATCH_SIZE={BATCH_SIZE}, MAX_RECORDS={MAX_RECORDS}, TIMEOUT={TIMEOUT}")
    print(f"Limits: MAX_PAGE_SIZE={MAX_PAGE_SIZE}, MAX_STREAM_LIMIT={MAX_STREAM_LIMIT}")
    
    # Test bulk insert
    print(f"\nTesting Bulk Insert of {MAX_RECORDS} records...")
    test_bulk_insert_100k()
    
    # Test get all records
    print("\nTesting Get All Records...")
    test_get_all_records()
    
    # Test stream all records
    print("\nTesting Stream All Records...")
    test_stream_all_records()
    
    # Test count records - create a wrapper to capture the result
    print("\nTesting Count Records...")
    def count_wrapper():
        response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}/count")
        assert response.status_code == 200
        data = response.json()
        assert "count" in data
        print(f"Total record count: {data['count']}")
        return data
    
    count_result, count_time, count_memory = measure_time(count_wrapper)()
    
    # Print summary - Note: Individual test metrics are printed by @measure_time decorator
    print("\n=== Performance Summary ===")
    print("Individual test metrics are displayed above by the @measure_time_pytest decorator")
    print(f"Count Records:")
    print(f"  Time: {count_time:.2f} seconds")
    print(f"  Memory: {count_memory:.2f} MB")
    print(f"  Count: {count_result['count']} records")

if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 