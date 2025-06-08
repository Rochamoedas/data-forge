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

# Global configuration
BASE_URL = "http://localhost:8080/api/v1"
TEST_SCHEMA = "well_production"
MOCKED_RESPONSE_FILE = "external/mocked_response_10K.json"
MAX_PAGE_SIZE = 1000  # Reduced page size for better testing
BATCH_SIZE = 1000     # Reduced batch size for better testing
TIMEOUT = 30  # Timeout in seconds

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

@measure_time
def test_bulk_insert_100k():
    """Test bulk insertion of 100K records and measure time"""
    # Stream all records from the file
    records = []
    with open(MOCKED_RESPONSE_FILE, 'rb') as f:
        parser = ijson.items(f, 'value.item')
        for item in parser:
            records.append(item)
    
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
            headers={'Content-Type': 'application/json'},
            data=json.dumps(new_records, cls=DecimalEncoder)
        )
        assert response.status_code == 201
        data = response.json()
        assert data["success"] is True
        total_inserted += len(batch)
        print(f"Inserted batch of {len(batch)} records. Total: {total_inserted}/{total_records}")
    
    return {"total_inserted": total_inserted}

@measure_time
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
                params={"page": page, "size": MAX_PAGE_SIZE},
                timeout=10
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
    
    return {"total_records": len(all_records)}

@measure_time
def test_stream_all_records():
    """Test streaming all records and measure time"""
    all_records = []
    offset = 0
    start_time = time.time()
    
    while True:
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(f"Test exceeded timeout of {TIMEOUT} seconds")
            
        try:
            response = requests.get(
                f"{BASE_URL}/records/{TEST_SCHEMA}/stream",
                params={"offset": offset, "limit": MAX_PAGE_SIZE},
                stream=True,
                timeout=10
            )
            response.raise_for_status()
            
            if response.headers.get("content-type") != "application/x-ndjson":
                raise ValueError(f"Unexpected content type: {response.headers.get('content-type')}")
            
            batch_records = []
            for line in response.iter_lines():
                if line:
                    try:
                        record = json.loads(line)
                        batch_records.append(record)
                    except json.JSONDecodeError as e:
                        raise ValueError(f"Failed to parse JSON from stream: {str(e)}")
            
            all_records.extend(batch_records)
            print(f"Streamed batch of {len(batch_records)} records. Total: {len(all_records)}")
            
            if len(batch_records) < MAX_PAGE_SIZE:
                break
                
            offset += len(batch_records)
            
        except RequestException as e:
            raise Exception(f"Request failed: {str(e)}")
    
    return {"total_records": len(all_records)}

@measure_time
def test_count_records():
    """Test counting all records and measure time"""
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}/count")
    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    return data

def test_performance_metrics():
    """Run all performance tests and collect metrics"""
    print("\n=== Performance Test Results ===")
    
    # Test bulk insert
    print("\nTesting Bulk Insert of 100K records...")
    bulk_insert_result, bulk_insert_time, bulk_insert_memory = test_bulk_insert_100k()
    
    # Test get all records
    print("\nTesting Get All Records...")
    get_all_result, get_all_time, get_all_memory = test_get_all_records()
    
    # Test stream all records
    print("\nTesting Stream All Records...")
    stream_all_result, stream_all_time, stream_all_memory = test_stream_all_records()
    
    # Test count records
    print("\nTesting Count Records...")
    count_result, count_time, count_memory = test_count_records()
    
    # Print summary
    print("\n=== Performance Summary ===")
    print(f"Bulk Insert:")
    print(f"  Time: {bulk_insert_time:.2f} seconds")
    print(f"  Memory: {bulk_insert_memory:.2f} MB")
    print(f"  Rate: {bulk_insert_result['total_inserted']/bulk_insert_time:.2f} records/second")
    
    print(f"\nGet All Records:")
    print(f"  Time: {get_all_time:.2f} seconds")
    print(f"  Memory: {get_all_memory:.2f} MB")
    print(f"  Rate: {get_all_result['total_records']/get_all_time:.2f} records/second")
    
    print(f"\nStream All Records:")
    print(f"  Time: {stream_all_time:.2f} seconds")
    print(f"  Memory: {stream_all_memory:.2f} MB")
    print(f"  Rate: {stream_all_result['total_records']/stream_all_time:.2f} records/second")
    
    print(f"\nCount Records:")
    print(f"  Time: {count_time:.2f} seconds")
    print(f"  Memory: {count_memory:.2f} MB")

if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 