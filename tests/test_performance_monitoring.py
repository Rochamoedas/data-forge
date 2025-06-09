#!/usr/bin/env python3
"""
Test script to verify performance monitoring improvements
"""

import requests
import time
import json
from typing import Dict, Any

BASE_URL = "http://localhost:8080/api/v1"
SCHEMA_NAME = "production_data"

def test_performance_monitoring():
    """Test that performance monitoring shows real metrics instead of zeros"""
    
    print("🧪 Testing Performance Monitoring Improvements")
    print("=" * 50)
    
    # Test 1: Small bulk insert
    print("\n1. Testing small bulk insert (100 records)...")
    small_data = [
        {
            "field_code": f"FIELD_{i:03d}",
            "field_name": f"Test Field {i}",
            "well_code": f"WELL_{i:03d}",
            "well_reference": f"REF_{i:03d}",
            "well_name": f"Test Well {i}",
            "production_period": "2024-01",
            "days_on_production": 30,
            "oil_production_kbd": 100.5 + i,
            "gas_production_mmcfd": 50.2 + i,
            "liquids_production_kbd": 25.1 + i,
            "water_production_kbd": 10.0 + i,
            "data_source": "test",
            "source_data": "automated_test",
            "partition_0": "test_partition"
        }
        for i in range(100)
    ]
    
    start_time = time.perf_counter()
    response = requests.post(
        f"{BASE_URL}/records/bulk",
        json={
            "schema_name": SCHEMA_NAME,
            "data": small_data
        },
        headers={"Content-Type": "application/json"}
    )
    duration = (time.perf_counter() - start_time) * 1000
    
    if response.status_code == 201:
        result = response.json()
        print(f"   ✅ Success: {len(small_data)} records inserted in {duration:.2f}ms")
        if "execution_time_ms" in result:
            print(f"   📊 API reported time: {result['execution_time_ms']:.2f}ms")
        else:
            print("   ⚠️  No execution time in response")
    else:
        print(f"   ❌ Failed: {response.status_code} - {response.text}")
    
    # Test 2: High-performance bulk insert
    print("\n2. Testing high-performance bulk insert (100 records)...")
    start_time = time.perf_counter()
    response = requests.post(
        f"{BASE_URL}/high-performance/ultra-fast-bulk/{SCHEMA_NAME}",
        json=small_data,
        headers={"Content-Type": "application/json"}
    )
    duration = (time.perf_counter() - start_time) * 1000
    
    if response.status_code == 200:
        result = response.json()
        print(f"   ✅ Success: {len(small_data)} records inserted in {duration:.2f}ms")
        if "performance_metrics" in result:
            metrics = result["performance_metrics"]
            print(f"   📊 API reported time: {metrics.get('duration_ms', 0):.2f}ms")
            print(f"   📊 Throughput: {metrics.get('throughput_rps', 0):,} records/sec")
        else:
            print("   ⚠️  No performance metrics in response")
    else:
        print(f"   ❌ Failed: {response.status_code} - {response.text}")
    
    # Test 3: Query performance
    print("\n3. Testing query performance...")
    start_time = time.perf_counter()
    response = requests.get(f"{BASE_URL}/records/{SCHEMA_NAME}?page=1&size=50")
    duration = (time.perf_counter() - start_time) * 1000
    
    if response.status_code == 200:
        result = response.json()
        print(f"   ✅ Success: Query completed in {duration:.2f}ms")
        if "execution_time_ms" in result:
            print(f"   📊 API reported time: {result['execution_time_ms']:.2f}ms")
        else:
            print("   ⚠️  No execution time in response")
    else:
        print(f"   ❌ Failed: {response.status_code} - {response.text}")
    
    # Test 4: Count performance
    print("\n4. Testing count performance...")
    start_time = time.perf_counter()
    response = requests.get(f"{BASE_URL}/records/{SCHEMA_NAME}/count")
    duration = (time.perf_counter() - start_time) * 1000
    
    if response.status_code == 200:
        result = response.json()
        print(f"   ✅ Success: Count completed in {duration:.2f}ms")
        print(f"   📊 Total records: {result.get('count', 0):,}")
        if "execution_time_ms" in result:
            print(f"   📊 API reported time: {result['execution_time_ms']:.2f}ms")
        else:
            print("   ⚠️  No execution time in response")
    else:
        print(f"   ❌ Failed: {response.status_code} - {response.text}")
    
    print("\n" + "=" * 50)
    print("🎯 Performance monitoring test completed!")
    print("\n💡 Check the logs/app.log file to see if performance metrics are now showing real values instead of zeros.")
    print("   Look for entries like:")
    print("   - 'API Performance - function_name: Time: X.XXms, Memory: X.XXmb, CPU: X.X%'")
    print("   - 'Bulk Performance - operation_name: Records: X, Time: X.XXms, Throughput: X records/sec'")

if __name__ == "__main__":
    try:
        test_performance_monitoring()
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to the API server.")
        print("   Make sure the server is running on http://localhost:8080")
        print("   Start it with: python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload")
    except Exception as e:
        print(f"❌ Test failed with error: {e}") 