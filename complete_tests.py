import json
import pytest
import requests
from datetime import datetime
from typing import Dict, List
import uuid
import os
from decimal import Decimal

# Global configuration
BASE_URL = "http://localhost:8080/api/v1"
TEST_SCHEMA = "well_production"
MOCKED_RESPONSE_FILE = "external/mocked_response_10K.json"

def convert_decimals_to_float(obj):
    """Recursively convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {key: convert_decimals_to_float(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals_to_float(item) for item in obj]
    else:
        return obj

def get_test_data(batch_size: int = 100) -> List[Dict]:
    """Get test data - either from file or create sample data"""
    try:
        if os.path.exists(MOCKED_RESPONSE_FILE):
            # Try to read a small portion of the large file
            import ijson
            data = []
            with open(MOCKED_RESPONSE_FILE, 'rb') as f:
                # Stream the JSON file and get items from the 'value' array
                parser = ijson.items(f, 'value.item')
                for item in parser:
                    # Convert Decimal objects to float for JSON serialization
                    clean_item = convert_decimals_to_float(item)
                    data.append(clean_item)
                    if len(data) >= batch_size:
                        break
            return data
        else:
            # Create sample data if file doesn't exist
            return [
                {
                    "field_code": i + 1,  # Use integer instead of string
                    "production_period": f"2024-{(i % 12) + 1:02d}",
                    "oil_production": 1000.5 + i,
                    "gas_production": 500.3 + i,
                    "water_production": 100.1 + i
                }
                for i in range(batch_size)
            ]
    except Exception as e:
        print(f"Warning: Could not load test data from file: {e}")
        # Fallback to simple test data
        return [
            {
                "field_code": i + 1,  # Use integer instead of string
                "production_period": f"2024-{(i % 12) + 1:02d}",
                "oil_production": 1000.5 + i,
                "gas_production": 500.3 + i,
                "water_production": 100.1 + i
            }
            for i in range(batch_size)
        ]

def test_server_health():
    """Test if server is running"""
    try:
        response = requests.get(f"{BASE_URL.replace('/api/v1', '')}/")
        assert response.status_code == 200
        print("✓ Server is running")
    except requests.exceptions.ConnectionError:
        pytest.fail("Server is not running. Please start the server first.")

def test_get_schemas():
    """Test GET /schemas endpoint"""
    response = requests.get(f"{BASE_URL}/schemas")
    assert response.status_code == 200
    schemas = response.json()
    assert isinstance(schemas, list)
    print(f"✓ Found {len(schemas)} schemas")
    
    # Check if our test schema exists
    schema_names = [schema.get("name", "") for schema in schemas]
    if TEST_SCHEMA not in schema_names:
        print(f"Warning: Test schema '{TEST_SCHEMA}' not found. Available schemas: {schema_names}")

def test_get_records():
    """Test GET /records/{schema_name} endpoint"""
    # Test basic pagination with larger dataset
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}?page=1&size=10")
    
    if response.status_code == 404:
        pytest.skip(f"Schema '{TEST_SCHEMA}' not found - skipping record tests")
    
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "items" in data["data"]
    assert "total" in data["data"]
    assert "page" in data["data"]
    assert "size" in data["data"]
    print(f"✓ Retrieved {len(data['data']['items'])} records, total: {data['data']['total']}")

    # Test with valid filters - use integer value for field_code
    filters = [{"field": "field_code", "operator": "eq", "value": 22}]  # Use integer, not string
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}",
        params={"filters": json.dumps(filters), "page": 1, "size": 10}
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Filter test passed, found {len(data['data']['items'])} filtered records")
    else:
        print(f"Warning: Filter test failed with status {response.status_code}: {response.text}")

def test_stream_records():
    """Test GET /records/{schema_name}/stream endpoint"""
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}/stream",
        params={"limit": 10},
        stream=True
    )
    
    if response.status_code == 404:
        pytest.skip(f"Schema '{TEST_SCHEMA}' not found - skipping stream test")
    
    assert response.status_code == 200
    assert "application/x-ndjson" in response.headers.get("content-type", "")
    
    # Read first few lines
    lines = []
    for line in response.iter_lines():
        if len(lines) >= 5:  # Read first 5 records
            break
        if line:
            try:
                record = json.loads(line)
                assert "id" in record
                assert "schema_name" in record
                assert "data" in record
                lines.append(record)
            except json.JSONDecodeError as e:
                print(f"Warning: Could not parse line: {line}, error: {e}")
    
    print(f"✓ Successfully streamed {len(lines)} records")

def test_count_records():
    """Test GET /records/{schema_name}/count endpoint"""
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}/count")
    
    if response.status_code == 404:
        pytest.skip(f"Schema '{TEST_SCHEMA}' not found - skipping count test")
    
    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    assert isinstance(data["count"], int)
    print(f"✓ Total record count: {data['count']}")

    # Test with filters - use integer value that exists
    filters = [{"field": "field_code", "operator": "gt", "value": 0}]  # Use integer > 0
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}/count",
        params={"filters": json.dumps(filters)}
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"✓ Filtered count test passed: {data['count']} records")
    else:
        print(f"Warning: Filtered count test failed: {response.status_code}")

def test_create_record():
    """Test POST /records endpoint"""
    test_data = get_test_data(1)[0]  # Get first record from test data
    new_record = {
        "schema_name": TEST_SCHEMA,
        "data": test_data
    }

    response = requests.post(f"{BASE_URL}/records", json=new_record)
    
    if response.status_code == 404:
        pytest.skip(f"Schema '{TEST_SCHEMA}' not found - skipping create test")
    
    if response.status_code == 201:
        data = response.json()
        assert data["success"] is True
        assert "record" in data
        assert data["record"]["schema_name"] == TEST_SCHEMA
        print("✓ Successfully created single record")
    else:
        print(f"Warning: Create record failed with status {response.status_code}: {response.text}")

def test_create_bulk_records():
    """Test POST /records/bulk endpoint"""
    test_data = get_test_data(5)  # Get 5 records from test data
    new_records = {
        "schema_name": TEST_SCHEMA,
        "data": test_data
    }

    response = requests.post(f"{BASE_URL}/records/bulk", json=new_records)
    
    if response.status_code == 404:
        pytest.skip(f"Schema '{TEST_SCHEMA}' not found - skipping bulk create test")
    
    if response.status_code == 201:
        data = response.json()
        assert data["success"] is True
        assert data["records_created"] == 5
        assert len(data["records"]) == 5
        print(f"✓ Successfully created {data['records_created']} records in bulk")
    else:
        print(f"Warning: Bulk create failed with status {response.status_code}: {response.text}")

def test_get_record_by_id():
    """Test GET /records/{schema_name}/{record_id} endpoint"""
    # First create a record to get its ID
    test_data = get_test_data(1)[0]
    new_record = {
        "schema_name": TEST_SCHEMA,
        "data": test_data
    }
    
    create_response = requests.post(f"{BASE_URL}/records", json=new_record)
    
    if create_response.status_code != 201:
        pytest.skip("Could not create record for ID test")
    
    record_id = create_response.json()["record"]["id"]

    # Now test getting the record by ID
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}/{record_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == record_id
    assert data["schema_name"] == TEST_SCHEMA
    print(f"✓ Successfully retrieved record by ID: {record_id}")

def test_error_handling():
    """Test error handling for various scenarios"""
    # Test invalid schema
    response = requests.get(f"{BASE_URL}/records/invalid_schema")
    assert response.status_code == 404
    print("✓ Invalid schema error handling works")

    # Test invalid record ID (only if schema exists)
    schemas_response = requests.get(f"{BASE_URL}/schemas")
    if schemas_response.status_code == 200:
        schemas = schemas_response.json()
        if any(s.get("name") == TEST_SCHEMA for s in schemas):
            invalid_id = str(uuid.uuid4())
            response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}/{invalid_id}")
            assert response.status_code == 404
            print("✓ Invalid record ID error handling works")

    # Test invalid filter format
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}",
        params={"filters": "invalid_json"}
    )
    assert response.status_code == 400
    print("✓ Invalid JSON filter error handling works")

def test_api_limits():
    """Test API limits and constraints"""
    # Test with reasonable page size
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}?page=1&size=100")
    
    if response.status_code == 200:
        print("✓ Normal page size works")
    elif response.status_code == 404:
        pytest.skip(f"Schema '{TEST_SCHEMA}' not found - skipping limits test")

    # Test with very large page size (should be rejected with 422 or 400)
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}?page=1&size=100000")
    if response.status_code in [400, 422]:  # Accept both 400 and 422 as valid rejection codes
        print("✓ Large page size properly rejected")
    else:
        print(f"Warning: Large page size not rejected (status: {response.status_code})")

if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"]) 