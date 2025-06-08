import json
import pytest
import requests
from datetime import datetime
from typing import Dict, List
import uuid

# Global configuration
BASE_URL = "http://localhost:8080/api/v1"
TEST_SCHEMA = "well_production"

# Load mocked response data
with open("external/mocked_response_100K.json", "r") as f:
    MOCKED_RESPONSE = json.load(f)

def get_test_data() -> List[Dict]:
    """Extract test data from mocked response"""
    return MOCKED_RESPONSE["value"]

def test_get_schemas():
    """Test GET /schemas endpoint"""
    response = requests.get(f"{BASE_URL}/schemas")
    assert response.status_code == 200
    schemas = response.json()
    assert isinstance(schemas, list)
    assert any(schema["name"] == TEST_SCHEMA for schema in schemas)

def test_get_records():
    """Test GET /records/{schema_name} endpoint"""
    # Test basic pagination
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}?page=1&size=10")
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert "items" in data["data"]
    assert "total" in data["data"]
    assert "page" in data["data"]
    assert "size" in data["data"]

    # Test with filters
    filters = [{"field": "field_code", "operator": "eq", "value": "22"}]
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}",
        params={"filters": json.dumps(filters)}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]["items"]) > 0

    # Test with sorting
    sort = [{"field": "production_period", "order": "desc"}]
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}",
        params={"sort": json.dumps(sort)}
    )
    assert response.status_code == 200

def test_stream_records():
    """Test GET /records/{schema_name}/stream endpoint"""
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}/stream",
        params={"limit": 10},
        stream=True
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-ndjson"
    
    # Read first few lines
    lines = []
    for line in response.iter_lines():
        if len(lines) >= 2:  # Read only first 2 records
            break
        if line:
            record = json.loads(line)
            assert "id" in record
            assert "schema_name" in record
            assert "data" in record
            lines.append(record)

def test_count_records():
    """Test GET /records/{schema_name}/count endpoint"""
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}/count")
    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    assert isinstance(data["count"], int)

    # Test with filters
    filters = [{"field": "field_code", "operator": "gt", "value": "100"}]
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}/count",
        params={"filters": json.dumps(filters)}
    )
    assert response.status_code == 200

def test_create_record():
    """Test POST /records endpoint"""
    test_data = get_test_data()[0]  # Use first record as template
    new_record = {
        "schema_name": TEST_SCHEMA,
        "data": {
            "field_code": 999,
            "_field_name": "Test Field",
            "well_code": 888,
            "_well_reference": "TEST-1",
            "well_name": "Test Well",
            "production_period": datetime.now().isoformat(),
            "days_on_production": 30,
            "oil_production_kbd": 1.0,
            "gas_production_mmcfd": 1.0,
            "liquids_production_kbd": 0,
            "water_production_kbd": 0,
            "data_source": "Test Source",
            "source_data": "{}",
            "partition_0": "latest"
        }
    }

    response = requests.post(f"{BASE_URL}/records", json=new_record)
    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True
    assert "record" in data
    assert data["record"]["schema_name"] == TEST_SCHEMA

def test_create_bulk_records():
    """Test POST /records/bulk endpoint"""
    test_data = get_test_data()[:2]  # Use first two records as template
    new_records = {
        "schema_name": TEST_SCHEMA,
        "data": [
            {
                "field_code": 999,
                "_field_name": "Test Field 1",
                "well_code": 888,
                "_well_reference": "TEST-1",
                "well_name": "Test Well 1",
                "production_period": datetime.now().isoformat(),
                "days_on_production": 30,
                "oil_production_kbd": 1.0,
                "gas_production_mmcfd": 1.0,
                "liquids_production_kbd": 0,
                "water_production_kbd": 0,
                "data_source": "Test Source",
                "source_data": "{}",
                "partition_0": "latest"
            },
            {
                "field_code": 998,
                "_field_name": "Test Field 2",
                "well_code": 887,
                "_well_reference": "TEST-2",
                "well_name": "Test Well 2",
                "production_period": datetime.now().isoformat(),
                "days_on_production": 31,
                "oil_production_kbd": 2.0,
                "gas_production_mmcfd": 2.0,
                "liquids_production_kbd": 0,
                "water_production_kbd": 0,
                "data_source": "Test Source",
                "source_data": "{}",
                "partition_0": "latest"
            }
        ]
    }

    response = requests.post(f"{BASE_URL}/records/bulk", json=new_records)
    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True
    assert data["records_created"] == 2
    assert len(data["records"]) == 2

def test_get_record_by_id():
    """Test GET /records/{schema_name}/{record_id} endpoint"""
    # First create a record to get its ID
    test_data = get_test_data()[0]
    new_record = {
        "schema_name": TEST_SCHEMA,
        "data": test_data
    }
    
    create_response = requests.post(f"{BASE_URL}/records", json=new_record)
    assert create_response.status_code == 201
    record_id = create_response.json()["record"]["id"]

    # Now test getting the record by ID
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}/{record_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == record_id
    assert data["schema_name"] == TEST_SCHEMA

def test_error_handling():
    """Test error handling for various scenarios"""
    # Test invalid schema
    response = requests.get(f"{BASE_URL}/records/invalid_schema")
    assert response.status_code == 404

    # Test invalid record ID
    invalid_id = str(uuid.uuid4())
    response = requests.get(f"{BASE_URL}/records/{TEST_SCHEMA}/{invalid_id}")
    assert response.status_code == 404

    # Test invalid filter format
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}",
        params={"filters": "invalid_json"}
    )
    assert response.status_code == 400

    # Test invalid sort format
    response = requests.get(
        f"{BASE_URL}/records/{TEST_SCHEMA}",
        params={"sort": "invalid_json"}
    )
    assert response.status_code == 400

if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 