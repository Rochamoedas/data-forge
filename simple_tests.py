import json
import requests
import pytest
from typing import List, Dict, Any
from datetime import datetime
from uuid import uuid4

# API configuration
API_BASE_URL = "http://localhost:8080/api/v1"
SCHEMA_NAME = "production_data"  # You may need to adjust this based on your schema

def load_mocked_data() -> List[Dict[str, Any]]:
    """Load the mocked data from the JSON file"""
    with open("external/mocked_response.json", "r") as f:
        data = json.load(f)
        return data["value"]

def test_bulk_create_and_query():
    # Load the test data
    test_data = load_mocked_data()
    
    # Prepare the bulk create request
    bulk_create_url = f"{API_BASE_URL}/records/bulk"
    bulk_create_payload = {
        "schema_name": SCHEMA_NAME,
        "data": test_data
    }
    
    try:
        # Send the bulk create request
        create_response = requests.post(bulk_create_url, json=bulk_create_payload)
        create_response.raise_for_status()  # Raise an exception for bad status codes
        
        # Verify the bulk create response
        create_result = create_response.json()
        assert create_result["success"] is True
        assert create_result["records_created"] == len(test_data)
        print(f"Successfully created {create_result['records_created']} records")
        
        # Query the data to verify it was saved
        query_url = f"{API_BASE_URL}/records/{SCHEMA_NAME}"
        query_params = {
            "page": 1,
            "size": 100  # Adjust based on your data size
        }
        
        query_response = requests.get(query_url, params=query_params)
        query_response.raise_for_status()
        
        # Verify the query results
        query_result = query_response.json()
        assert query_result["success"] is True
        assert len(query_result["data"]["items"]) >= len(test_data)
        
        # Verify specific fields from the first record
        first_record = query_result["data"]["items"][0]
        assert "field_code" in first_record["data"]
        assert "well_code" in first_record["data"]
        assert "production_period" in first_record["data"]
        
        print("Successfully verified data through query")
        
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {str(e)}")
        if hasattr(e.response, 'text'):
            print(f"Response text: {e.response.text}")
        raise
    except AssertionError as e:
        print(f"Assertion failed: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise

def test_count_records():
    """Test the count endpoint"""
    try:
        count_url = f"{API_BASE_URL}/records/{SCHEMA_NAME}/count"
        count_response = requests.get(count_url)
        count_response.raise_for_status()
        
        count_result = count_response.json()
        assert count_result["success"] is True
        assert count_result["count"] > 0
        print(f"Successfully counted {count_result['count']} records")
        
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {str(e)}")
        if hasattr(e.response, 'text'):
            print(f"Response text: {e.response.text}")
        raise
    except AssertionError as e:
        print(f"Assertion failed: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise

if __name__ == "__main__":
    print("Starting tests...")
    try:
        test_bulk_create_and_query()
        test_count_records()
        print("All tests passed successfully!")
    except Exception as e:
        print(f"Tests failed: {str(e)}")
        exit(1) 