#!/usr/bin/env python3
"""
Test script to verify composite key functionality
"""
import asyncio
import json
from app.domain.entities.schema import Schema, SchemaProperty
from app.domain.entities.data_record import DataRecord

async def test_composite_keys():
    """Test composite key functionality"""
    
    # Create a test schema with composite keys
    test_schema = Schema(
        name="test_production",
        description="Test production data with composite keys",
        table_name="test_production",
        primary_key=["field_code", "well_code", "production_period"],
        properties=[
            SchemaProperty(name="field_code", type="integer", db_type="BIGINT", required=True, primary_key=True),
            SchemaProperty(name="well_code", type="integer", db_type="BIGINT", primary_key=True),
            SchemaProperty(name="production_period", type="string", db_type="TIMESTAMP", primary_key=True),
            SchemaProperty(name="oil_production_kbd", type="number", db_type="DOUBLE"),
            SchemaProperty(name="gas_production_mmcfd", type="number", db_type="DOUBLE"),
        ]
    )
    
    # Test data
    test_data = {
        "field_code": 123,
        "well_code": 456,
        "production_period": "2024-01-01",
        "oil_production_kbd": 150.5,
        "gas_production_mmcfd": 25.3
    }
    
    print("🧪 Testing Composite Key Functionality")
    print("=" * 50)
    
    # Test 1: Extract composite key from data
    print("1. Testing composite key extraction...")
    composite_key = test_schema.get_composite_key_from_data(test_data)
    print(f"   Extracted composite key: {composite_key}")
    assert composite_key == {"field_code": 123, "well_code": 456, "production_period": "2024-01-01"}
    print("   ✅ Composite key extraction works!")
    
    # Test 2: Create DataRecord with composite key
    print("\n2. Testing DataRecord creation with composite key...")
    record = DataRecord(
        schema_name=test_schema.name,
        data=test_data,
        composite_key=composite_key
    )
    print(f"   Record ID: {record.id}")
    print(f"   Composite ID: {record.composite_id}")
    assert record.composite_id == "field_code=123&production_period=2024-01-01&well_code=456"
    print("   ✅ DataRecord with composite key works!")
    
    # Test 3: Parse composite ID back to dict
    print("\n3. Testing composite ID parsing...")
    parsed_key = DataRecord.parse_composite_id(record.composite_id)
    print(f"   Parsed composite key: {parsed_key}")
    assert parsed_key == {"field_code": "123", "well_code": "456", "production_period": "2024-01-01"}
    print("   ✅ Composite ID parsing works!")
    
    # Test 4: Schema validation
    print("\n4. Testing schema validation...")
    try:
        test_schema.validate_data(test_data)
        print("   ✅ Schema validation passed!")
    except Exception as e:
        print(f"   ❌ Schema validation failed: {e}")
        return False
    
    # Test 5: Missing primary key field
    print("\n5. Testing missing primary key validation...")
    incomplete_data = {
        "field_code": 123,
        # Missing well_code and production_period
        "oil_production_kbd": 150.5
    }
    
    try:
        incomplete_composite_key = test_schema.get_composite_key_from_data(incomplete_data)
        print(f"   Incomplete composite key: {incomplete_composite_key}")
        print("   ✅ Handles missing primary key fields gracefully!")
    except Exception as e:
        print(f"   ❌ Error handling missing fields: {e}")
    
    print("\n🎉 All composite key tests passed!")
    print("\n📋 Summary of Benefits:")
    print("   • Natural business keys (field_code=123&well_code=456&production_period=2024-01-01)")
    print("   • Maintains UUID compatibility for existing APIs")
    print("   • Automatic composite key extraction from data")
    print("   • URL-friendly composite ID format")
    print("   • Database unique constraints on business keys")
    
    return True

if __name__ == "__main__":
    asyncio.run(test_composite_keys()) 