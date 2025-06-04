# tests/test_infrastructure/test_schemas.py
import pytest
from app.infrastructure.metadata.schemas_description import (
    fields_prices_schema, 
    well_production_schema, 
    get_schema_by_name,
    list_available_schemas,
    ALL_SCHEMAS
)

def test_fields_prices_schema():
    """Test the fields prices schema definition."""
    schema = fields_prices_schema
    
    assert schema.name == "fields_prices"
    assert len(schema.fields) == 6
    
    field_names = schema.get_field_names()
    expected_fields = [
        "id", "field_code", "field_name", 
        "production_period", "price_brl_m3", "price_brl_mmcf"
    ]
    assert field_names == expected_fields
    
    # Check specific field types
    id_field = next(f for f in schema.fields if f.name == "id")
    assert id_field.type == "UUID"
    
    price_field = next(f for f in schema.fields if f.name == "price_brl_m3")
    assert price_field.type == "DOUBLE"

def test_well_production_schema():
    """Test the well production schema definition."""
    schema = well_production_schema
    
    assert schema.name == "well_production"
    assert len(schema.fields) == 15
    
    field_names = schema.get_field_names()
    assert "well_code" in field_names
    assert "oil_production_kbd" in field_names
    assert "gas_production_mmcfd" in field_names
    
    # Check specific field types
    oil_field = next(f for f in schema.fields if f.name == "oil_production_kbd")
    assert oil_field.type == "DOUBLE"
    
    days_field = next(f for f in schema.fields if f.name == "days_on_production")
    assert days_field.type == "INTEGER"

def test_get_schema_by_name():
    """Test getting schemas by name."""
    fields_schema = get_schema_by_name("fields_prices")
    assert fields_schema.name == "fields_prices"
    
    well_schema = get_schema_by_name("well_production")
    assert well_schema.name == "well_production"
    
    # Test non-existent schema
    with pytest.raises(ValueError, match="Schema 'nonexistent' not found"):
        get_schema_by_name("nonexistent")

def test_list_available_schemas():
    """Test listing available schemas."""
    schema_names = list_available_schemas()
    assert "fields_prices" in schema_names
    assert "well_production" in schema_names
    assert len(schema_names) == 2

def test_all_schemas_registry():
    """Test the ALL_SCHEMAS registry."""
    assert len(ALL_SCHEMAS) == 2
    assert "fields_prices" in ALL_SCHEMAS
    assert "well_production" in ALL_SCHEMAS