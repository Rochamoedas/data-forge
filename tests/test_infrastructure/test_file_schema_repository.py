# tests/test_infrastructure/test_file_schema_repository.py
import pytest
from unittest.mock import Mock
from app.infrastructure.persistence.repositories.file_schema_repository import FileSchemaRepository

class MockFileSchemaRepository(FileSchemaRepository):
    def __init__(self):
        # Create mock objects with configured name attributes
        fields_prices_mock = Mock()
        fields_prices_mock.name = "fields_prices"
        
        well_production_mock = Mock()
        well_production_mock.name = "well_production"
        
        self.schemas = {
            "fields_prices": fields_prices_mock,
            "well_production": well_production_mock
        }
    
    def get_schema_by_name(self, name):
        return self.schemas.get(name)
    
    def get_all(self):
        return list(self.schemas.values())
    
    def list_schema_names(self):
        return list(self.schemas.keys())
    
    def schema_exists(self, name):
        return name in self.schemas

@pytest.fixture
def schema_repo():
    return MockFileSchemaRepository()

def test_get_schema_by_name(schema_repo):
    """Test getting a schema by name."""
    schema = schema_repo.get_schema_by_name("fields_prices")
    assert schema is not None
    assert schema.name == "fields_prices"
    
    # Test non-existent schema
    schema = schema_repo.get_schema_by_name("nonexistent")
    assert schema is None

def test_get_all_schemas(schema_repo):
    """Test getting all schemas."""
    schemas = schema_repo.get_all()
    assert len(schemas) == 2
    schema_names = [s.name for s in schemas]
    assert "fields_prices" in schema_names
    assert "well_production" in schema_names

def test_list_schema_names(schema_repo):
    """Test listing schema names."""
    names = schema_repo.list_schema_names()
    assert "fields_prices" in names
    assert "well_production" in names

def test_schema_exists(schema_repo):
    """Test checking if schema exists."""
    assert schema_repo.schema_exists("fields_prices") == True
    assert schema_repo.schema_exists("well_production") == True
    assert schema_repo.schema_exists("nonexistent") == False