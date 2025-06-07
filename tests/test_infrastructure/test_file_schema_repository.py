import pytest
from app.infrastructure.persistence.repositories.file_schema_repository import FileSchemaRepository
from app.domain.entities.schema import Schema

@pytest.fixture
def repository():
    return FileSchemaRepository()

def test_get_schema_by_name_existing(repository):
    # Get an actual existing schema name first
    existing_names = repository.list_schema_names()
    if existing_names:
        schema_name = existing_names[0]  # Use the first available schema
        schema = repository.get_schema_by_name(schema_name)
        assert schema is not None
        assert isinstance(schema, Schema)
    else:
        pytest.skip("No schemas available for testing")

def test_get_schema_by_name_nonexistent(repository):
    schema = repository.get_schema_by_name("nonexistent_schema")
    assert schema is None

def test_get_all(repository):
    schemas = repository.get_all()
    assert isinstance(schemas, list)
    assert all(isinstance(schema, Schema) for schema in schemas)

def test_list_schema_names(repository):
    names = repository.list_schema_names()
    assert isinstance(names, list)
    assert all(isinstance(name, str) for name in names)

def test_schema_exists(repository):
    # Test with an actual existing schema
    existing_names = repository.list_schema_names()
    if existing_names:
        assert repository.schema_exists(existing_names[0]) is True
    
    # Test nonexistent schema
    assert repository.schema_exists("nonexistent_schema") is False