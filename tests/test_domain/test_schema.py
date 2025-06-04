# tests/test_domain/test_schema.py
import pytest
from app.domain.entities.schema import Schema, SchemaField

def test_schema_creation():
    schema = Schema(
        name="test_schema",
        fields=[
            SchemaField(name="id", type="UUID"),
            SchemaField(name="name", type="STRING")
        ]
    )
    assert schema.name == "test_schema"
    assert len(schema.fields) == 2
    assert schema.fields[0].name == "id"
    assert schema.fields[1].type == "STRING"

def test_schema_get_field_names():
    schema = Schema(
        name="test_schema",
        fields=[
            SchemaField(name="field1", type="STRING"),
            SchemaField(name="field2", type="INTEGER")
        ]
    )
    assert schema.get_field_names() == ["field1", "field2"]