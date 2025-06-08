import sys
import os
import pytest
import tempfile
from unittest.mock import Mock

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from fastapi.testclient import TestClient
    from app.main import app
    from app.domain.entities.schema import Schema, SchemaProperty
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    TestClient = None
    app = None
    Schema = None
    SchemaProperty = None


@pytest.fixture
def client():
    """Create a FastAPI test client."""
    if FASTAPI_AVAILABLE and TestClient and app:
        return TestClient(app)
    else:
        pytest.skip("FastAPI or dependencies not available")


@pytest.fixture
def sample_schema():
    """Create a sample schema for testing."""
    if not FASTAPI_AVAILABLE or not Schema or not SchemaProperty:
        pytest.skip("Domain entities not available")
    
    return Schema(
        name="test_schema",
        description="Test schema for unit tests",
        table_name="test_table",
        properties=[
            SchemaProperty(
                name="id",
                type="integer",
                db_type="BIGINT",
                required=True
            ),
            SchemaProperty(
                name="name",
                type="string",
                db_type="VARCHAR",
                required=True
            ),
            SchemaProperty(
                name="value",
                type="number",
                db_type="DOUBLE",
                required=False
            )
        ]
    )


@pytest.fixture
def mock_schema_repository():
    """Create a mock schema repository."""
    return Mock()


@pytest.fixture
def mock_data_repository():
    """Create a mock data repository."""
    return Mock()


@pytest.fixture
def temp_db_path():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp:
        db_path = tmp.name
    yield db_path
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return {
        "id": 1,
        "name": "Test Record",
        "value": 123.45
    }