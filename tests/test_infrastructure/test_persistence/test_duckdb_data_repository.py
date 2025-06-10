# tests/test_infrastructure/test_persistence/test_duckdb_data_repository.py
import unittest
from unittest.mock import patch, MagicMock, AsyncMock, call
import uuid
from datetime import datetime

from app.infrastructure.persistence.repositories.duckdb_data_repository import DuckDBDataRepository
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.domain.entities.schema import Schema, SchemaProperty
from app.domain.entities.data_record import DataRecord
from app.application.dto.query_dto import DataQueryRequest, PaginationParams, SortParams
from app.domain.exceptions import RepositoryException, DuplicateRecordException, DatabaseError, DataProcessingError
import duckdb # For duckdb.Error and its subtypes

class TestDuckDBDataRepository(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.mock_pool = AsyncMock(spec=AsyncDuckDBPool)
        self.mock_conn = MagicMock() # Synchronous mock for DuckDB connection methods
        self.mock_pool.acquire.return_value.__aenter__.return_value = self.mock_conn

        self.repository = DuckDBDataRepository(self.mock_pool)

        self.test_schema = Schema(
            name="repo_test_schema",
            table_name="repo_test_table",
            primary_key=["id"], # Assuming 'id' is part of properties and is PK
            properties=[
                SchemaProperty(name="id", type="string", db_type="VARCHAR"),
                SchemaProperty(name="name", type="string", db_type="VARCHAR"),
                SchemaProperty(name="value", type="integer", db_type="INTEGER"),
            ]
        )
        self.test_data = {"id": str(uuid.uuid4()), "name": "Test Record", "value": 123}
        self.test_record = DataRecord(
            schema_name=self.test_schema.name,
            data=self.test_data,
            id=self.test_data["id"] # Example
        )

    async def test_create_success(self):
        self.mock_conn.execute.return_value = None # INSERT OR IGNORE might not return anything specific

        created_record = await self.repository.create(self.test_schema, self.test_data)

        self.assertIsNotNone(created_record)
        self.assertEqual(created_record.data["name"], self.test_data["name"])
        self.mock_conn.execute.assert_called_once()
        args, _ = self.mock_conn.execute.call_args
        self.assertIn(f'INSERT OR IGNORE INTO "{self.test_schema.table_name}"', args[0])

    async def test_create_constraint_violation_raises_duplicate_record(self):
        self.mock_conn.execute.side_effect = duckdb.ConstraintException("Duplicate key")
        with self.assertRaises(DuplicateRecordException):
            await self.repository.create(self.test_schema, self.test_data)

    async def test_create_io_error_raises_repository_exception(self):
        self.mock_conn.execute.side_effect = duckdb.IOException("Disk full")
        with self.assertRaises(RepositoryException) as ctx:
            await self.repository.create(self.test_schema, self.test_data)
        self.assertIn("File system error", str(ctx.exception))

    async def test_create_generic_duckdb_error_raises_repository_exception(self):
        self.mock_conn.execute.side_effect = duckdb.Error("Generic DB error")
        with self.assertRaises(RepositoryException) as ctx:
            await self.repository.create(self.test_schema, self.test_data)
        self.assertIn("Database error creating record", str(ctx.exception))

    @patch('app.infrastructure.persistence.repositories.duckdb_data_repository.create_csv_from_records', new_callable=AsyncMock)
    @patch('app.infrastructure.persistence.repositories.duckdb_data_repository.execute_duckdb_copy_from_csv', new_callable=AsyncMock)
    @patch('app.infrastructure.persistence.repositories.duckdb_data_repository.get_duckdb_config_string')
    @patch('tempfile.NamedTemporaryFile')
    async def test_ultra_fast_copy_insert_success(
        self, mock_tempfile, mock_get_config, mock_exec_copy, mock_create_csv
    ):
        mock_temp_file_obj = MagicMock()
        mock_temp_file_obj.name = "/fake/temp_copy.csv"
        mock_tempfile.return_value.__enter__.return_value = mock_temp_file_obj

        mock_get_config.return_value = "SET bulk_config=true"

        records_list = [self.test_record, self.test_record] # List of DataRecord objects
        await self.repository._ultra_fast_copy_insert(self.test_schema, records_list)

        mock_create_csv.assert_called_once_with(records_list, self.test_schema, mock_temp_file_obj.name)
        mock_get_config.assert_called_once_with("bulk_insert")
        mock_exec_copy.assert_called_once_with(
            db_conn=self.mock_conn,
            schema_name=self.test_schema.name,
            table_name=self.test_schema.table_name,
            temp_csv_path=mock_temp_file_obj.name,
            use_temp_table=True,
            config_string="SET bulk_config=true"
        )

    @patch('app.infrastructure.persistence.repositories.duckdb_data_repository.create_csv_from_records', new_callable=AsyncMock, side_effect=DataProcessingError("CSV write failed"))
    @patch('tempfile.NamedTemporaryFile')
    async def test_ultra_fast_copy_insert_handles_csv_util_error(self, mock_tempfile, mock_create_csv):
        mock_temp_file_obj = MagicMock()
        mock_temp_file_obj.name = "/fake/temp_copy_fail.csv"
        mock_tempfile.return_value.__enter__.return_value = mock_temp_file_obj

        with self.assertRaises(RepositoryException) as ctx:
            await self.repository._ultra_fast_copy_insert(self.test_schema, [self.test_record])
        self.assertIn("Data processing error during bulk insert", str(ctx.exception.message))


    @patch('app.infrastructure.persistence.repositories.duckdb_data_repository.create_csv_from_records', new_callable=AsyncMock)
    @patch('app.infrastructure.persistence.repositories.duckdb_data_repository.execute_duckdb_copy_from_csv', new_callable=AsyncMock, side_effect=DatabaseError("DB COPY failed"))
    @patch('tempfile.NamedTemporaryFile')
    async def test_ultra_fast_copy_insert_handles_db_util_error(self, mock_tempfile, mock_exec_copy, mock_create_csv):
        mock_temp_file_obj = MagicMock()
        mock_temp_file_obj.name = "/fake/temp_copy_fail_db.csv"
        mock_tempfile.return_value.__enter__.return_value = mock_temp_file_obj

        with self.assertRaises(RepositoryException) as ctx:
            await self.repository._ultra_fast_copy_insert(self.test_schema, [self.test_record])
        self.assertIn("Database error during bulk insert", str(ctx.exception.message))


    async def test_get_by_id_found(self):
        # Mocking structure for fetchone() and description
        mock_cursor_proxy = MagicMock()
        mock_cursor_proxy.fetchone.return_value = (str(self.test_record.id), self.test_record.created_at.isoformat(), self.test_record.version, self.test_data["name"], self.test_data["value"])
        self.mock_conn.execute.return_value = mock_cursor_proxy
        self.mock_conn.description = [('id',), ('created_at',), ('version',), ('name',), ('value',)] # Mock description

        found_record = await self.repository.get_by_id(self.test_schema, self.test_record.id)

        self.assertIsNotNone(found_record)
        self.assertEqual(found_record.id, self.test_record.id)
        self.assertEqual(found_record.data["name"], self.test_data["name"])
        self.mock_conn.execute.assert_called_once()
        args, _ = self.mock_conn.execute.call_args
        self.assertIn(f'SELECT * FROM "{self.test_schema.table_name}" WHERE id = ?', args[0])

    async def test_get_by_id_not_found(self):
        mock_cursor_proxy = MagicMock()
        mock_cursor_proxy.fetchone.return_value = None
        self.mock_conn.execute.return_value = mock_cursor_proxy

        found_record = await self.repository.get_by_id(self.test_schema, uuid.uuid4())
        self.assertIsNone(found_record)

    async def test_get_by_id_duckdb_error(self):
        self.mock_conn.execute.side_effect = duckdb.Error("DB error on get_by_id")
        with self.assertRaises(RepositoryException):
            await self.repository.get_by_id(self.test_schema, self.test_record.id)

    # Example for one query method using get_duckdb_config_string
    @patch('app.infrastructure.persistence.repositories.duckdb_data_repository.get_duckdb_config_string')
    async def test_get_all_uses_config_string(self, mock_get_config):
        mock_get_config.return_value = "SET query_config=true"

        # Setup for get_all to run (minimal)
        mock_cursor_proxy_count = MagicMock()
        mock_cursor_proxy_count.fetchone.return_value = (0,) # Total 0 records
        mock_cursor_proxy_select = MagicMock()
        mock_cursor_proxy_select.fetchall.return_value = []
        self.mock_conn.execute.side_effect = [mock_cursor_proxy_count, mock_cursor_proxy_select]
        self.mock_conn.description = []

        query_request = DataQueryRequest(pagination=PaginationParams(page=1, size=10))
        await self.repository.get_all(self.test_schema, query_request)

        mock_get_config.assert_called_with("query_optimized")
        # Check if the config string was executed
        self.assertIn(call("SET query_config=true"), self.mock_conn.execute.mock_calls)

if __name__ == '__main__':
    unittest.main()
