# tests/test_infrastructure/test_persistence/test_duckdb_utils.py
import unittest
from unittest.mock import patch, mock_open, MagicMock, call, AsyncMock
import pytest # Using pytest for async tests if preferred, or unittest.IsolatedAsyncioTestCase
import asyncio # For async test setup if not using pytest-asyncio

from app.infrastructure.persistence.duckdb.duckdb_utils import (
    create_csv_from_records,
    create_csv_from_dicts,
    execute_duckdb_copy_from_csv
)
from app.domain.entities.schema import Schema, SchemaProperty
from app.domain.entities.data_record import DataRecord
from app.domain.exceptions import DataProcessingError, DatabaseError
import duckdb # For duckdb.Error

# Since the utils are async, test methods also need to be async or use asyncio.run
# For unittest, we can use IsolatedAsyncioTestCase (Python 3.8+)

class TestDuckDBUtils(unittest.IsolatedAsyncioTestCase): # Using IsolatedAsyncioTestCase for async methods

    def setUp(self):
        self.mock_schema = Schema(
            name="test_schema",
            table_name="test_table",
            properties=[
                SchemaProperty(name="col_a", type="string", db_type="VARCHAR"),
                SchemaProperty(name="col_b", type="integer", db_type="INTEGER"),
            ]
        )
        self.mock_records = [
            DataRecord(id="uuid1", schema_name="test_schema", created_at="2023-01-01T00:00:00", version=1, data={"col_a": "val1", "col_b": 10}),
            DataRecord(id="uuid2", schema_name="test_schema", created_at="2023-01-02T00:00:00", version=1, data={"col_a": "val2", "col_b": None}), # Test None value
        ]
        self.mock_data_dicts = [
            {"col_a": "dict_val1", "col_b": 100, "version": 2}, # version from dict
            {"col_a": "dict_val2", "col_b": 200},
        ]

    @patch("builtins.open", new_callable=mock_open)
    @patch("app.infrastructure.persistence.duckdb.duckdb_utils.logger")
    async def test_create_csv_from_records_success(self, mock_logger, mock_file_open):
        temp_file_path = "/fake/temp_records.csv"
        await create_csv_from_records(self.mock_records, self.mock_schema, temp_file_path)

        mock_file_open.assert_called_once_with(temp_file_path, 'w', newline='', encoding='utf-8')
        handle = mock_file_open()

        # Check header
        self.assertIn(call.writerow(["id", "created_at", "version", "col_a", "col_b"]), handle.writerow.mock_calls)
        # Check data rows
        self.assertIn(call.writerow(["uuid1", "2023-01-01T00:00:00", "1", "val1", "10"]), handle.writerow.mock_calls)
        self.assertIn(call.writerow(["uuid2", "2023-01-02T00:00:00", "1", "val2", ""]), handle.writerow.mock_calls) # None as empty string
        mock_logger.info.assert_called_with(f"[DBUtil] Successfully wrote {len(self.mock_records)} DataRecords to temporary CSV: '{temp_file_path}' for schema '{self.mock_schema.name}'.")

    @patch("builtins.open", side_effect=IOError("File open failed"))
    @patch("app.infrastructure.persistence.duckdb.duckdb_utils.logger")
    async def test_create_csv_from_records_io_error(self, mock_logger, mock_file_open):
        with self.assertRaises(DataProcessingError) as context:
            await create_csv_from_records(self.mock_records, self.mock_schema, "/fake/path.csv")
        self.assertIn("Failed to write DataRecord data", str(context.exception))
        mock_logger.error.assert_called_once()


    @patch("builtins.open", new_callable=mock_open)
    @patch("app.infrastructure.persistence.duckdb.duckdb_utils.logger")
    async def test_create_csv_from_dicts_with_generated_fields(self, mock_logger, mock_file_open):
        temp_file_path = "/fake/temp_dicts_generated.csv"
        await create_csv_from_dicts(self.mock_data_dicts, self.mock_schema, temp_file_path, include_generated_fields=True)

        mock_file_open.assert_called_once_with(temp_file_path, 'w', newline='', encoding='utf-8')
        handle = mock_file_open()

        self.assertIn(call.writerow(["id", "created_at", "version", "col_a", "col_b"]), handle.writerow.mock_calls)

        # Check data rows - id, created_at are generated, so we can't assert exact values easily without more mocking
        # We check that writerow was called the correct number of times for data
        data_row_calls = [c for c in handle.writerow.mock_calls if c != call.writerow(["id", "created_at", "version", "col_a", "col_b"])]
        self.assertEqual(len(data_row_calls), len(self.mock_data_dicts))

        # Check one row for structure (assuming first one)
        first_data_row_args = data_row_calls[0][1][0] # Get the list passed to writerow
        self.assertEqual(len(first_data_row_args), 5) # id, created_at, version, col_a, col_b
        self.assertEqual(first_data_row_args[2], "2") # Version from data
        self.assertEqual(first_data_row_args[3], "dict_val1")
        self.assertEqual(first_data_row_args[4], "100")

        mock_logger.info.assert_called_with(f"[DBUtil] Successfully wrote {len(self.mock_data_dicts)} dictionaries to temporary CSV: '{temp_file_path}' for schema '{self.mock_schema.name}'.")

    @patch("builtins.open", new_callable=mock_open)
    @patch("app.infrastructure.persistence.duckdb.duckdb_utils.logger")
    async def test_create_csv_from_dicts_no_generated_fields(self, mock_logger, mock_file_open):
        temp_file_path = "/fake/temp_dicts_no_gen.csv"
        await create_csv_from_dicts(self.mock_data_dicts, self.mock_schema, temp_file_path, include_generated_fields=False)
        handle = mock_file_open()
        self.assertIn(call.writerow(["col_a", "col_b"]), handle.writerow.mock_calls)
        self.assertIn(call.writerow(["dict_val1", "100"]), handle.writerow.mock_calls)


    @patch("app.infrastructure.persistence.duckdb.duckdb_utils.logger")
    async def test_execute_duckdb_copy_from_csv_success_with_temp_table(self, mock_logger):
        mock_db_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        # Mock fetchone for count result
        mock_db_conn.execute.return_value.fetchone.return_value = (len(self.mock_records),)

        temp_csv_path = "/fake/data.csv"
        schema_name = "test_schema_name"
        table_name = "test_table_name"
        config_string = "SET memory_limit='1GB'"

        # Mock open for row count estimation if use_temp_table was False
        # Not strictly needed here, but good for completeness if testing that path
        m_open = mock_open(read_data="header\nrow1\nrow2")
        with patch("builtins.open", m_open):
            rows = await execute_duckdb_copy_from_csv(
                mock_db_conn, schema_name, table_name, temp_csv_path,
                use_temp_table=True, config_string=config_string
            )

        self.assertEqual(rows, len(self.mock_records)) # Estimated from temp table count

        expected_calls = [
            call("BEGIN TRANSACTION"),
            call(config_string),
            call(unittest.mock.ANY), # CREATE TEMPORARY TABLE ...
            call(unittest.mock.ANY), # COPY ...
            call(f"SELECT COUNT(*) FROM \"{mock_db_conn.execute.call_args_list[2][0][0].split()[-4].strip('"')}\""), # Fragile: get temp table name
            call(unittest.mock.ANY), # INSERT OR IGNORE ...
            call(unittest.mock.ANY), # DROP TABLE ...
            call("COMMIT")
        ]
        mock_db_conn.execute.assert_has_calls(expected_calls, any_order=False)
        mock_logger.info.assert_any_call(f"[DBUtil] Successfully executed COPY FROM '{temp_csv_path}' to table '{table_name}'. Estimated rows processed/copied: {rows}")


    @patch("app.infrastructure.persistence.duckdb.duckdb_utils.logger")
    async def test_execute_duckdb_copy_from_csv_success_no_temp_table(self, mock_logger):
        mock_db_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        temp_csv_path = "/fake/data_no_temp.csv"

        # Simulate CSV content for row count estimation
        csv_content = "id,col_a\n1,val1\n2,val2\n3,val3"
        m_open = mock_open(read_data=csv_content)

        with patch("builtins.open", m_open):
            rows = await execute_duckdb_copy_from_csv(
                mock_db_conn, "s", "t", temp_csv_path, use_temp_table=False
            )

        self.assertEqual(rows, 3) # 3 data rows + 1 header

        expected_calls = [
            call("BEGIN TRANSACTION"),
            call(unittest.mock.ANY), # COPY directly to table 't'
            call("COMMIT")
        ]
        mock_db_conn.execute.assert_has_calls(expected_calls, any_order=False)


    @patch("app.infrastructure.persistence.duckdb.duckdb_utils.logger")
    async def test_execute_duckdb_copy_duckdb_error_rolls_back(self, mock_logger):
        mock_db_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        mock_db_conn.execute.side_effect = [
            None, # BEGIN TRANSACTION
            duckdb.Error("COPY failed!") # COPY command fails
        ]
        temp_csv_path = "/fake/fail.csv"
        with self.assertRaises(DatabaseError):
            await execute_duckdb_copy_from_csv(mock_db_conn, "s", "t", temp_csv_path)

        mock_db_conn.rollback.assert_called_once()
        mock_logger.error.assert_any_call(unittest.mock.ANY, exc_info=True) # Check if error was logged


    @patch("app.infrastructure.persistence.duckdb.duckdb_utils.logger")
    async def test_execute_duckdb_copy_io_error_rolls_back(self, mock_logger):
        mock_db_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        # Let BEGIN succeed, but COPY will effectively "fail" due to IOError before it by CSV count
        m_open = mock_open()
        m_open.side_effect = IOError("Cannot open CSV for counting")

        with patch("builtins.open", m_open), self.assertRaises(DataProcessingError):
             await execute_duckdb_copy_from_csv(
                mock_db_conn, "s", "t", "/fake/io_fail.csv", use_temp_table=False
            )
        mock_db_conn.rollback.assert_called_once() # Rollback should still be called
        mock_logger.error.assert_any_call(unittest.mock.ANY, exc_info=True)


if __name__ == '__main__':
    # This allows running tests with `python -m unittest path/to/this_file.py`
    # For async tests, ensure your test runner supports asyncio or use `asyncio.run`
    # For example, if not using a specific async test runner:
    # suite = unittest.TestSuite()
    # suite.addTest(unittest.makeSuite(TestDuckDBUtils))
    # runner = unittest.TextTestRunner()
    # runner.run(suite)
    # However, `unittest.main()` should work fine with IsolatedAsyncioTestCase
    unittest.main()
