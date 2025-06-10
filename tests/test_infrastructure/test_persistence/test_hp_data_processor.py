# tests/test_infrastructure/test_persistence/test_hp_data_processor.py
import unittest
from unittest.mock import patch, MagicMock, AsyncMock, call
import pytest # For async generator testing
import polars as pl
import pyarrow as pa
from typing import List, Dict, Any, AsyncIterator

from app.infrastructure.persistence.high_performance_data_processor import HighPerformanceDataProcessor
from app.infrastructure.persistence.duckdb.connection_pool import AsyncDuckDBPool
from app.domain.entities.schema import Schema, SchemaProperty
from app.config import duckdb_config # To mock its constants or functions

# Using unittest.IsolatedAsyncioTestCase for async methods
class TestHighPerformanceDataProcessorStreaming(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.mock_pool = AsyncMock(spec=AsyncDuckDBPool)
        self.mock_conn = MagicMock() # Synchronous mock for DuckDB connection methods
        self.mock_pool.acquire.return_value.__aenter__.return_value = self.mock_conn

        self.processor = HighPerformanceDataProcessor(self.mock_pool, max_workers=2)

        self.test_schema = Schema(
            name="stream_test_schema",
            table_name="stream_test_table",
            properties=[
                SchemaProperty(name="id", type="integer", db_type="INTEGER"),
                SchemaProperty(name="value", type="string", db_type="VARCHAR"),
            ]
        )
        # Sample data for mocking DuckDB responses
        self.sample_data_batch_1 = [{"id": 1, "value": "data1"}, {"id": 2, "value": "data2"}]
        self.sample_data_batch_2 = [{"id": 3, "value": "data3"}, {"id": 4, "value": "data4"}]
        self.sample_polars_df_batch_1 = pl.DataFrame(self.sample_data_batch_1)
        self.sample_polars_df_batch_2 = pl.DataFrame(self.sample_data_batch_2)

    async def _collect_async_iterator(self, iterator: AsyncIterator) -> list:
        results = []
        async for item in iterator:
            results.append(item)
        return results

    @patch('app.config.duckdb_config.OPERATION_PROFILES', {"streaming": {"strategy": "offset"}})
    @patch('app.config.duckdb_config.DEFAULT_STREAMING_STRATEGY', "offset")
    @patch('app.config.duckdb_config.ARROW_EXTENSION_CONFIG', {"load_by_default": False, "install_if_not_found": False})
    @patch('asyncio.to_thread', new_callable=AsyncMock) # Mock to_thread for sync DB calls
    async def test_stream_with_arrow_batches_offset_strategy(self, mock_to_thread):
        # Mock setup_streaming_optimizations_sync (called via to_thread)
        # First call to to_thread is setup_streaming_optimizations_sync
        # Second call is get_total_count_sync
        # Subsequent calls are fetch_and_convert_batch
        mock_to_thread.side_effect = [
            False, # use_arrow = False from setup_streaming_optimizations_sync
            4,     # total_records_for_offset = 4 from get_total_count_sync
            (self.sample_polars_df_batch_1, "traditional"), # First batch
            (self.sample_polars_df_batch_2, "traditional"), # Second batch
            (pl.DataFrame([]), "traditional") # Empty batch to terminate
        ]

        results = []
        async for df_batch in self.processor.stream_with_arrow_batches(self.test_schema, batch_size=2, streaming_strategy="offset"):
            results.append(df_batch)

        self.assertEqual(len(results), 2)
        self.assertTrue(results[0].equals(self.sample_polars_df_batch_1))
        self.assertTrue(results[1].equals(self.sample_polars_df_batch_2))

        # Verify SQL queries for offset
        # setup_streaming_optimizations_sync, get_total_count_sync, execute_batch_query (3 times)
        self.assertEqual(mock_to_thread.call_count, 5)

        # Check the SQL passed to execute_batch_query (which is called by fetch_and_convert_batch)
        # The actual conn.execute is inside the sync function called by to_thread, so we check args to to_thread
        # First data call
        args_batch1, _ = mock_to_thread.call_args_list[2] # 0 is setup, 1 is count
        self.assertEqual(args_batch1[0].__name__, "fetch_and_convert_batch") # Check it's calling the right function
        self.assertIn(f'SELECT * FROM "{self.test_schema.table_name}" ORDER BY id LIMIT 2 OFFSET 0', args_batch1[1]) # Query in args[1]

        # Second data call
        args_batch2, _ = mock_to_thread.call_args_list[3]
        self.assertIn(f'SELECT * FROM "{self.test_schema.table_name}" ORDER BY id LIMIT 2 OFFSET 2', args_batch2[1])


    @patch('app.config.duckdb_config.OPERATION_PROFILES', {"streaming": {"strategy": "keyset"}})
    @patch('app.config.duckdb_config.DEFAULT_STREAMING_STRATEGY', "keyset")
    @patch('app.config.duckdb_config.ARROW_EXTENSION_CONFIG', {"load_by_default": False, "install_if_not_found": False})
    @patch('asyncio.to_thread', new_callable=AsyncMock)
    async def test_stream_with_arrow_batches_keyset_strategy(self, mock_to_thread):
        # Mock setup_streaming_optimizations_sync and fetch_and_convert_batch (called via to_thread)
        mock_to_thread.side_effect = [
            False, # use_arrow = False
            (self.sample_polars_df_batch_1, "traditional"), # First batch
            (self.sample_polars_df_batch_2, "traditional"), # Second batch
            (pl.DataFrame([]), "traditional") # Empty batch to terminate
        ]

        results = []
        async for df_batch in self.processor.stream_with_arrow_batches(self.test_schema, batch_size=2, streaming_strategy="keyset"):
            results.append(df_batch)

        self.assertEqual(len(results), 2)
        self.assertTrue(results[0].equals(self.sample_polars_df_batch_1))
        self.assertTrue(results[1].equals(self.sample_polars_df_batch_2))

        self.assertEqual(mock_to_thread.call_count, 4) # setup + 3 batches

        # Check SQL queries for keyset
        # First data call (no WHERE clause)
        args_batch1, _ = mock_to_thread.call_args_list[1] # 0 is setup
        self.assertIn(f'SELECT * FROM "{self.test_schema.table_name}" ORDER BY "id" ASC LIMIT 2', args_batch1[1])

        # Second data call (WHERE "id" > last_id_from_batch1)
        # last_id_from_batch1 is 2 (from self.sample_data_batch_1)
        args_batch2, _ = mock_to_thread.call_args_list[2]
        self.assertIn(f'SELECT * FROM "{self.test_schema.table_name}" WHERE "id" > ? ORDER BY "id" ASC LIMIT 2', args_batch2[1])
        self.assertEqual(args_batch2[2], [2]) # Params list with last_keyset_value


    @patch('app.config.duckdb_config.get_duckdb_config_string')
    @patch('asyncio.to_thread', new_callable=AsyncMock)
    async def test_stream_uses_duckdb_config(self, mock_to_thread, mock_get_config_string):
        mock_get_config_string.return_value = "SET custom_setting = true;"
        # Make it run minimal iterations
        mock_to_thread.side_effect = [False, 0, (pl.DataFrame([]), "traditional")]

        async for _ in self.processor.stream_with_arrow_batches(self.test_schema, batch_size=2, streaming_strategy="offset"):
            pass # Just iterate once

        mock_get_config_string.assert_called_with("streaming")
        # The actual conn.execute(config_str) is inside the sync function called by to_thread.
        # We'd need a more complex mock of `conn` to verify this directly, or trust `setup_streaming_optimizations_sync`
        # For now, checking that get_duckdb_config_string was called is a good indicator.


    @patch('asyncio.to_thread', new_callable=AsyncMock)
    async def test_stream_keyset_error_on_missing_sort_column_in_results(self, mock_to_thread):
        # Simulate batch result not containing the sort key 'id'
        faulty_batch_data = [{"value": "data_no_id"}]
        faulty_df = pl.DataFrame(faulty_batch_data)

        mock_to_thread.side_effect = [
            False, # use_arrow
            (faulty_df, "traditional"), # First batch is faulty
        ]

        with self.assertRaises(DataProcessingError) as context:
            async for _ in self.processor.stream_with_arrow_batches(self.test_schema, batch_size=1, streaming_strategy="keyset"):
                pass
        self.assertIn("Keyset sort column 'id' missing in results", str(context.exception))


    @patch('app.config.duckdb_config.DEFAULT_STREAMING_STRATEGY', "keyset") # Test default strategy usage
    @patch('app.config.duckdb_config.OPERATION_PROFILES', {"streaming": {"strategy": "keyset"}}) # Ensure profile matches
    @patch('asyncio.to_thread', new_callable=AsyncMock)
    async def test_stream_uses_default_strategy_from_config(self, mock_to_thread):
        mock_to_thread.side_effect = [False, (pl.DataFrame([]), "traditional")] # Minimal execution

        # Call without explicitly passing streaming_strategy
        async for _ in self.processor.stream_with_arrow_batches(self.test_schema, batch_size=2):
            pass

        # We expect keyset strategy to be used. First data call for keyset (after setup)
        args_batch1, _ = mock_to_thread.call_args_list[1]
        self.assertIn(f'ORDER BY "id" ASC LIMIT 2', args_batch1[1]) # Keyset specific part
        self.assertNotIn("OFFSET", args_batch1[1])


if __name__ == '__main__':
    unittest.main()
