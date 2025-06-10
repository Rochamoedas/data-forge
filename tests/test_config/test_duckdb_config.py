# tests/test_config/test_duckdb_config.py
import unittest
from unittest.mock import patch, MagicMock
from app.config import duckdb_config # Import the module to test

# To test logging, we might need to capture log output or mock the logger
import logging

class TestDuckDBConfig(unittest.TestCase):

    def setUp(self):
        # Store original settings to restore them later if necessary,
        # though it's better if tests don't modify module-level constants directly.
        # For this test, we'll assume duckdb_config.DUCKDB_SETTINGS and .OPERATION_PROFILES are constants.
        self.default_settings = duckdb_config.DUCKDB_SETTINGS.copy()
        self.operation_profiles = duckdb_config.OPERATION_PROFILES.copy()

    @patch('app.config.duckdb_config.logger') # Mock logger to check calls
    def test_get_duckdb_config_string_default(self, mock_logger):
        """Test get_duckdb_config_string with no profile (default settings)."""
        config_str = duckdb_config.get_duckdb_config_string()

        expected_parts = []
        for key, value in self.default_settings.items():
            if key == "strategy": continue # Should be excluded
            expected_parts.append(f"SET {key} = '{value}'" if isinstance(value, str) else f"SET {key} = {value}")

        # Check if all expected parts are in the config string
        for part in expected_parts:
            self.assertIn(part, config_str)

        self.assertNotIn("strategy", config_str.lower()) # Ensure 'strategy' is not in the output
        mock_logger.debug.assert_any_call("Applying default DuckDB settings.")

    @patch('app.config.duckdb_config.logger')
    def test_get_duckdb_config_string_with_valid_profile(self, mock_logger):
        """Test with a valid profile, expecting merged settings."""
        profile_name = "bulk_insert"
        profile_settings = self.operation_profiles[profile_name]

        # Expected settings are defaults overridden by profile settings
        expected_config = self.default_settings.copy()
        expected_config.update(profile_settings)

        config_str = duckdb_config.get_duckdb_config_string(profile_name)

        expected_parts = []
        for key, value in expected_config.items():
            if key == "strategy": continue
            expected_parts.append(f"SET {key} = '{value}'" if isinstance(value, str) else f"SET {key} = {value}")

        for part in expected_parts:
            self.assertIn(part, config_str)

        self.assertNotIn("strategy", config_str.lower())
        mock_logger.debug.assert_any_call(f"Applying DuckDB operation profile: '{profile_name}'")

    @patch('app.config.duckdb_config.logger')
    def test_get_duckdb_config_string_with_invalid_profile(self, mock_logger):
        """Test with an invalid profile, expecting default settings and a log message."""
        invalid_profile_name = "non_existent_profile"
        config_str = duckdb_config.get_duckdb_config_string(invalid_profile_name)

        expected_parts = []
        for key, value in self.default_settings.items():
            if key == "strategy": continue
            expected_parts.append(f"SET {key} = '{value}'" if isinstance(value, str) else f"SET {key} = {value}")

        for part in expected_parts:
            self.assertIn(part, config_str)

        self.assertNotIn("strategy", config_str.lower())
        # Check that it logged applying default settings because profile was not found
        mock_logger.debug.assert_any_call("Applying default DuckDB settings.")


    def test_get_duckdb_config_string_profile_with_strategy(self):
        """Test that 'strategy' key from a profile is NOT included in DB settings string."""
        profile_name = "streaming" # This profile has a 'strategy' key
        config_str = duckdb_config.get_duckdb_config_string(profile_name)

        self.assertNotIn("SET strategy", config_str)
        self.assertNotIn("strategy =", config_str) # General check

        # Ensure other settings from the profile are present
        streaming_profile = self.operation_profiles[profile_name]
        self.assertIn(f"SET memory_limit = '{streaming_profile['memory_limit']}'", config_str)
        self.assertIn(f"SET threads = {streaming_profile['threads']}", config_str)


    def test_default_streaming_strategy_constant(self):
        """Test that DEFAULT_STREAMING_STRATEGY is accessible and has a value."""
        self.assertTrue(hasattr(duckdb_config, 'DEFAULT_STREAMING_STRATEGY'))
        # Check if it's one of the known strategies, or at least a string
        self.assertIsInstance(duckdb_config.DEFAULT_STREAMING_STRATEGY, str)
        self.assertIn(duckdb_config.DEFAULT_STREAMING_STRATEGY, ["offset", "keyset"])

        # Verify it matches the 'streaming' profile's strategy or the hardcoded default 'offset'
        expected_strategy = self.operation_profiles.get("streaming", {}).get("strategy", "offset")
        self.assertEqual(duckdb_config.DEFAULT_STREAMING_STRATEGY, expected_strategy)

if __name__ == '__main__':
    unittest.main()
