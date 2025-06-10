# app/config/duckdb_config.py
from app.config.logging_config import logger # Import logger

# Default DuckDB settings applicable to all connections unless overridden by a profile.
# These settings aim for a balance of performance and resource utilization.
DUCKDB_SETTINGS = {
    "threads": 8,  # Number of threads for parallel execution. Consider adjusting based on available CPU cores.
    "memory_limit": "8GB",  # Maximum memory DuckDB can use (e.g., "8GB", "1024MB").
    "enable_object_cache": True,  # Cache query objects (e.g., Parquet metadata) for faster subsequent queries.
    "temp_directory": "/tmp/duckdb_temp",  # Directory for DuckDB to spill temporary data to disk if memory_limit is exceeded.
    "default_null_order": "nulls_first",  # Defines the default ordering of NULL values in ORDER BY clauses ('nulls_first' or 'nulls_last').
    "enable_external_access": False,  # Disables access to the local file system and other external resources for security. Set to True if S3 or other external access is needed.
    "allow_unsigned_extensions": True, # Allows loading of unsigned extensions, useful for community extensions like 'arrow'.
    "enable_progress_bar": False, # Disables the progress bar for query execution, recommended for non-interactive applications for slight performance gain.
}

# Configuration for the Arrow extension, which enables zero-copy data transfer with PyArrow.
ARROW_EXTENSION_CONFIG = {
    "load_by_default": True,  # If True, attempts to load the Arrow extension when a connection is made or needed.
    "install_if_not_found": True,  # If True, attempts to run "INSTALL arrow;" if loading fails. Requires DuckDB to have internet access.
}

# Performance profiles for specific types of operations.
# These profiles can override settings from DUCKDB_SETTINGS to tune performance
# for memory-intensive tasks (like bulk inserts) or CPU-bound tasks.
OPERATION_PROFILES = {
    "bulk_insert": {
        "memory_limit": "12GB", # Allocate more memory for large bulk insert operations.
        "threads": 16,          # Utilize more threads for potentially faster parallel data ingestion.
    },
    "streaming": {
        "memory_limit": "6GB",  # Use a more conservative memory limit for streaming to prevent Out-Of-Memory (OOM) errors during long-running streams.
        "threads": 4,           # Fewer threads might be sufficient for streaming and reduce context switching.
        "strategy": "offset",   # Default streaming strategy: "offset" or "keyset". This is an application-level setting.
    },
    "query_optimized": {
        "memory_limit": "10GB", # Memory for general analytical queries.
        "threads": 8,           # Threads for general analytical queries.
    }
}

def get_duckdb_config_string(profile: str = None) -> str:
    """
    Generates a DuckDB configuration string (e.g., "SET threads = 8; SET memory_limit = '8GB';").

    If a profile name is provided (e.g., "bulk_insert"), it merges the profile's settings
    with the default DUCKDB_SETTINGS, with profile settings taking precedence.
    The 'strategy' key (used for application-level streaming strategy) is excluded
    from the generated DuckDB settings string.

    Args:
        profile: Optional name of the operation profile to use.

    Returns:
        A string of DuckDB SET commands.
    """
    config = DUCKDB_SETTINGS.copy()
    if profile and profile in OPERATION_PROFILES:
        logger.debug(f"Applying DuckDB operation profile: '{profile}'")
        config.update(OPERATION_PROFILES[profile])
    else:
        logger.debug("Applying default DuckDB settings.")

    # Exclude 'strategy' as it's not a direct DuckDB setting but an application-level config.
    return "; ".join([f"SET {key} = '{value}'" if isinstance(value, str) else f"SET {key} = {value}" for key, value in config.items() if key != "strategy"])

# Default streaming strategy for the application, can be "offset" or "keyset".
# This is derived from the "streaming" profile or defaults to "offset" if not specified.
DEFAULT_STREAMING_STRATEGY = OPERATION_PROFILES.get("streaming", {}).get("strategy", "offset")
logger.info(f"Default streaming strategy set to: '{DEFAULT_STREAMING_STRATEGY}'")
