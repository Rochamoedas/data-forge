import asyncio
from contextlib import asynccontextmanager
import duckdb
from app.config.settings import settings
from app.config.logging_config import logger

class AsyncDuckDBPool:
    _connection = None
    _lock = asyncio.Lock()

    def __init__(self, **kwargs):
        # The connection parameters are now managed by this pool.
        pass

    async def initialize(self):
        # This will now create the connection.
        async with self._lock:
            if self._connection is None:
                logger.info(f"Initializing DuckDB connection to database: {settings.DATABASE_PATH}")
                
                full_config = settings.DUCKDB_PERFORMANCE_CONFIG
                
                # Config keys that MUST be set at connection time
                startup_keys = {
                    'allow_unsigned_extensions',
                    'autoinstall_known_extensions',
                    'autoload_known_extensions',
                    'temp_directory'
                }
                
                # Separate configs
                startup_config = {k: v for k, v in full_config.items() if k in startup_keys}
                runtime_config = {k: v for k, v in full_config.items() if k not in startup_keys}

                # duckdb.connect handles typing for its config dict
                logger.info(f"Applying DuckDB startup config: {startup_config}")
                self._connection = duckdb.connect(
                    database=settings.DATABASE_PATH, 
                    read_only=False,
                    config=startup_config
                )

                logger.info(f"Applying DuckDB runtime settings: {runtime_config}")
                for key, value in runtime_config.items():
                    if value is not None:
                        # SET command is picky about quotes for strings vs other types
                        if isinstance(value, str):
                            # Don't set empty strings. For `disabled_optimizers`, empty is default.
                            if value:
                                self._connection.execute(f"SET {key} = '{value}'")
                        else:
                            self._connection.execute(f"SET {key} = {str(value).lower()}")

                # Load extensions if needed, e.g., arrow
                if settings.DUCKDB_ARROW_EXTENSION_ENABLED:
                    try:
                        logger.info("Installing and loading DuckDB arrow extension.")
                        self._connection.execute("INSTALL arrow")
                        self._connection.execute("LOAD arrow")
                        logger.info("Arrow extension loaded successfully.")
                    except Exception as e:
                        logger.warning(f"Could not install or load arrow extension: {e}")

    @asynccontextmanager
    async def acquire(self):
        if self._connection is None:
            await self.initialize()
            
        async with self._lock:
            try:
                yield self._connection
            finally:
                # The connection is no longer closed here.
                # It will be closed on shutdown.
                pass

    async def close(self):
        async with self._lock:
            if self._connection:
                logger.info("Closing DuckDB connection.")
                self._connection.close()
                self._connection = None

    def is_connected(self):
        """Return True if the connection is active."""
        return self._connection is not None 