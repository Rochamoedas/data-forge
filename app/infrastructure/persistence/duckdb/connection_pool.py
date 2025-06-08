import asyncio
from contextlib import asynccontextmanager
import duckdb
from app.config.settings import settings

class AsyncDuckDBPool:
    def __init__(self, database_path: str, min_connections: int = 2, max_connections: int = 5):
        self.database_path = database_path
        self.min_connections = min_connections
        self.max_connections = max_connections
        self._pool = asyncio.Queue(maxsize=max_connections)
        self._total_connections = 0
        self._lock = asyncio.Lock()

    async def initialize(self):
        async with self._lock:
            for _ in range(self.min_connections):
                conn = await self._create_connection()
                await self._pool.put(conn)
                self._total_connections += 1

    async def _create_connection(self):
        # Get the performance config
        perf_config = settings.DUCKDB_PERFORMANCE_CONFIG
        
        # Create connection with config
        conn = duckdb.connect(database=self.database_path, config=perf_config)
        
        # Apply additional performance settings
        conn.execute("SET enable_object_cache = true;")
        conn.execute("SET memory_limit = '4GB';")
        conn.execute("SET threads = 4;")
        
        return conn

    @asynccontextmanager
    async def acquire(self):
        conn = None
        async with self._lock:
            if self._pool.empty() and self._total_connections < self.max_connections:
                conn = await self._create_connection()
                self._total_connections += 1
            else:
                conn = await self._pool.get()
        try:
            yield conn
        finally:
            await self._pool.put(conn)

    async def close(self):
        while not self._pool.empty():
            conn = await self._pool.get()
            conn.close()
        self._total_connections = 0 