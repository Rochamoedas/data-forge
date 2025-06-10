import asyncio
from contextlib import asynccontextmanager
import duckdb
import multiprocessing
from app.config.settings import settings

class AsyncDuckDBPool:
    def __init__(self, database_path: str, min_connections: int = 8, max_connections: int = 16):
        self.database_path = database_path
        self.min_connections = min_connections
        self.max_connections = max_connections  # Increased for i7 10th Gen (12 threads)
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
        
        # Get number of CPU cores (i7 10th Gen = 6 cores, 12 threads)
        cpu_count = min(multiprocessing.cpu_count(), 12)  # Cap at 12 for i7 10th Gen
        
        # 🚀 ULTRA-OPTIMIZED settings for i7 10th Gen + 16GB RAM
        conn.execute("SET enable_object_cache = true")
        conn.execute("SET memory_limit = '12GB'")  # Use 75% of 16GB RAM
        conn.execute(f"SET threads = {cpu_count}")  # Use all logical cores
        conn.execute("SET enable_progress_bar = false")
        conn.execute("SET enable_profiling = false")
        
        # Write optimizations
        conn.execute("SET checkpoint_threshold = '1GB'")  # Less frequent checkpoints
        conn.execute("SET wal_autocheckpoint = 1000000")  # Reduce WAL checkpoint frequency
        conn.execute("SET temp_directory = '/tmp'")  # Fast temp storage
        
        # Query optimizations
        conn.execute("SET disabled_optimizers = ''")  # Enable all optimizers
        conn.execute("SET enable_object_cache = true")
        
        # Try to install Arrow extension for better performance
        try:
            conn.execute("INSTALL arrow")
            conn.execute("LOAD arrow")
        except Exception:
            pass  # Arrow extension optional
        
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