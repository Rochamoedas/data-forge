from pydantic import BaseModel

class APILimits(BaseModel):
    """Configuration for API limits and constraints - Optimized for PERFORMANCE TESTING"""
    
    # Pagination limits - Optimized for high-performance testing
    DEFAULT_PAGE_SIZE: int = 1000       # Reasonable default for performance tests
    MAX_PAGE_SIZE: int = 500000         # Increased for performance testing
    MIN_PAGE_SIZE: int = 1
    
    # Stream limits - Optimized for 100K+ record performance tests
    DEFAULT_STREAM_LIMIT: int = 50000   # Good default for streaming
    MAX_STREAM_LIMIT: int = 500000      # Increased for stress testing
    MIN_STREAM_LIMIT: int = 1
    
    # Bulk operation limits - Aligned with performance test requirements
    MAX_BULK_RECORDS: int = 500000      # Allow up to 500K records in single bulk operation
    MIN_BULK_RECORDS: int = 1
    DEFAULT_BULK_BATCH_SIZE: int = 50000 # Increased batch size
    
    # Query limits - Relaxed for complex performance testing
    MAX_FILTER_CONDITIONS: int = 100    # Support complex filtering scenarios
    MAX_SORT_FIELDS: int = 20           # Support multi-field sorting
    
    # Performance test specific limits
    PERFORMANCE_TEST_TIMEOUT: int = 600  # 10 minutes for performance tests
    PERFORMANCE_TEST_BATCH_SIZE: int = 50000  # Increased batch size
    PERFORMANCE_TEST_MAX_RECORDS: int = 500000  # Increased for testing
    
    # Benchmark settings
    BENCHMARK_TEST_SIZE: int = 1_000_000  # Default test size for benchmarks
    BENCHMARK_IO_TEST_SIZE: int = 900_000  # Test size for IO benchmarks
    BENCHMARK_TIMEOUT: int = 300  # 5 minutes timeout for individual benchmark operations
    
    # System resource limits
    MAX_MEMORY_BUFFER_MB: int = 2048    # 2GB buffer for large operations
    PARALLEL_WORKER_THREADS: int = 8    # Increased for better concurrency

# Global instance
api_limits = APILimits() 