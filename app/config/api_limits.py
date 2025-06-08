from pydantic import BaseModel

class APILimits(BaseModel):
    """Configuration for API limits and constraints"""
    
    # Pagination limits - Increased for local performance
    DEFAULT_PAGE_SIZE: int = 5000  # Increased from 999 for better local performance
    MAX_PAGE_SIZE: int = 50000     # Increased from 10000 for large dataset handling
    MIN_PAGE_SIZE: int = 1
    
    # Stream limits - Optimized for local large datasets
    DEFAULT_STREAM_LIMIT: int = 50000   # Increased from 10000 for better throughput
    MAX_STREAM_LIMIT: int = 500000      # Increased from 100001 for million-row datasets
    MIN_STREAM_LIMIT: int = 1
    
    # Bulk operation limits - Significantly increased for local operations
    MAX_BULK_RECORDS: int = 250000      # Increased from 100001 for better bulk performance
    MIN_BULK_RECORDS: int = 1
    
    # Query limits - Relaxed for complex local queries
    MAX_FILTER_CONDITIONS: int = 100    # Increased from 50 for complex filtering
    MAX_SORT_FIELDS: int = 20           # Increased from 10 for multi-field sorting
    
    # Performance limits - Optimized for local processing
    QUERY_TIMEOUT_SECONDS: int = 120    # Increased from 30 for complex local queries
    MAX_CONCURRENT_STREAMS: int = 3     # Reduced from 5, optimized for your use case
    
    # New limits for local optimization
    MAX_MEMORY_BUFFER_MB: int = 512     # Memory buffer for large operations (512MB)
    BATCH_PROCESSING_SIZE: int = 10000  # Optimal batch size for your hardware
    PARALLEL_WORKER_THREADS: int = 4    # Based on typical i5 10th gen (4-6 cores)

# Global instance
api_limits = APILimits() 