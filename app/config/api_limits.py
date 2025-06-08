from pydantic import BaseModel

class APILimits(BaseModel):
    """Configuration for API limits and constraints"""
    
    # Pagination limits
    DEFAULT_PAGE_SIZE: int = 10
    MAX_PAGE_SIZE: int = 10000
    MIN_PAGE_SIZE: int = 1
    
    # Stream limits
    DEFAULT_STREAM_LIMIT: int = 100
    MAX_STREAM_LIMIT: int = 10000
    MIN_STREAM_LIMIT: int = 1
    
    # Bulk operation limits
    MAX_BULK_RECORDS: int = 1000
    MIN_BULK_RECORDS: int = 1
    
    # Query limits
    MAX_FILTER_CONDITIONS: int = 50
    MAX_SORT_FIELDS: int = 10
    
    # Performance limits
    QUERY_TIMEOUT_SECONDS: int = 30
    MAX_CONCURRENT_STREAMS: int = 10

# Global instance
api_limits = APILimits() 