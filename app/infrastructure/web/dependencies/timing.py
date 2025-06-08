import time
import functools
from typing import Callable, Any
from fastapi import Request
from app.config.logging_config import logger

def timing_decorator(func: Callable) -> Callable:
    """
    A decorator that measures and logs the execution time of FastAPI endpoint methods.
    The timing information is added to the response if it's a dictionary.
    """
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.perf_counter()
        
        try:
            # Execute the endpoint function
            response = await func(*args, **kwargs)
            
            # Calculate execution time
            duration = time.perf_counter() - start_time
            duration_ms = duration * 1000
            
            # Log the timing information
            logger.info(f"Endpoint {func.__name__} executed in {duration_ms:.2f}ms")
            
            # If response is a dictionary, add timing information
            if isinstance(response, dict):
                response["execution_time_ms"] = duration_ms
            
            return response
            
        except Exception as e:
            # Calculate execution time even if there's an error
            duration = time.perf_counter() - start_time
            duration_ms = duration * 1000
            logger.error(f"Endpoint {func.__name__} failed after {duration_ms:.2f}ms: {str(e)}")
            raise
    
    return wrapper 