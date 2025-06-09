import time
import functools
import psutil
from typing import Callable, Any, Dict, Optional
from app.config.logging_config import logger
from contextlib import contextmanager
import threading
import asyncio

class PerformanceMetrics:
    def __init__(self):
        self.start_time = 0.0
        self.end_time = 0.0
        self.start_memory_mb = 0.0
        self.end_memory_mb = 0.0
        self.cpu_percent = 0.0
        self._process = psutil.Process()

    @property
    def execution_time_ms(self) -> float:
        return max(0.0, (self.end_time - self.start_time) * 1000)

    @property
    def memory_usage_mb(self) -> float:
        return max(0.0, self.end_memory_mb - self.start_memory_mb)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_time_ms": round(self.execution_time_ms, 2),
            "memory_usage_mb": round(self.memory_usage_mb, 2),
            "cpu_percent": round(self.cpu_percent, 1)
        }

# Thread-local storage for performance context
_performance_context = threading.local()

@contextmanager
def measure_performance():
    """Lightweight context manager for measuring performance metrics"""
    metrics = PerformanceMetrics()
    
    # Start measurements
    metrics.start_time = time.perf_counter()
    try:
        # Get initial memory usage
        memory_info = metrics._process.memory_info()
        metrics.start_memory_mb = memory_info.rss / (1024 * 1024)
        
        # Start CPU measurement (requires a small interval)
        metrics._process.cpu_percent()  # Initialize CPU measurement
    except:
        metrics.start_memory_mb = 0.0
    
    # Store in thread-local context
    _performance_context.active_metrics = metrics
    
    try:
        yield metrics
    finally:
        # End measurements
        metrics.end_time = time.perf_counter()
        
        try:
            # Get final memory usage
            memory_info = metrics._process.memory_info()
            metrics.end_memory_mb = memory_info.rss / (1024 * 1024)
            
            # Get CPU usage (with small interval for accuracy)
            metrics.cpu_percent = metrics._process.cpu_percent(interval=0.1)
        except:
            metrics.end_memory_mb = metrics.start_memory_mb
            metrics.cpu_percent = 0.0
        
        # Clear thread-local context
        _performance_context.active_metrics = None

def is_performance_monitoring_active() -> bool:
    """Check if performance monitoring is already active in current thread"""
    return hasattr(_performance_context, 'active_metrics') and _performance_context.active_metrics is not None

def profiling_decorator(func: Callable) -> Callable:
    """
    Lightweight profiling decorator that measures performance accurately
    """
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Always measure performance for API endpoints
        with measure_performance() as metrics:
            try:
                # Execute the endpoint function
                response = await func(*args, **kwargs)
                
                # Log performance metrics with appropriate detail level
                execution_time = metrics.execution_time_ms
                memory_usage = metrics.memory_usage_mb
                cpu_usage = metrics.cpu_percent
                
                # Only log if there's meaningful activity (avoid logging zeros)
                if execution_time > 0.1 or memory_usage > 0.1 or cpu_usage > 0.1:
                    logger.info(
                        f"API Performance - {func.__name__}: "
                        f"Time: {execution_time:.2f}ms, "
                        f"Memory: {memory_usage:.2f}MB, "
                        f"CPU: {cpu_usage:.1f}%"
                    )
                else:
                    # For very fast operations, just log the time
                    logger.info(
                        f"API Performance - {func.__name__}: "
                        f"Time: {execution_time:.2f}ms"
                    )
                
                # Add performance metrics to response if it's a dictionary
                if isinstance(response, dict):
                    response.update(metrics.to_dict())
                
                return response
                
            except Exception as e:
                # Log performance metrics even on error
                error_str = str(e)
                execution_time = metrics.execution_time_ms
                
                if "404" in error_str or "400" in error_str or "409" in error_str:
                    # Client errors - use warning level
                    logger.warning(
                        f"API Performance - {func.__name__} (CLIENT_ERROR): "
                        f"Time: {execution_time:.2f}ms, "
                        f"Error: {error_str}"
                    )
                else:
                    # Server errors - use error level
                    logger.error(
                        f"API Performance - {func.__name__} (SERVER_ERROR): "
                        f"Time: {execution_time:.2f}ms, "
                        f"Error: {error_str}"
                    )
                raise
    
    return wrapper

# Simplified repository-level performance logging
def log_repository_performance(operation_name: str, schema_name: str, metrics: Dict[str, Any], **extra_info):
    """
    Lightweight repository performance logging
    """
    execution_time = metrics.get('execution_time_ms', 0)
    memory_usage = metrics.get('memory_usage_mb', 0)
    cpu_usage = metrics.get('cpu_percent', 0)
    
    extra_details = ", ".join([f"{k}={v}" for k, v in extra_info.items()]) if extra_info else ""
    details_str = f", {extra_details}" if extra_details else ""
    
    # Only log meaningful metrics
    if execution_time > 1.0 or memory_usage > 1.0:
        logger.info(
            f"DB Performance - {operation_name} [{schema_name}]: "
            f"Time: {execution_time:.2f}ms, "
            f"Memory: {memory_usage:.2f}MB, "
            f"CPU: {cpu_usage:.1f}%"
            f"{details_str}"
        )

# Simplified use case performance logging  
def log_use_case_performance(use_case_name: str, schema_name: str, duration_ms: float, **extra_info):
    """
    Lightweight use case performance logging
    """
    extra_details = ", ".join([f"{k}={v}" for k, v in extra_info.items()]) if extra_info else ""
    details_str = f", {extra_details}" if extra_details else ""
    
    # Only log operations that take meaningful time
    if duration_ms > 1.0:
        logger.info(
            f"UseCase Performance - {use_case_name} [{schema_name}]: "
            f"Time: {duration_ms:.2f}ms"
            f"{details_str}"
        )

# Utility for bulk operation performance logging
def log_bulk_operation_performance(operation_name: str, schema_name: str, record_count: int, duration_ms: float, **extra_info):
    """
    Specialized logging for bulk operations with record count context
    """
    records_per_second = int(record_count / (duration_ms / 1000)) if duration_ms > 0 else 0
    extra_details = ", ".join([f"{k}={v}" for k, v in extra_info.items()]) if extra_info else ""
    details_str = f", {extra_details}" if extra_details else ""
    
    logger.info(
        f"Bulk Performance - {operation_name} [{schema_name}]: "
        f"Records: {record_count}, "
        f"Time: {duration_ms:.2f}ms, "
        f"Throughput: {records_per_second} records/sec"
        f"{details_str}"
    ) 