import time
import functools
import psutil
import tracemalloc
from typing import Callable, Any, Dict, Optional
from fastapi import Request
from app.config.logging_config import logger
from contextlib import contextmanager
import threading

class PerformanceMetrics:
    def __init__(self):
        self.start_time = 0.0
        self.end_time = 0.0
        self.start_memory = 0
        self.end_memory = 0
        self.cpu_percent = 0.0
        self.memory_percent = 0.0
        self._process = psutil.Process()

    @property
    def execution_time_ms(self) -> float:
        return max(0.0, (self.end_time - self.start_time) * 1000)

    @property
    def memory_usage_mb(self) -> float:
        return max(0.0, (self.end_memory - self.start_memory) / (1024 * 1024))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_time_ms": round(self.execution_time_ms, 2),
            "memory_usage_mb": round(self.memory_usage_mb, 2),
            "cpu_percent": round(self.cpu_percent, 1),
            "memory_percent": round(self.memory_percent, 1)
        }

# Thread-local storage for performance context
_performance_context = threading.local()

@contextmanager
def measure_performance():
    """Context manager for measuring performance metrics"""
    metrics = PerformanceMetrics()
    
    # Start measurements
    metrics.start_time = time.perf_counter()
    
    # Start memory tracing if not already started
    try:
        tracemalloc.start()
        metrics.start_memory = tracemalloc.get_traced_memory()[0]
    except RuntimeError:
        # tracemalloc already started
        metrics.start_memory = tracemalloc.get_traced_memory()[0]
    
    # Store in thread-local context to prevent nested logging
    _performance_context.active_metrics = metrics
    
    try:
        yield metrics
    finally:
        # End measurements
        metrics.end_time = time.perf_counter()
        try:
            metrics.end_memory = tracemalloc.get_traced_memory()[0]
        except:
            metrics.end_memory = metrics.start_memory
        
        # Get CPU and memory percentages (these are instantaneous)
        try:
            metrics.cpu_percent = metrics._process.cpu_percent(interval=None)
            metrics.memory_percent = metrics._process.memory_percent()
        except:
            metrics.cpu_percent = 0.0
            metrics.memory_percent = 0.0
        
        # Clear thread-local context
        _performance_context.active_metrics = None

def is_performance_monitoring_active() -> bool:
    """Check if performance monitoring is already active in current thread"""
    return hasattr(_performance_context, 'active_metrics') and _performance_context.active_metrics is not None

def profiling_decorator(func: Callable) -> Callable:
    """
    Consolidated profiling decorator that measures performance without duplication
    """
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Skip if already monitoring in this thread (prevents nested logging)
        if is_performance_monitoring_active():
            return await func(*args, **kwargs)
        
        with measure_performance() as metrics:
            try:
                # Execute the endpoint function
                response = await func(*args, **kwargs)
                
                # Log consolidated performance metrics
                logger.info(
                    f"API Performance - {func.__name__}: "
                    f"Time: {metrics.execution_time_ms:.2f}ms, "
                    f"Memory: {metrics.memory_usage_mb:.2f}MB, "
                    f"CPU: {metrics.cpu_percent:.1f}%"
                )
                
                # Add performance metrics to response if it's a dictionary
                if isinstance(response, dict):
                    response.update(metrics.to_dict())
                
                return response
                
            except Exception as e:
                # Log performance metrics even on error - use appropriate level based on error type
                error_str = str(e)
                if "404" in error_str or "400" in error_str or "409" in error_str:
                    # Client errors - use warning level
                    logger.warning(
                        f"API Performance - {func.__name__} (CLIENT_ERROR): "
                        f"Time: {metrics.execution_time_ms:.2f}ms, "
                        f"Memory: {metrics.memory_usage_mb:.2f}MB, "
                        f"CPU: {metrics.cpu_percent:.1f}%, "
                        f"Error: {error_str}"
                    )
                else:
                    # Server errors - use error level
                    logger.error(
                        f"API Performance - {func.__name__} (SERVER_ERROR): "
                        f"Time: {metrics.execution_time_ms:.2f}ms, "
                        f"Memory: {metrics.memory_usage_mb:.2f}MB, "
                        f"CPU: {metrics.cpu_percent:.1f}%, "
                        f"Error: {error_str}"
                    )
                raise
    
    return wrapper

# Utility function for repository-level performance logging
def log_repository_performance(operation_name: str, schema_name: str, metrics: Dict[str, Any], **extra_info):
    """
    Consolidated repository performance logging
    """
    # Skip if API-level monitoring is active (prevents duplicate logging)
    if is_performance_monitoring_active():
        return
    
    extra_details = ", ".join([f"{k}={v}" for k, v in extra_info.items()]) if extra_info else ""
    details_str = f", {extra_details}" if extra_details else ""
    
    logger.info(
        f"DB Performance - {operation_name} [{schema_name}]: "
        f"Time: {metrics.get('execution_time_ms', 0):.2f}ms, "
        f"Memory: {metrics.get('memory_usage_mb', 0):.2f}MB, "
        f"CPU: {metrics.get('cpu_percent', 0):.1f}%"
        f"{details_str}"
    )

# Utility function for use case performance logging  
def log_use_case_performance(use_case_name: str, schema_name: str, duration_ms: float, **extra_info):
    """
    Consolidated use case performance logging
    """
    # Skip if API-level monitoring is active (prevents duplicate logging)
    if is_performance_monitoring_active():
        return
    
    extra_details = ", ".join([f"{k}={v}" for k, v in extra_info.items()]) if extra_info else ""
    details_str = f", {extra_details}" if extra_details else ""
    
    logger.info(
        f"UseCase Performance - {use_case_name} [{schema_name}]: "
        f"Time: {duration_ms:.2f}ms"
        f"{details_str}"
    ) 