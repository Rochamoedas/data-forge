import time
import functools
import psutil
import tracemalloc
import cProfile
import pstats
import io
from typing import Callable, Any, Dict
from fastapi import Request
from app.config.logging_config import logger
from contextlib import contextmanager

class PerformanceMetrics:
    def __init__(self):
        self.start_time = 0.0
        self.end_time = 0.0
        self.start_memory = 0
        self.end_memory = 0
        self.cpu_percent = 0.0
        self.memory_percent = 0.0
        self.profiler = cProfile.Profile()
        self.profiler_stats = None

    @property
    def execution_time_ms(self) -> float:
        return max(0.0, (self.end_time - self.start_time) * 1000)

    @property
    def memory_usage_mb(self) -> float:
        return max(0.0, (self.end_memory - self.start_memory) / (1024 * 1024))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_time_ms": self.execution_time_ms,
            "memory_usage_mb": self.memory_usage_mb,
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "profiler_stats": self.get_profiler_stats()
        }

    def get_profiler_stats(self) -> Dict[str, Any]:
        if not self.profiler_stats:
            return {}
        
        s = io.StringIO()
        ps = pstats.Stats(self.profiler_stats, stream=s).sort_stats('cumulative')
        ps.print_stats(20)  # Top 20 time-consuming operations
        return {"top_operations": s.getvalue()}

@contextmanager
def measure_performance():
    """Context manager for measuring performance metrics"""
    metrics = PerformanceMetrics()
    
    # Start measurements
    metrics.start_time = time.perf_counter()
    metrics.start_memory = tracemalloc.get_traced_memory()[0]
    tracemalloc.start()
    metrics.profiler.enable()
    
    try:
        yield metrics
    finally:
        # End measurements
        metrics.profiler.disable()
        metrics.profiler_stats = metrics.profiler.getstats()
        metrics.end_time = time.perf_counter()
        metrics.end_memory = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()
        
        # Get CPU and memory percentages
        process = psutil.Process()
        metrics.cpu_percent = process.cpu_percent()
        metrics.memory_percent = process.memory_percent()

def profiling_decorator(func: Callable) -> Callable:
    """
    A comprehensive profiling decorator that measures:
    - Execution time
    - Memory usage
    - CPU utilization
    - Detailed function profiling
    """
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        with measure_performance() as metrics:
            try:
                # Execute the endpoint function
                response = await func(*args, **kwargs)
                
                # Log detailed performance metrics
                logger.info(
                    f"Performance metrics for {func.__name__}:\n"
                    f"  Execution time: {metrics.execution_time_ms:.2f}ms\n"
                    f"  Memory usage: {metrics.memory_usage_mb:.2f}MB\n"
                    f"  CPU usage: {metrics.cpu_percent:.1f}%\n"
                    f"  Memory usage: {metrics.memory_percent:.1f}%"
                )
                
                # If response is a dictionary, add performance metrics
                if isinstance(response, dict):
                    response.update(metrics.to_dict())
                
                return response
                
            except Exception as e:
                # Log performance metrics even if there's an error
                logger.error(
                    f"Performance metrics for failed {func.__name__}:\n"
                    f"  Execution time: {metrics.execution_time_ms:.2f}ms\n"
                    f"  Memory usage: {metrics.memory_usage_mb:.2f}MB\n"
                    f"  CPU usage: {metrics.cpu_percent:.1f}%\n"
                    f"  Memory usage: {metrics.memory_percent:.1f}%"
                )
                raise
    
    return wrapper 