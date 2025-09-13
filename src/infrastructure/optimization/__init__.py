"""
Optimization Infrastructure Module

Performance profiling, monitoring, and optimization tools for service-based architecture.
"""

from .performance_profiler import (
    # Performance metrics and profiling
    PerformanceMetrics,
    ProfileResult,
    PerformanceProfiler,
    PerformanceOptimizer,
    SyncProfileContext,

    # Global profiler access
    get_performance_profiler,
    profile_performance
)

__all__ = [
    # Performance profiling
    'PerformanceMetrics',
    'ProfileResult',
    'PerformanceProfiler',
    'PerformanceOptimizer',
    'SyncProfileContext',
    'get_performance_profiler',
    'profile_performance'
]