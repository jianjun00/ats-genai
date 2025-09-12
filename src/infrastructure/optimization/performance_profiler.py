"""
Performance Profiler and Optimization Tools

Advanced performance monitoring, profiling, and optimization recommendations
for service-based architecture.
"""

import asyncio
import cProfile
import io
import json
import pstats
import time
import tracemalloc
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional, Callable, AsyncGenerator, Tuple
import logging
import psutil

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics for operations."""
    operation_name: str
    execution_time_ms: float
    memory_usage_mb: float
    cpu_usage_percent: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'operation_name': self.operation_name,
            'execution_time_ms': self.execution_time_ms,
            'memory_usage_mb': self.memory_usage_mb,
            'cpu_usage_percent': self.cpu_usage_percent,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata
        }


@dataclass
class ProfileResult:
    """Results from performance profiling."""
    operation_name: str
    total_time_ms: float
    function_stats: List[Dict[str, Any]]
    memory_stats: Dict[str, Any]
    top_functions: List[Tuple[str, float, int]]
    bottlenecks: List[Dict[str, Any]]
    recommendations: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'operation_name': self.operation_name,
            'total_time_ms': self.total_time_ms,
            'function_stats': self.function_stats,
            'memory_stats': self.memory_stats,
            'top_functions': [
                {'function': func, 'time_ms': time_ms, 'calls': calls}
                for func, time_ms, calls in self.top_functions
            ],
            'bottlenecks': self.bottlenecks,
            'recommendations': self.recommendations
        }


class PerformanceProfiler:
    """Advanced performance profiler for service operations."""
    
    def __init__(self, enable_memory_profiling: bool = True):
        self.enable_memory_profiling = enable_memory_profiling
        self.metrics_history: List[PerformanceMetrics] = []
        self.profile_results: Dict[str, List[ProfileResult]] = {}
        
        # Performance thresholds
        self.slow_operation_threshold_ms = 1000
        self.high_memory_threshold_mb = 100
        self.high_cpu_threshold_percent = 80
        
        # Start memory tracing if enabled
        if self.enable_memory_profiling:
            try:
                tracemalloc.start()
            except RuntimeError:
                # Already started
                pass
    
    @asynccontextmanager
    async def profile_operation(self, operation_name: str) -> AsyncGenerator[None, None]:
        """Context manager for profiling operations."""
        # Start profiling
        profiler = cProfile.Profile()
        profiler.enable()
        
        # Memory tracking
        start_memory = self._get_memory_usage()
        if self.enable_memory_profiling:
            tracemalloc.start()
            start_trace = tracemalloc.take_snapshot()
        
        # CPU tracking
        process = psutil.Process()
        start_cpu = process.cpu_percent()
        
        start_time = time.time()
        
        try:
            yield
        finally:
            # Stop profiling
            end_time = time.time()
            profiler.disable()
            
            # Calculate metrics
            execution_time_ms = (end_time - start_time) * 1000
            end_memory = self._get_memory_usage()
            memory_usage_mb = end_memory - start_memory
            end_cpu = process.cpu_percent()
            cpu_usage_percent = max(end_cpu - start_cpu, 0)
            
            # Memory trace analysis
            memory_stats = {}
            if self.enable_memory_profiling:
                try:
                    end_trace = tracemalloc.take_snapshot()
                    memory_stats = self._analyze_memory_trace(start_trace, end_trace)
                except Exception as e:
                    logger.warning(f"Memory trace analysis failed: {e}")
            
            # Create performance metrics
            metrics = PerformanceMetrics(
                operation_name=operation_name,
                execution_time_ms=execution_time_ms,
                memory_usage_mb=memory_usage_mb,
                cpu_usage_percent=cpu_usage_percent,
                metadata={'memory_stats': memory_stats}
            )
            
            self.metrics_history.append(metrics)
            
            # Analyze profiler results
            profile_result = self._analyze_profile(profiler, operation_name, execution_time_ms, memory_stats)
            
            if operation_name not in self.profile_results:
                self.profile_results[operation_name] = []
            self.profile_results[operation_name].append(profile_result)
            
            # Log performance warnings
            self._log_performance_warnings(metrics)
    
    def profile_sync_function(self, func: Callable) -> Callable:
        """Decorator for profiling synchronous functions."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            operation_name = f"{func.__module__}.{func.__name__}"
            
            with self._sync_profile_context(operation_name):
                return func(*args, **kwargs)
        
        return wrapper
    
    def profile_async_function(self, func: Callable) -> Callable:
        """Decorator for profiling asynchronous functions."""
        @wraps(func)
        async def wrapper(*args, **kwargs):
            operation_name = f"{func.__module__}.{func.__name__}"
            
            async with self.profile_operation(operation_name):
                return await func(*args, **kwargs)
        
        return wrapper
    
    def _sync_profile_context(self, operation_name: str):
        """Synchronous version of profile operation context."""
        return SyncProfileContext(self, operation_name)
    
    def _analyze_profile(
        self, 
        profiler: cProfile.Profile, 
        operation_name: str, 
        total_time_ms: float,
        memory_stats: Dict[str, Any]
    ) -> ProfileResult:
        """Analyze profiler results and generate recommendations."""
        
        # Get profile statistics
        stats_stream = io.StringIO()
        stats = pstats.Stats(profiler, stream=stats_stream)
        stats.sort_stats('cumulative')
        
        # Extract function statistics
        function_stats = []
        top_functions = []
        
        for func_info, (calls, non_recursive_calls, total_time, cumulative_time) in stats.stats.items():
            filename, line_num, func_name = func_info
            
            func_stat = {
                'function': f"{filename}:{line_num}({func_name})",
                'calls': calls,
                'total_time_ms': total_time * 1000,
                'cumulative_time_ms': cumulative_time * 1000,
                'average_time_ms': (total_time / calls * 1000) if calls > 0 else 0
            }
            function_stats.append(func_stat)
            
            # Track top time-consuming functions
            if cumulative_time > 0.01:  # More than 10ms
                top_functions.append((func_stat['function'], cumulative_time * 1000, calls))
        
        # Sort top functions by time
        top_functions.sort(key=lambda x: x[1], reverse=True)
        top_functions = top_functions[:10]
        
        # Identify bottlenecks
        bottlenecks = self._identify_bottlenecks(function_stats, total_time_ms)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(function_stats, memory_stats, total_time_ms)
        
        return ProfileResult(
            operation_name=operation_name,
            total_time_ms=total_time_ms,
            function_stats=function_stats,
            memory_stats=memory_stats,
            top_functions=top_functions,
            bottlenecks=bottlenecks,
            recommendations=recommendations
        )
    
    def _identify_bottlenecks(self, function_stats: List[Dict[str, Any]], total_time_ms: float) -> List[Dict[str, Any]]:
        """Identify performance bottlenecks."""
        bottlenecks = []
        
        for func_stat in function_stats:
            # Functions taking more than 20% of total time
            if func_stat['cumulative_time_ms'] > total_time_ms * 0.2:
                bottlenecks.append({
                    'type': 'high_cumulative_time',
                    'function': func_stat['function'],
                    'cumulative_time_ms': func_stat['cumulative_time_ms'],
                    'percentage_of_total': (func_stat['cumulative_time_ms'] / total_time_ms) * 100,
                    'severity': 'high' if func_stat['cumulative_time_ms'] > total_time_ms * 0.5 else 'medium'
                })
            
            # Functions with high average call time
            if func_stat['average_time_ms'] > 100:
                bottlenecks.append({
                    'type': 'slow_function_calls',
                    'function': func_stat['function'],
                    'average_time_ms': func_stat['average_time_ms'],
                    'calls': func_stat['calls'],
                    'severity': 'high' if func_stat['average_time_ms'] > 500 else 'medium'
                })
            
            # Functions called too frequently
            if func_stat['calls'] > 10000:
                bottlenecks.append({
                    'type': 'excessive_calls',
                    'function': func_stat['function'],
                    'calls': func_stat['calls'],
                    'total_time_ms': func_stat['cumulative_time_ms'],
                    'severity': 'medium'
                })
        
        return sorted(bottlenecks, key=lambda x: x.get('cumulative_time_ms', 0), reverse=True)
    
    def _generate_recommendations(
        self, 
        function_stats: List[Dict[str, Any]], 
        memory_stats: Dict[str, Any],
        total_time_ms: float
    ) -> List[str]:
        """Generate performance optimization recommendations."""
        recommendations = []
        
        # Time-based recommendations
        if total_time_ms > self.slow_operation_threshold_ms:
            recommendations.append(
                f"Operation is slow ({total_time_ms:.1f}ms). Consider caching or optimization."
            )
        
        # Function-specific recommendations
        slow_functions = [f for f in function_stats if f['average_time_ms'] > 50]
        if slow_functions:
            recommendations.append(
                f"Found {len(slow_functions)} slow functions. Consider optimizing database queries or algorithms."
            )
        
        frequent_functions = [f for f in function_stats if f['calls'] > 1000]
        if frequent_functions:
            recommendations.append(
                f"Found {len(frequent_functions)} frequently called functions. Consider caching results."
            )
        
        # Memory-based recommendations
        if memory_stats.get('peak_memory_mb', 0) > self.high_memory_threshold_mb:
            recommendations.append(
                f"High memory usage ({memory_stats['peak_memory_mb']:.1f}MB). Consider memory optimization."
            )
        
        if memory_stats.get('memory_growth_mb', 0) > 50:
            recommendations.append(
                "Significant memory growth detected. Check for memory leaks."
            )
        
        # Database-specific recommendations
        db_functions = [f for f in function_stats if 'database' in f['function'].lower() or 'query' in f['function'].lower()]
        if db_functions:
            total_db_time = sum(f['cumulative_time_ms'] for f in db_functions)
            if total_db_time > total_time_ms * 0.5:
                recommendations.append(
                    "Database operations consume > 50% of execution time. Consider query optimization or caching."
                )
        
        # General recommendations
        if not recommendations:
            recommendations.append("Performance is within acceptable limits.")
        
        return recommendations
    
    def _analyze_memory_trace(self, start_snapshot, end_snapshot) -> Dict[str, Any]:
        """Analyze memory usage changes."""
        try:
            top_stats = end_snapshot.compare_to(start_snapshot, 'lineno')
            
            total_size_diff = sum(stat.size_diff for stat in top_stats)
            total_count_diff = sum(stat.count_diff for stat in top_stats)
            
            # Find top memory allocations
            top_allocations = []
            for index, stat in enumerate(top_stats[:10]):
                top_allocations.append({
                    'traceback': str(stat.traceback),
                    'size_diff_mb': stat.size_diff / 1024 / 1024,
                    'count_diff': stat.count_diff
                })
            
            return {
                'total_memory_growth_mb': total_size_diff / 1024 / 1024,
                'total_allocation_count_diff': total_count_diff,
                'top_allocations': top_allocations,
                'peak_memory_mb': end_snapshot.statistics('lineno')[0].size / 1024 / 1024 if end_snapshot.statistics('lineno') else 0
            }
            
        except Exception as e:
            logger.warning(f"Memory trace analysis failed: {e}")
            return {}
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0
    
    def _log_performance_warnings(self, metrics: PerformanceMetrics):
        """Log performance warnings based on metrics."""
        if metrics.execution_time_ms > self.slow_operation_threshold_ms:
            logger.warning(
                f"Slow operation detected: {metrics.operation_name} "
                f"took {metrics.execution_time_ms:.1f}ms"
            )
        
        if metrics.memory_usage_mb > self.high_memory_threshold_mb:
            logger.warning(
                f"High memory usage: {metrics.operation_name} "
                f"used {metrics.memory_usage_mb:.1f}MB"
            )
        
        if metrics.cpu_usage_percent > self.high_cpu_threshold_percent:
            logger.warning(
                f"High CPU usage: {metrics.operation_name} "
                f"used {metrics.cpu_usage_percent:.1f}% CPU"
            )
    
    def get_performance_summary(self, operation_name: Optional[str] = None) -> Dict[str, Any]:
        """Get performance summary for operations."""
        if operation_name:
            relevant_metrics = [m for m in self.metrics_history if m.operation_name == operation_name]
        else:
            relevant_metrics = self.metrics_history
        
        if not relevant_metrics:
            return {'message': 'No performance data available'}
        
        # Calculate statistics
        execution_times = [m.execution_time_ms for m in relevant_metrics]
        memory_usages = [m.memory_usage_mb for m in relevant_metrics]
        cpu_usages = [m.cpu_usage_percent for m in relevant_metrics]
        
        return {
            'operation_name': operation_name or 'All Operations',
            'total_operations': len(relevant_metrics),
            'execution_time_stats': {
                'average_ms': sum(execution_times) / len(execution_times),
                'min_ms': min(execution_times),
                'max_ms': max(execution_times),
                'p95_ms': sorted(execution_times)[int(len(execution_times) * 0.95)] if len(execution_times) > 20 else max(execution_times)
            },
            'memory_stats': {
                'average_mb': sum(memory_usages) / len(memory_usages),
                'min_mb': min(memory_usages),
                'max_mb': max(memory_usages)
            },
            'cpu_stats': {
                'average_percent': sum(cpu_usages) / len(cpu_usages),
                'max_percent': max(cpu_usages)
            },
            'slow_operations_count': len([m for m in relevant_metrics if m.execution_time_ms > self.slow_operation_threshold_ms]),
            'high_memory_operations_count': len([m for m in relevant_metrics if m.memory_usage_mb > self.high_memory_threshold_mb])
        }
    
    def get_latest_profile_result(self, operation_name: str) -> Optional[ProfileResult]:
        """Get the latest profile result for an operation."""
        if operation_name not in self.profile_results:
            return None
        
        return self.profile_results[operation_name][-1]
    
    def export_metrics(self, format: str = 'json') -> str:
        """Export performance metrics in specified format."""
        if format == 'json':
            metrics_data = [m.to_dict() for m in self.metrics_history]
            return json.dumps(metrics_data, indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def clear_history(self, older_than_hours: Optional[int] = None):
        """Clear metrics history."""
        if older_than_hours is None:
            self.metrics_history.clear()
            self.profile_results.clear()
        else:
            cutoff_time = datetime.utcnow() - timedelta(hours=older_than_hours)
            
            self.metrics_history = [
                m for m in self.metrics_history 
                if m.timestamp > cutoff_time
            ]
            
            # Clean up profile results (keep recent ones)
            for operation_name in list(self.profile_results.keys()):
                # Keep only the last 10 results for each operation
                self.profile_results[operation_name] = self.profile_results[operation_name][-10:]


class SyncProfileContext:
    """Synchronous context manager for profiling."""
    
    def __init__(self, profiler: PerformanceProfiler, operation_name: str):
        self.profiler = profiler
        self.operation_name = operation_name
        self.start_time = None
        self.cprofile = None
    
    def __enter__(self):
        self.cprofile = cProfile.Profile()
        self.cprofile.enable()
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cprofile:
            self.cprofile.disable()
        
        execution_time_ms = (time.time() - self.start_time) * 1000
        
        # Create simplified metrics for sync operations
        metrics = PerformanceMetrics(
            operation_name=self.operation_name,
            execution_time_ms=execution_time_ms,
            memory_usage_mb=self.profiler._get_memory_usage(),
            cpu_usage_percent=0  # CPU tracking is complex for sync operations
        )
        
        self.profiler.metrics_history.append(metrics)
        
        # Analyze profile
        if self.cprofile:
            profile_result = self.profiler._analyze_profile(
                self.cprofile, 
                self.operation_name, 
                execution_time_ms,
                {}
            )
            
            if self.operation_name not in self.profiler.profile_results:
                self.profiler.profile_results[self.operation_name] = []
            self.profiler.profile_results[self.operation_name].append(profile_result)


class PerformanceOptimizer:
    """Automated performance optimization suggestions."""
    
    def __init__(self, profiler: PerformanceProfiler):
        self.profiler = profiler
    
    async def analyze_and_recommend(self, operation_name: str) -> Dict[str, Any]:
        """Analyze operation performance and provide recommendations."""
        summary = self.profiler.get_performance_summary(operation_name)
        latest_profile = self.profiler.get_latest_profile_result(operation_name)
        
        if not summary or summary.get('message'):
            return {'error': 'No performance data available for analysis'}
        
        recommendations = {
            'operation_name': operation_name,
            'performance_grade': self._calculate_performance_grade(summary),
            'optimization_priority': self._calculate_optimization_priority(summary),
            'specific_recommendations': [],
            'implementation_suggestions': []
        }
        
        # Database optimization recommendations
        if summary['execution_time_stats']['average_ms'] > 1000:
            recommendations['specific_recommendations'].append({
                'type': 'caching',
                'priority': 'high',
                'description': 'Operation is slow - implement caching',
                'implementation': 'Add @cached decorator with appropriate TTL'
            })
        
        # Memory optimization recommendations
        if summary['memory_stats']['max_mb'] > 100:
            recommendations['specific_recommendations'].append({
                'type': 'memory_optimization',
                'priority': 'medium',
                'description': 'High memory usage detected',
                'implementation': 'Review data structures and implement streaming where possible'
            })
        
        # Concurrency recommendations
        if summary['total_operations'] > 1000:
            recommendations['specific_recommendations'].append({
                'type': 'concurrency',
                'priority': 'medium',
                'description': 'High operation frequency - consider async optimization',
                'implementation': 'Implement connection pooling and batch operations'
            })
        
        # Add profile-specific recommendations
        if latest_profile:
            recommendations['profile_recommendations'] = latest_profile.recommendations
        
        return recommendations
    
    def _calculate_performance_grade(self, summary: Dict[str, Any]) -> str:
        """Calculate performance grade A-F."""
        avg_time = summary['execution_time_stats']['average_ms']
        slow_ops_ratio = summary['slow_operations_count'] / summary['total_operations']
        
        if avg_time < 100 and slow_ops_ratio < 0.01:
            return 'A'
        elif avg_time < 300 and slow_ops_ratio < 0.05:
            return 'B'
        elif avg_time < 800 and slow_ops_ratio < 0.1:
            return 'C'
        elif avg_time < 2000 and slow_ops_ratio < 0.25:
            return 'D'
        else:
            return 'F'
    
    def _calculate_optimization_priority(self, summary: Dict[str, Any]) -> str:
        """Calculate optimization priority."""
        avg_time = summary['execution_time_stats']['average_ms']
        slow_ops_ratio = summary['slow_operations_count'] / summary['total_operations']
        
        if avg_time > 2000 or slow_ops_ratio > 0.3:
            return 'critical'
        elif avg_time > 1000 or slow_ops_ratio > 0.15:
            return 'high'
        elif avg_time > 500 or slow_ops_ratio > 0.05:
            return 'medium'
        else:
            return 'low'


# Global profiler instance
_global_profiler: Optional[PerformanceProfiler] = None


def get_performance_profiler() -> PerformanceProfiler:
    """Get or create global performance profiler."""
    global _global_profiler
    if _global_profiler is None:
        _global_profiler = PerformanceProfiler()
    return _global_profiler


# Convenience decorators
def profile_performance(operation_name: Optional[str] = None):
    """Decorator for profiling function performance."""
    def decorator(func):
        profiler = get_performance_profiler()
        name = operation_name or f"{func.__module__}.{func.__name__}"
        
        if asyncio.iscoroutinefunction(func):
            return profiler.profile_async_function(func)
        else:
            return profiler.profile_sync_function(func)
    
    return decorator