"""
Data Pipeline Performance Monitor - Real-time monitoring of data fetching performance.
Tracks query times, cache hit rates, and identifies performance bottlenecks.
"""
import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque, defaultdict
import logging
from config.environment import Environment


@dataclass
class PerformanceMetric:
    """Container for individual performance measurements."""
    operation: str
    duration_seconds: float
    instrument_count: int
    cache_hits: int = 0
    cache_misses: int = 0
    database_queries: int = 0
    timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def cache_hit_rate(self) -> float:
        """Calculate cache hit rate percentage."""
        total = self.cache_hits + self.cache_misses
        return (self.cache_hits / total * 100) if total > 0 else 0.0
    
    @property
    def avg_time_per_instrument(self) -> float:
        """Calculate average processing time per instrument."""
        return self.duration_seconds / max(self.instrument_count, 1)


class DataPipelinePerformanceMonitor:
    """
    Real-time performance monitoring for data pipeline operations.
    Tracks key metrics and provides optimization insights.
    """
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.metrics: deque = deque(maxlen=window_size)
        self.operation_stats: Dict[str, List[PerformanceMetric]] = defaultdict(list)
        self.logger = logging.getLogger(__name__)
        
        # Thresholds for performance alerts
        self.slow_query_threshold = 1.0  # seconds
        self.low_cache_hit_rate_threshold = 80.0  # percentage
        self.high_db_query_rate_threshold = 0.1  # queries per instrument
        
    def record_metric(self, metric: PerformanceMetric):
        """Record a new performance metric."""
        self.metrics.append(metric)
        self.operation_stats[metric.operation].append(metric)
        
        # Trim operation-specific stats to prevent memory growth
        if len(self.operation_stats[metric.operation]) > self.window_size:
            self.operation_stats[metric.operation] = self.operation_stats[metric.operation][-self.window_size:]
        
        # Check for performance issues
        self._check_performance_alerts(metric)
        
        self.logger.debug(f"Recorded metric: {metric.operation} - {metric.duration_seconds:.3f}s "
                         f"for {metric.instrument_count} instruments")
    
    def _check_performance_alerts(self, metric: PerformanceMetric):
        """Check if metric indicates performance issues and log alerts."""
        alerts = []
        
        if metric.duration_seconds > self.slow_query_threshold:
            alerts.append(f"Slow query detected: {metric.duration_seconds:.3f}s")
        
        if metric.cache_hit_rate < self.low_cache_hit_rate_threshold:
            alerts.append(f"Low cache hit rate: {metric.cache_hit_rate:.1f}%")
        
        db_queries_per_instrument = metric.database_queries / max(metric.instrument_count, 1)
        if db_queries_per_instrument > self.high_db_query_rate_threshold:
            alerts.append(f"High DB query rate: {db_queries_per_instrument:.3f} queries/instrument")
        
        if alerts:
            self.logger.warning(f"Performance alert for {metric.operation}: {'; '.join(alerts)}")
    
    def get_summary_stats(self, operation: Optional[str] = None) -> Dict:
        """Get summary statistics for all operations or specific operation."""
        if operation:
            metrics = [m for m in self.metrics if m.operation == operation]
        else:
            metrics = list(self.metrics)
        
        if not metrics:
            return {"error": f"No metrics found for operation: {operation}"}
        
        durations = [m.duration_seconds for m in metrics]
        cache_hit_rates = [m.cache_hit_rate for m in metrics if m.cache_hits + m.cache_misses > 0]
        avg_times = [m.avg_time_per_instrument for m in metrics]
        
        return {
            "operation": operation or "all_operations",
            "sample_count": len(metrics),
            "duration": {
                "avg": sum(durations) / len(durations),
                "min": min(durations),
                "max": max(durations),
                "p95": sorted(durations)[int(0.95 * len(durations))] if len(durations) >= 20 else max(durations)
            },
            "cache_performance": {
                "avg_hit_rate": sum(cache_hit_rates) / len(cache_hit_rates) if cache_hit_rates else 0,
                "samples_with_cache": len(cache_hit_rates)
            },
            "efficiency": {
                "avg_time_per_instrument": sum(avg_times) / len(avg_times),
                "total_instruments_processed": sum(m.instrument_count for m in metrics),
                "total_database_queries": sum(m.database_queries for m in metrics)
            },
            "time_range": {
                "start": min(m.timestamp for m in metrics).isoformat(),
                "end": max(m.timestamp for m in metrics).isoformat()
            }
        }
    
    def get_performance_comparison(self, operation1: str, operation2: str) -> Dict:
        """Compare performance between two operations (e.g., old vs new implementation)."""
        stats1 = self.get_summary_stats(operation1)
        stats2 = self.get_summary_stats(operation2)
        
        if "error" in stats1 or "error" in stats2:
            return {"error": "Insufficient data for comparison"}
        
        def calc_improvement(val1, val2, lower_is_better=True):
            if val2 == 0:
                return float('inf') if val1 > 0 else 0
            ratio = val1 / val2
            return ratio if lower_is_better else 1 / ratio
        
        return {
            "operations": [operation1, operation2],
            "duration_improvement": {
                "avg": calc_improvement(stats1["duration"]["avg"], stats2["duration"]["avg"]),
                "p95": calc_improvement(stats1["duration"]["p95"], stats2["duration"]["p95"])
            },
            "efficiency_improvement": {
                "time_per_instrument": calc_improvement(
                    stats1["efficiency"]["avg_time_per_instrument"],
                    stats2["efficiency"]["avg_time_per_instrument"]
                )
            },
            "cache_improvement": {
                "hit_rate_diff": stats2["cache_performance"]["avg_hit_rate"] - stats1["cache_performance"]["avg_hit_rate"]
            },
            "summary": {
                operation1: stats1,
                operation2: stats2
            }
        }
    
    def export_metrics_csv(self, filename: str):
        """Export metrics to CSV for analysis."""
        import csv
        
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['timestamp', 'operation', 'duration_seconds', 'instrument_count',
                         'cache_hits', 'cache_misses', 'cache_hit_rate', 'database_queries',
                         'avg_time_per_instrument']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for metric in self.metrics:
                writer.writerow({
                    'timestamp': metric.timestamp.isoformat(),
                    'operation': metric.operation,
                    'duration_seconds': metric.duration_seconds,
                    'instrument_count': metric.instrument_count,
                    'cache_hits': metric.cache_hits,
                    'cache_misses': metric.cache_misses,
                    'cache_hit_rate': metric.cache_hit_rate,
                    'database_queries': metric.database_queries,
                    'avg_time_per_instrument': metric.avg_time_per_instrument
                })
        
        self.logger.info(f"Exported {len(self.metrics)} metrics to {filename}")
    
    def print_dashboard(self):
        """Print a real-time performance dashboard to console."""
        print("\n" + "="*80)
        print("DATA PIPELINE PERFORMANCE DASHBOARD")
        print("="*80)
        
        # Overall stats
        if self.metrics:
            recent_metrics = list(self.metrics)[-20:]  # Last 20 operations
            avg_duration = sum(m.duration_seconds for m in recent_metrics) / len(recent_metrics)
            total_instruments = sum(m.instrument_count for m in recent_metrics)
            
            print(f"Recent Performance (last {len(recent_metrics)} operations):")
            print(f"  Average Duration: {avg_duration:.3f}s")
            print(f"  Total Instruments: {total_instruments}")
            print(f"  Operations/min: {len(recent_metrics) / max((recent_metrics[-1].timestamp - recent_metrics[0].timestamp).total_seconds() / 60, 1):.1f}")
        
        # Per-operation breakdown
        print("\nPer-Operation Statistics:")
        for operation, metrics in self.operation_stats.items():
            if not metrics:
                continue
            recent = metrics[-10:]  # Last 10 for this operation
            avg_duration = sum(m.duration_seconds for m in recent) / len(recent)
            avg_cache_rate = sum(m.cache_hit_rate for m in recent) / len(recent)
            
            print(f"  {operation}:")
            print(f"    Avg Duration: {avg_duration:.3f}s")
            print(f"    Avg Cache Hit Rate: {avg_cache_rate:.1f}%")
            print(f"    Samples: {len(recent)}")
        
        print("="*80)


class PerformanceTimer:
    """Context manager for timing operations with automatic metric recording."""
    
    def __init__(self, monitor: DataPipelinePerformanceMonitor, operation: str, 
                 instrument_count: int = 0):
        self.monitor = monitor
        self.operation = operation
        self.instrument_count = instrument_count
        self.start_time = None
        self.cache_hits = 0
        self.cache_misses = 0
        self.database_queries = 0
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            metric = PerformanceMetric(
                operation=self.operation,
                duration_seconds=duration,
                instrument_count=self.instrument_count,
                cache_hits=self.cache_hits,
                cache_misses=self.cache_misses,
                database_queries=self.database_queries
            )
            self.monitor.record_metric(metric)
    
    def record_cache_hit(self):
        """Record a cache hit."""
        self.cache_hits += 1
    
    def record_cache_miss(self):
        """Record a cache miss."""
        self.cache_misses += 1
    
    def record_database_query(self):
        """Record a database query."""
        self.database_queries += 1


# Global performance monitor instance
_global_monitor = None

def get_performance_monitor() -> DataPipelinePerformanceMonitor:
    """Get the global performance monitor instance."""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = DataPipelinePerformanceMonitor()
    return _global_monitor

def time_operation(operation: str, instrument_count: int = 0) -> PerformanceTimer:
    """Convenience function to create a performance timer."""
    return PerformanceTimer(get_performance_monitor(), operation, instrument_count)


# Example usage
async def example_usage():
    """Demonstrate how to use the performance monitoring system."""
    monitor = get_performance_monitor()
    
    # Example 1: Manual metric recording
    start = time.time()
    # ... do some work ...
    await asyncio.sleep(0.1)  # Simulate work
    duration = time.time() - start
    
    metric = PerformanceMetric(
        operation="fetch_ohlc_batch",
        duration_seconds=duration,
        instrument_count=50,
        cache_hits=45,
        cache_misses=5,
        database_queries=2
    )
    monitor.record_metric(metric)
    
    # Example 2: Using context manager
    with time_operation("symbol_resolution", instrument_count=100) as timer:
        await asyncio.sleep(0.05)  # Simulate work
        timer.record_cache_hit()
        timer.record_database_query()
    
    # View results
    print(monitor.get_summary_stats())
    monitor.print_dashboard()


if __name__ == "__main__":
    # Run example
    asyncio.run(example_usage())