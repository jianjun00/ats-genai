"""
ATS Code Usage Tracker - OpenTelemetry-based function usage monitoring

This module provides comprehensive tracking of Python function usage across the ATS platform
for identifying unused code that can be safely cleaned up.
"""

import functools
import json
import sys
import time
import threading
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Any
import inspect
import importlib


class CodeUsageTracker:
    """
    Tracks function usage across the ATS codebase using a lightweight monitoring system.

    Captures:
    - Function call frequency and timing
    - Module usage patterns
    - Call stack analysis for dependency mapping
    - Usage heatmaps over time
    """

    def __init__(self, output_dir: str = "/tmp/ats-usage-data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Thread-safe counters
        self._lock = threading.Lock()
        self.function_calls = defaultdict(int)
        self.function_timings = defaultdict(list)
        self.call_stacks = defaultdict(list)
        self.module_usage = defaultdict(int)
        self.daily_usage = defaultdict(lambda: defaultdict(int))

        # Track startup time
        self.start_time = datetime.now()
        self.last_flush = self.start_time

        # Auto-flush every 5 minutes
        self._start_auto_flush()

    def track_function_usage(self, func):
        """
        Decorator to track function usage with detailed metrics
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            # Get function details
            module_name = func.__module__ or "unknown"
            function_name = func.__qualname__
            full_name = f"{module_name}.{function_name}"

            # Get call stack for dependency analysis
            stack = inspect.stack()
            caller_info = []
            for frame_info in stack[1:6]:  # Get 5 levels of call stack
                caller_module = frame_info.frame.f_globals.get('__name__', 'unknown')
                caller_function = frame_info.function
                if caller_module.startswith('src.'):
                    caller_info.append(f"{caller_module}.{caller_function}")

            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                # Thread-safe tracking
                with self._lock:
                    self.function_calls[full_name] += 1
                    self.function_timings[full_name].append(execution_time)
                    self.module_usage[module_name] += 1

                    # Daily usage tracking
                    today = datetime.now().strftime('%Y-%m-%d')
                    self.daily_usage[today][full_name] += 1

                    # Call stack tracking
                    if caller_info:
                        self.call_stacks[full_name].extend(caller_info)

                return result

            except Exception as e:
                # Track failed function calls too
                with self._lock:
                    error_key = f"{full_name}._ERROR"
                    self.function_calls[error_key] += 1
                raise

        return wrapper

    def instrument_module(self, module_name: str):
        """
        Automatically instrument all functions in a given module
        """
        try:
            module = importlib.import_module(module_name)
            instrumented_count = 0

            for name, obj in inspect.getmembers(module):
                if (inspect.isfunction(obj) and
                    not name.startswith('_') and
                    obj.__module__ == module_name):

                    # Replace function with instrumented version
                    instrumented_func = self.track_function_usage(obj)
                    setattr(module, name, instrumented_func)
                    instrumented_count += 1

            print(f"✅ Instrumented {instrumented_count} functions in {module_name}")
            return instrumented_count

        except Exception as e:
            print(f"❌ Failed to instrument {module_name}: {e}")
            return 0

    def instrument_ats_modules(self, module_patterns: List[str] = None):
        """
        Auto-instrument all ATS modules based on patterns
        """
        if module_patterns is None:
            module_patterns = [
                'src.services',
                'src.domains',
                'src.infrastructure',
                'src.api',
                'src.utils'
            ]

        total_instrumented = 0
        ats_modules = [name for name in sys.modules.keys()
                      if any(name.startswith(pattern) for pattern in module_patterns)]

        print(f"🔍 Found {len(ats_modules)} ATS modules to instrument")

        for module_name in ats_modules:
            count = self.instrument_module(module_name)
            total_instrumented += count

        print(f"✅ Total instrumented functions: {total_instrumented}")
        return total_instrumented

    def get_usage_stats(self, min_calls: int = 1) -> Dict[str, Any]:
        """
        Get comprehensive usage statistics
        """
        with self._lock:
            # Calculate averages and totals
            stats = {
                'tracking_duration_hours': (datetime.now() - self.start_time).total_seconds() / 3600,
                'total_function_calls': sum(self.function_calls.values()),
                'unique_functions_called': len(self.function_calls),
                'modules_accessed': len(self.module_usage),

                # Function call frequency
                'function_frequency': dict(self.function_calls),

                # Timing analysis
                'function_avg_time': {
                    func: sum(times) / len(times)
                    for func, times in self.function_timings.items()
                    if len(times) > 0
                },

                # Module usage distribution
                'module_distribution': dict(self.module_usage),

                # Daily patterns
                'daily_usage': dict(self.daily_usage),

                # Hot functions (most called)
                'hot_functions': dict(Counter(self.function_calls).most_common(20)),

                # Dependency analysis
                'function_dependencies': {
                    func: list(set(callers))
                    for func, callers in self.call_stacks.items()
                }
            }

        return stats

    def get_unused_functions(self, all_functions: Set[str]) -> Dict[str, Any]:
        """
        Identify functions that haven't been called
        """
        with self._lock:
            called_functions = set(self.function_calls.keys())
            unused_functions = all_functions - called_functions

            return {
                'unused_count': len(unused_functions),
                'unused_functions': list(unused_functions),
                'usage_percentage': len(called_functions) / len(all_functions) * 100,
                'total_functions': len(all_functions),
                'called_functions': len(called_functions)
            }

    def flush_to_disk(self):
        """
        Write current usage data to disk for persistence
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        stats_file = self.output_dir / f"code_usage_{timestamp}.json"

        try:
            stats = self.get_usage_stats()
            with open(stats_file, 'w') as f:
                json.dump(stats, f, indent=2, default=str)

            self.last_flush = datetime.now()
            print(f"✅ Usage data flushed to {stats_file}")
            return stats_file

        except Exception as e:
            print(f"❌ Failed to flush usage data: {e}")
            return None

    def _start_auto_flush(self):
        """
        Start background thread for periodic data flushing
        """
        def auto_flush():
            while True:
                time.sleep(300)  # 5 minutes
                self.flush_to_disk()

        flush_thread = threading.Thread(target=auto_flush, daemon=True)
        flush_thread.start()

    def generate_cleanup_candidates(self, unused_threshold_days: int = 30) -> List[Dict]:
        """
        Generate list of cleanup candidates based on usage patterns
        """
        with self._lock:
            # Analyze daily usage patterns
            cutoff_date = (datetime.now() - timedelta(days=unused_threshold_days)).strftime('%Y-%m-%d')

            cleanup_candidates = []

            # Check for functions with no recent usage
            recent_usage = set()
            for date, functions in self.daily_usage.items():
                if date >= cutoff_date:
                    recent_usage.update(functions.keys())

            # Functions called before but not recently
            all_called = set(self.function_calls.keys())
            stale_functions = all_called - recent_usage

            for func in stale_functions:
                cleanup_candidates.append({
                    'function': func,
                    'total_calls': self.function_calls[func],
                    'last_used': 'more_than_30_days_ago',
                    'cleanup_priority': 'high',
                    'dependencies': list(set(self.call_stacks.get(func, [])))
                })

            # Sort by cleanup priority
            cleanup_candidates.sort(key=lambda x: x['total_calls'])

            return cleanup_candidates


# Global tracker instance
_code_tracker = None

def get_code_tracker() -> CodeUsageTracker:
    """Get or create global code tracker instance"""
    global _code_tracker
    if _code_tracker is None:
        _code_tracker = CodeUsageTracker()
    return _code_tracker

def track_usage(func):
    """Convenient decorator for function usage tracking"""
    return get_code_tracker().track_function_usage(func)