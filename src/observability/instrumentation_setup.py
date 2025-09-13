"""
ATS Platform Instrumentation Setup

This module sets up comprehensive observability instrumentation for the ATS platform,
enabling automatic tracking of Python function usage and database operations.
"""

import os
import sys
from pathlib import Path
from typing import List, Optional

from .code_usage_tracker import get_code_tracker, CodeUsageTracker
from .database_usage_tracker import install_db_tracking


class ATSInstrumentationManager:
    """
    Manages comprehensive instrumentation setup for ATS platform monitoring
    """

    def __init__(self, project_root: str = None):
        if project_root is None:
            project_root = os.environ.get('ATS_PROJECT_ROOT', '/workspace')

        self.project_root = Path(project_root)
        self.code_tracker: Optional[CodeUsageTracker] = None
        self.db_tracker = None
        self.instrumented_modules = set()

    def setup_environment_variables(self):
        """
        Set up required environment variables for observability
        """
        # SigNoz/OpenTelemetry configuration
        os.environ.setdefault('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://localhost:4317')
        os.environ.setdefault('OTEL_SERVICE_NAME', 'ats-platform')
        os.environ.setdefault('OTEL_SERVICE_VERSION', '1.0.0')
        os.environ.setdefault('OTEL_RESOURCE_ATTRIBUTES',
                             'service.name=ats-platform,service.version=1.0.0')

        # ATS-specific configuration
        os.environ.setdefault('ATS_OBSERVABILITY_ENABLED', 'true')
        os.environ.setdefault('ATS_USAGE_DATA_DIR', '/tmp/ats-usage-data')

        print("✅ Environment variables configured for observability")

    def initialize_trackers(self) -> tuple:
        """
        Initialize code and database usage trackers
        """
        print("🚀 Initializing ATS usage trackers...")

        # Initialize code usage tracker
        self.code_tracker = get_code_tracker()
        print("✅ Code usage tracker initialized")

        # Initialize database usage tracker with automatic query interception
        self.db_tracker = install_db_tracking()
        print("✅ Database usage tracker initialized")

        return self.code_tracker, self.db_tracker

    def discover_ats_modules(self) -> List[str]:
        """
        Discover all ATS modules that should be instrumented
        """
        ats_modules = []

        # Core ATS module patterns
        module_patterns = [
            'src.services',
            'src.domains',
            'src.infrastructure',
            'src.api',
            'src.utils',
            'src.core'
        ]

        # Find modules already loaded
        for module_name in sys.modules.keys():
            if any(module_name.startswith(pattern) for pattern in module_patterns):
                ats_modules.append(module_name)

        print(f"🔍 Discovered {len(ats_modules)} ATS modules for instrumentation")
        return ats_modules

    def instrument_all_modules(self, module_patterns: List[str] = None) -> int:
        """
        Instrument all ATS modules for usage tracking
        """
        if not self.code_tracker:
            self.code_tracker = get_code_tracker()

        if module_patterns is None:
            module_patterns = [
                'src.services',
                'src.domains',
                'src.infrastructure',
                'src.api',
                'src.utils'
            ]

        print("🔧 Starting comprehensive module instrumentation...")

        total_instrumented = 0

        # Instrument already loaded modules
        for module_name in sys.modules.keys():
            if any(module_name.startswith(pattern) for pattern in module_patterns):
                if module_name not in self.instrumented_modules:
                    count = self.code_tracker.instrument_module(module_name)
                    total_instrumented += count
                    self.instrumented_modules.add(module_name)

        print(f"✅ Instrumented {total_instrumented} functions across {len(self.instrumented_modules)} modules")
        return total_instrumented

    def setup_auto_instrumentation_hook(self):
        """
        Set up automatic instrumentation for newly imported modules
        """
        original_import = __import__

        def tracked_import(name, *args, **kwargs):
            module = original_import(name, *args, **kwargs)

            # Check if this is an ATS module that should be instrumented
            if (name.startswith('src.') and
                name not in self.instrumented_modules and
                self.code_tracker):

                count = self.code_tracker.instrument_module(name)
                if count > 0:
                    self.instrumented_modules.add(name)
                    print(f"🔧 Auto-instrumented {count} functions in {name}")

            return module

        # Replace built-in import with tracked version
        import builtins
        builtins.__import__ = tracked_import

        print("✅ Auto-instrumentation hook installed")

    def create_metrics_endpoint(self, port: int = 8000):
        """
        Create HTTP endpoint for Prometheus metrics scraping
        """
        try:
            from http.server import HTTPServer, BaseHTTPRequestHandler
            import threading
            import json

            class MetricsHandler(BaseHTTPRequestHandler):
                def do_GET(self):
                    if self.path == '/metrics':
                        self.send_response(200)
                        self.send_header('Content-type', 'text/plain')
                        self.end_headers()

                        # Generate Prometheus-style metrics
                        metrics = self._generate_prometheus_metrics()
                        self.wfile.write(metrics.encode())

                    elif self.path == '/health':
                        self.send_response(200)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()

                        health_data = {
                            'status': 'healthy',
                            'instrumented_modules': len(self.server.instrumented_modules),
                            'tracking_enabled': True
                        }
                        self.wfile.write(json.dumps(health_data).encode())

                    else:
                        self.send_error(404)

                def _generate_prometheus_metrics(self) -> str:
                    """Generate Prometheus-style metrics from tracking data"""
                    if not self.server.code_tracker:
                        return "# No tracking data available\n"

                    stats = self.server.code_tracker.get_usage_stats()
                    metrics = []

                    # Function call metrics
                    metrics.append("# HELP ats_function_calls_total Total function calls")
                    metrics.append("# TYPE ats_function_calls_total counter")

                    for func_name, count in stats['function_frequency'].items():
                        module, function = func_name.rsplit('.', 1) if '.' in func_name else ('unknown', func_name)
                        metrics.append(f'ats_function_calls_total{{module="{module}",function="{function}"}} {count}')

                    # Module usage metrics
                    metrics.append("# HELP ats_module_usage_total Total module usage")
                    metrics.append("# TYPE ats_module_usage_total counter")

                    for module, count in stats['module_distribution'].items():
                        metrics.append(f'ats_module_usage_total{{module="{module}"}} {count}')

                    return '\n'.join(metrics) + '\n'

                def log_message(self, format, *args):
                    # Suppress HTTP request logging
                    pass

            # Create HTTP server
            httpd = HTTPServer(('localhost', port), MetricsHandler)
            httpd.code_tracker = self.code_tracker
            httpd.instrumented_modules = self.instrumented_modules

            # Start server in background thread
            server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
            server_thread.start()

            print(f"✅ Metrics endpoint started at http://localhost:{port}/metrics")
            return httpd

        except Exception as e:
            print(f"⚠️ Failed to start metrics endpoint: {e}")
            return None

    def setup_complete_instrumentation(self, enable_metrics_endpoint: bool = True) -> bool:
        """
        Set up complete instrumentation for ATS platform
        """
        try:
            print("🚀 Starting complete ATS instrumentation setup...")

            # 1. Setup environment
            self.setup_environment_variables()

            # 2. Initialize trackers
            self.initialize_trackers()

            # 3. Instrument existing modules
            instrumented_count = self.instrument_all_modules()

            # 4. Setup auto-instrumentation for future imports
            self.setup_auto_instrumentation_hook()

            # 5. Create metrics endpoint for Prometheus scraping
            if enable_metrics_endpoint:
                self.create_metrics_endpoint()

            print(f"🎉 Complete instrumentation setup successful!")
            print(f"   - {instrumented_count} functions instrumented")
            print(f"   - {len(self.instrumented_modules)} modules tracked")
            print(f"   - Database queries automatically tracked")
            print(f"   - Metrics endpoint: http://localhost:8000/metrics")

            return True

        except Exception as e:
            print(f"❌ Instrumentation setup failed: {e}")
            return False

    def get_instrumentation_status(self) -> dict:
        """
        Get current instrumentation status and statistics
        """
        status = {
            'instrumentation_enabled': bool(self.code_tracker),
            'instrumented_modules_count': len(self.instrumented_modules),
            'instrumented_modules': list(self.instrumented_modules),
            'database_tracking_enabled': bool(self.db_tracker),
        }

        if self.code_tracker:
            stats = self.code_tracker.get_usage_stats()
            status.update({
                'total_function_calls': stats['total_function_calls'],
                'unique_functions_called': stats['unique_functions_called'],
                'tracking_duration_hours': stats['tracking_duration_hours']
            })

        return status


# Global instrumentation manager instance
_instrumentation_manager = None

def get_instrumentation_manager() -> ATSInstrumentationManager:
    """Get or create global instrumentation manager"""
    global _instrumentation_manager
    if _instrumentation_manager is None:
        _instrumentation_manager = ATSInstrumentationManager()
    return _instrumentation_manager

def setup_ats_instrumentation(enable_metrics_endpoint: bool = True) -> bool:
    """
    Convenient function to set up complete ATS instrumentation
    """
    manager = get_instrumentation_manager()
    return manager.setup_complete_instrumentation(enable_metrics_endpoint)

def get_instrumentation_status() -> dict:
    """Get current instrumentation status"""
    manager = get_instrumentation_manager()
    return manager.get_instrumentation_status()