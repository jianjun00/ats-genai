"""
Health and metrics API for the data agent.

This module provides HTTP endpoints for health checks and metrics
to allow external systems to monitor the health and performance of the agent.
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
from aiohttp import web

from .monitoring import DataAgentMetrics

logger = logging.getLogger(__name__)

class HealthAPI:
    """
    HTTP API for health checks and metrics.

    Provides endpoints for:
    - /health - Basic health check
    - /health/detailed - Detailed health status
    - /metrics - Current metrics
    """

    def __init__(
        self,
        metrics: DataAgentMetrics,
        host: str = "0.0.0.0",
        port: int = 8080,
        health_checks: Optional[List[Callable[[], Dict[str, Any]]]] = None
    ):
        """
        Initialize the health API.

        Args:
            metrics: Metrics collector to use
            host: Host to bind to
            port: Port to listen on
            health_checks: List of callables that return health check results
        """
        self.metrics = metrics
        self.host = host
        self.port = port
        self.health_checks = health_checks or []
        self.app = web.Application()
        self.runner = None
        self.site = None
        self._setup_routes()

        # Health status
        self.status = {
            "status": "starting",
            "uptime": 0,
            "start_time": datetime.now().isoformat()
        }

    def _setup_routes(self):
        """Set up the API routes."""
        self.app.add_routes([
            web.get("/health", self.health_handler),
            web.get("/health/detailed", self.detailed_health_handler),
            web.get("/metrics", self.metrics_handler)
        ])

    async def start(self):
        """Start the health API server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        self.status["status"] = "running"
        logger.info(f"Health API server started on http://{self.host}:{self.port}")

    async def stop(self):
        """Stop the health API server."""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        self.status["status"] = "stopped"
        logger.info("Health API server stopped")

    def add_health_check(self, check_func: Callable[[], Dict[str, Any]]):
        """
        Add a health check function.

        Args:
            check_func: Function that returns a health check result
        """
        self.health_checks.append(check_func)

    async def health_handler(self, request):
        """
        Handle basic health check requests.

        Returns:
            200 OK if the service is healthy, 503 otherwise
        """
        is_healthy = self.status["status"] == "running"

        # Run quick health checks if any
        for check in self.health_checks:
            try:
                result = check()
                if not result.get("healthy", True):
                    is_healthy = False
                    break
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                is_healthy = False
                break

        status_code = 200 if is_healthy else 503
        return web.json_response(
            {"status": "healthy" if is_healthy else "unhealthy"},
            status=status_code
        )

    async def detailed_health_handler(self, request):
        """
        Handle detailed health check requests.

        Returns:
            Detailed health status including all health checks
        """
        # Calculate uptime
        start_time = datetime.fromisoformat(self.status["start_time"])
        uptime_seconds = (datetime.now() - start_time).total_seconds()

        health_data = {
            "status": self.status["status"],
            "uptime_seconds": uptime_seconds,
            "start_time": self.status["start_time"],
            "checks": []
        }

        # Run all health checks
        all_healthy = True
        for check in self.health_checks:
            try:
                result = check()
                health_data["checks"].append(result)
                if not result.get("healthy", True):
                    all_healthy = False
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                health_data["checks"].append({
                    "name": getattr(check, "__name__", "unknown"),
                    "healthy": False,
                    "error": str(e)
                })
                all_healthy = False

        health_data["overall_health"] = "healthy" if all_healthy else "unhealthy"
        status_code = 200 if all_healthy else 503

        return web.json_response(health_data, status=status_code)

    async def metrics_handler(self, request):
        """
        Handle metrics requests.

        Returns:
            Current metrics from the metrics collector
        """
        metrics_report = self.metrics.get_metrics_report()
        return web.json_response(metrics_report)


class DataAgentHealthChecks:
    """
    Health check functions for the data agent.
    """

    @staticmethod
    def db_connection_check(pool) -> Dict[str, Any]:
        """
        Check database connection.

        Args:
            pool: Database connection pool

        Returns:
            Health check result
        """
        try:
            # Simple query to check connection
            async def _check():
                async with pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                return True

            # Run the async check in a new event loop
            loop = asyncio.new_event_loop()
            result = loop.run_until_complete(_check())
            loop.close()

            return {
                "name": "database_connection",
                "healthy": result,
                "details": "Database connection successful"
            }
        except Exception as e:
            return {
                "name": "database_connection",
                "healthy": False,
                "details": f"Database connection failed: {str(e)}"
            }

    @staticmethod
    def adapter_health_check(adapters) -> Dict[str, Any]:
        """
        Check adapter health.

        Args:
            adapters: Dictionary of adapters

        Returns:
            Health check result
        """
        if not adapters:
            return {
                "name": "adapters",
                "healthy": False,
                "details": "No adapters configured"
            }

        return {
            "name": "adapters",
            "healthy": True,
            "details": f"{len(adapters)} adapters configured",
            "adapter_count": len(adapters)
        }

    @staticmethod
    def metrics_health_check(metrics) -> Dict[str, Any]:
        """
        Check metrics health.

        Args:
            metrics: Metrics collector

        Returns:
            Health check result
        """
        if not metrics:
            return {
                "name": "metrics",
                "healthy": False,
                "details": "Metrics collector not available"
            }

        try:
            # Get metrics report to check if metrics are working
            metrics.get_metrics_report()
            return {
                "name": "metrics",
                "healthy": True,
                "details": "Metrics collector is working"
            }
        except Exception as e:
            return {
                "name": "metrics",
                "healthy": False,
                "details": f"Metrics collector error: {str(e)}"
            }


async def setup_health_api(
    metrics: DataAgentMetrics,
    pool=None,
    adapters=None,
    host: str = "0.0.0.0",
    port: int = 8080
) -> HealthAPI:
    """
    Set up and start the health API.

    Args:
        metrics: Metrics collector
        pool: Database connection pool (optional)
        adapters: Dictionary of adapters (optional)
        host: Host to bind to
        port: Port to listen on

    Returns:
        HealthAPI instance
    """
    health_checks = []

    # Add health checks if dependencies are provided
    if pool:
        health_checks.append(lambda: DataAgentHealthChecks.db_connection_check(pool))
    if adapters:
        health_checks.append(lambda: DataAgentHealthChecks.adapter_health_check(adapters))
    if metrics:
        health_checks.append(lambda: DataAgentHealthChecks.metrics_health_check(metrics))

    # Create and start the API
    api = HealthAPI(metrics, host, port, health_checks)
    await api.start()
    return api
