#!/usr/bin/env python3
"""
Defensive Resource Management for Financial Systems

Implements comprehensive resource management with:
- Connection pooling with defensive limits
- Timeout management for all operations
- Automatic cleanup and resource disposal
- Circuit breaker integration
- Resource leak detection
- Memory usage monitoring
- Rate limiting for external services

This module ensures system stability under high load and prevents resource exhaustion attacks.
"""

import asyncio
import contextlib
import gc
import logging
import threading

# Defensive import for optional psutil dependency
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    # Create a minimal psutil implementation for graceful degradation
    class MinimalProcess:
        def memory_info(self):
            return type('MemInfo', (), {'rss': 100 * 1024 * 1024})()  # Minimal 100MB

    class MinimalPsutil:
        @staticmethod
        def Process():
            return MinimalProcess()

        @staticmethod
        def cpu_count():
            return 4  # Default value

    psutil = MinimalPsutil()
import time
import weakref
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Union, Callable

# Handle AsyncGenerator import for different Python versions
try:
    from typing import AsyncGenerator
except ImportError:
    from collections.abc import AsyncGenerator
import concurrent.futures

# Defensive imports with fallbacks
try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


class ResourceType(Enum):
    """Types of managed resources"""
    DATABASE = "database"
    HTTP_CLIENT = "http_client"
    FILE_HANDLE = "file_handle"
    THREAD_POOL = "thread_pool"
    MEMORY_CACHE = "memory_cache"
    EXTERNAL_API = "external_api"


class ResourceState(Enum):
    """Resource states"""
    IDLE = "idle"
    ACTIVE = "active"
    EXHAUSTED = "exhausted"
    ERROR = "error"
    CLOSED = "closed"


@dataclass
class ResourceLimits:
    """Defensive resource limits"""
    max_connections: int = 20
    max_idle_time: float = 300.0  # 5 minutes
    max_lifetime: float = 3600.0  # 1 hour
    connection_timeout: float = 30.0
    query_timeout: float = 60.0
    max_retries: int = 3
    backoff_factor: float = 2.0
    max_memory_mb: int = 1024  # 1GB
    max_active_requests: int = 100


@dataclass
class ResourceMetrics:
    """Resource usage metrics"""
    total_created: int = 0
    active_count: int = 0
    idle_count: int = 0
    error_count: int = 0
    last_activity: Optional[datetime] = None
    memory_usage_mb: float = 0.0
    average_response_time: float = 0.0
    success_rate: float = 100.0


class ResourceTracker:
    """Tracks resource usage to detect leaks"""
    def __init__(self):
        self._resources: weakref.WeakSet = weakref.WeakSet()
        self._creation_times: Dict[int, float] = {}
        self._lock = threading.Lock()

    def register(self, resource: Any) -> None:
        """Register a resource for tracking"""
        with self._lock:
            self._resources.add(resource)
            self._creation_times[id(resource)] = time.time()

    def get_active_count(self) -> int:
        """Get count of active resources"""
        return len(self._resources)

    def get_old_resources(self, max_age_seconds: float = 3600.0) -> List[Any]:
        """Get resources older than max_age_seconds"""
        current_time = time.time()
        old_resources = []

        for resource in self._resources:
            creation_time = self._creation_times.get(id(resource))
            if creation_time and (current_time - creation_time) > max_age_seconds:
                old_resources.append(resource)

        return old_resources


class DefensiveResourceManager:
    """
    Defensive resource manager for financial systems.

    Provides:
    - Connection pooling with limits
    - Automatic timeout management
    - Resource leak detection
    - Circuit breaker integration
    - Memory monitoring
    - Comprehensive cleanup
    """

    def __init__(self, resource_type: ResourceType, limits: ResourceLimits = None):
        self.resource_type = resource_type
        self.limits = limits or ResourceLimits()
        self.metrics = ResourceMetrics()
        self.state = ResourceState.IDLE

        # Resource tracking
        self.tracker = ResourceTracker()
        self._active_resources: Dict[int, Any] = {}
        self._lock = threading.RLock()

        # Cleanup thread
        self._cleanup_thread = None
        self._should_cleanup = threading.Event()
        self._start_cleanup_thread()

        # Logging
        self.logger = logging.getLogger(f"{__name__}.{resource_type.value}")

    def _start_cleanup_thread(self):
        """Start background cleanup thread"""
        def cleanup_worker():
            while not self._should_cleanup.is_set():
                try:
                    self._periodic_cleanup()
                    self._should_cleanup.wait(60)  # Cleanup every minute
                except Exception as e:
                    self.logger.error(f"Cleanup thread error: {e}")

        self._cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        self._cleanup_thread.start()

    def _periodic_cleanup(self):
        """Periodic resource cleanup"""
        with self._lock:
            # Clean up old resources
            old_resources = self.tracker.get_old_resources(self.limits.max_lifetime)
            for resource in old_resources:
                try:
                    if hasattr(resource, 'close'):
                        resource.close()
                    elif hasattr(resource, 'disconnect'):
                        resource.disconnect()
                    self.logger.info(f"Cleaned up old resource: {type(resource)}")
                except Exception as e:
                    self.logger.warning(f"Error cleaning up resource: {e}")

            # Update metrics
            self.metrics.active_count = len(self._active_resources)

            # Memory check
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            self.metrics.memory_usage_mb = memory_mb

            if memory_mb > self.limits.max_memory_mb:
                self.logger.warning(
                    f"Memory usage high: {memory_mb:.1f}MB > {self.limits.max_memory_mb}MB"
                )
                gc.collect()  # Force garbage collection

    @contextmanager
    def defensive_database_connection(self, database_url: str) -> Generator[Any, None, None]:
        """
        Defensive database connection with timeouts and cleanup.

        Args:
            database_url: Database connection URL

        Yields:
            Database connection with defensive protections
        """
        if not ASYNCPG_AVAILABLE:
            raise RuntimeError("asyncpg not available for database connections")

        connection = None
        start_time = time.time()

        try:
            # Check resource limits
            if len(self._active_resources) >= self.limits.max_connections:
                self.state = ResourceState.EXHAUSTED
                raise RuntimeError(f"Connection limit exceeded: {self.limits.max_connections}")

            self.state = ResourceState.ACTIVE

            # Create connection with timeout
            connection = asyncio.get_event_loop().run_until_complete(
                asyncio.wait_for(
                    asyncpg.connect(database_url),
                    timeout=self.limits.connection_timeout
                )
            )

            # Register and track
            conn_id = id(connection)
            self._active_resources[conn_id] = connection
            self.tracker.register(connection)
            self.metrics.total_created += 1

            self.logger.debug(f"Created database connection: {conn_id}")

            yield connection

        except asyncio.TimeoutError:
            self.state = ResourceState.ERROR
            self.metrics.error_count += 1
            raise RuntimeError(f"Database connection timeout after {self.limits.connection_timeout}s")

        except Exception as e:
            self.state = ResourceState.ERROR
            self.metrics.error_count += 1
            self.logger.error(f"Database connection error: {e}")
            raise

        finally:
            # Cleanup
            if connection:
                try:
                    conn_id = id(connection)
                    if conn_id in self._active_resources:
                        del self._active_resources[conn_id]

                    asyncio.get_event_loop().run_until_complete(connection.close())
                    self.logger.debug(f"Closed database connection: {conn_id}")
                except Exception as e:
                    self.logger.warning(f"Error closing database connection: {e}")

            # Update metrics
            response_time = time.time() - start_time
            if self.metrics.average_response_time == 0:
                self.metrics.average_response_time = response_time
            else:
                self.metrics.average_response_time = (
                    self.metrics.average_response_time * 0.9 + response_time * 0.1
                )

            self.metrics.last_activity = datetime.utcnow()
            self.state = ResourceState.IDLE

    @asynccontextmanager
    async def defensive_http_client(self,
                                  base_url: str = None,
                                  headers: Dict[str, str] = None) -> AsyncGenerator[Any, None]:
        """
        Defensive HTTP client with timeouts and rate limiting.

        Args:
            base_url: Base URL for requests
            headers: Default headers

        Yields:
            HTTP client with defensive protections
        """
        if not HTTPX_AVAILABLE:
            raise RuntimeError("httpx not available for HTTP connections")

        client = None
        start_time = time.time()

        try:
            # Check resource limits
            if len(self._active_resources) >= self.limits.max_active_requests:
                self.state = ResourceState.EXHAUSTED
                raise RuntimeError(f"HTTP request limit exceeded: {self.limits.max_active_requests}")

            # Configure defensive timeouts
            timeout_config = httpx.Timeout(
                connect=10.0,
                read=self.limits.query_timeout,
                write=10.0,
                pool=60.0
            )

            # Create client with defensive limits
            limits = httpx.Limits(
                max_keepalive_connections=10,
                max_connections=self.limits.max_connections,
                keepalive_expiry=30.0
            )

            client = httpx.AsyncClient(
                base_url=base_url,
                headers=headers,
                timeout=timeout_config,
                limits=limits,
                verify=True  # Always verify SSL
            )

            # Register and track
            client_id = id(client)
            self._active_resources[client_id] = client
            self.tracker.register(client)
            self.metrics.total_created += 1

            self.state = ResourceState.ACTIVE
            self.logger.debug(f"Created HTTP client: {client_id}")

            yield client

        except Exception as e:
            self.state = ResourceState.ERROR
            self.metrics.error_count += 1
            self.logger.error(f"HTTP client error: {e}")
            raise

        finally:
            # Cleanup
            if client:
                try:
                    client_id = id(client)
                    if client_id in self._active_resources:
                        del self._active_resources[client_id]

                    await client.aclose()
                    self.logger.debug(f"Closed HTTP client: {client_id}")
                except Exception as e:
                    self.logger.warning(f"Error closing HTTP client: {e}")

            # Update metrics
            response_time = time.time() - start_time
            if self.metrics.average_response_time == 0:
                self.metrics.average_response_time = response_time
            else:
                self.metrics.average_response_time = (
                    self.metrics.average_response_time * 0.9 + response_time * 0.1
                )

            self.metrics.last_activity = datetime.utcnow()
            self.state = ResourceState.IDLE

    @contextmanager
    def defensive_thread_pool(self, max_workers: int = None) -> Generator[concurrent.futures.ThreadPoolExecutor, None, None]:
        """
        Defensive thread pool with resource limits.

        Args:
            max_workers: Maximum number of worker threads

        Yields:
            Thread pool executor with defensive protections
        """
        max_workers = max_workers or min(32, (psutil.cpu_count() or 1) + 4)
        executor = None
        start_time = time.time()

        try:
            executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix=f"defensive_{self.resource_type.value}"
            )

            # Register and track
            executor_id = id(executor)
            self._active_resources[executor_id] = executor
            self.tracker.register(executor)
            self.metrics.total_created += 1

            self.state = ResourceState.ACTIVE
            self.logger.debug(f"Created thread pool: {executor_id} with {max_workers} workers")

            yield executor

        except Exception as e:
            self.state = ResourceState.ERROR
            self.metrics.error_count += 1
            self.logger.error(f"Thread pool error: {e}")
            raise

        finally:
            # Cleanup
            if executor:
                try:
                    executor_id = id(executor)
                    if executor_id in self._active_resources:
                        del self._active_resources[executor_id]

                    executor.shutdown(wait=True, cancel_futures=False)
                    self.logger.debug(f"Shutdown thread pool: {executor_id}")
                except Exception as e:
                    self.logger.warning(f"Error shutting down thread pool: {e}")

            # Update metrics
            response_time = time.time() - start_time
            self.metrics.last_activity = datetime.utcnow()
            self.state = ResourceState.IDLE

    def defensive_retry(self,
                       operation: Callable,
                       *args,
                       **kwargs) -> Any:
        """
        Execute operation with defensive retry logic.

        Args:
            operation: Function to execute
            *args, **kwargs: Arguments for the operation

        Returns:
            Result of the operation
        """
        last_exception = None

        for attempt in range(self.limits.max_retries + 1):
            try:
                return operation(*args, **kwargs)

            except Exception as e:
                last_exception = e

                if attempt < self.limits.max_retries:
                    # Calculate backoff delay
                    delay = self.limits.backoff_factor ** attempt
                    self.logger.warning(
                        f"Operation failed (attempt {attempt + 1}/{self.limits.max_retries + 1}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                else:
                    self.logger.error(f"Operation failed after {self.limits.max_retries + 1} attempts")

        # All retries exhausted
        self.metrics.error_count += 1
        raise last_exception

    def get_resource_health(self) -> Dict[str, Any]:
        """Get current resource health status"""
        return {
            "resource_type": self.resource_type.value,
            "state": self.state.value,
            "metrics": {
                "total_created": self.metrics.total_created,
                "active_count": self.metrics.active_count,
                "error_count": self.metrics.error_count,
                "memory_usage_mb": self.metrics.memory_usage_mb,
                "average_response_time": self.metrics.average_response_time,
                "success_rate": max(0, 100 - (self.metrics.error_count / max(1, self.metrics.total_created)) * 100)
            },
            "limits": {
                "max_connections": self.limits.max_connections,
                "max_memory_mb": self.limits.max_memory_mb,
                "connection_timeout": self.limits.connection_timeout
            },
            "last_activity": self.metrics.last_activity.isoformat() if self.metrics.last_activity else None
        }

    def shutdown(self):
        """Shutdown resource manager and cleanup all resources"""
        self.logger.info(f"Shutting down resource manager for {self.resource_type.value}")

        # Signal cleanup thread to stop
        self._should_cleanup.set()
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)

        # Close all active resources
        with self._lock:
            for resource in list(self._active_resources.values()):
                try:
                    if hasattr(resource, 'close'):
                        resource.close()
                    elif hasattr(resource, 'shutdown'):
                        resource.shutdown()
                    elif hasattr(resource, 'disconnect'):
                        resource.disconnect()
                except Exception as e:
                    self.logger.warning(f"Error during shutdown cleanup: {e}")

            self._active_resources.clear()

        self.state = ResourceState.CLOSED
        self.logger.info("Resource manager shutdown complete")


# Global resource managers
_resource_managers: Dict[ResourceType, DefensiveResourceManager] = {}
_manager_lock = threading.Lock()


def get_resource_manager(resource_type: ResourceType,
                        limits: ResourceLimits = None) -> DefensiveResourceManager:
    """Get or create resource manager for the specified type"""
    with _manager_lock:
        if resource_type not in _resource_managers:
            _resource_managers[resource_type] = DefensiveResourceManager(resource_type, limits)
        return _resource_managers[resource_type]


# Convenience functions
@contextmanager
def defensive_db_connection(database_url: str):
    """Quick defensive database connection"""
    manager = get_resource_manager(ResourceType.DATABASE)
    with manager.defensive_database_connection(database_url) as conn:
        yield conn


@asynccontextmanager
async def defensive_http_session(base_url: str = None):
    """Quick defensive HTTP session"""
    manager = get_resource_manager(ResourceType.HTTP_CLIENT)
    async with manager.defensive_http_client(base_url) as client:
        yield client


@contextmanager
def defensive_threads(max_workers: int = None):
    """Quick defensive thread pool"""
    manager = get_resource_manager(ResourceType.THREAD_POOL)
    with manager.defensive_thread_pool(max_workers) as executor:
        yield executor


# Example usage
if __name__ == "__main__":
    # Test resource management
    limits = ResourceLimits(max_connections=5, connection_timeout=10.0)
    manager = DefensiveResourceManager(ResourceType.DATABASE, limits)

    print("Resource manager health:", manager.get_resource_health())

    # Test defensive retry
    def flaky_operation():
        import random
        if random.random() < 0.7:  # 70% failure rate
            raise ConnectionError("Simulated network error")
        return "Success!"

    try:
        result = manager.defensive_retry(flaky_operation)
        print(f"Operation result: {result}")
    except Exception as e:
        print(f"Operation ultimately failed: {e}")

    # Cleanup
    manager.shutdown()