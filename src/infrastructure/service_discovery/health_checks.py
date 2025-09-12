"""
Health Check Framework - Comprehensive health checking for services.

This module provides a robust health checking framework with multiple check types,
dependencies, and detailed health reporting.
"""

import asyncio
import json
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Any, Callable, Union
import logging
import aiohttp
import psutil
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health check status enumeration."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


class HealthCheckType(Enum):
    """Health check type enumeration."""
    LIVENESS = "liveness"      # Service is alive
    READINESS = "readiness"    # Service is ready to handle requests
    STARTUP = "startup"        # Service is starting up
    DEPENDENCY = "dependency"  # External dependency check
    CUSTOM = "custom"          # Custom business logic check


@dataclass
class HealthCheckResult:
    """Result of a health check operation."""
    check_name: str
    check_type: HealthCheckType
    status: HealthStatus
    message: str
    details: Dict[str, Any]
    timestamp: datetime
    duration_ms: float
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['check_type'] = self.check_type.value
        data['status'] = self.status.value
        return data


class HealthCheck(ABC):
    """Abstract base class for health checks."""
    
    def __init__(self, name: str, check_type: HealthCheckType, timeout_seconds: float = 5.0):
        self.name = name
        self.check_type = check_type
        self.timeout_seconds = timeout_seconds
    
    @abstractmethod
    async def perform_check(self) -> HealthCheckResult:
        """Perform the health check and return the result."""
        pass
    
    async def check_with_timeout(self) -> HealthCheckResult:
        """Perform health check with timeout protection."""
        start_time = time.time()
        try:
            result = await asyncio.wait_for(
                self.perform_check(),
                timeout=self.timeout_seconds
            )
            result.duration_ms = (time.time() - start_time) * 1000
            return result
        except asyncio.TimeoutError:
            duration_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                check_name=self.name,
                check_type=self.check_type,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check timed out after {self.timeout_seconds}s",
                details={},
                timestamp=datetime.utcnow(),
                duration_ms=duration_ms,
                error="TimeoutError"
            )
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                check_name=self.name,
                check_type=self.check_type,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                details={'exception_type': type(e).__name__},
                timestamp=datetime.utcnow(),
                duration_ms=duration_ms,
                error=str(e)
            )


class DatabaseHealthCheck(HealthCheck):
    """Health check for database connectivity."""
    
    def __init__(self, name: str, connection_factory: Callable, query: str = "SELECT 1"):
        super().__init__(name, HealthCheckType.DEPENDENCY)
        self.connection_factory = connection_factory
        self.query = query
    
    async def perform_check(self) -> HealthCheckResult:
        """Check database connectivity and responsiveness."""
        try:
            # Get database connection
            conn = await self.connection_factory()
            
            # Execute test query
            start_query_time = time.time()
            result = await conn.fetch(self.query)
            query_duration_ms = (time.time() - start_query_time) * 1000
            
            # Close connection if needed
            if hasattr(conn, 'close'):
                await conn.close()
            
            return HealthCheckResult(
                check_name=self.name,
                check_type=self.check_type,
                status=HealthStatus.HEALTHY,
                message="Database connection successful",
                details={
                    'query': self.query,
                    'query_duration_ms': round(query_duration_ms, 2),
                    'result_count': len(result) if result else 0
                },
                timestamp=datetime.utcnow(),
                duration_ms=0  # Will be set by parent
            )
            
        except Exception as e:
            return HealthCheckResult(
                check_name=self.name,
                check_type=self.check_type,
                status=HealthStatus.UNHEALTHY,
                message=f"Database check failed: {str(e)}",
                details={
                    'query': self.query,
                    'error_type': type(e).__name__
                },
                timestamp=datetime.utcnow(),
                duration_ms=0,
                error=str(e)
            )


class HttpServiceHealthCheck(HealthCheck):
    """Health check for HTTP service dependencies."""
    
    def __init__(self, name: str, url: str, expected_status: int = 200, headers: Dict[str, str] = None):
        super().__init__(name, HealthCheckType.DEPENDENCY)
        self.url = url
        self.expected_status = expected_status
        self.headers = headers or {}
    
    async def perform_check(self) -> HealthCheckResult:
        """Check HTTP service availability."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.url,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout_seconds)
                ) as response:
                    status_healthy = response.status == self.expected_status
                    
                    return HealthCheckResult(
                        check_name=self.name,
                        check_type=self.check_type,
                        status=HealthStatus.HEALTHY if status_healthy else HealthStatus.UNHEALTHY,
                        message=f"HTTP service responded with status {response.status}",
                        details={
                            'url': self.url,
                            'status_code': response.status,
                            'expected_status': self.expected_status,
                            'response_headers': dict(response.headers)
                        },
                        timestamp=datetime.utcnow(),
                        duration_ms=0
                    )
                    
        except Exception as e:
            return HealthCheckResult(
                check_name=self.name,
                check_type=self.check_type,
                status=HealthStatus.UNHEALTHY,
                message=f"HTTP service check failed: {str(e)}",
                details={
                    'url': self.url,
                    'error_type': type(e).__name__
                },
                timestamp=datetime.utcnow(),
                duration_ms=0,
                error=str(e)
            )


class SystemResourceHealthCheck(HealthCheck):
    """Health check for system resource usage."""
    
    def __init__(self, name: str, cpu_threshold: float = 90.0, memory_threshold: float = 90.0, disk_threshold: float = 90.0):
        super().__init__(name, HealthCheckType.LIVENESS)
        self.cpu_threshold = cpu_threshold
        self.memory_threshold = memory_threshold
        self.disk_threshold = disk_threshold
    
    async def perform_check(self) -> HealthCheckResult:
        """Check system resource usage."""
        try:
            # Get system metrics
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # Check thresholds
            issues = []
            if cpu_percent > self.cpu_threshold:
                issues.append(f"CPU usage {cpu_percent:.1f}% exceeds threshold {self.cpu_threshold}%")
            
            if memory.percent > self.memory_threshold:
                issues.append(f"Memory usage {memory.percent:.1f}% exceeds threshold {self.memory_threshold}%")
            
            if disk.percent > self.disk_threshold:
                issues.append(f"Disk usage {disk.percent:.1f}% exceeds threshold {self.disk_threshold}%")
            
            status = HealthStatus.UNHEALTHY if issues else HealthStatus.HEALTHY
            message = "System resources within normal limits" if not issues else f"Resource issues: {'; '.join(issues)}"
            
            return HealthCheckResult(
                check_name=self.name,
                check_type=self.check_type,
                status=status,
                message=message,
                details={
                    'cpu_percent': round(cpu_percent, 1),
                    'memory_percent': round(memory.percent, 1),
                    'memory_available_gb': round(memory.available / (1024**3), 2),
                    'disk_percent': round(disk.percent, 1),
                    'disk_free_gb': round(disk.free / (1024**3), 2),
                    'thresholds': {
                        'cpu': self.cpu_threshold,
                        'memory': self.memory_threshold,
                        'disk': self.disk_threshold
                    }
                },
                timestamp=datetime.utcnow(),
                duration_ms=0
            )
            
        except Exception as e:
            return HealthCheckResult(
                check_name=self.name,
                check_type=self.check_type,
                status=HealthStatus.UNHEALTHY,
                message=f"System resource check failed: {str(e)}",
                details={'error_type': type(e).__name__},
                timestamp=datetime.utcnow(),
                duration_ms=0,
                error=str(e)
            )


class CustomHealthCheck(HealthCheck):
    """Custom health check for application-specific logic."""
    
    def __init__(self, name: str, check_function: Callable[[], Union[bool, Dict[str, Any]]], check_type: HealthCheckType = HealthCheckType.CUSTOM):
        super().__init__(name, check_type)
        self.check_function = check_function
    
    async def perform_check(self) -> HealthCheckResult:
        """Perform custom health check."""
        try:
            # Execute custom check function
            if asyncio.iscoroutinefunction(self.check_function):
                result = await self.check_function()
            else:
                result = self.check_function()
            
            # Handle different return types
            if isinstance(result, bool):
                status = HealthStatus.HEALTHY if result else HealthStatus.UNHEALTHY
                message = "Custom check passed" if result else "Custom check failed"
                details = {}
            elif isinstance(result, dict):
                status = result.get('status', HealthStatus.HEALTHY)
                message = result.get('message', 'Custom check completed')
                details = {k: v for k, v in result.items() if k not in ['status', 'message']}
                
                # Convert string status to enum if needed
                if isinstance(status, str):
                    status = HealthStatus(status.lower())
            else:
                status = HealthStatus.HEALTHY
                message = f"Custom check returned: {result}"
                details = {'result': str(result)}
            
            return HealthCheckResult(
                check_name=self.name,
                check_type=self.check_type,
                status=status,
                message=message,
                details=details,
                timestamp=datetime.utcnow(),
                duration_ms=0
            )
            
        except Exception as e:
            return HealthCheckResult(
                check_name=self.name,
                check_type=self.check_type,
                status=HealthStatus.UNHEALTHY,
                message=f"Custom check failed: {str(e)}",
                details={
                    'error_type': type(e).__name__,
                    'traceback': traceback.format_exc()
                },
                timestamp=datetime.utcnow(),
                duration_ms=0,
                error=str(e)
            )


@dataclass
class OverallHealth:
    """Overall health status of a service."""
    status: HealthStatus
    message: str
    timestamp: datetime
    checks: List[HealthCheckResult]
    summary: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'status': self.status.value,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'checks': [check.to_dict() for check in self.checks],
            'summary': self.summary
        }


class HealthCheckManager:
    """Manager for coordinating multiple health checks."""
    
    def __init__(self):
        self._health_checks: Dict[str, HealthCheck] = {}
        self._check_groups: Dict[str, List[str]] = {}
        self._dependencies: Dict[str, List[str]] = {}
    
    def add_health_check(self, health_check: HealthCheck) -> None:
        """Add a health check to the manager."""
        self._health_checks[health_check.name] = health_check
        logger.info(f"Added health check: {health_check.name} ({health_check.check_type.value})")
    
    def remove_health_check(self, name: str) -> None:
        """Remove a health check from the manager."""
        if name in self._health_checks:
            del self._health_checks[name]
            logger.info(f"Removed health check: {name}")
    
    def add_check_group(self, group_name: str, check_names: List[str]) -> None:
        """Group health checks together."""
        self._check_groups[group_name] = check_names
    
    def set_dependencies(self, check_name: str, dependencies: List[str]) -> None:
        """Set dependencies for a health check."""
        self._dependencies[check_name] = dependencies
    
    async def perform_all_checks(self) -> OverallHealth:
        """Perform all registered health checks."""
        results = []
        
        # Execute all health checks concurrently
        tasks = [
            health_check.check_with_timeout()
            for health_check in self._health_checks.values()
        ]
        
        if tasks:
            check_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in check_results:
                if isinstance(result, Exception):
                    # Handle unexpected exceptions
                    error_result = HealthCheckResult(
                        check_name="unknown",
                        check_type=HealthCheckType.CUSTOM,
                        status=HealthStatus.UNHEALTHY,
                        message=f"Health check failed with exception: {str(result)}",
                        details={'exception_type': type(result).__name__},
                        timestamp=datetime.utcnow(),
                        duration_ms=0,
                        error=str(result)
                    )
                    results.append(error_result)
                else:
                    results.append(result)
        
        return self._compute_overall_health(results)
    
    async def perform_check_group(self, group_name: str) -> OverallHealth:
        """Perform health checks for a specific group."""
        if group_name not in self._check_groups:
            return OverallHealth(
                status=HealthStatus.UNKNOWN,
                message=f"Unknown check group: {group_name}",
                timestamp=datetime.utcnow(),
                checks=[],
                summary={'error': 'Group not found'}
            )
        
        check_names = self._check_groups[group_name]
        tasks = []
        
        for check_name in check_names:
            if check_name in self._health_checks:
                tasks.append(self._health_checks[check_name].check_with_timeout())
        
        results = []
        if tasks:
            check_results = await asyncio.gather(*tasks, return_exceptions=True)
            results = [r for r in check_results if isinstance(r, HealthCheckResult)]
        
        return self._compute_overall_health(results)
    
    async def perform_single_check(self, check_name: str) -> Optional[HealthCheckResult]:
        """Perform a single health check."""
        if check_name not in self._health_checks:
            return None
        
        return await self._health_checks[check_name].check_with_timeout()
    
    def _compute_overall_health(self, results: List[HealthCheckResult]) -> OverallHealth:
        """Compute overall health status from individual check results."""
        if not results:
            return OverallHealth(
                status=HealthStatus.UNKNOWN,
                message="No health checks performed",
                timestamp=datetime.utcnow(),
                checks=[],
                summary={'total_checks': 0}
            )
        
        # Count status types
        status_counts = {status: 0 for status in HealthStatus}
        for result in results:
            status_counts[result.status] += 1
        
        # Determine overall status
        if status_counts[HealthStatus.UNHEALTHY] > 0:
            overall_status = HealthStatus.UNHEALTHY
            message = f"{status_counts[HealthStatus.UNHEALTHY]} of {len(results)} health checks failed"
        elif status_counts[HealthStatus.DEGRADED] > 0:
            overall_status = HealthStatus.DEGRADED
            message = f"{status_counts[HealthStatus.DEGRADED]} of {len(results)} health checks degraded"
        elif status_counts[HealthStatus.UNKNOWN] > 0:
            overall_status = HealthStatus.UNKNOWN
            message = f"{status_counts[HealthStatus.UNKNOWN]} of {len(results)} health checks unknown"
        else:
            overall_status = HealthStatus.HEALTHY
            message = f"All {len(results)} health checks passed"
        
        # Create summary
        summary = {
            'total_checks': len(results),
            'healthy': status_counts[HealthStatus.HEALTHY],
            'unhealthy': status_counts[HealthStatus.UNHEALTHY],
            'degraded': status_counts[HealthStatus.DEGRADED],
            'unknown': status_counts[HealthStatus.UNKNOWN],
            'avg_duration_ms': round(sum(r.duration_ms for r in results) / len(results), 2) if results else 0,
            'check_types': {}
        }
        
        # Summary by check type
        for check_type in HealthCheckType:
            type_results = [r for r in results if r.check_type == check_type]
            if type_results:
                summary['check_types'][check_type.value] = {
                    'count': len(type_results),
                    'healthy': sum(1 for r in type_results if r.status == HealthStatus.HEALTHY),
                    'unhealthy': sum(1 for r in type_results if r.status == HealthStatus.UNHEALTHY)
                }
        
        return OverallHealth(
            status=overall_status,
            message=message,
            timestamp=datetime.utcnow(),
            checks=results,
            summary=summary
        )
    
    def get_registered_checks(self) -> Dict[str, str]:
        """Get list of registered health checks."""
        return {name: check.check_type.value for name, check in self._health_checks.items()}


# Global health check manager instance
_global_health_manager: Optional[HealthCheckManager] = None


def get_health_manager() -> HealthCheckManager:
    """Get or create global health check manager."""
    global _global_health_manager
    if _global_health_manager is None:
        _global_health_manager = HealthCheckManager()
    return _global_health_manager