"""
Service Registry - Central service discovery and registration system.

This module provides a comprehensive service registry for managing service instances,
health checks, and service discovery in a distributed architecture.
"""

import asyncio
import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Callable, Any
import logging
import aiohttp
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class ServiceStatus(Enum):
    """Service status enumeration."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    STARTING = "starting"
    STOPPING = "stopping"
    UNKNOWN = "unknown"


@dataclass
class ServiceEndpoint:
    """Service endpoint information."""
    host: str
    port: int
    protocol: str = "http"
    path: str = "/"
    
    @property
    def url(self) -> str:
        """Get the full URL for this endpoint."""
        return f"{self.protocol}://{self.host}:{self.port}{self.path}"


@dataclass
class HealthCheck:
    """Health check configuration."""
    endpoint: str = "/health"
    interval_seconds: int = 30
    timeout_seconds: int = 5
    failure_threshold: int = 3
    success_threshold: int = 1
    enabled: bool = True


@dataclass
class ServiceInstance:
    """Service instance registration information."""
    service_name: str
    instance_id: str
    version: str
    endpoint: ServiceEndpoint
    metadata: Dict[str, Any]
    health_check: HealthCheck
    status: ServiceStatus = ServiceStatus.STARTING
    last_heartbeat: Optional[datetime] = None
    failure_count: int = 0
    registration_time: datetime = None
    
    def __post_init__(self):
        if self.registration_time is None:
            self.registration_time = datetime.utcnow()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        if self.last_heartbeat:
            data['last_heartbeat'] = self.last_heartbeat.isoformat()
        if self.registration_time:
            data['registration_time'] = self.registration_time.isoformat()
        return data


class ServiceRegistry(ABC):
    """Abstract service registry interface."""
    
    @abstractmethod
    async def register_service(self, instance: ServiceInstance) -> bool:
        """Register a service instance."""
        pass
    
    @abstractmethod
    async def deregister_service(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance."""
        pass
    
    @abstractmethod
    async def get_service_instances(self, service_name: str) -> List[ServiceInstance]:
        """Get all healthy instances of a service."""
        pass
    
    @abstractmethod
    async def get_all_services(self) -> Dict[str, List[ServiceInstance]]:
        """Get all registered services and their instances."""
        pass
    
    @abstractmethod
    async def update_health_status(self, service_name: str, instance_id: str, status: ServiceStatus) -> bool:
        """Update health status of a service instance."""
        pass
    
    @abstractmethod
    async def heartbeat(self, service_name: str, instance_id: str) -> bool:
        """Update heartbeat for a service instance."""
        pass


class InMemoryServiceRegistry(ServiceRegistry):
    """In-memory service registry implementation."""
    
    def __init__(self):
        self._services: Dict[str, Dict[str, ServiceInstance]] = {}
        self._lock = asyncio.Lock()
        self._health_check_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self):
        """Start the service registry and health checking."""
        self._running = True
        if not self._health_check_task or self._health_check_task.done():
            self._health_check_task = asyncio.create_task(self._health_check_loop())
        logger.info("Service registry started")
    
    async def stop(self):
        """Stop the service registry."""
        self._running = False
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        logger.info("Service registry stopped")
    
    async def register_service(self, instance: ServiceInstance) -> bool:
        """Register a service instance."""
        async with self._lock:
            if instance.service_name not in self._services:
                self._services[instance.service_name] = {}
            
            # Generate unique instance ID if not provided
            if not instance.instance_id:
                instance.instance_id = str(uuid.uuid4())
            
            self._services[instance.service_name][instance.instance_id] = instance
            instance.last_heartbeat = datetime.utcnow()
            
            logger.info(f"Registered service instance: {instance.service_name}:{instance.instance_id} at {instance.endpoint.url}")
            return True
    
    async def deregister_service(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance."""
        async with self._lock:
            if service_name in self._services and instance_id in self._services[service_name]:
                instance = self._services[service_name][instance_id]
                instance.status = ServiceStatus.STOPPING
                del self._services[service_name][instance_id]
                
                # Clean up empty service entries
                if not self._services[service_name]:
                    del self._services[service_name]
                
                logger.info(f"Deregistered service instance: {service_name}:{instance_id}")
                return True
            return False
    
    async def get_service_instances(self, service_name: str) -> List[ServiceInstance]:
        """Get all healthy instances of a service."""
        async with self._lock:
            if service_name not in self._services:
                return []
            
            healthy_instances = []
            for instance in self._services[service_name].values():
                if instance.status == ServiceStatus.HEALTHY:
                    healthy_instances.append(instance)
            
            return healthy_instances
    
    async def get_all_services(self) -> Dict[str, List[ServiceInstance]]:
        """Get all registered services and their instances."""
        async with self._lock:
            result = {}
            for service_name, instances in self._services.items():
                result[service_name] = list(instances.values())
            return result
    
    async def update_health_status(self, service_name: str, instance_id: str, status: ServiceStatus) -> bool:
        """Update health status of a service instance."""
        async with self._lock:
            if service_name in self._services and instance_id in self._services[service_name]:
                instance = self._services[service_name][instance_id]
                old_status = instance.status
                instance.status = status
                
                if status == ServiceStatus.HEALTHY:
                    instance.failure_count = 0
                elif status == ServiceStatus.UNHEALTHY:
                    instance.failure_count += 1
                
                if old_status != status:
                    logger.info(f"Service {service_name}:{instance_id} status changed: {old_status.value} -> {status.value}")
                
                return True
            return False
    
    async def heartbeat(self, service_name: str, instance_id: str) -> bool:
        """Update heartbeat for a service instance."""
        async with self._lock:
            if service_name in self._services and instance_id in self._services[service_name]:
                instance = self._services[service_name][instance_id]
                instance.last_heartbeat = datetime.utcnow()
                return True
            return False
    
    async def _health_check_loop(self):
        """Background health checking loop."""
        while self._running:
            try:
                await self._perform_health_checks()
                await asyncio.sleep(10)  # Check every 10 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(5)
    
    async def _perform_health_checks(self):
        """Perform health checks on all registered services."""
        async with self._lock:
            services_to_check = []
            for service_name, instances in self._services.items():
                for instance_id, instance in instances.items():
                    if instance.health_check.enabled:
                        services_to_check.append((service_name, instance_id, instance))
        
        # Perform health checks concurrently
        if services_to_check:
            tasks = [
                self._check_instance_health(service_name, instance_id, instance)
                for service_name, instance_id, instance in services_to_check
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _check_instance_health(self, service_name: str, instance_id: str, instance: ServiceInstance):
        """Check health of a specific service instance."""
        try:
            health_url = f"{instance.endpoint.url.rstrip('/')}{instance.health_check.endpoint}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    health_url,
                    timeout=aiohttp.ClientTimeout(total=instance.health_check.timeout_seconds)
                ) as response:
                    if response.status == 200:
                        await self.update_health_status(service_name, instance_id, ServiceStatus.HEALTHY)
                    else:
                        await self.update_health_status(service_name, instance_id, ServiceStatus.UNHEALTHY)
        
        except Exception as e:
            logger.warning(f"Health check failed for {service_name}:{instance_id}: {e}")
            await self.update_health_status(service_name, instance_id, ServiceStatus.UNHEALTHY)


class ServiceDiscoveryClient:
    """Client for service discovery operations."""
    
    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self._instance_cache: Dict[str, List[ServiceInstance]] = {}
        self._cache_ttl: Dict[str, datetime] = {}
        self._cache_duration = timedelta(seconds=30)
    
    async def discover_service(self, service_name: str, use_cache: bool = True) -> List[ServiceInstance]:
        """Discover instances of a service."""
        if use_cache and self._is_cache_valid(service_name):
            return self._instance_cache[service_name]
        
        instances = await self.registry.get_service_instances(service_name)
        
        # Update cache
        self._instance_cache[service_name] = instances
        self._cache_ttl[service_name] = datetime.utcnow() + self._cache_duration
        
        return instances
    
    async def get_service_endpoint(self, service_name: str, load_balancer: Callable[[List[ServiceInstance]], ServiceInstance] = None) -> Optional[ServiceEndpoint]:
        """Get a service endpoint using load balancing."""
        instances = await self.discover_service(service_name)
        
        if not instances:
            return None
        
        if load_balancer:
            selected_instance = load_balancer(instances)
        else:
            # Default: round-robin (simplified)
            selected_instance = instances[0]  # In real implementation, use proper round-robin
        
        return selected_instance.endpoint
    
    def _is_cache_valid(self, service_name: str) -> bool:
        """Check if cache entry is still valid."""
        if service_name not in self._cache_ttl:
            return False
        return datetime.utcnow() < self._cache_ttl[service_name]
    
    def clear_cache(self, service_name: str = None):
        """Clear service discovery cache."""
        if service_name:
            self._instance_cache.pop(service_name, None)
            self._cache_ttl.pop(service_name, None)
        else:
            self._instance_cache.clear()
            self._cache_ttl.clear()


@asynccontextmanager
async def service_registration_context(registry: ServiceRegistry, instance: ServiceInstance):
    """Context manager for automatic service registration/deregistration."""
    try:
        await registry.register_service(instance)
        yield instance
    finally:
        await registry.deregister_service(instance.service_name, instance.instance_id)


# Load balancing strategies
def round_robin_balancer(instances: List[ServiceInstance]) -> ServiceInstance:
    """Simple round-robin load balancer."""
    # In a real implementation, maintain state for proper round-robin
    return instances[0]


def random_balancer(instances: List[ServiceInstance]) -> ServiceInstance:
    """Random load balancer."""
    import random
    return random.choice(instances)


def least_connections_balancer(instances: List[ServiceInstance]) -> ServiceInstance:
    """Least connections load balancer (simplified)."""
    # In real implementation, track active connections
    return instances[0]


# Global service registry instance
_global_registry: Optional[ServiceRegistry] = None


def get_global_registry() -> ServiceRegistry:
    """Get or create global service registry instance."""
    global _global_registry
    if _global_registry is None:
        _global_registry = InMemoryServiceRegistry()
    return _global_registry


async def initialize_service_registry() -> ServiceRegistry:
    """Initialize and start the global service registry."""
    registry = get_global_registry()
    if isinstance(registry, InMemoryServiceRegistry):
        await registry.start()
    return registry


async def shutdown_service_registry():
    """Shutdown the global service registry."""
    global _global_registry
    if _global_registry and isinstance(_global_registry, InMemoryServiceRegistry):
        await _global_registry.stop()
        _global_registry = None