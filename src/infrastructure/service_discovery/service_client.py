"""
Service Client - HTTP client with service discovery and resilience patterns.

This module provides a robust HTTP client that integrates with service discovery,
implements circuit breaker pattern, retry logic, and load balancing.
"""

import asyncio
import json
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable, Union
import logging
import aiohttp
from contextlib import asynccontextmanager

from .service_registry import ServiceRegistry, ServiceInstance, ServiceEndpoint, get_global_registry

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker state enumeration."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, requests fail fast
    HALF_OPEN = "half_open"  # Testing if service has recovered


@dataclass
class RetryConfig:
    """Configuration for retry logic."""
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    exponential_backoff: bool = True
    jitter: bool = True
    retryable_status_codes: List[int] = field(default_factory=lambda: [429, 502, 503, 504])


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    recovery_timeout_seconds: float = 60.0
    half_open_max_calls: int = 3


@dataclass
class RequestStats:
    """Statistics for service requests."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_response_time_ms: float = 0.0
    last_request_time: Optional[datetime] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100.0
    
    @property
    def average_response_time_ms(self) -> float:
        """Calculate average response time."""
        if self.successful_requests == 0:
            return 0.0
        return self.total_response_time_ms / self.successful_requests


class CircuitBreaker:
    """Circuit breaker implementation for service resilience."""
    
    def __init__(self, name: str, config: CircuitBreakerConfig = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_calls = 0
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if self.state == CircuitState.OPEN:
            if self._should_attempt_recovery():
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                logger.info(f"Circuit breaker {self.name} moving to HALF_OPEN state")
            else:
                raise CircuitBreakerError(f"Circuit breaker {self.name} is OPEN")
        
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls >= self.config.half_open_max_calls:
                raise CircuitBreakerError(f"Circuit breaker {self.name} max half-open calls exceeded")
            self.half_open_calls += 1
        
        try:
            # Execute the function
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # Success - reset circuit breaker
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure()
            raise e
    
    def _should_attempt_recovery(self) -> bool:
        """Check if circuit breaker should attempt recovery."""
        if self.last_failure_time is None:
            return True
        
        time_since_failure = datetime.utcnow() - self.last_failure_time
        return time_since_failure.total_seconds() >= self.config.recovery_timeout_seconds
    
    def _on_success(self):
        """Handle successful request."""
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            logger.info(f"Circuit breaker {self.name} recovered to CLOSED state")
        
        self.failure_count = 0
        self.half_open_calls = 0
    
    def _on_failure(self):
        """Handle failed request."""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker {self.name} failed during recovery, moving to OPEN state")
        elif self.failure_count >= self.config.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker {self.name} opened due to {self.failure_count} failures")


class CircuitBreakerError(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class LoadBalancer(ABC):
    """Abstract load balancer interface."""
    
    @abstractmethod
    def select_instance(self, instances: List[ServiceInstance]) -> Optional[ServiceInstance]:
        """Select an instance from the available instances."""
        pass


class RoundRobinBalancer(LoadBalancer):
    """Round-robin load balancer."""
    
    def __init__(self):
        self._current_index = 0
    
    def select_instance(self, instances: List[ServiceInstance]) -> Optional[ServiceInstance]:
        """Select instance using round-robin strategy."""
        if not instances:
            return None
        
        instance = instances[self._current_index % len(instances)]
        self._current_index = (self._current_index + 1) % len(instances)
        return instance


class RandomBalancer(LoadBalancer):
    """Random load balancer."""
    
    def select_instance(self, instances: List[ServiceInstance]) -> Optional[ServiceInstance]:
        """Select instance randomly."""
        if not instances:
            return None
        return random.choice(instances)


class WeightedBalancer(LoadBalancer):
    """Weighted load balancer based on instance metadata."""
    
    def select_instance(self, instances: List[ServiceInstance]) -> Optional[ServiceInstance]:
        """Select instance based on weights."""
        if not instances:
            return None
        
        # Get weights from instance metadata, default to 1
        weights = []
        for instance in instances:
            weight = instance.metadata.get('weight', 1)
            weights.append(max(weight, 1))  # Ensure positive weight
        
        # Weighted random selection
        total_weight = sum(weights)
        random_value = random.uniform(0, total_weight)
        
        cumulative_weight = 0
        for i, weight in enumerate(weights):
            cumulative_weight += weight
            if random_value <= cumulative_weight:
                return instances[i]
        
        # Fallback to last instance
        return instances[-1]


class ServiceClient:
    """HTTP client with service discovery and resilience patterns."""
    
    def __init__(
        self,
        service_name: str,
        registry: ServiceRegistry = None,
        load_balancer: LoadBalancer = None,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None,
        timeout_seconds: float = 30.0
    ):
        self.service_name = service_name
        self.registry = registry or get_global_registry()
        self.load_balancer = load_balancer or RoundRobinBalancer()
        self.retry_config = retry_config or RetryConfig()
        self.timeout_seconds = timeout_seconds
        
        # Circuit breaker for the service
        self.circuit_breaker = CircuitBreaker(
            name=f"{service_name}_circuit_breaker",
            config=circuit_breaker_config or CircuitBreakerConfig()
        )
        
        # Statistics tracking
        self.stats = RequestStats()
        
        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def _ensure_session(self):
        """Ensure HTTP session is available."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)
    
    async def close(self):
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def get(self, path: str, params: Dict[str, Any] = None, headers: Dict[str, str] = None) -> aiohttp.ClientResponse:
        """Perform GET request with service discovery."""
        return await self._make_request('GET', path, params=params, headers=headers)
    
    async def post(self, path: str, json_data: Dict[str, Any] = None, data: Any = None, headers: Dict[str, str] = None) -> aiohttp.ClientResponse:
        """Perform POST request with service discovery."""
        return await self._make_request('POST', path, json_data=json_data, data=data, headers=headers)
    
    async def put(self, path: str, json_data: Dict[str, Any] = None, data: Any = None, headers: Dict[str, str] = None) -> aiohttp.ClientResponse:
        """Perform PUT request with service discovery."""
        return await self._make_request('PUT', path, json_data=json_data, data=data, headers=headers)
    
    async def delete(self, path: str, headers: Dict[str, str] = None) -> aiohttp.ClientResponse:
        """Perform DELETE request with service discovery."""
        return await self._make_request('DELETE', path, headers=headers)
    
    async def _make_request(
        self,
        method: str,
        path: str,
        params: Dict[str, Any] = None,
        json_data: Dict[str, Any] = None,
        data: Any = None,
        headers: Dict[str, str] = None
    ) -> aiohttp.ClientResponse:
        """Make HTTP request with retry and circuit breaker."""
        await self._ensure_session()
        
        return await self.circuit_breaker.call(
            self._make_request_with_retry,
            method, path, params, json_data, data, headers
        )
    
    async def _make_request_with_retry(
        self,
        method: str,
        path: str,
        params: Dict[str, Any] = None,
        json_data: Dict[str, Any] = None,
        data: Any = None,
        headers: Dict[str, str] = None
    ) -> aiohttp.ClientResponse:
        """Make HTTP request with retry logic."""
        last_exception = None
        
        for attempt in range(self.retry_config.max_attempts):
            try:
                # Discover service instance
                endpoint = await self._discover_endpoint()
                if not endpoint:
                    raise ServiceDiscoveryError(f"No healthy instances found for service: {self.service_name}")
                
                # Build URL
                url = f"{endpoint.url.rstrip('/')}{path}"
                
                # Prepare request parameters
                request_kwargs = {
                    'method': method,
                    'url': url,
                    'params': params,
                    'headers': headers
                }
                
                if json_data is not None:
                    request_kwargs['json'] = json_data
                elif data is not None:
                    request_kwargs['data'] = data
                
                # Make request
                start_time = time.time()
                response = await self._session.request(**request_kwargs)
                response_time_ms = (time.time() - start_time) * 1000
                
                # Update statistics
                self._update_stats(success=True, response_time_ms=response_time_ms)
                
                # Check if response should trigger retry
                if response.status in self.retry_config.retryable_status_codes:
                    if attempt < self.retry_config.max_attempts - 1:
                        await self._wait_for_retry(attempt)
                        continue
                
                return response
                
            except Exception as e:
                last_exception = e
                logger.warning(f"Request attempt {attempt + 1} failed for {self.service_name}: {str(e)}")
                
                # Update statistics
                self._update_stats(success=False)
                
                if attempt < self.retry_config.max_attempts - 1:
                    await self._wait_for_retry(attempt)
                else:
                    break
        
        # All retry attempts failed
        raise last_exception or Exception("All retry attempts failed")
    
    async def _discover_endpoint(self) -> Optional[ServiceEndpoint]:
        """Discover service endpoint using service registry."""
        try:
            instances = await self.registry.get_service_instances(self.service_name)
            if not instances:
                return None
            
            selected_instance = self.load_balancer.select_instance(instances)
            return selected_instance.endpoint if selected_instance else None
            
        except Exception as e:
            logger.error(f"Service discovery failed for {self.service_name}: {str(e)}")
            return None
    
    async def _wait_for_retry(self, attempt: int):
        """Wait before retry attempt."""
        if self.retry_config.exponential_backoff:
            delay = min(
                self.retry_config.base_delay_seconds * (2 ** attempt),
                self.retry_config.max_delay_seconds
            )
        else:
            delay = self.retry_config.base_delay_seconds
        
        # Add jitter to avoid thundering herd
        if self.retry_config.jitter:
            delay = delay * (0.5 + random.random() * 0.5)
        
        await asyncio.sleep(delay)
    
    def _update_stats(self, success: bool, response_time_ms: float = 0.0):
        """Update request statistics."""
        self.stats.total_requests += 1
        self.stats.last_request_time = datetime.utcnow()
        
        if success:
            self.stats.successful_requests += 1
            self.stats.total_response_time_ms += response_time_ms
        else:
            self.stats.failed_requests += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        return {
            'service_name': self.service_name,
            'total_requests': self.stats.total_requests,
            'successful_requests': self.stats.successful_requests,
            'failed_requests': self.stats.failed_requests,
            'success_rate': round(self.stats.success_rate, 2),
            'average_response_time_ms': round(self.stats.average_response_time_ms, 2),
            'last_request_time': self.stats.last_request_time.isoformat() if self.stats.last_request_time else None,
            'circuit_breaker_state': self.circuit_breaker.state.value,
            'circuit_breaker_failure_count': self.circuit_breaker.failure_count
        }


class ServiceDiscoveryError(Exception):
    """Exception raised when service discovery fails."""
    pass


@asynccontextmanager
async def service_client(service_name: str, **kwargs):
    """Context manager for service client."""
    client = ServiceClient(service_name, **kwargs)
    try:
        async with client:
            yield client
    finally:
        await client.close()


# Convenience functions for quick service calls
async def call_service(service_name: str, method: str, path: str, **kwargs) -> aiohttp.ClientResponse:
    """Make a single service call with automatic client management."""
    async with service_client(service_name) as client:
        if method.upper() == 'GET':
            return await client.get(path, **kwargs)
        elif method.upper() == 'POST':
            return await client.post(path, **kwargs)
        elif method.upper() == 'PUT':
            return await client.put(path, **kwargs)
        elif method.upper() == 'DELETE':
            return await client.delete(path, **kwargs)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")


async def get_service_json(service_name: str, path: str, **kwargs) -> Dict[str, Any]:
    """Get JSON response from service."""
    async with service_client(service_name) as client:
        response = await client.get(path, **kwargs)
        response.raise_for_status()
        return await response.json()


async def post_service_json(service_name: str, path: str, json_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """Post JSON data to service and get JSON response."""
    async with service_client(service_name) as client:
        response = await client.post(path, json_data=json_data, **kwargs)
        response.raise_for_status()
        return await response.json()