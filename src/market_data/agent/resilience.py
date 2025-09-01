"""
Resilience patterns for the data agent.

This module provides circuit breaker and retry logic to enhance
the data agent's resilience against temporary failures and prevent
cascading failures when data sources are unavailable.
"""

import logging
import time
import asyncio
import random
from datetime import datetime
from typing import Dict, Optional, TypeVar, Union, List
from functools import wraps

logger = logging.getLogger(__name__)

# Type variable for generic function return type
T = TypeVar('T')

class CircuitBreakerError(Exception):
    """Exception raised when a circuit breaker is open."""

class CircuitBreaker:
    """
    Circuit breaker implementation to prevent cascading failures.
    
    The circuit breaker has three states:
    - CLOSED: All requests are allowed through
    - OPEN: All requests are blocked
    - HALF_OPEN: A limited number of requests are allowed through to test if the service is back
    """
    
    # Circuit breaker states
    CLOSED = 'CLOSED'
    OPEN = 'OPEN'
    HALF_OPEN = 'HALF_OPEN'
    
    def __init__(
        self, 
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 30,
        half_open_max_calls: int = 3
    ):
        """
        Initialize the circuit breaker.
        
        Args:
            name: Name of the circuit breaker (for logging)
            failure_threshold: Number of failures before opening the circuit
            recovery_timeout: Time in seconds before trying to recover (half-open)
            half_open_max_calls: Maximum number of calls allowed in half-open state
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        # State
        self.state = self.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.half_open_calls = 0
        self.half_open_successes = 0
        
    def record_success(self):
        """Record a successful call."""
        if self.state == self.HALF_OPEN:
            self.half_open_successes += 1
            if self.half_open_successes >= self.half_open_max_calls:
                logger.info(f"Circuit breaker '{self.name}' closing after successful recovery")
                self.state = self.CLOSED
                self.failure_count = 0
                self.half_open_calls = 0
                self.half_open_successes = 0
        elif self.state == self.CLOSED:
            # Reset failure count on success in closed state
            self.failure_count = 0
    
    def record_failure(self):
        """Record a failed call."""
        self.last_failure_time = datetime.now()
        
        if self.state == self.CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                logger.warning(f"Circuit breaker '{self.name}' opening after {self.failure_count} failures")
                self.state = self.OPEN
        elif self.state == self.HALF_OPEN:
            logger.warning(f"Circuit breaker '{self.name}' opening after failure in half-open state")
            self.state = self.OPEN
            self.half_open_calls = 0
            self.half_open_successes = 0
    
    def allow_request(self) -> bool:
        """
        Check if a request is allowed through the circuit breaker.
        
        Returns:
            True if the request is allowed, False otherwise
        """
        if self.state == self.CLOSED:
            return True
        elif self.state == self.OPEN:
            # Check if recovery timeout has elapsed
            if self.last_failure_time and \
               (datetime.now() - self.last_failure_time).total_seconds() >= self.recovery_timeout:
                logger.info(f"Circuit breaker '{self.name}' entering half-open state")
                self.state = self.HALF_OPEN
                self.half_open_calls = 0
                self.half_open_successes = 0
                return True
            return False
        elif self.state == self.HALF_OPEN:
            # Allow a limited number of requests in half-open state
            if self.half_open_calls < self.half_open_max_calls:
                self.half_open_calls += 1
                return True
            return False
        
        return False  # Default deny
    
    def __str__(self) -> str:
        return f"CircuitBreaker(name={self.name}, state={self.state}, failures={self.failure_count})"


class RetryWithBackoff:
    """
    Retry logic with exponential backoff.
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
        backoff_multiplier: float = 2.0,
        jitter: float = 0.1
    ):
        """
        Initialize the retry logic.
        
        Args:
            max_retries: Maximum number of retries
            initial_backoff: Initial backoff time in seconds
            max_backoff: Maximum backoff time in seconds
            backoff_multiplier: Multiplier for exponential backoff
            jitter: Random jitter factor (0-1) to add to backoff
        """
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        self.jitter = jitter
    
    def calculate_backoff(self, retry_count: int) -> float:
        """
        Calculate the backoff time for a retry.
        
        Args:
            retry_count: Current retry count
            
        Returns:
            Backoff time in seconds
        """
        backoff = min(
            self.max_backoff,
            self.initial_backoff * (self.backoff_multiplier ** retry_count)
        )
        
        # Add jitter
        if self.jitter > 0:
            jitter_amount = backoff * self.jitter
            backoff = backoff + random.uniform(-jitter_amount, jitter_amount)
            
        return max(0, backoff)  # Ensure non-negative


# Circuit breaker registry
_circuit_breakers: Dict[str, CircuitBreaker] = {}

def get_circuit_breaker(name: str) -> CircuitBreaker:
    """
    Get or create a circuit breaker by name.
    
    Args:
        name: Name of the circuit breaker
        
    Returns:
        CircuitBreaker instance
    """
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(name)
    return _circuit_breakers[name]


def with_circuit_breaker(circuit_breaker_name: str):
    """
    Decorator to apply circuit breaker pattern to a function.
    
    Args:
        circuit_breaker_name: Name of the circuit breaker to use
        
    Returns:
        Decorated function
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            circuit_breaker = get_circuit_breaker(circuit_breaker_name)
            
            if not circuit_breaker.allow_request():
                logger.warning(f"Circuit breaker '{circuit_breaker_name}' is open, request blocked")
                raise CircuitBreakerError(f"Circuit breaker '{circuit_breaker_name}' is open")
            
            try:
                result = await func(*args, **kwargs)
                circuit_breaker.record_success()
                return result
            except Exception:
                circuit_breaker.record_failure()
                raise
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            circuit_breaker = get_circuit_breaker(circuit_breaker_name)
            
            if not circuit_breaker.allow_request():
                logger.warning(f"Circuit breaker '{circuit_breaker_name}' is open, request blocked")
                raise CircuitBreakerError(f"Circuit breaker '{circuit_breaker_name}' is open")
            
            try:
                result = func(*args, **kwargs)
                circuit_breaker.record_success()
                return result
            except Exception:
                circuit_breaker.record_failure()
                raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator


def with_retry(
    max_retries: int = 3,
    retry_exceptions: Union[List[Exception], Exception] = Exception,
    initial_backoff: float = 1.0,
    max_backoff: float = 60.0,
    backoff_multiplier: float = 2.0,
    jitter: float = 0.1
):
    """
    Decorator to apply retry logic with exponential backoff to a function.
    
    Args:
        max_retries: Maximum number of retries
        retry_exceptions: Exception or list of exceptions to retry on
        initial_backoff: Initial backoff time in seconds
        max_backoff: Maximum backoff time in seconds
        backoff_multiplier: Multiplier for exponential backoff
        jitter: Random jitter factor (0-1) to add to backoff
        
    Returns:
        Decorated function
    """
    retry_logic = RetryWithBackoff(
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        max_backoff=max_backoff,
        backoff_multiplier=backoff_multiplier,
        jitter=jitter
    )
    
    # Convert single exception to list
    if not isinstance(retry_exceptions, list):
        retry_exceptions = [retry_exceptions]
    
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            retries = 0
            last_exception = None
            
            while retries <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except tuple(retry_exceptions) as e:
                    last_exception = e
                    retries += 1
                    
                    if retries > max_retries:
                        logger.warning(f"Max retries ({max_retries}) exceeded for {func.__name__}")
                        break
                    
                    backoff = retry_logic.calculate_backoff(retries)
                    logger.info(f"Retry {retries}/{max_retries} for {func.__name__} after {backoff:.2f}s: {str(e)}")
                    await asyncio.sleep(backoff)
                except Exception:
                    # Don't retry on exceptions not in retry_exceptions
                    raise
            
            # If we get here, all retries failed
            if last_exception:
                raise last_exception
            raise RuntimeError(f"All retries failed for {func.__name__}")
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            retries = 0
            last_exception = None
            
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except tuple(retry_exceptions) as e:
                    last_exception = e
                    retries += 1
                    
                    if retries > max_retries:
                        logger.warning(f"Max retries ({max_retries}) exceeded for {func.__name__}")
                        break
                    
                    backoff = retry_logic.calculate_backoff(retries)
                    logger.info(f"Retry {retries}/{max_retries} for {func.__name__} after {backoff:.2f}s: {str(e)}")
                    time.sleep(backoff)
                except Exception:
                    # Don't retry on exceptions not in retry_exceptions
                    raise
            
            # If we get here, all retries failed
            if last_exception:
                raise last_exception
            raise RuntimeError(f"All retries failed for {func.__name__}")
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator


def with_resilience(
    circuit_breaker_name: Optional[str] = None,
    max_retries: int = 3,
    retry_exceptions: Union[List[Exception], Exception] = Exception,
    initial_backoff: float = 1.0,
    max_backoff: float = 60.0
):
    """
    Combined decorator for both circuit breaker and retry logic.
    
    Args:
        circuit_breaker_name: Name of the circuit breaker to use (None to disable)
        max_retries: Maximum number of retries (0 to disable)
        retry_exceptions: Exception or list of exceptions to retry on
        initial_backoff: Initial backoff time in seconds
        max_backoff: Maximum backoff time in seconds
        
    Returns:
        Decorated function
    """
    def decorator(func):
        # Apply decorators in reverse order (retry first, then circuit breaker)
        decorated = func
        
        if max_retries > 0:
            decorated = with_retry(
                max_retries=max_retries,
                retry_exceptions=retry_exceptions,
                initial_backoff=initial_backoff,
                max_backoff=max_backoff
            )(decorated)
            
        if circuit_breaker_name:
            decorated = with_circuit_breaker(circuit_breaker_name)(decorated)
            
        return decorated
            
    return decorator
