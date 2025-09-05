#!/usr/bin/env python3
"""
Secure Error Handling and Logging for Financial Systems

Implements defensive error handling with:
- Sanitized error messages (no sensitive data exposure)
- Comprehensive audit logging with security controls
- Circuit breaker patterns for external services
- Rate limiting for error reporting
- Secure exception chaining
- PII (Personally Identifiable Information) scrubbing

This module follows security-first principles for financial error handling.
"""

import hashlib
import json
import logging
import re
import time
import traceback
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Any, Dict, List, Optional, Type, Union, Callable
from dataclasses import dataclass, field
import threading


class ErrorSeverity(Enum):
    """Error severity levels for financial systems"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    SECURITY = "security"  # Special level for security incidents


class ErrorCategory(Enum):
    """Categories of errors in financial systems"""
    VALIDATION = "validation"
    DATABASE = "database"
    NETWORK = "network"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_INTEGRITY = "data_integrity"
    EXTERNAL_API = "external_api"
    BUSINESS_LOGIC = "business_logic"
    SYSTEM = "system"
    SECURITY = "security"


@dataclass
class SecureError:
    """Secure error representation with sanitized data"""
    error_id: str
    category: ErrorCategory
    severity: ErrorSeverity
    message: str  # Sanitized, safe for display
    technical_details: str  # Full details for logging only
    timestamp: datetime = field(default_factory=datetime.utcnow)
    user_id_hash: Optional[str] = None  # Hashed user ID for privacy
    request_id: Optional[str] = None
    stack_trace_hash: Optional[str] = None  # Hash of stack trace for tracking
    metadata: Dict[str, Any] = field(default_factory=dict)


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"         # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreaker:
    """Circuit breaker for external service calls"""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    failure_count: int = 0
    last_failure_time: Optional[float] = None
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    
    def should_allow_request(self) -> bool:
        """Check if request should be allowed"""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def record_success(self):
        """Record successful operation"""
        self.failure_count = 0
        self.state = CircuitBreakerState.CLOSED
    
    def record_failure(self):
        """Record failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self.last_failure_time is None:
            return True
        return (time.time() - self.last_failure_time) >= self.recovery_timeout


class RateLimiter:
    """Rate limiter for error reporting to prevent spam"""
    def __init__(self, max_errors: int = 100, time_window: int = 3600):
        self.max_errors = max_errors
        self.time_window = time_window
        self.error_timestamps = []
        self._lock = threading.Lock()
    
    def should_allow_error(self) -> bool:
        """Check if error should be logged (not rate limited)"""
        with self._lock:
            now = time.time()
            # Remove old timestamps
            self.error_timestamps = [
                ts for ts in self.error_timestamps 
                if now - ts < self.time_window
            ]
            
            if len(self.error_timestamps) < self.max_errors:
                self.error_timestamps.append(now)
                return True
            return False


class SecureErrorHandler:
    """
    Defensive error handler for financial systems with security controls.
    
    Key features:
    - Sanitizes all error messages to prevent information disclosure
    - Implements rate limiting to prevent error spam
    - Uses circuit breakers for external service errors
    - Provides secure audit logging with PII protection
    - Tracks error patterns for security analysis
    """
    
    # Patterns to scrub from error messages (PII and sensitive data)
    SENSITIVE_PATTERNS = [
        (r'\b\d{16}\b', '[CARD_NUMBER]'),  # Credit card numbers
        (r'\b\d{3}-?\d{2}-?\d{4}\b', '[SSN]'),  # Social Security Numbers
        (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]'),  # Email addresses
        (r'\bpassword[=:]\s*[^\s]+', 'password=[REDACTED]'),  # Passwords
        (r'\bapi[_-]?key[=:]\s*[^\s]+', 'api_key=[REDACTED]'),  # API keys
        (r'\btoken[=:]\s*[^\s]+', 'token=[REDACTED]'),  # Tokens
        (r'\b(?:\d{4}[-\s]?){3}\d{4}\b', '[PAYMENT_CARD]'),  # Payment card patterns
    ]
    
    def __init__(self, service_name: str = "ats_financial"):
        self.service_name = service_name
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.rate_limiter = RateLimiter()
        self.error_cache: Dict[str, int] = {}  # Track repeated errors
        
        # Setup secure logging
        self.logger = logging.getLogger(f"{__name__}.{service_name}")
        self.audit_logger = logging.getLogger(f"{__name__}.{service_name}.audit")
        self.security_logger = logging.getLogger(f"{__name__}.{service_name}.security")
        
        self._setup_secure_logging()
    
    def _setup_secure_logging(self):
        """Setup secure logging with proper formatters"""
        # Main error logger
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.WARNING)
        
        # Audit logger for compliance
        if not self.audit_logger.handlers:
            audit_formatter = logging.Formatter(
                '%(asctime)s - AUDIT - %(levelname)s - %(message)s'
            )
            audit_handler = logging.StreamHandler()
            audit_handler.setFormatter(audit_formatter)
            self.audit_logger.addHandler(audit_handler)
            self.audit_logger.setLevel(logging.INFO)
        
        # Security logger for security incidents
        if not self.security_logger.handlers:
            security_formatter = logging.Formatter(
                '%(asctime)s - SECURITY_ALERT - %(levelname)s - %(message)s'
            )
            security_handler = logging.StreamHandler()
            security_handler.setFormatter(security_formatter)
            self.security_logger.addHandler(security_handler)
            self.security_logger.setLevel(logging.WARNING)
    
    def _sanitize_message(self, message: str) -> str:
        """Sanitize error message to remove sensitive information"""
        if not message:
            return "Unknown error"
        
        sanitized = str(message)
        
        # Apply PII scrubbing patterns
        for pattern, replacement in self.SENSITIVE_PATTERNS:
            sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)
        
        # Remove potential file paths that might contain usernames
        sanitized = re.sub(r'/(?:home|Users)/[^/\s]+', '/[USER]', sanitized)
        
        # Remove potential IP addresses (keep only first two octets)
        sanitized = re.sub(r'\b(\d{1,3}\.\d{1,3})\.\d{1,3}\.\d{1,3}\b', r'\1.XXX.XXX', sanitized)
        
        # Truncate if too long
        if len(sanitized) > 500:
            sanitized = sanitized[:497] + "..."
        
        return sanitized
    
    def _generate_error_id(self, error: Exception, context: str = "") -> str:
        """Generate unique error ID for tracking"""
        error_str = f"{type(error).__name__}:{str(error)}:{context}:{int(time.time() // 300)}"  # 5-minute window
        return hashlib.sha256(error_str.encode()).hexdigest()[:12]
    
    def _hash_user_id(self, user_id: str) -> str:
        """Hash user ID for privacy-safe logging"""
        if not user_id:
            return None
        return hashlib.sha256(f"user_salt_{user_id}".encode()).hexdigest()[:16]
    
    def _create_secure_error(self, 
                           error: Exception, 
                           category: ErrorCategory,
                           severity: ErrorSeverity,
                           context: str = "",
                           user_id: str = None,
                           request_id: str = None) -> SecureError:
        """Create secure error object with sanitized data"""
        error_id = self._generate_error_id(error, context)
        sanitized_message = self._sanitize_message(str(error))
        
        # Create stack trace hash for tracking (don't log full trace)
        stack_trace = traceback.format_exc()
        stack_trace_hash = hashlib.sha256(stack_trace.encode()).hexdigest()[:16]
        
        return SecureError(
            error_id=error_id,
            category=category,
            severity=severity,
            message=sanitized_message,
            technical_details=f"{type(error).__name__}: {str(error)[:200]}",
            user_id_hash=self._hash_user_id(user_id) if user_id else None,
            request_id=request_id,
            stack_trace_hash=stack_trace_hash,
            metadata={"context": context, "error_type": type(error).__name__}
        )
    
    def handle_error(self, 
                    error: Exception,
                    category: ErrorCategory,
                    severity: ErrorSeverity = ErrorSeverity.MEDIUM,
                    context: str = "",
                    user_id: str = None,
                    request_id: str = None,
                    should_raise: bool = True) -> SecureError:
        """
        Handle error with comprehensive security and logging.
        
        Args:
            error: The exception to handle
            category: Category of the error
            severity: Severity level
            context: Additional context information
            user_id: User ID (will be hashed for privacy)
            request_id: Request ID for tracing
            should_raise: Whether to re-raise the exception
            
        Returns:
            SecureError object with sanitized information
        """
        secure_error = self._create_secure_error(
            error, category, severity, context, user_id, request_id
        )
        
        # Rate limiting check
        if not self.rate_limiter.should_allow_error():
            # Still create the error but don't log it
            if should_raise:
                raise error
            return secure_error
        
        # Track repeated errors
        self.error_cache[secure_error.error_id] = self.error_cache.get(secure_error.error_id, 0) + 1
        
        # Log based on severity
        if severity == ErrorSeverity.SECURITY:
            self.security_logger.critical(
                f"SECURITY_INCIDENT: {secure_error.error_id} - {secure_error.message}"
            )
        elif severity == ErrorSeverity.CRITICAL:
            self.logger.critical(
                f"CRITICAL_ERROR: {secure_error.error_id} - {secure_error.message} - Context: {context}"
            )
        elif severity == ErrorSeverity.HIGH:
            self.logger.error(
                f"HIGH_ERROR: {secure_error.error_id} - {secure_error.message}"
            )
        else:
            self.logger.warning(
                f"ERROR: {secure_error.error_id} - {secure_error.message}"
            )
        
        # Audit logging
        audit_data = {
            "error_id": secure_error.error_id,
            "category": category.value,
            "severity": severity.value,
            "timestamp": secure_error.timestamp.isoformat(),
            "user_hash": secure_error.user_id_hash,
            "request_id": request_id,
            "repeat_count": self.error_cache[secure_error.error_id]
        }
        self.audit_logger.info(f"ERROR_AUDIT: {json.dumps(audit_data)}")
        
        if should_raise:
            raise error
        
        return secure_error
    
    def get_circuit_breaker(self, service_name: str) -> CircuitBreaker:
        """Get or create circuit breaker for a service"""
        if service_name not in self.circuit_breakers:
            self.circuit_breakers[service_name] = CircuitBreaker()
        return self.circuit_breakers[service_name]
    
    def with_circuit_breaker(self, service_name: str):
        """Decorator for circuit breaker protection"""
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                circuit_breaker = self.get_circuit_breaker(service_name)
                
                if not circuit_breaker.should_allow_request():
                    error = RuntimeError(f"Circuit breaker OPEN for service: {service_name}")
                    return self.handle_error(
                        error,
                        ErrorCategory.EXTERNAL_API,
                        ErrorSeverity.HIGH,
                        f"Circuit breaker protection for {service_name}",
                        should_raise=True
                    )
                
                try:
                    result = func(*args, **kwargs)
                    circuit_breaker.record_success()
                    return result
                except Exception as e:
                    circuit_breaker.record_failure()
                    return self.handle_error(
                        e,
                        ErrorCategory.EXTERNAL_API,
                        ErrorSeverity.HIGH,
                        f"Circuit breaker failure for {service_name}",
                        should_raise=True
                    )
            
            return wrapper
        return decorator
    
    def defensive_try(self, 
                     operation: Callable,
                     category: ErrorCategory,
                     safe_default_value: Any = None,
                     context: str = "",
                     user_id: str = None,
                     request_id: str = None) -> Any:
        """
        Execute operation with defensive error handling.
        
        Args:
            operation: Function to execute
            category: Error category
            safe_default_value: Value to return on error
            context: Additional context
            user_id: User ID for audit trail
            request_id: Request ID for tracing
            
        Returns:
            Result of operation or safe default value
        """
        try:
            return operation()
        except Exception as e:
            self.handle_error(
                e, category, ErrorSeverity.MEDIUM, 
                context, user_id, request_id, should_raise=False
            )
            return safe_default_value


# Global secure error handler instance
_global_handler = None


def get_secure_error_handler(service_name: str = "ats_financial") -> SecureErrorHandler:
    """Get global secure error handler instance"""
    global _global_handler
    if _global_handler is None:
        _global_handler = SecureErrorHandler(service_name)
    return _global_handler


# Convenience decorators
def secure_financial_operation(category: ErrorCategory = ErrorCategory.BUSINESS_LOGIC):
    """Decorator for secure financial operations"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            handler = get_secure_error_handler()
            try:
                return func(*args, **kwargs)
            except Exception as e:
                return handler.handle_error(
                    e, category, ErrorSeverity.HIGH,
                    f"Financial operation: {func.__name__}",
                    should_raise=True
                )
        return wrapper
    return decorator


def with_validation_error_handling(func: Callable):
    """Decorator for validation error handling"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        handler = get_secure_error_handler()
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            return handler.handle_error(
                e, ErrorCategory.VALIDATION, ErrorSeverity.MEDIUM,
                f"Validation in {func.__name__}",
                should_raise=False
            )
        except Exception as e:
            return handler.handle_error(
                e, ErrorCategory.SYSTEM, ErrorSeverity.HIGH,
                f"Unexpected error in {func.__name__}",
                should_raise=True
            )
    return wrapper


# Example usage and testing
if __name__ == "__main__":
    # Test secure error handling
    handler = SecureErrorHandler("test_service")
    
    # Test different error scenarios
    try:
        # Simulate a validation error with PII
        raise ValueError("Invalid email: john.doe@company.com for user 123-45-6789")
    except Exception as e:
        secure_error = handler.handle_error(
            e, 
            ErrorCategory.VALIDATION, 
            ErrorSeverity.MEDIUM,
            "User input validation",
            user_id="user123",
            request_id="req456",
            should_raise=False
        )
        print(f"Secure error: {secure_error.error_id} - {secure_error.message}")
    
    # Test circuit breaker
    @handler.with_circuit_breaker("external_api")
    def risky_api_call():
        raise ConnectionError("API unavailable")
    
    # Test defensive try
    result = handler.defensive_try(
        lambda: int("not_a_number"),
        ErrorCategory.VALIDATION,
        safe_default_value=0,
        context="String to integer conversion"
    )
    print(f"Defensive try result: {result}")