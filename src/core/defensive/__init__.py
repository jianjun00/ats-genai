"""
Defensive Coding Module for ATS Financial System

This module provides comprehensive defensive coding utilities for the ATS platform:
- Financial data validation with security audit trails
- SQL injection prevention and input sanitization
- Secure error handling with PII protection
- Resource management with connection pooling and timeouts
- Circuit breakers and rate limiting
- Comprehensive audit logging with compliance features

Key defensive principles implemented:
1. Never trust any input data - validate everything
2. Fail securely when validation fails
3. Log all operations with audit trails and security monitoring
4. Use precise decimal arithmetic for financial calculations
5. Implement timeouts and resource limits to prevent DoS
6. Sanitize all output to prevent information disclosure
7. Use circuit breakers for external service protection
8. Monitor and limit resource usage to prevent exhaustion
"""

from .financial_validator import (
    DefensiveFinancialValidator,
    ValidationResult,
    ValidationSeverity,
    SecurityLevel,
    validate_stock_symbol,
    validate_stock_price,
    validate_financial_data_record
)

from .secure_error_handler import (
    SecureErrorHandler,
    SecureError,
    ErrorSeverity,
    ErrorCategory,
    CircuitBreaker,
    RateLimiter,
    get_secure_error_handler,
    secure_financial_operation,
    with_validation_error_handling
)

from .resource_manager import (
    DefensiveResourceManager,
    ResourceType,
    ResourceLimits,
    ResourceMetrics,
    get_resource_manager,
    defensive_db_connection,
    defensive_http_session,
    defensive_threads
)

__all__ = [
    # Financial validation
    'DefensiveFinancialValidator',
    'ValidationResult',
    'ValidationSeverity',
    'SecurityLevel',
    'validate_stock_symbol',
    'validate_stock_price',
    'validate_financial_data_record',
    
    # Secure error handling
    'SecureErrorHandler',
    'SecureError', 
    'ErrorSeverity',
    'ErrorCategory',
    'CircuitBreaker',
    'RateLimiter',
    'get_secure_error_handler',
    'secure_financial_operation',
    'with_validation_error_handling',
    
    # Resource management
    'DefensiveResourceManager',
    'ResourceType',
    'ResourceLimits',
    'ResourceMetrics',
    'get_resource_manager',
    'defensive_db_connection',
    'defensive_http_session',
    'defensive_threads'
]