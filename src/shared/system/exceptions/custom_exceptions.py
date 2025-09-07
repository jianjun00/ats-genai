"""
Custom exceptions for ATS-GenAI system.

This module defines all custom exceptions used throughout the application
with proper error codes, messages, and context information.
"""

from typing import Optional, Dict, Any


class ATSBaseException(Exception):
    """Base exception for all ATS-GenAI exceptions."""
    
    def __init__(
        self, 
        message: str, 
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.context = context or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for logging/API responses."""
        return {
            "error_type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "context": self.context
        }


# Database Exceptions
class DatabaseError(ATSBaseException):
    """General database error."""


class DatabaseConnectionError(DatabaseError):
    """Database connection failed."""


class DatabaseMigrationError(DatabaseError):
    """Database migration failed."""


class DatabaseValidationError(DatabaseError):
    """Database validation failed."""


# Configuration Exceptions
class ConfigurationError(ATSBaseException):
    """Configuration error."""


class EnvironmentError(ConfigurationError):
    """Environment configuration error."""


class MissingConfigurationError(ConfigurationError):
    """Required configuration missing."""


# Data Exceptions
class DataError(ATSBaseException):
    """General data error."""


class DataValidationError(DataError):
    """Data validation failed."""


class DataQualityError(DataError):
    """Data quality check failed."""


class DataIngestionError(DataError):
    """Data ingestion failed."""


class DataReconciliationError(DataError):
    """Data reconciliation failed."""


# API Exceptions
class APIError(ATSBaseException):
    """General API error."""


class APIConnectionError(APIError):
    """API connection failed."""


class APIRateLimitError(APIError):
    """API rate limit exceeded."""


class APIAuthenticationError(APIError):
    """API authentication failed."""


class APITimeoutError(APIError):
    """API request timed out."""


# Portfolio Exceptions
class PortfolioError(ATSBaseException):
    """General portfolio error."""


class PortfolioOptimizationError(PortfolioError):
    """Portfolio optimization failed."""


class RiskManagementError(PortfolioError):
    """Risk management constraint violated."""


class FactorExposureError(PortfolioError):
    """Factor exposure constraint violated."""


# Signal Exceptions
class SignalError(ATSBaseException):
    """General signal error."""


class SignalCalculationError(SignalError):
    """Signal calculation failed."""


class SignalValidationError(SignalError):
    """Signal validation failed."""


class IndicatorError(SignalError):
    """Technical indicator calculation failed."""


# Market Data Exceptions
class MarketDataError(ATSBaseException):
    """General market data error."""


class MarketDataUnavailableError(MarketDataError):
    """Market data not available."""


class VendorError(MarketDataError):
    """Data vendor error."""


class DataStaleError(MarketDataError):
    """Data is stale/outdated."""


# Event Exceptions
class EventError(ATSBaseException):
    """General event error."""


class EventProcessingError(EventError):
    """Event processing failed."""


class EventValidationError(EventError):
    """Event validation failed."""


# Security Exceptions
class SecurityError(ATSBaseException):
    """Security-related error."""


class AuthenticationError(SecurityError):
    """Authentication failed."""


class AuthorizationError(SecurityError):
    """Authorization failed."""


class SecurityValidationError(SecurityError):
    """Security validation failed."""


# System Exceptions
class SystemError(ATSBaseException):
    """System-level error."""


class ResourceExhaustionError(SystemError):
    """System resources exhausted."""


class ServiceUnavailableError(SystemError):
    """Service temporarily unavailable."""


class CircuitBreakerError(SystemError):
    """Circuit breaker triggered."""


# Utility functions
def create_error_context(
    operation: str,
    component: str,
    **kwargs
) -> Dict[str, Any]:
    """
    Create standardized error context.
    
    Args:
        operation: The operation that failed
        component: The component where error occurred
        **kwargs: Additional context information
    
    Returns:
        Dictionary with error context
    """
    context = {
        "operation": operation,
        "component": component,
        "timestamp": __import__("datetime").datetime.utcnow().isoformat()
    }
    context.update(kwargs)
    return context


def handle_database_error(e: Exception, operation: str) -> DatabaseError:
    """
    Convert generic database exceptions to specific ATS exceptions.
    
    Args:
        e: Original exception
        operation: Database operation that failed
    
    Returns:
        Appropriate DatabaseError subclass
    """
    import psycopg2
    import sqlalchemy.exc
    
    context = create_error_context(
        operation=operation,
        component="database",
        original_error=str(e)
    )
    
    if isinstance(e, (psycopg2.OperationalError, sqlalchemy.exc.OperationalError)):
        return DatabaseConnectionError(
            f"Database connection failed during {operation}",
            context=context
        )
    elif isinstance(e, (psycopg2.IntegrityError, sqlalchemy.exc.IntegrityError)):
        return DatabaseValidationError(
            f"Database integrity constraint violated during {operation}",
            context=context
        )
    else:
        return DatabaseError(
            f"Database error during {operation}: {str(e)}",
            context=context
        )


def handle_api_error(e: Exception, vendor: str, operation: str) -> APIError:
    """
    Convert generic API exceptions to specific ATS exceptions.
    
    Args:
        e: Original exception
        vendor: API vendor name
        operation: API operation that failed
    
    Returns:
        Appropriate APIError subclass
    """
    import requests
    
    context = create_error_context(
        operation=operation,
        component="api",
        vendor=vendor,
        original_error=str(e)
    )
    
    if isinstance(e, requests.exceptions.Timeout):
        return APITimeoutError(
            f"{vendor} API timeout during {operation}",
            context=context
        )
    elif isinstance(e, requests.exceptions.ConnectionError):
        return APIConnectionError(
            f"{vendor} API connection failed during {operation}",
            context=context
        )
    elif hasattr(e, 'response') and e.response.status_code == 429:
        return APIRateLimitError(
            f"{vendor} API rate limit exceeded during {operation}",
            context=context
        )
    elif hasattr(e, 'response') and e.response.status_code == 401:
        return APIAuthenticationError(
            f"{vendor} API authentication failed during {operation}",
            context=context
        )
    else:
        return APIError(
            f"{vendor} API error during {operation}: {str(e)}",
            context=context
        )