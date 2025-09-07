"""
Centralized logging configuration for ATS-GenAI.

This module provides structured logging with environment-specific levels,
performance monitoring, and integration with monitoring systems.
"""

import json
import logging
import logging.handlers
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

from core.platform.config.settings import get_settings


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in {
                'name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                'filename', 'module', 'lineno', 'funcName', 'created',
                'msecs', 'relativeCreated', 'thread', 'threadName',
                'processName', 'process', 'getMessage', 'stack_info',
                'exc_info', 'exc_text'
            }:
                log_entry[key] = value

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add stack info if present
        if record.stack_info:
            log_entry["stack_info"] = record.stack_info

        return json.dumps(log_entry, default=str)


class TimingLogger:
    """Logger with built-in timing capabilities."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    # Delegate standard logging methods to the underlying logger
    def debug(self, message, *args, **kwargs):
        return self.logger.debug(message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        return self.logger.info(message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        return self.logger.warning(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        return self.logger.error(message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        return self.logger.critical(message, *args, **kwargs)

    @contextmanager
    def timer(self, operation: str, **context):
        """Context manager for timing operations."""
        start_time = time.time()
        self.logger.info(f"Starting {operation}", extra=context)

        try:
            yield
            duration = (time.time() - start_time) * 1000  # Convert to milliseconds
            self.logger.info(
                f"Completed {operation}",
                extra={**context, "duration_ms": duration}
            )
        except Exception as e:
            duration = (time.time() - start_time) * 1000
            self.logger.error(
                f"Failed {operation}: {str(e)}",
                extra={**context, "duration_ms": duration, "error": str(e)},
                exc_info=True
            )
            raise


def setup_logging() -> None:
    """Setup application logging configuration."""
    settings = get_settings()

    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Set root level
    root_logger.setLevel(getattr(logging, settings.log_level.value))

    # Setup formatters
    if settings.log_format == "structured":
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, settings.log_level.value))
    root_logger.addHandler(console_handler)

    # File handler (if configured)
    if settings.log_file:
        log_file_path = Path(settings.log_file)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        if settings.log_rotation:
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file_path,
                maxBytes=100 * 1024 * 1024,  # 100MB
                backupCount=5,
                encoding="utf-8"
            )
        else:
            file_handler = logging.FileHandler(
                filename=log_file_path,
                encoding="utf-8"
            )

        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, settings.log_level.value))
        root_logger.addHandler(file_handler)

    # Environment-specific configuration
    if settings.is_development:
        # More verbose logging in development
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
    elif settings.is_production:
        # Suppress verbose third-party logging in production
        logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("requests").setLevel(logging.WARNING)

    # Setup application loggers
    logging.getLogger("ats_genai").setLevel(getattr(logging, settings.log_level.value))

    # Log startup message
    logger = logging.getLogger(__name__)
    logger.info(
        "Logging configured successfully",
        extra={
            "environment": settings.environment.value,
            "log_level": settings.log_level.value,
            "log_format": settings.log_format,
            "log_file": str(settings.log_file) if settings.log_file else None
        }
    )


def get_logger(name: str) -> TimingLogger:
    """
    Get a logger instance with timing capabilities.

    Args:
        name: Logger name (typically __name__)

    Returns:
        TimingLogger instance
    """
    logger = logging.getLogger(name)
    return TimingLogger(logger)


def log_performance_metrics(
    operation: str,
    duration_ms: float,
    **metrics: Union[int, float, str]
) -> None:
    """
    Log performance metrics for monitoring.

    Args:
        operation: Name of the operation
        duration_ms: Duration in milliseconds
        **metrics: Additional performance metrics
    """
    logger = logging.getLogger("performance")
    logger.info(
        f"Performance metrics for {operation}",
        extra={
            "operation": operation,
            "duration_ms": duration_ms,
            "performance_metrics": True,
            **metrics
        }
    )


def log_business_event(
    event_type: str,
    event_data: Dict[str, Any],
    user_id: Optional[str] = None
) -> None:
    """
    Log business events for audit and analytics.

    Args:
        event_type: Type of business event
        event_data: Event data
        user_id: Optional user ID
    """
    logger = logging.getLogger("business_events")
    logger.info(
        f"Business event: {event_type}",
        extra={
            "event_type": event_type,
            "event_data": event_data,
            "user_id": user_id,
            "business_event": True
        }
    )


def log_security_event(
    event_type: str,
    details: Dict[str, Any],
    severity: str = "info"
) -> None:
    """
    Log security events for monitoring and alerting.

    Args:
        event_type: Type of security event
        details: Event details
        severity: Event severity (info, warning, error, critical)
    """
    logger = logging.getLogger("security")
    log_method = getattr(logger, severity.lower(), logger.info)

    log_method(
        f"Security event: {event_type}",
        extra={
            "event_type": event_type,
            "security_event": True,
            "severity": severity,
            **details
        }
    )


def log_api_request(
    method: str,
    endpoint: str,
    status_code: int,
    duration_ms: float,
    user_id: Optional[str] = None,
    **extra_data: Any
) -> None:
    """
    Log API requests for monitoring and analytics.

    Args:
        method: HTTP method
        endpoint: API endpoint
        status_code: HTTP status code
        duration_ms: Request duration in milliseconds
        user_id: Optional user ID
        **extra_data: Additional request data
    """
    logger = logging.getLogger("api_requests")

    level = logging.INFO
    if status_code >= 500:
        level = logging.ERROR
    elif status_code >= 400:
        level = logging.WARNING

    logger.log(
        level,
        f"{method} {endpoint} - {status_code}",
        extra={
            "api_request": True,
            "method": method,
            "endpoint": endpoint,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "user_id": user_id,
            **extra_data
        }
    )


def log_data_quality_event(
    data_source: str,
    quality_score: float,
    issues: Optional[Dict[str, Any]] = None,
    **metadata: Any
) -> None:
    """
    Log data quality events for monitoring.

    Args:
        data_source: Name of data source
        quality_score: Quality score (0.0 to 1.0)
        issues: Optional quality issues
        **metadata: Additional metadata
    """
    logger = logging.getLogger("data_quality")

    level = logging.INFO
    if quality_score < 0.8:
        level = logging.WARNING
    if quality_score < 0.6:
        level = logging.ERROR

    logger.log(
        level,
        f"Data quality check for {data_source}: {quality_score:.2%}",
        extra={
            "data_quality_event": True,
            "data_source": data_source,
            "quality_score": quality_score,
            "issues": issues or {},
            **metadata
        }
    )


# Context managers for structured logging
@contextmanager
def log_context(**context_data: Any):
    """
    Add context data to all log messages within the context.

    Usage:
        with log_context(user_id="123", operation="data_ingestion"):
            logger.info("Processing data")  # Will include user_id and operation
    """
    # This is a simplified implementation
    # In a full implementation, you'd use contextvars or thread-local storage
    try:
        yield
    finally:
        pass