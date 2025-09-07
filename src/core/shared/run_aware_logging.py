"""
Run-Aware Logging Configuration

Enhanced logging that includes run_id for better traceability and monitoring.
"""

import logging
import logging.config
from typing import Optional, Dict, Any
import contextvars
from datetime import datetime

from .run_context import RunContext, get_current_run_context

# Context variable for storing current run_id
current_run_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('run_id', default=None)


class RunAwareFormatter(logging.Formatter):
    """
    Logging formatter that includes run_id in log messages.
    """
    
    def __init__(self, fmt=None, datefmt=None, style='%', validate=True):
        """Initialize with run_id support."""
        # Add run_id to the format if not already present
        if fmt and 'run_id' not in fmt:
            # Insert run_id after timestamp but before logger name
            if '%(asctime)s' in fmt:
                fmt = fmt.replace('%(asctime)s', '%(asctime)s [%(run_id)s]')
            else:
                fmt = '[%(run_id)s] ' + fmt
        elif fmt is None:
            fmt = '%(asctime)s [%(run_id)s] - %(name)s - %(levelname)s - %(message)s'
        
        super().__init__(fmt, datefmt, style, validate)
    
    def format(self, record):
        """Format log record with run_id."""
        # Get run_id from context or current run context
        run_id = current_run_id.get()
        
        if run_id is None:
            # Try to get from current run context
            run_context = get_current_run_context()
            if run_context:
                run_id = run_context.run_id
        
        # Set run_id in the log record
        record.run_id = run_id or 'no-run'
        
        return super().format(record)


class RunAwareFileHandler(logging.FileHandler):
    """
    File handler that creates run-specific log files.
    """
    
    def __init__(self, filename=None, mode='a', encoding=None, delay=False, run_context: Optional[RunContext] = None):
        """Initialize with run-aware file naming."""
        if run_context is None:
            run_context = get_current_run_context()
        
        if run_context and filename:
            # Create run-specific filename in the run's logs directory
            if not filename.endswith('.log'):
                filename += '.log'
            
            # Use run context's logs directory
            log_file = run_context.logs_dir / filename
            filename = str(log_file)
        elif filename is None:
            # Default filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ats_genai_{timestamp}.log"
            
            if run_context:
                log_file = run_context.logs_dir / filename
                filename = str(log_file)
        
        super().__init__(filename, mode, encoding, delay)
        self.run_context = run_context


def configure_run_aware_logging(run_context: Optional[RunContext] = None, 
                               log_level: str = "INFO",
                               console_logging: bool = True,
                               file_logging: bool = True,
                               detailed_format: bool = False) -> Dict[str, Any]:
    """
    Configure run-aware logging.
    
    Args:
        run_context: Optional run context for file organization
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        console_logging: Enable console logging
        file_logging: Enable file logging
        detailed_format: Use detailed log format with more information
        
    Returns:
        Logging configuration dictionary
    """
    if run_context is None:
        run_context = get_current_run_context()
    
    # Set current run_id in context
    if run_context:
        current_run_id.set(run_context.run_id)
    
    # Determine log format
    if detailed_format:
        log_format = '%(asctime)s [%(run_id)s] - %(name)s:%(lineno)d - %(levelname)s - %(funcName)s() - %(message)s'
    else:
        log_format = '%(asctime)s [%(run_id)s] - %(name)s - %(levelname)s - %(message)s'
    
    # Build logging configuration
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'run_aware': {
                '()': RunAwareFormatter,
                'fmt': log_format,
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {},
        'loggers': {
            '': {  # Root logger
                'level': log_level,
                'handlers': []
            }
        }
    }
    
    # Add console handler
    if console_logging:
        config['handlers']['console'] = {
            'class': 'logging.StreamHandler',
            'level': log_level,
            'formatter': 'run_aware',
            'stream': 'ext://sys.stdout'
        }
        config['loggers']['']['handlers'].append('console')
    
    # Add file handler
    if file_logging:
        if run_context:
            # Use run-specific log file
            log_file = run_context.logs_dir / "ats_genai.log"
            config['handlers']['file'] = {
                'class': 'logging.FileHandler',
                'level': log_level,
                'formatter': 'run_aware',
                'filename': str(log_file),
                'mode': 'a'
            }
        else:
            # Use default log file
            config['handlers']['file'] = {
                'class': 'logging.FileHandler',
                'level': log_level,
                'formatter': 'run_aware',
                'filename': 'ats_genai.log',
                'mode': 'a'
            }
        
        config['loggers']['']['handlers'].append('file')
    
    return config


def setup_run_aware_logging(run_context: Optional[RunContext] = None,
                           log_level: str = "INFO",
                           console_logging: bool = True,
                           file_logging: bool = True,
                           detailed_format: bool = False):
    """
    Set up run-aware logging for the application.
    
    Args:
        run_context: Optional run context for file organization
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        console_logging: Enable console logging
        file_logging: Enable file logging
        detailed_format: Use detailed log format with more information
    """
    config = configure_run_aware_logging(
        run_context=run_context,
        log_level=log_level,
        console_logging=console_logging,
        file_logging=file_logging,
        detailed_format=detailed_format
    )
    
    logging.config.dictConfig(config)
    
    # Log setup completion
    logger = logging.getLogger(__name__)
    if run_context:
        logger.info(f"Configured run-aware logging for run_id: {run_context.run_id}")
        logger.info(f"Log file location: {run_context.logs_dir}")
    else:
        logger.info("Configured standard logging (no run context)")


def get_run_aware_logger(name: str, run_context: Optional[RunContext] = None) -> logging.Logger:
    """
    Get a logger configured for run-aware logging.
    
    Args:
        name: Logger name (usually __name__)
        run_context: Optional run context
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    
    # Set run_id in context if provided
    if run_context:
        current_run_id.set(run_context.run_id)
    
    return logger


def set_current_run_id(run_id: str):
    """Set the current run_id for logging context."""
    current_run_id.set(run_id)


def get_current_run_id() -> Optional[str]:
    """Get the current run_id from logging context."""
    return current_run_id.get()


# Convenience function for quick setup
def enable_run_aware_logging(run_context: Optional[RunContext] = None):
    """
    Enable run-aware logging with sensible defaults.
    
    Args:
        run_context: Optional run context
    """
    setup_run_aware_logging(
        run_context=run_context,
        log_level="INFO",
        console_logging=True,
        file_logging=True,
        detailed_format=False
    )