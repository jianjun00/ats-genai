"""
Logging configuration for the data agent.

This module provides configurable logging setup for the data agent,
allowing users to adjust logging levels and formats based on their needs.
"""

import logging
import logging.config
import os
import sys
import json
from typing import Dict, Any, Optional, Union
from pathlib import Path

# Default logging configuration
DEFAULT_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
        "detailed": {
            "format": "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
        }
        # JSON formatter will be added dynamically if python-json-logger is available
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "filename": "data_agent.log",
            "maxBytes": 10485760,  # 10 MB
            "backupCount": 5,
            "encoding": "utf8"
        }
    },
    "loggers": {
        "src.market_data.agent": {
            "level": "INFO",
            "handlers": ["console", "file"],
            "propagate": False
        }
    },
    "root": {
        "level": "WARNING",
        "handlers": ["console"]
    }
}

def setup_logging(
    config_path: Optional[Union[str, Path]] = None,
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    json_format: bool = False
) -> None:
    """
    Configure logging for the data agent.
    
    Args:
        config_path: Path to a JSON or YAML logging config file
        log_level: Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Override log file path
        json_format: Whether to use JSON formatting for logs
    """
    config = DEFAULT_CONFIG.copy()
    
    # Load config from file if provided
    if config_path:
        try:
            config_path = Path(config_path)
            if config_path.exists():
                with open(config_path, 'r') as f:
                    if config_path.suffix.lower() in ('.yaml', '.yml'):
                        try:
                            import yaml
                            file_config = yaml.safe_load(f)
                        except ImportError:
                            logging.warning("PyYAML not installed, falling back to default config")
                            file_config = {}
                    else:
                        file_config = json.load(f)
                
                # Update config with file settings
                if file_config:
                    _deep_update(config, file_config)
        except Exception as e:
            logging.warning(f"Error loading logging config from {config_path}: {e}")
    
    # Override with environment variables
    env_log_level = os.environ.get('DATA_AGENT_LOG_LEVEL')
    if env_log_level:
        log_level = env_log_level
        
    env_log_file = os.environ.get('DATA_AGENT_LOG_FILE')
    if env_log_file:
        log_file = env_log_file
        
    env_json_format = os.environ.get('DATA_AGENT_JSON_LOGS', '').lower() in ('true', '1', 'yes')
    if env_json_format:
        json_format = True
    
    # Override log level if provided
    if log_level:
        log_level = log_level.upper()
        # Update root logger and all other loggers
        config['root']['level'] = log_level
        for logger_name in config['loggers']:
            config['loggers'][logger_name]['level'] = log_level
    
    # Override log file if provided
    if log_file:
        for handler_name, handler_config in config['handlers'].items():
            if handler_config.get('class') == 'logging.handlers.RotatingFileHandler':
                handler_config['filename'] = log_file
    
    # Use JSON formatter if requested
    if json_format:
        try:
            # Check if python-json-logger is installed
            from pythonjsonlogger import jsonlogger
            # Add JSON formatter dynamically
            config['formatters']['json'] = {
                "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
                "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "datefmt": "%Y-%m-%dT%H:%M:%S%z"
            }
            for handler_name, handler_config in config['handlers'].items():
                handler_config['formatter'] = 'json'
        except ImportError:
            logging.warning("python-json-logger not installed, falling back to standard formatter")
    
    # Apply the configuration
    logging.config.dictConfig(config)
    
    # Log the configuration
    logging.info(f"Logging configured with level: {log_level or config['root']['level']}")
    if log_file:
        logging.info(f"Log file: {log_file}")
    if json_format:
        logging.info("Using JSON log format")

def _deep_update(d: Dict[str, Any], u: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively update a dictionary.
    
    Args:
        d: Dictionary to update
        u: Dictionary with updates
        
    Returns:
        Updated dictionary
    """
    for k, v in u.items():
        if isinstance(v, dict) and k in d and isinstance(d[k], dict):
            _deep_update(d[k], v)
        else:
            d[k] = v
    return d

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)
