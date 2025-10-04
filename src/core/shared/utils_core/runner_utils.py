"""
Shared utilities for runners - argument parsing and environment setup.

This module provides common functionality for CLI runners to eliminate code duplication
across training data runners, market data agents, and other command-line tools.
"""

import argparse
import asyncio
import logging
import os
import gin
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional, Dict, Any

from core.platform.config_env.environment import Environment, EnvironmentType


class RunnerConfig:
    """Configuration object for runners."""
    
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.environment: Optional[Environment] = None
        self.logger: Optional[logging.Logger] = None
        
    def __getattr__(self, name):
        """Allow direct access to args attributes."""
        return getattr(self.args, name)


def setup_logging(debug: bool = False, log_file: Optional[str] = None) -> logging.Logger:
    """
    Set up standard logging configuration for runners.
    
    Args:
        debug: Enable debug level logging
        log_file: Optional log file path
        
    Returns:
        Configured logger instance
    """
    level = logging.DEBUG if debug else logging.INFO
    
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers,
        force=True  # Override any existing configuration
    )
    
    return logging.getLogger("runner")


def load_gin_config(gin_config_path: Optional[str], logger: logging.Logger) -> bool:
    """
    Load gin configuration with fallback logic.
    
    Args:
        gin_config_path: Path to gin config file
        logger: Logger instance
        
    Returns:
        True if config loaded successfully, False otherwise
    """
    if not gin_config_path:
        logger.warning("No gin config path provided")
        return False
        
    gin_path = Path(gin_config_path)
    
    if gin_path.exists():
        logger.debug(f"Loading gin config from {gin_config_path}")
        gin.parse_config_file(str(gin_path))
        logger.info(f"✅ Gin config loaded successfully from {gin_config_path}")
        
        # Log operative config in debug mode
        operative_config = gin.operative_config_str()
        logger.debug(f"Current gin operative config:\n{operative_config}")
        return True
    else:
        logger.warning(f"❌ Gin config file not found at {gin_config_path}")
        return False


def setup_environment(environment_str: str, gin_config_path: Optional[str] = None, 
                     logger: Optional[logging.Logger] = None) -> Environment:
    """
    Set up Environment instance with proper configuration.
    
    Args:
        environment_str: Environment string ('dev', 'test', 'intg', 'prod')
        gin_config_path: Optional gin config path
        logger: Optional logger instance
        
    Returns:
        Configured Environment instance
    """
    if not logger:
        logger = logging.getLogger("runner")
    
    # Map environment string to EnvironmentType
    env_map = {
        'dev': EnvironmentType.DEV,
        'test': EnvironmentType.TEST,
        'intg': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION
    }
    
    if environment_str not in env_map:
        raise ValueError(f"Invalid environment: {environment_str}. Must be one of {list(env_map.keys())}")
    
    env_type = env_map[environment_str]
    logger.info(f"Setting up {environment_str} environment ({env_type})")
    
    # Create environment
    environment = Environment(gin_config_path, env_type)
    logger.info(f"✅ Environment setup complete: {environment.db_host}:{environment.db_port}/{environment.db_name}")
    
    return environment


def add_common_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """
    Add common arguments used across all runners.
    
    Args:
        parser: ArgumentParser instance to add arguments to
        
    Returns:
        Parser with common arguments added
    """
    # Environment configuration
    parser.add_argument('--environment', default='dev',
                       choices=['dev', 'test', 'intg', 'prod'],
                       help='Environment type (default: dev)')
    
    parser.add_argument('--gin-config', default=None,
                       help='Path to Gin config file (optional)')
    
    # Debugging and logging
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    
    parser.add_argument('--log-file', default=None,
                       help='Optional log file path')
    
    return parser


def add_date_range_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """
    Add date range arguments commonly used in data processing runners.
    
    Args:
        parser: ArgumentParser instance to add arguments to
        
    Returns:
        Parser with date range arguments added
    """
    parser.add_argument('--start-date', required=True,
                       help='Start date (YYYY-MM-DD)')
    
    parser.add_argument('--end-date', required=True,
                       help='End date (YYYY-MM-DD)')
    
    # Optional day offsets for expanded data collection window
    parser.add_argument('--start-day-offset', type=int, default=0,
                       help='Days to extend backwards from start date for data collection (default: 0)')
    
    parser.add_argument('--end-day-offset', type=int, default=0,
                       help='Days to extend forwards from end date for data collection (default: 0)')
    
    return parser


def add_symbol_selection_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """
    Add symbol/universe selection arguments.
    
    Args:
        parser: ArgumentParser instance to add arguments to
        
    Returns:
        Parser with symbol selection arguments added
    """
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--symbols', nargs='+', 
                      help='List of symbols (e.g. AAPL TSLA)')
    group.add_argument('--universe-id', type=int,
                      help='Universe ID to fetch all instruments')
    
    return parser


def add_output_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """
    Add output configuration arguments.
    
    Args:
        parser: ArgumentParser instance to add arguments to
        
    Returns:
        Parser with output arguments added
    """
    parser.add_argument('--output-dir', default='/data/training_data',
                       help='Output directory for training data (default: /data/training_data)')
    
    parser.add_argument('--storage-format', default='arrayrecord',
                       choices=['arrayrecord', 'json', 'parquet'],
                       help='Storage format for training data (default: arrayrecord)')
    
    return parser


def parse_dates(start_date_str: str, end_date_str: str) -> tuple[date, date]:
    """
    Parse date strings into date objects with validation.
    
    Args:
        start_date_str: Start date string (YYYY-MM-DD)
        end_date_str: End date string (YYYY-MM-DD)
        
    Returns:
        Tuple of (start_date, end_date)
        
    Raises:
        ValueError: If date parsing fails or date range is invalid
    """
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")
    
    if start_date > end_date:
        raise ValueError(f"Start date {start_date} must be before end date {end_date}")
    
    return start_date, end_date


def initialize_runner(parser: argparse.ArgumentParser, 
                     app_name: str = "runner") -> RunnerConfig:
    """
    Complete runner initialization: parse args, setup logging, load config, setup environment.
    
    Args:
        parser: Configured ArgumentParser instance
        app_name: Application name for logging
        
    Returns:
        RunnerConfig with initialized environment and logger
    """
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(debug=args.debug, log_file=getattr(args, 'log_file', None))
    logger.name = app_name
    
    logger.info(f"🚀 Starting {app_name}")
    logger.info(f"🔧 STEP 1: Loading configuration files")
    
    # Load gin configuration
    if hasattr(args, 'gin_config'):
        load_gin_config(args.gin_config, logger)
    
    logger.info(f"🌍 STEP 2: Environment setup")
    
    # Setup environment
    environment = setup_environment(args.environment, 
                                  getattr(args, 'gin_config', None), 
                                  logger)
    
    # Create config object
    config = RunnerConfig(args)
    config.environment = environment
    config.logger = logger
    
    logger.info(f"✅ {app_name} initialization complete")
    
    return config


# Pre-configured parser factories for common use cases

def create_training_data_parser(description: str) -> argparse.ArgumentParser:
    """Create a parser for training data runners with common arguments."""
    parser = argparse.ArgumentParser(description=description)
    
    # Add all common argument groups
    add_common_arguments(parser)
    add_date_range_arguments(parser)
    add_symbol_selection_arguments(parser)
    add_output_arguments(parser)
    
    # Training data specific arguments
    parser.add_argument('--base-duration', default='5m',
                       help='Runner base duration (default: 5m)')
    
    parser.add_argument('--base-interval', type=int, default=1,
                       help='Base data interval in minutes (default: 1)')
    
    parser.add_argument('--training-interval', type=int, default=60,
                       help='Training data generation interval in minutes (default: 60)')
    
    return parser


def create_market_data_parser(description: str) -> argparse.ArgumentParser:
    """Create a parser for market data runners with common arguments."""
    parser = argparse.ArgumentParser(description=description)
    
    # Add common arguments
    add_common_arguments(parser)
    add_date_range_arguments(parser)
    
    # Market data specific arguments
    parser.add_argument('--vendor', choices=['polygon', 'tiingo', 'eodhd'],
                       help='Data vendor to use')
    
    parser.add_argument('--data-type', choices=['daily', 'minute', 'fundamentals', 'news'],
                       default='daily',
                       help='Type of data to process (default: daily)')
    
    return parser


# Validation utilities

def validate_output_directory(output_dir: str, logger: logging.Logger) -> Path:
    """
    Validate and create output directory if needed.
    
    Args:
        output_dir: Output directory path
        logger: Logger instance
        
    Returns:
        Path object for the output directory
    """
    output_path = Path(output_dir)
    
    if not output_path.exists():
        logger.info(f"Creating output directory: {output_path}")
        output_path.mkdir(parents=True, exist_ok=True)
    
    if not output_path.is_dir():
        raise ValueError(f"Output path exists but is not a directory: {output_path}")
    
    logger.debug(f"✅ Output directory validated: {output_path}")
    return output_path


def generate_dataset_id(prefix: str = "dataset") -> str:
    """
    Generate a unique dataset ID with timestamp.
    
    Args:
        prefix: Prefix for the dataset ID
        
    Returns:
        Unique dataset ID string
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}"


def create_run_metadata(config: RunnerConfig, **kwargs) -> Dict[str, Any]:
    """
    Create standardized run metadata for logging and database storage.
    
    Args:
        config: RunnerConfig instance
        **kwargs: Additional metadata fields
        
    Returns:
        Dictionary with run metadata
    """
    metadata = {
        "start_time": datetime.now().isoformat(),
        "environment": config.environment.env_type.value,
        "debug": config.debug,
        "gin_config": getattr(config, 'gin_config', None),
        **kwargs
    }
    
    # Add date range if available
    if hasattr(config, 'start_date'):
        metadata["start_date"] = str(config.start_date)
    if hasattr(config, 'end_date'):
        metadata["end_date"] = str(config.end_date)
    
    # Add symbols if available  
    if hasattr(config, 'symbols') and config.symbols:
        metadata["symbols"] = config.symbols
    elif hasattr(config, 'universe_id') and config.universe_id:
        metadata["universe_id"] = config.universe_id
    
    return metadata