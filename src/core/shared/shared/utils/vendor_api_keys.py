#!/usr/bin/env python3
"""
Vendor API Key Management Utilities

Provides standardized API key resolution across all vendor integrations.
Replaces repeated patterns found in 15+ files across the codebase.

USAGE:
======

# Instead of this repeated pattern:
# polygon_api_key = os.environ.get("POLYGON_API_KEY") or env.get_api_key('polygon')

# Use this:
from src.core.shared.utils.vendor_api_keys import get_vendor_api_key

polygon_api_key = get_vendor_api_key('polygon')
eodhd_api_key = get_vendor_api_key('eodhd')
tiingo_api_key = get_vendor_api_key('tiingo')
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Vendor to environment variable mapping
VENDOR_API_KEY_MAP = {
    'polygon': 'POLYGON_API_KEY',
    'eodhd': 'EODHD_API_KEY',
    'tiingo': 'TIINGO_API_KEY',
    'alpha_vantage': 'ALPHA_VANTAGE_API_KEY',
    'finnhub': 'FINNHUB_API_KEY',
    'quandl': 'QUANDL_API_KEY',
    'iex': 'IEX_API_KEY',
    'newsapi': 'NEWS_API_KEY'
}

def get_vendor_api_key(vendor: str, env=None, required: bool = True) -> Optional[str]:
    """
    Get API key for a vendor using standardized resolution logic.

    Resolution order:
    1. Environment variable (e.g., POLYGON_API_KEY)
    2. Vendor utils system (if available)
    3. Gin configuration files (env.get_api_key)

    Args:
        vendor: Vendor name (e.g., 'polygon', 'eodhd', 'tiingo')
        env: Environment instance (optional, for gin config fallback)
        required: Whether to log error if key not found (default: True)

    Returns:
        API key string or None if not found

    Examples:
        >>> api_key = get_vendor_api_key('polygon')
        >>> api_key = get_vendor_api_key('eodhd', env=environment)
        >>> api_key = get_vendor_api_key('tiingo', required=False)
    """
    vendor = vendor.lower().strip()

    # Get the environment variable name
    env_var_name = VENDOR_API_KEY_MAP.get(vendor)
    if not env_var_name:
        if required:
            logger.error(f"Unknown vendor '{vendor}'. Supported vendors: {list(VENDOR_API_KEY_MAP.keys())}")
        return None

    # Method 1: Environment variable (highest priority)
    api_key = os.environ.get(env_var_name)
    if api_key:
        logger.debug(f"Using {env_var_name} from environment variable")
        return api_key

    # Method 2: Vendor utils system
    try:
        if vendor == 'polygon':
            from src.infrastructure.vendor.polygon.utils import POLYGON_API_KEY
            if POLYGON_API_KEY:
                logger.debug(f"Using {env_var_name} from polygon utils system")
                return POLYGON_API_KEY
        elif vendor == 'eodhd':
            from src.infrastructure.vendor.eodhd.utils import EODHD_API_KEY
            if EODHD_API_KEY:
                logger.debug(f"Using {env_var_name} from eodhd utils system")
                return EODHD_API_KEY
        elif vendor == 'tiingo':
            from src.infrastructure.vendor.tiingo.utils import TIINGO_API_KEY
            if TIINGO_API_KEY:
                logger.debug(f"Using {env_var_name} from tiingo utils system")
                return TIINGO_API_KEY
    except ImportError:
        pass

    # Method 3: Environment system with gin config
    if env:
        try:
            api_key = env.get_api_key(vendor)
            if api_key:
                logger.debug(f"Using {env_var_name} from gin configuration")
                return api_key
        except Exception:
            pass

    # Not found
    if required:
        logger.error(f"No API key found for vendor '{vendor}'. Set {env_var_name} environment variable or configure in gin files.")

    return None

def get_all_vendor_api_keys(env=None, required_vendors: Optional[list] = None) -> dict:
    """
    Get API keys for multiple vendors at once.

    Args:
        env: Environment instance (optional)
        required_vendors: List of vendors that must have keys (optional)

    Returns:
        Dictionary mapping vendor names to API keys

    Example:
        >>> keys = get_all_vendor_api_keys(required_vendors=['polygon', 'eodhd'])
        >>> polygon_key = keys.get('polygon')
        >>> eodhd_key = keys.get('eodhd')
    """
    api_keys = {}

    for vendor in VENDOR_API_KEY_MAP.keys():
        is_required = required_vendors and vendor in required_vendors
        api_key = get_vendor_api_key(vendor, env=env, required=is_required)
        if api_key:
            api_keys[vendor] = api_key

    return api_keys

def validate_vendor_api_key(vendor: str, api_key: str) -> bool:
    """
    Basic validation of API key format for a vendor.

    Args:
        vendor: Vendor name
        api_key: API key to validate

    Returns:
        True if key appears valid, False otherwise
    """
    if not api_key or not isinstance(api_key, str):
        return False

    vendor = vendor.lower().strip()

    # Vendor-specific validation rules
    validation_rules = {
        'polygon': lambda k: len(k) > 10 and k.replace('_', '').replace('.', '').isalnum(),
        'eodhd': lambda k: len(k) > 10 and k.replace('_', '').replace('.', '').isalnum(),
        'tiingo': lambda k: len(k) > 10 and k.replace('_', '').replace('-', '').isalnum(),
        'alpha_vantage': lambda k: len(k) > 10 and k.isalnum(),
    }

    validator = validation_rules.get(vendor, lambda k: len(k) > 5)
    return validator(api_key)

# Convenience functions for common vendors
def get_polygon_api_key(env=None, required: bool = True) -> Optional[str]:
    """Get Polygon.io API key"""
    return get_vendor_api_key('polygon', env=env, required=required)

def get_eodhd_api_key(env=None, required: bool = True) -> Optional[str]:
    """Get EOD Historical Data API key"""
    return get_vendor_api_key('eodhd', env=env, required=required)

def get_tiingo_api_key(env=None, required: bool = True) -> Optional[str]:
    """Get Tiingo API key"""
    return get_vendor_api_key('tiingo', env=env, required=required)

def get_alpha_vantage_api_key(env=None, required: bool = True) -> Optional[str]:
    """Get Alpha Vantage API key"""
    return get_vendor_api_key('alpha_vantage', env=env, required=required)