#!/usr/bin/env python3
"""
Vendor Data Parsers - Unified data transformation utilities for all vendor APIs

Consolidates data parsing logic from 15+ vendor service files into reusable utilities.
Handles API response transformation, data validation, and standardization.

USAGE:
======

from shared.utils.vendor_data_parsers import (
    parse_polygon_price_data,
    parse_eodhd_price_data,
    parse_tiingo_price_data,
    parse_vendor_date
)

# Polygon API response parsing
rows = parse_polygon_price_data(api_response_data, symbol, instrument_id)

# EODHD API response parsing
rows = parse_eodhd_price_data(api_response_data, symbol, instrument_id)

# Unified date parsing
date_obj = parse_vendor_date(date_string, vendor='tiingo')
"""

import logging
from datetime import datetime, date, timezone
from typing import List, Tuple, Optional, Dict, Any, Union
from decimal import Decimal

logger = logging.getLogger(__name__)

# =============================================================================
# DATE PARSING UTILITIES
# =============================================================================

def parse_vendor_date(
    val: Any,
    vendor: str = "generic",
    strict: bool = False
) -> Optional[date]:
    """
    Universal date parsing for all vendor APIs.

    Consolidates 5+ different parse_date implementations across vendor services.
    Handles multiple input formats and vendor-specific quirks.

    Args:
        val: Date value (string, datetime, date, or None)
        vendor: Vendor name for specific handling ('polygon', 'tiingo', 'eodhd')
        strict: If True, raises exceptions; if False, returns None on error

    Returns:
        date object or None if parsing fails

    Examples:
        >>> parse_vendor_date('2023-01-15')
        datetime.date(2023, 1, 15)

        >>> parse_vendor_date('2023-01-15T10:30:00Z', vendor='polygon')
        datetime.date(2023, 1, 15)

        >>> parse_vendor_date(None)
        None
    """
    if val is None or val == '':
        return None

    # Already a date object
    if isinstance(val, date):
        return val

    # Convert datetime to date
    if isinstance(val, datetime):
        return val.date()

    # Parse string values
    if isinstance(val, str):
        try:
            # Remove timezone info and extra formatting
            clean_val = val.strip()

            # Handle ISO format with time (2023-01-15T10:30:00Z)
            if 'T' in clean_val:
                clean_val = clean_val.split('T')[0]

            # Handle common formats
            if len(clean_val) >= 10:
                return datetime.strptime(clean_val[:10], "%Y-%m-%d").date()
            else:
                # Try other formats based on vendor
                if vendor == 'polygon':
                    # Polygon sometimes uses different formats
                    return datetime.strptime(clean_val, "%Y-%m-%d").date()
                else:
                    return datetime.strptime(clean_val, "%Y-%m-%d").date()

        except Exception as e:
            if strict:
                raise ValueError(f"Failed to parse date '{val}' for vendor '{vendor}': {e}")
            logger.debug(f"Failed to parse date '{val}' for vendor '{vendor}': {e}")
            return None

    # Try to convert other types to string first
    try:
        return parse_vendor_date(str(val), vendor, strict)
    except:
        if strict:
            raise ValueError(f"Cannot parse date value of type {type(val)}: {val}")
        return None

# =============================================================================
# POLYGON DATA PARSERS
# =============================================================================

def parse_polygon_price_data(
    prices: List[Dict[str, Any]],
    symbol: str,
    instrument_id: int
) -> List[Tuple]:
    """
    Parse Polygon API price response into database-ready rows.

    Extracted from polygon_30_year_daily_backfill.py insert_daily_prices_idempotent()

    Args:
        prices: List of price dictionaries from Polygon API
        symbol: Stock symbol
        instrument_id: Database instrument ID

    Returns:
        List of tuples ready for database insertion

    Example:
        >>> prices = [{'t': 1640995200000, 'o': 150.0, 'h': 155.0, 'l': 149.0, 'c': 154.0, 'v': 1000000}]
        >>> rows = parse_polygon_price_data(prices, 'AAPL', 12345)
        >>> # Returns: [(datetime.date(2022, 1, 1), 'AAPL', 150.0, 155.0, 149.0, 154.0, 1000000, 12345)]
    """
    if not prices:
        return []

    rows = []
    for price in prices:
        try:
            # Convert Polygon timestamp to date
            if 't' not in price:
                logger.debug(f"Skipping price record without timestamp for {symbol}")
                continue

            # Polygon uses millisecond timestamps
            date_val = datetime.fromtimestamp(price['t']/1000, tz=timezone.utc).date()

            # Extract OHLCV data with defaults
            row = (
                date_val,
                symbol,
                price.get('o'),  # open
                price.get('h'),  # high
                price.get('l'),  # low
                price.get('c'),  # close
                price.get('v', 0),  # volume (default to 0)
                instrument_id
            )
            rows.append(row)

        except Exception as e:
            logger.warning(f"Error processing Polygon price record for {symbol}: {e}")
            continue

    return rows

def parse_polygon_timestamp(timestamp_ms: int) -> date:
    """
    Convert Polygon millisecond timestamp to date.

    Args:
        timestamp_ms: Millisecond timestamp from Polygon API

    Returns:
        date object in UTC
    """
    return datetime.fromtimestamp(timestamp_ms/1000, tz=timezone.utc).date()

# =============================================================================
# EODHD DATA PARSERS
# =============================================================================

def parse_eodhd_price_data(
    prices: List[Dict[str, Any]],
    symbol: str,
    instrument_id: int
) -> List[Tuple]:
    """
    Parse EODHD API price response into database-ready rows.

    Extracted from eodhd_30_year_daily_backfill.py insert_daily_prices_idempotent()

    Args:
        prices: List of price dictionaries from EODHD API
        symbol: Stock symbol
        instrument_id: Database instrument ID

    Returns:
        List of tuples ready for database insertion
    """
    if not prices:
        return []

    rows = []
    for price in prices:
        try:
            # EODHD uses date strings
            date_val = parse_vendor_date(price.get('date'), vendor='eodhd')
            if not date_val:
                logger.debug(f"Skipping price record without valid date for {symbol}")
                continue

            # Extract OHLCV data - EODHD has adjusted_close
            row = (
                date_val,
                symbol,
                price.get('open'),
                price.get('high'),
                price.get('low'),
                price.get('close'),
                price.get('adjusted_close'),  # EODHD specific
                price.get('volume', 0),
                instrument_id
            )
            rows.append(row)

        except Exception as e:
            logger.warning(f"Error processing EODHD price record for {symbol}: {e}")
            continue

    return rows

# =============================================================================
# TIINGO DATA PARSERS
# =============================================================================

def parse_tiingo_price_data(
    prices: List[Dict[str, Any]],
    symbol: str,
    instrument_id: int
) -> List[Tuple]:
    """
    Parse Tiingo API price response into database-ready rows.

    Extracted from tiingo_30_year_daily_backfill.py insert_daily_prices_idempotent()

    Args:
        prices: List of price dictionaries from Tiingo API
        symbol: Stock symbol
        instrument_id: Database instrument ID

    Returns:
        List of tuples ready for database insertion
    """
    if not prices:
        return []

    rows = []
    for price in prices:
        try:
            # Tiingo uses date strings
            date_val = parse_vendor_date(price.get('date'), vendor='tiingo')
            if not date_val:
                logger.debug(f"Skipping price record without valid date for {symbol}")
                continue

            # Extract OHLCV data - Tiingo format
            row = (
                date_val,
                symbol,
                price.get('open'),
                price.get('high'),
                price.get('low'),
                price.get('close'),
                price.get('adjClose'),  # Tiingo uses adjClose
                price.get('volume', 0),
                instrument_id
            )
            rows.append(row)

        except Exception as e:
            logger.warning(f"Error processing Tiingo price record for {symbol}: {e}")
            continue

    return rows

def parse_tiingo_dividend_data(dividend_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse Tiingo dividend data with proper date handling.

    Extracted from map_tiingo_dividend() functions across multiple files.

    Args:
        dividend_data: Raw dividend data from Tiingo API

    Returns:
        Parsed dividend data with proper date objects
    """
    def parse_div_date(field_name: str) -> Optional[date]:
        return parse_vendor_date(dividend_data.get(field_name), vendor='tiingo')

    return {
        'symbol': dividend_data.get('symbol'),
        'ex_date': parse_div_date('exDate'),
        'declaration_date': parse_div_date('declarationDate'),
        'record_date': parse_div_date('recordDate'),
        'pay_date': parse_div_date('payDate'),
        'amount': dividend_data.get('cashAmount'),
        'currency': dividend_data.get('currency', 'USD'),
        'frequency': dividend_data.get('frequency'),
        'raw_data': dividend_data  # Preserve original for debugging
    }

# =============================================================================
# GENERIC DATA TRANSFORMATION UTILITIES
# =============================================================================

def normalize_ohlcv_data(
    price_data: Dict[str, Any],
    vendor_format: str = 'generic'
) -> Dict[str, Any]:
    """
    Normalize OHLCV data across different vendor formats.

    Args:
        price_data: Raw price data from vendor API
        vendor_format: Vendor format ('polygon', 'eodhd', 'tiingo')

    Returns:
        Normalized OHLCV dictionary with standard field names
    """
    if vendor_format == 'polygon':
        return {
            'date': parse_polygon_timestamp(price_data.get('t', 0)),
            'open': price_data.get('o'),
            'high': price_data.get('h'),
            'low': price_data.get('l'),
            'close': price_data.get('c'),
            'volume': price_data.get('v', 0),
            'timestamp': price_data.get('t')
        }
    elif vendor_format == 'eodhd':
        return {
            'date': parse_vendor_date(price_data.get('date'), vendor='eodhd'),
            'open': price_data.get('open'),
            'high': price_data.get('high'),
            'low': price_data.get('low'),
            'close': price_data.get('close'),
            'adjusted_close': price_data.get('adjusted_close'),
            'volume': price_data.get('volume', 0)
        }
    elif vendor_format == 'tiingo':
        return {
            'date': parse_vendor_date(price_data.get('date'), vendor='tiingo'),
            'open': price_data.get('open'),
            'high': price_data.get('high'),
            'low': price_data.get('low'),
            'close': price_data.get('close'),
            'adjusted_close': price_data.get('adjClose'),
            'volume': price_data.get('volume', 0)
        }
    else:
        # Generic format - pass through with date parsing
        result = price_data.copy()
        if 'date' in result:
            result['date'] = parse_vendor_date(result['date'])
        return result

def validate_price_data(price_data: Dict[str, Any], symbol: str) -> bool:
    """
    Validate price data meets basic requirements.

    Args:
        price_data: Normalized price data
        symbol: Stock symbol for logging

    Returns:
        True if valid, False otherwise
    """
    required_fields = ['date', 'close']

    for field in required_fields:
        if field not in price_data or price_data[field] is None:
            logger.debug(f"Invalid price data for {symbol}: missing {field}")
            return False

    # Validate price ranges
    close_price = price_data.get('close', 0)
    if isinstance(close_price, (int, float)) and (close_price <= 0 or close_price > 100000):
        logger.debug(f"Invalid close price for {symbol}: {close_price}")
        return False

    return True

# =============================================================================
# BULK DATA PROCESSING UTILITIES
# =============================================================================

def process_vendor_batch(
    api_response: List[Dict[str, Any]],
    symbol: str,
    instrument_id: int,
    vendor: str,
    validate: bool = True
) -> List[Tuple]:
    """
    Process a batch of vendor API data with unified parsing and validation.

    Args:
        api_response: Raw API response data
        symbol: Stock symbol
        instrument_id: Database instrument ID
        vendor: Vendor name ('polygon', 'eodhd', 'tiingo')
        validate: Whether to validate each record

    Returns:
        List of database-ready tuples
    """
    if vendor == 'polygon':
        parser = parse_polygon_price_data
    elif vendor == 'eodhd':
        parser = parse_eodhd_price_data
    elif vendor == 'tiingo':
        parser = parse_tiingo_price_data
    else:
        raise ValueError(f"Unsupported vendor: {vendor}")

    rows = parser(api_response, symbol, instrument_id)

    if validate:
        # Additional validation can be added here
        logger.debug(f"Processed {len(rows)} records for {symbol} from {vendor}")

    return rows