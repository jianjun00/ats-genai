#!/usr/bin/env python3
"""
Data Transformers - Unified data transformation utilities for vendor APIs

Consolidates data transformation and mapping patterns from 20+ vendor service files.
Provides standardized data mapping, field extraction, and type conversion utilities.

USAGE:
======

from shared.utils.data_transformers import (
    transform_vendor_dividend,
    transform_vendor_instrument,
    extract_price_fields,
    normalize_field_names
)

# Transform dividend data from any vendor
dividend = transform_vendor_dividend(
    raw_data,
    vendor='tiingo',
    symbol='AAPL'
)

# Transform instrument data
instrument = transform_vendor_instrument(
    raw_data,
    vendor='polygon',
    exchange_filter=['NYSE', 'NASDAQ']
)
"""

import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union, Tuple
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

# =============================================================================
# DATE AND TIMESTAMP TRANSFORMATIONS
# =============================================================================

def parse_vendor_date(val: Any, vendor: str = "generic", strict: bool = False) -> Optional[date]:
    """
    Universal date parsing for all vendor APIs.

    Consolidates date parsing logic from dividend_tiingo.py, native_range_dividend_tiingo.py,
    range_dividend_tiingo.py, populate_instrument_tiingo.py and other vendor services.

    Args:
        val: Date value in various formats (string, datetime, date, None)
        vendor: Vendor name for specific parsing logic
        strict: Raise exception on parse failure if True

    Returns:
        Parsed date object or None

    Examples:
        >>> parse_vendor_date("2024-01-15")
        datetime.date(2024, 1, 15)
        >>> parse_vendor_date("2024-01-15T10:30:00Z", "polygon")
        datetime.date(2024, 1, 15)
    """
    if val is None:
        return None

    if isinstance(val, date):
        return val

    if isinstance(val, datetime):
        return val.date()

    if isinstance(val, str):
        # Remove extra whitespace
        val = val.strip()
        if not val:
            return None

        try:
            # Handle ISO format with time (common in APIs)
            if 'T' in val:
                return datetime.fromisoformat(val.replace('Z', '+00:00')).date()

            # Handle standard date formats
            if len(val) >= 10:
                return datetime.strptime(val[:10], "%Y-%m-%d").date()
            elif len(val) == 8:  # YYYYMMDD
                return datetime.strptime(val, "%Y%m%d").date()
            elif len(val) == 6:  # YYMMDD
                return datetime.strptime(val, "%y%m%d").date()

        except ValueError as e:
            if strict:
                raise ValueError(f"Failed to parse date '{val}' for vendor {vendor}: {e}")
            logger.warning(f"Failed to parse date '{val}' for vendor {vendor}: {e}")
            return None

    if strict:
        raise TypeError(f"Unsupported date type {type(val)} for vendor {vendor}: {val}")

    logger.warning(f"Unsupported date type {type(val)} for vendor {vendor}: {val}")
    return None

def parse_vendor_timestamp(val: Any, vendor: str = "generic") -> Optional[datetime]:
    """
    Parse timestamp from vendor API response.

    Args:
        val: Timestamp value in various formats
        vendor: Vendor name for specific logic

    Returns:
        Parsed datetime object or None
    """
    if val is None:
        return None

    if isinstance(val, datetime):
        return val

    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None

        try:
            # Handle ISO format
            if 'T' in val:
                return datetime.fromisoformat(val.replace('Z', '+00:00'))
            # Handle epoch timestamps (common in some APIs)
            elif val.isdigit():
                timestamp = int(val)
                # Handle milliseconds vs seconds
                if timestamp > 10**10:  # Milliseconds
                    return datetime.fromtimestamp(timestamp / 1000)
                else:  # Seconds
                    return datetime.fromtimestamp(timestamp)
        except (ValueError, OSError) as e:
            logger.warning(f"Failed to parse timestamp '{val}' for vendor {vendor}: {e}")

    return None

# =============================================================================
# DIVIDEND DATA TRANSFORMATIONS
# =============================================================================

def transform_vendor_dividend(
    raw_data: Dict[str, Any],
    vendor: str,
    symbol: Optional[str] = None
) -> Dict[str, Any]:
    """
    Transform dividend data from any vendor API into standardized format.

    Consolidates mapping logic from map_tiingo_dividend() and map_tiingo_distribution()
    in dividend_tiingo.py, native_range_dividend_tiingo.py, range_dividend_tiingo.py.

    Args:
        raw_data: Raw dividend data from vendor API
        vendor: Vendor name (tiingo, polygon, eodhd)
        symbol: Symbol override if not in raw_data

    Returns:
        Standardized dividend dictionary
    """
    result = {
        'symbol': None,
        'ex_dividend_date': None,
        'cash_amount': None,
        'declaration_date': None,
        'payment_date': None,
        'record_date': None,
        'description': None,
        'refid': None,
        'qualified': None,
        'flag': None,
        'currency': None,
        'frequency': None,
    }

    if vendor == 'tiingo':
        # Handle both dividend and distribution endpoints
        result.update({
            'symbol': symbol or raw_data.get('ticker') or raw_data.get('symbol'),
            'ex_dividend_date': parse_vendor_date(raw_data.get('exDate'), vendor),
            'cash_amount': _parse_decimal(raw_data.get('amount') or raw_data.get('cashAmount')),
            'declaration_date': parse_vendor_date(raw_data.get('declaredDate') or raw_data.get('declarationDate'), vendor),
            'payment_date': parse_vendor_date(raw_data.get('paymentDate') or raw_data.get('payDate'), vendor),
            'record_date': parse_vendor_date(raw_data.get('recordDate'), vendor),
            'description': raw_data.get('description'),
            'refid': raw_data.get('id'),
            'qualified': raw_data.get('qualified'),
            'flag': raw_data.get('flag'),
            'currency': raw_data.get('currency'),
            'frequency': raw_data.get('frequency'),
        })

    elif vendor == 'polygon':
        result.update({
            'symbol': symbol or raw_data.get('ticker'),
            'ex_dividend_date': parse_vendor_date(raw_data.get('ex_dividend_date'), vendor),
            'cash_amount': _parse_decimal(raw_data.get('cash_amount')),
            'declaration_date': parse_vendor_date(raw_data.get('declaration_date'), vendor),
            'payment_date': parse_vendor_date(raw_data.get('pay_date'), vendor),
            'record_date': parse_vendor_date(raw_data.get('record_date'), vendor),
            'description': raw_data.get('description'),
            'refid': raw_data.get('id'),
            'currency': raw_data.get('currency', 'USD'),
            'frequency': _normalize_frequency(raw_data.get('frequency')),
        })

    elif vendor == 'eodhd':
        result.update({
            'symbol': symbol or raw_data.get('Code'),
            'ex_dividend_date': parse_vendor_date(raw_data.get('ExDate'), vendor),
            'cash_amount': _parse_decimal(raw_data.get('Dividend')),
            'declaration_date': parse_vendor_date(raw_data.get('DeclarationDate'), vendor),
            'payment_date': parse_vendor_date(raw_data.get('PaymentDate'), vendor),
            'record_date': parse_vendor_date(raw_data.get('RecordDate'), vendor),
            'currency': raw_data.get('Currency', 'USD'),
            'frequency': _normalize_frequency(raw_data.get('Period')),
        })

    else:
        # Generic transformation - try common field names
        result.update({
            'symbol': symbol or raw_data.get('symbol') or raw_data.get('ticker'),
            'ex_dividend_date': parse_vendor_date(
                raw_data.get('ex_dividend_date') or raw_data.get('exDate') or raw_data.get('ExDate'),
                vendor
            ),
            'cash_amount': _parse_decimal(
                raw_data.get('cash_amount') or raw_data.get('amount') or raw_data.get('Dividend')
            ),
            'declaration_date': parse_vendor_date(
                raw_data.get('declaration_date') or raw_data.get('declaredDate'), vendor
            ),
            'payment_date': parse_vendor_date(
                raw_data.get('payment_date') or raw_data.get('payDate') or raw_data.get('PaymentDate'), vendor
            ),
            'record_date': parse_vendor_date(raw_data.get('record_date') or raw_data.get('recordDate'), vendor),
            'currency': raw_data.get('currency') or raw_data.get('Currency', 'USD'),
        })

    # Clean up None values and validate required fields
    result = {k: v for k, v in result.items() if v is not None}

    if not result.get('symbol'):
        logger.warning(f"Missing symbol in {vendor} dividend data: {raw_data}")

    return result

# =============================================================================
# INSTRUMENT DATA TRANSFORMATIONS
# =============================================================================

def transform_vendor_instrument(
    raw_data: Dict[str, Any],
    vendor: str,
    exchange_filter: Optional[List[str]] = None
) -> Optional[Dict[str, Any]]:
    """
    Transform instrument data from any vendor API into standardized format.

    Consolidates instrument mapping logic from populate_instrument_tiingo.py and similar files.

    Args:
        raw_data: Raw instrument data from vendor API
        vendor: Vendor name
        exchange_filter: List of allowed exchange codes (e.g., ['NYSE', 'NASDAQ'])

    Returns:
        Standardized instrument dictionary or None if filtered out
    """
    result = {
        'symbol': None,
        'name': None,
        'exchange': None,
        'asset_type': None,
        'currency': None,
        'start_date': None,
        'end_date': None,
        'raw': raw_data
    }

    if vendor == 'tiingo':
        exchange_code = raw_data.get('exchangeCode', '')

        # Apply exchange filter if provided
        if exchange_filter and exchange_code not in exchange_filter:
            logger.debug(f"Filtering out {raw_data.get('ticker')} (exchange: {exchange_code})")
            return None

        result.update({
            'symbol': raw_data.get('ticker'),
            'name': raw_data.get('name'),
            'exchange': exchange_code,
            'asset_type': 'stock',  # Tiingo focuses on stocks
            'currency': 'USD',      # Default currency
            'start_date': parse_vendor_date(raw_data.get('startDate'), vendor),
            'end_date': parse_vendor_date(raw_data.get('endDate'), vendor),
        })

    elif vendor == 'polygon':
        result.update({
            'symbol': raw_data.get('ticker'),
            'name': raw_data.get('name'),
            'exchange': raw_data.get('primary_exchange') or raw_data.get('exchange'),
            'asset_type': _normalize_asset_type(raw_data.get('type')),
            'currency': raw_data.get('currency_name', 'USD'),
            'start_date': parse_vendor_date(raw_data.get('list_date'), vendor),
            'end_date': parse_vendor_date(raw_data.get('delisted_utc'), vendor),
        })

    elif vendor == 'eodhd':
        result.update({
            'symbol': raw_data.get('Code'),
            'name': raw_data.get('Name'),
            'exchange': raw_data.get('Exchange'),
            'asset_type': _normalize_asset_type(raw_data.get('Type')),
            'currency': raw_data.get('Currency', 'USD'),
        })

    return result

# =============================================================================
# PRICE DATA TRANSFORMATIONS
# =============================================================================

def extract_price_fields(
    raw_data: Dict[str, Any],
    vendor: str,
    symbol: str,
    instrument_id: int
) -> Tuple[str, int, Optional[date], float, float, float, float, int, Optional[float]]:
    """
    Extract standardized price fields from vendor API response.

    Consolidates price field extraction from multiple vendor services.

    Args:
        raw_data: Raw price data from vendor API
        vendor: Vendor name
        symbol: Stock symbol
        instrument_id: Internal instrument ID

    Returns:
        Tuple of (symbol, instrument_id, date, open, high, low, close, volume, vwap)
    """
    trade_date = None
    open_price = high_price = low_price = close_price = 0.0
    volume = 0
    vwap = None

    if vendor == 'polygon':
        trade_date = parse_vendor_date(raw_data.get('t'), vendor)
        open_price = float(raw_data.get('o', 0))
        high_price = float(raw_data.get('h', 0))
        low_price = float(raw_data.get('l', 0))
        close_price = float(raw_data.get('c', 0))
        volume = int(raw_data.get('v', 0))
        vwap = raw_data.get('vw')

    elif vendor == 'eodhd':
        trade_date = parse_vendor_date(raw_data.get('date'), vendor)
        open_price = float(raw_data.get('open', 0))
        high_price = float(raw_data.get('high', 0))
        low_price = float(raw_data.get('low', 0))
        close_price = float(raw_data.get('close', 0))
        volume = int(raw_data.get('volume', 0))

    elif vendor == 'tiingo':
        trade_date = parse_vendor_date(raw_data.get('date'), vendor)
        open_price = float(raw_data.get('open', 0))
        high_price = float(raw_data.get('high', 0))
        low_price = float(raw_data.get('low', 0))
        close_price = float(raw_data.get('close', 0))
        volume = int(raw_data.get('volume', 0))

    return (symbol, instrument_id, trade_date, open_price, high_price,
            low_price, close_price, volume, vwap)

# =============================================================================
# FIELD NORMALIZATION UTILITIES
# =============================================================================

def normalize_field_names(data: Dict[str, Any], vendor: str) -> Dict[str, Any]:
    """
    Normalize field names from vendor-specific to standard format.

    Args:
        data: Raw data dictionary
        vendor: Vendor name

    Returns:
        Dictionary with normalized field names
    """
    field_mappings = {
        'polygon': {
            'T': 'ticker',
            't': 'timestamp',
            'o': 'open',
            'h': 'high',
            'l': 'low',
            'c': 'close',
            'v': 'volume',
            'vw': 'vwap',
        },
        'eodhd': {
            'Code': 'symbol',
            'Name': 'name',
            'Exchange': 'exchange',
            'Currency': 'currency',
        },
        'tiingo': {
            'ticker': 'symbol',
            'exchangeCode': 'exchange',
            'startDate': 'start_date',
            'endDate': 'end_date',
        }
    }

    mapping = field_mappings.get(vendor, {})
    if not mapping:
        return data

    normalized = {}
    for key, value in data.items():
        normalized_key = mapping.get(key, key)
        normalized[normalized_key] = value

    return normalized

# =============================================================================
# PRIVATE HELPER FUNCTIONS
# =============================================================================

def _parse_decimal(val: Any) -> Optional[float]:
    """Parse decimal value safely."""
    if val is None:
        return None

    try:
        if isinstance(val, (int, float)):
            return float(val)
        elif isinstance(val, str):
            val = val.strip()
            if not val:
                return None
            return float(val)
        elif isinstance(val, Decimal):
            return float(val)
    except (ValueError, InvalidOperation):
        return None

    return None

def _normalize_frequency(val: Any) -> Optional[str]:
    """Normalize dividend frequency values."""
    if not val:
        return None

    val_str = str(val).lower().strip()

    frequency_mapping = {
        'quarterly': 'Q',
        'monthly': 'M',
        'annual': 'A',
        'yearly': 'A',
        'semi-annual': 'SA',
        'q': 'Q',
        'm': 'M',
        'a': 'A',
        'y': 'A',
    }

    return frequency_mapping.get(val_str, val_str.upper())

def _normalize_asset_type(val: Any) -> str:
    """Normalize asset type values."""
    if not val:
        return 'stock'

    val_str = str(val).lower().strip()

    type_mapping = {
        'cs': 'stock',
        'common stock': 'stock',
        'equity': 'stock',
        'etf': 'etf',
        'fund': 'fund',
        'index': 'index',
        'warrant': 'warrant',
        'right': 'right',
    }

    return type_mapping.get(val_str, val_str.lower())

# =============================================================================
# BATCH TRANSFORMATION UTILITIES
# =============================================================================

def transform_vendor_data_batch(
    data_list: List[Dict[str, Any]],
    vendor: str,
    data_type: str,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Transform batch of vendor data items.

    Args:
        data_list: List of raw data items from vendor API
        vendor: Vendor name
        data_type: Type of data (dividend, instrument, price)
        **kwargs: Additional arguments for specific transformers

    Returns:
        List of transformed data items
    """
    transformed = []

    for item in data_list:
        try:
            if data_type == 'dividend':
                result = transform_vendor_dividend(item, vendor, **kwargs)
            elif data_type == 'instrument':
                result = transform_vendor_instrument(item, vendor, **kwargs)
            else:
                logger.warning(f"Unknown data type: {data_type}")
                continue

            if result:
                transformed.append(result)

        except Exception as e:
            logger.error(f"Failed to transform {data_type} data for {vendor}: {e}")
            logger.debug(f"Raw data: {item}")

    return transformed

def validate_transformed_data(
    data: Dict[str, Any],
    data_type: str,
    required_fields: Optional[List[str]] = None
) -> bool:
    """
    Validate transformed data has required fields.

    Args:
        data: Transformed data dictionary
        data_type: Type of data for validation rules
        required_fields: Custom required fields list

    Returns:
        True if valid, False otherwise
    """
    if not required_fields:
        if data_type == 'dividend':
            required_fields = ['symbol', 'ex_dividend_date', 'cash_amount']
        elif data_type == 'instrument':
            required_fields = ['symbol', 'exchange']
        elif data_type == 'price':
            required_fields = ['symbol', 'date', 'close']
        else:
            required_fields = ['symbol']

    for field in required_fields:
        if field not in data or data[field] is None:
            logger.warning(f"Missing required field '{field}' in {data_type} data: {data}")
            return False

    return True