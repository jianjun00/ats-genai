#!/usr/bin/env python3
"""
Validation Utils - Unified validation utilities for data quality and business rules

Consolidates validation patterns from vendor services, data processing, and API endpoints.
Provides standardized validation for symbols, dates, data quality, and business logic.

USAGE:
======

from shared.utils.validation_utils import (
    validate_stock_symbol,
    validate_date_range,
    validate_dividend_data,
    validate_price_data,
    ValidationResult
)

# Validate stock symbol
if validate_stock_symbol('AAPL'):
    # Process valid symbol

# Validate data completeness
result = validate_dividend_data(dividend_record)
if result.is_valid:
    # Process valid dividend data
else:
    logger.error(f"Validation failed: {result.errors}")
"""

import re
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

# =============================================================================
# VALIDATION RESULT CLASSES
# =============================================================================

@dataclass
class ValidationResult:
    """Result of validation with details about success/failure."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
    
    def add_error(self, error: str):
        """Add validation error."""
        self.errors.append(error)
        self.is_valid = False
        
    def add_warning(self, warning: str):
        """Add validation warning."""
        self.warnings.append(warning)
    
    def has_errors(self) -> bool:
        """Check if validation has errors."""
        return len(self.errors) > 0
    
    def has_warnings(self) -> bool:
        """Check if validation has warnings."""
        return len(self.warnings) > 0
        
    def log_results(self, logger_instance: logging.Logger, context: str = ""):
        """Log validation results."""
        prefix = f"{context}: " if context else ""
        
        if self.has_errors():
            for error in self.errors:
                logger_instance.error(f"{prefix}Validation Error - {error}")
                
        if self.has_warnings():
            for warning in self.warnings:
                logger_instance.warning(f"{prefix}Validation Warning - {warning}")

# =============================================================================
# SYMBOL VALIDATION
# =============================================================================

def validate_stock_symbol(symbol: str, allow_extensions: bool = True) -> bool:
    """
    Validate stock symbol format.
    
    Consolidates symbol validation logic from multiple vendor services.
    
    Args:
        symbol: Stock symbol to validate
        allow_extensions: Allow symbols with extensions like .TO, .L
        
    Returns:
        True if valid symbol format
        
    Examples:
        >>> validate_stock_symbol("AAPL")
        True
        >>> validate_stock_symbol("BRK.A")
        True
        >>> validate_stock_symbol("INVALID.SYMBOL.123")
        False
    """
    if not symbol or not isinstance(symbol, str):
        return False
        
    symbol = symbol.strip().upper()
    
    if not symbol:
        return False
        
    # Basic pattern: 1-8 alphanumeric characters
    basic_pattern = r'^[A-Z0-9]{1,8}$'
    
    if allow_extensions:
        # Allow extensions like .A, .B, .TO, .L, etc.
        extended_pattern = r'^[A-Z0-9]{1,8}(\.[A-Z0-9]{1,4})?$'
        return bool(re.match(extended_pattern, symbol))
    else:
        return bool(re.match(basic_pattern, symbol))

def validate_symbol_list(
    symbols: List[str], 
    max_symbols: int = 1000,
    allow_duplicates: bool = False
) -> ValidationResult:
    """
    Validate list of stock symbols.
    
    Args:
        symbols: List of symbols to validate
        max_symbols: Maximum allowed symbols
        allow_duplicates: Allow duplicate symbols in list
        
    Returns:
        ValidationResult with details
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    
    if not symbols:
        result.add_error("Symbol list is empty")
        return result
        
    if not isinstance(symbols, list):
        result.add_error("Symbols must be a list")
        return result
        
    if len(symbols) > max_symbols:
        result.add_error(f"Too many symbols: {len(symbols)} > {max_symbols}")
        
    # Check for duplicates
    if not allow_duplicates:
        unique_symbols = set()
        duplicates = set()
        for symbol in symbols:
            if symbol in unique_symbols:
                duplicates.add(symbol)
            else:
                unique_symbols.add(symbol)
                
        if duplicates:
            result.add_warning(f"Duplicate symbols found: {sorted(duplicates)}")
    
    # Validate individual symbols
    invalid_symbols = []
    for symbol in symbols:
        if not validate_stock_symbol(symbol):
            invalid_symbols.append(symbol)
            
    if invalid_symbols:
        result.add_error(f"Invalid symbols: {invalid_symbols}")
        
    return result

# =============================================================================
# DATE VALIDATION
# =============================================================================

def validate_date_range(
    start_date: Union[str, date, datetime],
    end_date: Union[str, date, datetime],
    max_range_days: Optional[int] = None,
    allow_future: bool = False
) -> ValidationResult:
    """
    Validate date range for API requests.
    
    Consolidates date range validation from backfill and vendor services.
    
    Args:
        start_date: Start date
        end_date: End date  
        max_range_days: Maximum allowed days in range
        allow_future: Allow future dates
        
    Returns:
        ValidationResult with details
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    
    # Convert to date objects
    try:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()
            
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        elif isinstance(end_date, datetime):
            end_date = end_date.date()
            
    except ValueError as e:
        result.add_error(f"Invalid date format: {e}")
        return result
    
    # Validate date range
    if start_date > end_date:
        result.add_error(f"Start date {start_date} is after end date {end_date}")
        
    # Check future dates
    today = date.today()
    if not allow_future:
        if start_date > today:
            result.add_error(f"Start date {start_date} is in the future")
        if end_date > today:
            result.add_error(f"End date {end_date} is in the future")
    
    # Check maximum range
    if max_range_days and result.is_valid:
        range_days = (end_date - start_date).days
        if range_days > max_range_days:
            result.add_error(f"Date range too large: {range_days} days > {max_range_days} days")
            
    # Business day warnings
    if result.is_valid:
        weekends_in_range = 0
        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
                weekends_in_range += 1
            current_date += timedelta(days=1)
            
        if weekends_in_range > 0:
            result.add_warning(f"Date range includes {weekends_in_range} weekend days")
    
    return result

def validate_trading_date(check_date: Union[str, date, datetime]) -> ValidationResult:
    """
    Validate if date is a valid trading date.
    
    Args:
        check_date: Date to validate
        
    Returns:
        ValidationResult with details
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    
    # Convert to date object
    try:
        if isinstance(check_date, str):
            check_date = datetime.strptime(check_date, "%Y-%m-%d").date()
        elif isinstance(check_date, datetime):
            check_date = check_date.date()
    except ValueError as e:
        result.add_error(f"Invalid date format: {e}")
        return result
    
    # Check if weekend
    if check_date.weekday() >= 5:
        result.add_warning(f"Date {check_date} is a weekend")
        
    # Check if too far in past (before modern markets)
    min_date = date(1970, 1, 1)
    if check_date < min_date:
        result.add_error(f"Date {check_date} is before minimum date {min_date}")
        
    # Check if future date
    today = date.today()
    if check_date > today:
        result.add_warning(f"Date {check_date} is in the future")
        
    return result

# =============================================================================
# FINANCIAL DATA VALIDATION
# =============================================================================

def validate_dividend_data(dividend: Dict[str, Any]) -> ValidationResult:
    """
    Validate dividend data structure and values.
    
    Consolidates dividend validation from map_tiingo_dividend() and similar functions.
    
    Args:
        dividend: Dividend data dictionary
        
    Returns:
        ValidationResult with details
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    
    # Required fields
    required_fields = ['symbol', 'ex_dividend_date', 'cash_amount']
    for field in required_fields:
        if field not in dividend or dividend[field] is None:
            result.add_error(f"Missing required field: {field}")
    
    # Validate symbol
    if 'symbol' in dividend and dividend['symbol']:
        if not validate_stock_symbol(dividend['symbol']):
            result.add_error(f"Invalid symbol format: {dividend['symbol']}")
    
    # Validate cash amount
    if 'cash_amount' in dividend and dividend['cash_amount'] is not None:
        try:
            amount = float(dividend['cash_amount'])
            if amount < 0:
                result.add_error(f"Negative dividend amount: {amount}")
            elif amount == 0:
                result.add_warning("Zero dividend amount")
            elif amount > 1000:  # Unusually large dividend
                result.add_warning(f"Unusually large dividend amount: {amount}")
        except (ValueError, TypeError):
            result.add_error(f"Invalid cash amount format: {dividend['cash_amount']}")
    
    # Validate dates
    date_fields = ['ex_dividend_date', 'declaration_date', 'payment_date', 'record_date']
    for field in date_fields:
        if field in dividend and dividend[field]:
            date_result = validate_trading_date(dividend[field])
            if date_result.has_errors():
                result.add_error(f"Invalid {field}: {date_result.errors[0]}")
    
    # Business logic validations
    if result.is_valid:
        ex_date = dividend.get('ex_dividend_date')
        payment_date = dividend.get('payment_date')
        
        if ex_date and payment_date:
            if payment_date < ex_date:
                result.add_warning("Payment date is before ex-dividend date")
    
    return result

def validate_price_data(price: Dict[str, Any]) -> ValidationResult:
    """
    Validate price data structure and values.
    
    Args:
        price: Price data dictionary
        
    Returns:
        ValidationResult with details
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    
    # Required fields
    required_fields = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
    for field in required_fields:
        if field not in price or price[field] is None:
            result.add_error(f"Missing required field: {field}")
    
    # Validate symbol
    if 'symbol' in price and price['symbol']:
        if not validate_stock_symbol(price['symbol']):
            result.add_error(f"Invalid symbol format: {price['symbol']}")
    
    # Validate date
    if 'date' in price and price['date']:
        date_result = validate_trading_date(price['date'])
        if date_result.has_errors():
            result.add_error(f"Invalid date: {date_result.errors[0]}")
    
    # Validate OHLC values
    ohlc_fields = ['open', 'high', 'low', 'close']
    ohlc_values = {}
    
    for field in ohlc_fields:
        if field in price and price[field] is not None:
            try:
                value = float(price[field])
                if value <= 0:
                    result.add_error(f"Non-positive {field} price: {value}")
                elif value > 100000:  # Unusually high price
                    result.add_warning(f"Unusually high {field} price: {value}")
                ohlc_values[field] = value
            except (ValueError, TypeError):
                result.add_error(f"Invalid {field} price format: {price[field]}")
    
    # Validate OHLC relationships
    if len(ohlc_values) == 4:  # All OHLC values present
        o, h, l, c = ohlc_values['open'], ohlc_values['high'], ohlc_values['low'], ohlc_values['close']
        
        if h < max(o, c):
            result.add_error(f"High ({h}) is less than max of open ({o}) and close ({c})")
        if l > min(o, c):
            result.add_error(f"Low ({l}) is greater than min of open ({o}) and close ({c})")
        if h < l:
            result.add_error(f"High ({h}) is less than low ({l})")
    
    # Validate volume
    if 'volume' in price and price['volume'] is not None:
        try:
            volume = int(price['volume'])
            if volume < 0:
                result.add_error(f"Negative volume: {volume}")
            elif volume == 0:
                result.add_warning("Zero volume")
        except (ValueError, TypeError):
            result.add_error(f"Invalid volume format: {price['volume']}")
    
    return result

def validate_instrument_data(instrument: Dict[str, Any]) -> ValidationResult:
    """
    Validate instrument data structure and values.
    
    Args:
        instrument: Instrument data dictionary
        
    Returns:
        ValidationResult with details
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    
    # Required fields
    required_fields = ['symbol', 'exchange']
    for field in required_fields:
        if field not in instrument or not instrument[field]:
            result.add_error(f"Missing required field: {field}")
    
    # Validate symbol
    if 'symbol' in instrument and instrument['symbol']:
        if not validate_stock_symbol(instrument['symbol']):
            result.add_error(f"Invalid symbol format: {instrument['symbol']}")
    
    # Validate exchange
    if 'exchange' in instrument and instrument['exchange']:
        valid_exchanges = ['NYSE', 'NASDAQ', 'AMEX', 'BATS', 'IEX', 'OTC']
        if instrument['exchange'] not in valid_exchanges:
            result.add_warning(f"Unknown exchange: {instrument['exchange']}")
    
    # Validate asset type
    if 'asset_type' in instrument and instrument['asset_type']:
        valid_types = ['stock', 'etf', 'fund', 'index', 'warrant', 'right']
        if instrument['asset_type'] not in valid_types:
            result.add_warning(f"Unknown asset type: {instrument['asset_type']}")
    
    # Validate dates
    if 'start_date' in instrument and 'end_date' in instrument:
        if instrument['start_date'] and instrument['end_date']:
            if instrument['start_date'] > instrument['end_date']:
                result.add_error("Start date is after end date")
    
    return result

# =============================================================================
# API RESPONSE VALIDATION
# =============================================================================

def validate_api_response(
    response_data: Any,
    expected_type: type,
    required_fields: Optional[List[str]] = None
) -> ValidationResult:
    """
    Validate API response structure and content.
    
    Args:
        response_data: Response data from API
        expected_type: Expected data type (dict, list)
        required_fields: Required fields if dict response
        
    Returns:
        ValidationResult with details
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    
    # Check type
    if not isinstance(response_data, expected_type):
        result.add_error(f"Expected {expected_type.__name__}, got {type(response_data).__name__}")
        return result
    
    # Check required fields for dict responses
    if expected_type == dict and required_fields:
        for field in required_fields:
            if field not in response_data:
                result.add_error(f"Missing required field: {field}")
    
    # Check for empty responses
    if expected_type == list and len(response_data) == 0:
        result.add_warning("Empty list response")
    elif expected_type == dict and len(response_data) == 0:
        result.add_warning("Empty dict response")
    
    return result

def validate_batch_data(
    data_list: List[Dict[str, Any]],
    validator_func: callable,
    max_errors: int = 10
) -> Tuple[ValidationResult, List[Dict[str, Any]]]:
    """
    Validate batch of data items.
    
    Args:
        data_list: List of data items to validate
        validator_func: Validation function for individual items
        max_errors: Maximum errors before stopping validation
        
    Returns:
        Tuple of (ValidationResult, list of valid items)
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    valid_items = []
    error_count = 0
    
    for i, item in enumerate(data_list):
        try:
            item_result = validator_func(item)
            
            if item_result.is_valid:
                valid_items.append(item)
            else:
                error_count += 1
                for error in item_result.errors:
                    result.add_error(f"Item {i}: {error}")
                    
                if error_count >= max_errors:
                    result.add_error(f"Stopped validation after {max_errors} errors")
                    break
                    
            # Aggregate warnings
            for warning in item_result.warnings:
                result.add_warning(f"Item {i}: {warning}")
                
        except Exception as e:
            error_count += 1
            result.add_error(f"Item {i}: Validation exception - {e}")
            
            if error_count >= max_errors:
                break
    
    # Summary
    total_items = len(data_list)
    valid_count = len(valid_items)
    invalid_count = total_items - valid_count
    
    if invalid_count > 0:
        result.add_warning(f"Validation summary: {valid_count}/{total_items} items valid, {invalid_count} invalid")
    
    return result, valid_items

# =============================================================================
# DATA QUALITY VALIDATION
# =============================================================================

def validate_data_completeness(
    data: Dict[str, Any],
    required_fields: List[str],
    optional_fields: Optional[List[str]] = None
) -> ValidationResult:
    """
    Validate data completeness and quality.
    
    Args:
        data: Data dictionary to validate
        required_fields: Fields that must be present and non-null
        optional_fields: Fields that should be present but can be null
        
    Returns:
        ValidationResult with details
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    
    # Check required fields
    for field in required_fields:
        if field not in data:
            result.add_error(f"Missing required field: {field}")
        elif data[field] is None:
            result.add_error(f"Required field is null: {field}")
        elif isinstance(data[field], str) and not data[field].strip():
            result.add_error(f"Required field is empty: {field}")
    
    # Check optional fields
    if optional_fields:
        for field in optional_fields:
            if field not in data:
                result.add_warning(f"Missing optional field: {field}")
    
    return result

def validate_numeric_ranges(
    data: Dict[str, Any],
    field_ranges: Dict[str, Tuple[Optional[float], Optional[float]]]
) -> ValidationResult:
    """
    Validate numeric fields are within expected ranges.
    
    Args:
        data: Data dictionary to validate
        field_ranges: Dict of {field_name: (min_value, max_value)}
        
    Returns:
        ValidationResult with details
    """
    result = ValidationResult(is_valid=True, errors=[], warnings=[])
    
    for field, (min_val, max_val) in field_ranges.items():
        if field in data and data[field] is not None:
            try:
                value = float(data[field])
                
                if min_val is not None and value < min_val:
                    result.add_error(f"{field} ({value}) is below minimum ({min_val})")
                    
                if max_val is not None and value > max_val:
                    result.add_error(f"{field} ({value}) is above maximum ({max_val})")
                    
            except (ValueError, TypeError):
                result.add_error(f"Invalid numeric value for {field}: {data[field]}")
    
    return result