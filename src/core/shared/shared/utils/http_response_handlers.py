#!/usr/bin/env python3
"""
HTTP Response Handlers - Unified HTTP response handling for vendor APIs

Consolidates HTTP response handling patterns from 10+ vendor service files.
Provides standardized error handling, retry logic, and response parsing.

USAGE:
======

from core.shared.utils.http_response_handlers import (
    handle_vendor_response,
    should_retry_response,
    parse_json_response
)

# Unified response handling
result = handle_vendor_response(
    response, 
    symbol='AAPL', 
    vendor='polygon',
    retry_callback=lambda: fetch_data_again()
)
"""

import logging
import time
import asyncio
from typing import Any, Dict, List, Optional, Callable, Union
from requests import Response

logger = logging.getLogger(__name__)

# =============================================================================
# HTTP STATUS CODE HANDLERS
# =============================================================================

def handle_vendor_response(
    response: Response,
    symbol: str,
    vendor: str = 'generic',
    retry_callback: Optional[Callable] = None,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Unified HTTP response handler for all vendor APIs.
    
    Consolidates response handling patterns from polygon, eodhd, tiingo services.
    Handles common status codes with vendor-specific logic.
    
    Args:
        response: HTTP response object
        symbol: Stock symbol being processed
        vendor: Vendor name for specific handling
        retry_callback: Function to call for retries
        max_retries: Maximum retry attempts
        
    Returns:
        Dictionary with 'success', 'data', 'error', 'should_retry' keys
        
    Examples:
        >>> result = handle_vendor_response(response, 'AAPL', 'polygon')
        >>> if result['success']:
        ...     data = result['data']
        >>> elif result['should_retry']:
        ...     # Handle retry logic
    """
    result = {
        'success': False,
        'data': None,
        'error': None,
        'should_retry': False,
        'status_code': response.status_code
    }
    
    # Success responses
    if response.status_code == 200:
        try:
            data = response.json()
            
            # Vendor-specific success validation
            if vendor == 'polygon':
                if data.get('status') in ['OK', 'DELAYED']:
                    result['success'] = True
                    result['data'] = data.get('results', [])
                elif data.get('status') == 'ERROR':
                    result['error'] = f"Polygon API error: {data.get('error', 'Unknown error')}"
                    logger.warning(f"⚠️ {result['error']} for {symbol}")
                else:
                    result['error'] = f"Unexpected Polygon response: {data}"
                    logger.warning(f"⚠️ {result['error']} for {symbol}")
                    
            elif vendor == 'eodhd':
                # EODHD returns array directly or error object
                if isinstance(data, list):
                    result['success'] = True
                    result['data'] = data
                elif isinstance(data, dict) and 'error' in data:
                    result['error'] = f"EODHD API error: {data.get('error')}"
                    logger.warning(f"⚠️ {result['error']} for {symbol}")
                else:
                    result['success'] = True
                    result['data'] = data
                    
            elif vendor == 'tiingo':
                # Tiingo returns array directly
                if isinstance(data, list):
                    result['success'] = True
                    result['data'] = data
                elif isinstance(data, dict):
                    if 'error' in data:
                        result['error'] = f"Tiingo API error: {data.get('error')}"
                        logger.warning(f"⚠️ {result['error']} for {symbol}")
                    else:
                        result['success'] = True
                        result['data'] = data
                else:
                    result['success'] = True
                    result['data'] = data
                    
            else:
                # Generic JSON response
                result['success'] = True
                result['data'] = data
                
        except Exception as e:
            result['error'] = f"JSON parsing error: {e}"
            logger.error(f"❌ {result['error']} for {symbol}")
            
    # Rate limiting - vendor specific handling
    elif response.status_code == 429:
        result['should_retry'] = True
        result['error'] = f"Rate limit exceeded for {symbol}"
        
        if vendor == 'polygon':
            # Polygon: wait and retry
            logger.warning(f"⚠️ Polygon rate limit hit for {symbol}, backing off...")
            result['retry_delay'] = 60  # 1 minute
            
        elif vendor == 'eodhd':
            # EODHD: shorter backoff
            logger.warning(f"⚠️ EODHD rate limit hit for {symbol}, backing off...")
            result['retry_delay'] = 60  # 1 minute
            
        elif vendor == 'tiingo':
            # Tiingo: shorter backoff
            logger.warning(f"⚠️ Tiingo rate limit hit for {symbol}, backing off...")
            result['retry_delay'] = 30  # 30 seconds
            
        else:
            result['retry_delay'] = 60  # Generic backoff
            
    # Not found - usually not an error for financial data
    elif response.status_code == 404:
        result['error'] = f"No data available for {symbol}"
        logger.debug(f"⚠️ {result['error']}")
        # Don't retry for 404s - symbol might not exist
        
    # Client errors - don't retry
    elif 400 <= response.status_code < 500:
        result['error'] = f"Client error {response.status_code} for {symbol}: {response.text}"
        logger.error(f"❌ {result['error']}")
        
    # Server errors - might retry
    elif 500 <= response.status_code < 600:
        result['error'] = f"Server error {response.status_code} for {symbol}"
        result['should_retry'] = True
        result['retry_delay'] = 30
        logger.warning(f"⚠️ {result['error']} - will retry")
        
    # Other status codes
    else:
        result['error'] = f"Unexpected status {response.status_code} for {symbol}: {response.text}"
        logger.error(f"❌ {result['error']}")
    
    return result

def should_retry_response(response: Response, vendor: str = 'generic') -> bool:
    """
    Determine if a response should be retried.
    
    Args:
        response: HTTP response object
        vendor: Vendor name for specific logic
        
    Returns:
        True if should retry, False otherwise
    """
    # Always retry rate limits and server errors
    if response.status_code in [429, 502, 503, 504]:
        return True
        
    # Vendor-specific retry logic
    if vendor == 'polygon':
        # Retry on service unavailable
        if response.status_code in [500, 503]:
            return True
            
    return False

def get_retry_delay(response: Response, vendor: str = 'generic', attempt: int = 1) -> int:
    """
    Get appropriate retry delay based on response and vendor.
    
    Args:
        response: HTTP response object
        vendor: Vendor name
        attempt: Current attempt number (for exponential backoff)
        
    Returns:
        Delay in seconds
    """
    base_delay = 30
    
    if response.status_code == 429:  # Rate limiting
        if vendor == 'polygon':
            return 60  # Polygon free tier is strict
        elif vendor == 'eodhd':
            return 60  
        elif vendor == 'tiingo':
            return 30  # Tiingo is more generous
        else:
            return 60
            
    elif 500 <= response.status_code < 600:  # Server errors
        # Exponential backoff for server errors
        return min(base_delay * (2 ** (attempt - 1)), 300)  # Max 5 minutes
        
    return base_delay

# =============================================================================
# ASYNC HTTP HANDLERS
# =============================================================================

async def handle_async_response(
    response_coro: Callable,
    symbol: str,
    vendor: str = 'generic',
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Handle async HTTP responses with automatic retry logic.
    
    Args:
        response_coro: Coroutine that returns HTTP response
        symbol: Stock symbol being processed
        vendor: Vendor name
        max_retries: Maximum retry attempts
        
    Returns:
        Response handling result
    """
    for attempt in range(max_retries + 1):
        try:
            response = await response_coro()
            result = handle_vendor_response(response, symbol, vendor)
            
            if result['success'] or not result['should_retry']:
                return result
                
            if attempt < max_retries:
                delay = result.get('retry_delay', 30)
                logger.info(f"Retrying {symbol} after {delay}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(delay)
                
        except Exception as e:
            if attempt == max_retries:
                return {
                    'success': False,
                    'data': None,
                    'error': f"Request failed after {max_retries} attempts: {e}",
                    'should_retry': False
                }
            else:
                delay = 30 * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Request error for {symbol}, retrying in {delay}s: {e}")
                await asyncio.sleep(delay)
                
    return {
        'success': False,
        'data': None,
        'error': f"Max retries ({max_retries}) exceeded for {symbol}",
        'should_retry': False
    }

# =============================================================================
# JSON RESPONSE UTILITIES
# =============================================================================

def parse_json_response(
    response: Response, 
    symbol: str,
    expected_type: Optional[type] = None
) -> Dict[str, Any]:
    """
    Safely parse JSON response with validation.
    
    Args:
        response: HTTP response object
        symbol: Stock symbol for logging
        expected_type: Expected type of parsed data (list, dict)
        
    Returns:
        Dictionary with 'success', 'data', 'error' keys
    """
    result = {
        'success': False,
        'data': None,
        'error': None
    }
    
    try:
        data = response.json()
        
        if expected_type and not isinstance(data, expected_type):
            result['error'] = f"Expected {expected_type.__name__} but got {type(data).__name__} for {symbol}"
            logger.warning(f"⚠️ {result['error']}")
        else:
            result['success'] = True
            result['data'] = data
            
    except ValueError as e:
        result['error'] = f"JSON decode error for {symbol}: {e}"
        logger.error(f"❌ {result['error']}")
    except Exception as e:
        result['error'] = f"Unexpected error parsing response for {symbol}: {e}"
        logger.error(f"❌ {result['error']}")
        
    return result

def extract_error_message(response: Response, vendor: str = 'generic') -> Optional[str]:
    """
    Extract error message from vendor API response.
    
    Args:
        response: HTTP response object
        vendor: Vendor name for specific parsing
        
    Returns:
        Error message string or None
    """
    try:
        data = response.json()
        
        if vendor == 'polygon':
            return data.get('error') or data.get('message')
        elif vendor == 'eodhd':
            if isinstance(data, dict):
                return data.get('error') or data.get('message')
        elif vendor == 'tiingo':
            if isinstance(data, dict):
                return data.get('error') or data.get('message')
        else:
            # Generic error extraction
            if isinstance(data, dict):
                return data.get('error') or data.get('message') or data.get('detail')
                
    except:
        pass
        
    # Fallback to response text
    return response.text if len(response.text) < 200 else None

# =============================================================================
# RESPONSE STATISTICS AND MONITORING
# =============================================================================

class ResponseStats:
    """Track HTTP response statistics for monitoring."""
    
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.rate_limit_hits = 0
        self.server_errors = 0
        self.client_errors = 0
        self.response_times = []
        
    def record_response(self, response: Response, response_time: float):
        """Record response for statistics."""
        self.total_requests += 1
        self.response_times.append(response_time)
        
        if 200 <= response.status_code < 300:
            self.successful_requests += 1
        elif response.status_code == 429:
            self.rate_limit_hits += 1
            self.failed_requests += 1
        elif 400 <= response.status_code < 500:
            self.client_errors += 1
            self.failed_requests += 1
        elif 500 <= response.status_code < 600:
            self.server_errors += 1
            self.failed_requests += 1
        else:
            self.failed_requests += 1
            
    def get_success_rate(self) -> float:
        """Get success rate as percentage."""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
        
    def get_average_response_time(self) -> float:
        """Get average response time in seconds."""
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)
        
    def log_summary(self, logger_instance: logging.Logger):
        """Log statistics summary."""
        logger_instance.info(f"HTTP Response Statistics:")
        logger_instance.info(f"  Total requests: {self.total_requests}")
        logger_instance.info(f"  Success rate: {self.get_success_rate():.1f}%")
        logger_instance.info(f"  Rate limit hits: {self.rate_limit_hits}")
        logger_instance.info(f"  Server errors: {self.server_errors}")
        logger_instance.info(f"  Client errors: {self.client_errors}")
        logger_instance.info(f"  Average response time: {self.get_average_response_time():.2f}s")

# Global response statistics instance
response_stats = ResponseStats()