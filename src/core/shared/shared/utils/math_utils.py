#!/usr/bin/env python3
"""
Math Utils - Mathematical and statistical utilities for financial data processing

Consolidates mathematical calculations from vendor services, analytics, and data processing.
Provides standardized financial calculations, statistical analysis, and numerical operations.

USAGE:
======

from src.core.shared.utils.math_utils import (
    calculate_returns,
    calculate_vwap,
    calculate_moving_average,
    calculate_volatility,
    calculate_percentage_change
)

# Calculate price returns
returns = calculate_returns(prices)

# Calculate Volume Weighted Average Price
vwap = calculate_vwap(prices, volumes)

# Calculate moving average
ma = calculate_moving_average(values, window=20)

# Calculate volatility
vol = calculate_volatility(returns, window=30)
"""

import math
import statistics
import logging
from typing import List, Optional, Union, Tuple, Dict, Any
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import numpy as np

logger = logging.getLogger(__name__)

# =============================================================================
# BASIC MATHEMATICAL OPERATIONS
# =============================================================================

def safe_divide(numerator: Union[int, float, Decimal], 
               denominator: Union[int, float, Decimal],
               default: Optional[Union[int, float]] = None) -> Optional[Union[float, int]]:
    """
    Safely divide two numbers, handling zero division.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        default: Default value if division by zero
        
    Returns:
        Division result or default value
        
    Examples:
        >>> safe_divide(10, 2)
        5.0
        >>> safe_divide(10, 0, default=0)
        0
        >>> safe_divide(10, 0)
        None
    """
    try:
        if denominator == 0:
            return default
        return float(numerator) / float(denominator)
    except (TypeError, ValueError, InvalidOperation):
        return default

def safe_percentage(part: Union[int, float], 
                   whole: Union[int, float],
                   precision: int = 2) -> Optional[float]:
    """
    Safely calculate percentage, handling zero values.
    
    Args:
        part: Part value
        whole: Whole value
        precision: Decimal places for rounding
        
    Returns:
        Percentage value or None
    """
    result = safe_divide(part, whole)
    if result is not None:
        return round(result * 100, precision)
    return None

def round_to_precision(value: Union[int, float, Decimal], 
                      precision: int = 2) -> Optional[float]:
    """
    Round value to specified precision, handling invalid inputs.
    
    Args:
        value: Value to round
        precision: Number of decimal places
        
    Returns:
        Rounded value or None
    """
    try:
        if isinstance(value, Decimal):
            quantizer = Decimal('0.1') ** precision
            return float(value.quantize(quantizer, rounding=ROUND_HALF_UP))
        else:
            return round(float(value), precision)
    except (TypeError, ValueError, InvalidOperation):
        return None

def clamp(value: Union[int, float], 
          min_val: Union[int, float], 
          max_val: Union[int, float]) -> Union[int, float]:
    """
    Clamp value between minimum and maximum bounds.
    
    Args:
        value: Value to clamp
        min_val: Minimum value
        max_val: Maximum value
        
    Returns:
        Clamped value
    """
    return max(min_val, min(value, max_val))

# =============================================================================
# FINANCIAL CALCULATIONS
# =============================================================================

def calculate_returns(prices: List[float], 
                     method: str = 'simple') -> List[float]:
    """
    Calculate price returns from price series.
    
    Args:
        prices: List of price values
        method: Return calculation method ('simple' or 'log')
        
    Returns:
        List of return values
        
    Examples:
        >>> calculate_returns([100, 105, 102])
        [0.05, -0.0286]
    """
    if len(prices) < 2:
        return []
    
    returns = []
    
    for i in range(1, len(prices)):
        if prices[i-1] == 0:
            returns.append(0.0)
            continue
            
        if method == 'simple':
            ret = (prices[i] - prices[i-1]) / prices[i-1]
        elif method == 'log':
            ret = math.log(prices[i] / prices[i-1]) if prices[i] > 0 else 0.0
        else:
            raise ValueError(f"Unknown return method: {method}")
            
        returns.append(ret)
    
    return returns

def calculate_percentage_change(old_value: float, 
                              new_value: float,
                              precision: int = 4) -> Optional[float]:
    """
    Calculate percentage change between two values.
    
    Args:
        old_value: Original value
        new_value: New value
        precision: Decimal places for result
        
    Returns:
        Percentage change or None
    """
    if old_value == 0:
        return None if new_value == 0 else float('inf')
    
    change = (new_value - old_value) / old_value
    return round(change * 100, precision)

def calculate_vwap(prices: List[float], 
                  volumes: List[int],
                  high: Optional[List[float]] = None,
                  low: Optional[List[float]] = None) -> Optional[float]:
    """
    Calculate Volume Weighted Average Price (VWAP).
    
    Args:
        prices: List of prices (typically close prices)
        volumes: List of volumes
        high: List of high prices (for HLC/3 calculation)
        low: List of low prices (for HLC/3 calculation)
        
    Returns:
        VWAP value or None
    """
    if not prices or not volumes or len(prices) != len(volumes):
        return None
        
    if len(prices) == 0:
        return None
    
    # Use HLC/3 if high and low prices available
    if high and low and len(high) == len(prices) and len(low) == len(prices):
        typical_prices = [(h + l + c) / 3 for h, l, c in zip(high, low, prices)]
    else:
        typical_prices = prices
    
    weighted_sum = sum(p * v for p, v in zip(typical_prices, volumes))
    total_volume = sum(volumes)
    
    return safe_divide(weighted_sum, total_volume)

def calculate_moving_average(values: List[float], 
                           window: int,
                           method: str = 'simple') -> List[Optional[float]]:
    """
    Calculate moving average of values.
    
    Args:
        values: List of values
        window: Moving average window size
        method: Averaging method ('simple', 'exponential')
        
    Returns:
        List of moving average values (None for insufficient data points)
    """
    if window <= 0 or len(values) < window:
        return [None] * len(values)
    
    if method == 'simple':
        return _calculate_sma(values, window)
    elif method == 'exponential':
        return _calculate_ema(values, window)
    else:
        raise ValueError(f"Unknown moving average method: {method}")

def _calculate_sma(values: List[float], window: int) -> List[Optional[float]]:
    """Calculate Simple Moving Average."""
    sma = [None] * (window - 1)  # Not enough data for first window-1 points
    
    for i in range(window - 1, len(values)):
        window_values = values[i - window + 1:i + 1]
        sma.append(statistics.mean(window_values))
    
    return sma

def _calculate_ema(values: List[float], window: int) -> List[Optional[float]]:
    """Calculate Exponential Moving Average."""
    if not values:
        return []
        
    alpha = 2 / (window + 1)
    ema = [None] * (window - 1)
    
    # Initialize with SMA for first value
    if len(values) >= window:
        ema.append(statistics.mean(values[:window]))
        
        # Calculate EMA for remaining values
        for i in range(window, len(values)):
            prev_ema = ema[-1]
            current_ema = alpha * values[i] + (1 - alpha) * prev_ema
            ema.append(current_ema)
    
    return ema

def calculate_volatility(returns: List[float], 
                        window: Optional[int] = None,
                        annualized: bool = False,
                        trading_days: int = 252) -> Optional[float]:
    """
    Calculate volatility (standard deviation of returns).
    
    Args:
        returns: List of return values
        window: Rolling window size (None for all returns)
        annualized: Whether to annualize the volatility
        trading_days: Trading days per year for annualization
        
    Returns:
        Volatility value or None
    """
    if not returns:
        return None
        
    if window:
        if len(returns) < window:
            return None
        returns = returns[-window:]  # Use last 'window' returns
    
    try:
        volatility = statistics.stdev(returns)
        
        if annualized:
            volatility *= math.sqrt(trading_days)
            
        return volatility
        
    except statistics.StatisticsError:
        return None

def calculate_sharpe_ratio(returns: List[float],
                          risk_free_rate: float = 0.02,
                          trading_days: int = 252) -> Optional[float]:
    """
    Calculate Sharpe ratio for returns.
    
    Args:
        returns: List of return values
        risk_free_rate: Risk-free rate (annualized)
        trading_days: Trading days per year
        
    Returns:
        Sharpe ratio or None
    """
    if not returns:
        return None
        
    try:
        avg_return = statistics.mean(returns)
        volatility = statistics.stdev(returns)
        
        if volatility == 0:
            return None
            
        # Annualize returns and volatility
        annualized_return = avg_return * trading_days
        annualized_volatility = volatility * math.sqrt(trading_days)
        
        return (annualized_return - risk_free_rate) / annualized_volatility
        
    except statistics.StatisticsError:
        return None

# =============================================================================
# STATISTICAL CALCULATIONS
# =============================================================================

def calculate_statistics(values: List[Union[int, float]]) -> Dict[str, Optional[float]]:
    """
    Calculate comprehensive statistics for a list of values.
    
    Args:
        values: List of numeric values
        
    Returns:
        Dictionary with statistical measures
    """
    if not values:
        return {
            'count': 0,
            'mean': None,
            'median': None,
            'std_dev': None,
            'variance': None,
            'min': None,
            'max': None,
            'range': None,
            'q25': None,
            'q75': None,
            'iqr': None
        }
    
    try:
        # Convert to float to handle mixed types
        float_values = [float(v) for v in values if v is not None]
        
        if not float_values:
            return {'count': 0, 'mean': None, 'median': None, 'std_dev': None, 
                   'variance': None, 'min': None, 'max': None, 'range': None,
                   'q25': None, 'q75': None, 'iqr': None}
        
        # Basic statistics
        mean = statistics.mean(float_values)
        median = statistics.median(float_values)
        min_val = min(float_values)
        max_val = max(float_values)
        
        # Variance and standard deviation
        variance = statistics.variance(float_values) if len(float_values) > 1 else 0
        std_dev = statistics.stdev(float_values) if len(float_values) > 1 else 0
        
        # Quartiles
        q25 = statistics.quantiles(float_values, n=4)[0] if len(float_values) >= 4 else None
        q75 = statistics.quantiles(float_values, n=4)[2] if len(float_values) >= 4 else None
        iqr = (q75 - q25) if q25 and q75 else None
        
        return {
            'count': len(float_values),
            'mean': round(mean, 6),
            'median': round(median, 6),
            'std_dev': round(std_dev, 6),
            'variance': round(variance, 6),
            'min': round(min_val, 6),
            'max': round(max_val, 6),
            'range': round(max_val - min_val, 6),
            'q25': round(q25, 6) if q25 else None,
            'q75': round(q75, 6) if q75 else None,
            'iqr': round(iqr, 6) if iqr else None
        }
        
    except (ValueError, statistics.StatisticsError) as e:
        logger.error(f"Error calculating statistics: {e}")
        return {'count': 0, 'mean': None, 'median': None, 'std_dev': None,
                'variance': None, 'min': None, 'max': None, 'range': None,
                'q25': None, 'q75': None, 'iqr': None}

def calculate_correlation(x_values: List[float], 
                         y_values: List[float]) -> Optional[float]:
    """
    Calculate Pearson correlation coefficient between two series.
    
    Args:
        x_values: First series
        y_values: Second series
        
    Returns:
        Correlation coefficient or None
    """
    if not x_values or not y_values or len(x_values) != len(y_values):
        return None
        
    if len(x_values) < 2:
        return None
    
    try:
        # Remove pairs with None values
        pairs = [(x, y) for x, y in zip(x_values, y_values) if x is not None and y is not None]
        
        if len(pairs) < 2:
            return None
            
        x_clean, y_clean = zip(*pairs)
        return statistics.correlation(x_clean, y_clean)
        
    except statistics.StatisticsError:
        return None

def detect_outliers(values: List[float], 
                   method: str = 'iqr',
                   threshold: float = 1.5) -> List[int]:
    """
    Detect outliers in a list of values.
    
    Args:
        values: List of values to check
        method: Detection method ('iqr' or 'zscore')
        threshold: Threshold for outlier detection
        
    Returns:
        List of indices of outlier values
    """
    if not values or len(values) < 4:
        return []
    
    outlier_indices = []
    
    if method == 'iqr':
        try:
            q25, q75 = statistics.quantiles(values, n=4)[0], statistics.quantiles(values, n=4)[2]
            iqr = q75 - q25
            lower_bound = q25 - threshold * iqr
            upper_bound = q75 + threshold * iqr
            
            for i, value in enumerate(values):
                if value < lower_bound or value > upper_bound:
                    outlier_indices.append(i)
                    
        except statistics.StatisticsError:
            pass
            
    elif method == 'zscore':
        try:
            mean = statistics.mean(values)
            std_dev = statistics.stdev(values)
            
            if std_dev == 0:
                return []
                
            for i, value in enumerate(values):
                z_score = abs((value - mean) / std_dev)
                if z_score > threshold:
                    outlier_indices.append(i)
                    
        except statistics.StatisticsError:
            pass
    
    return outlier_indices

# =============================================================================
# NUMERICAL ARRAY OPERATIONS
# =============================================================================

def normalize_values(values: List[float], 
                    method: str = 'minmax',
                    feature_range: Tuple[float, float] = (0.0, 1.0)) -> List[float]:
    """
    Normalize values using specified method.
    
    Args:
        values: List of values to normalize
        method: Normalization method ('minmax', 'zscore')
        feature_range: Target range for minmax scaling
        
    Returns:
        List of normalized values
    """
    if not values:
        return []
        
    if method == 'minmax':
        min_val = min(values)
        max_val = max(values)
        
        if min_val == max_val:
            return [feature_range[0]] * len(values)
            
        range_val = max_val - min_val
        min_target, max_target = feature_range
        target_range = max_target - min_target
        
        return [min_target + ((v - min_val) / range_val) * target_range for v in values]
        
    elif method == 'zscore':
        try:
            mean = statistics.mean(values)
            std_dev = statistics.stdev(values)
            
            if std_dev == 0:
                return [0.0] * len(values)
                
            return [(v - mean) / std_dev for v in values]
            
        except statistics.StatisticsError:
            return [0.0] * len(values)
    
    else:
        raise ValueError(f"Unknown normalization method: {method}")

def interpolate_missing_values(values: List[Optional[float]], 
                              method: str = 'linear') -> List[float]:
    """
    Interpolate missing values in a series.
    
    Args:
        values: List with potential None values
        method: Interpolation method ('linear', 'forward_fill', 'backward_fill')
        
    Returns:
        List with interpolated values
    """
    if not values:
        return []
        
    result = values.copy()
    
    if method == 'forward_fill':
        last_valid = None
        for i in range(len(result)):
            if result[i] is not None:
                last_valid = result[i]
            elif last_valid is not None:
                result[i] = last_valid
                
    elif method == 'backward_fill':
        next_valid = None
        for i in range(len(result) - 1, -1, -1):
            if result[i] is not None:
                next_valid = result[i]
            elif next_valid is not None:
                result[i] = next_valid
                
    elif method == 'linear':
        # Simple linear interpolation
        for i in range(len(result)):
            if result[i] is None:
                # Find previous and next valid values
                prev_idx = prev_val = next_idx = next_val = None
                
                for j in range(i - 1, -1, -1):
                    if result[j] is not None:
                        prev_idx, prev_val = j, result[j]
                        break
                        
                for j in range(i + 1, len(result)):
                    if result[j] is not None:
                        next_idx, next_val = j, result[j]
                        break
                
                # Interpolate if both boundaries found
                if prev_val is not None and next_val is not None:
                    weight = (i - prev_idx) / (next_idx - prev_idx)
                    result[i] = prev_val + weight * (next_val - prev_val)
                elif prev_val is not None:
                    result[i] = prev_val  # Forward fill
                elif next_val is not None:
                    result[i] = next_val  # Backward fill
    
    # Convert None to 0.0 for any remaining missing values
    return [v if v is not None else 0.0 for v in result]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def is_numeric(value: Any) -> bool:
    """Check if value is numeric."""
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False

def safe_log(value: Union[int, float], base: Optional[float] = None) -> Optional[float]:
    """Safely calculate logarithm, handling non-positive values."""
    try:
        if value <= 0:
            return None
        if base is None:
            return math.log(value)
        else:
            return math.log(value, base)
    except (TypeError, ValueError):
        return None

def compound_return(returns: List[float]) -> Optional[float]:
    """Calculate compound return from a series of returns."""
    if not returns:
        return None
        
    try:
        product = 1.0
        for ret in returns:
            product *= (1 + ret)
        return product - 1
    except (TypeError, ValueError):
        return None

def annualize_return(total_return: float, 
                    periods: int, 
                    periods_per_year: int = 252) -> float:
    """
    Annualize a total return.
    
    Args:
        total_return: Total return over the period
        periods: Number of periods
        periods_per_year: Periods per year (252 for daily returns)
        
    Returns:
        Annualized return
    """
    if periods == 0:
        return 0.0
        
    return (1 + total_return) ** (periods_per_year / periods) - 1