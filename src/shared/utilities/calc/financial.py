"""
Financial calculation utilities.

Common calculations for returns, volatility, technical analysis, etc.
"""

import numpy as np
import pandas as pd
from typing import Dict, Union


def calculate_returns(
    prices: pd.Series,
    method: str = "simple",
    periods: int = 1
) -> pd.Series:
    """
    Calculate returns from price series.
    
    Args:
        prices: Price series
        method: Return calculation method ('simple', 'log')
        periods: Number of periods for return calculation
    
    Returns:
        Returns series
    """
    if method == "simple":
        returns = prices.pct_change(periods=periods)
    elif method == "log":
        returns = np.log(prices / prices.shift(periods))
    else:
        raise ValueError(f"Unknown return method: {method}")
    
    return returns


def calculate_volatility(
    returns: pd.Series,
    window: int = 252,
    annualize: bool = True
) -> Union[float, pd.Series]:
    """
    Calculate volatility from returns.
    
    Args:
        returns: Returns series
        window: Rolling window for calculation
        annualize: Whether to annualize volatility
    
    Returns:
        Volatility (scalar or series)
    """
    if len(returns) < window:
        vol = returns.std()
    else:
        vol = returns.rolling(window=window).std()
    
    if annualize:
        vol *= np.sqrt(252)  # Annualize assuming 252 trading days
    
    return vol


def calculate_technical_levels(
    data: pd.DataFrame,
    lookback_periods: int = 52
) -> Dict[str, float]:
    """
    Calculate key technical levels (support, resistance, etc.).
    
    Args:
        data: Price data
        lookback_periods: Number of periods to look back
    
    Returns:
        Dictionary of technical levels
    """
    if len(data) < lookback_periods:
        recent_data = data
    else:
        recent_data = data.tail(lookback_periods)
    
    levels = {}
    
    if "high" in recent_data.columns:
        levels["resistance"] = recent_data["high"].max()
        levels["resistance_52w"] = recent_data["high"].max()
    
    if "low" in recent_data.columns:
        levels["support"] = recent_data["low"].min()
        levels["support_52w"] = recent_data["low"].min()
    
    if "close" in recent_data.columns:
        closes = recent_data["close"]
        levels["current_price"] = closes.iloc[-1]
        levels["average_price"] = closes.mean()
        levels["median_price"] = closes.median()
    
    # Calculate pivot points if OHLC available
    if all(col in recent_data.columns for col in ["open", "high", "low", "close"]):
        last_row = recent_data.iloc[-1]
        pivot = (last_row["high"] + last_row["low"] + last_row["close"]) / 3
        levels["pivot_point"] = pivot
        levels["resistance_1"] = 2 * pivot - last_row["low"]
        levels["support_1"] = 2 * pivot - last_row["high"]
    
    return levels


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default if denominator is zero."""
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ZeroDivisionError):
        return default