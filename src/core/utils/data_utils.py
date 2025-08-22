"""
Data manipulation utilities for ATS-GenAI.

This module provides common data processing functions used across
the trading system for data cleaning, transformation, and analysis.
"""

import numpy as np
import pandas as pd
from typing import Any, Dict, List, Optional, Union, Tuple
from decimal import Decimal, ROUND_HALF_UP
import warnings

from core.validation.data_validators import ValidationResult


def clean_numeric_data(
    data: Union[pd.Series, pd.DataFrame],
    fill_method: str = "forward",
    remove_outliers: bool = False,
    outlier_std: float = 3.0
) -> Union[pd.Series, pd.DataFrame]:
    """
    Clean numeric data by handling missing values and outliers.
    
    Args:
        data: Input data (Series or DataFrame)
        fill_method: Method to fill missing values ('forward', 'backward', 'interpolate', 'drop')
        remove_outliers: Whether to remove statistical outliers
        outlier_std: Standard deviations for outlier detection
    
    Returns:
        Cleaned data
    """
    result = data.copy()
    
    # Handle missing values
    if fill_method == "forward":
        result = result.ffill()
    elif fill_method == "backward":
        result = result.bfill()
    elif fill_method == "interpolate":
        if isinstance(result, pd.DataFrame):
            result = result.interpolate()
        else:
            result = result.interpolate()
    elif fill_method == "drop":
        result = result.dropna()
    
    # Remove outliers if requested
    if remove_outliers and isinstance(result, (pd.Series, pd.DataFrame)):
        if isinstance(result, pd.Series):
            z_scores = np.abs((result - result.mean()) / result.std())
            result = result[z_scores <= outlier_std]
        else:
            # For DataFrames, remove outliers column by column
            for column in result.select_dtypes(include=[np.number]).columns:
                z_scores = np.abs((result[column] - result[column].mean()) / result[column].std())
                result = result[z_scores <= outlier_std]
    
    return result


def normalize_symbol(symbol: str) -> str:
    """Normalize stock symbol to standard format."""
    if not symbol:
        return ""
    
    # Remove whitespace and convert to uppercase
    normalized = symbol.strip().upper()
    
    # Remove common prefixes/suffixes that might cause issues
    # Remove exchange suffixes like .NASDAQ, .NYSE
    if "." in normalized:
        parts = normalized.split(".")
        if len(parts) == 2 and parts[1] in ["NASDAQ", "NYSE", "AMEX"]:
            normalized = parts[0]
    
    return normalized


def standardize_price_data(
    data: pd.DataFrame,
    required_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Standardize price data format across different vendors.
    
    Args:
        data: Raw price data
        required_columns: Required columns to validate
    
    Returns:
        Standardized DataFrame
    """
    if required_columns is None:
        required_columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
    
    result = data.copy()
    
    # Standardize column names (handle case variations)
    column_mapping = {}
    for col in result.columns:
        lower_col = col.lower()
        if lower_col in ["o", "open_price", "opening"]:
            column_mapping[col] = "open"
        elif lower_col in ["h", "high_price"]:
            column_mapping[col] = "high"
        elif lower_col in ["l", "low_price"]:
            column_mapping[col] = "low"
        elif lower_col in ["c", "close_price", "closing"]:
            column_mapping[col] = "close"
        elif lower_col in ["v", "vol", "volume_traded"]:
            column_mapping[col] = "volume"
        elif lower_col in ["ticker", "sym"]:
            column_mapping[col] = "symbol"
        elif lower_col in ["timestamp", "dt", "datetime"]:
            column_mapping[col] = "date"
    
    result = result.rename(columns=column_mapping)
    
    # Ensure required columns exist
    missing_columns = set(required_columns) - set(result.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Standardize data types
    if "symbol" in result.columns:
        result["symbol"] = result["symbol"].astype(str).apply(normalize_symbol)
    
    if "date" in result.columns:
        result["date"] = pd.to_datetime(result["date"])
    
    # Convert price columns to float
    price_columns = ["open", "high", "low", "close"]
    for col in price_columns:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")
    
    # Convert volume to integer
    if "volume" in result.columns:
        result["volume"] = pd.to_numeric(result["volume"], errors="coerce").fillna(0).astype(int)
    
    # Remove any rows with invalid data
    result = result.dropna(subset=[col for col in price_columns if col in result.columns])
    
    return result


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


def resample_price_data(
    data: pd.DataFrame,
    timeframe: str,
    price_column: str = "close"
) -> pd.DataFrame:
    """
    Resample price data to different timeframe.
    
    Args:
        data: Price data with datetime index
        timeframe: Target timeframe ('1H', '1D', '1W', etc.)
        price_column: Column to use for resampling
    
    Returns:
        Resampled data
    """
    if "date" in data.columns and data.index.name != "date":
        data = data.set_index("date")
    
    # Define aggregation rules
    agg_rules = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }
    
    # Only use columns that exist
    agg_rules = {k: v for k, v in agg_rules.items() if k in data.columns}
    
    resampled = data.resample(timeframe).agg(agg_rules)
    
    # Remove rows with no data
    resampled = resampled.dropna()
    
    return resampled


def detect_splits_and_dividends(
    data: pd.DataFrame,
    split_threshold: float = 0.5,
    dividend_threshold: float = 0.02
) -> Dict[str, List[pd.Timestamp]]:
    """
    Detect potential stock splits and dividend payments from price data.
    
    Args:
        data: Price data with OHLC
        split_threshold: Threshold for detecting splits (e.g., 0.5 for 2:1 split)
        dividend_threshold: Threshold for detecting dividends (as fraction of price)
    
    Returns:
        Dictionary with detected splits and dividends
    """
    if len(data) < 2:
        return {"splits": [], "dividends": []}
    
    data_sorted = data.sort_values("date")
    
    # Calculate price changes
    price_changes = data_sorted["close"].pct_change()
    
    # Detect splits (large negative price changes)
    potential_splits = data_sorted[price_changes < -split_threshold]["date"].tolist()
    
    # Detect dividends (moderate negative price changes)
    potential_dividends = data_sorted[
        (price_changes < -dividend_threshold) & 
        (price_changes > -split_threshold)
    ]["date"].tolist()
    
    return {
        "splits": potential_splits,
        "dividends": potential_dividends
    }


def adjust_prices_for_splits(
    data: pd.DataFrame,
    split_dates: List[pd.Timestamp],
    split_ratios: List[float]
) -> pd.DataFrame:
    """
    Adjust historical prices for stock splits.
    
    Args:
        data: Price data
        split_dates: Dates of splits
        split_ratios: Split ratios (e.g., 2.0 for 2:1 split)
    
    Returns:
        Split-adjusted price data
    """
    result = data.copy()
    
    price_columns = ["open", "high", "low", "close"]
    
    for split_date, ratio in zip(split_dates, split_ratios):
        # Adjust prices before split date
        mask = result["date"] < split_date
        for col in price_columns:
            if col in result.columns:
                result.loc[mask, col] = result.loc[mask, col] / ratio
        
        # Adjust volume after split date
        if "volume" in result.columns:
            volume_mask = result["date"] >= split_date
            result.loc[volume_mask, "volume"] = result.loc[volume_mask, "volume"] * ratio
    
    return result


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


def format_currency(value: Union[float, Decimal], currency: str = "USD") -> str:
    """Format number as currency."""
    if isinstance(value, Decimal):
        rounded = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    else:
        rounded = round(value, 2)
    
    if currency == "USD":
        return f"${rounded:,.2f}"
    else:
        return f"{rounded:,.2f} {currency}"


def format_percentage(value: float, decimal_places: int = 2) -> str:
    """Format number as percentage."""
    return f"{value * 100:.{decimal_places}f}%"


def format_large_number(value: Union[int, float]) -> str:
    """Format large numbers with appropriate suffixes."""
    if abs(value) >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    elif abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    else:
        return str(int(value))


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default if denominator is zero."""
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ZeroDivisionError):
        return default


def chunk_dataframe(df: pd.DataFrame, chunk_size: int) -> List[pd.DataFrame]:
    """Split DataFrame into chunks of specified size."""
    chunks = []
    for i in range(0, len(df), chunk_size):
        chunks.append(df.iloc[i:i + chunk_size])
    return chunks


def merge_dataframes_safely(
    dfs: List[pd.DataFrame],
    on: Union[str, List[str]],
    how: str = "outer"
) -> pd.DataFrame:
    """Safely merge multiple DataFrames with error handling."""
    if not dfs:
        return pd.DataFrame()
    
    if len(dfs) == 1:
        return dfs[0]
    
    result = dfs[0]
    for df in dfs[1:]:
        try:
            result = pd.merge(result, df, on=on, how=how)
        except Exception as e:
            warnings.warn(f"Failed to merge DataFrame: {e}")
            continue
    
    return result


def validate_data_consistency(
    data: pd.DataFrame,
    reference_data: pd.DataFrame,
    tolerance: float = 0.01
) -> ValidationResult:
    """
    Validate data consistency between datasets.
    
    Args:
        data: Primary dataset
        reference_data: Reference dataset for comparison
        tolerance: Tolerance for numeric comparisons
    
    Returns:
        Validation result
    """
    from core.validation.data_validators import ValidationResult
    
    result = ValidationResult(is_valid=True)
    
    # Check if datasets have same shape
    if data.shape != reference_data.shape:
        result.add_warning(
            f"Shape mismatch: {data.shape} vs {reference_data.shape}"
        )
    
    # Check numeric columns for consistency
    numeric_columns = data.select_dtypes(include=[np.number]).columns
    common_columns = set(numeric_columns) & set(reference_data.columns)
    
    for column in common_columns:
        if column in reference_data.select_dtypes(include=[np.number]).columns:
            # Calculate differences
            differences = np.abs(data[column] - reference_data[column])
            relative_differences = differences / np.abs(reference_data[column])
            
            # Check for large differences
            large_diffs = (relative_differences > tolerance).sum()
            if large_diffs > 0:
                result.add_warning(
                    f"Column {column}: {large_diffs} values differ by >{tolerance:.1%}"
                )
    
    return result