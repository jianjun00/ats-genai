"""
Price data parsing and standardization utilities.
"""

import numpy as np
import pandas as pd
from typing import List, Optional

from .symbols import normalize_symbol


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