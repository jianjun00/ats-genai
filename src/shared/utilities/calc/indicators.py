"""
Market indicators and detection utilities.

Functions for detecting splits, dividends, and other market events.
"""

import pandas as pd
from typing import Dict, List


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