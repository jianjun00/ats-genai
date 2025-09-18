"""
DataFrame parsing and manipulation utilities.
"""

import numpy as np
import pandas as pd
import warnings
from typing import List, Union


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