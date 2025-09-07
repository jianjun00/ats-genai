"""
Data validation utilities.
"""

import numpy as np
import pandas as pd


class ValidationResult:
    """Simple validation result class."""
    
    def __init__(self, is_valid: bool = True):
        self.is_valid = is_valid
        self.warnings = []
        self.errors = []
    
    def add_warning(self, message: str):
        self.warnings.append(message)
    
    def add_error(self, message: str):
        self.errors.append(message)
        self.is_valid = False


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