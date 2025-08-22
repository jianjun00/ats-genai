"""
Unified data validation framework for ATS-GenAI.

This module provides standardized data validation across all components
with reusable validators and comprehensive error reporting.
"""

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from typing import Any, List, Dict, Optional, Union, Callable, Type
import pandas as pd
import numpy as np

from core.exceptions.custom_exceptions import DataValidationError, create_error_context


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_error(self, message: str):
        """Add validation error."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str):
        """Add validation warning."""
        self.warnings.append(message)
    
    def add_metadata(self, key: str, value: Any):
        """Add metadata."""
        self.metadata[key] = value
    
    def merge(self, other: 'ValidationResult'):
        """Merge another validation result."""
        self.is_valid = self.is_valid and other.is_valid
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.metadata.update(other.metadata)


class BaseValidator(ABC):
    """Base class for all validators."""
    
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def validate(self, data: Any) -> ValidationResult:
        """Validate data and return result."""
        pass
    
    def __call__(self, data: Any) -> ValidationResult:
        """Allow validator to be called directly."""
        return self.validate(data)


class FieldValidator(BaseValidator):
    """Validator for individual fields."""
    
    def __init__(
        self,
        name: str,
        required: bool = False,
        data_type: Optional[Type] = None,
        min_value: Optional[Union[int, float, Decimal]] = None,
        max_value: Optional[Union[int, float, Decimal]] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        regex_pattern: Optional[str] = None,
        allowed_values: Optional[List[Any]] = None,
        custom_validator: Optional[Callable[[Any], bool]] = None
    ):
        super().__init__(name)
        self.required = required
        self.data_type = data_type
        self.min_value = min_value
        self.max_value = max_value
        self.min_length = min_length
        self.max_length = max_length
        self.regex_pattern = regex_pattern
        self.allowed_values = allowed_values
        self.custom_validator = custom_validator
    
    def validate(self, data: Any) -> ValidationResult:
        """Validate field value."""
        result = ValidationResult(is_valid=True)
        
        # Check if required
        if self.required and (data is None or (isinstance(data, str) and not data.strip())):
            result.add_error(f"{self.name} is required")
            return result
        
        # If not required and empty, skip other validations
        if data is None or (isinstance(data, str) and not data.strip()):
            return result
        
        # Type validation
        if self.data_type and not isinstance(data, self.data_type):
            try:
                # Try to convert
                if self.data_type in (int, float, Decimal):
                    data = self.data_type(data)
                elif self.data_type == str:
                    data = str(data)
                else:
                    result.add_error(f"{self.name} must be of type {self.data_type.__name__}")
                    return result
            except (ValueError, TypeError):
                result.add_error(f"{self.name} cannot be converted to {self.data_type.__name__}")
                return result
        
        # Numeric range validation
        if self.min_value is not None and isinstance(data, (int, float, Decimal)):
            if data < self.min_value:
                result.add_error(f"{self.name} must be >= {self.min_value}")
        
        if self.max_value is not None and isinstance(data, (int, float, Decimal)):
            if data > self.max_value:
                result.add_error(f"{self.name} must be <= {self.max_value}")
        
        # String length validation
        if isinstance(data, str):
            if self.min_length is not None and len(data) < self.min_length:
                result.add_error(f"{self.name} must be at least {self.min_length} characters")
            
            if self.max_length is not None and len(data) > self.max_length:
                result.add_error(f"{self.name} must be at most {self.max_length} characters")
            
            # Regex validation
            if self.regex_pattern and not re.match(self.regex_pattern, data):
                result.add_error(f"{self.name} does not match required pattern")
        
        # Allowed values validation
        if self.allowed_values is not None and data not in self.allowed_values:
            result.add_error(f"{self.name} must be one of: {self.allowed_values}")
        
        # Custom validation
        if self.custom_validator and not self.custom_validator(data):
            result.add_error(f"{self.name} failed custom validation")
        
        return result


class DataFrameValidator(BaseValidator):
    """Validator for pandas DataFrames."""
    
    def __init__(
        self,
        name: str,
        required_columns: Optional[List[str]] = None,
        column_validators: Optional[Dict[str, FieldValidator]] = None,
        min_rows: Optional[int] = None,
        max_rows: Optional[int] = None,
        allow_duplicates: bool = True,
        duplicate_columns: Optional[List[str]] = None
    ):
        super().__init__(name)
        self.required_columns = required_columns or []
        self.column_validators = column_validators or {}
        self.min_rows = min_rows
        self.max_rows = max_rows
        self.allow_duplicates = allow_duplicates
        self.duplicate_columns = duplicate_columns or []
    
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate DataFrame."""
        result = ValidationResult(is_valid=True)
        
        if not isinstance(data, pd.DataFrame):
            result.add_error(f"{self.name} must be a pandas DataFrame")
            return result
        
        # Check required columns
        missing_columns = set(self.required_columns) - set(data.columns)
        if missing_columns:
            result.add_error(f"Missing required columns: {list(missing_columns)}")
        
        # Check row count
        if self.min_rows is not None and len(data) < self.min_rows:
            result.add_error(f"DataFrame must have at least {self.min_rows} rows")
        
        if self.max_rows is not None and len(data) > self.max_rows:
            result.add_error(f"DataFrame must have at most {self.max_rows} rows")
        
        # Check for duplicates
        if not self.allow_duplicates:
            duplicate_cols = self.duplicate_columns if self.duplicate_columns else None
            duplicates = data.duplicated(subset=duplicate_cols).sum()
            if duplicates > 0:
                result.add_error(f"Found {duplicates} duplicate rows")
        
        # Validate individual columns
        for column, validator in self.column_validators.items():
            if column in data.columns:
                for idx, value in data[column].items():
                    col_result = validator.validate(value)
                    if not col_result.is_valid:
                        for error in col_result.errors:
                            result.add_error(f"Row {idx}, Column {column}: {error}")
        
        # Add metadata
        result.add_metadata("row_count", len(data))
        result.add_metadata("column_count", len(data.columns))
        result.add_metadata("memory_usage_mb", data.memory_usage(deep=True).sum() / 1024 / 1024)
        
        return result


class MarketDataValidator(BaseValidator):
    """Specialized validator for market data."""
    
    def __init__(self, name: str = "market_data"):
        super().__init__(name)
        
        # Define standard market data validators
        self.symbol_validator = FieldValidator(
            "symbol",
            required=True,
            data_type=str,
            min_length=1,
            max_length=10,
            regex_pattern=r"^[A-Z0-9._-]+$"
        )
        
        self.price_validator = FieldValidator(
            "price",
            required=True,
            data_type=float,
            min_value=0.0,
            max_value=999999.99
        )
        
        self.volume_validator = FieldValidator(
            "volume",
            required=True,
            data_type=int,
            min_value=0
        )
        
        self.date_validator = FieldValidator(
            "date",
            required=True,
            custom_validator=self._validate_trading_date
        )
    
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate market data DataFrame."""
        result = ValidationResult(is_valid=True)
        
        # Use DataFrame validator first
        df_validator = DataFrameValidator(
            self.name,
            required_columns=["symbol", "date", "open", "high", "low", "close", "volume"],
            min_rows=1
        )
        
        df_result = df_validator.validate(data)
        result.merge(df_result)
        
        if not result.is_valid:
            return result
        
        # Market data specific validations
        self._validate_ohlc_consistency(data, result)
        self._validate_price_changes(data, result)
        self._validate_volume_consistency(data, result)
        self._detect_data_anomalies(data, result)
        
        return result
    
    def _validate_trading_date(self, date_value: Any) -> bool:
        """Validate trading date."""
        try:
            if isinstance(date_value, str):
                parsed_date = datetime.strptime(date_value, "%Y-%m-%d").date()
            elif isinstance(date_value, datetime):
                parsed_date = date_value.date()
            elif isinstance(date_value, date):
                parsed_date = date_value
            else:
                return False
            
            # Check if date is not in the future
            if parsed_date > date.today():
                return False
            
            # Check if date is not too old (e.g., before 1900)
            if parsed_date.year < 1900:
                return False
            
            return True
        except (ValueError, TypeError):
            return False
    
    def _validate_ohlc_consistency(self, data: pd.DataFrame, result: ValidationResult):
        """Validate OHLC price consistency."""
        if not all(col in data.columns for col in ["open", "high", "low", "close"]):
            return
        
        # High should be >= Open, Low, Close
        high_errors = (
            (data["high"] < data["open"]) |
            (data["high"] < data["low"]) |
            (data["high"] < data["close"])
        ).sum()
        
        if high_errors > 0:
            result.add_error(f"Found {high_errors} rows where high price is inconsistent")
        
        # Low should be <= Open, High, Close
        low_errors = (
            (data["low"] > data["open"]) |
            (data["low"] > data["high"]) |
            (data["low"] > data["close"])
        ).sum()
        
        if low_errors > 0:
            result.add_error(f"Found {low_errors} rows where low price is inconsistent")
    
    def _validate_price_changes(self, data: pd.DataFrame, result: ValidationResult):
        """Validate reasonable price changes."""
        if "close" not in data.columns or len(data) < 2:
            return
        
        # Calculate daily returns
        data_sorted = data.sort_values("date")
        returns = data_sorted["close"].pct_change().abs()
        
        # Flag extreme returns (>50% in a day)
        extreme_returns = (returns > 0.5).sum()
        if extreme_returns > 0:
            result.add_warning(f"Found {extreme_returns} days with >50% price changes")
        
        # Flag returns >100% (likely splits or errors)
        very_extreme_returns = (returns > 1.0).sum()
        if very_extreme_returns > 0:
            result.add_error(f"Found {very_extreme_returns} days with >100% price changes")
    
    def _validate_volume_consistency(self, data: pd.DataFrame, result: ValidationResult):
        """Validate volume consistency."""
        if "volume" not in data.columns:
            return
        
        # Check for zero volume days
        zero_volume = (data["volume"] == 0).sum()
        if zero_volume > 0:
            result.add_warning(f"Found {zero_volume} days with zero volume")
        
        # Check for extremely high volume (>10x median)
        if len(data) > 10:  # Need enough data for median
            median_volume = data["volume"].median()
            extreme_volume = (data["volume"] > median_volume * 10).sum()
            if extreme_volume > 0:
                result.add_warning(f"Found {extreme_volume} days with extremely high volume")
    
    def _detect_data_anomalies(self, data: pd.DataFrame, result: ValidationResult):
        """Detect data anomalies using statistical methods."""
        if "close" not in data.columns or len(data) < 10:
            return
        
        # Detect potential outliers using Z-score
        prices = data["close"]
        z_scores = np.abs((prices - prices.mean()) / prices.std())
        outliers = (z_scores > 3).sum()
        
        if outliers > 0:
            result.add_warning(f"Found {outliers} potential price outliers (Z-score > 3)")
        
        # Check for data gaps (missing dates)
        if "date" in data.columns and len(data) > 1:
            dates = pd.to_datetime(data["date"]).sort_values()
            date_diffs = dates.diff().dt.days
            # Gaps > 10 days might indicate missing data
            large_gaps = (date_diffs > 10).sum()
            if large_gaps > 0:
                result.add_warning(f"Found {large_gaps} potential data gaps (>10 days)")


class DataQualityValidator(BaseValidator):
    """Comprehensive data quality validator."""
    
    def __init__(self, name: str = "data_quality"):
        super().__init__(name)
    
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data quality metrics."""
        result = ValidationResult(is_valid=True)
        
        # Completeness checks
        completeness_result = self._check_completeness(data)
        result.merge(completeness_result)
        
        # Uniqueness checks
        uniqueness_result = self._check_uniqueness(data)
        result.merge(uniqueness_result)
        
        # Consistency checks
        consistency_result = self._check_consistency(data)
        result.merge(consistency_result)
        
        # Timeliness checks
        timeliness_result = self._check_timeliness(data)
        result.merge(timeliness_result)
        
        # Calculate overall quality score
        quality_score = self._calculate_quality_score(result)
        result.add_metadata("quality_score", quality_score)
        
        return result
    
    def _check_completeness(self, data: pd.DataFrame) -> ValidationResult:
        """Check data completeness."""
        result = ValidationResult(is_valid=True)
        
        total_cells = data.size
        missing_cells = data.isnull().sum().sum()
        completeness_ratio = 1 - (missing_cells / total_cells) if total_cells > 0 else 0
        
        result.add_metadata("completeness_ratio", completeness_ratio)
        
        if completeness_ratio < 0.95:
            result.add_warning(f"Data completeness is {completeness_ratio:.1%} (< 95%)")
        
        if completeness_ratio < 0.80:
            result.add_error(f"Data completeness is {completeness_ratio:.1%} (< 80%)")
        
        return result
    
    def _check_uniqueness(self, data: pd.DataFrame) -> ValidationResult:
        """Check data uniqueness."""
        result = ValidationResult(is_valid=True)
        
        if len(data) == 0:
            return result
        
        duplicate_ratio = data.duplicated().sum() / len(data)
        result.add_metadata("duplicate_ratio", duplicate_ratio)
        
        if duplicate_ratio > 0.05:
            result.add_warning(f"Duplicate ratio is {duplicate_ratio:.1%} (> 5%)")
        
        if duplicate_ratio > 0.10:
            result.add_error(f"Duplicate ratio is {duplicate_ratio:.1%} (> 10%)")
        
        return result
    
    def _check_consistency(self, data: pd.DataFrame) -> ValidationResult:
        """Check data consistency."""
        result = ValidationResult(is_valid=True)
        
        # Check for consistent data types within columns
        inconsistent_columns = []
        for column in data.columns:
            if data[column].dtype == 'object':
                # Check if all non-null values have consistent types
                non_null_values = data[column].dropna()
                if len(non_null_values) > 0:
                    first_type = type(non_null_values.iloc[0])
                    inconsistent = any(type(val) != first_type for val in non_null_values)
                    if inconsistent:
                        inconsistent_columns.append(column)
        
        if inconsistent_columns:
            result.add_warning(f"Inconsistent data types in columns: {inconsistent_columns}")
        
        return result
    
    def _check_timeliness(self, data: pd.DataFrame) -> ValidationResult:
        """Check data timeliness."""
        result = ValidationResult(is_valid=True)
        
        # Look for date/datetime columns
        date_columns = []
        for column in data.columns:
            if 'date' in column.lower() or 'time' in column.lower():
                try:
                    pd.to_datetime(data[column].dropna().iloc[:5])
                    date_columns.append(column)
                except:
                    continue
        
        if date_columns:
            for column in date_columns:
                try:
                    dates = pd.to_datetime(data[column]).dropna()
                    if len(dates) > 0:
                        latest_date = dates.max()
                        days_old = (datetime.now() - latest_date).days
                        
                        result.add_metadata(f"{column}_latest", latest_date.isoformat())
                        result.add_metadata(f"{column}_days_old", days_old)
                        
                        if days_old > 7:
                            result.add_warning(f"{column} data is {days_old} days old")
                        
                        if days_old > 30:
                            result.add_error(f"{column} data is {days_old} days old (> 30 days)")
                except:
                    continue
        
        return result
    
    def _calculate_quality_score(self, result: ValidationResult) -> float:
        """Calculate overall data quality score (0.0 to 1.0)."""
        score = 1.0
        
        # Deduct points for errors and warnings
        score -= len(result.errors) * 0.1
        score -= len(result.warnings) * 0.05
        
        # Factor in completeness
        completeness = result.metadata.get("completeness_ratio", 1.0)
        score *= completeness
        
        # Factor in duplicates
        duplicate_ratio = result.metadata.get("duplicate_ratio", 0.0)
        score *= (1 - duplicate_ratio)
        
        return max(0.0, min(1.0, score))


# Factory function for creating common validators
def create_market_data_validator() -> MarketDataValidator:
    """Create a standard market data validator."""
    return MarketDataValidator()


def create_price_validator(
    min_price: float = 0.01,
    max_price: float = 999999.99
) -> FieldValidator:
    """Create a price field validator."""
    return FieldValidator(
        "price",
        required=True,
        data_type=float,
        min_value=min_price,
        max_value=max_price
    )


def create_symbol_validator() -> FieldValidator:
    """Create a symbol field validator."""
    return FieldValidator(
        "symbol",
        required=True,
        data_type=str,
        min_length=1,
        max_length=10,
        regex_pattern=r"^[A-Z0-9._-]+$"
    )


def create_date_validator() -> FieldValidator:
    """Create a date field validator."""
    return FieldValidator(
        "date",
        required=True,
        custom_validator=lambda x: MarketDataValidator()._validate_trading_date(x)
    )