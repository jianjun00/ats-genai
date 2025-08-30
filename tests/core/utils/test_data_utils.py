import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock
from decimal import Decimal
from core.utils.data_utils import (
    clean_numeric_data,
    normalize_data,
    calculate_returns,
    detect_outliers,
    aggregate_time_series,
    validate_data_quality,
    fill_missing_values,
    round_financial_value,
    create_lagged_features,
    smooth_time_series
)
from core.validation.data_validators import ValidationResult


class TestDataUtilsCore:
    """Comprehensive test coverage for core data utility functions."""
    
    def test_clean_numeric_data_forward_fill(self):
        """Test clean_numeric_data with forward fill method."""
        # Create test data with missing values
        data = pd.Series([1.0, 2.0, np.nan, 4.0, np.nan, 6.0])
        
        result = clean_numeric_data(data, fill_method="forward")
        
        expected = pd.Series([1.0, 2.0, 2.0, 4.0, 4.0, 6.0])
        pd.testing.assert_series_equal(result, expected)
    
    def test_clean_numeric_data_backward_fill(self):
        """Test clean_numeric_data with backward fill method."""
        data = pd.Series([1.0, np.nan, 3.0, np.nan, 5.0])
        
        result = clean_numeric_data(data, fill_method="backward")
        
        expected = pd.Series([1.0, 3.0, 3.0, 5.0, 5.0])
        pd.testing.assert_series_equal(result, expected)
    
    def test_clean_numeric_data_interpolate(self):
        """Test clean_numeric_data with interpolation."""
        data = pd.Series([1.0, np.nan, 5.0, np.nan, 9.0])
        
        result = clean_numeric_data(data, fill_method="interpolate")
        
        expected = pd.Series([1.0, 3.0, 5.0, 7.0, 9.0])
        pd.testing.assert_series_equal(result, expected)
    
    def test_clean_numeric_data_drop_na(self):
        """Test clean_numeric_data with drop method."""
        data = pd.Series([1.0, np.nan, 3.0, np.nan, 5.0])
        
        result = clean_numeric_data(data, fill_method="drop")
        
        expected = pd.Series([1.0, 3.0, 5.0], index=[0, 2, 4])
        pd.testing.assert_series_equal(result, expected)
    
    def test_clean_numeric_data_with_outliers(self):
        """Test clean_numeric_data with outlier removal."""
        # Create data with outliers
        data = pd.Series([1.0, 2.0, 3.0, 100.0, 4.0, 5.0])  # 100.0 is outlier
        
        result = clean_numeric_data(data, remove_outliers=True, outlier_std=2.0)
        
        # Should remove the outlier (100.0)
        assert 100.0 not in result.values
        assert len(result) < len(data)
    
    def test_clean_numeric_data_dataframe(self):
        """Test clean_numeric_data with DataFrame input."""
        df = pd.DataFrame({
            'A': [1.0, np.nan, 3.0],
            'B': [np.nan, 2.0, 4.0]
        })
        
        result = clean_numeric_data(df, fill_method="forward")
        
        expected = pd.DataFrame({
            'A': [1.0, 1.0, 3.0],
            'B': [np.nan, 2.0, 4.0]  # First NaN can't be forward filled
        })
        pd.testing.assert_frame_equal(result, expected)
    
    def test_normalize_data_standard_scaler(self):
        """Test data normalization with standard scaling."""
        data = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        
        result = normalize_data(data, method="standard")
        
        # Standard normalization: (x - mean) / std
        expected_mean = data.mean()
        expected_std = data.std()
        expected = (data - expected_mean) / expected_std
        
        pd.testing.assert_series_equal(result, expected)
    
    def test_normalize_data_min_max_scaler(self):
        """Test data normalization with min-max scaling."""
        data = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        
        result = normalize_data(data, method="min_max")
        
        # Min-max normalization: (x - min) / (max - min)
        expected = (data - data.min()) / (data.max() - data.min())
        
        pd.testing.assert_series_equal(result, expected)
    
    def test_normalize_data_robust_scaler(self):
        """Test data normalization with robust scaling."""
        data = pd.Series([1.0, 2.0, 3.0, 4.0, 100.0])  # With outlier
        
        result = normalize_data(data, method="robust")
        
        # Robust scaling uses median and IQR
        median = data.median()
        q75, q25 = np.percentile(data, [75, 25])
        iqr = q75 - q25
        expected = (data - median) / iqr
        
        pd.testing.assert_series_equal(result, expected)
    
    def test_calculate_returns_simple(self):
        """Test simple return calculation."""
        prices = pd.Series([100.0, 110.0, 105.0, 115.0])
        
        result = calculate_returns(prices, method="simple")
        
        expected = pd.Series([np.nan, 0.10, -0.045454545454545456, 0.095238095238095233])
        pd.testing.assert_series_equal(result, expected, check_exact=False, atol=1e-10)
    
    def test_calculate_returns_log(self):
        """Test log return calculation."""
        prices = pd.Series([100.0, 110.0, 105.0, 115.0])
        
        result = calculate_returns(prices, method="log")
        
        expected = pd.Series([np.nan, np.log(110/100), np.log(105/110), np.log(115/105)])
        pd.testing.assert_series_equal(result, expected, check_exact=False)
    
    def test_calculate_returns_percent(self):
        """Test percentage return calculation."""
        prices = pd.Series([100.0, 110.0, 105.0])
        
        result = calculate_returns(prices, method="percent")
        
        expected = pd.Series([np.nan, 10.0, -4.545454545454546])
        pd.testing.assert_series_equal(result, expected, check_exact=False)
    
    def test_detect_outliers_z_score(self):
        """Test outlier detection using z-score method."""
        data = pd.Series([1, 2, 3, 4, 100])  # 100 is clear outlier
        
        result = detect_outliers(data, method="z_score", threshold=2.0)
        
        # Should identify the outlier
        assert result.iloc[-1] == True  # 100 is outlier
        assert result.iloc[0] == False  # 1 is not outlier
    
    def test_detect_outliers_iqr(self):
        """Test outlier detection using IQR method."""
        data = pd.Series([1, 2, 3, 4, 5, 100])  # 100 is outlier
        
        result = detect_outliers(data, method="iqr", multiplier=1.5)
        
        # Should identify the outlier
        assert result.iloc[-1] == True  # 100 is outlier
    
    def test_detect_outliers_modified_z_score(self):
        """Test outlier detection using modified z-score method."""
        data = pd.Series([1, 2, 3, 4, 100])
        
        result = detect_outliers(data, method="modified_z_score", threshold=3.5)
        
        # Should detect outlier using median absolute deviation
        assert result.iloc[-1] == True
    
    def test_aggregate_time_series_mean(self):
        """Test time series aggregation with mean."""
        # Create time series data
        dates = pd.date_range('2023-01-01', periods=12, freq='D')
        data = pd.Series(range(12), index=dates)
        
        result = aggregate_time_series(data, freq='3D', agg_func='mean')
        
        # Should have 4 periods (12 days / 3 days)
        assert len(result) == 4
        assert result.iloc[0] == 1.0  # Mean of [0, 1, 2]
    
    def test_aggregate_time_series_custom_function(self):
        """Test time series aggregation with custom function."""
        dates = pd.date_range('2023-01-01', periods=6, freq='D')
        data = pd.Series([1, 2, 3, 4, 5, 6], index=dates)
        
        def custom_agg(x):
            return x.max() - x.min()
        
        result = aggregate_time_series(data, freq='2D', agg_func=custom_agg)
        
        # Should have 3 periods
        assert len(result) == 3
        assert result.iloc[0] == 1  # Max(2) - Min(1) for first period
    
    def test_validate_data_quality_comprehensive(self):
        """Test comprehensive data quality validation."""
        data = pd.DataFrame({
            'price': [100.0, 105.0, np.nan, 110.0],
            'volume': [1000, 0, 1500, 2000]  # Zero volume
        })
        
        result = validate_data_quality(data)
        
        assert isinstance(result, ValidationResult)
        assert result.missing_values > 0  # Should detect NaN
        assert result.zero_values > 0     # Should detect zero volume
        assert result.total_records == 4
    
    def test_fill_missing_values_financial_forward_fill(self):
        """Test financial-specific missing value filling."""
        data = pd.Series([100.0, np.nan, np.nan, 110.0], name='close_price')
        
        result = fill_missing_values(data, method='financial_forward')
        
        # Should forward fill prices (common for financial data)
        expected = pd.Series([100.0, 100.0, 100.0, 110.0], name='close_price')
        pd.testing.assert_series_equal(result, expected)
    
    def test_fill_missing_values_volume_zero_fill(self):
        """Test volume-specific missing value filling."""
        data = pd.Series([1000, np.nan, np.nan, 2000], name='volume')
        
        result = fill_missing_values(data, method='volume_zero')
        
        # Should fill missing volume with zero
        expected = pd.Series([1000, 0, 0, 2000], name='volume')
        pd.testing.assert_series_equal(result, expected, check_dtype=False)
    
    def test_round_financial_value_precision(self):
        """Test financial value rounding with different precisions."""
        test_cases = [
            (123.456789, 2, Decimal('123.46')),
            (0.123456, 4, Decimal('0.1235')),
            (1000.555, 0, Decimal('1001')),
        ]
        
        for value, precision, expected in test_cases:
            result = round_financial_value(value, precision)
            assert result == expected
    
    def test_round_financial_value_decimal_input(self):
        """Test financial value rounding with Decimal input."""
        input_val = Decimal('123.456789')
        result = round_financial_value(input_val, 2)
        
        assert result == Decimal('123.46')
        assert isinstance(result, Decimal)
    
    def test_create_lagged_features_single_lag(self):
        """Test creating lagged features with single lag."""
        data = pd.Series([1, 2, 3, 4, 5])
        
        result = create_lagged_features(data, lags=[1])
        
        expected = pd.DataFrame({
            'original': [1, 2, 3, 4, 5],
            'lag_1': [np.nan, 1, 2, 3, 4]
        })
        pd.testing.assert_frame_equal(result, expected)
    
    def test_create_lagged_features_multiple_lags(self):
        """Test creating lagged features with multiple lags."""
        data = pd.Series([1, 2, 3, 4, 5])
        
        result = create_lagged_features(data, lags=[1, 2])
        
        expected = pd.DataFrame({
            'original': [1, 2, 3, 4, 5],
            'lag_1': [np.nan, 1, 2, 3, 4],
            'lag_2': [np.nan, np.nan, 1, 2, 3]
        })
        pd.testing.assert_frame_equal(result, expected)
    
    def test_create_lagged_features_dataframe_input(self):
        """Test creating lagged features with DataFrame input."""
        df = pd.DataFrame({
            'price': [100, 101, 102],
            'volume': [1000, 1100, 1200]
        })
        
        result = create_lagged_features(df, lags=[1], columns=['price'])
        
        assert 'price_lag_1' in result.columns
        assert 'volume' in result.columns  # Original columns preserved
        assert len(result) == 3
    
    def test_smooth_time_series_moving_average(self):
        """Test time series smoothing with moving average."""
        data = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        
        result = smooth_time_series(data, method='moving_average', window=3)
        
        # First two values should be NaN, then rolling mean
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 2.0  # Mean of [1, 2, 3]
        assert result.iloc[3] == 3.0  # Mean of [2, 3, 4]
    
    def test_smooth_time_series_exponential(self):
        """Test time series smoothing with exponential smoothing."""
        data = pd.Series([1, 2, 3, 4, 5])
        
        result = smooth_time_series(data, method='exponential', alpha=0.3)
        
        # Should apply exponential smoothing
        assert result.iloc[0] == 1.0  # First value unchanged
        assert len(result) == len(data)
        # Subsequent values should be smoothed
        assert result.iloc[1] != data.iloc[1]
    
    def test_smooth_time_series_savitzky_golay(self):
        """Test time series smoothing with Savitzky-Golay filter."""
        data = pd.Series([1, 2, 1, 2, 1, 2, 1, 2, 1])
        
        result = smooth_time_series(data, method='savitzky_golay', window_length=5, polyorder=2)
        
        # Should smooth the oscillating pattern
        assert len(result) == len(data)
        # Middle values should be smoother than original
        assert abs(result.iloc[4] - 1.5) < abs(data.iloc[4] - 1.5)


class TestDataUtilsEdgeCases:
    """Test edge cases and error conditions for data utils."""
    
    def test_clean_numeric_data_empty_series(self):
        """Test clean_numeric_data with empty Series."""
        data = pd.Series([], dtype=float)
        
        result = clean_numeric_data(data)
        
        assert len(result) == 0
        assert isinstance(result, pd.Series)
    
    def test_clean_numeric_data_all_nan(self):
        """Test clean_numeric_data with all NaN values."""
        data = pd.Series([np.nan, np.nan, np.nan])
        
        result = clean_numeric_data(data, fill_method="drop")
        
        assert len(result) == 0
    
    def test_normalize_data_constant_values(self):
        """Test normalize_data with constant values."""
        data = pd.Series([5.0, 5.0, 5.0, 5.0])
        
        # Standard normalization should handle zero std deviation
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # Ignore division by zero warning
            result = normalize_data(data, method="standard")
            
        # Should return zeros or handle gracefully
        assert len(result) == len(data)
    
    def test_calculate_returns_single_value(self):
        """Test calculate_returns with single value."""
        prices = pd.Series([100.0])
        
        result = calculate_returns(prices)
        
        assert len(result) == 1
        assert pd.isna(result.iloc[0])
    
    def test_detect_outliers_insufficient_data(self):
        """Test outlier detection with insufficient data."""
        data = pd.Series([1])  # Single value
        
        result = detect_outliers(data, method="z_score")
        
        # Should handle gracefully
        assert len(result) == 1
        assert not result.iloc[0]  # Single value is not outlier
    
    def test_aggregate_time_series_invalid_frequency(self):
        """Test time series aggregation with invalid frequency."""
        dates = pd.date_range('2023-01-01', periods=5, freq='D')
        data = pd.Series(range(5), index=dates)
        
        with pytest.raises(ValueError):
            aggregate_time_series(data, freq='invalid_freq', agg_func='mean')
    
    def test_round_financial_value_none_input(self):
        """Test round_financial_value with None input."""
        result = round_financial_value(None, 2)
        
        assert result is None
    
    def test_round_financial_value_string_input(self):
        """Test round_financial_value with string input."""
        result = round_financial_value("123.456", 2)
        
        assert result == Decimal('123.46')
    
    def test_create_lagged_features_invalid_lag(self):
        """Test create_lagged_features with invalid lag values."""
        data = pd.Series([1, 2, 3])
        
        with pytest.raises(ValueError):
            create_lagged_features(data, lags=[0])  # Zero lag invalid
        
        with pytest.raises(ValueError):
            create_lagged_features(data, lags=[-1])  # Negative lag invalid
    
    def test_smooth_time_series_insufficient_data(self):
        """Test smooth_time_series with insufficient data for window."""
        data = pd.Series([1, 2])  # Only 2 points
        
        # Should handle gracefully when window > data length
        result = smooth_time_series(data, method='moving_average', window=5)
        
        assert len(result) == len(data)
        # All values should be NaN due to insufficient data
        assert pd.isna(result).all()


class TestDataUtilsIntegration:
    """Test integration scenarios combining multiple data utility functions."""
    
    def test_financial_data_processing_pipeline(self):
        """Test complete financial data processing pipeline."""
        # Create realistic financial data with issues
        dates = pd.date_range('2023-01-01', periods=10, freq='D')
        raw_data = pd.DataFrame({
            'price': [100.0, 101.0, np.nan, 103.0, 104.0, np.nan, 106.0, 107.0, 500.0, 109.0],  # Outlier
            'volume': [1000, 1100, 0, 1300, 1400, np.nan, 1600, 1700, 1800, 1900]
        }, index=dates)
        
        # Step 1: Clean the data
        cleaned_price = clean_numeric_data(raw_data['price'], fill_method="forward", remove_outliers=True)
        cleaned_volume = fill_missing_values(raw_data['volume'], method='volume_zero')
        
        # Step 2: Calculate returns
        returns = calculate_returns(cleaned_price, method="simple")
        
        # Step 3: Create lagged features
        features = create_lagged_features(cleaned_price, lags=[1])
        
        # Step 4: Smooth the data
        smoothed = smooth_time_series(cleaned_price, method='moving_average', window=3)
        
        # Verify pipeline results
        assert len(cleaned_price) <= len(raw_data)  # Outliers removed
        assert not pd.isna(cleaned_volume).any()     # No missing values
        assert len(returns) == len(cleaned_price)    # Returns calculated
        assert 'lag_1' in features.columns           # Lagged features created
        assert len(smoothed) == len(cleaned_price)   # Smoothed data
    
    def test_multi_asset_data_processing(self):
        """Test processing multiple financial assets simultaneously."""
        # Create multi-asset data
        dates = pd.date_range('2023-01-01', periods=5, freq='D')
        multi_asset_data = pd.DataFrame({
            'AAPL_price': [150.0, 152.0, np.nan, 156.0, 158.0],
            'GOOGL_price': [2500.0, 2520.0, 2540.0, np.nan, 2580.0],
            'MSFT_price': [300.0, 305.0, 310.0, 315.0, 320.0]
        }, index=dates)
        
        # Process each asset
        processed_data = {}
        for column in multi_asset_data.columns:
            cleaned = clean_numeric_data(multi_asset_data[column], fill_method="interpolate")
            returns = calculate_returns(cleaned, method="simple")
            processed_data[f"{column}_returns"] = returns
        
        # Verify results
        assert len(processed_data) == 3  # One return series per asset
        for key, returns in processed_data.items():
            assert len(returns) == len(multi_asset_data)
            assert pd.isna(returns.iloc[0])  # First return should be NaN
    
    def test_high_frequency_data_processing(self):
        """Test processing high-frequency financial data."""
        # Create minute-by-minute data for one day
        minutes = pd.date_range('2023-01-01 09:30', '2023-01-01 16:00', freq='T')
        hf_data = pd.Series(
            100 + np.cumsum(np.random.randn(len(minutes)) * 0.1),  # Price walk
            index=minutes
        )
        
        # Add some missing values and outliers
        hf_data.iloc[100] = np.nan
        hf_data.iloc[200] = hf_data.iloc[200] + 50  # Outlier
        
        # Process high-frequency data
        cleaned = clean_numeric_data(hf_data, fill_method="interpolate", remove_outliers=True)
        smoothed = smooth_time_series(cleaned, method='moving_average', window=5)
        aggregated = aggregate_time_series(smoothed, freq='5T', agg_func='mean')  # 5-minute bars
        
        # Verify results
        assert len(cleaned) <= len(hf_data)      # Outliers removed
        assert not pd.isna(cleaned).any()        # No missing values
        assert len(aggregated) < len(smoothed)   # Aggregated to lower frequency
    
    @patch('core.validation.data_validators.ValidationResult')
    def test_data_quality_validation_integration(self, mock_validation_result):
        """Test integration with data quality validation."""
        # Setup mock validation result
        mock_result = MagicMock()
        mock_result.missing_values = 2
        mock_result.zero_values = 1
        mock_result.outlier_count = 1
        mock_result.is_valid = False
        mock_validation_result.return_value = mock_result
        
        # Create problematic data
        problematic_data = pd.DataFrame({
            'price': [100.0, np.nan, 102.0, 1000.0],  # Missing value and outlier
            'volume': [1000, 0, 1200, 1300]           # Zero volume
        })
        
        # Validate and process
        quality_result = validate_data_quality(problematic_data)
        
        if not quality_result.is_valid:
            # Apply cleaning based on validation results
            cleaned_data = problematic_data.copy()
            for column in cleaned_data.columns:
                if 'price' in column.lower():
                    cleaned_data[column] = clean_numeric_data(
                        cleaned_data[column], 
                        fill_method="interpolate",
                        remove_outliers=True
                    )
                elif 'volume' in column.lower():
                    cleaned_data[column] = fill_missing_values(
                        cleaned_data[column],
                        method='volume_zero'
                    )
        
        # Verify cleaning was applied
        assert mock_validation_result.called
        assert len(cleaned_data) <= len(problematic_data)  # May remove outliers