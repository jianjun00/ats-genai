#!/usr/bin/env python3
"""
Unit Test: InstrumentInterval Scalar Conversion
Test that InstrumentInterval objects receive scalar values, not pandas Series.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from domains.trading.services.state.instrument_interval import InstrumentInterval


class TestInstrumentIntervalScalarConversion:
    """Test scalar conversion for InstrumentInterval creation."""
    
    def test_instrument_interval_requires_scalar_values(self):
        """Test that InstrumentInterval correctly handles scalar OHLC values."""
        # Create valid scalar values
        start_time = datetime(2025, 7, 1, 14, 0, 0)
        end_time = datetime(2025, 7, 1, 15, 0, 0)
        
        # Test with proper scalar values - should work
        interval = InstrumentInterval(
            instrument_id=9034,
            start_date_time=start_time,
            end_date_time=end_time,
            open=299.45,
            high=302.77,
            low=299.30,
            close=302.08,
            traded_volume=1000.0,
            traded_dollar=302080.0,
            status='ok'
        )
        
        # Verify all values are scalars
        assert isinstance(interval.open, (int, float))
        assert isinstance(interval.high, (int, float))
        assert isinstance(interval.low, (int, float))
        assert isinstance(interval.close, (int, float))
        assert isinstance(interval.traded_volume, (int, float))
        
        # Verify values are correct
        assert interval.open == 299.45
        assert interval.high == 302.77
        
    def test_pandas_series_conversion_to_scalar(self):
        """Test conversion of pandas Series to scalar values."""
        # Simulate problematic pandas Series data
        ohlc_series = pd.Series([299.45, 299.32, 299.30, 299.35])
        
        # Extract scalar values properly - this is what should be done
        open_scalar = float(ohlc_series.iloc[0]) if len(ohlc_series) > 0 else None
        high_scalar = float(ohlc_series.max()) if len(ohlc_series) > 0 else None
        low_scalar = float(ohlc_series.min()) if len(ohlc_series) > 0 else None
        close_scalar = float(ohlc_series.iloc[-1]) if len(ohlc_series) > 0 else None
        
        # Verify conversions produce scalars
        assert isinstance(open_scalar, float)
        assert isinstance(high_scalar, float)
        assert isinstance(low_scalar, float)
        assert isinstance(close_scalar, float)
        
        # Test that these work in InstrumentInterval
        start_time = datetime(2025, 7, 1, 14, 0, 0)
        end_time = datetime(2025, 7, 1, 15, 0, 0)
        
        interval = InstrumentInterval(
            instrument_id=9034,
            start_date_time=start_time,
            end_date_time=end_time,
            open=open_scalar,
            high=high_scalar,
            low=low_scalar,
            close=close_scalar,
            traded_volume=1000.0,
            traded_dollar=302080.0,
            status='ok'
        )
        
        assert interval.open == 299.45
        assert interval.high == 299.45
        
    def test_conversion_utility_function(self):
        """Test utility function for safe scalar conversion."""
        
        def safe_scalar_conversion(value, default=0.0):
            """Convert pandas Series or other types to scalar float."""
            if value is None:
                return None
            elif isinstance(value, (pd.Series, np.ndarray)):
                if len(value) > 0:
                    # For OHLC, typically we want the first value for open, last for close
                    # This is a simple conversion - may need refinement for specific use cases
                    return float(value.iloc[0] if hasattr(value, 'iloc') else value[0])
                else:
                    return default
            elif isinstance(value, (int, float, np.integer, np.floating)):
                return float(value)
            else:
                # Try to convert to float
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default
        
        # Test with various inputs
        assert safe_scalar_conversion(299.45) == 299.45
        assert safe_scalar_conversion(pd.Series([299.45, 300.0])) == 299.45
        assert safe_scalar_conversion(np.array([299.45, 300.0])) == 299.45
        assert safe_scalar_conversion(None) is None
        assert safe_scalar_conversion("invalid", 0.0) == 0.0
        
        print("✅ Scalar conversion utility function works correctly")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])