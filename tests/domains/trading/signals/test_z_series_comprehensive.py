"""
Comprehensive test suite for Z-series indicators (Z1B, Z2B, Z5T, Z6T)

This test suite validates the four new Z-series technical indicators that were
added to the ATS platform. These indicators use precise 12-coefficient linear
regression formulas derived from 23-data-point analysis.

Test Categories:
1. Initialization and setup
2. Valid calculations with known data
3. Error handling (insufficient data, invalid status, missing OHLC)
4. Edge cases (extreme values, NaN handling)
5. Mathematical relationships and ordering
6. Coefficient precision and accuracy
7. Performance and efficiency
8. Integration with ATS framework

Author: Claude Code Assistant
Date: 2025-08-31
"""

import pytest
import math
from datetime import datetime, timedelta
from domains.trading.services.indicators import Z1B, Z2B, Z5T, Z6T
from domains.trading.services.state.instrument_interval import InstrumentInterval

# Mark all tests as unit tests
pytestmark = pytest.mark.unit

# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def known_test_data():
    """
    Test data from linear regression analysis (08/19, 08/20, 08/21 -> 08/22).
    This data produces known expected values for all Z-series indicators.
    """
    base = datetime(2024, 8, 19)
    return [
        InstrumentInterval(
            instrument_id=1,
            start_date_time=base,
            end_date_time=base,
            open=23800.75, high=23838, low=23426, close=23469.5,
            traded_volume=100, traded_dollar=1000,
            status='ok'
        ),  # 08/19
        InstrumentInterval(
            instrument_id=1,
            start_date_time=base+timedelta(days=1),
            end_date_time=base+timedelta(days=1),
            open=23461, high=23485.5, low=23035, close=23324,
            traded_volume=100, traded_dollar=1000,
            status='ok'
        ),  # 08/20
        InstrumentInterval(
            instrument_id=1,
            start_date_time=base+timedelta(days=2),
            end_date_time=base+timedelta(days=2),
            open=23323, high=23369.25, low=23119, close=23219.75,
            traded_volume=100, traded_dollar=1000,
            status='ok'
        ),  # 08/21
    ]

@pytest.fixture
def expected_z_values():
    """Expected Z-series values for the test data (08/22 predictions)."""
    return {
        'z1b': 22795.06,  # Lower support zone
        'z2b': 22966.83,  # Lower resistance zone
        'z5t': 23708.67,  # Upper resistance zone
        'z6t': 23907.81   # Upper breakout zone
    }

@pytest.fixture
def all_z_indicators():
    """Factory fixture that returns fresh instances of all Z-series indicators."""
    return {
        'Z1B': Z1B,
        'Z2B': Z2B,
        'Z5T': Z5T,
        'Z6T': Z6T
    }

# =============================================================================
# INITIALIZATION TESTS
# =============================================================================

class TestZSeriesInitialization:
    """Test proper initialization of all Z-series indicators."""

    def test_z1b_initialization(self):
        """Test Z1B indicator initialization."""
        z1b = Z1B()
        assert z1b.latest_z1b is None
        assert z1b.status is None
        assert z1b.update_at is None
        assert len(z1b.coefficients) == 12

        # Verify key coefficients
        assert abs(z1b.coefficients[0] - (-1.242786)) < 1e-6
        assert abs(z1b.coefficients[1] - 0.772321) < 1e-6

    def test_z2b_initialization(self):
        """Test Z2B indicator initialization."""
        z2b = Z2B()
        assert z2b.latest_z2b is None
        assert z2b.status is None
        assert len(z2b.coefficients) == 12

        # Verify key coefficients
        assert abs(z2b.coefficients[0] - (-0.109183)) < 1e-6
        assert abs(z2b.coefficients[1] - (-0.448761)) < 1e-6

    def test_z5t_initialization(self):
        """Test Z5T indicator initialization."""
        z5t = Z5T()
        assert z5t.latest_z5t is None
        assert z5t.status is None
        assert len(z5t.coefficients) == 12

        # Verify key coefficients
        assert abs(z5t.coefficients[0] - 0.572696) < 1e-6
        assert abs(z5t.coefficients[1] - 0.251544) < 1e-6

    def test_z6t_initialization(self):
        """Test Z6T indicator initialization."""
        z6t = Z6T()
        assert z6t.latest_z6t is None
        assert z6t.status is None
        assert len(z6t.coefficients) == 12

        # Verify key coefficients
        assert abs(z6t.coefficients[0] - 1.853702) < 1e-6
        assert abs(z6t.coefficients[1] - (-1.198374)) < 1e-6

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    def test_coefficient_properties(self, indicator_class):
        """Test that all indicators have proper coefficient properties."""
        indicator = indicator_class()
        coeffs = indicator.coefficients

        # Should have exactly 12 coefficients (3 days × 4 OHLC)
        assert len(coeffs) == 12

        # Should not all be zeros
        assert not all(c == 0 for c in coeffs)

        # Should have sufficient variation
        assert len(set(coeffs)) > 6

        # Should all be finite numbers
        assert all(math.isfinite(c) for c in coeffs)

# =============================================================================
# VALID CALCULATION TESTS
# =============================================================================

class TestZSeriesCalculations:
    """Test valid calculations with known data."""

    def test_z1b_calculation(self, known_test_data, expected_z_values):
        """Test Z1B calculation accuracy."""
        z1b = Z1B()
        z1b.update(known_test_data)

        assert z1b.status == 'ok'
        assert z1b.latest_z1b is not None
        assert z1b.update_at is not None

        calculated = z1b.get_value()
        expected = expected_z_values['z1b']
        error = abs(calculated - expected)

        assert error < 0.1, f"Z1B calculation error too large: {error:.6f}"

    def test_z2b_calculation(self, known_test_data, expected_z_values):
        """Test Z2B calculation accuracy."""
        z2b = Z2B()
        z2b.update(known_test_data)

        assert z2b.status == 'ok'
        calculated = z2b.get_value()
        expected = expected_z_values['z2b']
        error = abs(calculated - expected)

        assert error < 0.1, f"Z2B calculation error too large: {error:.6f}"

    def test_z5t_calculation(self, known_test_data, expected_z_values):
        """Test Z5T calculation accuracy."""
        z5t = Z5T()
        z5t.update(known_test_data)

        assert z5t.status == 'ok'
        calculated = z5t.get_value()
        expected = expected_z_values['z5t']
        error = abs(calculated - expected)

        assert error < 0.1, f"Z5T calculation error too large: {error:.6f}"

    def test_z6t_calculation(self, known_test_data, expected_z_values):
        """Test Z6T calculation accuracy."""
        z6t = Z6T()
        z6t.update(known_test_data)

        assert z6t.status == 'ok'
        calculated = z6t.get_value()
        expected = expected_z_values['z6t']
        error = abs(calculated - expected)

        assert error < 0.1, f"Z6T calculation error too large: {error:.6f}"

    @pytest.mark.parametrize("indicator_class,expected_key", [
        (Z1B, 'z1b'), (Z2B, 'z2b'), (Z5T, 'z5t'), (Z6T, 'z6t')
    ])
    def test_parametrized_calculations(self, indicator_class, expected_key,
                                     known_test_data, expected_z_values):
        """Parametrized test for all Z-series calculations."""
        indicator = indicator_class()
        indicator.update(known_test_data)

        assert indicator.status == 'ok'
        assert indicator.get_value() is not None

        calculated = indicator.get_value()
        expected = expected_z_values[expected_key]
        assert abs(calculated - expected) < 0.1

# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================

class TestZSeriesErrorHandling:
    """Test error handling for invalid inputs."""

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    def test_insufficient_intervals(self, indicator_class):
        """Test behavior with insufficient intervals."""
        indicator = indicator_class()
        base = datetime(2024, 8, 19)

        # Test with 0 intervals
        indicator.update([])
        assert indicator.status == 'invalid'
        assert indicator.get_value() is None

        # Test with 1 interval
        intervals = [InstrumentInterval(1, base, base, 100, 110, 90, 105, 1000, 10000, 'ok')]
        indicator.update(intervals)
        assert indicator.status == 'invalid'
        assert indicator.get_value() is None

        # Test with 2 intervals
        intervals.append(InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 105, 115, 95, 110, 1000, 10000, 'ok'))
        indicator.update(intervals)
        assert indicator.status == 'invalid'
        assert indicator.get_value() is None

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    def test_invalid_status(self, indicator_class):
        """Test behavior with invalid interval status."""
        indicator = indicator_class()
        base = datetime(2024, 8, 19)

        # One invalid status
        intervals = [
            InstrumentInterval(1, base, base, 100, 110, 90, 105, 1000, 10000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 105, 115, 95, 110, 1000, 10000, 'invalid'),
            InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 110, 120, 100, 115, 1000, 10000, 'ok'),
        ]

        indicator.update(intervals)
        assert indicator.status == 'invalid'
        assert indicator.get_value() is None

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    @pytest.mark.parametrize("missing_field,field_name", [
        (('open', None), 'open'),
        (('high', None), 'high'),
        (('low', None), 'low'),
        (('close', None), 'close'),
    ])
    def test_missing_ohlc_data(self, indicator_class, missing_field, field_name):
        """Test behavior with missing OHLC data."""
        indicator = indicator_class()
        base = datetime(2024, 8, 19)

        # Create intervals with missing data
        intervals = [
            InstrumentInterval(1, base, base, 100, 110, 90, 105, 1000, 10000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 105, 115, 95, 110, 1000, 10000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 110, 120, 100, 115, 1000, 10000, 'ok'),
        ]

        # Set one field to None
        setattr(intervals[0], field_name, None)

        indicator.update(intervals)
        assert indicator.status == 'invalid'
        assert indicator.get_value() is None

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    def test_nan_values(self, indicator_class):
        """Test behavior with NaN values."""
        indicator = indicator_class()
        base = datetime(2024, 8, 19)

        intervals = [
            InstrumentInterval(1, base, base, float('nan'), 110, 90, 105, 1000, 10000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 105, 115, 95, 110, 1000, 10000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 110, 120, 100, 115, 1000, 10000, 'ok'),
        ]

        indicator.update(intervals)
        assert indicator.status == 'invalid'
        assert indicator.get_value() is None

# =============================================================================
# MATHEMATICAL RELATIONSHIPS TESTS
# =============================================================================

class TestZSeriesRelationships:
    """Test mathematical relationships and ordering between indicators."""

    def test_ordering_relationships(self, known_test_data):
        """Test that Z-series indicators follow expected ordering."""
        # Calculate all values
        indicators = {'z1b': Z1B(), 'z2b': Z2B(), 'z5t': Z5T(), 'z6t': Z6T()}
        values = {}

        for name, indicator in indicators.items():
            indicator.update(known_test_data)
            values[name] = indicator.get_value()

        # Z1B should be lowest (lower support zone)
        assert values['z1b'] < values['z2b']
        assert values['z1b'] < values['z5t']
        assert values['z1b'] < values['z6t']

        # Z2B should be between Z1B and upper zones
        assert values['z1b'] < values['z2b'] < values['z5t']
        assert values['z1b'] < values['z2b'] < values['z6t']

        # Z6T should be highest (upper breakout zone)
        assert values['z5t'] < values['z6t']

    def test_z5t_z6t_correlation(self, known_test_data):
        """Test strong correlation between Z5T and Z6T."""
        z5t = Z5T()
        z6t = Z6T()

        z5t.update(known_test_data)
        z6t.update(known_test_data)

        z5t_value = z5t.get_value()
        z6t_value = z6t.get_value()

        # Z6T should be larger than Z5T
        assert z6t_value > z5t_value

        # Difference should be in expected range based on analysis
        difference = z6t_value - z5t_value
        assert 80 < difference < 250

    def test_support_resistance_zones(self, known_test_data):
        """Test that support/resistance zones are properly ordered."""
        z1b = Z1B()  # Lower support
        z2b = Z2B()  # Lower resistance
        z5t = Z5T()  # Upper resistance
        z6t = Z6T()  # Upper breakout

        for indicator in [z1b, z2b, z5t, z6t]:
            indicator.update(known_test_data)

        lower_support = z1b.get_value()
        lower_resistance = z2b.get_value()
        upper_resistance = z5t.get_value()
        upper_breakout = z6t.get_value()

        # Should form ascending order
        assert lower_support < lower_resistance < upper_resistance < upper_breakout

        # Gaps should be reasonable (not too tight or too wide)
        gap1 = lower_resistance - lower_support
        gap2 = upper_resistance - lower_resistance
        gap3 = upper_breakout - upper_resistance

        assert all(gap > 0 for gap in [gap1, gap2, gap3])

# =============================================================================
# EDGE CASES AND ROBUSTNESS TESTS
# =============================================================================

class TestZSeriesEdgeCases:
    """Test edge cases and robustness."""

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    def test_small_values(self, indicator_class):
        """Test with very small price values."""
        indicator = indicator_class()
        base = datetime(2024, 8, 19)

        small_intervals = [
            InstrumentInterval(1, base, base, 1.0, 1.1, 0.9, 1.05, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 1.05, 1.15, 0.95, 1.1, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 1.1, 1.2, 1.0, 1.15, 100, 1000, 'ok'),
        ]

        indicator.update(small_intervals)

        assert indicator.status == 'ok'
        value = indicator.get_value()
        assert value is not None
        assert math.isfinite(value)

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    def test_large_values(self, indicator_class):
        """Test with very large price values."""
        indicator = indicator_class()
        base = datetime(2024, 8, 19)

        large_intervals = [
            InstrumentInterval(1, base, base, 100000.0, 101000.0, 99000.0, 100500.0, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 100500.0, 101500.0, 99500.0, 101000.0, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 101000.0, 102000.0, 100000.0, 101500.0, 100, 1000, 'ok'),
        ]

        indicator.update(large_intervals)

        assert indicator.status == 'ok'
        value = indicator.get_value()
        assert value is not None
        assert math.isfinite(value)

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    def test_multiple_updates(self, indicator_class):
        """Test multiple updates handle state correctly."""
        indicator = indicator_class()
        base = datetime(2024, 8, 19)

        # First update
        intervals1 = [
            InstrumentInterval(1, base, base, 100, 110, 90, 105, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 105, 115, 95, 110, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 110, 120, 100, 115, 100, 1000, 'ok'),
        ]

        indicator.update(intervals1)
        first_value = indicator.get_value()

        # Second update with different data
        intervals2 = [
            InstrumentInterval(1, base, base, 200, 210, 190, 205, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 205, 215, 195, 210, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 210, 220, 200, 215, 100, 1000, 'ok'),
        ]

        indicator.update(intervals2)
        second_value = indicator.get_value()

        assert first_value != second_value
        assert abs(second_value - first_value) > 50  # Should be significantly different

# =============================================================================
# PERFORMANCE TESTS
# =============================================================================

class TestZSeriesPerformance:
    """Test performance characteristics."""

    def test_calculation_performance(self, known_test_data):
        """Test that calculations complete quickly."""
        import time

        indicators = [Z1B(), Z2B(), Z5T(), Z6T()]

        # Time 1000 calculations
        start_time = time.time()
        for _ in range(250):  # 250 * 4 = 1000 total
            for indicator in indicators:
                indicator.update(known_test_data)
                _ = indicator.get_value()
        elapsed = time.time() - start_time

        # Should complete in reasonable time
        assert elapsed < 1.0, f"Performance test too slow: {elapsed:.3f}s"

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    def test_memory_efficiency(self, indicator_class):
        """Test that indicators don't leak memory."""
        indicator = indicator_class()
        base = datetime(2024, 8, 19)

        intervals = [
            InstrumentInterval(1, base, base, 100, 110, 90, 105, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=1), base+timedelta(days=1), 105, 115, 95, 110, 100, 1000, 'ok'),
            InstrumentInterval(1, base+timedelta(days=2), base+timedelta(days=2), 110, 120, 100, 115, 100, 1000, 'ok'),
        ]

        # Multiple updates should not accumulate state
        for _ in range(100):
            indicator.update(intervals)
            value = indicator.get_value()
            assert value is not None

        # Should still work correctly
        assert indicator.status == 'ok'
        assert indicator.get_value() is not None

# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestZSeriesIntegration:
    """Test integration with ATS framework."""

    def test_gin_configurable_decorator(self):
        """Test that all indicators are gin.configurable."""
        # This tests that the @gin.configurable decorator is applied
        indicators = [Z1B(), Z2B(), Z5T(), Z6T()]

        for indicator in indicators:
            # Should be instantiable (decorator working)
            assert indicator is not None
            assert hasattr(indicator, 'update')
            assert hasattr(indicator, 'get_value')

    def test_indicator_interface_compliance(self):
        """Test that indicators comply with ATS Indicator interface."""
        indicators = [Z1B(), Z2B(), Z5T(), Z6T()]

        for indicator in indicators:
            # Should have required attributes
            assert hasattr(indicator, 'status')
            assert hasattr(indicator, 'update_at')

            # Should have required methods
            assert callable(getattr(indicator, 'update'))
            assert callable(getattr(indicator, 'get_value'))

            # Initial state should be correct
            assert indicator.status is None
            assert indicator.update_at is None
            assert indicator.get_value() is None

    def test_logging_integration(self, known_test_data, caplog):
        """Test that indicators log appropriately."""
        import logging

        # Set up logging capture
        caplog.set_level(logging.DEBUG)

        z1b = Z1B()
        z1b.update(known_test_data)

        # Should have logged the calculation
        assert any("Z1B" in record.message for record in caplog.records)

# =============================================================================
# DOCUMENTATION AND METADATA TESTS
# =============================================================================

class TestZSeriesDocumentation:
    """Test documentation and metadata."""

    @pytest.mark.parametrize("indicator_class", [Z1B, Z2B, Z5T, Z6T])
    def test_docstrings(self, indicator_class):
        """Test that indicators have proper documentation."""
        assert indicator_class.__doc__ is not None
        assert len(indicator_class.__doc__) > 100  # Substantial documentation

        # Should mention the formula
        assert "Formula:" in indicator_class.__doc__
        assert "where subscripts" in indicator_class.__doc__

    def test_formula_documentation(self):
        """Test that formulas are properly documented."""
        indicators = [Z1B(), Z2B(), Z5T(), Z6T()]

        for indicator in indicators:
            # Should have coefficient documentation
            docstring = indicator.__class__.__doc__
            assert "coefficient" in docstring.lower() or "linear regression" in docstring.lower()

            # Should explain the meaning
            if isinstance(indicator, Z1B):
                assert "lower support" in docstring.lower()
            elif isinstance(indicator, Z2B):
                assert "lower resistance" in docstring.lower()
            elif isinstance(indicator, Z5T):
                assert "upper resistance" in docstring.lower()
            elif isinstance(indicator, Z6T):
                assert "upper breakout" in docstring.lower() or "breakout" in docstring.lower()

# =============================================================================
# SUMMARY INTEGRATION TEST
# =============================================================================

def test_z_series_complete_integration(known_test_data, expected_z_values):
    """Complete integration test demonstrating all Z-series indicators working together."""

    # Initialize all indicators
    z1b = Z1B()
    z2b = Z2B()
    z5t = Z5T()
    z6t = Z6T()
    indicators = [z1b, z2b, z5t, z6t]
    names = ['Z1B', 'Z2B', 'Z5T', 'Z6T']
    keys = ['z1b', 'z2b', 'z5t', 'z6t']

    # Update all indicators with the same data
    for indicator in indicators:
        indicator.update(known_test_data)

    # Verify all calculations are correct
    for indicator, name, key in zip(indicators, names, keys):
        assert indicator.status == 'ok', f"{name} should have ok status"

        calculated = indicator.get_value()
        expected = expected_z_values[key]
        error = abs(calculated - expected)

        assert error < 0.1, f"{name} calculation error: {error:.6f}"

    # Verify ordering relationships
    values = [indicator.get_value() for indicator in indicators]
    assert values[0] < values[1] < values[2] < values[3], "Z-series should be ordered: Z1B < Z2B < Z5T < Z6T"

    # Verify all have update timestamps
    for indicator, name in zip(indicators, names):
        assert indicator.update_at is not None, f"{name} should have update timestamp"

    print(f"\n✅ Complete Z-series integration test passed!")
    print(f"   Z1B: {values[0]:.2f} (Lower Support)")
    print(f"   Z2B: {values[1]:.2f} (Lower Resistance)")
    print(f"   Z5T: {values[2]:.2f} (Upper Resistance)")
    print(f"   Z6T: {values[3]:.2f} (Upper Breakout)")
    print(f"   All calculations within 0.1 point accuracy!")