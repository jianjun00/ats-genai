import pytest
from datetime import datetime
from core.business.calendars.time_duration import TimeDuration
from domains.trading.services.state.instrument_interval import InstrumentInterval

class TestTimeDurationAggregate:
    """Test the missing aggregate_intervals method in TimeDuration."""

    def create_sample_interval(self, instrument_id, start_time, end_time,
                             open_price=100.0, high_price=105.0, low_price=95.0, close_price=102.0,
                             volume=1000, dollar_volume=102000, status='ok'):
        """Create a sample InstrumentInterval for testing."""
        return InstrumentInterval(
            instrument_id=instrument_id,
            start_date_time=start_time,
            end_date_time=end_time,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            traded_volume=volume,
            traded_dollar=dollar_volume,
            status=status
        )

    def test_aggregate_intervals_basic(self):
        """Test basic aggregation of multiple intervals."""
        duration = TimeDuration("15m")

        # Create 3 consecutive 5-minute intervals to aggregate into 15 minutes
        start_time = datetime(2023, 1, 1, 9, 30, 0)
        intervals = [
            self.create_sample_interval(1,
                start_time, start_time.replace(minute=35),
                open_price=100.0, high_price=103.0, low_price=99.0, close_price=101.0,
                volume=500, dollar_volume=50500),
            self.create_sample_interval(1,
                start_time.replace(minute=35), start_time.replace(minute=40),
                open_price=101.0, high_price=105.0, low_price=100.0, close_price=104.0,
                volume=700, dollar_volume=72100),
            self.create_sample_interval(1,
                start_time.replace(minute=40), start_time.replace(minute=45),
                open_price=104.0, high_price=106.0, low_price=102.0, close_price=103.0,
                volume=600, dollar_volume=62400)
        ]

        result = duration.aggregate_intervals(intervals)

        # Check aggregated values
        assert result.instrument_id == 1
        assert result.start_date_time == start_time
        assert result.end_date_time == start_time.replace(minute=45)
        assert result.open == 100.0  # First interval's open
        assert result.close == 103.0  # Last interval's close
        assert result.high == 106.0   # Max of all highs
        assert result.low == 99.0     # Min of all lows
        assert result.traded_volume == 1800  # Sum of volumes
        assert result.traded_dollar == 185000  # Sum of dollar volumes
        assert result.status == 'ok'

    def test_aggregate_intervals_single(self):
        """Test aggregation with single interval."""
        duration = TimeDuration("5m")

        start_time = datetime(2023, 1, 1, 9, 30, 0)
        intervals = [
            self.create_sample_interval(1,
                start_time, start_time.replace(minute=35),
                open_price=100.0, high_price=103.0, low_price=99.0, close_price=101.0,
                volume=500, dollar_volume=50500)
        ]

        result = duration.aggregate_intervals(intervals)

        # Should be identical to the single interval
        assert result.instrument_id == 1
        assert result.open == 100.0
        assert result.close == 101.0
        assert result.high == 103.0
        assert result.low == 99.0
        assert result.traded_volume == 500
        assert result.traded_dollar == 50500
        assert result.status == 'ok'

    def test_aggregate_intervals_unreliable_status(self):
        """Test aggregation with mixed status intervals."""
        duration = TimeDuration("15m")

        start_time = datetime(2023, 1, 1, 9, 30, 0)
        intervals = [
            self.create_sample_interval(1,
                start_time, start_time.replace(minute=35),
                status='ok'),
            self.create_sample_interval(1,
                start_time.replace(minute=35), start_time.replace(minute=40),
                status='unreliable'),  # One unreliable interval
            self.create_sample_interval(1,
                start_time.replace(minute=40), start_time.replace(minute=45),
                status='ok')
        ]

        result = duration.aggregate_intervals(intervals)

        # Should be marked as unreliable due to one unreliable interval
        assert result.status == 'unreliable'

    def test_aggregate_intervals_all_ok_status(self):
        """Test aggregation with all 'ok' status intervals."""
        duration = TimeDuration("15m")

        start_time = datetime(2023, 1, 1, 9, 30, 0)
        intervals = [
            self.create_sample_interval(1,
                start_time, start_time.replace(minute=35),
                status='ok'),
            self.create_sample_interval(1,
                start_time.replace(minute=35), start_time.replace(minute=40),
                status='ok'),
            self.create_sample_interval(1,
                start_time.replace(minute=40), start_time.replace(minute=45),
                status='ok')
        ]

        result = duration.aggregate_intervals(intervals)

        # Should be marked as 'ok'
        assert result.status == 'ok'

    def test_aggregate_intervals_empty_list(self):
        """Test aggregation with empty intervals list raises ValueError."""
        duration = TimeDuration("15m")

        with pytest.raises(ValueError, match="No intervals to aggregate"):
            duration.aggregate_intervals([])

    def test_aggregate_intervals_extreme_prices(self):
        """Test aggregation handles extreme price values correctly."""
        duration = TimeDuration("30m")

        start_time = datetime(2023, 1, 1, 9, 30, 0)
        intervals = [
            self.create_sample_interval(1,
                start_time, start_time.replace(minute=40),
                open_price=50.0, high_price=55.0, low_price=45.0, close_price=52.0),
            self.create_sample_interval(1,
                start_time.replace(minute=40), start_time.replace(minute=50),
                open_price=52.0, high_price=200.0, low_price=48.0, close_price=180.0),  # Extreme high
            self.create_sample_interval(1,
                start_time.replace(minute=50), start_time.replace(hour=10),
                open_price=180.0, high_price=185.0, low_price=10.0, close_price=150.0)  # Extreme low
        ]

        result = duration.aggregate_intervals(intervals)

        assert result.high == 200.0  # Extreme high captured
        assert result.low == 10.0    # Extreme low captured
        assert result.open == 50.0   # First open
        assert result.close == 150.0 # Last close

    def test_aggregate_intervals_zero_volume(self):
        """Test aggregation with zero volume intervals."""
        duration = TimeDuration("15m")

        start_time = datetime(2023, 1, 1, 9, 30, 0)
        intervals = [
            self.create_sample_interval(1,
                start_time, start_time.replace(minute=35),
                volume=0, dollar_volume=0),
            self.create_sample_interval(1,
                start_time.replace(minute=35), start_time.replace(minute=40),
                volume=1000, dollar_volume=100000),
            self.create_sample_interval(1,
                start_time.replace(minute=40), start_time.replace(minute=45),
                volume=0, dollar_volume=0)
        ]

        result = duration.aggregate_intervals(intervals)

        assert result.traded_volume == 1000
        assert result.traded_dollar == 100000

    def test_aggregate_intervals_different_instrument_ids(self):
        """Test aggregation assumes same instrument_id (uses first)."""
        duration = TimeDuration("15m")

        start_time = datetime(2023, 1, 1, 9, 30, 0)
        intervals = [
            self.create_sample_interval(1, start_time, start_time.replace(minute=35)),
            self.create_sample_interval(2, start_time.replace(minute=35), start_time.replace(minute=40)),  # Different ID
            self.create_sample_interval(3, start_time.replace(minute=40), start_time.replace(minute=45))   # Different ID
        ]

        result = duration.aggregate_intervals(intervals)

        # Should use the first interval's instrument_id
        assert result.instrument_id == 1

    def test_aggregate_intervals_missing_status_attribute(self):
        """Test aggregation handles intervals without status attribute."""
        duration = TimeDuration("15m")

        start_time = datetime(2023, 1, 1, 9, 30, 0)

        # Create interval without status attribute
        interval_no_status = self.create_sample_interval(1,
            start_time, start_time.replace(minute=35))
        delattr(interval_no_status, 'status')

        intervals = [
            interval_no_status,
            self.create_sample_interval(1,
                start_time.replace(minute=35), start_time.replace(minute=40),
                status='ok')
        ]

        result = duration.aggregate_intervals(intervals)

        # getattr(i, 'status', 'ok') handles missing status, but one interval without
        # explicit status='ok' means the all() check fails, resulting in 'unreliable'
        assert result.status == 'unreliable'