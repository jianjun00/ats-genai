from datetime import datetime
from state.indicator_interval import IndicatorInterval

class TestIndicatorInterval:
    """Test cases for IndicatorInterval class."""

    def create_sample_indicator_interval(self, instrument_id=1, start_time=None, end_time=None):
        """Create a sample IndicatorInterval for testing."""
        if start_time is None:
            start_time = datetime(2023, 1, 1, 9, 30, 0)
        if end_time is None:
            end_time = datetime(2023, 1, 1, 9, 35, 0)

        return IndicatorInterval(
            instrument_id=instrument_id,
            start_date_time=start_time,
            end_date_time=end_time
        )

    def test_init_basic(self):
        """Test basic initialization of IndicatorInterval."""
        start_time = datetime(2023, 1, 1, 9, 30, 0)
        end_time = datetime(2023, 1, 1, 9, 35, 0)

        interval = IndicatorInterval(
            instrument_id=1,
            start_date_time=start_time,
            end_date_time=end_time
        )

        assert interval.instrument_id == 1
        assert interval.start_date_time == start_time
        assert interval.end_date_time == end_time
        assert interval.indicators == {}

    def test_add_indicator(self):
        """Test adding indicators to the interval."""
        interval = self.create_sample_indicator_interval()
        update_time = datetime(2023, 1, 1, 9, 32, 0)

        interval.add_indicator('RSI', 65.5, 'ok', update_time)

        assert interval.has_indicator('RSI')
        assert interval.get_indicator_value('RSI') == 65.5
        assert interval.get_indicator_status('RSI') == 'ok'
        assert interval.indicators['RSI']['update_at'] == update_time

    def test_add_indicator_without_update_time(self):
        """Test adding indicator without specifying update_at uses current time."""
        interval = self.create_sample_indicator_interval()

        before_add = datetime.now()
        interval.add_indicator('MACD', -1.2, 'ok')
        after_add = datetime.now()

        assert interval.has_indicator('MACD')
        assert interval.get_indicator_value('MACD') == -1.2
        assert interval.get_indicator_status('MACD') == 'ok'

        update_time = interval.indicators['MACD']['update_at']
        assert before_add <= update_time <= after_add

    def test_get_indicator_value_missing(self):
        """Test getting value of non-existent indicator returns None."""
        interval = self.create_sample_indicator_interval()

        assert interval.get_indicator_value('NonExistent') is None

    def test_has_indicator(self):
        """Test has_indicator method."""
        interval = self.create_sample_indicator_interval()

        assert not interval.has_indicator('RSI')

        interval.add_indicator('RSI', 65.5, 'ok')

        assert interval.has_indicator('RSI')
        assert not interval.has_indicator('MACD')

    def test_get_indicator_names(self):
        """Test getting list of all indicator names."""
        interval = self.create_sample_indicator_interval()

        assert interval.get_indicator_names() == []

        interval.add_indicator('RSI', 65.5, 'ok')
        interval.add_indicator('MACD', -1.2, 'ok')
        interval.add_indicator('SMA', 150.0, 'ok')

        names = interval.get_indicator_names()
        assert set(names) == {'RSI', 'MACD', 'SMA'}
        assert len(names) == 3

    def test_is_indicator_valid(self):
        """Test checking if indicator is valid (status == 'ok')."""
        interval = self.create_sample_indicator_interval()

        # Non-existent indicator should return False
        assert not interval.is_indicator_valid('NonExistent')

        # Add valid indicator
        interval.add_indicator('RSI', 65.5, 'ok')
        assert interval.is_indicator_valid('RSI')

        # Add invalid indicator
        interval.add_indicator('BrokenIndicator', None, 'invalid')
        assert not interval.is_indicator_valid('BrokenIndicator')

    def test_overwrite_indicator(self):
        """Test overwriting an existing indicator."""
        interval = self.create_sample_indicator_interval()

        # Add initial indicator
        interval.add_indicator('RSI', 65.5, 'ok')
        assert interval.get_indicator_value('RSI') == 65.5
        assert interval.get_indicator_status('RSI') == 'ok'

        # Overwrite with new values
        interval.add_indicator('RSI', 72.3, 'unreliable')
        assert interval.get_indicator_value('RSI') == 72.3
        assert interval.get_indicator_status('RSI') == 'unreliable'
        assert not interval.is_indicator_valid('RSI')