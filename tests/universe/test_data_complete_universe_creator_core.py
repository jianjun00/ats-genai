import pytest
from unittest.mock import MagicMock
from datetime import date, datetime, timedelta

from domains.trading.services.data_complete_universe_creator import (
    DataCompleteUniverseCreator,
    DataCompleteness
)


class TestDataCompleteUniverseCreatorCore:
    """Core business logic tests for DataCompleteUniverseCreator."""

    def test_init_with_custom_environment(self):
        """Test initialization with custom environment."""
        custom_env = MagicMock()
        creator = DataCompleteUniverseCreator(env=custom_env)

        assert creator.env == custom_env
        assert creator.min_years == 5
        assert creator.min_daily_completeness == 0.95
        assert creator.min_minute_completeness == 0.85
        assert creator.min_overall_quality == 0.80

    def test_calculate_expected_trading_days_full_years(self):
        """Test expected trading days calculation for full years."""
        creator = DataCompleteUniverseCreator()

        # 5 years from 2019 to 2024
        start_date = date(2019, 1, 1)
        end_date = date(2024, 1, 1)

        result = creator._calculate_expected_trading_days(start_date, end_date)

        # Approximately 252 trading days per year * 5 years
        # Using 70% approximation from source code
        expected_days = (end_date - start_date).days + 1
        expected_trading = int(expected_days * 0.70)
        assert result == expected_trading

    def test_calculate_expected_trading_days_partial_year(self):
        """Test expected trading days calculation for partial year."""
        creator = DataCompleteUniverseCreator()

        # 6 months
        start_date = date(2023, 1, 1)
        end_date = date(2023, 7, 1)

        result = creator._calculate_expected_trading_days(start_date, end_date)

        expected_days = (end_date - start_date).days + 1
        expected_trading = int(expected_days * 0.70)
        assert result == expected_trading

    def test_calculate_expected_trading_days_none_dates(self):
        """Test expected trading days with None dates."""
        creator = DataCompleteUniverseCreator()

        result = creator._calculate_expected_trading_days(None, None)
        assert result == 0

        result = creator._calculate_expected_trading_days(date(2023, 1, 1), None)
        assert result == 0

    def test_calculate_expected_minute_bars_full_trading_days(self):
        """Test expected minute bars calculation."""
        creator = DataCompleteUniverseCreator()

        trading_days = 1260

        # 390 minute bars per trading day (6.5 hours * 60 minutes)
        result = creator._calculate_expected_minute_bars(trading_days)

        assert result == 491400  # 1260 * 390

    def test_calculate_expected_minute_bars_zero_days(self):
        """Test expected minute bars with zero trading days."""
        creator = DataCompleteUniverseCreator()

        result = creator._calculate_expected_minute_bars(0)
        assert result == 0

    def test_calculate_quality_score_high_quality(self):
        """Test quality score calculation for high-quality data."""
        creator = DataCompleteUniverseCreator()

        # High completeness ratios and high data counts
        daily_ratio = 0.98
        minute_ratio = 0.96
        daily_count = 1260  # > 1000, gets bonus
        minute_count = 550000  # > 500000, gets bonus

        score = creator._calculate_quality_score(daily_ratio, minute_ratio, daily_count, minute_count)

        # Base score: (0.98 * 0.3) + (0.96 * 0.7) = 0.294 + 0.672 = 0.966
        # Bonus: 0.05 (daily) + 0.05 (minute) = 0.10
        # Total: 0.966 + 0.10 = 1.066, but capped at 1.0
        expected = 1.0
        assert score == expected

    def test_calculate_quality_score_medium_quality(self):
        """Test quality score calculation for medium-quality data."""
        creator = DataCompleteUniverseCreator()

        daily_ratio = 0.90
        minute_ratio = 0.85
        daily_count = 1200  # > 1000, gets bonus
        minute_count = 400000  # < 500000, no bonus

        score = creator._calculate_quality_score(daily_ratio, minute_ratio, daily_count, minute_count)

        # Base score: (0.90 * 0.3) + (0.85 * 0.7) = 0.27 + 0.595 = 0.865
        # Bonus: 0.05 (daily only)
        # Total: 0.865 + 0.05 = 0.915
        expected = 0.915
        assert score == pytest.approx(expected, abs=0.001)

    def test_calculate_quality_score_low_quality(self):
        """Test quality score calculation for low-quality data."""
        creator = DataCompleteUniverseCreator()

        # Low completeness ratios and low data counts
        daily_ratio = 0.70
        minute_ratio = 0.60
        daily_count = 500  # < 1000, no bonus
        minute_count = 100000  # < 500000, no bonus

        score = creator._calculate_quality_score(daily_ratio, minute_ratio, daily_count, minute_count)

        # Base score only: (0.70 * 0.3) + (0.60 * 0.7) = 0.21 + 0.42 = 0.63
        # No bonus
        expected = 0.63
        assert score == pytest.approx(expected, abs=0.001)

    def test_filter_qualified_instruments_all_pass(self):
        """Test filtering instruments where all pass quality thresholds."""
        creator = DataCompleteUniverseCreator()

        instruments = [
            DataCompleteness(
                symbol="AAPL", instrument_id=1,
                daily_start_date=date(2018, 1, 1),  # > 5 years ago
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2018, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.96, overall_quality_score=0.97
            ),
            DataCompleteness(
                symbol="MSFT", instrument_id=2,
                daily_start_date=date(2017, 1, 1),  # > 5 years ago
                daily_end_date=date(2024, 1, 1), daily_count=1250,
                minute_start_date=datetime(2017, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=485250,
                minute_trading_days=1250, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.96,
                minute_completeness_ratio=0.96, overall_quality_score=0.96
            )
        ]

        result = creator._filter_qualified_instruments(instruments)

        assert len(result) == 2
        # Should be sorted by quality score descending
        assert result[0].symbol == "AAPL"  # Higher quality score
        assert result[1].symbol == "MSFT"

    def test_filter_qualified_instruments_some_fail_history(self):
        """Test filtering instruments where some fail history requirement."""
        creator = DataCompleteUniverseCreator()

        # Calculate cutoff date (5 years ago)
        cutoff_date = date.today() - timedelta(days=5 * 365)

        instruments = [
            # Good instrument - old enough
            DataCompleteness(
                symbol="AAPL", instrument_id=1,
                daily_start_date=cutoff_date - timedelta(days=365),  # 6 years ago
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.96, overall_quality_score=0.97
            ),
            # Bad instrument - too recent
            DataCompleteness(
                symbol="NEWSTOCK", instrument_id=3,
                daily_start_date=cutoff_date + timedelta(days=365),  # Too recent
                daily_end_date=date(2024, 1, 1), daily_count=800,
                minute_start_date=datetime(2022, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=300000,
                minute_trading_days=800, expected_daily_count=1000,
                expected_minute_count=400000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.96, overall_quality_score=0.97
            )
        ]

        result = creator._filter_qualified_instruments(instruments)

        assert len(result) == 1  # Only AAPL should pass
        assert result[0].symbol == "AAPL"

    def test_filter_qualified_instruments_fail_daily_threshold(self):
        """Test filtering instruments that fail daily completeness threshold."""
        creator = DataCompleteUniverseCreator()

        cutoff_date = date.today() - timedelta(days=6 * 365)

        instruments = [
            DataCompleteness(
                symbol="BADSTOCK", instrument_id=3,
                daily_start_date=cutoff_date,
                daily_end_date=date(2024, 1, 1), daily_count=800,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.90,  # Below 0.95
                minute_completeness_ratio=0.96, overall_quality_score=0.85
            )
        ]

        result = creator._filter_qualified_instruments(instruments)

        assert len(result) == 0

    def test_filter_qualified_instruments_fail_minute_threshold(self):
        """Test filtering instruments that fail minute completeness threshold."""
        creator = DataCompleteUniverseCreator()

        cutoff_date = date.today() - timedelta(days=6 * 365)

        instruments = [
            DataCompleteness(
                symbol="BADSTOCK", instrument_id=3,
                daily_start_date=cutoff_date,
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=300000,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.80,  # Below 0.85
                overall_quality_score=0.85
            )
        ]

        result = creator._filter_qualified_instruments(instruments)

        assert len(result) == 0

    def test_filter_qualified_instruments_fail_quality_threshold(self):
        """Test filtering instruments that fail overall quality threshold."""
        creator = DataCompleteUniverseCreator()

        cutoff_date = date.today() - timedelta(days=6 * 365)

        instruments = [
            DataCompleteness(
                symbol="BADSTOCK", instrument_id=3,
                daily_start_date=cutoff_date,
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.96, overall_quality_score=0.75  # Below 0.80
            )
        ]

        result = creator._filter_qualified_instruments(instruments)

        assert len(result) == 0

    def test_filter_qualified_instruments_empty_list(self):
        """Test filtering with empty instruments list."""
        creator = DataCompleteUniverseCreator()

        result = creator._filter_qualified_instruments([])

        assert result == []

    def test_quality_thresholds_configuration(self):
        """Test that quality thresholds can be configured."""
        creator = DataCompleteUniverseCreator()

        # Test default thresholds
        assert creator.min_daily_completeness == 0.95
        assert creator.min_minute_completeness == 0.85
        assert creator.min_overall_quality == 0.80
        assert creator.min_years == 5

        # Test threshold modification
        creator.min_daily_completeness = 0.90
        creator.min_minute_completeness = 0.80
        creator.min_overall_quality = 0.75
        creator.min_years = 3

        assert creator.min_daily_completeness == 0.90
        assert creator.min_minute_completeness == 0.80
        assert creator.min_overall_quality == 0.75
        assert creator.min_years == 3


class TestDataCompletenessDataclass:
    """Test DataCompleteness dataclass functionality."""

    def test_dataclass_creation_complete(self):
        """Test DataCompleteness dataclass creation with all fields."""
        completeness = DataCompleteness(
            symbol="AAPL",
            instrument_id=1,
            daily_start_date=date(2019, 1, 1),
            daily_end_date=date(2024, 1, 1),
            daily_count=1260,
            minute_start_date=datetime(2019, 1, 1, 9, 30),
            minute_end_date=datetime(2024, 1, 1, 16, 0),
            minute_count=489600,
            minute_trading_days=1260,
            expected_daily_count=1300,
            expected_minute_count=507000,
            daily_completeness_ratio=0.97,
            minute_completeness_ratio=0.96,
            overall_quality_score=0.97
        )

        assert completeness.symbol == "AAPL"
        assert completeness.instrument_id == 1
        assert completeness.daily_count == 1260
        assert completeness.minute_count == 489600
        assert completeness.overall_quality_score == 0.97
        assert completeness.daily_completeness_ratio == 0.97
        assert completeness.minute_completeness_ratio == 0.96

    def test_dataclass_creation_with_none_values(self):
        """Test DataCompleteness with None values for optional fields."""
        completeness = DataCompleteness(
            symbol="UNKNOWN",
            instrument_id=None,
            daily_start_date=None,
            daily_end_date=None,
            daily_count=0,
            minute_start_date=None,
            minute_end_date=None,
            minute_count=0,
            minute_trading_days=0,
            expected_daily_count=0,
            expected_minute_count=0,
            daily_completeness_ratio=0.0,
            minute_completeness_ratio=0.0,
            overall_quality_score=0.0
        )

        assert completeness.symbol == "UNKNOWN"
        assert completeness.instrument_id is None
        assert completeness.daily_start_date is None
        assert completeness.minute_start_date is None
        assert completeness.overall_quality_score == 0.0

    def test_dataclass_equality(self):
        """Test DataCompleteness equality comparison."""
        completeness1 = DataCompleteness(
            symbol="AAPL", instrument_id=1, daily_start_date=date(2019, 1, 1),
            daily_end_date=date(2024, 1, 1), daily_count=1260,
            minute_start_date=datetime(2019, 1, 1, 9, 30),
            minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
            minute_trading_days=1260, expected_daily_count=1300,
            expected_minute_count=507000, daily_completeness_ratio=0.97,
            minute_completeness_ratio=0.96, overall_quality_score=0.97
        )

        completeness2 = DataCompleteness(
            symbol="AAPL", instrument_id=1, daily_start_date=date(2019, 1, 1),
            daily_end_date=date(2024, 1, 1), daily_count=1260,
            minute_start_date=datetime(2019, 1, 1, 9, 30),
            minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
            minute_trading_days=1260, expected_daily_count=1300,
            expected_minute_count=507000, daily_completeness_ratio=0.97,
            minute_completeness_ratio=0.96, overall_quality_score=0.97
        )

        completeness3 = DataCompleteness(
            symbol="MSFT", instrument_id=2, daily_start_date=date(2019, 1, 1),
            daily_end_date=date(2024, 1, 1), daily_count=1250,
            minute_start_date=datetime(2019, 1, 1, 9, 30),
            minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=485250,
            minute_trading_days=1250, expected_daily_count=1300,
            expected_minute_count=507000, daily_completeness_ratio=0.96,
            minute_completeness_ratio=0.96, overall_quality_score=0.96
        )

        assert completeness1 == completeness2
        assert completeness1 != completeness3


class TestDataCompleteUniverseCreatorBusinessLogic:
    """Test core business logic and edge cases."""

    def test_quality_score_bonus_thresholds(self):
        """Test quality score bonus application based on data volume thresholds."""
        creator = DataCompleteUniverseCreator()

        # Test above thresholds (need > 1000 and > 500000)
        score_above_thresholds = creator._calculate_quality_score(0.90, 0.90, 1001, 500001)
        # Test at thresholds (exactly 1000 and 500000 - no bonus)
        score_at_thresholds = creator._calculate_quality_score(0.90, 0.90, 1000, 500000)

        # Base score: (0.90 * 0.3) + (0.90 * 0.7) = 0.27 + 0.63 = 0.90
        base_score = 0.90

        # Above thresholds: gets both bonuses (0.05 each)
        expected_above = base_score + 0.1
        # At thresholds: gets no bonuses (need strictly greater than)
        expected_at = base_score

        assert score_above_thresholds == pytest.approx(expected_above, abs=0.001)
        assert score_at_thresholds == pytest.approx(expected_at, abs=0.001)

    def test_quality_score_capping(self):
        """Test that quality score is properly capped at 1.0."""
        creator = DataCompleteUniverseCreator()

        # Perfect completeness with large bonuses
        score = creator._calculate_quality_score(1.0, 1.0, 2000, 1000000)

        # Should be capped at 1.0 despite bonuses potentially pushing it higher
        assert score == 1.0

    def test_filter_qualified_instruments_sorting(self):
        """Test that qualified instruments are sorted by quality score descending."""
        creator = DataCompleteUniverseCreator()

        cutoff_date = date.today() - timedelta(days=6 * 365)

        # Create instruments with different quality scores but all passing thresholds
        instruments = [
            DataCompleteness(
                symbol="LOW", instrument_id=1, daily_start_date=cutoff_date,
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.96,
                minute_completeness_ratio=0.86, overall_quality_score=0.82
            ),
            DataCompleteness(
                symbol="HIGH", instrument_id=2, daily_start_date=cutoff_date,
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.98,
                minute_completeness_ratio=0.97, overall_quality_score=0.98
            ),
            DataCompleteness(
                symbol="MED", instrument_id=3, daily_start_date=cutoff_date,
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.90, overall_quality_score=0.90
            )
        ]

        result = creator._filter_qualified_instruments(instruments)

        assert len(result) == 3
        assert result[0].symbol == "HIGH"  # Highest quality (0.98)
        assert result[1].symbol == "MED"   # Medium quality (0.90)
        assert result[2].symbol == "LOW"   # Lowest quality (0.82)

    def test_edge_case_none_daily_start_date(self):
        """Test filtering with None daily_start_date."""
        creator = DataCompleteUniverseCreator()

        instruments = [
            DataCompleteness(
                symbol="NODATE", instrument_id=1,
                daily_start_date=None,  # This should fail
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.96, overall_quality_score=0.97
            )
        ]

        result = creator._filter_qualified_instruments(instruments)

        # Should be filtered out due to None daily_start_date
        assert len(result) == 0