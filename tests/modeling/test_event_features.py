"""
Tests for event feature extraction and event-driven modeling.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock
import asyncpg

from domains.ml.services.event_features import (
    EventPattern,
    EventFeatures,
    EventCalendar,
    EventSequenceExtractor,
    flatten_event_features_for_model
)
from state.universe_state_manager import UniverseStateManager


@pytest.fixture
def mock_connection_pool():
    """Mock database connection pool."""
    pool = Mock(spec=asyncpg.Pool)
    conn = Mock(spec=asyncpg.Connection)
    pool.acquire.return_value.__aenter__.return_value = conn
    pool.acquire.return_value.__aexit__.return_value = None
    return pool, conn


@pytest.fixture
def mock_env():
    """Mock environment configuration."""
    env = Mock()
    env.get_table_name.side_effect = lambda x: f"test_{x}"
    return env


@pytest.fixture
def mock_universe_state_manager():
    """Mock universe state manager."""
    manager = Mock(spec=UniverseStateManager)

    # Default price data
    default_prices = pd.DataFrame({
        'high': [102, 104, 106, 108, 110],
        'low': [98, 100, 102, 104, 106],
        'close': [100, 102, 104, 106, 108]
    })

    manager.get_lag_prices.return_value = default_prices
    manager.get_lead_prices.return_value = default_prices

    return manager


@pytest.fixture
def sample_events():
    """Sample events for testing."""
    return [
        {
            'event_date': datetime(2024, 1, 15).date(),
            'event_name': 'Federal Reserve Meeting',
            'type': 'economic',
            'subtype': 'monetary_policy',
            'importance': 'High',
            'metadata': {'country': 'US'}
        },
        {
            'event_date': datetime(2024, 1, 20).date(),
            'event_name': 'Earnings Release',
            'type': 'company',
            'subtype': 'earnings',
            'importance': 'Medium',
            'instrument_id': 123,
            'metadata': {}
        },
        {
            'event_date': datetime(2024, 1, 19).date(),
            'event_name': 'Options Expiration',
            'type': 'options_expiration',
            'subtype': 'monthly',
            'importance': 'Medium',
            'metadata': {'expiration_type': 'monthly'}
        }
    ]


class TestEventPattern:
    """Test EventPattern dataclass."""

    def test_event_pattern_creation(self):
        """Test EventPattern creation and properties."""
        pattern = EventPattern(
            event_type='earnings',
            importance='High',
            avg_reaction=0.05,
            volatility_spike=1.8,
            volume_surge=2.1,
            duration_days=3,
            confidence=0.85,
            sample_size=50
        )

        assert pattern.event_type == 'earnings'
        assert pattern.importance == 'High'
        assert pattern.avg_reaction == 0.05
        assert pattern.volatility_spike == 1.8
        assert pattern.volume_surge == 2.1
        assert pattern.duration_days == 3
        assert pattern.confidence == 0.85
        assert pattern.sample_size == 50


class TestEventFeatures:
    """Test EventFeatures dataclass."""

    def test_event_features_creation(self, sample_events):
        """Test EventFeatures creation."""
        pattern = EventPattern(
            event_type='earnings',
            importance='High',
            avg_reaction=0.05,
            volatility_spike=1.8,
            volume_surge=2.1,
            duration_days=3,
            confidence=0.85,
            sample_size=50
        )

        features = EventFeatures(
            instrument_id=123,
            date=datetime(2024, 1, 10),
            upcoming_events=sample_events,
            historical_patterns={'earnings': pattern},
            pre_event_sequences={'earnings_sequence': np.array([0.01, -0.02, 0.015])},
            event_proximity_score=0.8,
            event_importance_weighted_score=0.65
        )

        assert features.instrument_id == 123
        assert features.date == datetime(2024, 1, 10)
        assert len(features.upcoming_events) == 3
        assert 'earnings' in features.historical_patterns
        assert 'earnings_sequence' in features.pre_event_sequences
        assert features.event_proximity_score == 0.8
        assert features.event_importance_weighted_score == 0.65


class TestEventCalendar:
    """Test EventCalendar functionality."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_upcoming_events(self, mock_connection_pool, mock_env, sample_events):
        """Test getting upcoming events."""
        pool, conn = mock_connection_pool

        # Mock database responses
        economic_events = [
            {
                'event_date': datetime(2024, 1, 15).date(),
                'event_name': 'Federal Reserve Meeting',
                'event_type': 'monetary_policy',
                'importance': 'High',
                'country': 'US',
                'actual_value': None,
                'forecast_value': None,
                'previous_value': None
            }
        ]

        company_events = [
            {
                'event_date': datetime(2024, 1, 20).date(),
                'event_type': 'earnings',
                'event_description': 'Earnings Release',
                'importance': 'Medium'
            }
        ]

        def mock_fetch(query, *args):
            if 'economic_events' in query:
                return economic_events
            elif 'company_events' in query:
                return company_events
            else:
                return []

        def mock_fetchval(query, *args):
            return True  # Table exists

        conn.fetch.side_effect = mock_fetch
        conn.fetchval.side_effect = mock_fetchval

        calendar = EventCalendar(pool, mock_env)

        events = await calendar.get_upcoming_events(
            datetime(2024, 1, 10),
            instrument_id=123,
            days_ahead=15
        )

        assert isinstance(events, list)
        assert len(events) >= 1  # Should have at least economic events + options expiration

        # Check event structure
        for event in events:
            assert 'event_date' in event
            assert 'event_name' in event
            assert 'type' in event
            assert 'importance' in event

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_economic_events_no_table(self, mock_connection_pool, mock_env):
        """Test economic events when table doesn't exist."""
        pool, conn = mock_connection_pool

        conn.fetchval.return_value = False  # Table doesn't exist

        calendar = EventCalendar(pool, mock_env)

        events = await calendar._get_economic_events(
            datetime(2024, 1, 1),
            datetime(2024, 1, 10)
        )

        assert events == []

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_company_events(self, mock_connection_pool, mock_env):
        """Test company events retrieval."""
        pool, conn = mock_connection_pool

        company_events = [
            {
                'event_date': datetime(2024, 1, 20).date(),
                'event_type': 'earnings',
                'event_description': 'Q4 Earnings Release',
                'importance': 'High'
            }
        ]

        conn.fetchval.return_value = True  # Table exists
        conn.fetch.return_value = company_events

        calendar = EventCalendar(pool, mock_env)

        events = await calendar._get_company_events(
            123,
            datetime(2024, 1, 15),
            datetime(2024, 1, 25)
        )

        assert len(events) == 1
        assert events[0]['type'] == 'company'
        assert events[0]['subtype'] == 'earnings'
        assert events[0]['instrument_id'] == 123

    def test_get_options_expirations(self, mock_connection_pool, mock_env):
        """Test options expiration calculation."""
        calendar = EventCalendar(None, None)

        # Test for January 2024 (third Friday should be Jan 19)
        events = calendar._get_options_expirations(
            datetime(2024, 1, 1),
            datetime(2024, 1, 31)
        )

        assert len(events) >= 1

        # Find the monthly expiration
        monthly_exp = [e for e in events if e['metadata']['expiration_type'] == 'monthly']
        assert len(monthly_exp) >= 1

        # January 2024 third Friday should be 19th
        jan_exp = [e for e in monthly_exp if e['event_date'].month == 1]
        assert len(jan_exp) == 1
        assert jan_exp[0]['event_date'].day == 19

    def test_get_calendar_events(self, mock_connection_pool, mock_env):
        """Test calendar events (month end, quarter end)."""
        calendar = EventCalendar(None, None)

        # Test for end of March 2024 (quarter end)
        events = calendar._get_calendar_events(
            datetime(2024, 3, 28),
            datetime(2024, 3, 31)
        )

        # Should have month end event
        month_end_events = [e for e in events if e['type'] == 'quarter_end']
        assert len(month_end_events) >= 1

        quarter_end = month_end_events[0]
        assert quarter_end['importance'] == 'High'  # Quarter end is high importance
        assert quarter_end['metadata']['quarter'] == 1  # Q1


class TestEventSequenceExtractor:
    """Test EventSequenceExtractor functionality."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_extract_event_features(self, mock_universe_state_manager, sample_events):
        """Test event feature extraction."""
        # Mock event calendar
        event_calendar = Mock()
        event_calendar.get_upcoming_events = AsyncMock(return_value=sample_events)

        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            event_calendar,
            lookback_days=5,
            forward_days=3
        )

        features = await extractor.extract_event_features(
            datetime(2024, 1, 10),
            123
        )

        assert isinstance(features, EventFeatures)
        assert features.instrument_id == 123
        assert features.date == datetime(2024, 1, 10)
        assert len(features.upcoming_events) == 3
        assert isinstance(features.event_proximity_score, float)
        assert isinstance(features.event_importance_weighted_score, float)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_historical_event_pattern(self, mock_universe_state_manager):
        """Test historical event pattern analysis."""
        event_calendar = Mock()
        historical_events = [
            {'event_date': datetime(2023, 10, 15), 'type': 'earnings', 'importance': 'High'},
            {'event_date': datetime(2023, 7, 15), 'type': 'earnings', 'importance': 'High'},
        ]
        event_calendar.get_upcoming_events = AsyncMock(return_value=historical_events)

        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            event_calendar
        )

        # Mock the reaction analysis
        with patch.object(extractor, '_analyze_event_reaction') as mock_analyze:
            mock_analyze.return_value = {
                'price_change': 0.05,
                'volatility_change': 1.5,
                'volume_change': 2.0,
                'duration': 3
            }

            pattern = await extractor._get_historical_event_pattern(
                'earnings', 'High', 123
            )

        assert isinstance(pattern, EventPattern)
        assert pattern.event_type == 'earnings'
        assert pattern.importance == 'High'
        assert pattern.avg_reaction > 0
        assert pattern.confidence > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_event_reaction(self, mock_universe_state_manager):
        """Test event reaction analysis."""
        # Setup specific price data for reaction analysis
        pre_event_data = pd.DataFrame({
            'close': [100, 101, 102],
            'high': [102, 103, 104],
            'low': [98, 99, 100]
        })

        post_event_data = pd.DataFrame({
            'close': [105, 107, 106],
            'high': [107, 109, 108],
            'low': [103, 105, 104]
        })

        def mock_get_prices(instrument_id, date, days):
            if 'lag' in str(mock_universe_state_manager.get_lag_prices.call_args):
                return pre_event_data
            else:
                return post_event_data

        mock_universe_state_manager.get_lag_prices.return_value = pre_event_data
        mock_universe_state_manager.get_lead_prices.return_value = post_event_data

        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            Mock()
        )

        reaction = await extractor._analyze_event_reaction(
            123,
            datetime(2024, 1, 15)
        )

        assert reaction is not None
        assert 'price_change' in reaction
        assert 'volatility_change' in reaction
        assert 'volume_change' in reaction
        assert 'duration' in reaction

        # Price change should be positive (from 102 to max of 109)
        assert reaction['price_change'] > 0

    def test_create_default_pattern(self, mock_universe_state_manager):
        """Test default pattern creation."""
        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            Mock()
        )

        pattern = extractor._create_default_pattern('earnings', 'High')

        assert isinstance(pattern, EventPattern)
        assert pattern.event_type == 'earnings'
        assert pattern.importance == 'High'
        assert pattern.confidence == 0.3  # Low confidence for defaults
        assert pattern.sample_size == 0
        assert pattern.avg_reaction > 0  # Should have positive default for earnings

    def test_extract_pre_event_sequences(self, mock_universe_state_manager):
        """Test pre-event sequence extraction."""
        # Setup price data with OHLC columns
        price_data = pd.DataFrame({
            'open': [100, 102, 101, 103],
            'high': [102, 104, 103, 105],
            'low': [98, 100, 99, 101],
            'close': [101, 103, 102, 104]
        })

        mock_universe_state_manager.get_lag_prices.return_value = price_data

        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            Mock(),
            lookback_days=5
        )

        upcoming_events = [
            {
                'type': 'earnings',
                'event_date': datetime(2024, 1, 12).date()  # 2 days ahead
            }
        ]

        sequences = extractor._extract_pre_event_sequences(
            datetime(2024, 1, 10),
            123,
            upcoming_events
        )

        assert isinstance(sequences, dict)
        assert 'earnings_sequence' in sequences
        assert isinstance(sequences['earnings_sequence'], np.ndarray)

    def test_calculate_event_proximity_score(self, mock_universe_state_manager):
        """Test event proximity score calculation."""
        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            Mock()
        )

        current_date = datetime(2024, 1, 10)
        upcoming_events = [
            {'event_date': datetime(2024, 1, 11).date()},  # 1 day away
            {'event_date': datetime(2024, 1, 15).date()},  # 5 days away
        ]

        score = extractor._calculate_event_proximity_score(upcoming_events, current_date)

        assert isinstance(score, float)
        assert 0 <= score <= 1
        # Should be higher because one event is very close
        assert score > 0.5

    def test_calculate_importance_weighted_score(self, mock_universe_state_manager):
        """Test importance-weighted score calculation."""
        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            Mock()
        )

        current_date = datetime(2024, 1, 10)
        upcoming_events = [
            {
                'event_date': datetime(2024, 1, 11).date(),
                'importance': 'High'
            },
            {
                'event_date': datetime(2024, 1, 15).date(),
                'importance': 'Medium'
            }
        ]

        score = extractor._calculate_importance_weighted_score(upcoming_events, current_date)

        assert isinstance(score, float)
        assert score >= 0
        # Should be higher due to high-importance nearby event
        assert score > 0.5


class TestFlattenEventFeatures:
    """Test event feature flattening for model input."""

    def test_flatten_event_features_for_model(self, sample_events):
        """Test flattening event features for model input."""
        # Create sample event features
        pattern = EventPattern(
            event_type='earnings',
            importance='High',
            avg_reaction=0.05,
            volatility_spike=1.8,
            volume_surge=2.1,
            duration_days=3,
            confidence=0.85,
            sample_size=50
        )

        features = EventFeatures(
            instrument_id=123,
            date=datetime(2024, 1, 10),
            upcoming_events=sample_events,
            historical_patterns={'earnings': pattern},
            pre_event_sequences={'earnings_sequence': np.array([0.01, -0.02, 0.015, 0.008])},
            event_proximity_score=0.8,
            event_importance_weighted_score=0.65
        )

        flattened = flatten_event_features_for_model(features)

        assert isinstance(flattened, dict)

        # Check basic scores
        assert 'event_proximity_score' in flattened
        assert 'event_importance_weighted_score' in flattened
        assert flattened['event_proximity_score'] == 0.8
        assert flattened['event_importance_weighted_score'] == 0.65

        # Check pattern features
        assert 'earnings_avg_reaction' in flattened
        assert 'earnings_volatility_spike' in flattened
        assert 'earnings_confidence' in flattened
        assert flattened['earnings_avg_reaction'] == 0.05

        # Check sequence features
        sequence_features = [k for k in flattened.keys() if 'earnings_sequence_' in k]
        assert len(sequence_features) == 4  # Should have 4 sequence values

        # Check event counts
        count_features = [k for k in flattened.keys() if '_count' in k]
        assert len(count_features) >= 1  # Should have at least one count feature

    def test_flatten_empty_event_features(self):
        """Test flattening empty event features."""
        features = EventFeatures(
            instrument_id=123,
            date=datetime(2024, 1, 10),
            upcoming_events=[],
            historical_patterns={},
            pre_event_sequences={},
            event_proximity_score=0.0,
            event_importance_weighted_score=0.0
        )

        flattened = flatten_event_features_for_model(features)

        assert isinstance(flattened, dict)
        assert flattened['event_proximity_score'] == 0.0
        assert flattened['event_importance_weighted_score'] == 0.0
        # Should have minimal features for empty data

    def test_flatten_large_sequence_truncation(self, sample_events):
        """Test that large sequences are properly truncated."""
        # Create large sequence
        large_sequence = np.random.random(50)  # 50 values

        features = EventFeatures(
            instrument_id=123,
            date=datetime(2024, 1, 10),
            upcoming_events=sample_events,
            historical_patterns={},
            pre_event_sequences={'large_sequence': large_sequence},
            event_proximity_score=0.8,
            event_importance_weighted_score=0.65
        )

        flattened = flatten_event_features_for_model(features)

        # Should only have first 20 values
        sequence_features = [k for k in flattened.keys() if 'large_sequence_' in k]
        assert len(sequence_features) == 20


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_extract_event_features_no_upcoming_events(self, mock_universe_state_manager):
        """Test event feature extraction with no upcoming events."""
        event_calendar = Mock()
        event_calendar.get_upcoming_events = AsyncMock(return_value=[])

        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            event_calendar
        )

        features = await extractor.extract_event_features(datetime(2024, 1, 10), 123)

        assert isinstance(features, EventFeatures)
        assert len(features.upcoming_events) == 0
        assert features.event_proximity_score == 0.0
        assert features.event_importance_weighted_score == 0.0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_extract_event_features_exception_handling(self, mock_universe_state_manager):
        """Test graceful handling of exceptions during feature extraction."""
        event_calendar = Mock()
        event_calendar.get_upcoming_events = AsyncMock(side_effect=Exception("Database error"))

        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            event_calendar
        )

        # Should not raise exception, but return empty features
        features = await extractor.extract_event_features(datetime(2024, 1, 10), 123)

        assert isinstance(features, EventFeatures)
        assert len(features.upcoming_events) == 0
        assert features.event_proximity_score == 0.0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_event_reaction_no_data(self, mock_universe_state_manager):
        """Test event reaction analysis with no price data."""
        mock_universe_state_manager.get_lag_prices.return_value = pd.DataFrame()
        mock_universe_state_manager.get_lead_prices.return_value = pd.DataFrame()

        extractor = EventSequenceExtractor(
            mock_universe_state_manager,
            Mock()
        )

        reaction = await extractor._analyze_event_reaction(123, datetime(2024, 1, 15))

        assert reaction is None

    def test_options_expiration_edge_dates(self):
        """Test options expiration calculation for edge cases."""
        calendar = EventCalendar(None, None)

        # Test for February (short month)
        events = calendar._get_options_expirations(
            datetime(2024, 2, 1),
            datetime(2024, 2, 29)  # Leap year
        )

        feb_events = [e for e in events if e['event_date'].month == 2]
        assert len(feb_events) == 1

        # February 2024 third Friday should be 16th
        assert feb_events[0]['event_date'].day == 16

    def test_calendar_events_year_boundary(self):
        """Test calendar events across year boundary."""
        calendar = EventCalendar(None, None)

        # Test December to January transition
        events = calendar._get_calendar_events(
            datetime(2023, 12, 29),
            datetime(2024, 1, 2)
        )

        # Should have December quarter end
        dec_events = [e for e in events if e['event_date'].month == 12]
        assert len(dec_events) >= 1

        quarter_end = [e for e in dec_events if e['type'] == 'quarter_end']
        assert len(quarter_end) >= 1


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_integration_workflow(mock_connection_pool, mock_env, mock_universe_state_manager):
    """Test complete integration workflow."""
    pool, conn = mock_connection_pool

    # Mock complete event data
    conn.fetchval.return_value = True  # Tables exist
    conn.fetch.return_value = [
        {
            'event_date': datetime(2024, 1, 15).date(),
            'event_name': 'Fed Meeting',
            'event_type': 'monetary_policy',
            'importance': 'High',
            'country': 'US',
            'actual_value': None,
            'forecast_value': None,
            'previous_value': None
        }
    ]

    # Create event calendar and extractor
    event_calendar = EventCalendar(pool, mock_env)
    extractor = EventSequenceExtractor(
        mock_universe_state_manager,
        event_calendar,
        lookback_days=5,
        forward_days=3
    )

    # Extract features
    features = await extractor.extract_event_features(datetime(2024, 1, 10), 123)

    # Flatten for model
    flattened = flatten_event_features_for_model(features)

    # Validate complete workflow
    assert isinstance(flattened, dict)
    assert len(flattened) > 0
    assert 'event_proximity_score' in flattened

    # All values should be numeric and finite
    for key, value in flattened.items():
        if isinstance(value, (int, float)):
            assert np.isfinite(value)


if __name__ == "__main__":
    pytest.main([__file__])