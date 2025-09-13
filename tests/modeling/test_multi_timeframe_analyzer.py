"""
Tests for multi-timeframe analysis system.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock

from domains.ml.services.multi_timeframe_analyzer import (
    TimeFrame,
    TimeFrameConfig,
    MultiTimeFrameFeatures,
    TimeFrameAggregator,
    MultiTimeFrameAnalyzer,
    flatten_multi_timeframe_features,
    analyze_multi_timeframe_patterns
)
from domains.ml.services.llm_pattern_recognition import LLMPatternRecognizer, PatternAnalysis
from state.universe_state_manager import UniverseStateManager


@pytest.fixture
def sample_daily_data():
    """Sample daily price data."""
    dates = pd.date_range('2024-01-01', '2024-02-29', freq='D')  # 2 months
    np.random.seed(42)

    base_price = 100
    prices = [base_price]

    for i in range(len(dates) - 1):
        change = np.random.normal(0.001, 0.02)  # Slight upward trend with volatility
        prices.append(prices[-1] * (1 + change))

    return pd.DataFrame({
        'open': [p * (1 + np.random.normal(0, 0.005)) for p in prices],
        'high': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
        'low': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
        'close': prices,
        'volume': [np.random.randint(100000, 1000000) for _ in prices]
    }, index=dates)


@pytest.fixture
def mock_universe_state_manager():
    """Mock universe state manager."""
    manager = Mock(spec=UniverseStateManager)

    # Will be set by individual tests
    manager.get_lag_prices.return_value = pd.DataFrame()

    return manager


@pytest.fixture
def mock_llm_recognizer():
    """Mock LLM pattern recognizer."""
    recognizer = Mock(spec=LLMPatternRecognizer)

    # Default pattern analysis
    pattern_analysis = PatternAnalysis(
        pattern_type="ascending_triangle",
        confidence=0.8,
        description="Test pattern",
        technical_indicators={
            "trend_direction": "up",
            "momentum": "strong",
            "volume_confirmation": "confirmed"
        },
        predicted_direction="bullish",
        support_resistance={"support": 95.0, "resistance": 105.0},
        volume_analysis="Strong volume",
        risk_assessment="Low risk",
        timeframe_relevance=["short_term", "medium_term"]
    )

    recognizer.analyze_price_pattern = AsyncMock(return_value=pattern_analysis)

    return recognizer


class TestTimeFrame:
    """Test TimeFrame enum."""

    def test_timeframe_values(self):
        """Test TimeFrame enum values."""
        assert TimeFrame.QUARTERLY.value == "quarterly"
        assert TimeFrame.MONTHLY.value == "monthly"
        assert TimeFrame.WEEKLY.value == "weekly"
        assert TimeFrame.DAILY.value == "daily"
        assert TimeFrame.HOURLY.value == "hourly"


class TestTimeFrameConfig:
    """Test TimeFrameConfig dataclass."""

    def test_timeframe_config_creation(self):
        """Test TimeFrameConfig creation."""
        config = TimeFrameConfig(
            timeframe=TimeFrame.WEEKLY,
            lookback_periods=52,
            aggregation_method='ohlc',
            weight=0.3,
            min_data_points=12,
            pattern_sensitivity=0.7
        )

        assert config.timeframe == TimeFrame.WEEKLY
        assert config.lookback_periods == 52
        assert config.aggregation_method == 'ohlc'
        assert config.weight == 0.3
        assert config.min_data_points == 12
        assert config.pattern_sensitivity == 0.7


class TestMultiTimeFrameFeatures:
    """Test MultiTimeFrameFeatures dataclass."""

    def test_multi_timeframe_features_creation(self):
        """Test MultiTimeFrameFeatures creation."""
        features = MultiTimeFrameFeatures(
            daily_features={'daily_rsi': 60.0, 'daily_momentum': 0.05},
            weekly_features={'weekly_trend': 1.0, 'weekly_volume': 1.2},
            monthly_features={'monthly_ma_cross': 1.0},
            quarterly_features={'quarterly_trend': 1.0},
            cross_timeframe_features={'trend_alignment': 0.8},
            timeframe_alignment={'daily_weekly_alignment': 0.75},
            dominant_trend="bullish",
            trend_strength=0.85
        )

        assert len(features.daily_features) == 2
        assert len(features.weekly_features) == 2
        assert features.dominant_trend == "bullish"
        assert features.trend_strength == 0.85
        assert 'trend_alignment' in features.cross_timeframe_features


class TestTimeFrameAggregator:
    """Test TimeFrameAggregator functionality."""

    def test_aggregate_to_weekly(self, sample_daily_data):
        """Test weekly aggregation."""
        weekly_data = TimeFrameAggregator.aggregate_to_weekly(sample_daily_data)

        assert isinstance(weekly_data, pd.DataFrame)
        assert not weekly_data.empty
        assert len(weekly_data) < len(sample_daily_data)  # Should be fewer weekly than daily

        # Check OHLC columns
        assert 'open' in weekly_data.columns
        assert 'high' in weekly_data.columns
        assert 'low' in weekly_data.columns
        assert 'close' in weekly_data.columns
        assert 'volume' in weekly_data.columns

        # Check OHLC consistency
        assert (weekly_data['high'] >= weekly_data['open']).all()
        assert (weekly_data['high'] >= weekly_data['close']).all()
        assert (weekly_data['low'] <= weekly_data['open']).all()
        assert (weekly_data['low'] <= weekly_data['close']).all()

    def test_aggregate_to_monthly(self, sample_daily_data):
        """Test monthly aggregation."""
        monthly_data = TimeFrameAggregator.aggregate_to_monthly(sample_daily_data)

        assert isinstance(monthly_data, pd.DataFrame)
        assert not monthly_data.empty
        assert len(monthly_data) <= 2  # 2 months of data

        # Check OHLC consistency
        assert (monthly_data['high'] >= monthly_data['low']).all()

    def test_aggregate_to_quarterly(self, sample_daily_data):
        """Test quarterly aggregation."""
        quarterly_data = TimeFrameAggregator.aggregate_to_quarterly(sample_daily_data)

        assert isinstance(quarterly_data, pd.DataFrame)
        assert not quarterly_data.empty
        assert len(quarterly_data) == 1  # Should have 1 quarter for 2 months

    def test_aggregate_empty_data(self):
        """Test aggregation with empty data."""
        empty_data = pd.DataFrame()

        weekly = TimeFrameAggregator.aggregate_to_weekly(empty_data)
        monthly = TimeFrameAggregator.aggregate_to_monthly(empty_data)
        quarterly = TimeFrameAggregator.aggregate_to_quarterly(empty_data)

        assert weekly.empty
        assert monthly.empty
        assert quarterly.empty

    def test_aggregate_insufficient_data(self):
        """Test aggregation with insufficient data."""
        # Single day of data
        single_day = pd.DataFrame({
            'open': [100],
            'high': [102],
            'low': [98],
            'close': [101],
            'volume': [1000]
        }, index=[datetime(2024, 1, 1)])

        weekly = TimeFrameAggregator.aggregate_to_weekly(single_day)

        # Should still work, just return the single aggregated period
        assert not weekly.empty
        assert len(weekly) == 1

    def test_aggregate_missing_date_column(self):
        """Test aggregation when date is not in index."""
        data_with_date_col = pd.DataFrame({
            'date': pd.date_range('2024-01-01', '2024-01-10'),
            'open': [100] * 10,
            'high': [102] * 10,
            'low': [98] * 10,
            'close': [101] * 10,
            'volume': [1000] * 10
        })

        weekly = TimeFrameAggregator.aggregate_to_weekly(data_with_date_col)

        assert not weekly.empty

    def test_aggregate_missing_volume(self, sample_daily_data):
        """Test aggregation without volume column."""
        data_no_volume = sample_daily_data.drop(columns=['volume'])

        weekly = TimeFrameAggregator.aggregate_to_weekly(data_no_volume)

        assert not weekly.empty
        # Should not have volume column in result
        assert 'volume' not in weekly.columns


class TestMultiTimeFrameAnalyzer:
    """Test MultiTimeFrameAnalyzer functionality."""

    def test_analyzer_initialization(self, mock_universe_state_manager, mock_llm_recognizer):
        """Test analyzer initialization."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        assert analyzer.universe_state_manager == mock_universe_state_manager
        assert analyzer.llm_recognizer == mock_llm_recognizer
        assert isinstance(analyzer.aggregator, TimeFrameAggregator)

        # Check default configurations
        assert TimeFrame.QUARTERLY in analyzer.timeframe_configs
        assert TimeFrame.MONTHLY in analyzer.timeframe_configs
        assert TimeFrame.WEEKLY in analyzer.timeframe_configs
        assert TimeFrame.DAILY in analyzer.timeframe_configs

        # Check config properties
        quarterly_config = analyzer.timeframe_configs[TimeFrame.QUARTERLY]
        assert quarterly_config.weight == 0.4  # Highest weight for long-term
        assert quarterly_config.lookback_periods == 8

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_multi_timeframe_basic(self, mock_universe_state_manager, mock_llm_recognizer, sample_daily_data):
        """Test basic multi-timeframe analysis."""
        # Setup mock to return sample data
        mock_universe_state_manager.get_lag_prices.return_value = sample_daily_data

        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        features = await analyzer.analyze_multi_timeframe(
            instrument_id=123,
            current_date=datetime(2024, 2, 15),
            symbol="AAPL"
        )

        assert isinstance(features, MultiTimeFrameFeatures)
        assert isinstance(features.daily_features, dict)
        assert isinstance(features.weekly_features, dict)
        assert isinstance(features.monthly_features, dict)
        assert isinstance(features.quarterly_features, dict)
        assert isinstance(features.cross_timeframe_features, dict)
        assert isinstance(features.timeframe_alignment, dict)
        assert features.dominant_trend in ["bullish", "bearish", "neutral"]
        assert 0 <= features.trend_strength <= 1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_multi_timeframe_no_data(self, mock_universe_state_manager, mock_llm_recognizer):
        """Test multi-timeframe analysis with no data."""
        # Setup mock to return empty data
        mock_universe_state_manager.get_lag_prices.return_value = pd.DataFrame()

        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        features = await analyzer.analyze_multi_timeframe(
            instrument_id=123,
            current_date=datetime(2024, 2, 15),
            symbol="AAPL"
        )

        # Should return empty features
        assert isinstance(features, MultiTimeFrameFeatures)
        assert len(features.daily_features) == 0
        assert len(features.weekly_features) == 0
        assert features.dominant_trend == "neutral"
        assert features.trend_strength == 0.0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_prepare_timeframe_data(self, mock_universe_state_manager, sample_daily_data):
        """Test timeframe data preparation."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, None)

        timeframe_data = await analyzer._prepare_timeframe_data(sample_daily_data)

        assert isinstance(timeframe_data, dict)
        assert TimeFrame.DAILY in timeframe_data
        assert TimeFrame.WEEKLY in timeframe_data
        assert TimeFrame.MONTHLY in timeframe_data
        assert TimeFrame.QUARTERLY in timeframe_data

        # Daily data should be same as input
        assert len(timeframe_data[TimeFrame.DAILY]) == len(sample_daily_data)

        # Other timeframes should be aggregated
        assert len(timeframe_data[TimeFrame.WEEKLY]) < len(sample_daily_data)
        assert len(timeframe_data[TimeFrame.MONTHLY]) <= 2
        assert len(timeframe_data[TimeFrame.QUARTERLY]) == 1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_single_timeframe(self, mock_universe_state_manager, mock_llm_recognizer, sample_daily_data):
        """Test single timeframe analysis."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        # Test daily timeframe analysis
        daily_analysis = await analyzer._analyze_single_timeframe(
            sample_daily_data,
            TimeFrame.DAILY,
            "AAPL"
        )

        assert isinstance(daily_analysis, dict)
        assert len(daily_analysis) > 0

        # Features should be prefixed with timeframe
        for key in daily_analysis.keys():
            assert key.startswith('daily_')

        # Should have technical features
        tech_features = [k for k in daily_analysis.keys() if 'ema' in k or 'rsi' in k or 'atr' in k]
        assert len(tech_features) > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_single_timeframe_insufficient_data(self, mock_universe_state_manager, mock_llm_recognizer):
        """Test single timeframe analysis with insufficient data."""
        # Very small dataset
        small_data = pd.DataFrame({
            'close': [100, 101],
            'high': [102, 103],
            'low': [98, 99]
        })

        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        analysis = await analyzer._analyze_single_timeframe(
            small_data,
            TimeFrame.DAILY,
            "AAPL"
        )

        # Should return empty analysis for insufficient data
        assert isinstance(analysis, dict)
        # May be empty or have limited features

    def test_extract_timeframe_technical_features(self, mock_universe_state_manager, sample_daily_data):
        """Test technical feature extraction for timeframe."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, None)

        features = analyzer._extract_timeframe_technical_features(
            sample_daily_data,
            TimeFrame.DAILY
        )

        assert isinstance(features, dict)
        assert len(features) > 0

        # Should have technical indicators
        assert any('ema' in k for k in features.keys())

    def test_extract_price_action_features(self, mock_universe_state_manager, sample_daily_data):
        """Test price action feature extraction."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, None)

        features = analyzer._extract_price_action_features(
            sample_daily_data,
            TimeFrame.DAILY
        )

        assert isinstance(features, dict)

        # Should have return features
        return_features = [k for k in features.keys() if 'return' in k]
        assert len(return_features) > 0

        # Should have volatility
        assert 'volatility' in features

        # Should have range analysis
        assert 'avg_range' in features

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_extract_llm_pattern_features(self, mock_universe_state_manager, mock_llm_recognizer, sample_daily_data):
        """Test LLM pattern feature extraction."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        features = await analyzer._extract_llm_pattern_features(
            sample_daily_data,
            TimeFrame.DAILY,
            "AAPL"
        )

        assert isinstance(features, dict)
        assert 'llm_confidence' in features
        assert 'llm_bullish' in features
        assert 'llm_bearish' in features
        assert 'llm_pattern_strength' in features

        # Should have pattern type features
        pattern_features = [k for k in features.keys() if 'llm_pattern_' in k]
        assert len(pattern_features) > 0

    def test_extract_trend_features(self, mock_universe_state_manager, sample_daily_data):
        """Test trend feature extraction."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, None)

        features = analyzer._extract_trend_features(
            sample_daily_data,
            TimeFrame.DAILY
        )

        assert isinstance(features, dict)
        assert 'trend_slope' in features
        assert 'trend_strength' in features
        assert 'trend_direction' in features

        # Trend direction should be -1 or 1
        assert features['trend_direction'] in [-1.0, 1.0]

        # Trend strength should be R-squared (0-1)
        assert 0 <= features['trend_strength'] <= 1

    def test_extract_support_resistance_features(self, mock_universe_state_manager, sample_daily_data):
        """Test support/resistance feature extraction."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, None)

        features = analyzer._extract_support_resistance_features(
            sample_daily_data,
            TimeFrame.DAILY
        )

        assert isinstance(features, dict)
        assert 'support_distance' in features
        assert 'resistance_distance' in features
        assert 'support_strength' in features
        assert 'resistance_strength' in features
        assert 'sr_position' in features

        # Distances should be reasonable
        assert isinstance(features['support_distance'], (int, float))
        assert isinstance(features['resistance_distance'], (int, float))

    def test_calculate_cross_timeframe_features(self, mock_universe_state_manager, sample_daily_data):
        """Test cross-timeframe feature calculation."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, None)

        # Create mock timeframe data
        timeframe_data = {
            TimeFrame.DAILY: sample_daily_data,
            TimeFrame.WEEKLY: sample_daily_data.iloc[::7],  # Sample weekly
        }

        # Create mock analyses
        timeframe_analyses = {
            TimeFrame.DAILY: {
                'daily_trend_direction': 1.0,
                'daily_trend_strength': 0.8,
                'daily_volatility': 0.02
            },
            TimeFrame.WEEKLY: {
                'weekly_trend_direction': 1.0,
                'weekly_trend_strength': 0.7,
                'weekly_volatility': 0.015
            }
        }

        features = analyzer._calculate_cross_timeframe_features(
            timeframe_data, timeframe_analyses
        )

        assert isinstance(features, dict)
        assert 'trend_alignment_score' in features
        assert 'momentum_consistency' in features

        # Alignment should be high for same direction trends
        assert features['trend_alignment_score'] >= 0.5

    def test_calculate_timeframe_alignment(self, mock_universe_state_manager):
        """Test timeframe alignment calculation."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, None)

        # Create aligned analyses (both bullish)
        timeframe_analyses = {
            TimeFrame.DAILY: {
                'trend_direction': 1.0,
                'trend_strength': 0.8,
                'llm_bullish': 0.9
            },
            TimeFrame.WEEKLY: {
                'trend_direction': 1.0,
                'trend_strength': 0.7,
                'llm_bullish': 0.8
            }
        }

        alignment = analyzer._calculate_timeframe_alignment(timeframe_analyses)

        assert isinstance(alignment, dict)
        assert 'daily_weekly_alignment' in alignment

        # Should be high alignment for similar signals
        assert alignment['daily_weekly_alignment'] >= 0.5

    def test_determine_dominant_trend(self, mock_universe_state_manager):
        """Test dominant trend determination."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, None)

        # Create bullish analyses
        timeframe_analyses = {
            TimeFrame.QUARTERLY: {
                'quarterly_trend_direction': 1.0,
                'quarterly_trend_strength': 0.8,
                'quarterly_llm_bullish': 0.9,
                'quarterly_llm_bearish': 0.1
            },
            TimeFrame.MONTHLY: {
                'monthly_trend_direction': 1.0,
                'monthly_trend_strength': 0.7,
                'monthly_llm_bullish': 0.8,
                'monthly_llm_bearish': 0.2
            }
        }

        timeframe_alignment = {'quarterly_monthly_alignment': 0.9}

        dominant_trend, trend_strength = analyzer._determine_dominant_trend(
            timeframe_analyses, timeframe_alignment
        )

        assert dominant_trend in ["bullish", "bearish", "neutral"]
        assert 0 <= trend_strength <= 1

        # Should be bullish given the inputs
        assert dominant_trend == "bullish"
        assert trend_strength > 0.5

    def test_calculate_pattern_strength(self, mock_universe_state_manager, mock_llm_recognizer):
        """Test pattern strength calculation."""
        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        # Create pattern analysis
        pattern_analysis = PatternAnalysis(
            pattern_type="test",
            confidence=0.8,
            description="",
            technical_indicators={"momentum": "strong"},
            predicted_direction="bullish",
            support_resistance={},
            volume_analysis="",
            risk_assessment="",
            timeframe_relevance=[]
        )

        strength = analyzer._calculate_pattern_strength(pattern_analysis)

        assert isinstance(strength, float)
        assert 0 <= strength <= 1
        # Should be higher than base confidence due to strong momentum
        assert strength >= 0.8


class TestFlattenMultiTimeFrameFeatures:
    """Test feature flattening functionality."""

    def test_flatten_multi_timeframe_features(self):
        """Test flattening of multi-timeframe features."""
        features = MultiTimeFrameFeatures(
            daily_features={'daily_rsi': 60.0, 'daily_momentum': 0.05},
            weekly_features={'weekly_trend': 1.0, 'weekly_volume': 1.2},
            monthly_features={'monthly_ma_cross': 1.0},
            quarterly_features={'quarterly_trend': 1.0},
            cross_timeframe_features={'trend_alignment': 0.8, 'momentum_consistency': 1.0},
            timeframe_alignment={'daily_weekly_alignment': 0.75, 'weekly_monthly_alignment': 0.8},
            dominant_trend="bullish",
            trend_strength=0.85
        )

        flattened = flatten_multi_timeframe_features(features)

        assert isinstance(flattened, dict)

        # Should have all original features
        assert 'daily_rsi' in flattened
        assert 'weekly_trend' in flattened
        assert 'monthly_ma_cross' in flattened
        assert 'quarterly_trend' in flattened

        # Should have cross-timeframe features
        assert 'trend_alignment' in flattened
        assert 'momentum_consistency' in flattened

        # Should have alignment features
        assert 'daily_weekly_alignment' in flattened
        assert 'weekly_monthly_alignment' in flattened

        # Should have summary features
        assert 'mtf_dominant_trend_bullish' in flattened
        assert 'mtf_dominant_trend_bearish' in flattened
        assert 'mtf_trend_strength' in flattened

        # Check values
        assert flattened['mtf_dominant_trend_bullish'] == 1.0
        assert flattened['mtf_dominant_trend_bearish'] == 0.0
        assert flattened['mtf_trend_strength'] == 0.85

    def test_flatten_empty_features(self):
        """Test flattening of empty features."""
        features = MultiTimeFrameFeatures(
            daily_features={},
            weekly_features={},
            monthly_features={},
            quarterly_features={},
            cross_timeframe_features={},
            timeframe_alignment={},
            dominant_trend="neutral",
            trend_strength=0.0
        )

        flattened = flatten_multi_timeframe_features(features)

        assert isinstance(flattened, dict)
        assert flattened['mtf_dominant_trend_bullish'] == 0.0
        assert flattened['mtf_dominant_trend_bearish'] == 0.0
        assert flattened['mtf_trend_strength'] == 0.0


class TestConvenienceFunction:
    """Test convenience function."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_multi_timeframe_patterns(self, mock_universe_state_manager, sample_daily_data):
        """Test convenience function for multi-timeframe analysis."""
        # Setup mock
        mock_universe_state_manager.get_lag_prices.return_value = sample_daily_data

        with patch('modeling.multi_timeframe_analyzer.LLMPatternRecognizer') as mock_class:
            # Mock LLM recognizer
            mock_recognizer = Mock()
            mock_recognizer.analyze_price_pattern = AsyncMock(return_value=PatternAnalysis(
                pattern_type="test", confidence=0.8, description="", technical_indicators={},
                predicted_direction="bullish", support_resistance={}, volume_analysis="",
                risk_assessment="", timeframe_relevance=[]
            ))
            mock_class.return_value = mock_recognizer

            features = await analyze_multi_timeframe_patterns(
                mock_universe_state_manager,
                123,
                datetime(2024, 2, 15),
                "AAPL",
                "test_api_key"
            )

            assert isinstance(features, MultiTimeFrameFeatures)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_multi_timeframe_patterns_no_llm(self, mock_universe_state_manager, sample_daily_data):
        """Test convenience function without LLM API key."""
        mock_universe_state_manager.get_lag_prices.return_value = sample_daily_data

        features = await analyze_multi_timeframe_patterns(
            mock_universe_state_manager,
            123,
            datetime(2024, 2, 15),
            "AAPL",
            None  # No API key
        )

        assert isinstance(features, MultiTimeFrameFeatures)
        # Should still work without LLM features


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_with_corrupt_data(self, mock_universe_state_manager, mock_llm_recognizer):
        """Test analysis with corrupt/invalid data."""
        # Create data with NaN values
        corrupt_data = pd.DataFrame({
            'close': [100, np.nan, 102, np.nan, 104],
            'high': [102, 103, np.nan, 105, 106],
            'low': [98, np.nan, 100, 101, np.nan],
            'volume': [1000, np.nan, 1200, 1100, np.nan]
        })

        mock_universe_state_manager.get_lag_prices.return_value = corrupt_data

        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        # Should handle gracefully
        features = await analyzer.analyze_multi_timeframe(123, datetime(2024, 2, 15), "AAPL")

        assert isinstance(features, MultiTimeFrameFeatures)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_with_extreme_values(self, mock_universe_state_manager, mock_llm_recognizer):
        """Test analysis with extreme price values."""
        # Create data with extreme values
        extreme_data = pd.DataFrame({
            'close': [1e-10, 1e10, 0.001, 1000000, 100],
            'high': [1e-9, 1.1e10, 0.002, 1100000, 102],
            'low': [1e-11, 9e9, 0.0005, 900000, 98],
            'volume': [1, 1e15, 100, 1e12, 1000]
        })

        mock_universe_state_manager.get_lag_prices.return_value = extreme_data

        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        features = await analyzer.analyze_multi_timeframe(123, datetime(2024, 2, 15), "AAPL")

        assert isinstance(features, MultiTimeFrameFeatures)

        # Features should be finite
        flattened = flatten_multi_timeframe_features(features)
        for key, value in flattened.items():
            if isinstance(value, (int, float)):
                assert np.isfinite(value)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_llm_error_handling(self, mock_universe_state_manager, sample_daily_data):
        """Test handling of LLM errors."""
        mock_universe_state_manager.get_lag_prices.return_value = sample_daily_data

        # Mock LLM to raise errors
        mock_llm = Mock()
        mock_llm.analyze_price_pattern = AsyncMock(side_effect=Exception("LLM API Error"))

        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm)

        # Should handle LLM errors gracefully
        features = await analyzer.analyze_multi_timeframe(123, datetime(2024, 2, 15), "AAPL")

        assert isinstance(features, MultiTimeFrameFeatures)
        # Should still have non-LLM features
        assert len(features.daily_features) > 0 or len(features.weekly_features) > 0

    def test_aggregation_with_missing_columns(self):
        """Test aggregation when expected columns are missing."""
        data_missing_cols = pd.DataFrame({
            'close': [100, 101, 102, 103, 104],
            'volume': [1000, 1100, 1200, 1300, 1400]
        }, index=pd.date_range('2024-01-01', periods=5))

        # Should handle missing high/low gracefully
        weekly = TimeFrameAggregator.aggregate_to_weekly(data_missing_cols)

        # Should still work, but may not have all OHLC
        assert not weekly.empty

    def test_trend_calculation_constant_prices(self, mock_universe_state_manager):
        """Test trend calculation with constant prices."""
        # Constant prices (no trend)
        constant_data = pd.DataFrame({
            'close': [100] * 10,
            'high': [100] * 10,
            'low': [100] * 10
        })

        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, None)

        features = analyzer._extract_trend_features(constant_data, TimeFrame.DAILY)

        assert isinstance(features, dict)
        assert 'trend_slope' in features
        assert features['trend_slope'] == 0.0  # No slope for constant prices

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analysis_with_single_timeframe_data(self, mock_universe_state_manager, mock_llm_recognizer):
        """Test analysis when only one timeframe has sufficient data."""
        # Very limited data that only works for daily
        limited_data = pd.DataFrame({
            'close': [100, 101, 102],
            'high': [102, 103, 104],
            'low': [98, 99, 100]
        }, index=pd.date_range('2024-01-01', periods=3))

        mock_universe_state_manager.get_lag_prices.return_value = limited_data

        analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm_recognizer)

        features = await analyzer.analyze_multi_timeframe(123, datetime(2024, 1, 3), "AAPL")

        assert isinstance(features, MultiTimeFrameFeatures)
        # May have limited features due to insufficient data for higher timeframes


@pytest.mark.integration
class TestIntegrationScenarios:
    """Test integration scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_multi_timeframe_workflow(self, mock_universe_state_manager, sample_daily_data):
        """Test complete multi-timeframe analysis workflow."""
        # Setup comprehensive mock data
        mock_universe_state_manager.get_lag_prices.return_value = sample_daily_data

        # Create real LLM recognizer mock
        with patch('modeling.multi_timeframe_analyzer.LLMPatternRecognizer') as mock_llm_class:
            mock_llm = Mock()
            mock_llm.analyze_price_pattern = AsyncMock(return_value=PatternAnalysis(
                pattern_type="ascending_triangle",
                confidence=0.85,
                description="Strong pattern",
                technical_indicators={"trend_direction": "up", "momentum": "strong"},
                predicted_direction="bullish",
                support_resistance={"support": 95.0, "resistance": 105.0},
                volume_analysis="Strong volume",
                risk_assessment="Low risk",
                timeframe_relevance=["short_term", "medium_term"]
            ))
            mock_llm_class.return_value = mock_llm

            # Run complete analysis
            analyzer = MultiTimeFrameAnalyzer(mock_universe_state_manager, mock_llm)

            features = await analyzer.analyze_multi_timeframe(
                123, datetime(2024, 2, 15), "AAPL"
            )

            # Flatten for model use
            flattened = flatten_multi_timeframe_features(features)

            # Validate complete workflow
            assert isinstance(features, MultiTimeFrameFeatures)
            assert isinstance(flattened, dict)
            assert len(flattened) > 10  # Should have many features

            # Should have features from all timeframes
            daily_features = [k for k in flattened.keys() if k.startswith('daily_')]
            weekly_features = [k for k in flattened.keys() if k.startswith('weekly_')]
            monthly_features = [k for k in flattened.keys() if k.startswith('monthly_')]

            assert len(daily_features) > 0
            assert len(weekly_features) > 0
            assert len(monthly_features) > 0

            # Should have LLM features
            llm_features = [k for k in flattened.keys() if 'llm_' in k]
            assert len(llm_features) > 0

            # Should have alignment features
            alignment_features = [k for k in flattened.keys() if 'alignment' in k]
            assert len(alignment_features) > 0

            # All features should be numeric and finite
            for key, value in flattened.items():
                if isinstance(value, (int, float)):
                    assert np.isfinite(value), f"Feature {key} is not finite: {value}"


if __name__ == "__main__":
    pytest.main([__file__])