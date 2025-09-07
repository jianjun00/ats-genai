"""
Tests for sentiment integration framework.
"""

import pytest
import asyncio
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import asyncpg

from sentiment.sentiment_integrator import (
    UnifiedSentimentSignal,
    SentimentBasedPrediction,
    SentimentFeatureExtractor,
    SentimentIntegrator,
    generate_sentiment_enhanced_predictions,
    analyze_unified_sentiment
)
from sentiment.news_sentiment_analyzer import SentimentSignal, NewsArticle
from sentiment.social_media_analyzer import SocialTradingSignal, SocialSentimentMetrics, SocialMediaPost


@pytest.fixture
def mock_connection_pool():
    """Mock database connection pool."""
    pool = Mock(spec=asyncpg.Pool)
    conn = Mock(spec=asyncpg.Connection)

    # Create async context manager mock
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = conn
    context_manager.__aexit__.return_value = None

    pool.acquire.return_value = context_manager
    return pool, conn


@pytest.fixture
def mock_env():
    """Mock environment configuration."""
    env = Mock()
    env.get_table_name.side_effect = lambda x: f"test_{x}"
    return env


@pytest.fixture
def sample_news_signal():
    """Sample news sentiment signal."""
    article = NewsArticle(
        title="Apple Reports Strong Earnings",
        content="Apple Inc. reported record quarterly earnings...",
        url="https://example.com/apple-earnings",
        source="reuters",
        published_date=datetime(2024, 1, 15, 9, 0, 0),
        symbols=["AAPL"],
        sentiment=None,
        relevance_score=0.95,
        impact_score=0.8
    )

    return SentimentSignal(
        symbol="AAPL",
        signal_strength=0.7,
        signal_direction="bullish",
        confidence=0.8,
        time_horizon="short",
        supporting_articles=[article],
        sentiment_momentum=0.3,
        volume_weighted_sentiment=0.75,
        timestamp=datetime(2024, 1, 15, 10, 0, 0)
    )


@pytest.fixture
def sample_social_signal():
    """Sample social media sentiment signal."""
    post = SocialMediaPost(
        post_id="tweet_123",
        platform="twitter",
        content="$AAPL to the moon! 🚀",
        author="trader_pro",
        followers_count=10000,
        retweets_count=50,
        likes_count=200,
        timestamp=datetime(2024, 1, 15, 10, 30, 0),
        symbols=["AAPL"],
        hashtags=["trading", "bullish"],
        mentions=[],
        engagement_score=0.025,
        author_influence_score=0.6
    )

    metrics = SocialSentimentMetrics(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 15, 10, 30, 0),
        total_posts=25,
        total_engagement=1250,
        average_sentiment=0.6,
        sentiment_std=0.2,
        bullish_ratio=0.8,
        bearish_ratio=0.1,
        neutral_ratio=0.1,
        trending_score=0.7,
        influencer_sentiment=0.7,
        retail_sentiment=0.5,
        momentum_score=0.4,
        top_hashtags=["trading", "bullish"],
        top_keywords=["moon", "strong"]
    )

    return SocialTradingSignal(
        symbol="AAPL",
        signal_type="momentum",
        signal_strength=0.6,
        confidence=0.7,
        time_horizon="short",
        supporting_posts=[post],
        key_metrics=metrics,
        risk_factors=[],
        timestamp=datetime(2024, 1, 15, 10, 30, 0)
    )


@pytest.fixture
def sample_unified_signal(sample_news_signal, sample_social_signal):
    """Sample unified sentiment signal."""
    return UnifiedSentimentSignal(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 15, 11, 0, 0),
        overall_sentiment_score=0.68,  # Weighted combination
        overall_confidence=0.75,
        signal_strength=0.68,
        signal_direction="bullish",
        news_sentiment=sample_news_signal,
        social_sentiment=sample_social_signal,
        sentiment_features={},  # Will be filled by tests
        time_horizon="short",
        risk_score=0.2,
        volume_indicator=0.6,
        consensus_score=0.8,
        divergence_score=0.1,
        total_news_articles=1,
        total_social_posts=25,
        key_themes=["earnings", "trading"],
        risk_factors=[]
    )


class TestUnifiedSentimentSignal:
    """Test UnifiedSentimentSignal dataclass."""

    def test_unified_signal_creation(self, sample_unified_signal):
        """Test UnifiedSentimentSignal creation and attributes."""
        signal = sample_unified_signal

        assert signal.symbol == "AAPL"
        assert signal.signal_direction == "bullish"
        assert 0 <= signal.overall_confidence <= 1
        assert -1 <= signal.overall_sentiment_score <= 1
        assert signal.news_sentiment is not None
        assert signal.social_sentiment is not None
        assert signal.total_news_articles == 1
        assert signal.total_social_posts == 25
        assert "earnings" in signal.key_themes


class TestSentimentBasedPrediction:
    """Test SentimentBasedPrediction dataclass."""

    def test_sentiment_prediction_creation(self, sample_unified_signal):
        """Test SentimentBasedPrediction creation."""
        prediction = SentimentBasedPrediction(
            symbol="AAPL",
            prediction_date=datetime(2024, 1, 15, 12, 0, 0),
            horizon_days=2,
            base_residual_return=0.015,
            base_confidence=0.7,
            sentiment_adjusted_return=0.018,
            sentiment_confidence_boost=0.1,
            final_confidence=0.8,
            news_contribution=0.012,
            social_contribution=0.006,
            sentiment_risk_adjustment=0.2,
            unified_sentiment=sample_unified_signal,
            prediction_explanation="Positive sentiment adjustment based on earnings news and social momentum"
        )

        assert prediction.symbol == "AAPL"
        assert prediction.sentiment_adjusted_return > prediction.base_residual_return
        assert prediction.final_confidence > prediction.base_confidence
        assert prediction.unified_sentiment == sample_unified_signal
        assert "earnings" in prediction.prediction_explanation


class TestSentimentFeatureExtractor:
    """Test SentimentFeatureExtractor functionality."""

    def test_feature_extractor_initialization(self):
        """Test SentimentFeatureExtractor initialization."""
        extractor = SentimentFeatureExtractor()

        assert hasattr(extractor, 'feature_names')
        assert len(extractor.feature_names) > 20  # Should have 23+ features
        assert 'news_sentiment_score' in extractor.feature_names
        assert 'social_sentiment_score' in extractor.feature_names
        assert 'sentiment_consensus' in extractor.feature_names

    def test_extract_features_with_both_signals(self, sample_unified_signal):
        """Test feature extraction with both news and social signals."""
        extractor = SentimentFeatureExtractor()

        # Mock historical signals
        historical_signals = [
            Mock(overall_sentiment_score=0.5),
            Mock(overall_sentiment_score=0.6),
            Mock(overall_sentiment_score=0.4)
        ]

        features = extractor.extract_features(sample_unified_signal, historical_signals)

        assert isinstance(features, dict)
        assert len(features) == len(extractor.feature_names)

        # Check news features are populated
        assert features['news_sentiment_score'] > 0
        assert features['news_confidence'] > 0
        assert features['news_article_count'] > 0

        # Check social features are populated
        assert features['social_sentiment_score'] > 0
        assert features['social_confidence'] > 0
        assert features['social_post_count'] > 0

        # Check cross-signal features
        assert 0 <= features['sentiment_consensus'] <= 1
        assert features['time_decay_factor'] > 0

    def test_extract_features_news_only(self, sample_news_signal):
        """Test feature extraction with only news signal."""
        extractor = SentimentFeatureExtractor()

        unified_signal = UnifiedSentimentSignal(
            symbol="AAPL",
            timestamp=datetime.now(),
            overall_sentiment_score=0.7,
            overall_confidence=0.8,
            signal_strength=0.7,
            signal_direction="bullish",
            news_sentiment=sample_news_signal,
            social_sentiment=None,  # No social signal
            sentiment_features={},
            time_horizon="short",
            risk_score=0.1,
            volume_indicator=0.3,
            consensus_score=0.5,
            divergence_score=0.0,
            total_news_articles=1,
            total_social_posts=0,
            key_themes=["earnings"],
            risk_factors=[]
        )

        features = extractor.extract_features(unified_signal, [])

        # News features should be populated
        assert features['news_sentiment_score'] > 0
        assert features['news_confidence'] > 0

        # Social features should be zero
        assert features['social_sentiment_score'] == 0.0
        assert features['social_confidence'] == 0.0
        assert features['social_post_count'] == 0.0

    def test_extract_features_social_only(self, sample_social_signal):
        """Test feature extraction with only social signal."""
        extractor = SentimentFeatureExtractor()

        unified_signal = UnifiedSentimentSignal(
            symbol="AAPL",
            timestamp=datetime.now(),
            overall_sentiment_score=0.6,
            overall_confidence=0.7,
            signal_strength=0.6,
            signal_direction="bullish",
            news_sentiment=None,  # No news signal
            social_sentiment=sample_social_signal,
            sentiment_features={},
            time_horizon="short",
            risk_score=0.2,
            volume_indicator=0.4,
            consensus_score=0.5,
            divergence_score=0.0,
            total_news_articles=0,
            total_social_posts=25,
            key_themes=["trading"],
            risk_factors=[]
        )

        features = extractor.extract_features(unified_signal, [])

        # Social features should be populated
        assert features['social_sentiment_score'] > 0
        assert features['social_confidence'] > 0
        assert features['social_post_count'] > 0

        # News features should be zero
        assert features['news_sentiment_score'] == 0.0
        assert features['news_confidence'] == 0.0
        assert features['news_article_count'] == 0.0

    def test_calculate_derived_features(self, sample_unified_signal):
        """Test calculation of derived features from historical data."""
        extractor = SentimentFeatureExtractor()

        # Create historical signals with trend
        historical_signals = []
        for i in range(10):
            signal = Mock()
            signal.overall_sentiment_score = 0.3 + i * 0.05  # Increasing trend
            historical_signals.append(signal)

        features = extractor.extract_features(sample_unified_signal, historical_signals)

        # Should have derived features
        assert 'sentiment_volatility' in features
        assert 'sentiment_surprise' in features
        assert 'sentiment_persistence' in features

        # Volatility should be reasonable
        assert features['sentiment_volatility'] >= 0

        # Persistence should be positive (increasing trend)
        assert features['sentiment_persistence'] > 0

    def test_time_decay_calculation(self):
        """Test time decay factor calculation."""
        extractor = SentimentFeatureExtractor()

        # Recent timestamp
        recent_time = datetime.now()
        recent_decay = extractor._calculate_time_decay(recent_time)
        assert 0.9 <= recent_decay <= 1.0

        # Old timestamp
        old_time = datetime.now() - timedelta(days=1)
        old_decay = extractor._calculate_time_decay(old_time)
        assert old_decay < recent_decay
        assert 0 <= old_decay <= 1.0


class TestSentimentIntegrator:
    """Test SentimentIntegrator functionality."""

    def test_integrator_initialization(self, mock_connection_pool, mock_env):
        """Test SentimentIntegrator initialization."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            assert integrator.pool == pool
            assert integrator.env == mock_env
            assert hasattr(integrator, 'news_analyzer')
            assert hasattr(integrator, 'social_analyzer')
            assert hasattr(integrator, 'feature_extractor')
            assert 'news' in integrator.sentiment_weights
            assert 'social' in integrator.sentiment_weights

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_unified_sentiment_signals(self, mock_connection_pool, mock_env,
                                                     sample_news_signal, sample_social_signal):
        """Test generation of unified sentiment signals."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer') as mock_news, \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer') as mock_social, \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            # Mock analyzers
            mock_news_instance = AsyncMock()
            mock_social_instance = AsyncMock()
            mock_news.return_value = mock_news_instance
            mock_social.return_value = mock_social_instance

            # Mock analyzer results
            mock_news_instance.analyze_news_sentiment.return_value = {"AAPL": sample_news_signal}
            mock_social_instance.analyze_social_sentiment.return_value = {"AAPL": sample_social_signal}

            integrator = SentimentIntegrator(pool, mock_env)

            with patch.object(integrator, '_get_historical_signals', return_value=[]), \
                 patch.object(integrator, '_store_unified_signals', new_callable=AsyncMock):

                signals = await integrator.generate_unified_sentiment_signals(["AAPL"], hours_back=24)

                assert isinstance(signals, dict)
                assert "AAPL" in signals
                assert isinstance(signals["AAPL"], UnifiedSentimentSignal)
                assert signals["AAPL"].symbol == "AAPL"
                assert signals["AAPL"].news_sentiment == sample_news_signal
                assert signals["AAPL"].social_sentiment == sample_social_signal

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_combine_sentiment_signals(self, mock_connection_pool, mock_env,
                                           sample_news_signal, sample_social_signal):
        """Test combining news and social sentiment signals."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            with patch.object(integrator, '_get_historical_signals', return_value=[]):

                unified_signal = await integrator._combine_sentiment_signals(
                    "AAPL", sample_news_signal, sample_social_signal
                )

                assert isinstance(unified_signal, UnifiedSentimentSignal)
                assert unified_signal.symbol == "AAPL"

                # Check weighted combination
                expected_sentiment = (
                    sample_news_signal.volume_weighted_sentiment * integrator.sentiment_weights['news'] +
                    sample_social_signal.key_metrics.average_sentiment * integrator.sentiment_weights['social']
                )
                assert abs(unified_signal.overall_sentiment_score - expected_sentiment) < 0.01

                # Check consensus calculation
                assert 0 <= unified_signal.consensus_score <= 1
                assert unified_signal.divergence_score >= 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_combine_sentiment_signals_news_only(self, mock_connection_pool, mock_env, sample_news_signal):
        """Test combining with only news signal."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            with patch.object(integrator, '_get_historical_signals', return_value=[]):

                unified_signal = await integrator._combine_sentiment_signals(
                    "AAPL", sample_news_signal, None
                )

                assert isinstance(unified_signal, UnifiedSentimentSignal)
                assert unified_signal.news_sentiment == sample_news_signal
                assert unified_signal.social_sentiment is None
                assert unified_signal.consensus_score == 0.5  # Neutral when only one source

    def test_determine_time_horizon(self, mock_connection_pool, mock_env, sample_news_signal, sample_social_signal):
        """Test time horizon determination."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Both signals have "short" horizon
            horizon = integrator._determine_time_horizon(sample_news_signal, sample_social_signal)
            assert horizon == "short"

            # Mixed horizons
            sample_news_signal.time_horizon = "medium"
            sample_social_signal.time_horizon = "short"
            horizon = integrator._determine_time_horizon(sample_news_signal, sample_social_signal)
            assert horizon == "short"  # Short takes priority

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_calculate_risk_score(self, mock_connection_pool, mock_env, sample_news_signal, sample_social_signal):
        """Test risk score calculation."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Normal case
            risk_score = await integrator._calculate_risk_score("AAPL", sample_news_signal, sample_social_signal)
            assert 0 <= risk_score <= 1

            # High divergence case
            sample_news_signal.volume_weighted_sentiment = 0.8
            sample_social_signal.key_metrics.average_sentiment = -0.3  # Opposite sentiment
            risk_score_high = await integrator._calculate_risk_score("AAPL", sample_news_signal, sample_social_signal)
            assert risk_score_high > risk_score

    def test_extract_key_themes(self, mock_connection_pool, mock_env, sample_news_signal, sample_social_signal):
        """Test key theme extraction."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Modify article content to include recognizable themes
            sample_news_signal.supporting_articles[0].title = "Apple earnings report shows dividend increase"
            sample_news_signal.supporting_articles[0].content = "Strong earnings and new dividend announcement"

            themes = integrator._extract_key_themes(sample_news_signal, sample_social_signal)

            assert isinstance(themes, list)
            assert len(themes) <= 5
            # Should detect earnings and dividend themes
            assert any(theme in ['earnings', 'dividend', 'trading'] for theme in themes)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_enhance_residual_return_predictions(self, mock_connection_pool, mock_env):
        """Test enhancement of residual return predictions with sentiment."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Sample base predictions
            base_predictions = pd.DataFrame({
                'symbol': ['AAPL', 'MSFT'],
                'prediction_date': [datetime(2024, 1, 15), datetime(2024, 1, 15)],
                'horizon': [1, 2],
                'predicted_return': [0.015, -0.008],
                'confidence': [0.7, 0.6]
            })

            # Mock unified signals
            mock_unified_signal = Mock()
            mock_unified_signal.overall_sentiment_score = 0.5
            mock_unified_signal.overall_confidence = 0.8
            mock_unified_signal.consensus_score = 0.9
            mock_unified_signal.risk_score = 0.1
            mock_unified_signal.news_sentiment = Mock(volume_weighted_sentiment=0.6)
            mock_unified_signal.social_sentiment = Mock()
            mock_unified_signal.social_sentiment.key_metrics.average_sentiment = 0.4

            with patch.object(integrator, 'generate_unified_sentiment_signals') as mock_generate:
                mock_generate.return_value = {"AAPL": mock_unified_signal}

                enhanced_predictions = await integrator.enhance_residual_return_predictions(base_predictions)

                assert isinstance(enhanced_predictions, pd.DataFrame)
                assert len(enhanced_predictions) == 2
                assert 'sentiment_adjusted_return' in enhanced_predictions.columns
                assert 'final_confidence' in enhanced_predictions.columns

                # AAPL should have sentiment adjustment
                aapl_row = enhanced_predictions[enhanced_predictions['symbol'] == 'AAPL'].iloc[0]
                assert aapl_row['sentiment_adjusted_return'] != aapl_row['base_predicted_return']
                assert aapl_row['final_confidence'] >= aapl_row['base_confidence']

                # MSFT should be unchanged (no sentiment signal)
                msft_row = enhanced_predictions[enhanced_predictions['symbol'] == 'MSFT'].iloc[0]
                assert msft_row['sentiment_adjusted_return'] == msft_row['base_predicted_return']

    def test_calculate_sentiment_adjustment(self, mock_connection_pool, mock_env):
        """Test sentiment adjustment calculation."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Mock prediction row
            prediction_row = pd.Series({
                'symbol': 'AAPL',
                'predicted_return': 0.015,
                'confidence': 0.7
            })

            # Mock unified signal with strong positive sentiment
            unified_signal = Mock()
            unified_signal.overall_sentiment_score = 0.8
            unified_signal.overall_confidence = 0.9
            unified_signal.consensus_score = 0.9
            unified_signal.risk_score = 0.1
            unified_signal.news_sentiment = Mock(volume_weighted_sentiment=0.8)
            unified_signal.social_sentiment = Mock()
            unified_signal.social_sentiment.key_metrics.average_sentiment = 0.7

            adjustment = integrator._calculate_sentiment_adjustment(prediction_row, unified_signal)

            assert isinstance(adjustment, dict)
            assert 'return_adjustment' in adjustment
            assert 'confidence_boost' in adjustment
            assert 'explanation' in adjustment

            # Should have positive adjustment for positive sentiment
            assert adjustment['return_adjustment'] > 0
            assert adjustment['confidence_boost'] > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_store_unified_signals(self, mock_connection_pool, mock_env, sample_unified_signal):
        """Test storing unified signals in database."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Mock database operations
            conn.execute = AsyncMock()

            # Add features to signal
            sample_unified_signal.sentiment_features = {
                'news_sentiment_score': 0.7,
                'social_sentiment_score': 0.6,
                'sentiment_consensus': 0.8
            }

            await integrator._store_unified_signals({"AAPL": sample_unified_signal})

            # Verify database call was made
            conn.execute.assert_called_once()

            # Check that JSON serialization works
            call_args = conn.execute.call_args[0]
            assert json.loads(call_args[15]) == sample_unified_signal.sentiment_features

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_historical_signals(self, mock_connection_pool, mock_env):
        """Test getting historical unified signals."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Mock database results
            mock_rows = [
                {
                    'symbol': 'AAPL',
                    'created_at': datetime(2024, 1, 14, 10, 0, 0),
                    'overall_sentiment_score': 0.6,
                    'overall_confidence': 0.8,
                    'signal_strength': 0.6,
                    'signal_direction': 'bullish',
                    'time_horizon': 'short',
                    'risk_score': 0.2,
                    'volume_indicator': 0.5,
                    'consensus_score': 0.7,
                    'divergence_score': 0.3,
                    'total_news_articles': 2,
                    'total_social_posts': 15,
                    'key_themes': ['earnings'],
                    'risk_factors': [],
                    'sentiment_features': '{"test_feature": 0.5}'
                }
            ]
            conn.fetch.return_value = mock_rows

            signals = await integrator._get_historical_signals("AAPL", days_back=7)

            assert isinstance(signals, list)
            assert len(signals) == 1
            assert isinstance(signals[0], UnifiedSentimentSignal)
            assert signals[0].symbol == "AAPL"
            assert signals[0].sentiment_features == {"test_feature": 0.5}

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_close_resources(self, mock_connection_pool, mock_env):
        """Test closing integrator resources."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer') as mock_news, \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            mock_news_instance = AsyncMock()
            mock_news.return_value = mock_news_instance

            integrator = SentimentIntegrator(pool, mock_env)
            await integrator.close()

            mock_news_instance.close.assert_called_once()


class TestConvenienceFunctions:
    """Test convenience functions."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_sentiment_enhanced_predictions(self, mock_connection_pool, mock_env):
        """Test convenience function for enhanced predictions."""
        pool, conn = mock_connection_pool

        base_predictions = pd.DataFrame({
            'symbol': ['AAPL'],
            'prediction_date': [datetime(2024, 1, 15)],
            'predicted_return': [0.015],
            'confidence': [0.7]
        })

        with patch('sentiment.sentiment_integrator.SentimentIntegrator') as mock_integrator_class:
            mock_integrator = AsyncMock()
            mock_integrator.enhance_residual_return_predictions.return_value = base_predictions
            mock_integrator.close = AsyncMock()
            mock_integrator_class.return_value = mock_integrator

            result = await generate_sentiment_enhanced_predictions(pool, mock_env, base_predictions)

            assert isinstance(result, pd.DataFrame)
            mock_integrator.close.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_unified_sentiment(self, mock_connection_pool, mock_env):
        """Test convenience function for unified sentiment analysis."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.SentimentIntegrator') as mock_integrator_class:
            mock_integrator = AsyncMock()
            mock_integrator.generate_unified_sentiment_signals.return_value = {"AAPL": Mock()}
            mock_integrator.close = AsyncMock()
            mock_integrator_class.return_value = mock_integrator

            result = await analyze_unified_sentiment(pool, mock_env, ["AAPL"], hours_back=24)

            assert isinstance(result, dict)
            assert "AAPL" in result
            mock_integrator.close.assert_called_once()


class TestErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_no_sentiment_signals_available(self, mock_connection_pool, mock_env):
        """Test handling when no sentiment signals are available."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer') as mock_news, \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer') as mock_social, \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            # Mock empty results
            mock_news_instance = AsyncMock()
            mock_social_instance = AsyncMock()
            mock_news.return_value = mock_news_instance
            mock_social.return_value = mock_social_instance

            mock_news_instance.analyze_news_sentiment.return_value = {}
            mock_social_instance.analyze_social_sentiment.return_value = {}

            integrator = SentimentIntegrator(pool, mock_env)

            signals = await integrator.generate_unified_sentiment_signals(["AAPL"], hours_back=24)

            assert isinstance(signals, dict)
            assert len(signals) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_sentiment_analysis_error(self, mock_connection_pool, mock_env):
        """Test handling of sentiment analysis errors."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer') as mock_news, \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer') as mock_social, \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            # Mock error in news analysis
            mock_news_instance = AsyncMock()
            mock_social_instance = AsyncMock()
            mock_news.return_value = mock_news_instance
            mock_social.return_value = mock_social_instance

            mock_news_instance.analyze_news_sentiment.side_effect = Exception("API Error")
            mock_social_instance.analyze_social_sentiment.return_value = {}

            integrator = SentimentIntegrator(pool, mock_env)

            signals = await integrator.generate_unified_sentiment_signals(["AAPL"], hours_back=24)

            # Should return empty dict on error
            assert isinstance(signals, dict)
            assert len(signals) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_error_handling(self, mock_connection_pool, mock_env):
        """Test handling of database errors."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Mock database error
            conn.execute.side_effect = asyncpg.PostgresError("Database error")

            # Should not raise exception
            await integrator._store_unified_signals({})

    def test_invalid_feature_extraction(self):
        """Test feature extraction with invalid data."""
        extractor = SentimentFeatureExtractor()

        # Signal with None news and social
        invalid_signal = UnifiedSentimentSignal(
            symbol="AAPL",
            timestamp=datetime.now(),
            overall_sentiment_score=0.0,
            overall_confidence=0.0,
            signal_strength=0.0,
            signal_direction="neutral",
            news_sentiment=None,
            social_sentiment=None,
            sentiment_features={},
            time_horizon="medium",
            risk_score=0.5,
            volume_indicator=0.0,
            consensus_score=0.5,
            divergence_score=0.0,
            total_news_articles=0,
            total_social_posts=0,
            key_themes=[],
            risk_factors=[]
        )

        features = extractor.extract_features(invalid_signal, [])

        # Should return valid feature dict with zeros
        assert isinstance(features, dict)
        assert len(features) == len(extractor.feature_names)
        assert features['news_sentiment_score'] == 0.0
        assert features['social_sentiment_score'] == 0.0


class TestIntegrationScenarios:
    """Test integration scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_high_consensus_scenario(self, mock_connection_pool, mock_env):
        """Test scenario with high consensus between news and social."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Create aligned signals (high consensus)
            news_signal = Mock()
            news_signal.volume_weighted_sentiment = 0.8
            news_signal.confidence = 0.9
            news_signal.supporting_articles = [Mock()]

            social_signal = Mock()
            social_signal.key_metrics.average_sentiment = 0.75  # Similar to news
            social_signal.confidence = 0.8
            social_signal.risk_factors = []

            with patch.object(integrator, '_get_historical_signals', return_value=[]):

                unified_signal = await integrator._combine_sentiment_signals(
                    "AAPL", news_signal, social_signal
                )

                # Should have high consensus, low divergence
                assert unified_signal.consensus_score > 0.9
                assert unified_signal.divergence_score < 0.1
                assert unified_signal.overall_confidence > 0.8

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_high_divergence_scenario(self, mock_connection_pool, mock_env):
        """Test scenario with high divergence between news and social."""
        pool, conn = mock_connection_pool

        with patch('sentiment.sentiment_integrator.NewsSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.SocialSentimentAnalyzer'), \
             patch('sentiment.sentiment_integrator.ResidualReturnCalculator'):

            integrator = SentimentIntegrator(pool, mock_env)

            # Create divergent signals
            news_signal = Mock()
            news_signal.volume_weighted_sentiment = 0.8  # Very positive
            news_signal.confidence = 0.9
            news_signal.supporting_articles = [Mock()]

            social_signal = Mock()
            social_signal.key_metrics.average_sentiment = -0.6  # Very negative
            social_signal.confidence = 0.8
            social_signal.risk_factors = []

            with patch.object(integrator, '_get_historical_signals', return_value=[]):

                unified_signal = await integrator._combine_sentiment_signals(
                    "AAPL", news_signal, social_signal
                )

                # Should have low consensus, high divergence
                assert unified_signal.consensus_score < 0.3
                assert unified_signal.divergence_score > 1.0
                assert unified_signal.risk_score > 0.3  # Higher risk due to divergence


if __name__ == "__main__":
    pytest.main([__file__])