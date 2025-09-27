"""
Tests for social media sentiment analysis framework.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
import asyncpg

from domains.analytics.services.sentiment.social_media_analyzer import (
    SocialMediaPost,
    SocialSentimentMetrics,
    SocialTradingSignal,
    CryptoTwitterAnalyzer,
    SocialMediaDataGenerator,
    SocialSentimentAnalyzer,
    analyze_social_media_sentiment
)

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
def sample_social_media_post():
    """Sample social media post."""
    return SocialMediaPost(
        post_id="tweet_123456",
        platform="twitter",
        content="$AAPL to the moon! 🚀 Diamond hands! #trading #stocks",
        author="crypto_trader",
        followers_count=5000,
        retweets_count=25,
        likes_count=100,
        timestamp=datetime(2024, 1, 15, 14, 30, 0),
        symbols=["AAPL"],
        hashtags=["trading", "stocks"],
        mentions=[],
        engagement_score=0.025,  # (25+100)/5000
        author_influence_score=0.3
    )

@pytest.fixture
def sample_sentiment_metrics():
    """Sample social sentiment metrics."""
    return SocialSentimentMetrics(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 15, 15, 0, 0),
        total_posts=50,
        total_engagement=2500,
        average_sentiment=0.6,
        sentiment_std=0.3,
        bullish_ratio=0.7,
        bearish_ratio=0.2,
        neutral_ratio=0.1,
        trending_score=0.8,
        influencer_sentiment=0.75,
        retail_sentiment=0.55,
        momentum_score=0.4,
        top_hashtags=["trading", "bullish", "moon"],
        top_keywords=["earnings", "growth", "strong"]
    )

class TestSocialMediaPost:
    """Test SocialMediaPost dataclass."""

    def test_social_media_post_creation(self, sample_social_media_post):
        """Test SocialMediaPost creation and attributes."""
        post = sample_social_media_post

        assert post.post_id == "tweet_123456"
        assert post.platform == "twitter"
        assert "AAPL" in post.content
        assert "AAPL" in post.symbols
        assert "trading" in post.hashtags
        assert post.followers_count == 5000
        assert post.engagement_score > 0
        assert post.author_influence_score > 0

class TestSocialSentimentMetrics:
    """Test SocialSentimentMetrics dataclass."""

    def test_sentiment_metrics_creation(self, sample_sentiment_metrics):
        """Test SocialSentimentMetrics creation and attributes."""
        metrics = sample_sentiment_metrics

        assert metrics.symbol == "AAPL"
        assert metrics.total_posts == 50
        assert metrics.average_sentiment == 0.6
        assert metrics.bullish_ratio + metrics.bearish_ratio + metrics.neutral_ratio == 1.0
        assert "trading" in metrics.top_hashtags
        assert "earnings" in metrics.top_keywords
        assert metrics.influencer_sentiment > metrics.retail_sentiment

class TestCryptoTwitterAnalyzer:
    """Test CryptoTwitterAnalyzer functionality."""

    def test_analyzer_initialization(self):
        """Test CryptoTwitterAnalyzer initialization."""
        with patch('sentiment.social_media_analyzer.pipeline') as mock_pipeline:
            mock_pipeline.return_value = Mock()

            analyzer = CryptoTwitterAnalyzer()

            assert hasattr(analyzer, 'bullish_indicators')
            assert hasattr(analyzer, 'bearish_indicators')
            assert 'moon' in analyzer.bullish_indicators
            assert 'crash' in analyzer.bearish_indicators
            assert analyzer.sentiment_analyzer is not None

    def test_extract_financial_entities(self):
        """Test extraction of financial entities from text."""
        with patch('sentiment.social_media_analyzer.pipeline'):
            analyzer = CryptoTwitterAnalyzer()

            text = "$AAPL and $MSFT looking strong! #trading #bullish @ElonMusk"
            symbols, hashtags, mentions = analyzer.extract_financial_entities(text)

            assert "AAPL" in symbols
            assert "MSFT" in symbols
            assert "trading" in hashtags
            assert "bullish" in hashtags
            assert "elonmusk" in mentions

    def test_calculate_financial_sentiment_bullish(self):
        """Test financial sentiment calculation for bullish text."""
        with patch('sentiment.social_media_analyzer.pipeline') as mock_pipeline:
            mock_classifier = Mock()
            mock_classifier.return_value = [{'label': 'POSITIVE', 'score': 0.8}]
            mock_pipeline.return_value = mock_classifier

            analyzer = CryptoTwitterAnalyzer()

            bullish_text = "$AAPL to the moon! 🚀 Diamond hands, bulls in control!"
            sentiment = analyzer.calculate_financial_sentiment(bullish_text)

            assert sentiment > 0  # Should be positive
            assert sentiment <= 1.0

    def test_calculate_financial_sentiment_bearish(self):
        """Test financial sentiment calculation for bearish text."""
        with patch('sentiment.social_media_analyzer.pipeline') as mock_pipeline:
            mock_classifier = Mock()
            mock_classifier.return_value = [{'label': 'NEGATIVE', 'score': 0.7}]
            mock_pipeline.return_value = mock_classifier

            analyzer = CryptoTwitterAnalyzer()

            bearish_text = "$AAPL crash incoming! Bears taking control, dump everything!"
            sentiment = analyzer.calculate_financial_sentiment(bearish_text)

            assert sentiment < 0  # Should be negative
            assert sentiment >= -1.0

    def test_calculate_engagement_score(self):
        """Test engagement score calculation."""
        with patch('sentiment.social_media_analyzer.pipeline'):
            analyzer = CryptoTwitterAnalyzer()

            # High engagement post
            high_engagement_post = SocialMediaPost(
                post_id="test1", platform="twitter", content="test",
                author="influencer", followers_count=10000,
                retweets_count=200, likes_count=800, timestamp=datetime.now(),
                symbols=[], hashtags=[], mentions=[],
                engagement_score=0, author_influence_score=0
            )

            score = analyzer.calculate_engagement_score(high_engagement_post)
            assert 0 <= score <= 1.0
            assert score > 0  # Should have positive engagement

            # Low engagement post
            low_engagement_post = SocialMediaPost(
                post_id="test2", platform="twitter", content="test",
                author="user", followers_count=100,
                retweets_count=1, likes_count=2, timestamp=datetime.now(),
                symbols=[], hashtags=[], mentions=[],
                engagement_score=0, author_influence_score=0
            )

            low_score = analyzer.calculate_engagement_score(low_engagement_post)
            assert low_score < score  # Should be lower than high engagement

    def test_calculate_author_influence(self):
        """Test author influence calculation."""
        with patch('sentiment.social_media_analyzer.pipeline'):
            analyzer = CryptoTwitterAnalyzer()

            # High influence user
            high_influence = SocialMediaPost(
                post_id="test", platform="twitter", content="test",
                author="whale", followers_count=50000,
                retweets_count=0, likes_count=0, timestamp=datetime.now(),
                symbols=[], hashtags=[], mentions=[],
                engagement_score=0, author_influence_score=0
            )

            influence = analyzer.calculate_author_influence(high_influence)
            assert influence == 1.0  # Should be maximum influence

            # Low influence user
            low_influence = SocialMediaPost(
                post_id="test", platform="twitter", content="test",
                author="newbie", followers_count=50,
                retweets_count=0, likes_count=0, timestamp=datetime.now(),
                symbols=[], hashtags=[], mentions=[],
                engagement_score=0, author_influence_score=0
            )

            low_inf = analyzer.calculate_author_influence(low_influence)
            assert low_inf == 0.1  # Should be minimum influence
            assert low_inf < influence

class TestSocialMediaDataGenerator:
    """Test SocialMediaDataGenerator functionality."""

    def test_generator_initialization(self):
        """Test SocialMediaDataGenerator initialization."""
        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'):
            generator = SocialMediaDataGenerator()

            assert hasattr(generator, 'sample_users')
            assert hasattr(generator, 'bullish_templates')
            assert hasattr(generator, 'bearish_templates')
            assert hasattr(generator, 'neutral_templates')
            assert len(generator.sample_users) > 0

    def test_generate_social_media_posts(self):
        """Test generation of social media posts."""
        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.extract_financial_entities.return_value = (["AAPL"], ["trading"], [])
            mock_analyzer_instance.calculate_engagement_score.return_value = 0.5
            mock_analyzer_instance.calculate_author_influence.return_value = 0.6
            mock_analyzer.return_value = mock_analyzer_instance

            generator = SocialMediaDataGenerator()
            posts = generator.generate_social_media_posts(["AAPL", "MSFT"], hours_back=2, posts_per_hour=5)

            assert len(posts) == 10  # 2 hours * 5 posts per hour
            assert all(isinstance(post, SocialMediaPost) for post in posts)
            assert all(post.platform == "twitter" for post in posts)

            # Check that posts contain symbols
            symbols_found = set()
            for post in posts:
                symbols_found.update(post.symbols)
            assert "AAPL" in symbols_found or "MSFT" in symbols_found

class TestSocialSentimentAnalyzer:
    """Test SocialSentimentAnalyzer functionality."""

    def test_analyzer_initialization(self, mock_connection_pool, mock_env):
        """Test SocialSentimentAnalyzer initialization."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            assert analyzer.pool == pool
            assert analyzer.env == mock_env
            assert hasattr(analyzer, 'crypto_analyzer')
            assert hasattr(analyzer, 'data_generator')

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_social_sentiment(self, mock_connection_pool, mock_env, sample_social_media_post):
        """Test complete social sentiment analysis."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer') as mock_crypto, \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator') as mock_generator:

            # Mock components
            mock_crypto_instance = Mock()
            mock_generator_instance = Mock()

            mock_crypto.return_value = mock_crypto_instance
            mock_generator.return_value = mock_generator_instance

            # Mock data generation
            mock_generator_instance.generate_social_media_posts.return_value = [sample_social_media_post]

            # Mock sentiment analysis
            mock_crypto_instance.calculate_financial_sentiment.return_value = 0.8

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            with patch.object(analyzer, '_calculate_sentiment_metrics') as mock_metrics, \
                 patch.object(analyzer, '_generate_social_trading_signal') as mock_signal, \
                 patch.object(analyzer, '_store_social_analysis', new_callable=AsyncMock):

                mock_metrics.return_value = SocialSentimentMetrics(
                    symbol="AAPL", timestamp=datetime.now(), total_posts=1,
                    total_engagement=125, average_sentiment=0.8, sentiment_std=0.1,
                    bullish_ratio=1.0, bearish_ratio=0.0, neutral_ratio=0.0,
                    trending_score=0.5, influencer_sentiment=0.8, retail_sentiment=0.8,
                    momentum_score=0.2, top_hashtags=["trading"], top_keywords=["moon"]
                )

                mock_signal.return_value = SocialTradingSignal(
                    symbol="AAPL", signal_type="momentum", signal_strength=0.8,
                    confidence=0.9, time_horizon="short", supporting_posts=[sample_social_media_post],
                    key_metrics=mock_metrics.return_value, risk_factors=[], timestamp=datetime.now()
                )

                signals = await analyzer.analyze_social_sentiment(["AAPL"], hours_back=24)

                assert isinstance(signals, dict)
                assert "AAPL" in signals
                assert signals["AAPL"].signal_type == "momentum"

    def test_calculate_sentiment_metrics(self, mock_connection_pool, mock_env):
        """Test sentiment metrics calculation."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            # Create test posts with varying sentiment
            analyzed_posts = []
            for i in range(10):
                post = SocialMediaPost(
                    post_id=f"post_{i}",
                    platform="twitter",
                    content=f"Test post {i}",
                    author=f"user_{i}",
                    followers_count=1000 + i * 500,
                    retweets_count=5 + i,
                    likes_count=20 + i * 3,
                    timestamp=datetime.now() - timedelta(hours=i),
                    symbols=["AAPL"],
                    hashtags=["trading"],
                    mentions=[],
                    engagement_score=0.02 + i * 0.01,
                    author_influence_score=0.3 + (i % 3) * 0.2  # Vary influence
                )
                sentiment = 0.5 + (i % 3 - 1) * 0.3  # Mix of positive, negative, neutral
                analyzed_posts.append((post, sentiment))

            metrics = analyzer._calculate_sentiment_metrics("AAPL", analyzed_posts)

            assert isinstance(metrics, SocialSentimentMetrics)
            assert metrics.symbol == "AAPL"
            assert metrics.total_posts == 10
            assert 0 <= metrics.bullish_ratio <= 1
            assert 0 <= metrics.bearish_ratio <= 1
            assert 0 <= metrics.neutral_ratio <= 1
            assert abs(metrics.bullish_ratio + metrics.bearish_ratio + metrics.neutral_ratio - 1.0) < 0.01

    def test_calculate_sentiment_momentum(self, mock_connection_pool, mock_env):
        """Test sentiment momentum calculation."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            # Create posts with increasing sentiment over time
            symbol_posts = []
            for i in range(8):
                post = SocialMediaPost(
                    post_id=f"post_{i}",
                    platform="twitter",
                    content="test",
                    author="user",
                    followers_count=1000,
                    retweets_count=5,
                    likes_count=20,
                    timestamp=datetime.now() - timedelta(hours=8-i),  # Chronological
                    symbols=["AAPL"],
                    hashtags=[],
                    mentions=[],
                    engagement_score=0.02,
                    author_influence_score=0.3
                )
                sentiment = -0.5 + i * 0.15  # Increasing sentiment
                symbol_posts.append((post, sentiment))

            momentum = analyzer._calculate_sentiment_momentum(symbol_posts)

            # Should be positive since sentiment is increasing
            assert momentum > 0
            assert -1.0 <= momentum <= 1.0

    def test_generate_social_trading_signal(self, mock_connection_pool, mock_env):
        """Test social trading signal generation."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            # Create strong momentum scenario
            metrics = SocialSentimentMetrics(
                symbol="AAPL",
                timestamp=datetime.now(),
                total_posts=20,
                total_engagement=1000,
                average_sentiment=0.7,
                sentiment_std=0.2,
                bullish_ratio=0.8,
                bearish_ratio=0.1,
                neutral_ratio=0.1,
                trending_score=0.9,
                influencer_sentiment=0.8,
                retail_sentiment=0.6,
                momentum_score=0.5,  # Strong momentum
                top_hashtags=["bullish"],
                top_keywords=["moon"]
            )

            # Mock analyzed posts
            analyzed_posts = [(Mock(), 0.7) for _ in range(20)]

            signal = analyzer._generate_social_trading_signal("AAPL", metrics, analyzed_posts)

            assert isinstance(signal, SocialTradingSignal)
            assert signal.symbol == "AAPL"
            assert signal.signal_type == "momentum"  # Should detect momentum
            assert signal.signal_strength > 0  # Should be bullish
            assert 0 <= signal.confidence <= 1

    def test_generate_contrarian_signal(self, mock_connection_pool, mock_env):
        """Test contrarian signal generation."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            # Create contrarian scenario (high sentiment but negative momentum)
            metrics = SocialSentimentMetrics(
                symbol="AAPL",
                timestamp=datetime.now(),
                total_posts=25,
                total_engagement=1500,
                average_sentiment=0.8,  # High positive sentiment
                sentiment_std=0.3,
                bullish_ratio=0.9,
                bearish_ratio=0.05,
                neutral_ratio=0.05,
                trending_score=0.6,
                influencer_sentiment=0.8,
                retail_sentiment=0.8,
                momentum_score=-0.3,  # Negative momentum (decreasing)
                top_hashtags=["bullish"],
                top_keywords=["high"]
            )

            analyzed_posts = [(Mock(), 0.8) for _ in range(25)]

            signal = analyzer._generate_social_trading_signal("AAPL", metrics, analyzed_posts)

            assert isinstance(signal, SocialTradingSignal)
            assert signal.signal_type == "contrarian"
            assert signal.signal_strength < 0  # Should be bearish (contrarian)
            assert "Contrarian signal" in signal.risk_factors

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_store_social_analysis(self, mock_connection_pool, mock_env):
        """Test storing social analysis results."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            # Mock database operations
            conn.execute = AsyncMock()

            # Test data
            post = SocialMediaPost(
                post_id="test_post",
                platform="twitter",
                content="Test content",
                author="test_user",
                followers_count=1000,
                retweets_count=5,
                likes_count=20,
                timestamp=datetime.now(),
                symbols=["AAPL"],
                hashtags=["trading"],
                mentions=[],
                engagement_score=0.025,
                author_influence_score=0.3
            )

            signal = SocialTradingSignal(
                symbol="AAPL",
                signal_type="momentum",
                signal_strength=0.8,
                confidence=0.9,
                time_horizon="short",
                supporting_posts=[post],
                key_metrics=SocialSentimentMetrics(
                    symbol="AAPL", timestamp=datetime.now(), total_posts=1,
                    total_engagement=25, average_sentiment=0.8, sentiment_std=0.1,
                    bullish_ratio=1.0, bearish_ratio=0.0, neutral_ratio=0.0,
                    trending_score=0.5, influencer_sentiment=0.8, retail_sentiment=0.8,
                    momentum_score=0.2, top_hashtags=["trading"], top_keywords=["test"]
                ),
                risk_factors=[],
                timestamp=datetime.now()
            )

            await analyzer._store_social_analysis([(post, 0.8)], {"AAPL": signal})

            # Verify database calls were made
            assert conn.execute.call_count >= 2  # At least one for post, one for signal

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_social_sentiment_history(self, mock_connection_pool, mock_env):
        """Test getting social sentiment history."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            # Mock database results
            mock_rows = [
                {
                    'date': datetime(2024, 1, 15).date(),
                    'avg_signal_strength': 0.7,
                    'avg_confidence': 0.8,
                    'avg_sentiment': 0.6,
                    'avg_trending_score': 0.5,
                    'total_posts': 50
                }
            ]
            conn.fetch.return_value = mock_rows

            df = await analyzer.get_social_sentiment_history("AAPL", days_back=7)

            assert isinstance(df, pd.DataFrame)
            if not df.empty:
                assert 'date' in df.columns
                assert 'avg_signal_strength' in df.columns

class TestConvenienceFunction:
    """Test convenience function."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_social_media_sentiment(self, mock_connection_pool, mock_env):
        """Test convenience function."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.SocialSentimentAnalyzer') as mock_analyzer_class:
            mock_analyzer = AsyncMock()
            mock_analyzer.analyze_social_sentiment.return_value = {
                "AAPL": SocialTradingSignal(
                    symbol="AAPL",
                    signal_type="momentum",
                    signal_strength=0.8,
                    confidence=0.9,
                    time_horizon="short",
                    supporting_posts=[],
                    key_metrics=Mock(),
                    risk_factors=[],
                    timestamp=datetime.now()
                )
            }
            mock_analyzer_class.return_value = mock_analyzer

            result = await analyze_social_media_sentiment(pool, mock_env, ["AAPL"], hours_back=24)

            assert isinstance(result, dict)
            assert "AAPL" in result
            assert result["AAPL"].signal_type == "momentum"

class TestErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_no_social_posts_found(self, mock_connection_pool, mock_env):
        """Test handling when no social media posts are found."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator') as mock_generator:

            mock_generator_instance = Mock()
            mock_generator_instance.generate_social_media_posts.return_value = []
            mock_generator.return_value = mock_generator_instance

            analyzer = SocialSentimentAnalyzer(pool, mock_env)
            result = await analyzer.analyze_social_sentiment(["AAPL"], hours_back=24)

            assert isinstance(result, dict)
            assert len(result) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_social_analysis_error(self, mock_connection_pool, mock_env):
        """Test handling of social analysis errors."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator') as mock_generator:

            mock_generator_instance = Mock()
            mock_generator_instance.generate_social_media_posts.side_effect = Exception("Data error")
            mock_generator.return_value = mock_generator_instance

            analyzer = SocialSentimentAnalyzer(pool, mock_env)
            result = await analyzer.analyze_social_sentiment(["AAPL"], hours_back=24)

            # Should return empty dict on error
            assert isinstance(result, dict)
            assert len(result) == 0

    def test_sentiment_analyzer_load_failure(self):
        """Test handling when sentiment analyzer fails to load."""
        with patch('sentiment.social_media_analyzer.pipeline', side_effect=Exception("Model error")):
            analyzer = CryptoTwitterAnalyzer()

            # Should handle gracefully
            assert analyzer.sentiment_analyzer is None

            # Should still return valid sentiment
            sentiment = analyzer.calculate_financial_sentiment("Test text")
            assert isinstance(sentiment, float)
            assert -1.0 <= sentiment <= 1.0

    def test_empty_metrics_handling(self, mock_connection_pool, mock_env):
        """Test handling of empty sentiment metrics."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            # Test with no posts for symbol
            metrics = analyzer._calculate_sentiment_metrics("UNKNOWN", [])

            assert isinstance(metrics, SocialSentimentMetrics)
            assert metrics.total_posts == 0
            assert metrics.average_sentiment == 0.0
            assert metrics.bullish_ratio == 0.0

    def test_insufficient_posts_for_signal(self, mock_connection_pool, mock_env):
        """Test signal generation with insufficient posts."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            # Metrics with too few posts
            metrics = SocialSentimentMetrics(
                symbol="AAPL", timestamp=datetime.now(), total_posts=2,  # Too few
                total_engagement=10, average_sentiment=0.5, sentiment_std=0.1,
                bullish_ratio=0.5, bearish_ratio=0.5, neutral_ratio=0.0,
                trending_score=0.3, influencer_sentiment=0.5, retail_sentiment=0.5,
                momentum_score=0.1, top_hashtags=[], top_keywords=[]
            )

            signal = analyzer._generate_social_trading_signal("AAPL", metrics, [])

            # Should return None for insufficient data
            assert signal is None

class TestPerformanceAndEdgeCases:
    """Test performance and edge cases."""

    def test_large_post_volume(self, mock_connection_pool, mock_env):
        """Test handling large volumes of posts."""
        pool, conn = mock_connection_pool

        with patch('sentiment.social_media_analyzer.CryptoTwitterAnalyzer'), \
             patch('sentiment.social_media_analyzer.SocialMediaDataGenerator'):

            analyzer = SocialSentimentAnalyzer(pool, mock_env)

            # Create large number of posts
            analyzed_posts = []
            for i in range(1000):
                post = SocialMediaPost(
                    post_id=f"post_{i}",
                    platform="twitter",
                    content=f"Post {i}",
                    author=f"user_{i}",
                    followers_count=100 + i,
                    retweets_count=1,
                    likes_count=5,
                    timestamp=datetime.now() - timedelta(minutes=i),
                    symbols=["AAPL"],
                    hashtags=["trading"],
                    mentions=[],
                    engagement_score=0.01,
                    author_influence_score=0.1
                )
                sentiment = np.random.normal(0, 0.3)  # Random sentiment
                analyzed_posts.append((post, sentiment))

            # Should handle large volume without errors
            metrics = analyzer._calculate_sentiment_metrics("AAPL", analyzed_posts)

            assert isinstance(metrics, SocialSentimentMetrics)
            assert metrics.total_posts == 1000
            assert len(metrics.top_hashtags) <= 5
            assert len(metrics.top_keywords) <= 5

    def test_extreme_sentiment_values(self):
        """Test handling of extreme sentiment values."""
        with patch('sentiment.social_media_analyzer.pipeline'):
            analyzer = CryptoTwitterAnalyzer()

            # Test with extreme positive indicators
            extreme_positive = "moon rocket bulls diamond hands pump to moon bulls bulls bulls"
            sentiment = analyzer.calculate_financial_sentiment(extreme_positive)
            assert sentiment <= 1.0  # Should be capped

            # Test with extreme negative indicators
            extreme_negative = "crash dump bears crash dump bears crash dump bears"
            sentiment = analyzer.calculate_financial_sentiment(extreme_negative)
            assert sentiment >= -1.0  # Should be capped

    def test_unicode_and_emoji_handling(self):
        """Test handling of unicode characters and emojis."""
        with patch('sentiment.social_media_analyzer.pipeline') as mock_pipeline:
            mock_classifier = Mock()
            mock_classifier.return_value = [{'label': 'POSITIVE', 'score': 0.8}]
            mock_pipeline.return_value = mock_classifier

            analyzer = CryptoTwitterAnalyzer()

            # Text with emojis and unicode
            emoji_text = "$AAPL 🚀🌙💎🙌 going to the moon! 📈💰"
            sentiment = analyzer.calculate_financial_sentiment(emoji_text)

            assert isinstance(sentiment, float)
            assert -1.0 <= sentiment <= 1.0

if __name__ == "__main__":
    pytest.main([__file__])