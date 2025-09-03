"""
Tests for news sentiment analysis framework.
"""

import pytest
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import asyncpg
import feedparser

from sentiment.news_sentiment_analyzer import (
    SentimentScore,
    NewsArticle,
    SentimentSignal,
    FinBERTSentimentAnalyzer,
    VADERSentimentAnalyzer,
    NewsContentFetcher,
    NewsSentimentAnalyzer,
    analyze_symbol_sentiment
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
def sample_sentiment_score():
    """Sample sentiment score."""
    return SentimentScore(
        compound_score=0.75,
        positive=0.8,
        negative=0.1,
        neutral=0.1,
        confidence=0.9,
        source="finbert",
        timestamp=datetime(2024, 1, 15, 10, 30, 0),
        text_snippet="Apple reports strong quarterly earnings with record revenue growth"
    )


@pytest.fixture
def sample_news_article():
    """Sample news article."""
    return NewsArticle(
        title="Apple Reports Record Q4 Earnings",
        content="Apple Inc. reported record quarterly earnings with revenue growth of 12%...",
        url="https://example.com/apple-earnings",
        source="reuters",
        published_date=datetime(2024, 1, 15, 9, 0, 0),
        symbols=["AAPL"],
        sentiment=None,  # Will be filled by analyzer
        relevance_score=0.95,
        impact_score=0.8
    )


class TestSentimentScore:
    """Test SentimentScore dataclass."""
    
    def test_sentiment_score_creation(self, sample_sentiment_score):
        """Test SentimentScore creation and attributes."""
        score = sample_sentiment_score
        
        assert score.compound_score == 0.75
        assert score.positive == 0.8
        assert score.negative == 0.1
        assert score.neutral == 0.1
        assert score.confidence == 0.9
        assert score.source == "finbert"
        assert isinstance(score.timestamp, datetime)
        assert "earnings" in score.text_snippet.lower()


class TestNewsArticle:
    """Test NewsArticle dataclass."""
    
    def test_news_article_creation(self, sample_news_article):
        """Test NewsArticle creation and attributes."""
        article = sample_news_article
        
        assert article.title == "Apple Reports Record Q4 Earnings"
        assert "AAPL" in article.symbols
        assert article.source == "reuters"
        assert article.relevance_score == 0.95
        assert isinstance(article.published_date, datetime)


class TestFinBERTSentimentAnalyzer:
    """Test FinBERT sentiment analyzer."""
    
    def test_finbert_analyzer_initialization(self):
        """Test FinBERT analyzer initialization."""
        with patch('sentiment.news_sentiment_analyzer.AutoTokenizer'), \
             patch('sentiment.news_sentiment_analyzer.AutoModelForSequenceClassification'), \
             patch('sentiment.news_sentiment_analyzer.pipeline') as mock_pipeline:
            
            mock_pipeline.return_value = Mock()
            analyzer = FinBERTSentimentAnalyzer()
            
            assert analyzer.model_name == "finbert"
            assert analyzer.classifier is not None
    
    def test_finbert_analyzer_fallback(self):
        """Test FinBERT analyzer fallback to alternative model."""
        with patch('sentiment.news_sentiment_analyzer.AutoTokenizer', side_effect=Exception("Model not found")), \
             patch('sentiment.news_sentiment_analyzer.pipeline') as mock_pipeline:
            
            mock_pipeline.return_value = Mock()
            analyzer = FinBERTSentimentAnalyzer()
            
            assert analyzer.model_name == "bert-financial"
    
    def test_analyze_sentiment_positive(self):
        """Test sentiment analysis for positive text."""
        with patch('sentiment.news_sentiment_analyzer.AutoTokenizer'), \
             patch('sentiment.news_sentiment_analyzer.AutoModelForSequenceClassification'), \
             patch('sentiment.news_sentiment_analyzer.pipeline') as mock_pipeline:
            
            mock_classifier = Mock()
            mock_classifier.return_value = [{'label': 'positive', 'score': 0.85}]
            mock_pipeline.return_value = mock_classifier
            
            analyzer = FinBERTSentimentAnalyzer()
            text = "Apple reports strong earnings with record revenue growth"
            
            result = analyzer.analyze_sentiment(text)
            
            assert isinstance(result, SentimentScore)
            assert result.compound_score == 0.85
            assert result.positive == 0.85
            assert result.confidence == 0.85
            assert result.source == "finbert"
    
    def test_analyze_sentiment_negative(self):
        """Test sentiment analysis for negative text."""
        with patch('sentiment.news_sentiment_analyzer.AutoTokenizer'), \
             patch('sentiment.news_sentiment_analyzer.AutoModelForSequenceClassification'), \
             patch('sentiment.news_sentiment_analyzer.pipeline') as mock_pipeline:
            
            mock_classifier = Mock()
            mock_classifier.return_value = [{'label': 'negative', 'score': 0.75}]
            mock_pipeline.return_value = mock_classifier
            
            analyzer = FinBERTSentimentAnalyzer()
            text = "Apple faces significant challenges with declining sales"
            
            result = analyzer.analyze_sentiment(text)
            
            assert result.compound_score == -0.75
            assert result.negative == 0.75
            assert result.confidence == 0.75
    
    def test_clean_financial_text(self):
        """Test financial text cleaning."""
        with patch('sentiment.news_sentiment_analyzer.AutoTokenizer'), \
             patch('sentiment.news_sentiment_analyzer.AutoModelForSequenceClassification'), \
             patch('sentiment.news_sentiment_analyzer.pipeline'):
            
            analyzer = FinBERTSentimentAnalyzer()
            
            text = "Stock up 15% to $125.50 with https://example.com/news"
            cleaned = analyzer._clean_financial_text(text)
            
            assert "125.50 dollars" in cleaned
            assert "15 percent" in cleaned
            assert "https://" not in cleaned
    
    def test_fallback_sentiment(self):
        """Test fallback sentiment analysis."""
        with patch('sentiment.news_sentiment_analyzer.AutoTokenizer'), \
             patch('sentiment.news_sentiment_analyzer.AutoModelForSequenceClassification'), \
             patch('sentiment.news_sentiment_analyzer.pipeline'):
            
            analyzer = FinBERTSentimentAnalyzer()
            
            with patch('sentiment.news_sentiment_analyzer.TextBlob') as mock_textblob:
                mock_blob = Mock()
                mock_blob.sentiment.polarity = 0.6
                mock_textblob.return_value = mock_blob
                
                result = analyzer._fallback_sentiment("Positive news about earnings")
                
                assert result.compound_score == 0.6
                assert result.source == "textblob_fallback"


class TestVADERSentimentAnalyzer:
    """Test VADER sentiment analyzer."""
    
    def test_vader_analyzer_initialization(self):
        """Test VADER analyzer initialization."""
        with patch('sentiment.news_sentiment_analyzer.nltk.download'), \
             patch('sentiment.news_sentiment_analyzer.SentimentIntensityAnalyzer') as mock_sia:
            
            mock_analyzer = Mock()
            mock_sia.return_value = mock_analyzer
            
            analyzer = VADERSentimentAnalyzer()
            assert analyzer.analyzer == mock_analyzer
    
    def test_analyze_sentiment_with_vader(self):
        """Test VADER sentiment analysis."""
        with patch('sentiment.news_sentiment_analyzer.nltk.download'), \
             patch('sentiment.news_sentiment_analyzer.SentimentIntensityAnalyzer') as mock_sia:
            
            mock_analyzer = Mock()
            mock_analyzer.polarity_scores.return_value = {
                'compound': 0.8,
                'pos': 0.7,
                'neg': 0.1,
                'neu': 0.2
            }
            mock_sia.return_value = mock_analyzer
            
            analyzer = VADERSentimentAnalyzer()
            result = analyzer.analyze_sentiment("Great earnings report!")
            
            assert isinstance(result, SentimentScore)
            assert result.compound_score == 0.8
            assert result.positive == 0.7
            assert result.negative == 0.1
            assert result.neutral == 0.2
            assert result.source == "vader"


class TestNewsContentFetcher:
    """Test news content fetcher."""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_content_fetcher_initialization(self):
        """Test news content fetcher initialization."""
        with patch('sentiment.news_sentiment_analyzer.aiohttp.ClientSession'):
            fetcher = NewsContentFetcher()
            assert hasattr(fetcher, 'rss_feeds')
            assert 'reuters_business' in fetcher.rss_feeds
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_news_for_symbols(self):
        """Test fetching news for symbols."""
        with patch('sentiment.news_sentiment_analyzer.aiohttp.ClientSession') as mock_session:
            # Mock session and response
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text.return_value = """<?xml version="1.0"?>
                <rss><channel>
                    <item>
                        <title>Apple Reports Strong Earnings</title>
                        <summary>Apple Inc. reported record quarterly earnings</summary>
                        <link>https://example.com/apple-news</link>
                        <pubDate>Mon, 15 Jan 2024 10:00:00 GMT</pubDate>
                    </item>
                </channel></rss>"""
            
            mock_session_instance = AsyncMock()
            mock_session_instance.get.return_value.__aenter__.return_value = mock_response
            mock_session.return_value = mock_session_instance
            
            fetcher = NewsContentFetcher()
            
            with patch('sentiment.news_sentiment_analyzer.feedparser.parse') as mock_parse:
                mock_parse.return_value = Mock(entries=[
                    Mock(
                        title="Apple Reports Strong Earnings",
                        summary="Apple Inc. reported record quarterly earnings",
                        link="https://example.com/apple-news",
                        published_parsed=(2024, 1, 15, 10, 0, 0)
                    )
                ])
                
                articles = await fetcher.fetch_news_for_symbols(["AAPL"], hours_back=24)
                
                assert len(articles) > 0
                assert any("Apple" in article.title for article in articles)
    
    def test_extract_symbols_from_text(self):
        """Test symbol extraction from text."""
        with patch('sentiment.news_sentiment_analyzer.aiohttp.ClientSession'):
            fetcher = NewsContentFetcher()
            
            text = "AAPL reports earnings while MSFT shows growth"
            symbols = fetcher._extract_symbols_from_text(text, ["AAPL", "MSFT", "GOOGL"])
            
            assert "AAPL" in symbols
            assert "MSFT" in symbols
            assert "GOOGL" not in symbols
    
    def test_calculate_relevance_score(self):
        """Test relevance score calculation."""
        with patch('sentiment.news_sentiment_analyzer.aiohttp.ClientSession'):
            fetcher = NewsContentFetcher()
            
            # High relevance text
            high_relevance_text = "Apple earnings report shows strong revenue and profit growth"
            score = fetcher._calculate_relevance_score(high_relevance_text, ["AAPL"])
            assert score > 0.7
            
            # Low relevance text
            low_relevance_text = "General market news"
            score = fetcher._calculate_relevance_score(low_relevance_text, [])
            assert score < 0.3
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_close_session(self):
        """Test closing aiohttp session."""
        with patch('sentiment.news_sentiment_analyzer.aiohttp.ClientSession') as mock_session:
            mock_session_instance = AsyncMock()
            mock_session.return_value = mock_session_instance
            
            fetcher = NewsContentFetcher()
            await fetcher.close()
            
            mock_session_instance.close.assert_called_once()


class TestNewsSentimentAnalyzer:
    """Test main news sentiment analyzer."""
    
    def test_analyzer_initialization(self, mock_connection_pool, mock_env):
        """Test news sentiment analyzer initialization."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher'):
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            
            assert analyzer.pool == pool
            assert analyzer.env == mock_env
            assert hasattr(analyzer, 'finbert')
            assert hasattr(analyzer, 'vader')
            assert hasattr(analyzer, 'content_fetcher')
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_news_sentiment(self, mock_connection_pool, mock_env, sample_news_article):
        """Test complete news sentiment analysis."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer') as mock_finbert, \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer') as mock_vader, \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher') as mock_fetcher:
            
            # Mock components
            mock_finbert_instance = Mock()
            mock_vader_instance = Mock()
            mock_fetcher_instance = AsyncMock()
            
            mock_finbert.return_value = mock_finbert_instance
            mock_vader.return_value = mock_vader_instance
            mock_fetcher.return_value = mock_fetcher_instance
            
            # Mock fetch results
            mock_fetcher_instance.fetch_news_for_symbols.return_value = [sample_news_article]
            
            # Mock sentiment analysis
            mock_sentiment = SentimentScore(
                compound_score=0.8, positive=0.8, negative=0.1, neutral=0.1,
                confidence=0.9, source="finbert_vader_ensemble",
                timestamp=datetime.now(), text_snippet="earnings"
            )
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            
            with patch.object(analyzer, '_analyze_article_sentiment', return_value=mock_sentiment), \
                 patch.object(analyzer, '_generate_sentiment_signal') as mock_generate, \
                 patch.object(analyzer, '_store_sentiment_analysis', new_callable=AsyncMock):
                
                mock_signal = SentimentSignal(
                    symbol="AAPL",
                    signal_strength=0.8,
                    signal_direction="bullish",
                    confidence=0.9,
                    time_horizon="short",
                    supporting_articles=[sample_news_article],
                    sentiment_momentum=0.2,
                    volume_weighted_sentiment=0.8,
                    timestamp=datetime.now()
                )
                mock_generate.return_value = mock_signal
                
                signals = await analyzer.analyze_news_sentiment(["AAPL"], hours_back=24)
                
                assert isinstance(signals, dict)
                assert "AAPL" in signals
                assert signals["AAPL"].signal_direction == "bullish"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_article_sentiment(self, mock_connection_pool, mock_env, sample_news_article):
        """Test individual article sentiment analysis."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer') as mock_finbert, \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer') as mock_vader, \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher'):
            
            # Mock sentiment results
            finbert_sentiment = SentimentScore(
                compound_score=0.8, positive=0.8, negative=0.1, neutral=0.1,
                confidence=0.9, source="finbert", timestamp=datetime.now(), text_snippet="test"
            )
            
            vader_sentiment = SentimentScore(
                compound_score=0.7, positive=0.7, negative=0.15, neutral=0.15,
                confidence=0.8, source="vader", timestamp=datetime.now(), text_snippet="test"
            )
            
            mock_finbert_instance = Mock()
            mock_vader_instance = Mock()
            mock_finbert_instance.analyze_sentiment.return_value = finbert_sentiment
            mock_vader_instance.analyze_sentiment.return_value = vader_sentiment
            
            mock_finbert.return_value = mock_finbert_instance
            mock_vader.return_value = mock_vader_instance
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            result = await analyzer._analyze_article_sentiment(sample_news_article)
            
            assert isinstance(result, SentimentScore)
            assert result.source == "finbert_vader_ensemble"
            # Combined score should be weighted average
            expected_score = 0.8 * 0.6 + 0.7 * 0.4  # finbert_weight * score + vader_weight * score
            assert abs(result.compound_score - expected_score) < 0.01
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_sentiment_signal(self, mock_connection_pool, mock_env):
        """Test sentiment signal generation."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher'):
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            
            # Create test articles with sentiment
            articles = []
            for i in range(3):
                article = NewsArticle(
                    title=f"Test Article {i}",
                    content="Test content",
                    url=f"https://example.com/{i}",
                    source="test",
                    published_date=datetime.now() - timedelta(hours=i),
                    symbols=["AAPL"],
                    sentiment=SentimentScore(
                        compound_score=0.5 + i * 0.1,
                        positive=0.6 + i * 0.1,
                        negative=0.2,
                        neutral=0.2,
                        confidence=0.8,
                        source="test",
                        timestamp=datetime.now(),
                        text_snippet="test"
                    ),
                    relevance_score=0.9,
                    impact_score=0.8
                )
                articles.append(article)
            
            signal = await analyzer._generate_sentiment_signal("AAPL", articles)
            
            assert isinstance(signal, SentimentSignal)
            assert signal.symbol == "AAPL"
            assert signal.signal_direction in ["bullish", "bearish", "neutral"]
            assert 0 <= signal.confidence <= 1
            assert len(signal.supporting_articles) == 3
    
    def test_calculate_sentiment_momentum(self, mock_connection_pool, mock_env):
        """Test sentiment momentum calculation."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher'):
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            
            # Create articles with increasing sentiment over time
            articles = []
            for i in range(6):
                article = NewsArticle(
                    title=f"Article {i}",
                    content="content",
                    url=f"url{i}",
                    source="test",
                    published_date=datetime.now() - timedelta(hours=6-i),  # Chronological order
                    symbols=["AAPL"],
                    sentiment=SentimentScore(
                        compound_score=-0.5 + i * 0.2,  # Increasing sentiment
                        positive=0.3 + i * 0.1,
                        negative=0.4 - i * 0.1,
                        neutral=0.3,
                        confidence=0.8,
                        source="test",
                        timestamp=datetime.now(),
                        text_snippet="test"
                    ),
                    relevance_score=0.9,
                    impact_score=0.8
                )
                articles.append(article)
            
            momentum = analyzer._calculate_sentiment_momentum(articles)
            
            # Should be positive since sentiment is increasing over time
            assert momentum > 0
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_store_sentiment_analysis(self, mock_connection_pool, mock_env):
        """Test storing sentiment analysis results."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher'):
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            
            # Mock database operations
            conn.execute = AsyncMock()
            
            # Test data
            article = NewsArticle(
                title="Test Article",
                content="Test content",
                url="https://example.com/test",
                source="test",
                published_date=datetime.now(),
                symbols=["AAPL"],
                sentiment=SentimentScore(
                    compound_score=0.8,
                    positive=0.8,
                    negative=0.1,
                    neutral=0.1,
                    confidence=0.9,
                    source="test",
                    timestamp=datetime.now(),
                    text_snippet="test"
                ),
                relevance_score=0.9,
                impact_score=0.8
            )
            
            signal = SentimentSignal(
                symbol="AAPL",
                signal_strength=0.8,
                signal_direction="bullish",
                confidence=0.9,
                time_horizon="short",
                supporting_articles=[article],
                sentiment_momentum=0.2,
                volume_weighted_sentiment=0.8,
                timestamp=datetime.now()
            )
            
            await analyzer._store_sentiment_analysis([article], {"AAPL": signal})
            
            # Verify database calls were made
            assert conn.execute.call_count >= 2  # At least one for article, one for signal
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_sentiment_history(self, mock_connection_pool, mock_env):
        """Test getting sentiment history."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher'):
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            
            # Mock database results
            mock_rows = [
                {
                    'date': datetime(2024, 1, 15).date(),
                    'avg_signal_strength': 0.7,
                    'avg_confidence': 0.8,
                    'avg_momentum': 0.2,
                    'signal_count': 5
                }
            ]
            conn.fetch.return_value = mock_rows
            
            df = await analyzer.get_sentiment_history("AAPL", days_back=7)
            
            assert isinstance(df, pd.DataFrame)
            if not df.empty:
                assert 'date' in df.columns
                assert 'avg_signal_strength' in df.columns
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_close_resources(self, mock_connection_pool, mock_env):
        """Test closing analyzer resources."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher') as mock_fetcher:
            
            mock_fetcher_instance = AsyncMock()
            mock_fetcher.return_value = mock_fetcher_instance
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            await analyzer.close()
            
            mock_fetcher_instance.close.assert_called_once()


class TestConvenienceFunction:
    """Test convenience function."""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analyze_symbol_sentiment(self, mock_connection_pool, mock_env):
        """Test convenience function."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.NewsSentimentAnalyzer') as mock_analyzer_class:
            mock_analyzer = AsyncMock()
            mock_analyzer.analyze_news_sentiment.return_value = {
                "AAPL": SentimentSignal(
                    symbol="AAPL",
                    signal_strength=0.8,
                    signal_direction="bullish", 
                    confidence=0.9,
                    time_horizon="short",
                    supporting_articles=[],
                    sentiment_momentum=0.2,
                    volume_weighted_sentiment=0.8,
                    timestamp=datetime.now()
                )
            }
            mock_analyzer.close = AsyncMock()
            mock_analyzer_class.return_value = mock_analyzer
            
            result = await analyze_symbol_sentiment(pool, mock_env, ["AAPL"], hours_back=24)
            
            assert isinstance(result, dict)
            assert "AAPL" in result
            mock_analyzer.close.assert_called_once()


class TestErrorHandling:
    """Test error handling scenarios."""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_no_news_articles_found(self, mock_connection_pool, mock_env):
        """Test handling when no news articles are found."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher') as mock_fetcher:
            
            mock_fetcher_instance = AsyncMock()
            mock_fetcher_instance.fetch_news_for_symbols.return_value = []
            mock_fetcher.return_value = mock_fetcher_instance
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            result = await analyzer.analyze_news_sentiment(["AAPL"], hours_back=24)
            
            assert isinstance(result, dict)
            assert len(result) == 0
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_sentiment_analysis_error(self, mock_connection_pool, mock_env):
        """Test handling of sentiment analysis errors."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher') as mock_fetcher:
            
            mock_fetcher_instance = AsyncMock()
            mock_fetcher_instance.fetch_news_for_symbols.side_effect = Exception("API Error")
            mock_fetcher.return_value = mock_fetcher_instance
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            result = await analyzer.analyze_news_sentiment(["AAPL"], hours_back=24)
            
            # Should return empty dict on error
            assert isinstance(result, dict)
            assert len(result) == 0
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_error_handling(self, mock_connection_pool, mock_env):
        """Test handling of database errors."""
        pool, conn = mock_connection_pool
        
        with patch('sentiment.news_sentiment_analyzer.FinBERTSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.VADERSentimentAnalyzer'), \
             patch('sentiment.news_sentiment_analyzer.NewsContentFetcher'):
            
            analyzer = NewsSentimentAnalyzer(pool, mock_env)
            
            # Mock database error
            conn.execute.side_effect = asyncpg.PostgresError("Database error")
            
            # Should not raise exception
            await analyzer._store_sentiment_analysis([], {})
            # Error should be logged but not raised
    
    def test_finbert_model_load_failure(self):
        """Test handling when FinBERT model fails to load."""
        with patch('sentiment.news_sentiment_analyzer.AutoTokenizer', side_effect=Exception("Model error")), \
             patch('sentiment.news_sentiment_analyzer.pipeline', side_effect=Exception("Pipeline error")):
            
            analyzer = FinBERTSentimentAnalyzer()
            
            # Should fall back to error handling
            assert analyzer.classifier is None
            assert analyzer.model_name == "none"
            
            # Should still return valid sentiment score
            result = analyzer.analyze_sentiment("Test text")
            assert isinstance(result, SentimentScore)


if __name__ == "__main__":
    pytest.main([__file__])