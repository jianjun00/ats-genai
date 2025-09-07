"""
Advanced News Sentiment Analysis for Trading Signals

Integrates multiple sentiment analysis approaches for financial news and social media.
Based on MathTypes ATS sentiment analysis framework.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import re
import time

import asyncpg
import pandas as pd
import numpy as np
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import yfinance as yf
from textblob import TextBlob
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import feedparser
import aiohttp


@dataclass
class SentimentScore:
    """Comprehensive sentiment score for financial content."""
    compound_score: float  # -1 to 1, overall sentiment
    positive: float       # 0 to 1, positive sentiment strength
    negative: float       # 0 to 1, negative sentiment strength
    neutral: float        # 0 to 1, neutral sentiment strength
    confidence: float     # 0 to 1, confidence in the score
    source: str          # Source of the sentiment (finbert, vader, textblob, etc.)
    timestamp: datetime
    text_snippet: str    # First 200 chars of analyzed text


@dataclass
class NewsArticle:
    """News article with sentiment analysis."""
    title: str
    content: str
    url: str
    source: str
    published_date: datetime
    symbols: List[str]  # Associated stock symbols
    sentiment: SentimentScore
    relevance_score: float  # 0 to 1, relevance to symbols
    impact_score: float     # 0 to 1, estimated market impact


@dataclass
class SentimentSignal:
    """Trading signal based on sentiment analysis."""
    symbol: str
    signal_strength: float    # -1 to 1, strength of signal
    signal_direction: str     # 'bullish', 'bearish', 'neutral'
    confidence: float         # 0 to 1, confidence in signal
    time_horizon: str         # 'short', 'medium', 'long'
    supporting_articles: List[NewsArticle]
    sentiment_momentum: float # Change in sentiment over time
    volume_weighted_sentiment: float  # Sentiment weighted by news volume
    timestamp: datetime


class FinBERTSentimentAnalyzer:
    """
    Financial BERT-based sentiment analyzer optimized for financial text.
    """
    
    def __init__(self):
        try:
            # Try FinBERT model first (best for financial content)
            self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
            self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
            self.classifier = pipeline("sentiment-analysis", 
                                      model=self.model, 
                                      tokenizer=self.tokenizer)
            self.model_name = "finbert"
            logging.info("Loaded FinBERT model for sentiment analysis")
        except Exception as e:
            logging.warning(f"Failed to load FinBERT: {e}. Falling back to general model.")
            # Fallback to general financial sentiment model
            try:
                self.classifier = pipeline("sentiment-analysis", 
                                         model="nlptown/bert-base-multilingual-uncased-sentiment")
                self.model_name = "bert-financial"
                logging.info("Loaded fallback BERT financial model")
            except Exception as e2:
                logging.error(f"Failed to load any sentiment model: {e2}")
                self.classifier = None
                self.model_name = "none"
    
    def analyze_sentiment(self, text: str) -> SentimentScore:
        """Analyze sentiment of financial text using FinBERT."""
        if not self.classifier:
            return self._fallback_sentiment(text)
        
        try:
            # Clean and truncate text for model
            cleaned_text = self._clean_financial_text(text)
            if len(cleaned_text) > 512:
                cleaned_text = cleaned_text[:512]
            
            # Get sentiment from model
            result = self.classifier(cleaned_text)[0]
            
            # Convert to standardized format
            if self.model_name == "finbert":
                # FinBERT returns: positive, negative, neutral
                label = result['label'].lower()
                score = result['score']
                
                if label == 'positive':
                    compound_score = score
                    positive, negative, neutral = score, 0.0, 1 - score
                elif label == 'negative':
                    compound_score = -score
                    positive, negative, neutral = 0.0, score, 1 - score
                else:  # neutral
                    compound_score = 0.0
                    positive, negative, neutral = 0.0, 0.0, score
            else:
                # General model format conversion
                compound_score = result['score'] if result['label'] == 'POSITIVE' else -result['score']
                positive = result['score'] if result['label'] == 'POSITIVE' else 0.0
                negative = result['score'] if result['label'] == 'NEGATIVE' else 0.0
                neutral = 1.0 - abs(compound_score)
            
            return SentimentScore(
                compound_score=compound_score,
                positive=positive,
                negative=negative,
                neutral=neutral,
                confidence=result['score'],
                source=self.model_name,
                timestamp=datetime.now(),
                text_snippet=text[:200]
            )
            
        except Exception as e:
            logging.error(f"FinBERT sentiment analysis failed: {e}")
            return self._fallback_sentiment(text)
    
    def _clean_financial_text(self, text: str) -> str:
        """Clean financial text for better analysis."""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Normalize financial terms
        text = re.sub(r'\$([0-9,]+\.?[0-9]*)', r'\1 dollars', text)
        text = re.sub(r'([0-9]+)%', r'\1 percent', text)
        
        return text
    
    def _fallback_sentiment(self, text: str) -> SentimentScore:
        """Fallback sentiment analysis using TextBlob."""
        try:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity  # -1 to 1
            
            return SentimentScore(
                compound_score=polarity,
                positive=max(0, polarity),
                negative=max(0, -polarity),
                neutral=1 - abs(polarity),
                confidence=0.5,  # Lower confidence for fallback
                source="textblob_fallback",
                timestamp=datetime.now(),
                text_snippet=text[:200]
            )
        except Exception as e:
            logging.error(f"Fallback sentiment analysis failed: {e}")
            return SentimentScore(0.0, 0.0, 0.0, 1.0, 0.0, "error", datetime.now(), text[:200])


class VADERSentimentAnalyzer:
    """
    VADER sentiment analyzer optimized for social media and news.
    Complements FinBERT with different strengths.
    """
    
    def __init__(self):
        try:
            nltk.download('vader_lexicon', quiet=True)
            self.analyzer = SentimentIntensityAnalyzer()
            logging.info("Loaded VADER sentiment analyzer")
        except Exception as e:
            logging.error(f"Failed to load VADER: {e}")
            self.analyzer = None
    
    def analyze_sentiment(self, text: str) -> SentimentScore:
        """Analyze sentiment using VADER."""
        if not self.analyzer:
            return SentimentScore(0.0, 0.0, 0.0, 1.0, 0.0, "vader_error", datetime.now(), text[:200])
        
        try:
            scores = self.analyzer.polarity_scores(text)
            
            return SentimentScore(
                compound_score=scores['compound'],
                positive=scores['pos'],
                negative=scores['neg'],
                neutral=scores['neu'],
                confidence=abs(scores['compound']),  # Use compound absolute value as confidence
                source="vader",
                timestamp=datetime.now(),
                text_snippet=text[:200]
            )
        except Exception as e:
            logging.error(f"VADER sentiment analysis failed: {e}")
            return SentimentScore(0.0, 0.0, 0.0, 1.0, 0.0, "vader_error", datetime.now(), text[:200])


class NewsContentFetcher:
    """
    Fetches news content from multiple sources for sentiment analysis.
    """
    
    def __init__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={'User-Agent': 'Mozilla/5.0 (compatible; ATS News Bot)'}
        )
        
        # RSS feeds for financial news
        self.rss_feeds = {
            'reuters_business': 'http://feeds.reuters.com/reuters/businessNews',
            'reuters_markets': 'http://feeds.reuters.com/reuters/usnews',
            'marketwatch': 'http://feeds.marketwatch.com/marketwatch/topstories',
            'cnbc': 'https://www.cnbc.com/id/100003114/device/rss/rss.html',
            'bloomberg': 'https://feeds.bloomberg.com/markets/news.rss',
            'financial_times': 'https://www.ft.com/rss/home',
        }
    
    async def fetch_news_for_symbols(self, symbols: List[str], hours_back: int = 24) -> List[NewsArticle]:
        """Fetch recent news articles for given symbols."""
        all_articles = []
        
        # Fetch from RSS feeds
        rss_articles = await self._fetch_from_rss_feeds(symbols, hours_back)
        all_articles.extend(rss_articles)
        
        # Fetch from Yahoo Finance news
        yf_articles = await self._fetch_yahoo_finance_news(symbols, hours_back)
        all_articles.extend(yf_articles)
        
        # Remove duplicates and sort by relevance
        unique_articles = self._deduplicate_articles(all_articles)
        return sorted(unique_articles, key=lambda x: x.relevance_score, reverse=True)
    
    async def _fetch_from_rss_feeds(self, symbols: List[str], hours_back: int) -> List[NewsArticle]:
        """Fetch articles from RSS feeds."""
        articles = []
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        for source_name, feed_url in self.rss_feeds.items():
            try:
                async with self.session.get(feed_url) as response:
                    if response.status == 200:
                        content = await response.text()
                        feed = feedparser.parse(content)
                        
                        for entry in feed.entries:
                            # Parse publication date
                            try:
                                pub_date = datetime(*entry.published_parsed[:6])
                                if pub_date < cutoff_time:
                                    continue
                            except:
                                pub_date = datetime.now()
                            
                            # Extract content
                            title = entry.get('title', '')
                            summary = entry.get('summary', '')
                            link = entry.get('link', '')
                            
                            # Check if article mentions any of our symbols
                            relevant_symbols = self._extract_symbols_from_text(title + ' ' + summary, symbols)
                            if relevant_symbols:
                                articles.append(NewsArticle(
                                    title=title,
                                    content=summary,
                                    url=link,
                                    source=source_name,
                                    published_date=pub_date,
                                    symbols=relevant_symbols,
                                    sentiment=None,  # Will be filled later
                                    relevance_score=self._calculate_relevance_score(title + ' ' + summary, relevant_symbols),
                                    impact_score=0.0  # Will be calculated later
                                ))
                            
            except Exception as e:
                logging.warning(f"Failed to fetch from {source_name}: {e}")
        
        return articles
    
    async def _fetch_yahoo_finance_news(self, symbols: List[str], hours_back: int) -> List[NewsArticle]:
        """Fetch news from Yahoo Finance for specific symbols."""
        articles = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                news_items = ticker.news
                
                for item in news_items[:10]:  # Limit to recent articles
                    try:
                        # Convert timestamp to datetime
                        pub_date = datetime.fromtimestamp(item.get('providerPublishTime', time.time()))
                        
                        # Check if within time window
                        if pub_date < datetime.now() - timedelta(hours=hours_back):
                            continue
                        
                        articles.append(NewsArticle(
                            title=item.get('title', ''),
                            content=item.get('summary', ''),
                            url=item.get('link', ''),
                            source='yahoo_finance',
                            published_date=pub_date,
                            symbols=[symbol],
                            sentiment=None,
                            relevance_score=1.0,  # High relevance since it's symbol-specific
                            impact_score=0.0
                        ))
                    except Exception as e:
                        logging.warning(f"Failed to process Yahoo Finance news item: {e}")
                        
            except Exception as e:
                logging.warning(f"Failed to fetch Yahoo Finance news for {symbol}: {e}")
        
        return articles
    
    def _extract_symbols_from_text(self, text: str, target_symbols: List[str]) -> List[str]:
        """Extract mentioned symbols from text."""
        text_upper = text.upper()
        found_symbols = []
        
        for symbol in target_symbols:
            # Look for exact symbol matches (with word boundaries)
            pattern = r'\b' + re.escape(symbol.upper()) + r'\b'
            if re.search(pattern, text_upper):
                found_symbols.append(symbol)
        
        return found_symbols
    
    def _calculate_relevance_score(self, text: str, symbols: List[str]) -> float:
        """Calculate how relevant the article is to the symbols."""
        text_lower = text.lower()
        
        # Base score for symbol mention
        score = 0.5 if symbols else 0.0
        
        # Financial keywords that increase relevance
        financial_keywords = [
            'earnings', 'revenue', 'profit', 'loss', 'guidance', 'outlook',
            'merger', 'acquisition', 'ipo', 'stock', 'shares', 'dividend',
            'upgrade', 'downgrade', 'analyst', 'price target', 'recommendation'
        ]
        
        for keyword in financial_keywords:
            if keyword in text_lower:
                score += 0.1
        
        return min(score, 1.0)
    
    def _deduplicate_articles(self, articles: List[NewsArticle]) -> List[NewsArticle]:
        """Remove duplicate articles based on title similarity."""
        if not articles:
            return []
        
        # Create TF-IDF vectors for titles
        titles = [article.title for article in articles]
        vectorizer = TfidfVectorizer(stop_words='english', ngram_range=(1, 2))
        
        try:
            tfidf_matrix = vectorizer.fit_transform(titles)
            similarity_matrix = cosine_similarity(tfidf_matrix)
            
            # Mark duplicates (similarity > 0.8)
            duplicates = set()
            for i in range(len(articles)):
                for j in range(i + 1, len(articles)):
                    if similarity_matrix[i][j] > 0.8:
                        duplicates.add(j)
            
            # Return non-duplicates
            return [article for i, article in enumerate(articles) if i not in duplicates]
        
        except Exception as e:
            logging.warning(f"Deduplication failed: {e}. Returning all articles.")
            return articles
    
    async def close(self):
        """Close the aiohttp session."""
        await self.session.close()


class NewsSentimentAnalyzer:
    """
    Main news sentiment analyzer that combines multiple approaches.
    """
    
    def __init__(self, pool: asyncpg.Pool, env):
        self.pool = pool
        self.env = env
        self.finbert = FinBERTSentimentAnalyzer()
        self.vader = VADERSentimentAnalyzer()
        self.content_fetcher = NewsContentFetcher()
        
        # Sentiment aggregation weights
        self.model_weights = {
            'finbert': 0.6,    # Higher weight for financial-specific model
            'vader': 0.4       # Lower weight but good for social media style
        }
    
    async def analyze_news_sentiment(self, symbols: List[str], hours_back: int = 24) -> Dict[str, SentimentSignal]:
        """Analyze sentiment for given symbols based on recent news."""
        try:
            # Fetch recent news articles
            articles = await self.content_fetcher.fetch_news_for_symbols(symbols, hours_back)
            
            if not articles:
                logging.info(f"No news articles found for symbols: {symbols}")
                return {}
            
            # Analyze sentiment for each article
            analyzed_articles = []
            for article in articles:
                sentiment = await self._analyze_article_sentiment(article)
                article.sentiment = sentiment
                analyzed_articles.append(article)
            
            # Generate trading signals by symbol
            signals = {}
            for symbol in symbols:
                signal = await self._generate_sentiment_signal(symbol, analyzed_articles)
                if signal:
                    signals[symbol] = signal
            
            # Store results in database
            await self._store_sentiment_analysis(analyzed_articles, signals)
            
            return signals
            
        except Exception as e:
            logging.error(f"News sentiment analysis failed: {e}")
            return {}
    
    async def _analyze_article_sentiment(self, article: NewsArticle) -> SentimentScore:
        """Analyze sentiment of a single article using multiple models."""
        full_text = f"{article.title} {article.content}"
        
        # Get sentiment from both models
        finbert_sentiment = self.finbert.analyze_sentiment(full_text)
        vader_sentiment = self.vader.analyze_sentiment(full_text)
        
        # Combine sentiments using weighted average
        combined_compound = (
            finbert_sentiment.compound_score * self.model_weights['finbert'] +
            vader_sentiment.compound_score * self.model_weights['vader']
        )
        
        combined_positive = (
            finbert_sentiment.positive * self.model_weights['finbert'] +
            vader_sentiment.positive * self.model_weights['vader']
        )
        
        combined_negative = (
            finbert_sentiment.negative * self.model_weights['finbert'] +
            vader_sentiment.negative * self.model_weights['vader']
        )
        
        combined_neutral = (
            finbert_sentiment.neutral * self.model_weights['finbert'] +
            vader_sentiment.neutral * self.model_weights['vader']
        )
        
        # Confidence is the minimum of the two models (conservative approach)
        combined_confidence = min(finbert_sentiment.confidence, vader_sentiment.confidence)
        
        return SentimentScore(
            compound_score=combined_compound,
            positive=combined_positive,
            negative=combined_negative,
            neutral=combined_neutral,
            confidence=combined_confidence,
            source="finbert_vader_ensemble",
            timestamp=datetime.now(),
            text_snippet=full_text[:200]
        )
    
    async def _generate_sentiment_signal(self, symbol: str, articles: List[NewsArticle]) -> Optional[SentimentSignal]:
        """Generate trading signal based on sentiment analysis."""
        # Filter articles for this symbol
        symbol_articles = [a for a in articles if symbol in a.symbols]
        
        if not symbol_articles:
            return None
        
        # Calculate aggregated sentiment metrics
        sentiments = [a.sentiment for a in symbol_articles if a.sentiment]
        if not sentiments:
            return None
        
        # Volume-weighted sentiment (more recent articles have higher weight)
        now = datetime.now()
        total_weight = 0
        weighted_sentiment = 0
        
        for article in symbol_articles:
            # Time decay: more recent articles get higher weight
            hours_old = (now - article.published_date).total_seconds() / 3600
            time_weight = np.exp(-hours_old / 24)  # Exponential decay over 24 hours
            
            # Relevance weight
            relevance_weight = article.relevance_score
            
            # Combined weight
            weight = time_weight * relevance_weight * article.sentiment.confidence
            total_weight += weight
            weighted_sentiment += article.sentiment.compound_score * weight
        
        if total_weight == 0:
            return None
        
        volume_weighted_sentiment = weighted_sentiment / total_weight
        
        # Calculate sentiment momentum (change over time)
        sentiment_momentum = self._calculate_sentiment_momentum(symbol_articles)
        
        # Determine signal direction and strength
        if volume_weighted_sentiment > 0.1:
            signal_direction = 'bullish'
            signal_strength = min(volume_weighted_sentiment * 2, 1.0)  # Scale to 0-1
        elif volume_weighted_sentiment < -0.1:
            signal_direction = 'bearish'
            signal_strength = max(volume_weighted_sentiment * 2, -1.0)  # Scale to -1-0
        else:
            signal_direction = 'neutral'
            signal_strength = 0.0
        
        # Calculate overall confidence
        avg_confidence = np.mean([s.confidence for s in sentiments])
        article_count_factor = min(len(symbol_articles) / 5.0, 1.0)  # More articles = higher confidence
        overall_confidence = avg_confidence * article_count_factor
        
        # Determine time horizon based on sentiment strength and momentum
        if abs(signal_strength) > 0.7 and abs(sentiment_momentum) > 0.3:
            time_horizon = 'short'  # Strong, momentum-driven signals
        elif abs(signal_strength) > 0.4:
            time_horizon = 'medium'  # Moderate signals
        else:
            time_horizon = 'long'   # Weak signals
        
        return SentimentSignal(
            symbol=symbol,
            signal_strength=signal_strength,
            signal_direction=signal_direction,
            confidence=overall_confidence,
            time_horizon=time_horizon,
            supporting_articles=symbol_articles,
            sentiment_momentum=sentiment_momentum,
            volume_weighted_sentiment=volume_weighted_sentiment,
            timestamp=datetime.now()
        )
    
    def _calculate_sentiment_momentum(self, articles: List[NewsArticle]) -> float:
        """Calculate the momentum of sentiment change over time."""
        if len(articles) < 2:
            return 0.0
        
        # Sort articles by publication date
        sorted_articles = sorted(articles, key=lambda x: x.published_date)
        
        # Split into earlier and later halves
        midpoint = len(sorted_articles) // 2
        earlier_articles = sorted_articles[:midpoint]
        later_articles = sorted_articles[midpoint:]
        
        # Calculate average sentiment for each half
        earlier_sentiment = np.mean([a.sentiment.compound_score for a in earlier_articles])
        later_sentiment = np.mean([a.sentiment.compound_score for a in later_articles])
        
        # Momentum is the change in sentiment
        return later_sentiment - earlier_sentiment
    
    async def _store_sentiment_analysis(self, articles: List[NewsArticle], signals: Dict[str, SentimentSignal]):
        """Store sentiment analysis results in database."""
        try:
            async with self.pool.acquire() as conn:
                # Store articles
                for article in articles:
                    if article.sentiment:
                        await conn.execute("""
                            INSERT INTO news_sentiment_analysis 
                            (title, url, source, published_date, symbols, 
                             sentiment_score, confidence, relevance_score, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (url) DO UPDATE SET
                                sentiment_score = EXCLUDED.sentiment_score,
                                confidence = EXCLUDED.confidence,
                                updated_at = CURRENT_TIMESTAMP
                        """, 
                        article.title, article.url, article.source, article.published_date,
                        article.symbols, article.sentiment.compound_score, 
                        article.sentiment.confidence, article.relevance_score, datetime.now()
                        )
                
                # Store signals
                for symbol, signal in signals.items():
                    await conn.execute("""
                        INSERT INTO sentiment_signals
                        (symbol, signal_strength, signal_direction, confidence, 
                         time_horizon, sentiment_momentum, volume_weighted_sentiment,
                         article_count, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    signal.symbol, signal.signal_strength, signal.signal_direction,
                    signal.confidence, signal.time_horizon, signal.sentiment_momentum,
                    signal.volume_weighted_sentiment, len(signal.supporting_articles),
                    signal.timestamp
                    )
                    
        except Exception as e:
            logging.error(f"Failed to store sentiment analysis: {e}")
    
    async def get_sentiment_history(self, symbol: str, days_back: int = 30) -> pd.DataFrame:
        """Get historical sentiment data for a symbol."""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetch("""
                    SELECT 
                        DATE(created_at) as date,
                        AVG(signal_strength) as avg_signal_strength,
                        AVG(confidence) as avg_confidence,
                        AVG(sentiment_momentum) as avg_momentum,
                        COUNT(*) as signal_count
                    FROM sentiment_signals 
                    WHERE symbol = $1 
                        AND created_at >= $2
                    GROUP BY DATE(created_at)
                    ORDER BY date DESC
                """, symbol, datetime.now() - timedelta(days=days_back))
                
                return pd.DataFrame([dict(row) for row in result])
                
        except Exception as e:
            logging.error(f"Failed to get sentiment history: {e}")
            return pd.DataFrame()
    
    async def close(self):
        """Close resources."""
        await self.content_fetcher.close()


# Convenience function for external use
async def analyze_symbol_sentiment(pool: asyncpg.Pool, env, symbols: List[str], hours_back: int = 24) -> Dict[str, SentimentSignal]:
    """
    Convenience function to analyze sentiment for given symbols.
    
    Args:
        pool: Database connection pool
        env: Environment configuration
        symbols: List of stock symbols to analyze
        hours_back: How many hours back to look for news
        
    Returns:
        Dict mapping symbols to sentiment signals
    """
    analyzer = NewsSentimentAnalyzer(pool, env)
    try:
        return await analyzer.analyze_news_sentiment(symbols, hours_back)
    finally:
        await analyzer.close()