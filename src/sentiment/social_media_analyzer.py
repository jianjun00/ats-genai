"""
Social Media Sentiment Analysis for Trading Signals

Integrates Twitter sentiment analysis and social media monitoring for financial markets.
Based on MathTypes ATS social intelligence framework with enhanced features.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re
from collections import Counter

import asyncpg
import pandas as pd
import numpy as np
from transformers import pipeline
import nltk
from nltk.corpus import stopwords


@dataclass
class SocialMediaPost:
    """Social media post with metadata."""
    post_id: str
    platform: str  # 'twitter', 'reddit', 'stocktwits', etc.
    content: str
    author: str
    followers_count: int
    retweets_count: int
    likes_count: int
    timestamp: datetime
    symbols: List[str]
    hashtags: List[str]
    mentions: List[str]
    engagement_score: float  # Normalized engagement metric
    author_influence_score: float  # Influence of the author
    

@dataclass
class SocialSentimentMetrics:
    """Social sentiment metrics for a symbol."""
    symbol: str
    timestamp: datetime
    total_posts: int
    total_engagement: int
    average_sentiment: float  # -1 to 1
    sentiment_std: float
    bullish_ratio: float  # Ratio of bullish posts
    bearish_ratio: float  # Ratio of bearish posts
    neutral_ratio: float
    trending_score: float  # How trending the symbol is
    influencer_sentiment: float  # Sentiment from high-influence users
    retail_sentiment: float  # Sentiment from regular users
    momentum_score: float  # Sentiment momentum over time
    top_hashtags: List[str]
    top_keywords: List[str]


@dataclass
class SocialTradingSignal:
    """Trading signal derived from social media sentiment."""
    symbol: str
    signal_type: str  # 'momentum', 'contrarian', 'trending', 'influencer'
    signal_strength: float  # -1 to 1
    confidence: float  # 0 to 1
    time_horizon: str  # 'intraday', 'short', 'medium'
    supporting_posts: List[SocialMediaPost]
    key_metrics: SocialSentimentMetrics
    risk_factors: List[str]  # Potential risks in the signal
    timestamp: datetime


class CryptoTwitterAnalyzer:
    """
    Twitter-like sentiment analyzer for cryptocurrency and stock discussions.
    Simulates advanced social media analysis without requiring API access.
    """
    
    def __init__(self):
        # Initialize sentiment analyzer
        try:
            self.sentiment_analyzer = pipeline("sentiment-analysis", 
                                             model="nlptown/bert-base-multilingual-uncased-sentiment")
            logging.info("Loaded social media sentiment analyzer")
        except Exception as e:
            logging.error(f"Failed to load sentiment analyzer: {e}")
            self.sentiment_analyzer = None
        
        # Download required NLTK data
        try:
            nltk.download('stopwords', quiet=True)
            nltk.download('punkt', quiet=True)
            self.stop_words = set(stopwords.words('english'))
        except:
            self.stop_words = set()
        
        # Financial slang and sentiment indicators
        self.bullish_indicators = {
            'moon', 'rocket', 'bulls', 'bullish', 'pump', 'long', 'buy', 'hodl',
            'diamond hands', 'to the moon', 'breakout', 'rally', 'surge', 'gains',
            'profit', 'strong', 'support', 'resistance', 'uptick', 'green'
        }
        
        self.bearish_indicators = {
            'bears', 'bearish', 'dump', 'crash', 'short', 'sell', 'paper hands',
            'correction', 'dip', 'fall', 'drop', 'loss', 'weak', 'red', 'decline',
            'breakdown', 'resistance', 'support broken', 'bear market'
        }
        
        # Cashtag pattern for stock symbols
        self.cashtag_pattern = re.compile(r'\$([A-Z]{1,5})\b')
        self.hashtag_pattern = re.compile(r'#(\w+)')
        self.mention_pattern = re.compile(r'@(\w+)')
    
    def extract_financial_entities(self, text: str) -> Tuple[List[str], List[str], List[str]]:
        """Extract symbols, hashtags, and mentions from social media text."""
        symbols = self.cashtag_pattern.findall(text.upper())
        hashtags = self.hashtag_pattern.findall(text.lower())
        mentions = self.mention_pattern.findall(text.lower())
        
        return symbols, hashtags, mentions
    
    def calculate_financial_sentiment(self, text: str) -> float:
        """Calculate sentiment with financial context."""
        text_lower = text.lower()
        
        # Count bullish/bearish indicators
        bullish_count = sum(1 for indicator in self.bullish_indicators if indicator in text_lower)
        bearish_count = sum(1 for indicator in self.bearish_indicators if indicator in text_lower)
        
        # Financial sentiment score
        if bullish_count > bearish_count:
            financial_sentiment = min(0.8, bullish_count * 0.2)
        elif bearish_count > bullish_count:
            financial_sentiment = max(-0.8, -bearish_count * 0.2)
        else:
            financial_sentiment = 0.0
        
        # Use model sentiment as base
        model_sentiment = 0.0
        if self.sentiment_analyzer:
            try:
                result = self.sentiment_analyzer(text[:512])  # Truncate for model
                score = result[0]['score']
                if result[0]['label'] == 'POSITIVE':
                    model_sentiment = score
                else:
                    model_sentiment = -score
            except:
                pass
        
        # Combine financial and model sentiment
        combined_sentiment = 0.6 * financial_sentiment + 0.4 * model_sentiment
        return np.clip(combined_sentiment, -1.0, 1.0)
    
    def calculate_engagement_score(self, post: SocialMediaPost) -> float:
        """Calculate normalized engagement score."""
        if post.followers_count == 0:
            return 0.0
        
        # Engagement rate
        engagement_rate = (post.likes_count + post.retweets_count) / max(post.followers_count, 1)
        
        # Normalize to 0-1 scale (log transform for better distribution)
        normalized_score = min(1.0, np.log10(max(1, engagement_rate * 10000)) / 4)
        
        return normalized_score
    
    def calculate_author_influence(self, post: SocialMediaPost) -> float:
        """Calculate author influence score."""
        # Simple influence based on followers (in real implementation, use more factors)
        if post.followers_count < 100:
            return 0.1  # Low influence
        elif post.followers_count < 1000:
            return 0.3  # Medium influence
        elif post.followers_count < 10000:
            return 0.6  # High influence
        else:
            return 1.0  # Very high influence


class SocialMediaDataGenerator:
    """
    Generates realistic social media data for testing and simulation.
    In production, this would be replaced with real API integrations.
    """
    
    def __init__(self):
        self.crypto_analyzer = CryptoTwitterAnalyzer()
        
        # Sample usernames and realistic followers
        self.sample_users = [
            ('crypto_king', 50000), ('diamond_trader', 25000), ('moonshot_master', 15000),
            ('bear_hunter', 30000), ('hodl_strong', 8000), ('day_trader_pro', 12000),
            ('market_wizard', 45000), ('bulls_eye', 7000), ('stock_sensei', 20000),
            ('crypto_whale', 100000), ('retail_trader', 2000), ('finance_guru', 35000)
        ]
        
        # Sample post templates for different sentiment types
        self.bullish_templates = [
            "${symbol} breaking resistance! 🚀 Time to buy more!",
            "Big moves coming for ${symbol}. Diamond hands! 💎🙌",
            "${symbol} looking strong. Bullish setup forming!",
            "Loading up on ${symbol}. This is going to moon! 🌙",
            "${symbol} chart looking beautiful. Breakout incoming!",
            "Why I'm buying more ${symbol}: fundamentals + technicals = 🚀"
        ]
        
        self.bearish_templates = [
            "${symbol} looking weak. Time to take profits.",
            "Concerned about ${symbol} recent price action. Might be topping.",
            "${symbol} breaking support. Bear market vibes 🐻",
            "Selling my ${symbol} position. Risk/reward not good anymore.",
            "${symbol} chart looking ugly. Correction incoming?",
            "Red flags everywhere for ${symbol}. Be careful!"
        ]
        
        self.neutral_templates = [
            "${symbol} sideways action. Waiting for clear direction.",
            "What's everyone's thoughts on ${symbol}?",
            "${symbol} earnings coming up. Could go either way.",
            "Watching ${symbol} closely. Key levels to watch.",
            "${symbol} consolidating. Need to see volume increase.",
            "Mixed signals on ${symbol}. Staying patient."
        ]
    
    def generate_social_media_posts(self, symbols: List[str], hours_back: int = 24, posts_per_hour: int = 10) -> List[SocialMediaPost]:
        """Generate realistic social media posts for given symbols."""
        posts = []
        
        for hour_offset in range(hours_back):
            timestamp = datetime.now() - timedelta(hours=hour_offset)
            
            for _ in range(posts_per_hour):
                symbol = np.random.choice(symbols)
                user, followers = np.random.choice(self.sample_users)
                
                # Determine sentiment bias
                sentiment_type = np.random.choice(['bullish', 'bearish', 'neutral'], p=[0.4, 0.3, 0.3])
                
                if sentiment_type == 'bullish':
                    template = np.random.choice(self.bullish_templates)
                elif sentiment_type == 'bearish':
                    template = np.random.choice(self.bearish_templates)
                else:
                    template = np.random.choice(self.neutral_templates)
                
                # Generate post content
                content = template.replace('${symbol}', symbol)
                
                # Add random hashtags
                hashtags = ['trading', 'stocks', 'crypto', 'investing', 'finance']
                content += f" #{np.random.choice(hashtags)}"
                
                # Generate engagement metrics
                base_engagement = followers * 0.01  # 1% engagement rate
                likes = int(np.random.poisson(base_engagement))
                retweets = int(np.random.poisson(base_engagement * 0.2))
                
                # Extract entities
                symbols_found, hashtags_found, mentions = self.crypto_analyzer.extract_financial_entities(content)
                
                post = SocialMediaPost(
                    post_id=f"post_{int(timestamp.timestamp())}_{np.random.randint(1000, 9999)}",
                    platform='twitter',
                    content=content,
                    author=user,
                    followers_count=followers,
                    retweets_count=retweets,
                    likes_count=likes,
                    timestamp=timestamp,
                    symbols=symbols_found,
                    hashtags=hashtags_found,
                    mentions=mentions,
                    engagement_score=0.0,  # Will be calculated
                    author_influence_score=0.0  # Will be calculated
                )
                
                # Calculate scores
                post.engagement_score = self.crypto_analyzer.calculate_engagement_score(post)
                post.author_influence_score = self.crypto_analyzer.calculate_author_influence(post)
                
                posts.append(post)
        
        return posts


class SocialSentimentAnalyzer:
    """
    Main social sentiment analyzer that processes social media data.
    """
    
    def __init__(self, pool: asyncpg.Pool, env):
        self.pool = pool
        self.env = env
        self.crypto_analyzer = CryptoTwitterAnalyzer()
        self.data_generator = SocialMediaDataGenerator()
    
    async def analyze_social_sentiment(self, symbols: List[str], hours_back: int = 24) -> Dict[str, SocialTradingSignal]:
        """Analyze social sentiment for given symbols."""
        try:
            # Generate social media posts (in production, fetch from APIs)
            posts = self.data_generator.generate_social_media_posts(symbols, hours_back)
            
            if not posts:
                logging.info(f"No social media posts found for symbols: {symbols}")
                return {}
            
            # Analyze sentiment for each post
            analyzed_posts = []
            for post in posts:
                sentiment_score = self.crypto_analyzer.calculate_financial_sentiment(post.content)
                analyzed_posts.append((post, sentiment_score))
            
            # Generate metrics and signals by symbol
            signals = {}
            for symbol in symbols:
                metrics = self._calculate_sentiment_metrics(symbol, analyzed_posts)
                signal = self._generate_social_trading_signal(symbol, metrics, analyzed_posts)
                
                if signal:
                    signals[symbol] = signal
            
            # Store results
            await self._store_social_analysis(analyzed_posts, signals)
            
            return signals
            
        except Exception as e:
            logging.error(f"Social sentiment analysis failed: {e}")
            return {}
    
    def _calculate_sentiment_metrics(self, symbol: str, analyzed_posts: List[Tuple[SocialMediaPost, float]]) -> SocialSentimentMetrics:
        """Calculate comprehensive sentiment metrics for a symbol."""
        # Filter posts for this symbol
        symbol_posts = [(post, sentiment) for post, sentiment in analyzed_posts 
                       if symbol in post.symbols]
        
        if not symbol_posts:
            return SocialSentimentMetrics(
                symbol=symbol,
                timestamp=datetime.now(),
                total_posts=0,
                total_engagement=0,
                average_sentiment=0.0,
                sentiment_std=0.0,
                bullish_ratio=0.0,
                bearish_ratio=0.0,
                neutral_ratio=0.0,
                trending_score=0.0,
                influencer_sentiment=0.0,
                retail_sentiment=0.0,
                momentum_score=0.0,
                top_hashtags=[],
                top_keywords=[]
            )
        
        posts = [post for post, _ in symbol_posts]
        sentiments = [sentiment for _, sentiment in symbol_posts]
        
        # Basic metrics
        total_posts = len(posts)
        total_engagement = sum(post.likes_count + post.retweets_count for post in posts)
        average_sentiment = np.mean(sentiments)
        sentiment_std = np.std(sentiments)
        
        # Sentiment distribution
        bullish_count = sum(1 for s in sentiments if s > 0.1)
        bearish_count = sum(1 for s in sentiments if s < -0.1)
        neutral_count = total_posts - bullish_count - bearish_count
        
        bullish_ratio = bullish_count / total_posts
        bearish_ratio = bearish_count / total_posts
        neutral_ratio = neutral_count / total_posts
        
        # Trending score (based on volume and engagement)
        recent_posts = [p for p in posts if (datetime.now() - p.timestamp).hours < 6]
        trending_score = len(recent_posts) / max(1, total_posts) * np.mean([p.engagement_score for p in posts])
        
        # Influencer vs retail sentiment
        influencer_posts = [(post, sentiment) for post, sentiment in symbol_posts 
                           if post.author_influence_score > 0.5]
        retail_posts = [(post, sentiment) for post, sentiment in symbol_posts 
                       if post.author_influence_score <= 0.5]
        
        influencer_sentiment = np.mean([s for _, s in influencer_posts]) if influencer_posts else 0.0
        retail_sentiment = np.mean([s for _, s in retail_posts]) if retail_posts else 0.0
        
        # Momentum score (sentiment change over time)
        momentum_score = self._calculate_sentiment_momentum(symbol_posts)
        
        # Top hashtags and keywords
        all_hashtags = []
        all_content = []
        for post in posts:
            all_hashtags.extend(post.hashtags)
            all_content.append(post.content.lower())
        
        hashtag_counts = Counter(all_hashtags)
        top_hashtags = [tag for tag, _ in hashtag_counts.most_common(5)]
        
        # Extract keywords using simple frequency analysis
        all_words = []
        for content in all_content:
            words = [word for word in content.split() 
                    if len(word) > 3 and word not in self.crypto_analyzer.stop_words]
            all_words.extend(words)
        
        word_counts = Counter(all_words)
        top_keywords = [word for word, _ in word_counts.most_common(5)]
        
        return SocialSentimentMetrics(
            symbol=symbol,
            timestamp=datetime.now(),
            total_posts=total_posts,
            total_engagement=total_engagement,
            average_sentiment=average_sentiment,
            sentiment_std=sentiment_std,
            bullish_ratio=bullish_ratio,
            bearish_ratio=bearish_ratio,
            neutral_ratio=neutral_ratio,
            trending_score=trending_score,
            influencer_sentiment=influencer_sentiment,
            retail_sentiment=retail_sentiment,
            momentum_score=momentum_score,
            top_hashtags=top_hashtags,
            top_keywords=top_keywords
        )
    
    def _calculate_sentiment_momentum(self, symbol_posts: List[Tuple[SocialMediaPost, float]]) -> float:
        """Calculate sentiment momentum over time."""
        if len(symbol_posts) < 4:
            return 0.0
        
        # Sort by timestamp
        sorted_posts = sorted(symbol_posts, key=lambda x: x[0].timestamp)
        
        # Split into quarters
        quarter_size = len(sorted_posts) // 4
        quarters = [
            sorted_posts[i*quarter_size:(i+1)*quarter_size] 
            for i in range(4)
        ]
        
        # Calculate average sentiment for each quarter
        quarter_sentiments = []
        for quarter in quarters:
            if quarter:
                avg_sentiment = np.mean([sentiment for _, sentiment in quarter])
                quarter_sentiments.append(avg_sentiment)
        
        if len(quarter_sentiments) < 2:
            return 0.0
        
        # Linear regression to find trend
        x = np.arange(len(quarter_sentiments))
        y = np.array(quarter_sentiments)
        
        if len(x) > 1:
            slope = np.polyfit(x, y, 1)[0]
            return np.clip(slope * 4, -1.0, 1.0)  # Scale and clip
        
        return 0.0
    
    def _generate_social_trading_signal(self, symbol: str, metrics: SocialSentimentMetrics, 
                                      analyzed_posts: List[Tuple[SocialMediaPost, float]]) -> Optional[SocialTradingSignal]:
        """Generate trading signal from social sentiment metrics."""
        if metrics.total_posts < 5:  # Need minimum posts for reliable signal
            return None
        
        # Filter posts for this symbol
        symbol_posts = [(post, sentiment) for post, sentiment in analyzed_posts 
                       if symbol in post.symbols]
        
        risk_factors = []
        
        # Determine signal type and strength
        signal_type = 'momentum'  # Default
        signal_strength = 0.0
        confidence = 0.0
        
        # Momentum-based signal
        if abs(metrics.momentum_score) > 0.2:
            signal_type = 'momentum'
            signal_strength = metrics.momentum_score
            confidence = min(0.8, metrics.total_posts / 20.0)  # More posts = higher confidence
            
            if metrics.sentiment_std > 0.6:
                risk_factors.append("High sentiment volatility")
                confidence *= 0.8
        
        # Trending signal
        elif metrics.trending_score > 0.5:
            signal_type = 'trending'
            signal_strength = metrics.average_sentiment * 0.8
            confidence = metrics.trending_score
            
            if metrics.total_posts < 10:
                risk_factors.append("Low post volume for trending signal")
                confidence *= 0.7
        
        # Influencer signal
        elif abs(metrics.influencer_sentiment) > 0.3 and metrics.influencer_sentiment != 0:
            signal_type = 'influencer'
            signal_strength = metrics.influencer_sentiment * 0.6
            confidence = 0.7
            
            # Check for divergence between influencer and retail sentiment
            if abs(metrics.influencer_sentiment - metrics.retail_sentiment) > 0.4:
                risk_factors.append("Divergence between influencer and retail sentiment")
                confidence *= 0.6
        
        # Contrarian signal (high sentiment but decreasing momentum)
        elif abs(metrics.average_sentiment) > 0.5 and metrics.momentum_score * metrics.average_sentiment < -0.1:
            signal_type = 'contrarian'
            signal_strength = -metrics.average_sentiment * 0.5  # Contrarian
            confidence = 0.6
            risk_factors.append("Contrarian signal - high risk")
        
        else:
            # No clear signal
            return None
        
        # Determine time horizon
        if metrics.trending_score > 0.7:
            time_horizon = 'intraday'
        elif abs(signal_strength) > 0.5:
            time_horizon = 'short'
        else:
            time_horizon = 'medium'
        
        # Apply risk adjustments
        if metrics.sentiment_std > 0.8:
            risk_factors.append("Very high sentiment volatility")
            confidence *= 0.6
        
        if metrics.total_posts < 10:
            risk_factors.append("Low sample size")
            confidence *= 0.8
        
        # Minimum confidence threshold
        if confidence < 0.3:
            return None
        
        return SocialTradingSignal(
            symbol=symbol,
            signal_type=signal_type,
            signal_strength=signal_strength,
            confidence=confidence,
            time_horizon=time_horizon,
            supporting_posts=[post for post, _ in symbol_posts],
            key_metrics=metrics,
            risk_factors=risk_factors,
            timestamp=datetime.now()
        )
    
    async def _store_social_analysis(self, analyzed_posts: List[Tuple[SocialMediaPost, float]], 
                                   signals: Dict[str, SocialTradingSignal]):
        """Store social sentiment analysis results."""
        try:
            async with self.pool.acquire() as conn:
                # Store posts
                for post, sentiment in analyzed_posts:
                    await conn.execute("""
                        INSERT INTO social_media_posts 
                        (post_id, platform, content, author, followers_count, 
                         likes_count, retweets_count, timestamp, symbols, 
                         hashtags, sentiment_score, engagement_score, 
                         author_influence_score, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        ON CONFLICT (post_id) DO NOTHING
                    """, 
                    post.post_id, post.platform, post.content, post.author,
                    post.followers_count, post.likes_count, post.retweets_count,
                    post.timestamp, post.symbols, post.hashtags, sentiment,
                    post.engagement_score, post.author_influence_score, datetime.now()
                    )
                
                # Store signals
                for symbol, signal in signals.items():
                    await conn.execute("""
                        INSERT INTO social_trading_signals
                        (symbol, signal_type, signal_strength, confidence, 
                         time_horizon, total_posts, average_sentiment,
                         bullish_ratio, bearish_ratio, trending_score,
                         momentum_score, risk_factors, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    """,
                    signal.symbol, signal.signal_type, signal.signal_strength,
                    signal.confidence, signal.time_horizon, signal.key_metrics.total_posts,
                    signal.key_metrics.average_sentiment, signal.key_metrics.bullish_ratio,
                    signal.key_metrics.bearish_ratio, signal.key_metrics.trending_score,
                    signal.key_metrics.momentum_score, signal.risk_factors, signal.timestamp
                    )
                    
        except Exception as e:
            logging.error(f"Failed to store social analysis: {e}")
    
    async def get_social_sentiment_history(self, symbol: str, days_back: int = 7) -> pd.DataFrame:
        """Get historical social sentiment data."""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetch("""
                    SELECT 
                        DATE(created_at) as date,
                        AVG(signal_strength) as avg_signal_strength,
                        AVG(confidence) as avg_confidence,
                        AVG(average_sentiment) as avg_sentiment,
                        AVG(trending_score) as avg_trending_score,
                        SUM(total_posts) as total_posts
                    FROM social_trading_signals 
                    WHERE symbol = $1 
                        AND created_at >= $2
                    GROUP BY DATE(created_at)
                    ORDER BY date DESC
                """, symbol, datetime.now() - timedelta(days=days_back))
                
                return pd.DataFrame([dict(row) for row in result])
                
        except Exception as e:
            logging.error(f"Failed to get social sentiment history: {e}")
            return pd.DataFrame()


# Convenience function
async def analyze_social_media_sentiment(pool: asyncpg.Pool, env, symbols: List[str], 
                                       hours_back: int = 24) -> Dict[str, SocialTradingSignal]:
    """
    Convenience function to analyze social media sentiment for given symbols.
    
    Args:
        pool: Database connection pool
        env: Environment configuration  
        symbols: List of stock symbols to analyze
        hours_back: How many hours back to analyze
        
    Returns:
        Dict mapping symbols to social trading signals
    """
    analyzer = SocialSentimentAnalyzer(pool, env)
    return await analyzer.analyze_social_sentiment(symbols, hours_back)