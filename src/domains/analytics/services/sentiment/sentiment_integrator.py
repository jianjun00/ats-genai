"""
Sentiment Integration Framework

Combines news sentiment and social media sentiment into unified trading signals.
Integrates with existing residual return prediction and portfolio evaluation systems.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json

import asyncpg
import pandas as pd
import numpy as np

from .news_sentiment_analyzer import NewsSentimentAnalyzer, SentimentSignal
from .social_media_analyzer import SocialSentimentAnalyzer, SocialTradingSignal
from domains.ml.modeling.features.factor_models import ResidualReturnCalculator


@dataclass
class UnifiedSentimentSignal:
    """Unified sentiment signal combining news and social media analysis."""
    symbol: str
    timestamp: datetime

    # Combined scores
    overall_sentiment_score: float  # -1 to 1
    overall_confidence: float       # 0 to 1
    signal_strength: float          # -1 to 1, directional signal
    signal_direction: str           # 'bullish', 'bearish', 'neutral'

    # Component signals
    news_sentiment: Optional[SentimentSignal]
    social_sentiment: Optional[SocialTradingSignal]

    # Sentiment features for ML models
    sentiment_features: Dict[str, float]

    # Risk and timing
    time_horizon: str               # 'short', 'medium', 'long'
    risk_score: float              # 0 to 1, higher = riskier
    volume_indicator: float        # News/social volume relative to baseline

    # Consensus and divergence
    consensus_score: float         # Agreement between news and social
    divergence_score: float        # Disagreement between sources

    # Supporting data
    total_news_articles: int
    total_social_posts: int
    key_themes: List[str]
    risk_factors: List[str]


@dataclass
class SentimentBasedPrediction:
    """Prediction incorporating sentiment signals."""
    symbol: str
    prediction_date: datetime
    horizon_days: int

    # Base prediction (without sentiment)
    base_residual_return: float
    base_confidence: float

    # Sentiment-enhanced prediction
    sentiment_adjusted_return: float
    sentiment_confidence_boost: float
    final_confidence: float

    # Sentiment contribution analysis
    news_contribution: float        # How much news sentiment affected prediction
    social_contribution: float      # How much social sentiment affected prediction
    sentiment_risk_adjustment: float # Risk adjustment from sentiment

    # Supporting signals
    unified_sentiment: UnifiedSentimentSignal

    # Explanation
    prediction_explanation: str


class SentimentFeatureExtractor:
    """
    Extracts features from sentiment signals for ML models.
    """

    def __init__(self):
        self.feature_names = [
            # News sentiment features
            'news_sentiment_score', 'news_confidence', 'news_momentum',
            'news_article_count', 'news_relevance_avg', 'news_volume_weighted_sentiment',

            # Social sentiment features
            'social_sentiment_score', 'social_confidence', 'social_momentum',
            'social_post_count', 'social_engagement_total', 'social_trending_score',
            'social_influencer_sentiment', 'social_retail_sentiment',
            'social_bullish_ratio', 'social_bearish_ratio',

            # Cross-signal features
            'sentiment_consensus', 'sentiment_divergence', 'total_volume_indicator',
            'combined_momentum', 'risk_adjusted_sentiment', 'time_decay_factor',

            # Derived features
            'sentiment_volatility', 'sentiment_surprise', 'sentiment_persistence'
        ]

    def extract_features(self, unified_signal: UnifiedSentimentSignal,
                        historical_signals: List[UnifiedSentimentSignal]) -> Dict[str, float]:
        """Extract feature vector from unified sentiment signal."""
        features = {}

        # News features
        if unified_signal.news_sentiment:
            news = unified_signal.news_sentiment
            features.update({
                'news_sentiment_score': news.volume_weighted_sentiment,
                'news_confidence': news.confidence,
                'news_momentum': news.sentiment_momentum,
                'news_article_count': len(news.supporting_articles),
                'news_relevance_avg': np.mean([a.relevance_score for a in news.supporting_articles]) if news.supporting_articles else 0.0,
                'news_volume_weighted_sentiment': news.volume_weighted_sentiment
            })
        else:
            features.update({
                'news_sentiment_score': 0.0, 'news_confidence': 0.0, 'news_momentum': 0.0,
                'news_article_count': 0.0, 'news_relevance_avg': 0.0, 'news_volume_weighted_sentiment': 0.0
            })

        # Social features
        if unified_signal.social_sentiment:
            social = unified_signal.social_sentiment
            metrics = social.key_metrics
            features.update({
                'social_sentiment_score': metrics.average_sentiment,
                'social_confidence': social.confidence,
                'social_momentum': metrics.momentum_score,
                'social_post_count': metrics.total_posts,
                'social_engagement_total': metrics.total_engagement,
                'social_trending_score': metrics.trending_score,
                'social_influencer_sentiment': metrics.influencer_sentiment,
                'social_retail_sentiment': metrics.retail_sentiment,
                'social_bullish_ratio': metrics.bullish_ratio,
                'social_bearish_ratio': metrics.bearish_ratio
            })
        else:
            features.update({
                'social_sentiment_score': 0.0, 'social_confidence': 0.0, 'social_momentum': 0.0,
                'social_post_count': 0.0, 'social_engagement_total': 0.0, 'social_trending_score': 0.0,
                'social_influencer_sentiment': 0.0, 'social_retail_sentiment': 0.0,
                'social_bullish_ratio': 0.0, 'social_bearish_ratio': 0.0
            })

        # Cross-signal features
        features.update({
            'sentiment_consensus': unified_signal.consensus_score,
            'sentiment_divergence': unified_signal.divergence_score,
            'total_volume_indicator': unified_signal.volume_indicator,
            'combined_momentum': (features['news_momentum'] + features['social_momentum']) / 2,
            'risk_adjusted_sentiment': unified_signal.overall_sentiment_score * (1 - unified_signal.risk_score),
            'time_decay_factor': self._calculate_time_decay(unified_signal.timestamp)
        })

        # Derived features from historical data
        if historical_signals:
            features.update(self._calculate_derived_features(unified_signal, historical_signals))
        else:
            features.update({
                'sentiment_volatility': 0.0,
                'sentiment_surprise': 0.0,
                'sentiment_persistence': 0.0
            })

        # Ensure all features are present
        for feature_name in self.feature_names:
            if feature_name not in features:
                features[feature_name] = 0.0

        return features

    def _calculate_time_decay(self, timestamp: datetime) -> float:
        """Calculate time decay factor (1.0 = recent, 0.0 = old)."""
        hours_old = (datetime.now() - timestamp).total_seconds() / 3600
        return np.exp(-hours_old / 24)  # Exponential decay over 24 hours

    def _calculate_derived_features(self, current_signal: UnifiedSentimentSignal,
                                  historical_signals: List[UnifiedSentimentSignal]) -> Dict[str, float]:
        """Calculate derived features from historical context."""
        if len(historical_signals) < 2:
            return {'sentiment_volatility': 0.0, 'sentiment_surprise': 0.0, 'sentiment_persistence': 0.0}

        # Get historical sentiment scores
        historical_scores = [s.overall_sentiment_score for s in historical_signals[-10:]]  # Last 10

        # Sentiment volatility
        sentiment_volatility = np.std(historical_scores) if len(historical_scores) > 1 else 0.0

        # Sentiment surprise (current vs historical average)
        historical_avg = np.mean(historical_scores)
        sentiment_surprise = abs(current_signal.overall_sentiment_score - historical_avg)

        # Sentiment persistence (how consistent recent sentiment has been)
        if len(historical_scores) >= 3:
            recent_trend = np.polyfit(range(len(historical_scores)), historical_scores, 1)[0]
            sentiment_persistence = abs(recent_trend)
        else:
            sentiment_persistence = 0.0

        return {
            'sentiment_volatility': sentiment_volatility,
            'sentiment_surprise': sentiment_surprise,
            'sentiment_persistence': sentiment_persistence
        }


class SentimentIntegrator:
    """
    Main sentiment integration system that combines news and social sentiment
    with existing residual return prediction framework.
    """

    def __init__(self, pool: asyncpg.Pool, env):
        self.pool = pool
        self.env = env
        self.news_analyzer = NewsSentimentAnalyzer(pool, env)
        self.social_analyzer = SocialSentimentAnalyzer(pool, env)
        self.feature_extractor = SentimentFeatureExtractor()
        self.residual_calculator = ResidualReturnCalculator(pool, env)

        # Sentiment combination weights
        self.sentiment_weights = {
            'news': 0.6,        # News generally more reliable
            'social': 0.4       # Social media more noisy but captures momentum
        }

        # Risk factors that increase caution
        self.high_risk_keywords = {
            'earnings', 'bankruptcy', 'investigation', 'lawsuit', 'scandal',
            'manipulation', 'pump', 'dump', 'short squeeze', 'gamma squeeze'
        }

    async def generate_unified_sentiment_signals(self, symbols: List[str],
                                               hours_back: int = 24) -> Dict[str, UnifiedSentimentSignal]:
        """Generate unified sentiment signals combining news and social media."""
        try:
            # Get sentiment signals from both sources
            news_signals = await self.news_analyzer.analyze_news_sentiment(symbols, hours_back)
            social_signals = await self.social_analyzer.analyze_social_sentiment(symbols, hours_back)

            unified_signals = {}

            for symbol in symbols:
                news_signal = news_signals.get(symbol)
                social_signal = social_signals.get(symbol)

                # Skip if no signals from either source
                if not news_signal and not social_signal:
                    continue

                unified_signal = await self._combine_sentiment_signals(
                    symbol, news_signal, social_signal
                )

                if unified_signal:
                    unified_signals[symbol] = unified_signal

            # Store unified signals
            await self._store_unified_signals(unified_signals)

            return unified_signals

        except Exception as e:
            logging.error(f"Unified sentiment generation failed: {e}")
            return {}

    async def _combine_sentiment_signals(self, symbol: str,
                                       news_signal: Optional[SentimentSignal],
                                       social_signal: Optional[SocialTradingSignal]) -> Optional[UnifiedSentimentSignal]:
        """Combine news and social sentiment into unified signal."""

        # Calculate overall sentiment score
        sentiment_scores = []
        confidences = []

        if news_signal:
            sentiment_scores.append(news_signal.volume_weighted_sentiment * self.sentiment_weights['news'])
            confidences.append(news_signal.confidence * self.sentiment_weights['news'])

        if social_signal:
            social_sentiment = social_signal.key_metrics.average_sentiment
            sentiment_scores.append(social_sentiment * self.sentiment_weights['social'])
            confidences.append(social_signal.confidence * self.sentiment_weights['social'])

        if not sentiment_scores:
            return None

        overall_sentiment_score = sum(sentiment_scores)
        overall_confidence = sum(confidences)

        # Calculate consensus and divergence
        if news_signal and social_signal:
            news_sent = news_signal.volume_weighted_sentiment
            social_sent = social_signal.key_metrics.average_sentiment

            consensus_score = 1.0 - abs(news_sent - social_sent) / 2.0  # How much they agree
            divergence_score = abs(news_sent - social_sent)  # How much they disagree
        else:
            consensus_score = 0.5  # Neutral when only one source
            divergence_score = 0.0

        # Determine signal direction and strength
        if overall_sentiment_score > 0.1:
            signal_direction = 'bullish'
            signal_strength = min(overall_sentiment_score, 1.0)
        elif overall_sentiment_score < -0.1:
            signal_direction = 'bearish'
            signal_strength = max(overall_sentiment_score, -1.0)
        else:
            signal_direction = 'neutral'
            signal_strength = 0.0

        # Calculate volume indicator
        news_volume = len(news_signal.supporting_articles) if news_signal else 0
        social_volume = social_signal.key_metrics.total_posts if social_signal else 0
        volume_indicator = min((news_volume + social_volume) / 20.0, 1.0)  # Normalize to 0-1

        # Determine time horizon
        time_horizon = self._determine_time_horizon(news_signal, social_signal)

        # Calculate risk score
        risk_score = await self._calculate_risk_score(symbol, news_signal, social_signal)

        # Extract key themes
        key_themes = self._extract_key_themes(news_signal, social_signal)

        # Collect risk factors
        risk_factors = []
        if news_signal:
            risk_factors.extend([f"News: {rf}" for rf in getattr(news_signal, 'risk_factors', [])])
        if social_signal:
            risk_factors.extend([f"Social: {rf}" for rf in social_signal.risk_factors])

        # Create sentiment features
        historical_signals = await self._get_historical_signals(symbol, days_back=7)

        unified_signal = UnifiedSentimentSignal(
            symbol=symbol,
            timestamp=datetime.now(),
            overall_sentiment_score=overall_sentiment_score,
            overall_confidence=overall_confidence,
            signal_strength=signal_strength,
            signal_direction=signal_direction,
            news_sentiment=news_signal,
            social_sentiment=social_signal,
            sentiment_features={},  # Will be filled below
            time_horizon=time_horizon,
            risk_score=risk_score,
            volume_indicator=volume_indicator,
            consensus_score=consensus_score,
            divergence_score=divergence_score,
            total_news_articles=news_volume,
            total_social_posts=social_volume,
            key_themes=key_themes,
            risk_factors=risk_factors
        )

        # Extract features
        unified_signal.sentiment_features = self.feature_extractor.extract_features(
            unified_signal, historical_signals
        )

        return unified_signal

    def _determine_time_horizon(self, news_signal: Optional[SentimentSignal],
                              social_signal: Optional[SocialTradingSignal]) -> str:
        """Determine appropriate time horizon for the signal."""
        horizons = []

        if news_signal:
            horizons.append(news_signal.time_horizon)

        if social_signal:
            horizons.append(social_signal.time_horizon)

        if not horizons:
            return 'medium'

        # Priority: short > medium > long > intraday
        if 'short' in horizons:
            return 'short'
        elif 'medium' in horizons:
            return 'medium'
        elif 'long' in horizons:
            return 'long'
        else:
            return 'short'  # Default for intraday

    async def _calculate_risk_score(self, symbol: str,
                                  news_signal: Optional[SentimentSignal],
                                  social_signal: Optional[SocialTradingSignal]) -> float:
        """Calculate risk score based on sentiment characteristics."""
        risk_factors = 0.0

        # High divergence between sources
        if news_signal and social_signal:
            divergence = abs(news_signal.volume_weighted_sentiment -
                           social_signal.key_metrics.average_sentiment)
            if divergence > 0.5:
                risk_factors += 0.3

        # Low confidence scores
        if news_signal and news_signal.confidence < 0.4:
            risk_factors += 0.2
        if social_signal and social_signal.confidence < 0.4:
            risk_factors += 0.2

        # High social media volatility
        if social_signal and social_signal.key_metrics.sentiment_std > 0.7:
            risk_factors += 0.2

        # Check for high-risk keywords
        all_text = ""
        if news_signal:
            for article in news_signal.supporting_articles:
                all_text += f" {article.title} {article.content}"
        if social_signal:
            for post in social_signal.supporting_posts:
                all_text += f" {post.content}"

        all_text_lower = all_text.lower()
        risk_keyword_count = sum(1 for keyword in self.high_risk_keywords
                               if keyword in all_text_lower)
        if risk_keyword_count > 0:
            risk_factors += min(risk_keyword_count * 0.1, 0.3)

        return min(risk_factors, 1.0)

    def _extract_key_themes(self, news_signal: Optional[SentimentSignal],
                           social_signal: Optional[SocialTradingSignal]) -> List[str]:
        """Extract key themes from sentiment signals."""
        themes = set()

        # From news
        if news_signal:
            for article in news_signal.supporting_articles:
                # Simple keyword extraction (in production, use more sophisticated NLP)
                text = f"{article.title} {article.content}".lower()
                if 'earnings' in text:
                    themes.add('earnings')
                if 'merger' in text or 'acquisition' in text:
                    themes.add('m&a')
                if 'dividend' in text:
                    themes.add('dividend')
                if 'upgrade' in text or 'downgrade' in text:
                    themes.add('analyst_rating')

        # From social media
        if social_signal:
            hashtags = []
            for post in social_signal.supporting_posts:
                hashtags.extend(post.hashtags)

            # Most common hashtags become themes
            from collections import Counter
            common_hashtags = Counter(hashtags).most_common(3)
            themes.update([tag for tag, _ in common_hashtags])

        return list(themes)[:5]  # Limit to top 5 themes

    async def _get_historical_signals(self, symbol: str, days_back: int = 7) -> List[UnifiedSentimentSignal]:
        """Get historical unified sentiment signals for context."""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetch("""
                    SELECT * FROM unified_sentiment_signals
                    WHERE symbol = $1
                        AND created_at >= $2
                    ORDER BY created_at DESC
                    LIMIT 20
                """, symbol, datetime.now() - timedelta(days=days_back))

                signals = []
                for row in result:
                    # Reconstruct unified signal from stored data
                    signal = UnifiedSentimentSignal(
                        symbol=row['symbol'],
                        timestamp=row['created_at'],
                        overall_sentiment_score=row['overall_sentiment_score'],
                        overall_confidence=row['overall_confidence'],
                        signal_strength=row['signal_strength'],
                        signal_direction=row['signal_direction'],
                        news_sentiment=None,  # Don't reconstruct full objects for historical
                        social_sentiment=None,
                        sentiment_features=json.loads(row['sentiment_features']),
                        time_horizon=row['time_horizon'],
                        risk_score=row['risk_score'],
                        volume_indicator=row['volume_indicator'],
                        consensus_score=row['consensus_score'],
                        divergence_score=row['divergence_score'],
                        total_news_articles=row['total_news_articles'],
                        total_social_posts=row['total_social_posts'],
                        key_themes=row['key_themes'],
                        risk_factors=row['risk_factors']
                    )
                    signals.append(signal)

                return signals

        except Exception as e:
            logging.error(f"Failed to get historical signals: {e}")
            return []

    async def _store_unified_signals(self, signals: Dict[str, UnifiedSentimentSignal]):
        """Store unified sentiment signals in database."""
        try:
            async with self.pool.acquire() as conn:
                for symbol, signal in signals.items():
                    await conn.execute("""
                        INSERT INTO unified_sentiment_signals
                        (symbol, overall_sentiment_score, overall_confidence,
                         signal_strength, signal_direction, time_horizon,
                         risk_score, volume_indicator, consensus_score,
                         divergence_score, total_news_articles, total_social_posts,
                         key_themes, risk_factors, sentiment_features, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                    """,
                    signal.symbol, signal.overall_sentiment_score, signal.overall_confidence,
                    signal.signal_strength, signal.signal_direction, signal.time_horizon,
                    signal.risk_score, signal.volume_indicator, signal.consensus_score,
                    signal.divergence_score, signal.total_news_articles, signal.total_social_posts,
                    signal.key_themes, signal.risk_factors,
                    json.dumps(signal.sentiment_features), signal.timestamp
                    )

        except Exception as e:
            logging.error(f"Failed to store unified signals: {e}")

    async def enhance_residual_return_predictions(self, base_predictions: pd.DataFrame) -> pd.DataFrame:
        """
        Enhance residual return predictions with sentiment signals.

        Args:
            base_predictions: DataFrame with columns [symbol, prediction_date, horizon,
                            predicted_return, confidence]

        Returns:
            Enhanced DataFrame with sentiment-adjusted predictions
        """
        try:
            enhanced_predictions = []

            for _, pred_row in base_predictions.iterrows():
                symbol = pred_row['symbol']
                prediction_date = pred_row['prediction_date']

                # Get unified sentiment signal for this symbol
                unified_signals = await self.generate_unified_sentiment_signals([symbol], hours_back=24)
                unified_signal = unified_signals.get(symbol)

                if unified_signal:
                    # Calculate sentiment adjustment
                    sentiment_adjustment = self._calculate_sentiment_adjustment(
                        pred_row, unified_signal
                    )

                    # Apply adjustment
                    base_return = pred_row['predicted_return']
                    base_confidence = pred_row['confidence']

                    adjusted_return = base_return + sentiment_adjustment['return_adjustment']
                    confidence_boost = sentiment_adjustment['confidence_boost']
                    final_confidence = min(base_confidence + confidence_boost, 1.0)

                    # Create enhanced prediction
                    enhanced_pred = SentimentBasedPrediction(
                        symbol=symbol,
                        prediction_date=prediction_date,
                        horizon_days=pred_row.get('horizon', 1),
                        base_residual_return=base_return,
                        base_confidence=base_confidence,
                        sentiment_adjusted_return=adjusted_return,
                        sentiment_confidence_boost=confidence_boost,
                        final_confidence=final_confidence,
                        news_contribution=sentiment_adjustment['news_contribution'],
                        social_contribution=sentiment_adjustment['social_contribution'],
                        sentiment_risk_adjustment=sentiment_adjustment['risk_adjustment'],
                        unified_sentiment=unified_signal,
                        prediction_explanation=sentiment_adjustment['explanation']
                    )

                    enhanced_predictions.append({
                        'symbol': symbol,
                        'prediction_date': prediction_date,
                        'horizon': pred_row.get('horizon', 1),
                        'base_predicted_return': base_return,
                        'sentiment_adjusted_return': adjusted_return,
                        'base_confidence': base_confidence,
                        'final_confidence': final_confidence,
                        'sentiment_score': unified_signal.overall_sentiment_score,
                        'sentiment_confidence': unified_signal.overall_confidence,
                        'news_contribution': sentiment_adjustment['news_contribution'],
                        'social_contribution': sentiment_adjustment['social_contribution'],
                        'risk_adjustment': sentiment_adjustment['risk_adjustment'],
                        'explanation': sentiment_adjustment['explanation']
                    })
                else:
                    # No sentiment signal, keep original prediction
                    enhanced_predictions.append({
                        'symbol': symbol,
                        'prediction_date': prediction_date,
                        'horizon': pred_row.get('horizon', 1),
                        'base_predicted_return': pred_row['predicted_return'],
                        'sentiment_adjusted_return': pred_row['predicted_return'],
                        'base_confidence': pred_row['confidence'],
                        'final_confidence': pred_row['confidence'],
                        'sentiment_score': 0.0,
                        'sentiment_confidence': 0.0,
                        'news_contribution': 0.0,
                        'social_contribution': 0.0,
                        'risk_adjustment': 0.0,
                        'explanation': 'No sentiment data available'
                    })

            return pd.DataFrame(enhanced_predictions)

        except Exception as e:
            logging.error(f"Failed to enhance predictions with sentiment: {e}")
            return base_predictions

    def _calculate_sentiment_adjustment(self, prediction_row: pd.Series,
                                      unified_signal: UnifiedSentimentSignal) -> Dict[str, float]:
        """Calculate how sentiment should adjust the base prediction."""

        # Sentiment impact factors
        sentiment_score = unified_signal.overall_sentiment_score
        sentiment_confidence = unified_signal.overall_confidence
        consensus = unified_signal.consensus_score
        risk_score = unified_signal.risk_score

        # Base return adjustment (proportional to sentiment strength and confidence)
        max_adjustment = 0.02  # Maximum 2% adjustment
        return_adjustment = sentiment_score * sentiment_confidence * consensus * max_adjustment

        # Risk adjustment (reduce adjustment for high-risk signals)
        risk_factor = 1.0 - risk_score
        return_adjustment *= risk_factor

        # Confidence boost (sentiment can increase confidence when consensus is high)
        max_confidence_boost = 0.2  # Maximum 20% confidence boost
        confidence_boost = sentiment_confidence * consensus * max_confidence_boost

        # Individual contributions
        news_contribution = 0.0
        social_contribution = 0.0

        if unified_signal.news_sentiment:
            news_weight = self.sentiment_weights['news']
            news_contribution = unified_signal.news_sentiment.volume_weighted_sentiment * news_weight

        if unified_signal.social_sentiment:
            social_weight = self.sentiment_weights['social']
            social_contribution = unified_signal.social_sentiment.key_metrics.average_sentiment * social_weight

        # Generate explanation
        explanation_parts = []
        if abs(return_adjustment) > 0.005:  # Significant adjustment
            direction = "positive" if return_adjustment > 0 else "negative"
            explanation_parts.append(f"Sentiment provides {direction} adjustment of {return_adjustment:.1%}")

        if unified_signal.consensus_score < 0.5:
            explanation_parts.append("Low consensus between news and social sentiment")

        if unified_signal.risk_score > 0.5:
            explanation_parts.append("High-risk sentiment factors detected")

        explanation = "; ".join(explanation_parts) if explanation_parts else "Neutral sentiment impact"

        return {
            'return_adjustment': return_adjustment,
            'confidence_boost': confidence_boost,
            'risk_adjustment': risk_score,
            'news_contribution': news_contribution,
            'social_contribution': social_contribution,
            'explanation': explanation
        }

    async def close(self):
        """Close resources."""
        await self.news_analyzer.close()


# Convenience functions
async def generate_sentiment_enhanced_predictions(pool: asyncpg.Pool, env,
                                                base_predictions: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience function to enhance predictions with sentiment analysis.

    Args:
        pool: Database connection pool
        env: Environment configuration
        base_predictions: Base predictions DataFrame

    Returns:
        Sentiment-enhanced predictions DataFrame
    """
    integrator = SentimentIntegrator(pool, env)
    try:
        return await integrator.enhance_residual_return_predictions(base_predictions)
    finally:
        await integrator.close()


async def analyze_unified_sentiment(pool: asyncpg.Pool, env, symbols: List[str],
                                  hours_back: int = 24) -> Dict[str, UnifiedSentimentSignal]:
    """
    Convenience function to generate unified sentiment signals.

    Args:
        pool: Database connection pool
        env: Environment configuration
        symbols: List of symbols to analyze
        hours_back: Hours of historical data to analyze

    Returns:
        Dict mapping symbols to unified sentiment signals
    """
    integrator = SentimentIntegrator(pool, env)
    try:
        return await integrator.generate_unified_sentiment_signals(symbols, hours_back)
    finally:
        await integrator.close()