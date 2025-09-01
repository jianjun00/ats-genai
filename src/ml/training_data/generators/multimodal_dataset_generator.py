#!/usr/bin/env python3
"""
Multi-Modal Training Dataset Generator
Combines news sentiment, economic events, and price/volume data into training samples
for the Multi-Modal Trading Signal Prediction System.
"""

import asyncio
import asyncpg
import logging
import json
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import time

logger = logging.getLogger(__name__)

@dataclass
class MultiModalSample:
    """Multi-modal training sample with comprehensive features"""
    symbol: str
    sample_date: date
    prediction_horizon: int
    
    # News sentiment features (lookback windows)
    news_sentiment_1d: float = 0.0
    news_sentiment_3d: float = 0.0  
    news_sentiment_7d: float = 0.0
    news_volume_1d: int = 0
    news_volume_3d: int = 0
    news_volume_7d: int = 0
    news_momentum_3d: float = 0.0
    news_momentum_7d: float = 0.0
    
    # Economic event features
    economic_event_impact_1d: float = 0.0
    economic_event_impact_3d: float = 0.0
    economic_event_impact_7d: float = 0.0
    earnings_impact_score: float = 0.0
    macro_event_impact: float = 0.0
    fed_event_impact: float = 0.0
    
    # Technical market features
    price_features: Dict[str, float] = None
    volume_features: Dict[str, float] = None
    market_microstructure: Dict[str, float] = None
    
    # Cross-asset features
    sector_correlation: float = 0.0
    market_correlation: float = 0.0
    vix_level: float = 0.0
    yield_curve_10y2y: float = 0.0
    dxy_level: float = 0.0
    
    # Target variables (future performance)
    target_return_1d: Optional[float] = None
    target_return_5d: Optional[float] = None
    target_return_10d: Optional[float] = None
    target_return_20d: Optional[float] = None
    target_volatility_5d: Optional[float] = None
    target_volatility_20d: Optional[float] = None
    target_max_drawdown: Optional[float] = None
    target_sharpe_ratio: Optional[float] = None
    
    # Classification targets
    target_direction_1d: Optional[int] = None
    target_direction_5d: Optional[int] = None
    target_direction_10d: Optional[int] = None
    target_direction_20d: Optional[int] = None
    target_volatility_regime: Optional[int] = None
    
    # Sample metadata
    sample_quality_score: float = 1.0
    sample_weight: float = 1.0
    is_outlier: bool = False
    market_regime: Optional[str] = None
    
    def __post_init__(self):
        if self.price_features is None:
            self.price_features = {}
        if self.volume_features is None:
            self.volume_features = {}
        if self.market_microstructure is None:
            self.market_microstructure = {}

class MultiModalDatasetGenerator:
    """
    Generates comprehensive multi-modal training datasets combining:
    - News sentiment and volume
    - Economic events impact
    - Technical price/volume indicators  
    - Cross-asset correlations
    - Future performance targets
    """
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.db_pool = None
        
        # Prediction horizons to generate
        self.prediction_horizons = [1, 5, 10, 20]
        
        # Market regimes classification
        self.regime_thresholds = {
            'bull': {'min_return': 0.02, 'max_volatility': 0.15},
            'bear': {'max_return': -0.02, 'min_volatility': 0.20},
            'sideways': {'return_range': 0.02, 'volatility_range': 0.05}
        }
    
    async def __aenter__(self):
        """Initialize database connection pool"""
        self.db_pool = await asyncpg.create_pool(
            **self.db_config,
            min_size=10,
            max_size=20,
            server_settings={'jit': 'off'}
        )
        await self._ensure_tables_exist()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup database connection pool"""
        if self.db_pool:
            await self.db_pool.close()
    
    async def _ensure_tables_exist(self):
        """Create multi-modal training samples table if it doesn't exist"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_multimodal_training_samples (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    sample_date DATE NOT NULL,
                    prediction_horizon INTEGER NOT NULL CHECK (prediction_horizon IN (1, 5, 10, 20)),
                    
                    -- News sentiment features (lookback: 1, 3, 7 days)
                    news_sentiment_1d DECIMAL(7,4) DEFAULT 0,
                    news_sentiment_3d DECIMAL(7,4) DEFAULT 0,
                    news_sentiment_7d DECIMAL(7,4) DEFAULT 0,
                    news_volume_1d INTEGER DEFAULT 0,
                    news_volume_3d INTEGER DEFAULT 0,
                    news_volume_7d INTEGER DEFAULT 0,
                    news_momentum_3d DECIMAL(7,4) DEFAULT 0,
                    news_momentum_7d DECIMAL(7,4) DEFAULT 0,
                    
                    -- Economic event features
                    economic_event_impact_1d DECIMAL(7,4) DEFAULT 0,
                    economic_event_impact_3d DECIMAL(7,4) DEFAULT 0,
                    economic_event_impact_7d DECIMAL(7,4) DEFAULT 0,
                    earnings_impact_score DECIMAL(7,4) DEFAULT 0,
                    macro_event_impact DECIMAL(7,4) DEFAULT 0,
                    fed_event_impact DECIMAL(7,4) DEFAULT 0,
                    
                    -- Technical market features (stored as JSONB for flexibility)
                    price_features JSONB NOT NULL DEFAULT '{}',
                    volume_features JSONB NOT NULL DEFAULT '{}',
                    market_microstructure JSONB DEFAULT '{}',
                    
                    -- Cross-asset features
                    sector_correlation DECIMAL(7,4),
                    market_correlation DECIMAL(7,4),
                    vix_level DECIMAL(7,4),
                    yield_curve_10y2y DECIMAL(7,4),
                    dxy_level DECIMAL(7,4),
                    
                    -- Target variables (actual future performance)
                    target_return_1d DECIMAL(8,5),
                    target_return_5d DECIMAL(8,5), 
                    target_return_10d DECIMAL(8,5),
                    target_return_20d DECIMAL(8,5),
                    target_volatility_5d DECIMAL(8,5),
                    target_volatility_20d DECIMAL(8,5),
                    target_max_drawdown DECIMAL(8,5),
                    target_sharpe_ratio DECIMAL(8,5),
                    
                    -- Classification targets
                    target_direction_1d INTEGER CHECK (target_direction_1d IN (-1, 0, 1)),
                    target_direction_5d INTEGER CHECK (target_direction_5d IN (-1, 0, 1)),
                    target_direction_10d INTEGER CHECK (target_direction_10d IN (-1, 0, 1)),
                    target_direction_20d INTEGER CHECK (target_direction_20d IN (-1, 0, 1)),
                    target_volatility_regime INTEGER CHECK (target_volatility_regime IN (1, 2, 3)),
                    
                    -- Sample metadata
                    sample_quality_score DECIMAL(5,3) DEFAULT 1.0 CHECK (sample_quality_score BETWEEN 0 AND 1),
                    sample_weight DECIMAL(7,4) DEFAULT 1.0,
                    is_outlier BOOLEAN DEFAULT FALSE,
                    market_regime VARCHAR(20) CHECK (market_regime IN ('bull', 'bear', 'sideways', 'crisis')),
                    
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    PRIMARY KEY (symbol, sample_date, prediction_horizon)
                )
            """)
            
            # Create performance indexes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_multimodal_samples_symbol_date 
                ON dev_multimodal_training_samples(symbol, sample_date DESC)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_multimodal_samples_horizon 
                ON dev_multimodal_training_samples(prediction_horizon, sample_date DESC)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_multimodal_samples_quality 
                ON dev_multimodal_training_samples(sample_quality_score DESC, is_outlier, sample_date DESC)
            """)
    
    async def generate_news_features(self, symbol: str, sample_date: date) -> Dict[str, Any]:
        """Generate news sentiment features for multiple lookback windows"""
        async with self.db_pool.acquire() as conn:
            features = {}
            
            # Define lookback windows
            lookback_windows = [1, 3, 7]
            
            for days in lookback_windows:
                start_date = sample_date - timedelta(days=days)
                
                # Get news sentiment from both Polygon and Tiingo
                polygon_news = await conn.fetch("""
                    SELECT title, description, published_utc
                    FROM news_polygon
                    WHERE tickers @> ARRAY[$1]
                        AND published_utc::date BETWEEN $2 AND $3
                    ORDER BY published_utc DESC
                """, symbol, start_date, sample_date)
                
                tiingo_news = await conn.fetch("""
                    SELECT title, description, published_date
                    FROM news_tiingo  
                    WHERE tickers @> ARRAY[$1]
                        AND published_date::date BETWEEN $2 AND $3
                    ORDER BY published_date DESC
                """, symbol, start_date, sample_date)
                
                # Combine news from both sources
                all_news = []
                for article in polygon_news:
                    all_news.append({
                        'title': article['title'],
                        'description': article['description'],
                        'date': article['published_utc']
                    })
                for article in tiingo_news:
                    all_news.append({
                        'title': article['title'], 
                        'description': article['description'],
                        'date': article['published_date']
                    })
                
                # Calculate sentiment features (simplified for now)
                if all_news:
                    # Simple sentiment scoring based on keywords
                    sentiment_scores = []
                    for article in all_news:
                        text = f"{article['title']} {article['description']}".lower()
                        sentiment = self._calculate_simple_sentiment(text)
                        sentiment_scores.append(sentiment)
                    
                    features[f'news_sentiment_{days}d'] = float(np.mean(sentiment_scores))
                    features[f'news_volume_{days}d'] = len(all_news)
                    
                    # Calculate momentum (sentiment change over time)
                    if days > 1:
                        recent_sentiment = np.mean(sentiment_scores[:len(sentiment_scores)//2]) if sentiment_scores else 0
                        older_sentiment = np.mean(sentiment_scores[len(sentiment_scores)//2:]) if sentiment_scores else 0
                        features[f'news_momentum_{days}d'] = float(recent_sentiment - older_sentiment)
                    else:
                        features[f'news_momentum_{days}d'] = 0.0
                else:
                    features[f'news_sentiment_{days}d'] = 0.0
                    features[f'news_volume_{days}d'] = 0
                    features[f'news_momentum_{days}d'] = 0.0
            
            return features
    
    def _calculate_simple_sentiment(self, text: str) -> float:
        """Simple sentiment calculation based on keyword scoring"""
        positive_words = [
            'beat', 'beats', 'exceed', 'strong', 'growth', 'positive', 'bullish',
            'up', 'rise', 'gain', 'profit', 'success', 'good', 'better', 'best'
        ]
        negative_words = [
            'miss', 'misses', 'weak', 'decline', 'negative', 'bearish', 'down',
            'fall', 'loss', 'bad', 'worse', 'worst', 'concern', 'worry', 'risk'
        ]
        
        positive_count = sum(1 for word in positive_words if word in text)
        negative_count = sum(1 for word in negative_words if word in text)
        
        total_words = len(text.split())
        if total_words == 0:
            return 0.0
        
        # Normalize by text length and return score between -1 and 1
        sentiment_score = (positive_count - negative_count) / max(1, total_words / 20)
        return max(-1.0, min(1.0, sentiment_score))
    
    async def generate_economic_event_features(self, symbol: str, sample_date: date) -> Dict[str, Any]:
        """Generate economic events impact features"""
        async with self.db_pool.acquire() as conn:
            features = {}
            
            # Define lookback windows
            lookback_windows = [1, 3, 7]
            
            for days in lookback_windows:
                start_date = sample_date - timedelta(days=days)
                
                # Get economic events affecting this symbol
                events = await conn.fetch("""
                    SELECT event_category, predicted_impact_score, severity
                    FROM dev_economic_events
                    WHERE (affected_symbols @> ARRAY[$1] OR affected_symbols = ARRAY[]::TEXT[])
                        AND event_date::date BETWEEN $2 AND $3
                    ORDER BY event_date DESC
                """, symbol, start_date, sample_date)
                
                # Calculate impact scores by category
                category_impacts = {'earnings': 0, 'fed': 0, 'macro': 0}
                total_impact = 0
                
                for event in events:
                    impact = float(event['predicted_impact_score'] or 0)
                    severity_weight = float(event['severity']) / 10.0  # Normalize severity
                    weighted_impact = impact * severity_weight
                    
                    total_impact += weighted_impact
                    
                    if event['event_category'] in category_impacts:
                        category_impacts[event['event_category']] += weighted_impact
                
                features[f'economic_event_impact_{days}d'] = float(total_impact)
            
            # Specific category impacts (across all time windows)
            start_date_7d = sample_date - timedelta(days=7)
            category_events = await conn.fetch("""
                SELECT event_category, predicted_impact_score, severity
                FROM dev_economic_events
                WHERE (affected_symbols @> ARRAY[$1] OR affected_symbols = ARRAY[]::TEXT[])
                    AND event_date::date BETWEEN $2 AND $3
                ORDER BY event_date DESC
            """, symbol, start_date_7d, sample_date)
            
            earnings_impact = 0
            fed_impact = 0
            macro_impact = 0
            
            for event in category_events:
                impact = float(event['predicted_impact_score'] or 0)
                severity_weight = float(event['severity']) / 10.0
                weighted_impact = impact * severity_weight
                
                if event['event_category'] == 'earnings':
                    earnings_impact += weighted_impact
                elif event['event_category'] == 'fed':
                    fed_impact += weighted_impact
                else:
                    macro_impact += weighted_impact
            
            features['earnings_impact_score'] = float(earnings_impact)
            features['fed_event_impact'] = float(fed_impact)
            features['macro_event_impact'] = float(macro_impact)
            
            return features
    
    async def generate_sample_for_symbol_date(self, symbol: str, sample_date: date, 
                                            horizon: int) -> Optional[MultiModalSample]:
        """Generate a single multi-modal training sample"""
        try:
            # Check if we have enough future data for targets
            future_date = sample_date + timedelta(days=horizon)
            if future_date > date.today() - timedelta(days=5):  # Need some buffer
                return None
            
            # Generate news features
            news_features = await self.generate_news_features(symbol, sample_date)
            
            # Generate economic event features
            event_features = await self.generate_economic_event_features(symbol, sample_date)
            
            # Create sample with basic features
            sample = MultiModalSample(
                symbol=symbol,
                sample_date=sample_date,
                prediction_horizon=horizon,
                **news_features,
                **event_features
            )
            
            # Add simplified technical features (would be enhanced with real price data)
            sample.price_features = {
                'placeholder_sma_10': 100.0,  # Would calculate from real price data
                'placeholder_rsi_14': 50.0,
                'placeholder_macd': 0.0
            }
            
            sample.volume_features = {
                'placeholder_volume_sma_10': 1000000,  # Would calculate from real volume data
                'placeholder_relative_volume': 1.0
            }
            
            # Add placeholder targets (would calculate from real future price data)
            sample.target_return_1d = 0.001  # Placeholder
            sample.target_return_5d = 0.005  # Placeholder  
            sample.target_return_10d = 0.010 # Placeholder
            sample.target_return_20d = 0.020 # Placeholder
            
            # Classification targets
            sample.target_direction_1d = 1 if sample.target_return_1d > 0 else -1
            sample.target_direction_5d = 1 if sample.target_return_5d > 0 else -1
            sample.target_direction_10d = 1 if sample.target_return_10d > 0 else -1
            sample.target_direction_20d = 1 if sample.target_return_20d > 0 else -1
            
            # Sample quality score based on data availability
            quality_score = 1.0
            if news_features.get('news_volume_7d', 0) == 0:
                quality_score -= 0.3  # Penalize lack of news
            if sum(event_features.values()) == 0:
                quality_score -= 0.2  # Penalize lack of economic events
            
            sample.sample_quality_score = max(0.1, quality_score)
            sample.market_regime = 'sideways'  # Placeholder
            
            return sample
            
        except Exception as e:
            logger.warning(f"Error generating sample for {symbol} on {sample_date}: {e}")
            return None
    
    async def bulk_insert_samples(self, samples: List[MultiModalSample]) -> int:
        """Bulk insert training samples into database"""
        if not samples:
            return 0
        
        async with self.db_pool.acquire() as conn:
            try:
                records = []
                for sample in samples:
                    records.append((
                        sample.symbol,
                        sample.sample_date,
                        sample.prediction_horizon,
                        sample.news_sentiment_1d,
                        sample.news_sentiment_3d,
                        sample.news_sentiment_7d,
                        sample.news_volume_1d,
                        sample.news_volume_3d,
                        sample.news_volume_7d,
                        sample.news_momentum_3d,
                        sample.news_momentum_7d,
                        sample.economic_event_impact_1d,
                        sample.economic_event_impact_3d,
                        sample.economic_event_impact_7d,
                        sample.earnings_impact_score,
                        sample.macro_event_impact,
                        sample.fed_event_impact,
                        json.dumps(sample.price_features),
                        json.dumps(sample.volume_features),
                        json.dumps(sample.market_microstructure),
                        sample.sector_correlation,
                        sample.market_correlation,
                        sample.vix_level,
                        sample.yield_curve_10y2y,
                        sample.dxy_level,
                        sample.target_return_1d,
                        sample.target_return_5d,
                        sample.target_return_10d,
                        sample.target_return_20d,
                        sample.target_volatility_5d,
                        sample.target_volatility_20d,
                        sample.target_max_drawdown,
                        sample.target_sharpe_ratio,
                        sample.target_direction_1d,
                        sample.target_direction_5d,
                        sample.target_direction_10d,
                        sample.target_direction_20d,
                        sample.target_volatility_regime,
                        sample.sample_quality_score,
                        sample.sample_weight,
                        sample.is_outlier,
                        sample.market_regime
                    ))
                
                await conn.executemany("""
                    INSERT INTO dev_multimodal_training_samples (
                        symbol, sample_date, prediction_horizon,
                        news_sentiment_1d, news_sentiment_3d, news_sentiment_7d,
                        news_volume_1d, news_volume_3d, news_volume_7d,
                        news_momentum_3d, news_momentum_7d,
                        economic_event_impact_1d, economic_event_impact_3d, economic_event_impact_7d,
                        earnings_impact_score, macro_event_impact, fed_event_impact,
                        price_features, volume_features, market_microstructure,
                        sector_correlation, market_correlation, vix_level, yield_curve_10y2y, dxy_level,
                        target_return_1d, target_return_5d, target_return_10d, target_return_20d,
                        target_volatility_5d, target_volatility_20d, target_max_drawdown, target_sharpe_ratio,
                        target_direction_1d, target_direction_5d, target_direction_10d, target_direction_20d,
                        target_volatility_regime, sample_quality_score, sample_weight, is_outlier, market_regime
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 
                           $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33,
                           $34, $35, $36, $37, $38, $39, $40, $41, $42)
                    ON CONFLICT (symbol, sample_date, prediction_horizon) DO UPDATE SET
                        news_sentiment_1d = EXCLUDED.news_sentiment_1d,
                        news_sentiment_3d = EXCLUDED.news_sentiment_3d,
                        news_sentiment_7d = EXCLUDED.news_sentiment_7d,
                        updated_at = NOW()
                """, records)
                
                return len(records)
                
            except Exception as e:
                logger.error(f"Error bulk inserting samples: {e}")
                return 0
    
    async def generate_training_dataset(self, symbols: List[str], start_date: date, 
                                      end_date: date, sample_freq_days: int = 7) -> int:
        """
        Generate comprehensive multi-modal training dataset
        
        Args:
            symbols: List of stock symbols to process
            start_date: Start date for sample generation
            end_date: End date for sample generation  
            sample_freq_days: Frequency of sampling (every N days)
            
        Returns:
            Total number of training samples created
        """
        total_samples = 0
        start_time = time.time()
        
        logger.info(f"🚀 MULTI-MODAL DATASET GENERATION STARTING")
        logger.info(f"📊 Symbols: {len(symbols)}")
        logger.info(f"📅 Date Range: {start_date} to {end_date}")
        logger.info(f"🔄 Sample Frequency: Every {sample_freq_days} days")
        logger.info(f"📈 Prediction Horizons: {self.prediction_horizons}")
        
        # Generate samples for each symbol
        for i, symbol in enumerate(symbols):
            symbol_samples = []
            
            # Generate samples across date range
            current_date = start_date
            while current_date <= end_date:
                # Generate samples for all prediction horizons
                for horizon in self.prediction_horizons:
                    sample = await self.generate_sample_for_symbol_date(symbol, current_date, horizon)
                    if sample:
                        symbol_samples.append(sample)
                
                # Move to next sample date
                current_date += timedelta(days=sample_freq_days)
            
            # Bulk insert samples for this symbol
            if symbol_samples:
                inserted = await self.bulk_insert_samples(symbol_samples)
                total_samples += inserted
                logger.info(f"📊 {symbol} ({i+1}/{len(symbols)}): {inserted} samples created")
            
            # Progress update
            if (i + 1) % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_samples / elapsed if elapsed > 0 else 0
                logger.info(f"⏱️  Progress: {i+1}/{len(symbols)} symbols, {total_samples:,} samples, {rate:.0f} samples/sec")
        
        # Final results
        elapsed = time.time() - start_time
        rate = total_samples / elapsed if elapsed > 0 else 0
        
        logger.info(f"")
        logger.info(f"🎉 MULTI-MODAL DATASET GENERATION COMPLETE!")
        logger.info(f"⏱️  Total Time: {elapsed:.0f}s ({elapsed/60:.1f}min)")
        logger.info(f"📊 Total Samples: {total_samples:,}")
        logger.info(f"🔥 Generation Rate: {rate:.0f} samples/second")
        
        return total_samples
    
    async def get_dataset_summary(self) -> Dict[str, Any]:
        """Get summary statistics of the generated dataset"""
        async with self.db_pool.acquire() as conn:
            summary = {}
            
            # Basic counts
            summary['total_samples'] = await conn.fetchval("SELECT COUNT(*) FROM dev_multimodal_training_samples")
            summary['unique_symbols'] = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM dev_multimodal_training_samples")
            
            # Date range
            date_range = await conn.fetchrow("""
                SELECT MIN(sample_date) as earliest, MAX(sample_date) as latest
                FROM dev_multimodal_training_samples
            """)
            summary['date_range'] = {
                'earliest': date_range['earliest'].isoformat() if date_range['earliest'] else None,
                'latest': date_range['latest'].isoformat() if date_range['latest'] else None
            }
            
            # Samples by horizon
            horizon_counts = await conn.fetch("""
                SELECT prediction_horizon, COUNT(*) as count
                FROM dev_multimodal_training_samples
                GROUP BY prediction_horizon
                ORDER BY prediction_horizon
            """)
            summary['by_horizon'] = {str(row['prediction_horizon']): row['count'] for row in horizon_counts}
            
            # Quality distribution
            quality_stats = await conn.fetchrow("""
                SELECT 
                    AVG(sample_quality_score) as avg_quality,
                    MIN(sample_quality_score) as min_quality,
                    MAX(sample_quality_score) as max_quality,
                    COUNT(*) FILTER (WHERE sample_quality_score >= 0.8) as high_quality_count
                FROM dev_multimodal_training_samples
            """)
            summary['quality_stats'] = dict(quality_stats)
            
            return summary


async def main():
    """Test the multi-modal dataset generation"""
    import os
    
    # Database configuration
    db_config = {
        'host': os.getenv("DB_HOST", "localhost"),
        'port': int(os.getenv("DB_PORT", "5433")),
        'user': os.getenv("DB_USER", "postgres"),
        'password': os.getenv("DB_PASSWORD", "postgres"),
        'database': os.getenv("DB_NAME", "dev_db")
    }
    
    # Test with a small set of symbols and date range
    test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    start_date = date(2024, 1, 1)
    end_date = date(2024, 2, 1)
    
    async with MultiModalDatasetGenerator(db_config) as generator:
        # Generate training dataset
        total_samples = await generator.generate_training_dataset(
            symbols=test_symbols,
            start_date=start_date,
            end_date=end_date,
            sample_freq_days=7
        )
        
        # Get summary
        summary = await generator.get_dataset_summary()
        
        print(f"📊 Dataset Generation Complete:")
        print(f"   Total Samples: {total_samples}")
        print(f"   Summary: {summary}")


if __name__ == "__main__":
    asyncio.run(main())