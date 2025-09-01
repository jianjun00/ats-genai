#!/usr/bin/env python3
"""
Multi-Modal Training Data Generator
Creates training samples combining news sentiment, market data, and economic events
for multi-modal price prediction models.
"""

import os
import asyncio
import asyncpg
import logging
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import json
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class TrainingSample:
    """Structure for a multi-modal training sample"""
    symbol: str
    sample_date: date
    prediction_horizon: int
    
    # News features
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
    
    # Technical features
    price_features: Dict = None
    volume_features: Dict = None
    market_microstructure: Dict = None
    
    # Cross-asset features
    sector_correlation: float = None
    market_correlation: float = None
    vix_level: float = None
    yield_curve_10y2y: float = None
    dxy_level: float = None
    
    # Target variables
    target_return_1d: float = None
    target_return_5d: float = None
    target_return_10d: float = None
    target_return_20d: float = None
    target_volatility_5d: float = None
    target_volatility_20d: float = None
    target_max_drawdown: float = None
    target_sharpe_ratio: float = None
    
    # Classification targets
    target_direction_1d: int = None
    target_direction_5d: int = None
    target_direction_10d: int = None
    target_direction_20d: int = None
    target_volatility_regime: int = None
    
    # Metadata
    sample_quality_score: float = 1.0
    sample_weight: float = 1.0
    is_outlier: bool = False
    market_regime: str = None

class MultiModalFeatureGenerator:
    """Generate features for multi-modal training samples"""
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.pool = db_pool
    
    async def generate_news_features(self, symbol: str, sample_date: date, 
                                   lookback_days: List[int] = [1, 3, 7]) -> Dict[str, float]:
        """Generate news sentiment features for multiple lookback periods"""
        features = {}
        
        async with self.pool.acquire() as conn:
            for days in lookback_days:
                start_date = sample_date - timedelta(days=days)
                
                # Query aggregated news sentiment from multiple sources
                query = """
                WITH polygon_news AS (
                    SELECT published_utc as pub_date, 0.0 as sentiment_score, tickers
                    FROM dev_news_polygon 
                    WHERE published_utc BETWEEN $1 AND $2
                    AND $3 = ANY(tickers)
                ),
                tiingo_news AS (
                    SELECT published_date as pub_date, 0.0 as sentiment_score, tickers
                    FROM dev_news_tiingo
                    WHERE published_date BETWEEN $1 AND $2
                    AND $3 = ANY(tickers)
                ),
                alpha_vantage_news AS (
                    SELECT time_published as pub_date, overall_sentiment_score as sentiment_score, tickers
                    FROM dev_news_alpha_vantage
                    WHERE time_published BETWEEN $1 AND $2
                    AND $3 = ANY(tickers)
                ),
                all_news AS (
                    SELECT pub_date, sentiment_score FROM polygon_news
                    UNION ALL
                    SELECT pub_date, sentiment_score FROM tiingo_news
                    UNION ALL
                    SELECT pub_date, sentiment_score FROM alpha_vantage_news
                )
                SELECT 
                    AVG(sentiment_score) as avg_sentiment,
                    COUNT(*) as news_count,
                    STDDEV(sentiment_score) as sentiment_volatility
                FROM all_news
                """
                
                result = await conn.fetchrow(query, start_date, sample_date, symbol)
                
                if result and result['news_count'] > 0:
                    features.update({
                        f'news_sentiment_{days}d': float(result['avg_sentiment'] or 0.0),
                        f'news_volume_{days}d': int(result['news_count']),
                    })
                else:
                    features.update({
                        f'news_sentiment_{days}d': 0.0,
                        f'news_volume_{days}d': 0,
                    })
                
                # Calculate news momentum (change in sentiment)
                if days > 1:
                    # Compare first half vs second half of period
                    mid_date = start_date + timedelta(days=days//2)
                    
                    early_sentiment = await conn.fetchval("""
                        WITH all_news AS (
                            SELECT 0.0 as sentiment_score FROM dev_news_polygon 
                            WHERE published_utc BETWEEN $1 AND $2 AND $3 = ANY(tickers)
                            UNION ALL
                            SELECT 0.0 FROM dev_news_tiingo
                            WHERE published_date BETWEEN $1 AND $2 AND $3 = ANY(tickers)
                            UNION ALL
                            SELECT overall_sentiment_score FROM dev_news_alpha_vantage
                            WHERE time_published BETWEEN $1 AND $2 AND $3 = ANY(tickers)
                        )
                        SELECT AVG(sentiment_score) FROM all_news
                    """, start_date, mid_date, symbol) or 0.0
                    
                    late_sentiment = await conn.fetchval("""
                        WITH all_news AS (
                            SELECT 0.0 as sentiment_score FROM dev_news_polygon 
                            WHERE published_utc BETWEEN $1 AND $2 AND $3 = ANY(tickers)
                            UNION ALL
                            SELECT 0.0 FROM dev_news_tiingo
                            WHERE published_date BETWEEN $1 AND $2 AND $3 = ANY(tickers)
                            UNION ALL
                            SELECT overall_sentiment_score FROM dev_news_alpha_vantage
                            WHERE time_published BETWEEN $1 AND $2 AND $3 = ANY(tickers)
                        )
                        SELECT AVG(sentiment_score) FROM all_news
                    """, mid_date, sample_date, symbol) or 0.0
                    
                    features[f'news_momentum_{days}d'] = float(late_sentiment) - float(early_sentiment)
        
        return features
    
    async def generate_economic_event_features(self, symbol: str, sample_date: date) -> Dict[str, float]:
        """Generate economic event impact features"""
        features = {
            'economic_event_impact_1d': 0.0,
            'economic_event_impact_3d': 0.0,
            'economic_event_impact_7d': 0.0,
            'earnings_impact_score': 0.0,
            'macro_event_impact': 0.0,
            'fed_event_impact': 0.0
        }
        
        async with self.pool.acquire() as conn:
            # Query economic events affecting this symbol
            for days in [1, 3, 7]:
                start_date = sample_date - timedelta(days=days)
                
                # General economic events
                general_impact = await conn.fetchval("""
                    SELECT AVG(predicted_impact_score * severity / 10.0)
                    FROM dev_economic_events
                    WHERE event_date BETWEEN $1 AND $2
                    AND (
                        $3 = ANY(affected_symbols) OR
                        cardinality(affected_symbols) = 0  -- General market events
                    )
                """, start_date, sample_date, symbol) or 0.0
                
                features[f'economic_event_impact_{days}d'] = float(general_impact)
            
            # Specific event type impacts
            categories = {
                'earnings_impact_score': 'earnings',
                'macro_event_impact': 'macro', 
                'fed_event_impact': 'fed'
            }
            
            for feature_name, category in categories.items():
                impact = await conn.fetchval("""
                    SELECT AVG(predicted_impact_score * severity / 10.0)
                    FROM dev_economic_events
                    WHERE event_date BETWEEN $1 AND $2
                    AND event_category = $3
                    AND ($4 = ANY(affected_symbols) OR cardinality(affected_symbols) = 0)
                """, sample_date - timedelta(days=7), sample_date, category, symbol) or 0.0
                
                features[feature_name] = float(impact)
        
        return features
    
    async def generate_technical_features(self, symbol: str, sample_date: date) -> Dict[str, Dict]:
        """Generate technical analysis features from price data"""
        
        async with self.pool.acquire() as conn:
            # Get 60 days of price history for technical indicators
            price_data = await conn.fetch("""
                SELECT date, open, high, low, close, adjusted_close, volume
                FROM dev_daily_prices
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC
                LIMIT 60
            """, symbol, sample_date)
            
            if len(price_data) < 20:  # Need minimum data for indicators
                return {'price_features': {}, 'volume_features': {}}
            
            # Convert to DataFrame for easier calculation
            df = pd.DataFrame([dict(row) for row in price_data])
            df = df.sort_values('date').reset_index(drop=True)
            
            # Calculate technical indicators
            price_features = self._calculate_price_indicators(df)
            volume_features = self._calculate_volume_indicators(df)
            
            return {
                'price_features': price_features,
                'volume_features': volume_features
            }
    
    def _calculate_price_indicators(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate price-based technical indicators"""
        try:
            close = df['close'].astype(float)
            df['high'].astype(float)
            df['low'].astype(float)
            
            # Moving averages
            sma_10 = close.rolling(10).mean().iloc[-1] if len(close) >= 10 else close.iloc[-1]
            sma_20 = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else close.iloc[-1]
            sma_50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else close.iloc[-1]
            
            # Exponential moving averages
            ema_12 = close.ewm(span=12).mean().iloc[-1]
            ema_26 = close.ewm(span=26).mean().iloc[-1]
            
            # RSI
            rsi_14 = self._calculate_rsi(close, 14) if len(close) >= 14 else 50.0
            
            # MACD
            macd_line = ema_12 - ema_26
            macd_signal = pd.Series([macd_line]).ewm(span=9).mean().iloc[-1]
            macd = macd_line - macd_signal
            
            # Bollinger Bands
            bb_middle = sma_20
            bb_std = close.rolling(20).std().iloc[-1] if len(close) >= 20 else 0
            bb_upper = bb_middle + (2 * bb_std)
            bb_lower = bb_middle - (2 * bb_std)
            
            # ATR
            atr_14 = self._calculate_atr(df, 14) if len(df) >= 14 else 0.0
            
            # Price momentum
            price_momentum_1d = (close.iloc[-1] / close.iloc[-2] - 1) if len(close) >= 2 else 0.0
            price_momentum_5d = (close.iloc[-1] / close.iloc[-6] - 1) if len(close) >= 6 else 0.0
            price_momentum_20d = (close.iloc[-1] / close.iloc[-21] - 1) if len(close) >= 21 else 0.0
            
            return {
                'sma_10': float(sma_10),
                'sma_20': float(sma_20),
                'sma_50': float(sma_50),
                'ema_12': float(ema_12),
                'ema_26': float(ema_26),
                'rsi_14': float(rsi_14),
                'macd': float(macd),
                'bollinger_upper': float(bb_upper),
                'bollinger_lower': float(bb_lower),
                'bollinger_position': float((close.iloc[-1] - bb_lower) / (bb_upper - bb_lower)) if bb_upper > bb_lower else 0.5,
                'atr_14': float(atr_14),
                'price_momentum_1d': float(price_momentum_1d),
                'price_momentum_5d': float(price_momentum_5d),
                'price_momentum_20d': float(price_momentum_20d)
            }
        except Exception as e:
            logger.error(f"Error calculating price indicators: {e}")
            return {}
    
    def _calculate_volume_indicators(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate volume-based indicators"""
        try:
            volume = df['volume'].astype(float)
            close = df['close'].astype(float)
            
            # Volume moving averages
            volume_sma_10 = volume.rolling(10).mean().iloc[-1] if len(volume) >= 10 else volume.iloc[-1]
            volume_sma_20 = volume.rolling(20).mean().iloc[-1] if len(volume) >= 20 else volume.iloc[-1]
            
            # Relative volume
            relative_volume = volume.iloc[-1] / volume_sma_20 if volume_sma_20 > 0 else 1.0
            
            # Volume momentum
            volume_momentum = (volume.iloc[-1] / volume.iloc[-6] - 1) if len(volume) >= 6 else 0.0
            
            # On Balance Volume
            obv = self._calculate_obv(close, volume)
            
            # Volume Weighted Average Price (simple approximation)
            vwap = (close * volume).sum() / volume.sum() if volume.sum() > 0 else close.iloc[-1]
            
            return {
                'volume_sma_10': float(volume_sma_10),
                'volume_sma_20': float(volume_sma_20),
                'relative_volume': float(relative_volume),
                'volume_momentum': float(volume_momentum),
                'on_balance_volume': float(obv),
                'vwap': float(vwap)
            }
        except Exception as e:
            logger.error(f"Error calculating volume indicators: {e}")
            return {}
    
    def _calculate_rsi(self, prices: pd.Series, window: int) -> float:
        """Calculate RSI indicator"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1] if not np.isnan(rsi.iloc[-1]) else 50.0
    
    def _calculate_atr(self, df: pd.DataFrame, window: int) -> float:
        """Calculate Average True Range"""
        high = df['high'].astype(float)
        low = df['low'].astype(float)
        close = df['close'].astype(float)
        
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = true_range.rolling(window).mean()
        return atr.iloc[-1] if not np.isnan(atr.iloc[-1]) else 0.0
    
    def _calculate_obv(self, close: pd.Series, volume: pd.Series) -> float:
        """Calculate On Balance Volume"""
        obv = 0
        for i in range(1, len(close)):
            if close.iloc[i] > close.iloc[i-1]:
                obv += volume.iloc[i]
            elif close.iloc[i] < close.iloc[i-1]:
                obv -= volume.iloc[i]
        return obv
    
    async def generate_cross_asset_features(self, symbol: str, sample_date: date) -> Dict[str, float]:
        """Generate cross-asset correlation and market features"""
        features = {
            'sector_correlation': None,
            'market_correlation': None,
            'vix_level': None,
            'yield_curve_10y2y': None,
            'dxy_level': None
        }
        
        async with self.pool.acquire() as conn:
            # Get SPY price for market correlation (simplified)
            spy_prices = await conn.fetch("""
                SELECT close FROM dev_daily_prices
                WHERE symbol = 'SPY' AND date <= $1
                ORDER BY date DESC LIMIT 20
            """, sample_date)
            
            symbol_prices = await conn.fetch("""
                SELECT close FROM dev_daily_prices  
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC LIMIT 20
            """, symbol, sample_date)
            
            if len(spy_prices) >= 10 and len(symbol_prices) >= 10:
                spy_returns = np.diff([float(p['close']) for p in spy_prices[::-1]]) / [float(p['close']) for p in spy_prices[::-1]][:-1]
                symbol_returns = np.diff([float(p['close']) for p in symbol_prices[::-1]]) / [float(p['close']) for p in symbol_prices[::-1]][:-1]
                
                if len(spy_returns) > 0 and len(symbol_returns) > 0:
                    correlation = np.corrcoef(symbol_returns, spy_returns)[0, 1]
                    features['market_correlation'] = float(correlation) if not np.isnan(correlation) else 0.0
            
            # Get VIX level (simplified - would need VIX data)
            # For now, calculate implied volatility from price movements
            if len(symbol_prices) >= 20:
                prices = [float(p['close']) for p in symbol_prices]
                returns = np.diff(prices) / prices[:-1]
                volatility = np.std(returns) * np.sqrt(252)  # Annualized
                features['vix_level'] = float(volatility)
        
        return features

    async def generate_target_variables(self, symbol: str, sample_date: date, 
                                      prediction_horizon: int) -> Dict[str, Any]:
        """Generate target variables for the prediction horizon"""
        targets = {}
        
        async with self.pool.acquire() as conn:
            # Get future price data for targets
            future_prices = await conn.fetch("""
                SELECT date, close, adjusted_close, volume
                FROM dev_daily_prices
                WHERE symbol = $1 
                AND date > $2
                AND date <= $3
                ORDER BY date ASC
            """, symbol, sample_date, sample_date + timedelta(days=prediction_horizon + 5))
            
            if len(future_prices) == 0:
                return targets
            
            # Get current price
            current_price_row = await conn.fetchrow("""
                SELECT close, adjusted_close 
                FROM dev_daily_prices
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC LIMIT 1
            """, symbol, sample_date)
            
            if not current_price_row:
                return targets
            
            current_price = float(current_price_row['adjusted_close'])
            
            # Calculate returns for different horizons
            for horizon in [1, 5, 10, 20]:
                if horizon <= len(future_prices):
                    future_price = float(future_prices[horizon-1]['adjusted_close'])
                    return_pct = (future_price / current_price) - 1
                    targets[f'target_return_{horizon}d'] = float(return_pct)
                    
                    # Direction classification
                    if return_pct > 0.02:  # > 2% up
                        targets[f'target_direction_{horizon}d'] = 1
                    elif return_pct < -0.02:  # < -2% down
                        targets[f'target_direction_{horizon}d'] = -1
                    else:  # flat
                        targets[f'target_direction_{horizon}d'] = 0
            
            # Calculate volatility targets
            if len(future_prices) >= 5:
                prices_5d = [float(p['adjusted_close']) for p in future_prices[:5]]
                returns_5d = np.diff(prices_5d) / prices_5d[:-1]
                targets['target_volatility_5d'] = float(np.std(returns_5d))
            
            if len(future_prices) >= 20:
                prices_20d = [float(p['adjusted_close']) for p in future_prices[:20]]
                returns_20d = np.diff(prices_20d) / prices_20d[:-1]
                targets['target_volatility_20d'] = float(np.std(returns_20d))
                
                # Max drawdown
                cumulative = np.cumprod(1 + returns_20d)
                running_max = np.maximum.accumulate(cumulative)
                drawdown = (cumulative - running_max) / running_max
                targets['target_max_drawdown'] = float(np.min(drawdown))
                
                # Sharpe ratio (simplified)
                if np.std(returns_20d) > 0:
                    sharpe = np.mean(returns_20d) / np.std(returns_20d) * np.sqrt(252)
                    targets['target_sharpe_ratio'] = float(sharpe)
                
                # Volatility regime
                vol_20d = targets['target_volatility_20d']
                if vol_20d < 0.01:  # Low vol
                    targets['target_volatility_regime'] = 1
                elif vol_20d > 0.03:  # High vol
                    targets['target_volatility_regime'] = 3
                else:  # Medium vol
                    targets['target_volatility_regime'] = 2
        
        return targets

class MultiModalTrainingDataGenerator:
    """Main class for generating multi-modal training data"""
    
    def __init__(self, db_config: Dict[str, Any], pool_size: int = 20):
        self.db_config = db_config
        self.pool_size = pool_size
        self.pool = None
        self.feature_generator = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.pool = await asyncpg.create_pool(
            host=self.db_config['host'],
            port=self.db_config['port'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            min_size=self.pool_size,
            max_size=self.pool_size * 2,
            server_settings={'jit': 'off'}
        )
        
        self.feature_generator = MultiModalFeatureGenerator(self.pool)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.pool:
            await self.pool.close()
    
    async def generate_training_samples(self, symbols: List[str], 
                                      start_date: date, end_date: date,
                                      prediction_horizons: List[int] = [1, 5, 10, 20]) -> int:
        """Generate training samples for symbols and date range"""
        
        logger.info(f"🚀 Generating training samples for {len(symbols)} symbols")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info(f"🎯 Prediction horizons: {prediction_horizons}")
        
        total_samples = 0
        batch_size = 100
        current_batch = []
        
        for symbol in symbols:
            logger.info(f"📊 Processing {symbol}...")
            
            current_date = start_date
            while current_date <= end_date:
                # Skip weekends
                if current_date.weekday() >= 5:
                    current_date += timedelta(days=1)
                    continue
                
                # Skip if too close to end date (need future data for targets)
                if current_date > end_date - timedelta(days=25):
                    current_date += timedelta(days=1)
                    continue
                
                for horizon in prediction_horizons:
                    try:
                        sample = await self._generate_single_sample(symbol, current_date, horizon)
                        if sample:
                            current_batch.append(sample)
                            
                            if len(current_batch) >= batch_size:
                                inserted = await self._insert_batch(current_batch)
                                total_samples += inserted
                                current_batch = []
                                logger.info(f"📈 Progress: {total_samples:,} samples generated")
                                
                    except Exception as e:
                        logger.error(f"❌ Failed to generate sample for {symbol} on {current_date}, horizon {horizon}: {e}")
                
                current_date += timedelta(days=1)
        
        # Insert remaining samples
        if current_batch:
            inserted = await self._insert_batch(current_batch)
            total_samples += inserted
        
        logger.info(f"🎉 Training data generation completed: {total_samples:,} samples")
        return total_samples
    
    async def _generate_single_sample(self, symbol: str, sample_date: date, 
                                    prediction_horizon: int) -> Optional[TrainingSample]:
        """Generate a single training sample"""
        
        try:
            # Generate all feature types
            news_features = await self.feature_generator.generate_news_features(symbol, sample_date)
            economic_features = await self.feature_generator.generate_economic_event_features(symbol, sample_date)
            technical_features = await self.feature_generator.generate_technical_features(symbol, sample_date)
            cross_asset_features = await self.feature_generator.generate_cross_asset_features(symbol, sample_date)
            target_variables = await self.feature_generator.generate_target_variables(symbol, sample_date, prediction_horizon)
            
            # Check if we have minimum required data
            if not technical_features.get('price_features'):
                return None
            
            # Create training sample
            sample = TrainingSample(
                symbol=symbol,
                sample_date=sample_date,
                prediction_horizon=prediction_horizon,
                price_features=technical_features.get('price_features', {}),
                volume_features=technical_features.get('volume_features', {})
            )
            
            # Set news features
            for key, value in news_features.items():
                if hasattr(sample, key):
                    setattr(sample, key, value)
            
            # Set economic features
            for key, value in economic_features.items():
                if hasattr(sample, key):
                    setattr(sample, key, value)
            
            # Set cross-asset features
            for key, value in cross_asset_features.items():
                if hasattr(sample, key):
                    setattr(sample, key, value)
            
            # Set targets
            for key, value in target_variables.items():
                if hasattr(sample, key):
                    setattr(sample, key, value)
            
            # Calculate sample quality and weight
            sample.sample_quality_score = self._calculate_sample_quality(sample)
            sample.sample_weight = self._calculate_sample_weight(sample)
            sample.is_outlier = self._detect_outlier(sample)
            sample.market_regime = self._determine_market_regime(sample)
            
            return sample
            
        except Exception as e:
            logger.error(f"Error generating sample for {symbol} on {sample_date}: {e}")
            return None
    
    def _calculate_sample_quality(self, sample: TrainingSample) -> float:
        """Calculate quality score for the sample based on data completeness"""
        quality = 1.0
        
        # Penalize for missing news data
        if sample.news_volume_7d == 0:
            quality -= 0.2
        
        # Penalize for missing target data
        target_fields = ['target_return_1d', 'target_return_5d', 'target_return_10d', 'target_return_20d']
        missing_targets = sum(1 for field in target_fields if getattr(sample, field) is None)
        quality -= missing_targets * 0.1
        
        # Penalize for missing technical features
        if not sample.price_features or len(sample.price_features) < 5:
            quality -= 0.3
        
        return max(quality, 0.1)  # Minimum quality
    
    def _calculate_sample_weight(self, sample: TrainingSample) -> float:
        """Calculate training weight for the sample"""
        weight = 1.0
        
        # Higher weight for samples with more news coverage
        if sample.news_volume_7d > 5:
            weight *= 1.2
        elif sample.news_volume_7d > 10:
            weight *= 1.5
        
        # Higher weight for earnings periods
        if sample.earnings_impact_score > 0.1:
            weight *= 1.3
        
        # Lower weight for outliers
        if sample.is_outlier:
            weight *= 0.5
        
        return weight
    
    def _detect_outlier(self, sample: TrainingSample) -> bool:
        """Detect if sample is a statistical outlier"""
        # Simple outlier detection based on extreme returns
        extreme_thresholds = {
            'target_return_1d': 0.15,  # 15% daily return
            'target_return_5d': 0.3,   # 30% weekly return
        }
        
        for field, threshold in extreme_thresholds.items():
            value = getattr(sample, field)
            if value and abs(value) > threshold:
                return True
        
        return False
    
    def _determine_market_regime(self, sample: TrainingSample) -> str:
        """Determine market regime based on features"""
        vix = sample.vix_level
        market_corr = sample.market_correlation
        
        # Simple regime classification
        if vix and vix > 0.4:
            return 'crisis'
        elif market_corr and market_corr > 0.8:
            return 'bull'
        elif market_corr and market_corr < 0.3:
            return 'bear'
        else:
            return 'sideways'
    
    async def _insert_batch(self, samples: List[TrainingSample]) -> int:
        """Insert batch of training samples into database"""
        if not samples:
            return 0
        
        async with self.pool.acquire() as conn:
            try:
                records = []
                for sample in samples:
                    record = (
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
                        json.dumps(sample.price_features or {}),
                        json.dumps(sample.volume_features or {}),
                        json.dumps(sample.market_microstructure or {}),
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
                    )
                    records.append(record)
                
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
                        target_volatility_regime,
                        sample_quality_score, sample_weight, is_outlier, market_regime
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42)
                    ON CONFLICT (symbol, sample_date, prediction_horizon) 
                    DO UPDATE SET
                        news_sentiment_1d = EXCLUDED.news_sentiment_1d,
                        news_sentiment_3d = EXCLUDED.news_sentiment_3d,
                        news_sentiment_7d = EXCLUDED.news_sentiment_7d,
                        updated_at = CURRENT_TIMESTAMP
                """, records)
                
                return len(records)
                
            except Exception as e:
                logger.error(f"❌ Batch insert error: {e}")
                return 0

async def main():
    """Main function for training data generation"""
    parser = argparse.ArgumentParser(description="Multi-Modal Training Data Generator")
    parser.add_argument('--symbols', nargs='+', help='Symbols to generate training data for')
    parser.add_argument('--start_date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--horizons', nargs='+', type=int, default=[1, 5, 10, 20], help='Prediction horizons')
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'postgres'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'dev_password'),
        'database': os.getenv('DB_NAME', 'dev_db')
    }
    
    # Default symbols if not provided
    symbols = args.symbols or [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA',
        'BRK.B', 'JPM', 'JNJ', 'V', 'PG', 'HD', 'MA', 'UNH'
    ]
    
    # Run training data generation
    async with MultiModalTrainingDataGenerator(db_config) as generator:
        total_samples = await generator.generate_training_samples(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            prediction_horizons=args.horizons
        )
        
        logger.info(f"🎉 Generated {total_samples:,} training samples successfully!")

if __name__ == "__main__":
    asyncio.run(main())