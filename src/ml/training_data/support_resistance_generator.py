"""
Support and Resistance Training Data Generator

Generates training data for predicting next-day support and resistance levels
using comprehensive technical analysis and price action features.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Tuple, NamedTuple
from dataclasses import dataclass
from config.environment import Environment
import gin

class SupportResistanceLevel(NamedTuple):
    """Represents a support or resistance level with metadata"""
    level: float
    level_type: str  # 'support' or 'resistance'
    strength: float  # 0-1 confidence score
    tests_count: int  # Number of times level was tested
    volume_at_level: float  # Volume when level was tested
    time_held: float  # Minutes level held
    break_through: bool  # Whether level was broken
    
@dataclass
class TrainingExample:
    """Single training example for support/resistance prediction"""
    symbol: str
    date: date
    # Features (what we know at end of day T)
    features: Dict[str, float]
    # Labels (what happened on day T+1)
    next_day_support_levels: List[SupportResistanceLevel]
    next_day_resistance_levels: List[SupportResistanceLevel]
    next_day_high: float
    next_day_low: float
    next_day_close: float
    next_day_volume: float

@gin.configurable
class SupportResistanceTrainingGenerator:
    """
    Generates training data for support/resistance prediction models.
    
    This system:
    1. Extracts comprehensive technical features from daily/minute data
    2. Identifies actual support/resistance levels from next-day price action
    3. Creates labeled training examples for ML models
    """
    
    def __init__(self, env: Environment = None):
        self.env = env or Environment()
        self.logger = logging.getLogger(__name__)
        
        # Level detection parameters
        self.min_level_strength = 0.3  # Minimum strength to consider a level
        self.level_tolerance_pct = 0.2  # % tolerance for level matching
        self.min_test_volume_ratio = 0.1  # Minimum volume ratio for valid test
        self.min_hold_minutes = 5  # Minimum minutes to hold level
        
    async def generate_training_data(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date,
        min_examples_per_symbol: int = 100
    ) -> List[TrainingExample]:
        """
        Generate training data for support/resistance prediction.
        
        Args:
            symbols: List of stock symbols to generate data for
            start_date: Start date for training data
            end_date: End date for training data
            min_examples_per_symbol: Minimum examples needed per symbol
            
        Returns:
            List of training examples
        """
        self.logger.info(f"Generating support/resistance training data for {len(symbols)} symbols")
        self.logger.info(f"Date range: {start_date} to {end_date}")
        
        training_examples = []
        
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                for symbol in symbols:
                    self.logger.info(f"Processing {symbol}...")
                    
                    symbol_examples = await self._generate_symbol_training_data(
                        conn, symbol, start_date, end_date
                    )
                    
                    if len(symbol_examples) >= min_examples_per_symbol:
                        training_examples.extend(symbol_examples)
                        self.logger.info(f"Added {len(symbol_examples)} examples for {symbol}")
                    else:
                        self.logger.warning(f"Only {len(symbol_examples)} examples for {symbol}, "
                                          f"skipping (need {min_examples_per_symbol})")
        finally:
            await pool.close()
        
        self.logger.info(f"Generated {len(training_examples)} total training examples")
        return training_examples
    
    async def _generate_symbol_training_data(
        self,
        conn,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> List[TrainingExample]:
        """Generate training data for a single symbol"""
        
        # Get daily price data for feature generation
        daily_data = await self._get_daily_price_data(conn, symbol, start_date, end_date)
        if len(daily_data) < 50:  # Need minimum history for indicators
            return []
        
        training_examples = []
        
        # Process each trading day
        for i in range(20, len(daily_data) - 1):  # Need history + next day
            current_date = daily_data.iloc[i]['date']
            next_date = daily_data.iloc[i + 1]['date']
            
            # Generate features from current and historical data
            features = await self._generate_features(
                conn, symbol, daily_data, i, current_date
            )
            
            if features is None:
                continue
            
            # Generate labels from next day's price action
            labels = await self._generate_labels(
                conn, symbol, next_date, daily_data.iloc[i + 1]
            )
            
            if labels is None:
                continue
            
            # Create training example
            example = TrainingExample(
                symbol=symbol,
                date=current_date,
                features=features,
                next_day_support_levels=labels['support_levels'],
                next_day_resistance_levels=labels['resistance_levels'],
                next_day_high=labels['high'],
                next_day_low=labels['low'],
                next_day_close=labels['close'],
                next_day_volume=labels['volume']
            )
            
            training_examples.append(example)
        
        return training_examples
    
    async def _get_daily_price_data(
        self, 
        conn, 
        symbol: str, 
        start_date: date, 
        end_date: date
    ) -> pd.DataFrame:
        """Get daily OHLCV data for a symbol"""
        
        # Try multiple data sources
        queries = [
            f"""
            SELECT date, open, high, low, close, volume
            FROM {self.env.get_table_name('daily_prices_polygon')}
            WHERE symbol = $1 AND date >= $2 AND date <= $3
            ORDER BY date
            """,
            f"""
            SELECT date, open, high, low, close, volume
            FROM {self.env.get_table_name('daily_prices_tiingo')}
            WHERE symbol = $1 AND date >= $2 AND date <= $3
            ORDER BY date
            """,
            f"""
            SELECT date, open, high, low, close, volume
            FROM {self.env.get_table_name('daily_prices')}
            WHERE symbol = $1 AND date >= $2 AND date <= $3
            ORDER BY date
            """
        ]
        
        for query in queries:
            try:
                rows = await conn.fetch(query, symbol, start_date, end_date)
                if rows:
                    df = pd.DataFrame(rows)
                    if len(df) > 0:
                        return df
            except Exception as e:
                continue
        
        return pd.DataFrame()
    
    async def _generate_features(
        self,
        conn,
        symbol: str,
        daily_data: pd.DataFrame,
        current_idx: int,
        current_date: date
    ) -> Optional[Dict[str, float]]:
        """Generate comprehensive features for the current trading day"""
        
        try:
            current_data = daily_data.iloc[:current_idx + 1].copy()
            
            features = {}
            
            # === PRICE ACTION FEATURES ===
            current_price = current_data.iloc[-1]
            features.update(self._price_action_features(current_data, current_price))
            
            # === TECHNICAL INDICATOR FEATURES ===
            features.update(self._technical_indicator_features(current_data))
            
            # === HISTORICAL SUPPORT/RESISTANCE FEATURES ===
            features.update(self._historical_sr_features(current_data))
            
            # === VOLUME FEATURES ===
            features.update(self._volume_features(current_data))
            
            # === VOLATILITY FEATURES ===
            features.update(self._volatility_features(current_data))
            
            # === MARKET STRUCTURE FEATURES ===
            features.update(self._market_structure_features(current_data))
            
            # === INTRADAY FEATURES (if minute data available) ===
            intraday_features = await self._intraday_features(conn, symbol, current_date)
            if intraday_features:
                features.update(intraday_features)
            
            return features
            
        except Exception as e:
            self.logger.warning(f"Error generating features for {symbol} on {current_date}: {e}")
            return None
    
    def _price_action_features(self, data: pd.DataFrame, current: pd.Series) -> Dict[str, float]:
        """Generate price action features"""
        features = {}
        
        # Current price levels
        features['close'] = current['close']
        features['high'] = current['high']
        features['low'] = current['low']
        features['open'] = current['open']
        features['volume'] = current['volume']
        
        # Price ranges and ratios
        features['daily_range'] = (current['high'] - current['low']) / current['close']
        features['body_ratio'] = abs(current['close'] - current['open']) / (current['high'] - current['low'] + 1e-8)
        features['upper_shadow'] = (current['high'] - max(current['open'], current['close'])) / current['close']
        features['lower_shadow'] = (min(current['open'], current['close']) - current['low']) / current['close']
        
        # Recent price changes
        if len(data) >= 5:
            features['return_1d'] = (current['close'] - data.iloc[-2]['close']) / data.iloc[-2]['close']
            features['return_3d'] = (current['close'] - data.iloc[-4]['close']) / data.iloc[-4]['close']
            features['return_5d'] = (current['close'] - data.iloc[-6]['close']) / data.iloc[-6]['close']
        
        return features
    
    def _technical_indicator_features(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate technical indicator features"""
        features = {}
        
        closes = data['close'].values
        highs = data['high'].values
        lows = data['low'].values
        volumes = data['volume'].values
        
        # Moving averages
        for period in [5, 10, 20, 50]:
            if len(closes) >= period:
                ma = np.mean(closes[-period:])
                features[f'ma_{period}'] = closes[-1] / ma - 1  # Distance from MA
                features[f'ma_{period}_slope'] = (ma - np.mean(closes[-period-5:-5])) / np.mean(closes[-period-5:-5]) if len(closes) >= period + 5 else 0
        
        # RSI
        if len(closes) >= 14:
            rsi = self._calculate_rsi(closes, 14)
            features['rsi_14'] = rsi
            features['rsi_oversold'] = 1.0 if rsi < 30 else 0.0
            features['rsi_overbought'] = 1.0 if rsi > 70 else 0.0
        
        # Bollinger Bands
        if len(closes) >= 20:
            bb_upper, bb_middle, bb_lower = self._calculate_bollinger_bands(closes, 20, 2)
            features['bb_position'] = (closes[-1] - bb_lower) / (bb_upper - bb_lower)
            features['bb_width'] = (bb_upper - bb_lower) / bb_middle
        
        # MACD
        if len(closes) >= 26:
            macd, signal = self._calculate_macd(closes)
            features['macd'] = macd
            features['macd_signal'] = signal
            features['macd_histogram'] = macd - signal
        
        return features
    
    def _historical_sr_features(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate features from historical support/resistance levels"""
        features = {}
        
        # Recent pivot points
        highs = data['high'].values
        lows = data['low'].values
        closes = data['close'].values
        current_price = closes[-1]
        
        # Find recent pivot highs and lows
        pivot_highs = []
        pivot_lows = []
        
        lookback = min(20, len(highs) - 2)
        for i in range(2, lookback):
            # Pivot high: higher than neighbors
            if highs[-(i+1)] > highs[-i] and highs[-(i+1)] > highs[-(i+2)]:
                pivot_highs.append(highs[-(i+1)])
            
            # Pivot low: lower than neighbors
            if lows[-(i+1)] < lows[-i] and lows[-(i+1)] < lows[-(i+2)]:
                pivot_lows.append(lows[-(i+1)])
        
        # Distance to nearest resistance/support
        if pivot_highs:
            nearest_resistance = min([h for h in pivot_highs if h > current_price], default=None)
            if nearest_resistance:
                features['distance_to_resistance'] = (nearest_resistance - current_price) / current_price
            
        if pivot_lows:
            nearest_support = max([l for l in pivot_lows if l < current_price], default=None)
            if nearest_support:
                features['distance_to_support'] = (current_price - nearest_support) / current_price
        
        # Psychological levels (round numbers)
        round_levels = [round(current_price / 5) * 5, round(current_price / 10) * 10]
        nearest_round = min(round_levels, key=lambda x: abs(x - current_price))
        features['distance_to_round_number'] = abs(current_price - nearest_round) / current_price
        
        return features
    
    def _volume_features(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate volume-based features"""
        features = {}
        
        volumes = data['volume'].values
        current_volume = volumes[-1]
        
        # Volume ratios
        if len(volumes) >= 20:
            avg_volume_20 = np.mean(volumes[-20:])
            features['volume_ratio_20d'] = current_volume / avg_volume_20
            
        if len(volumes) >= 5:
            avg_volume_5 = np.mean(volumes[-5:])
            features['volume_ratio_5d'] = current_volume / avg_volume_5
        
        # Volume trend
        if len(volumes) >= 10:
            recent_avg = np.mean(volumes[-5:])
            older_avg = np.mean(volumes[-10:-5])
            features['volume_trend'] = (recent_avg - older_avg) / older_avg
        
        return features
    
    def _volatility_features(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate volatility features"""
        features = {}
        
        # True Range and ATR
        if len(data) >= 14:
            tr_values = []
            for i in range(1, len(data)):
                current = data.iloc[i]
                previous = data.iloc[i-1]
                
                tr = max(
                    current['high'] - current['low'],
                    abs(current['high'] - previous['close']),
                    abs(current['low'] - previous['close'])
                )
                tr_values.append(tr)
            
            if tr_values:
                atr = np.mean(tr_values[-14:])
                features['atr'] = atr / data.iloc[-1]['close']  # Normalized ATR
        
        # Price volatility
        if len(data) >= 20:
            returns = data['close'].pct_change().dropna()
            features['volatility_20d'] = returns.tail(20).std() * np.sqrt(252)  # Annualized
        
        return features
    
    def _market_structure_features(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate market structure features"""
        features = {}
        
        # Trend direction
        if len(data) >= 20:
            closes = data['close'].values
            features['trend_strength'] = (closes[-1] - closes[-20]) / closes[-20]
            
            # Higher highs, higher lows pattern
            recent_highs = data['high'].tail(10).values
            recent_lows = data['low'].tail(10).values
            
            hh_count = sum(1 for i in range(1, len(recent_highs)) if recent_highs[i] > recent_highs[i-1])
            hl_count = sum(1 for i in range(1, len(recent_lows)) if recent_lows[i] > recent_lows[i-1])
            
            features['higher_highs_ratio'] = hh_count / (len(recent_highs) - 1)
            features['higher_lows_ratio'] = hl_count / (len(recent_lows) - 1)
        
        return features
    
    async def _intraday_features(
        self, 
        conn, 
        symbol: str, 
        trade_date: date
    ) -> Optional[Dict[str, float]]:
        """Generate features from intraday minute data"""
        
        try:
            # Get minute data for the current day
            query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {self.env.get_table_name('minute_bars')}
            WHERE symbol = $1 
              AND DATE(timestamp) = $2
            ORDER BY timestamp
            """
            
            rows = await conn.fetch(query, symbol, trade_date)
            if not rows:
                return None
            
            minute_data = pd.DataFrame(rows)
            features = {}
            
            # Opening gap
            if len(minute_data) > 0:
                first_bar = minute_data.iloc[0]
                prev_close = await self._get_previous_close(conn, symbol, trade_date)
                if prev_close:
                    features['opening_gap'] = (first_bar['open'] - prev_close) / prev_close
            
            # Intraday range and momentum
            if len(minute_data) > 60:  # Need sufficient data
                highs = minute_data['high'].values
                lows = minute_data['low'].values
                closes = minute_data['close'].values
                volumes = minute_data['volume'].values
                
                features['intraday_range'] = (max(highs) - min(lows)) / closes[0]
                features['morning_momentum'] = (closes[59] - closes[0]) / closes[0]  # First hour
                
                # Volume distribution
                total_volume = sum(volumes)
                if total_volume > 0:
                    morning_volume = sum(volumes[:60])  # First hour
                    features['morning_volume_ratio'] = morning_volume / total_volume
            
            return features
            
        except Exception as e:
            self.logger.warning(f"Error getting intraday features for {symbol} on {trade_date}: {e}")
            return None
    
    async def _get_previous_close(self, conn, symbol: str, trade_date: date) -> Optional[float]:
        """Get previous trading day's close price"""
        
        query = f"""
        SELECT close 
        FROM {self.env.get_table_name('daily_prices_polygon')}
        WHERE symbol = $1 AND date < $2
        ORDER BY date DESC
        LIMIT 1
        """
        
        try:
            row = await conn.fetchrow(query, symbol, trade_date)
            return row['close'] if row else None
        except:
            return None
    
    async def _generate_labels(
        self,
        conn,
        symbol: str,
        next_date: date,
        next_day_daily: pd.Series
    ) -> Optional[Dict]:
        """Generate labels from next day's actual price action"""
        
        try:
            # Get minute-level data for next day to identify actual support/resistance
            minute_data = await self._get_minute_data(conn, symbol, next_date)
            
            if minute_data is None or len(minute_data) < 10:
                return None
            
            # Identify support and resistance levels from minute data
            support_levels = self._identify_support_levels(minute_data)
            resistance_levels = self._identify_resistance_levels(minute_data)
            
            return {
                'support_levels': support_levels,
                'resistance_levels': resistance_levels,
                'high': next_day_daily['high'],
                'low': next_day_daily['low'],
                'close': next_day_daily['close'],
                'volume': next_day_daily['volume']
            }
            
        except Exception as e:
            self.logger.warning(f"Error generating labels for {symbol} on {next_date}: {e}")
            return None
    
    async def _get_minute_data(self, conn, symbol: str, trade_date: date) -> Optional[pd.DataFrame]:
        """Get minute-level data for a specific trading day"""
        
        query = f"""
        SELECT timestamp, open, high, low, close, volume
        FROM {self.env.get_table_name('minute_bars')}
        WHERE symbol = $1 
          AND DATE(timestamp) = $2
        ORDER BY timestamp
        """
        
        try:
            rows = await conn.fetch(query, symbol, trade_date)
            if rows:
                return pd.DataFrame(rows)
        except Exception as e:
            self.logger.warning(f"Error getting minute data for {symbol} on {trade_date}: {e}")
        
        return None
    
    def _identify_support_levels(self, minute_data: pd.DataFrame) -> List[SupportResistanceLevel]:
        """Identify support levels from minute-level price action"""
        
        levels = []
        lows = minute_data['low'].values
        volumes = minute_data['volume'].values
        timestamps = minute_data['timestamp'].values
        
        # Find local lows that acted as support
        for i in range(2, len(lows) - 2):
            current_low = lows[i]
            
            # Check if this is a local low
            if (lows[i] <= lows[i-1] and lows[i] <= lows[i-2] and 
                lows[i] <= lows[i+1] and lows[i] <= lows[i+2]):
                
                # Count tests of this level
                tests = self._count_level_tests(lows, volumes, current_low, 'support')
                
                if tests['count'] >= 2:  # Minimum 2 tests to be considered support
                    strength = min(tests['strength'], 1.0)
                    
                    if strength >= self.min_level_strength:
                        level = SupportResistanceLevel(
                            level=current_low,
                            level_type='support',
                            strength=strength,
                            tests_count=tests['count'],
                            volume_at_level=tests['total_volume'],
                            time_held=tests['hold_time'],
                            break_through=tests['broken']
                        )
                        levels.append(level)
        
        # Sort by strength and return top levels
        levels.sort(key=lambda x: x.strength, reverse=True)
        return levels[:5]  # Return top 5 support levels
    
    def _identify_resistance_levels(self, minute_data: pd.DataFrame) -> List[SupportResistanceLevel]:
        """Identify resistance levels from minute-level price action"""
        
        levels = []
        highs = minute_data['high'].values
        volumes = minute_data['volume'].values
        
        # Find local highs that acted as resistance
        for i in range(2, len(highs) - 2):
            current_high = highs[i]
            
            # Check if this is a local high
            if (highs[i] >= highs[i-1] and highs[i] >= highs[i-2] and 
                highs[i] >= highs[i+1] and highs[i] >= highs[i+2]):
                
                # Count tests of this level
                tests = self._count_level_tests(highs, volumes, current_high, 'resistance')
                
                if tests['count'] >= 2:  # Minimum 2 tests to be considered resistance
                    strength = min(tests['strength'], 1.0)
                    
                    if strength >= self.min_level_strength:
                        level = SupportResistanceLevel(
                            level=current_high,
                            level_type='resistance',
                            strength=strength,
                            tests_count=tests['count'],
                            volume_at_level=tests['total_volume'],
                            time_held=tests['hold_time'],
                            break_through=tests['broken']
                        )
                        levels.append(level)
        
        # Sort by strength and return top levels
        levels.sort(key=lambda x: x.strength, reverse=True)
        return levels[:5]  # Return top 5 resistance levels
    
    def _count_level_tests(
        self, 
        prices: np.ndarray, 
        volumes: np.ndarray, 
        level: float, 
        level_type: str
    ) -> Dict:
        """Count how many times a price level was tested"""
        
        tolerance = level * self.level_tolerance_pct / 100
        
        tests_count = 0
        total_volume = 0
        hold_time = 0
        broken = False
        
        in_test = False
        test_start_idx = None
        
        for i, price in enumerate(prices):
            if level_type == 'support':
                # For support, price comes down to test the level
                at_level = abs(price - level) <= tolerance and price <= level * (1 + self.level_tolerance_pct/100)
                breaks_level = price < level * (1 - self.level_tolerance_pct/100)
            else:  # resistance
                # For resistance, price comes up to test the level
                at_level = abs(price - level) <= tolerance and price >= level * (1 - self.level_tolerance_pct/100)
                breaks_level = price > level * (1 + self.level_tolerance_pct/100)
            
            if at_level and not in_test:
                # Start of a new test
                in_test = True
                test_start_idx = i
                
            elif in_test and not at_level:
                # End of test
                if test_start_idx is not None:
                    test_duration = i - test_start_idx
                    if test_duration >= self.min_hold_minutes:
                        tests_count += 1
                        total_volume += sum(volumes[test_start_idx:i])
                        hold_time += test_duration
                
                in_test = False
                test_start_idx = None
            
            if breaks_level:
                broken = True
        
        # Calculate strength based on tests, volume, and hold time
        volume_strength = min(total_volume / (np.mean(volumes) * tests_count + 1e-8), 2.0) / 2.0
        time_strength = min(hold_time / (tests_count * 10), 1.0)  # Normalize to reasonable hold time
        test_strength = min(tests_count / 5.0, 1.0)  # More tests = stronger
        
        overall_strength = (volume_strength + time_strength + test_strength) / 3.0
        
        return {
            'count': tests_count,
            'total_volume': total_volume,
            'hold_time': hold_time,
            'broken': broken,
            'strength': overall_strength
        }
    
    # Technical indicator calculation methods
    def _calculate_rsi(self, prices: np.ndarray, period: int = 14) -> float:
        """Calculate RSI"""
        if len(prices) < period + 1:
            return 50.0
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_bollinger_bands(self, prices: np.ndarray, period: int = 20, std_dev: float = 2) -> Tuple[float, float, float]:
        """Calculate Bollinger Bands"""
        if len(prices) < period:
            return prices[-1], prices[-1], prices[-1]
        
        sma = np.mean(prices[-period:])
        std = np.std(prices[-period:])
        
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        
        return upper, sma, lower
    
    def _calculate_macd(self, prices: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[float, float]:
        """Calculate MACD"""
        if len(prices) < slow:
            return 0.0, 0.0
        
        # Simple approximation of EMA with SMA
        ema_fast = np.mean(prices[-fast:])
        ema_slow = np.mean(prices[-slow:])
        
        macd = ema_fast - ema_slow
        
        # Simple signal line approximation
        if len(prices) >= slow + signal:
            recent_macds = []
            for i in range(signal):
                if len(prices) >= slow + i:
                    f = np.mean(prices[-(fast + i):len(prices) - i]) if i > 0 else np.mean(prices[-fast:])
                    s = np.mean(prices[-(slow + i):len(prices) - i]) if i > 0 else np.mean(prices[-slow:])
                    recent_macds.append(f - s)
            signal_line = np.mean(recent_macds)
        else:
            signal_line = macd
        
        return macd, signal_line


async def main():
    """Main function to test training data generation"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate support/resistance training data")
    parser.add_argument("--symbols", nargs="+", default=["AAPL", "MSFT", "GOOGL"],
                       help="Symbols to generate data for")
    parser.add_argument("--start-date", type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                       default=date(2023, 1, 1), help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                       default=date(2023, 12, 31), help="End date (YYYY-MM-DD)")
    parser.add_argument("--min-examples", type=int, default=50,
                       help="Minimum examples per symbol")
    parser.add_argument("--output-file", help="Output file for training data")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Generate training data
    generator = SupportResistanceTrainingGenerator()
    
    training_examples = await generator.generate_training_data(
        symbols=args.symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        min_examples_per_symbol=args.min_examples
    )
    
    print(f"Generated {len(training_examples)} training examples")
    
    # Save if requested
    if args.output_file:
        import pickle
        with open(args.output_file, 'wb') as f:
            pickle.dump(training_examples, f)
        print(f"Training data saved to: {args.output_file}")
    
    # Print some statistics
    if training_examples:
        example = training_examples[0]
        print(f"\nExample features ({len(example.features)}): {list(example.features.keys())[:10]}...")
        print(f"Example support levels: {len(example.next_day_support_levels)}")
        print(f"Example resistance levels: {len(example.next_day_resistance_levels)}")


if __name__ == "__main__":
    asyncio.run(main())