"""
TimeSeriesSequenceTrainingGenerator - Multi-timeframe sequence-based training data generation.

This module generates training data using the universe state builder infrastructure,
supporting multiple timeframes and sequence-based features for advanced ML models.
"""

import pandas as pd
import numpy as np
import logging
import asyncio
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
import gin

# Optional imports - will be None if not available
try:
    from domains.trading.services.state.universe_state_manager import UniverseStateManager
except ImportError:
    UniverseStateManager = None

try:
    from core.business.calendars.time_duration import TimeDuration
except ImportError:
    TimeDuration = None

try:
    from core.platform.config.environment import Environment
except ImportError:
    Environment = None




@gin.configurable
class TrainingDataConfig:
    """Configuration for training data generation."""

    def __init__(self,
                 base_interval_minutes: int = 1,
                 training_interval_minutes: int = 60,
                 timeframes: Optional[List[str]] = None,
                 feature_types: Optional[List[str]] = None,
                 signal_names: Optional[List[str]] = None):
        """
        Initialize training data configuration.

        Args:
            base_interval_minutes: Base data collection interval (1 minute)
            training_interval_minutes: Training data generation interval (60 minutes)
            timeframes: List of timeframes to generate features for
            feature_types: Types of features to extract
            signal_names: List of technical indicator signal names to retrieve from UniverseStateManager
        """
        self.base_interval_minutes = base_interval_minutes
        self.training_interval_minutes = training_interval_minutes

        self.timeframes = timeframes or ['1m', '5m', '15m', '1h', '1d', '1w', '1M']

        self.feature_types = feature_types or [
            'ohlcv',           # Open, High, Low, Close, Volume
            'returns',         # Price returns
            'volatility',      # Volatility measures
            'volume_profile',  # Volume-based features
            'technical',       # Technical indicators (computed locally)
            'indicators',      # Pre-computed indicators from IndicatorBuilder (pldot, envelope_top, etc.)
            'support_resistance', # Support/resistance level analysis and tests
            'market_structure' # Support/resistance, trends
        ]

        self.signal_names = signal_names or [
            'etop',           # Envelope top indicator
            'ebot',           # Envelope bottom indicator
            'pldot',          # Price/location dot indicator
            'envelope_top',   # Envelope top (alternative name)
            'envelope_bot',   # Envelope bottom (alternative name)
            'z1b',            # Z1 bottom indicator
            'z2b',            # Z2 bottom indicator
            'z5t',            # Z5 top indicator
            'z6t',            # Z6 top indicator
            'sma_20',         # Simple moving average 20
            'ema_12',         # Exponential moving average 12
            'rsi_14',         # Relative strength index 14
            'macd_line',      # MACD line
            'macd_signal',    # MACD signal line
            'bb_upper',       # Bollinger band upper
            'bb_lower',       # Bollinger band lower
            'bb_middle'       # Bollinger band middle
        ]


@gin.configurable
class MultiTimeframeFeatureExtractor:
    """Extract features across multiple timeframes."""

    def __init__(self, config: TrainingDataConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Initialize S/R feature extractor if support_resistance is in feature_types
        if 'support_resistance' in config.feature_types:
            try:
                from domains.ml.services.training_data.features.support_resistance_features import (
                    SupportResistanceFeatureExtractor
                )
                self.sr_extractor = SupportResistanceFeatureExtractor()
                self.logger.info("S/R feature extractor initialized")
            except ImportError as e:
                self.logger.warning(f"Could not import S/R feature extractor: {e}")
                self.sr_extractor = None
        else:
            self.sr_extractor = None

    def extract_ohlcv_features(self, data: pd.DataFrame, timeframe: str) -> Dict[str, float]:
        """Extract OHLCV features for a given timeframe."""
        if data.empty:
            return {}

        latest = data.iloc[-1]
        features = {
            f'{timeframe}_open': float(latest.get('open', np.nan)),
            f'{timeframe}_high': float(latest.get('high', np.nan)),
            f'{timeframe}_low': float(latest.get('low', np.nan)),
            f'{timeframe}_close': float(latest.get('close', np.nan)),
            f'{timeframe}_volume': float(latest.get('volume', np.nan)),
        }

        # Add derived features
        if not np.isnan(features[f'{timeframe}_high']) and not np.isnan(features[f'{timeframe}_low']):
            features[f'{timeframe}_range'] = features[f'{timeframe}_high'] - features[f'{timeframe}_low']

        if not np.isnan(features[f'{timeframe}_close']) and features[f'{timeframe}_close'] > 0:
            features[f'{timeframe}_range_pct'] = features.get(f'{timeframe}_range', 0) / features[f'{timeframe}_close']

        return features

    def extract_technical_indicators(self, data: pd.DataFrame, timeframe: str) -> Dict[str, float]:
        """Extract technical indicators computed by IndicatorBuilder."""
        if data.empty:
            return {}

        latest = data.iloc[-1]
        features = {}

        # Use configured signal names from config instead of hardcoded lists
        for indicator in self.config.signal_names:
            if indicator in latest:
                value = latest.get(indicator)
                if pd.notna(value):
                    features[f'{timeframe}_{indicator}'] = float(value)
                else:
                    features[f'{timeframe}_{indicator}'] = np.nan

        return features

    def extract_returns_features(self, data: pd.DataFrame, timeframe: str) -> Dict[str, float]:
        """Extract return-based features."""
        if len(data) < 2:
            return {}

        closes = data['close'].dropna()
        if len(closes) < 2:
            return {}

        # Simple returns
        returns = closes.pct_change().dropna()

        features = {}
        if len(returns) > 0:
            features[f'{timeframe}_return_1'] = float(returns.iloc[-1])

        if len(returns) >= 5:
            features[f'{timeframe}_return_5'] = float(returns.tail(5).mean())
            features[f'{timeframe}_volatility_5'] = float(returns.tail(5).std())

        if len(returns) >= 20:
            features[f'{timeframe}_return_20'] = float(returns.tail(20).mean())
            features[f'{timeframe}_volatility_20'] = float(returns.tail(20).std())

        return features

    def extract_volume_features(self, data: pd.DataFrame, timeframe: str) -> Dict[str, float]:
        """Extract volume-based features including Volume Profile analysis."""
        if data.empty or 'volume' not in data.columns:
            return {}

        volumes = data['volume'].dropna()
        if len(volumes) == 0:
            return {}

        features = {
            f'{timeframe}_volume_latest': float(volumes.iloc[-1]),
        }

        if len(volumes) >= 5:
            vol_mean_5 = volumes.tail(5).mean()
            features[f'{timeframe}_volume_ratio_5'] = float(volumes.iloc[-1] / vol_mean_5) if vol_mean_5 > 0 else 1.0

        # Volume Profile features
        if len(data) >= 20 and all(col in data.columns for col in ['open', 'high', 'low', 'close', 'volume']):
            try:
                from signals.indicator import VolumeProfile

                # Create InstrumentInterval-compatible objects for Volume Profile
                intervals = []
                for _, row in data.tail(20).iterrows():
                    # Create an object with required attributes matching InstrumentInterval
                    class IntervalData:
                        def __init__(self, row):
                            self.open = float(row['open'])
                            self.high = float(row['high'])
                            self.low = float(row['low'])
                            self.close = float(row['close'])
                            self.volume = float(row['volume'])
                            self.traded_volume = float(row['volume'])  # VolumeProfile expects this field name
                            self.status = 'ok'  # Required by VolumeProfile

                    intervals.append(IntervalData(row))

                # Calculate Volume Profile
                vp = VolumeProfile(period=20, bin_count=30)
                vp.update(intervals)

                # Extract Volume Profile features
                if vp.latest_poc is not None:
                    features[f'{timeframe}_volume_profile_poc'] = float(vp.latest_poc)

                if vp.latest_val is not None and vp.latest_vah is not None:
                    features[f'{timeframe}_volume_profile_val'] = float(vp.latest_val)
                    features[f'{timeframe}_volume_profile_vah'] = float(vp.latest_vah)
                    features[f'{timeframe}_volume_profile_va_range'] = float(vp.latest_vah - vp.latest_val)

                    # Current price relative to Volume Profile levels
                    current_price = float(data['close'].iloc[-1])
                    features[f'{timeframe}_volume_profile_price_vs_poc'] = float(current_price - vp.latest_poc) if vp.latest_poc else 0.0
                    features[f'{timeframe}_volume_profile_price_vs_val'] = float(current_price - vp.latest_val)
                    features[f'{timeframe}_volume_profile_price_vs_vah'] = float(current_price - vp.latest_vah)

                    # Price position within Value Area (0.0 = at VAL, 1.0 = at VAH)
                    va_range = vp.latest_vah - vp.latest_val
                    if va_range > 0:
                        va_position = (current_price - vp.latest_val) / va_range
                        features[f'{timeframe}_volume_profile_va_position'] = float(max(0.0, min(1.0, va_position)))

            except ImportError:
                # Volume Profile not available, skip these features
                pass
            except Exception as e:
                # Log error but continue with other features
                pass

        if len(volumes) >= 20:
            vol_mean_20 = volumes.tail(20).mean()
            features[f'{timeframe}_volume_ratio_20'] = float(volumes.iloc[-1] / vol_mean_20) if vol_mean_20 > 0 else 1.0

        return features

    def extract_technical_features(self, data: pd.DataFrame, timeframe: str) -> Dict[str, float]:
        """Extract technical indicator features."""
        if len(data) < 14:  # Need minimum data for most indicators
            return {}

        closes = data['close'].dropna()
        highs = data['high'].dropna()
        lows = data['low'].dropna()

        if len(closes) < 14:
            return {}

        features = {}

        # Simple moving averages
        for period in [5, 10, 20]:
            if len(closes) >= period:
                sma = closes.tail(period).mean()
                features[f'{timeframe}_sma_{period}'] = float(sma)
                features[f'{timeframe}_sma_{period}_ratio'] = float(closes.iloc[-1] / sma) if sma > 0 else 1.0

        # RSI (simplified)
        if len(closes) >= 14:
            delta = closes.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            features[f'{timeframe}_rsi'] = float(rsi.iloc[-1]) if not np.isnan(rsi.iloc[-1]) else 50.0

        return features

    def extract_support_resistance_features(self, data: pd.DataFrame, timeframe: str) -> Dict[str, float]:
        """Extract support/resistance features using post-facto analysis."""
        if not self.sr_extractor or data.empty:
            # Return empty features if no S/R extractor or no data
            return {
                f'{timeframe}_support_distance': 0.1,
                f'{timeframe}_support_strength': 0.0,
                f'{timeframe}_resistance_distance': 0.1,
                f'{timeframe}_resistance_strength': 0.0,
                f'{timeframe}_recent_tests': 0,
                f'{timeframe}_tests_confidence': 0.0,
                f'{timeframe}_tests_volume_spike': 1.0,
                f'{timeframe}_hold_strong_tests': 0,
                f'{timeframe}_break_clean_tests': 0,
                f'{timeframe}_penetration_tests': 0,
                f'{timeframe}_sr_level_density': 0,
                f'{timeframe}_near_support': 0.0,
                f'{timeframe}_near_resistance': 0.0,
            }

        try:
            return self.sr_extractor.extract_sr_features(data, timeframe)
        except Exception as e:
            self.logger.warning(f"Error extracting S/R features for {timeframe}: {e}")
            return {
                f'{timeframe}_support_distance': 0.1,
                f'{timeframe}_support_strength': 0.0,
                f'{timeframe}_resistance_distance': 0.1,
                f'{timeframe}_resistance_strength': 0.0,
                f'{timeframe}_recent_tests': 0,
                f'{timeframe}_tests_confidence': 0.0,
                f'{timeframe}_tests_volume_spike': 1.0,
                f'{timeframe}_hold_strong_tests': 0,
                f'{timeframe}_break_clean_tests': 0,
                f'{timeframe}_penetration_tests': 0,
                f'{timeframe}_sr_level_density': 0,
                f'{timeframe}_near_support': 0.0,
                f'{timeframe}_near_resistance': 0.0,
            }

    def extract_all_features(self, data: pd.DataFrame, timeframe: str) -> Dict[str, float]:
        """Extract all configured feature types for a timeframe."""
        all_features = {}

        for feature_type in self.config.feature_types:
            if feature_type == 'ohlcv':
                all_features.update(self.extract_ohlcv_features(data, timeframe))
            elif feature_type == 'returns':
                all_features.update(self.extract_returns_features(data, timeframe))
            elif feature_type == 'volume_profile':
                all_features.update(self.extract_volume_features(data, timeframe))
            elif feature_type == 'technical':
                all_features.update(self.extract_technical_features(data, timeframe))
            elif feature_type == 'indicators':
                # Extract pre-computed indicators from IndicatorBuilder (via UniverseStateManager)
                all_features.update(self.extract_technical_indicators(data, timeframe))
            elif feature_type == 'support_resistance':
                # Extract post-facto S/R features
                all_features.update(self.extract_support_resistance_features(data, timeframe))

        # Always include technical indicators from UniverseStateManager if available
        all_features.update(self.extract_technical_indicators(data, timeframe))

        return all_features


@gin.configurable
class SequenceWindowBuilder:
    """Build sequence windows with lag/lead capabilities."""

    def __init__(self, config: TrainingDataConfig, universe_manager: UniverseStateManager):
        self.config = config
        self.universe_manager = universe_manager
        self.feature_extractor = MultiTimeframeFeatureExtractor(config)
        self.logger = logging.getLogger(__name__)

    async def get_timeframe_data(self, instrument_id: int, center_datetime: datetime,
                          timeframe: str, is_future: bool = False) -> Dict[str, float]:
        """
        Get current data point for a specific timeframe at center_datetime.

        Args:
            instrument_id: Target instrument
            center_datetime: Center datetime for the data point
            timeframe: Timeframe string (e.g., '5m', '1h', '1d')
            is_future: Whether to get future data (lead) or current data (lag)
        """
        try:
            if is_future:
                # Get current future data point (1 interval ahead)
                data_df = self.universe_manager.get_lead_prices(instrument_id, center_datetime, 1)
            else:
                # Get current historical data point
                ohlcv_df = self.universe_manager.get_lag_prices(instrument_id, center_datetime, 1)

                # Get technical indicators for current point
                signals_df = await self.universe_manager.get_lagged_signals(
                    instrument_id=instrument_id,
                    cur_datetime=center_datetime,
                    lag_periods=1,
                    time_interval=timeframe,
                    signal_names=self.config.signal_names
                )

                # Merge OHLCV and signals data
                if not ohlcv_df.empty and not signals_df.empty:
                    data_df = ohlcv_df.copy()

                    # Add technical indicators columns
                    for col in signals_df.columns:
                        if col != 'timestamp':
                            signal_col = col.replace('_value', '').replace('_status', '')
                            if '_value' in col:
                                data_df[signal_col] = signals_df[col].iloc[-1] if len(signals_df) >= 1 else np.nan
                else:
                    data_df = ohlcv_df

            if data_df.empty:
                return {}

            # Extract features for the single data point
            single_point_features = self.feature_extractor.extract_all_features(
                data_df, timeframe
            )

            return single_point_features

        except Exception as e:
            self.logger.warning(f"Failed to get {timeframe} data for instrument {instrument_id}: {e}")
            return {}

    async def build_timeframe_features(self, instrument_id: int, prediction_timestamp: datetime) -> Dict[str, Dict[str, float]]:
        """Build single-point features for all configured timeframes."""
        timeframe_features = {}

        # Build current features for each timeframe
        for timeframe in self.config.timeframes:
            features = await self.get_timeframe_data(
                instrument_id, prediction_timestamp, timeframe, is_future=False
            )
            timeframe_features[timeframe] = features

        return timeframe_features

    async def build_prediction_targets(self, instrument_id: int, prediction_timestamp: datetime) -> Dict[str, Dict[str, float]]:
        """Build single-point prediction targets for configured timeframes."""
        targets = {}

        # Build future targets for each timeframe (single point)
        for timeframe in self.config.timeframes:
            target_features = await self.get_timeframe_data(
                instrument_id, prediction_timestamp, timeframe, is_future=True
            )
            targets[f'future_{timeframe}'] = target_features

        return targets


@gin.configurable
class TimeSeriesSequenceTrainingGenerator:
    """
    Multi-timeframe sequence-based training data generator using universe state infrastructure.

    This generator leverages UniverseStateManager for efficient multi-timeframe data access
    and generates training examples suitable for sequence-based ML models.
    """

    def __init__(self,
                 env: Optional[Environment] = None,
                 config: Optional[TrainingDataConfig] = None,
                 universe_manager: Optional[UniverseStateManager] = None):
        """
        Initialize the training data generator.

        Args:
            env: Environment configuration
            config: Training data configuration
            universe_manager: Universe state manager for data access
        """
        # Handle optional imports gracefully
        if env is None and Environment is not None:
            self.env = Environment()
        else:
            self.env = env

        self.config = config or TrainingDataConfig()

        if universe_manager is None and UniverseStateManager is not None and self.env is not None:
            self.universe_manager = UniverseStateManager(env=self.env)
        else:
            self.universe_manager = universe_manager

        # Initialize components only if dependencies are available
        if self.universe_manager is not None:
            self.sequence_builder = SequenceWindowBuilder(self.config, self.universe_manager)
        else:
            self.sequence_builder = None

        self.feature_extractor = MultiTimeframeFeatureExtractor(self.config)

        self.logger = logging.getLogger(__name__)

        # Cache for symbol to instrument_id mapping
        self._symbol_to_id_cache = {}

    async def get_instrument_id(self, symbol: str) -> Optional[int]:
        """Get instrument_id for a symbol, with caching."""
        if symbol in self._symbol_to_id_cache:
            return self._symbol_to_id_cache[symbol]

        # TODO: Implement symbol to instrument_id lookup using DAO
        # For now, use hardcoded mapping for AAPL/TSLA
        symbol_mapping = {'AAPL': 1, 'TSLA': 6}
        instrument_id = symbol_mapping.get(symbol.upper())

        if instrument_id:
            self._symbol_to_id_cache[symbol] = instrument_id

        return instrument_id

    def generate_base_features(self, instrument_id: int, prediction_timestamp: datetime) -> Dict[str, float]:
        """Generate base scalar features for the prediction timestamp."""
        prediction_date = prediction_timestamp.date()

        # Get recent data for base features
        try:
            recent_data = self.universe_manager.get_lag_prices(instrument_id, prediction_date, 5)
            base_features = self.feature_extractor.extract_all_features(recent_data, 'base')
        except Exception as e:
            self.logger.warning(f"Failed to generate base features for instrument {instrument_id}: {e}")
            base_features = {}

        return base_features

    def generate_timeframe_features(self, instrument_id: int, prediction_timestamp: datetime) -> Dict[str, Dict[str, float]]:
        """Generate aggregated features across all timeframes."""
        prediction_date = prediction_timestamp.date()
        timeframe_features = {}

        for timeframe in self.config.timeframes:
            try:
                # Get appropriate window size for each timeframe
                window_size = {
                    '1m': 60, '5m': 12, '15m': 4, '1h': 24,
                    '1d': 5, '1w': 4, '1M': 3
                }.get(timeframe, 10)

                recent_data = self.universe_manager.get_lag_prices(instrument_id, prediction_date, window_size)
                features = self.feature_extractor.extract_all_features(recent_data, timeframe)
                timeframe_features[timeframe] = features

            except Exception as e:
                self.logger.warning(f"Failed to generate {timeframe} features for instrument {instrument_id}: {e}")
                timeframe_features[timeframe] = {}

        return timeframe_features

    async def generate_training_example(self,
                                       symbol: str,
                                       prediction_timestamp: datetime) -> Optional[Dict]:
        """
        Generate a single training example for the given symbol and timestamp.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            prediction_timestamp: Timestamp to generate prediction for

        Returns:
            Dict with single-point features per timeframe or None if insufficient data
        """
        instrument_id = await self.get_instrument_id(symbol)
        if not instrument_id:
            self.logger.warning(f"Could not find instrument_id for symbol {symbol}")
            return None

        try:
            # Generate base features
            base_features = self.generate_base_features(instrument_id, prediction_timestamp)

            # Build single-point timeframe features
            timeframe_features = await self.sequence_builder.build_timeframe_features(instrument_id, prediction_timestamp)

            # Build single-point prediction targets
            targets = await self.sequence_builder.build_prediction_targets(instrument_id, prediction_timestamp)

            # Validate that we have data for at least one timeframe
            if not any(timeframe_features.values()):
                self.logger.warning(f"No timeframe data available for {symbol} at {prediction_timestamp}")
                return None

            # Create simple training example dict
            example = {
                'instrument_id': instrument_id,
                'symbol': symbol,
                'prediction_timestamp': prediction_timestamp,
                'base_features': base_features,
                'timeframe_features': timeframe_features,
                'prediction_targets': targets
            }

            return example

        except Exception as e:
            self.logger.error(f"Failed to generate training example for {symbol} at {prediction_timestamp}: {e}")
            return None


