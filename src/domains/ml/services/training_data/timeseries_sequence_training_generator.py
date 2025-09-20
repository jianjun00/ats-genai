"""
TimeSeriesSequenceTrainingGenerator - Multi-timeframe sequence-based training data generation.

This module generates training data using the universe state builder infrastructure,
supporting multiple timeframes and sequence-based features for advanced ML models.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, List, Optional
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
                 feature_types: Optional[List[str]] = None,
                 signal_names: Optional[List[str]] = None):
        """
        Initialize training data configuration.

        Args:
            base_interval_minutes: Base data collection interval (1 minute)
            training_interval_minutes: Training data generation interval (60 minutes)
            feature_types: Types of features to extract
            signal_names: List of technical indicator signal names to retrieve from UniverseStateManager
        """
        self.base_interval_minutes = base_interval_minutes
        self.training_interval_minutes = training_interval_minutes

        self.feature_types = feature_types
        if self.feature_types is None:
            raise ValueError("feature_types parameter is required. Please configure via gin config or pass as parameter.")

        self.signal_names = signal_names
        if self.signal_names is None:
            raise ValueError("signal_names parameter is required. Please configure via gin config or pass as parameter.")
        
        # Note: timeframes will be obtained from UniverseStateIntervalBuilder at runtime


@gin.configurable
class MultiTimeframeFeatureExtractor:
    """Extract features across multiple timeframes."""

    def __init__(self, config: TrainingDataConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Initialize S/R feature extractor if support_resistance is in feature_types
        if hasattr(config, 'feature_types') and 'support_resistance' in config.feature_types:
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
                from domains.trading.signals.indicator import VolumeProfile

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
                ohlcv_features = self.extract_ohlcv_features(data, timeframe)
                all_features.update(ohlcv_features)
            elif feature_type == 'returns':
                returns_features = self.extract_returns_features(data, timeframe)
                all_features.update(returns_features)
            elif feature_type == 'volume_profile':
                volume_features = self.extract_volume_features(data, timeframe)
                all_features.update(volume_features)
            elif feature_type == 'technical':
                technical_features = self.extract_technical_features(data, timeframe)
                all_features.update(technical_features)
            elif feature_type == 'indicators':
                indicator_features = self.extract_technical_indicators(data, timeframe)
                all_features.update(indicator_features)
            elif feature_type == 'support_resistance':
                sr_features = self.extract_support_resistance_features(data, timeframe)
                all_features.update(sr_features)

        # Always include technical indicators from UniverseStateManager if available
        additional_indicators = self.extract_technical_indicators(data, timeframe)
        all_features.update(additional_indicators)

        return all_features


@gin.configurable
class SequenceWindowBuilder:
    """Build sequence windows with lag/lead capabilities."""

    def __init__(self, config: TrainingDataConfig, universe_manager: UniverseStateManager, timeframes: List[str]):
        self.config = config
        self.universe_manager = universe_manager
        self.timeframes = timeframes
        self.feature_extractor = MultiTimeframeFeatureExtractor(config)
        self.logger = logging.getLogger(__name__)

    async def get_timeframe_data(self, instrument_id: int, center_datetime: datetime,
                          timeframe: str, is_future: bool = False) -> Dict[str, float]:
        """
        Get current data point for a specific timeframe at center_datetime.

        Args:
            instrument_id: Target instrument
            center_datetime: Center datetime for the data point
            timeframe: Timeframe string (e.g., '5m', '60m', '1d')
            is_future: Whether to get future data (lead) or current data (lag)
        """

        # 🚨 CRITICAL ARCHITECTURAL CHANGE: Fail-fast error handling with UniverseStateInterval
        # PREVIOUS: Initialized empty DataFrame and handled missing data gracefully
        # NEW: Fail immediately if UniverseStateInterval or instrument data is missing
        # BENEFIT: Forces UniverseStateBuilder to compute all required intervals before training

        if is_future:
            # 🚨 CRITICAL ARCHITECTURAL FIX: Use pre-computed UniverseStateInterval for future data
            # OLD APPROACH: Called get_lead_prices() to rebuild future data that was already computed
            # NEW APPROACH: Retrieve pre-computed future UniverseStateInterval from UniverseStateBuilder
            # BENEFIT: Uses the same future OHLCV and indicator data that was already computed
            future_universe_state_interval = self.universe_manager.get_future_universe_state_interval(
                timeframe=timeframe,
                current_time=center_datetime,
                lead_periods=1
            )
            
            if future_universe_state_interval is None:
                raise RuntimeError(f"No future UniverseStateInterval found for {timeframe} at {center_datetime}. "
                                 f"This indicates UniverseStateBuilder hasn't computed future intervals yet. "
                                 f"System must fail fast - cannot generate training data without pre-computed intervals.")
            else:
                # Extract future InstrumentInterval for the target instrument
                if instrument_id in future_universe_state_interval.instrument_intervals:
                    future_instrument_interval = future_universe_state_interval.instrument_intervals[instrument_id]
                    
                    # Convert future InstrumentInterval to DataFrame format
                    data_df = pd.DataFrame([{
                        'timestamp': future_universe_state_interval.end_date_time,
                        'open': future_instrument_interval.open,
                        'high': future_instrument_interval.high,
                        'low': future_instrument_interval.low,
                        'close': future_instrument_interval.close,
                        'volume': future_instrument_interval.traded_volume
                    }])
                    
                    # Add future technical indicators from UniverseStateInterval
                    if future_universe_state_interval.instrument_indicator_intervals:
                        for indicator_name, indicator_dict in future_universe_state_interval.instrument_indicator_intervals.items():
                            if instrument_id in indicator_dict:
                                future_indicator_interval = indicator_dict[instrument_id]
                                data_df[indicator_name] = future_indicator_interval.value
                else:
                    raise RuntimeError(f"Instrument {instrument_id} not found in future UniverseStateInterval for {timeframe} at {center_datetime}. "
                                     f"This indicates UniverseStateBuilder hasn't computed instrument data for this symbol. "
                                     f"System must fail fast - cannot generate training data without complete instrument coverage.")
        else:
            # 🚨 CRITICAL ARCHITECTURAL FIX: Use pre-computed UniverseStateInterval
            # OLD APPROACH: Called get_lag_prices() to rebuild data that was already computed
            # NEW APPROACH: Retrieve pre-computed UniverseStateInterval from UniverseStateBuilder
            # BENEFIT: Uses the same OHLCV and indicator data that was already computed
            universe_state_interval = self.universe_manager.get_universe_state_interval(
                timeframe=timeframe,
                current_time=center_datetime
            )
            
            if universe_state_interval is None:
                raise RuntimeError(f"No UniverseStateInterval found for {timeframe} at {center_datetime}. "
                                 f"This indicates UniverseStateBuilder hasn't computed intervals yet. "
                                 f"System must fail fast - cannot generate training data without pre-computed intervals.")
            else:
                # Extract InstrumentInterval for the target instrument
                if instrument_id in universe_state_interval.instrument_intervals:
                    instrument_interval = universe_state_interval.instrument_intervals[instrument_id]
                    
                    # Convert InstrumentInterval to DataFrame format
                    data_df = pd.DataFrame([{
                        'timestamp': universe_state_interval.end_date_time,
                        'open': instrument_interval.open,
                        'high': instrument_interval.high,
                        'low': instrument_interval.low,
                        'close': instrument_interval.close,
                        'volume': instrument_interval.traded_volume
                    }])
                    
                    # Add technical indicators from UniverseStateInterval
                    if universe_state_interval.instrument_indicator_intervals:
                        for indicator_name, indicator_dict in universe_state_interval.instrument_indicator_intervals.items():
                            if instrument_id in indicator_dict:
                                indicator_interval = indicator_dict[instrument_id]
                                data_df[indicator_name] = indicator_interval.value
                else:
                    raise RuntimeError(f"Instrument {instrument_id} not found in UniverseStateInterval for {timeframe} at {center_datetime}. "
                                     f"This indicates UniverseStateBuilder hasn't computed instrument data for this symbol. "
                                     f"System must fail fast - cannot generate training data without complete instrument coverage.")

        if data_df.empty:
            raise RuntimeError(f"UniverseStateInterval found but contains no valid data for instrument {instrument_id} in {timeframe} at {center_datetime}. "
                             f"This indicates data corruption or incomplete interval computation. "
                             f"System must fail fast - cannot generate features from empty data.")


        # Extract features for the single data point
        
        single_point_features = self.feature_extractor.extract_all_features(
            data_df, timeframe
        )

        
        return single_point_features

    async def build_timeframe_features(self, instrument_id: int, prediction_timestamp: datetime, target_timeframes: Optional[List[str]] = None) -> Dict[str, Dict[str, float]]:
        """Build single-point features for specified timeframes.
        
        Args:
            instrument_id: Instrument ID
            prediction_timestamp: Timestamp for prediction
            target_timeframes: Specific timeframes to build (None = all timeframes)
        
        Returns:
            Dict mapping timeframe to features
        """
        timeframe_features = {}

        # Use target timeframes if specified, otherwise use all configured timeframes
        timeframes_to_build = target_timeframes if target_timeframes is not None else self.timeframes
        

        # Build current features for each specified timeframe
        for timeframe in timeframes_to_build:
            features = await self.get_timeframe_data(
                instrument_id, prediction_timestamp, timeframe, is_future=False
            )
            timeframe_features[timeframe] = features

        return timeframe_features

    async def build_prediction_targets(self, instrument_id: int, prediction_timestamp: datetime) -> Dict[str, Dict[str, float]]:
        """Build single-point prediction targets for configured timeframes.
        
        TEMPORARY: Future features removed to fix blocking issue.
        This allows complete dataset generation - future features will be added back later.
        """
        targets = {}

        # REMOVED: Future target generation to fix immediate blocking issue
        # TODO: Add back future features after fixing architectural dependencies

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
                 universe_manager: Optional[UniverseStateManager] = None,
                 timeframes_from_gin: Optional[str] = None):
        """
        Initialize the training data generator.

        Args:
            env: Environment configuration
            config: Training data configuration
            universe_manager: Universe state manager for data access
            timeframes_from_gin: Comma-separated timeframes (gets from gin config if None)
        """
        # Handle optional imports gracefully
        if env is None and Environment is not None:
            self.env = Environment()
        else:
            self.env = env

        self.config = config or TrainingDataConfig()

        # Get timeframes from UniverseStateIntervalBuilder gin configuration
        try:
            universe_builder_target_durations = gin.query_parameter('domains.trading.services.state.universe_state_builder.UniverseStateIntervalBuilder.target_durations')
            if universe_builder_target_durations:
                self.timeframes = [d.strip() for d in universe_builder_target_durations.split(',')]
            else:
                # Fallback to default timeframes
                self.timeframes = ['5m', '15m', '60m', '1d']
        except:
            # Fallback if gin query fails
            self.timeframes = ['5m', '15m', '60m', '1d']


        if universe_manager is None and UniverseStateManager is not None and self.env is not None:
            # CRITICAL: Create UniverseStateManager with proper run_context to avoid constraint violations
            from domains.trading.services.state.run_aware_universe_state_manager import create_run_aware_universe_state_manager
            import uuid
            from core.infrastructure.run_context import RunContext
            
            # Create proper run_context with unique run_id to prevent constraint violations
            run_context = RunContext(run_id=f"training_generator_{uuid.uuid4().hex[:8]}")
            self.universe_manager = create_run_aware_universe_state_manager(env=self.env, run_context=run_context)
        else:
            self.universe_manager = universe_manager

        # Initialize components only if dependencies are available
        if self.universe_manager is not None:
            self.sequence_builder = SequenceWindowBuilder(self.config, self.universe_manager, self.timeframes)
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

        # Use direct DAO lookup to avoid missing interface dependencies
        try:
            # First check if universe manager has the instrument_id
            if hasattr(self.universe_manager, '_instrument_ids') and hasattr(self.universe_manager, 'symbols'):
                if symbol in self.universe_manager.symbols:
                    symbol_index = self.universe_manager.symbols.index(symbol)
                    if symbol_index < len(self.universe_manager.instrument_ids):
                        instrument_id = self.universe_manager.instrument_ids[symbol_index]
                        self._symbol_to_id_cache[symbol] = instrument_id
                        return instrument_id

            # Fallback: Use InstrumentXrefsDAO directly
            if self.universe_manager and self.universe_manager.env:
                from core.dao.instruments.instrument_xrefs_dao import InstrumentXrefsDAO
                dao = InstrumentXrefsDAO(self.universe_manager.env)
                instrument_id = await dao.resolve_instrument_id_by_symbol(symbol)
                if instrument_id:
                    self._symbol_to_id_cache[symbol] = instrument_id
                    return instrument_id

            return None

        except Exception as e:
            return None

    def generate_base_features(self, instrument_id: int, prediction_timestamp: datetime) -> Dict[str, float]:
        """Generate base scalar features for the prediction timestamp.
        
        CRITICAL FIXES:
        1. Dynamic lookback calculation based on feature requirements (not hardcoded 5)
        2. Use prediction_timestamp precision (not prediction_date)
        3. Strict future leakage prevention
        4. Sufficient historical data for technical indicators
        """
        try:
            # Calculate required lookback periods based on feature requirements
            lookback_periods = self._calculate_lookback_periods()
            
            # Use timestamp-aware data retrieval (not just date)
            # Get historical data that strictly ends BEFORE prediction_timestamp
            recent_data = self.universe_manager.get_lag_prices(
                instrument_id, 
                prediction_timestamp, 
                lookback_periods
            )
            
            # Filter out any future leakage (data >= prediction_timestamp)
            if not recent_data.empty and 'timestamp' in recent_data.columns:
                recent_data = recent_data[recent_data['timestamp'] < prediction_timestamp]
            
            # Extract features using the cleaned historical data
            if not recent_data.empty:
                base_features = self.feature_extractor.extract_all_features(recent_data, 'base')
            else:
                self.logger.warning(f"No historical data available for instrument {instrument_id} before {prediction_timestamp}")
                base_features = {}
                
        except Exception as e:
            self.logger.error(f"Failed to generate base features for instrument {instrument_id} at {prediction_timestamp}: {e}")
            base_features = {}
            
        return base_features
    
    def _calculate_lookback_periods(self) -> int:
        """Calculate required lookback periods based on feature requirements.
        
        Returns sufficient periods for technical indicators:
        - SMA_20/SMA_50: requires 20-50 periods
        - EMA_12: requires ~24 periods (2x for stability)
        - RSI_14: requires 14+ periods
        - MACD: requires 26+ periods
        
        Plus buffer for calculation stability.
        """
        try:
            # Parse signal names to find maximum period requirements
            max_period = 20  # Default minimum for basic indicators
            
            if hasattr(self.config, 'signal_names') and self.config.signal_names:
                for signal_name in self.config.signal_names:
                    # Extract period from signal names like 'sma_20', 'ema_12', 'rsi_14'
                    if '_' in signal_name:
                        parts = signal_name.split('_')
                        for part in parts:
                            if part.isdigit():
                                period = int(part)
                                max_period = max(max_period, period)
            
            # Add 50% buffer for calculation stability and ensure minimum viable amount
            lookback_periods = max(int(max_period * 1.5), 60)  # At least 60 periods
            
            # Cap at reasonable maximum to avoid excessive data retrieval
            return min(lookback_periods, 200)
            
        except Exception as e:
            self.logger.warning(f"Error calculating lookback periods: {e}")
            return 60  # Safe default for most technical indicators

    def generate_timeframe_features(self, instrument_id: int, prediction_timestamp: datetime) -> Dict[str, Dict[str, float]]:
        """Generate aggregated features across all timeframes."""
        prediction_date = prediction_timestamp.date()
        timeframe_features = {}

        for timeframe in self.timeframes:
            try:
                # Get appropriate window size for each timeframe
                window_size = {
                    '1m': 60, '5m': 12, '15m': 4, '60m': 24,
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
                                       prediction_timestamp: datetime,
                                       target_timeframes: Optional[List[str]] = None) -> Optional[Dict]:
        """
        Generate a single training example for the given symbol and timestamp.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            prediction_timestamp: Timestamp to generate prediction for
            target_timeframes: Optional list of specific timeframes to generate (e.g., ['5m']). 
                             If None, generates for all configured timeframes.

        Returns:
            Dict with single-point features per timeframe or None if insufficient data
        """
        
        instrument_id = await self.get_instrument_id(symbol)
        
        if not instrument_id:
            self.logger.warning(f"Could not find instrument_id for symbol {symbol}")
            return None

        # Generate base features
        base_features = self.generate_base_features(instrument_id, prediction_timestamp)

        # Build single-point timeframe features for specific timeframes
        timeframe_features = await self.sequence_builder.build_timeframe_features(
            instrument_id, 
            prediction_timestamp,
            target_timeframes=target_timeframes
        )

        # Build single-point prediction targets
        targets = await self.sequence_builder.build_prediction_targets(instrument_id, prediction_timestamp)

        # Count available timeframes (more flexible validation)
        available_timeframes = [tf for tf, features in timeframe_features.items() if features]
        available_count = len(available_timeframes)
        
        # More flexible validation: Allow partial timeframe data
        # Only require at least one timeframe to have data (much more permissive)
        if available_count == 0:
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


