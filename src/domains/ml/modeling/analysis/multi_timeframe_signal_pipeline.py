"""
Multi-Timeframe Signal Pipeline

Computes all 15 ATS indicators across multiple timeframes from a common 1-minute base.
Ensures consistent signal computation and proper timeframe aggregation.
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
import logging
from dataclasses import dataclass
from enum import Enum

# Import the 15 ATS indicators
try:
    from signals.indicator import (
        # HLC Linear Regression Indicators (9)
        PL, L11, H11, Z1B, Z2B, EnvelopeBot, EnvelopeTop, Z5T, Z6T,
        # Five Nine Arithmetic Indicators (2)
        FiveNineSell, FiveNineBuy,
        # Five One Conditional Indicators (2)
        FiveOneBuy, FiveOneSell,
        # Five Two Conditional Indicators (2)
        FiveTwoBuy, FiveTwoSell
    )
except ImportError as e:
    logging.error(f"Could not import ATS indicators: {e}")
    # Create placeholder classes for testing
    class DummyIndicator:
        def __init__(self):
            self.status = None
            self.update_at = None
        def update(self, intervals):
            pass
        def get_value(self):
            return None

    PL = L11 = H11 = Z1B = Z2B = EnvelopeBot = EnvelopeTop = Z5T = Z6T = DummyIndicator
    FiveNineSell = FiveNineBuy = FiveOneBuy = FiveOneSell = FiveTwoBuy = FiveTwoSell = DummyIndicator

logger = logging.getLogger(__name__)


class Timeframe(Enum):
    """Supported timeframes for signal computation."""
    MINUTE_1 = "1min"
    MINUTE_5 = "5min"
    MINUTE_15 = "15min"
    HOUR_1 = "1hour"
    DAILY = "1day"
    WEEKLY = "1week"
    MONTHLY = "1month"


@dataclass
class TimeframeConfig:
    """Configuration for a specific timeframe."""
    timeframe: Timeframe
    lookback_periods: int = 60  # How many periods to look back for indicators
    min_data_periods: int = 20  # Minimum periods required for valid signals


@dataclass
class TestInstrumentInterval:
    """Simplified InstrumentInterval for testing indicators."""
    high: float
    low: float
    close: float
    open: Optional[float] = None
    status: str = 'ok'
    timestamp: Optional[datetime] = None
    volume: Optional[float] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.open is None:
            self.open = self.close


class MultiTimeframeSignalPipeline:
    """
    Computes all 15 ATS indicators across multiple timeframes from 1-minute base data.

    Pipeline flow:
    1. Load 1-minute OHLC data
    2. Aggregate to all target timeframes
    3. Compute indicators for each timeframe
    4. Return structured signal results
    """

    def __init__(self, timeframe_configs: List[TimeframeConfig] = None):
        """
        Initialize with timeframe configurations.

        Args:
            timeframe_configs: List of timeframe configurations to compute
        """
        if timeframe_configs is None:
            # Default configuration for all supported timeframes
            self.timeframe_configs = [
                TimeframeConfig(Timeframe.MINUTE_5, lookback_periods=60),
                TimeframeConfig(Timeframe.MINUTE_15, lookback_periods=60),
                TimeframeConfig(Timeframe.HOUR_1, lookback_periods=24),
                TimeframeConfig(Timeframe.DAILY, lookback_periods=20),
                TimeframeConfig(Timeframe.WEEKLY, lookback_periods=12),
                TimeframeConfig(Timeframe.MONTHLY, lookback_periods=6)
            ]
        else:
            self.timeframe_configs = timeframe_configs

        # All 15 ATS indicators organized by category
        self.hlc_indicators = {
            'PL': PL, 'L11': L11, 'H11': H11, 'Z1B': Z1B, 'Z2B': Z2B,
            'EnvelopeBot': EnvelopeBot, 'EnvelopeTop': EnvelopeTop, 'Z5T': Z5T, 'Z6T': Z6T
        }

        self.five_nine_indicators = {
            'FiveNineSell': FiveNineSell,
            'FiveNineBuy': FiveNineBuy
        }

        self.five_one_indicators = {
            'FiveOneBuy': FiveOneBuy,
            'FiveOneSell': FiveOneSell
        }

        self.five_two_indicators = {
            'FiveTwoBuy': FiveTwoBuy,
            'FiveTwoSell': FiveTwoSell
        }

        self.all_indicators = {
            **self.hlc_indicators,
            **self.five_nine_indicators,
            **self.five_one_indicators,
            **self.five_two_indicators
        }

        logger.info(f"Initialized MultiTimeframeSignalPipeline with {len(self.all_indicators)} indicators across {len(self.timeframe_configs)} timeframes")

    async def compute_signals(
        self,
        minute_data: pd.DataFrame,
        symbol: str = None
    ) -> Dict[str, Any]:
        """
        Compute all signals across all timeframes from 1-minute data.

        Args:
            minute_data: DataFrame with 1-minute OHLC data
            symbol: Optional symbol name for logging

        Returns:
            Dict with structure:
            {
                'timeframes': {
                    '5min': {
                        'data': DataFrame,
                        'signals': {'PL': value, 'L11': value, ...}
                    },
                    ...
                },
                'metadata': {
                    'symbol': str,
                    'computation_time': float,
                    'data_periods': int
                }
            }
        """
        if minute_data.empty:
            logger.warning(f"No minute data provided for signal computation")
            return {'timeframes': {}, 'metadata': {'error': 'no_data'}}

        start_time = datetime.now()
        symbol_log = f" for {symbol}" if symbol else ""

        logger.info(f"🚀 Computing signals{symbol_log} across {len(self.timeframe_configs)} timeframes from {len(minute_data)} 1-minute bars")

        results = {
            'timeframes': {},
            'metadata': {
                'symbol': symbol,
                'data_periods': len(minute_data),
                'start_time': start_time.isoformat()
            }
        }

        # Compute signals for each timeframe
        for config in self.timeframe_configs:
            try:
                timeframe_result = await self._compute_timeframe_signals(
                    minute_data, config, symbol
                )
                results['timeframes'][config.timeframe.value] = timeframe_result

            except Exception as e:
                logger.error(f"❌ Error computing {config.timeframe.value} signals{symbol_log}: {e}")
                results['timeframes'][config.timeframe.value] = {
                    'error': str(e),
                    'signals': {}
                }

        # Add computation metadata
        end_time = datetime.now()
        results['metadata']['computation_time'] = (end_time - start_time).total_seconds()
        results['metadata']['end_time'] = end_time.isoformat()

        successful_timeframes = [tf for tf, data in results['timeframes'].items() if 'error' not in data]
        logger.info(f"✅ Completed signal computation{symbol_log}: {len(successful_timeframes)}/{len(self.timeframe_configs)} timeframes successful")

        return results

    async def _compute_timeframe_signals(
        self,
        minute_data: pd.DataFrame,
        config: TimeframeConfig,
        symbol: str = None
    ) -> Dict[str, Any]:
        """Compute signals for a specific timeframe."""

        # Aggregate 1-minute data to target timeframe
        aggregated_data = self._aggregate_to_timeframe(minute_data, config.timeframe)

        if aggregated_data.empty:
            logger.warning(f"No data after aggregating to {config.timeframe.value}")
            return {'data': pd.DataFrame(), 'signals': {}}

        # Check minimum data requirement
        if len(aggregated_data) < config.min_data_periods:
            logger.warning(f"Insufficient data for {config.timeframe.value}: {len(aggregated_data)} < {config.min_data_periods}")
            return {
                'data': aggregated_data,
                'signals': {},
                'warning': 'insufficient_data'
            }

        # Get recent data for indicator computation
        recent_data = aggregated_data.tail(config.lookback_periods)

        # Convert to InstrumentInterval format for indicators
        intervals = self._convert_to_intervals(recent_data)

        # Compute all indicators
        signals = {}
        for indicator_name, indicator_class in self.all_indicators.items():
            try:
                value = self._compute_single_indicator(indicator_class, intervals)
                signals[indicator_name] = value

            except Exception as e:
                logger.error(f"Error computing {indicator_name} for {config.timeframe.value}: {e}")
                signals[indicator_name] = None

        logger.debug(f"Computed {len([s for s in signals.values() if s is not None])}/{len(signals)} indicators for {config.timeframe.value}")

        return {
            'data': aggregated_data,
            'signals': signals,
            'data_periods': len(aggregated_data),
            'lookback_periods': len(recent_data)
        }

    def _aggregate_to_timeframe(
        self,
        minute_data: pd.DataFrame,
        timeframe: Timeframe
    ) -> pd.DataFrame:
        """Aggregate 1-minute data to specified timeframe."""

        if minute_data.empty:
            return minute_data

        # Copy and prepare data
        df = minute_data.copy()

        # Ensure timestamp column and set as index
        if 'timestamp' not in df.columns:
            logger.error("No timestamp column in minute data")
            return pd.DataFrame()

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').sort_index()

        # Define aggregation frequency
        freq_map = {
            Timeframe.MINUTE_1: '1min',
            Timeframe.MINUTE_5: '5min',
            Timeframe.MINUTE_15: '15min',
            Timeframe.HOUR_1: '1H',
            Timeframe.DAILY: '1D',
            Timeframe.WEEKLY: '1W',
            Timeframe.MONTHLY: '1M'
        }

        freq = freq_map.get(timeframe)
        if not freq:
            logger.error(f"No frequency mapping for timeframe: {timeframe}")
            return pd.DataFrame()

        # For 1-minute, return as-is
        if timeframe == Timeframe.MINUTE_1:
            return df.reset_index()

        # OHLC aggregation rules
        agg_rules = {
            'open': 'first',   # First open of the period
            'high': 'max',     # Maximum high of the period
            'low': 'min',      # Minimum low of the period
            'close': 'last',   # Last close of the period
            'volume': 'sum'    # Total volume of the period
        }

        # Add any additional columns that might exist
        for col in df.columns:
            if col not in agg_rules:
                if col in ['vwap', 'trade_count']:
                    agg_rules[col] = 'mean'  # Average for these fields
                elif col in ['vendor', 'symbol']:
                    agg_rules[col] = 'first'  # First value

        # Perform aggregation
        try:
            aggregated = df.resample(freq).agg(agg_rules)

            # Remove periods with no data
            aggregated = aggregated.dropna(subset=['open', 'high', 'low', 'close'])

            # Reset index to get timestamp as column
            result = aggregated.reset_index()

            logger.debug(f"Aggregated from {len(df)} 1-minute bars to {len(result)} {timeframe.value} bars")
            return result

        except Exception as e:
            logger.error(f"Error aggregating to {timeframe.value}: {e}")
            return pd.DataFrame()

    def _convert_to_intervals(self, df: pd.DataFrame) -> List[TestInstrumentInterval]:
        """Convert DataFrame to list of InstrumentInterval objects."""

        intervals = []

        for _, row in df.iterrows():
            try:
                interval = TestInstrumentInterval(
                    high=float(row['high']),
                    low=float(row['low']),
                    close=float(row['close']),
                    open=float(row.get('open', row['close'])),
                    timestamp=row.get('timestamp', datetime.now()),
                    volume=float(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0,
                    status='ok'
                )
                intervals.append(interval)

            except Exception as e:
                logger.warning(f"Error converting row to interval: {e}")
                continue

        return intervals

    def _compute_single_indicator(
        self,
        indicator_class,
        intervals: List[TestInstrumentInterval]
    ) -> Optional[float]:
        """Compute a single indicator value."""

        if not intervals:
            return None

        try:
            indicator = indicator_class()
            indicator.update(intervals)
            return indicator.get_value()

        except Exception as e:
            logger.debug(f"Error computing {indicator_class.__name__}: {e}")
            return None

    def get_timeframe_configs(self) -> List[TimeframeConfig]:
        """Get current timeframe configurations."""
        return self.timeframe_configs.copy()

    def get_supported_indicators(self) -> Dict[str, str]:
        """Get mapping of indicator names to categories."""

        result = {}

        for name in self.hlc_indicators:
            result[name] = 'HLC_Linear_Regression'

        for name in self.five_nine_indicators:
            result[name] = 'Five_Nine_Arithmetic'

        for name in self.five_one_indicators:
            result[name] = 'Five_One_Conditional'

        for name in self.five_two_indicators:
            result[name] = 'Five_Two_Conditional'

        return result


# Factory function for easy initialization
def create_signal_pipeline(
    timeframes: List[str] = None,
    lookback_periods: int = 60
) -> MultiTimeframeSignalPipeline:
    """
    Create a signal pipeline with specified timeframes.

    Args:
        timeframes: List of timeframe strings (e.g., ['5min', '15min', '1hour'])
        lookback_periods: Default lookback periods for indicators

    Returns:
        Configured MultiTimeframeSignalPipeline
    """

    if timeframes is None:
        # Default to all supported timeframes
        timeframes = ['5min', '15min', '1hour', '1day', '1week', '1month']

    # Convert strings to TimeframeConfig objects
    timeframe_map = {
        '1min': Timeframe.MINUTE_1,
        '5min': Timeframe.MINUTE_5,
        '15min': Timeframe.MINUTE_15,
        '1hour': Timeframe.HOUR_1,
        '1day': Timeframe.DAILY,
        '1week': Timeframe.WEEKLY,
        '1month': Timeframe.MONTHLY
    }

    configs = []
    for tf_str in timeframes:
        if tf_str in timeframe_map:
            # Adjust lookback periods based on timeframe
            if tf_str in ['1day', '1week', '1month']:
                periods = min(lookback_periods // 4, 20)  # Fewer periods for longer timeframes
            else:
                periods = lookback_periods

            config = TimeframeConfig(
                timeframe=timeframe_map[tf_str],
                lookback_periods=periods
            )
            configs.append(config)
        else:
            logger.warning(f"Unknown timeframe: {tf_str}")

    return MultiTimeframeSignalPipeline(configs)