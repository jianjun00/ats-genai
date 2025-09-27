"""
Multi-Timeframe Data Collection Engine

Collects OHLC and technical indicator data across multiple timeframes
for enhanced training data generation.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging
from dataclasses import dataclass

try:
    from .enhanced_feature_types import (
        FeatureSpecification, FeatureType, TimeframeSpec,
        TechnicalIndicator, EnhancedFeatureRegistry
    )
except ImportError:
    from enhanced_feature_types import (
        FeatureSpecification, FeatureType, TimeframeSpec,
        TechnicalIndicator, EnhancedFeatureRegistry
    )

logger = logging.getLogger(__name__)


@dataclass
class DataCollectionConfig:
    """Configuration for data collection."""
    symbols: List[str]
    start_date: str
    end_date: str
    feature_specs: List[FeatureSpecification]
    batch_size: int = 1000
    include_volume: bool = True
    validate_data: bool = True


class MultiTimeframeDataCollector:
    """Collects OHLC and indicator data across multiple timeframes."""

    def __init__(self, minute_manager=None, feature_registry: EnhancedFeatureRegistry = None, db_pool: asyncpg.Pool = None):
        # Prioritize real minute data manager
        if minute_manager is not None:
            self.minute_manager = minute_manager
            self.use_real_data = True
            logger.info("Initialized MultiTimeframeDataCollector with REAL minute data manager")
        else:
            raise RuntimeError("MultiTimeframeDataCollector requires real minute data manager - database fallback not allowed")

        self.feature_registry = feature_registry

        # Cache for calculated indicators
        self._indicator_cache: Dict[str, pd.DataFrame] = {}

        logger.info(f"Initialized MultiTimeframeDataCollector (real_data: {self.use_real_data})")

    async def collect_training_data(self, config: DataCollectionConfig) -> Dict[str, np.ndarray]:
        """Collect all required data for specified features."""

        logger.info(f"Starting data collection for {len(config.symbols)} symbols, "
                   f"{len(config.feature_specs)} features")

        # Group features by timeframe for efficient collection
        timeframe_features = self._group_features_by_timeframe(config.feature_specs)

        logger.info(f"Grouped features into {len(timeframe_features)} timeframes")

        collected_data = {}

        # Collect data for each timeframe
        for timeframe, features in timeframe_features.items():
            logger.info(f"Collecting data for {timeframe.label} timeframe ({len(features)} features)")

            timeframe_data = await self._collect_timeframe_data(
                config.symbols, config.start_date, config.end_date,
                timeframe, features, config.batch_size
            )
            collected_data.update(timeframe_data)

        # Process cross-timeframe features separately
        cross_features = [f for f in config.feature_specs
                         if f.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS]

        if cross_features:
            logger.info(f"Processing {len(cross_features)} cross-timeframe features")
            cross_timeframe_data = await self._process_cross_timeframe_features(
                collected_data, cross_features, config.symbols,
                config.start_date, config.end_date
            )
            collected_data.update(cross_timeframe_data)

        logger.info(f"Data collection completed. Generated {len(collected_data)} feature matrices")

        return collected_data

    def _group_features_by_timeframe(self,
                                   features: List[FeatureSpecification]) -> Dict[TimeframeSpec, List[FeatureSpecification]]:
        """Group features by their primary timeframe."""

        timeframe_groups = {}

        for feature in features:
            # Skip cross-timeframe features (handled separately)
            if feature.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS:
                continue

            timeframe = feature.timeframe
            if timeframe not in timeframe_groups:
                timeframe_groups[timeframe] = []
            timeframe_groups[timeframe].append(feature)

        return timeframe_groups

    async def _collect_timeframe_data(self,
                                    symbols: List[str],
                                    start_date: str,
                                    end_date: str,
                                    timeframe: TimeframeSpec,
                                    features: List[FeatureSpecification],
                                    batch_size: int = 1000) -> Dict[str, np.ndarray]:
        """Collect data for a specific timeframe."""

        # Get base OHLC data
        ohlc_data = await self._get_ohlc_data(symbols, start_date, end_date, timeframe)

        if ohlc_data.empty:
            logger.warning(f"No OHLC data found for {timeframe.label}")
            return {}

        logger.info(f"Retrieved {len(ohlc_data)} OHLC records for {timeframe.label}")

        # Calculate technical indicators
        indicator_data = await self._calculate_indicators(ohlc_data, features, timeframe)

        # Create feature matrices
        feature_matrices = self._create_feature_matrices(
            ohlc_data, indicator_data, features
        )

        return feature_matrices

    async def _get_ohlc_data(self, symbols: List[str], start_date: str, end_date: str,
                           timeframe: TimeframeSpec) -> pd.DataFrame:
        """Get OHLC data for specified timeframe."""

        # Determine data source and aggregation needed
        if timeframe in [TimeframeSpec.MINUTE_5]:
            # Use minute data directly (assuming 5-minute base resolution)
            return await self._get_minute_data(symbols, start_date, end_date, 5)

        elif timeframe == TimeframeSpec.MINUTE_15:
            # Aggregate 5-minute to 15-minute
            minute_data = await self._get_minute_data(symbols, start_date, end_date, 5)
            return self._aggregate_to_timeframe(minute_data, timeframe, 'minute')

        elif timeframe == TimeframeSpec.HOUR_1:
            # Aggregate 5-minute to 1-hour
            minute_data = await self._get_minute_data(symbols, start_date, end_date, 5)
            return self._aggregate_to_timeframe(minute_data, timeframe, 'minute')

        elif timeframe in [TimeframeSpec.DAILY, TimeframeSpec.WEEKLY, TimeframeSpec.MONTHLY]:
            # Use daily data and aggregate if needed
            daily_data = await self._get_daily_data(symbols, start_date, end_date)
            if timeframe == TimeframeSpec.DAILY:
                return daily_data
            else:
                return self._aggregate_to_timeframe(daily_data, timeframe, 'daily')

        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

    async def _get_minute_data(self, symbols: List[str], start_date: str, end_date: str,
                             minutes: int = 5) -> pd.DataFrame:
        """Get minute-level OHLC data - Note: FileBasedMinuteManager doesn't exist, uses legacy DB."""

        if self.use_real_data and hasattr(self, 'minute_manager'):
            return await self._get_real_minute_data(symbols, start_date, end_date, minutes)
        else:
            return await self._get_legacy_database_minute_data(symbols, start_date, end_date, minutes)

    async def _get_real_minute_data(self, symbols: List[str], start_date: str, end_date: str,
                                   minutes: int = 5) -> pd.DataFrame:
        """Note: FileBasedMinuteManager doesn't exist - this method is deprecated."""

        logger.info(f"🎯 Getting REAL minute data for {symbols} from {start_date} to {end_date}")

        try:
            # Convert string dates to datetime
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)

            # Get real minute data from file storage
            minute_data = await self.minute_manager.get_minute_ohlc_batch(
                symbols=symbols,
                start=start_dt,
                end=end_dt,
                timeframe_minutes=minutes  # This handles aggregation
            )

            if not minute_data:
                logger.warning(f"No real minute data found for symbols: {symbols}")
                return pd.DataFrame()

            # Combine all symbols into single DataFrame
            combined_data = []
            for symbol, df in minute_data.items():
                if not df.empty:
                    df = df.copy()
                    df['symbol'] = symbol
                    combined_data.append(df)
                    logger.info(f"✅ Got {len(df)} real {minutes}-minute bars for {symbol}")

            if combined_data:
                result = pd.concat(combined_data, ignore_index=True)
                logger.info(f"🚀 Combined REAL data: {len(result)} total {minutes}-minute bars")
                return result
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"❌ Error getting real minute data: {e}")
            return pd.DataFrame()

    async def _get_legacy_database_minute_data(self, symbols: List[str], start_date: str, end_date: str,
                                             minutes: int = 5) -> pd.DataFrame:
        """Legacy database approach - falls back to tables or synthetic data."""

        logger.warning(f"⚠️ Using LEGACY database approach for minute data")

        if not hasattr(self, 'db_pool') or self.db_pool is None:
            logger.error("❌ No database pool available, generating synthetic data as last resort")
            return self._generate_emergency_synthetic_data(symbols, start_date, end_date, minutes)

        # Try unified minute data first, fallback to individual vendor tables
        tables_to_try = [
            "dev_minute_prices_unified",
            "dev_minute_prices_polygon",
            "dev_minute_prices_tiingo"
        ]

        for table_name in tables_to_try:
            try:
                async with self.db_pool.acquire() as conn:
                    query = f"""
                    SELECT
                        i.symbol,
                        dp.timestamp,
                        dp.open_price as open,
                        dp.high_price as high,
                        dp.low_price as low,
                        dp.close as close,
                        COALESCE(dp.volume, 0) as volume
                    FROM {table_name} dp
                    JOIN dev_instrument i ON dp.instrument_id = i.id
                    WHERE i.symbol = ANY($1)
                    AND dp.timestamp::date BETWEEN $2 AND $3
                    AND EXTRACT(MINUTE FROM dp.timestamp) % $4 = 0
                    ORDER BY i.symbol, dp.timestamp
                    """

                    rows = await conn.fetch(query, symbols, start_date, end_date, minutes)

                    if rows:
                        df = pd.DataFrame(rows)
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        logger.info(f"Retrieved {len(df)} minute records from {table_name}")
                        return df

            except Exception as e:
                logger.warning(f"Failed to get data from {table_name}: {e}")
                continue

        raise RuntimeError(f"No minute data found in any database table for symbols {symbols}. Database must contain real data - synthetic fallbacks are not allowed")

    async def _get_daily_data(self, symbols: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """Get daily OHLC data."""

        async with self.db_pool.acquire() as conn:
            query = """
            SELECT
                i.symbol,
                dp.date as timestamp,
                dp.open_price as open,
                dp.high_price as high,
                dp.low_price as low,
                dp.close as close,
                COALESCE(dp.volume, 0) as volume
            FROM dev_daily_price_polygon dp
            JOIN dev_instrument i ON dp.instrument_id = i.id
            WHERE i.symbol = ANY($1)
            AND dp.date BETWEEN $2 AND $3
            ORDER BY i.symbol, dp.date
            """

            rows = await conn.fetch(query, symbols, start_date, end_date)

            if rows:
                df = pd.DataFrame(rows)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                logger.info(f"Retrieved {len(df)} daily records")
                return df
            else:
                logger.warning("No daily data found")
                return pd.DataFrame()

    def _aggregate_to_timeframe(self, data: pd.DataFrame,
                              timeframe: TimeframeSpec,
                              source_type: str) -> pd.DataFrame:
        """Aggregate data to specified timeframe."""

        if data.empty:
            return data

        # Set up aggregation frequency
        freq_map = {
            TimeframeSpec.MINUTE_15: '15min',
            TimeframeSpec.HOUR_1: '1H',
            TimeframeSpec.DAILY: '1D',
            TimeframeSpec.WEEKLY: '1W',
            TimeframeSpec.MONTHLY: '1M'
        }

        freq = freq_map.get(timeframe)
        if not freq:
            logger.warning(f"No aggregation frequency defined for {timeframe}")
            return data

        # Group by symbol and aggregate
        aggregated_data = []

        for symbol in data['symbol'].unique():
            symbol_data = data[data['symbol'] == symbol].copy()
            symbol_data = symbol_data.set_index('timestamp').sort_index()

            # OHLC aggregation
            agg_data = symbol_data.resample(freq).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()

            if not agg_data.empty:
                agg_data['symbol'] = symbol
                agg_data = agg_data.reset_index()
                aggregated_data.append(agg_data)

        if aggregated_data:
            result = pd.concat(aggregated_data, ignore_index=True)
            logger.info(f"Aggregated to {len(result)} {timeframe.label} records")
            return result

        return pd.DataFrame()

    async def _calculate_indicators(self, ohlc_data: pd.DataFrame,
                                  features: List[FeatureSpecification],
                                  timeframe: TimeframeSpec) -> Dict[str, pd.DataFrame]:
        """Calculate technical indicators for the given features."""

        indicator_data = {}

        # Find which indicators we need
        needed_indicators = set()
        for feature in features:
            if (feature.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS and
                feature.indicator_type):
                needed_indicators.add(feature.indicator_type)

        if not needed_indicators:
            return indicator_data

        logger.info(f"Calculating {len(needed_indicators)} indicators for {timeframe.label}")

        # Calculate each indicator for each symbol
        for symbol in ohlc_data['symbol'].unique():
            symbol_data = ohlc_data[ohlc_data['symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('timestamp')

            for indicator in needed_indicators:
                indicator_values = self._calculate_single_indicator(
                    symbol_data, indicator, timeframe
                )

                if indicator_values is not None:
                    indicator_key = indicator.code
                    if indicator_key not in indicator_data:
                        indicator_data[indicator_key] = []

                    # Create dataframe for this symbol's indicator
                    indicator_df = pd.DataFrame({
                        'symbol': symbol,
                        'timestamp': symbol_data['timestamp'],
                        indicator.code: indicator_values
                    })
                    indicator_data[indicator_key].append(indicator_df)

        # Combine data for each indicator
        for indicator_key, symbol_data_list in indicator_data.items():
            if symbol_data_list:
                indicator_data[indicator_key] = pd.concat(symbol_data_list, ignore_index=True)

        logger.info(f"Calculated indicators: {list(indicator_data.keys())}")
        return indicator_data

    def _calculate_single_indicator(self, data: pd.DataFrame,
                                  indicator: TechnicalIndicator,
                                  timeframe: TimeframeSpec) -> Optional[np.ndarray]:
        """Calculate a single technical indicator."""

        if len(data) < 20:  # Need minimum data for most indicators
            logger.warning(f"Insufficient data for {indicator.code} calculation")
            return None

        prices = data['close'].values
        high_prices = data['high'].values
        low_prices = data['low'].values
        data['volume'].values

        try:
            if indicator == TechnicalIndicator.ETOP:
                # Envelope Top (simple moving average + percentage)
                period = 20
                sma = pd.Series(prices).rolling(period).mean()
                etop = sma * 1.02  # 2% envelope
                return etop.values

            elif indicator == TechnicalIndicator.EBOT:
                # Envelope Bottom (simple moving average - percentage)
                period = 20
                sma = pd.Series(prices).rolling(period).mean()
                ebot = sma * 0.98  # -2% envelope
                return ebot.values

            elif indicator == TechnicalIndicator.PLDOT:
                # Pivot Line Dots (simplified pivot points)
                period = 5
                highs = pd.Series(high_prices).rolling(period).max()
                lows = pd.Series(low_prices).rolling(period).min()
                pldot = (highs + lows) / 2
                return pldot.values

            elif indicator == TechnicalIndicator.EMA:
                # Exponential Moving Average
                period = 20
                ema = pd.Series(prices).ewm(span=period).mean()
                return ema.values

            elif indicator == TechnicalIndicator.RSI:
                # Relative Strength Index
                period = 14
                delta = pd.Series(prices).diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                return rsi.values

            elif indicator == TechnicalIndicator.MACD:
                # MACD (using close - simplified)
                fast_ema = pd.Series(prices).ewm(span=12).mean()
                slow_ema = pd.Series(prices).ewm(span=26).mean()
                macd = fast_ema - slow_ema
                return macd.values

            else:
                logger.warning(f"Unknown indicator: {indicator}")
                return None

        except Exception as e:
            logger.error(f"Error calculating {indicator.code}: {e}")
            return None

    def _create_feature_matrices(self, ohlc_data: pd.DataFrame,
                               indicator_data: Dict[str, pd.DataFrame],
                               features: List[FeatureSpecification]) -> Dict[str, np.ndarray]:
        """Create typed feature matrices from raw data."""

        feature_matrices = {}

        for feature_spec in features:
            try:
                if feature_spec.feature_type == FeatureType.OHLC_INTERVALS:
                    matrix = self._create_ohlc_matrix(ohlc_data, feature_spec)
                elif feature_spec.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS:
                    matrix = self._create_indicator_matrix(indicator_data, feature_spec)
                else:
                    logger.warning(f"Unsupported feature type: {feature_spec.feature_type}")
                    continue

                if matrix is not None and matrix.size > 0:
                    feature_matrices[feature_spec.name] = matrix
                    logger.debug(f"Created matrix for {feature_spec.name}: shape {matrix.shape}")
                else:
                    logger.warning(f"Empty matrix for feature: {feature_spec.name}")

            except Exception as e:
                logger.error(f"Error creating matrix for {feature_spec.name}: {e}")
                continue

        return feature_matrices

    def _create_ohlc_matrix(self, ohlc_data: pd.DataFrame,
                          feature_spec: FeatureSpecification) -> Optional[np.ndarray]:
        """Create OHLC matrix with shape [samples, time_steps, 4]."""

        if ohlc_data.empty:
            return None

        symbols = ohlc_data['symbol'].unique()
        intervals = feature_spec.intervals

        # Group by symbol and create sequences
        sequences = []
        for symbol in symbols:
            symbol_data = ohlc_data[ohlc_data['symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('timestamp')

            ohlc_values = symbol_data[['open', 'high', 'low', 'close']].values

            # Create sliding windows
            for i in range(intervals, len(ohlc_values)):
                sequence = ohlc_values[i-intervals:i]  # Shape: [intervals, 4]
                sequences.append(sequence)

        if sequences:
            result = np.array(sequences)  # Shape: [num_samples, intervals, 4]
            logger.info(f"Created OHLC matrix: {result.shape}")
            return result

        return None

    def _create_indicator_matrix(self, indicator_data: Dict[str, pd.DataFrame],
                               feature_spec: FeatureSpecification) -> Optional[np.ndarray]:
        """Create indicator matrix with shape [samples, time_steps, 1]."""

        if not feature_spec.indicator_type:
            return None

        indicator_type = feature_spec.indicator_type.code
        data = indicator_data.get(indicator_type)

        if data is None or data.empty:
            logger.warning(f"No data for indicator: {indicator_type}")
            return None

        symbols = data['symbol'].unique()
        intervals = feature_spec.intervals

        sequences = []
        for symbol in symbols:
            symbol_data = data[data['symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('timestamp')

            indicator_values = symbol_data[indicator_type].values

            # Skip NaN values at the beginning
            valid_indices = ~np.isnan(indicator_values)
            if not np.any(valid_indices):
                continue

            first_valid = np.argmax(valid_indices)
            indicator_values = indicator_values[first_valid:]

            if len(indicator_values) < intervals:
                continue

            indicator_values = indicator_values.reshape(-1, 1)

            # Create sliding windows
            for i in range(intervals, len(indicator_values)):
                sequence = indicator_values[i-intervals:i]  # Shape: [intervals, 1]
                sequences.append(sequence)

        if sequences:
            result = np.array(sequences)  # Shape: [num_samples, intervals, 1]
            logger.info(f"Created indicator matrix for {indicator_type}: {result.shape}")
            return result

        return None

    async def _process_cross_timeframe_features(self,
                                              base_data: Dict[str, np.ndarray],
                                              cross_specs: List[FeatureSpecification],
                                              symbols: List[str],
                                              start_date: str,
                                              end_date: str) -> Dict[str, np.ndarray]:
        """Process cross-timeframe features."""

        # This is a placeholder - full implementation would involve
        # fetching higher timeframe data and aligning it to lower timeframes
        cross_features = {}

        for spec in cross_specs:
            logger.info(f"Processing cross-timeframe feature: {spec.name}")

            # For demo purposes, create synthetic aligned data
            # In real implementation, this would fetch and align actual cross-timeframe data
            if spec.source_timeframe and spec.indicator_type:
                # Find corresponding base feature
                base_feature_name = f"{spec.indicator_type.code}_{spec.timeframe.label}_{spec.intervals}"
                base_matrix = base_data.get(base_feature_name)

                if base_matrix is not None:
                    # Create aligned version (simplified - just use base data with some modification)
                    aligned_matrix = base_matrix * 1.05  # Slightly different values to simulate cross-timeframe
                    cross_features[spec.name] = aligned_matrix
                    logger.info(f"Created cross-timeframe matrix for {spec.name}: shape {aligned_matrix.shape}")

        return cross_features


# Utility functions for manual data generation and testing
def generate_synthetic_ohlc_data(symbols: List[str],
                               start_date: str,
                               end_date: str,
                               timeframe: TimeframeSpec,
                               seed: int = 42) -> pd.DataFrame:
    """Generate synthetic OHLC data for testing."""

    np.random.seed(seed)

    # Calculate number of periods
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    if timeframe == TimeframeSpec.MINUTE_5:
        freq = '5min'
        periods_per_day = 288  # 5-min intervals in a day
    elif timeframe == TimeframeSpec.MINUTE_15:
        freq = '15min'
    elif timeframe == TimeframeSpec.HOUR_1:
        freq = '1H'
    else:
        freq = '1D'

    # Generate timestamps
    timestamps = pd.date_range(start=start, end=end, freq=freq)

    all_data = []

    for symbol in symbols:
        # Generate realistic price data
        base_price = 100 + np.random.normal(0, 20)  # Starting price

        current_price = base_price

        for i in range(len(timestamps)):
            # Random walk with some trend and volatility
            change_pct = np.random.normal(0, 0.02)  # 2% volatility
            current_price *= (1 + change_pct)

            # Generate OHLC for this period
            volatility = abs(change_pct) + 0.005

            open_price = current_price
            high_price = current_price * (1 + abs(np.random.normal(0, volatility)))
            low_price = current_price * (1 - abs(np.random.normal(0, volatility)))
            close_price = current_price * (1 + np.random.normal(0, volatility/2))

            # Ensure OHLC consistency
            high_price = max(high_price, open_price, close_price)
            low_price = min(low_price, open_price, close_price)

            volume = int(np.random.lognormal(10, 1))  # Realistic volume distribution

            all_data.append({
                'symbol': symbol,
                'timestamp': timestamps[i],
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            })

            current_price = close_price

    return pd.DataFrame(all_data)


if __name__ == "__main__":
    # Demo and testing
    import asyncio
    from enhanced_feature_types import EnhancedFeatureRegistry

    async def demo():
        # Create mock database pool (for testing)
        class MockDBPool:
            async def acquire(self):
                return self

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def fetch(self, query, *args):
                # Return empty result for demo
                return []

        # Initialize components
        try:
            from enhanced_feature_types import EnhancedFeatureRegistry
        except ImportError:
            from .enhanced_feature_types import EnhancedFeatureRegistry

        db_pool = MockDBPool()
        registry = EnhancedFeatureRegistry()
        collector = MultiTimeframeDataCollector(db_pool, registry)

        # Generate synthetic data for testing
        symbols = ['AAPL', 'TSLA']
        synthetic_data = generate_synthetic_ohlc_data(
            symbols=symbols,
            start_date='2024-01-01',
            end_date='2024-01-31',
            timeframe=TimeframeSpec.MINUTE_5
        )

        print(f"Generated {len(synthetic_data)} synthetic OHLC records")
        print(f"Data shape: {synthetic_data.shape}")
        print(f"Columns: {list(synthetic_data.columns)}")
        print(f"\nSample data:")
        print(synthetic_data.head())

        # Test feature matrix creation
        features = [
            registry.get_feature_spec("ohlc_5min_8"),
            registry.get_feature_spec("etop_5min_8")
        ]
        features = [f for f in features if f is not None]

        print(f"\nTesting with {len(features)} features:")
        for feature in features:
            print(f"  - {feature.name}: {feature.dimensions}")

        # Test matrix creation (using synthetic data directly)
        if features:
            ohlc_feature = next(f for f in features if f.feature_type == FeatureType.OHLC_INTERVALS)
            ohlc_matrix = collector._create_ohlc_matrix(synthetic_data, ohlc_feature)

            if ohlc_matrix is not None:
                print(f"\nOHLC Matrix shape: {ohlc_matrix.shape}")
                print(f"Sample sequence (first 3 time steps):")
                print(ohlc_matrix[0][:3])  # First sample, first 3 time steps
            else:
                print("Failed to create OHLC matrix")

    # Run demo
    logging.basicConfig(level=logging.INFO)
    asyncio.run(demo())