"""
1-Minute Data Ingestion Pipeline for TFT Models

High-performance data pipeline for ingesting, processing, and storing 1-minute
OHLCV data from Polygon and Tiingo APIs optimized for TFT model training.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import asyncpg
import pandas as pd
import numpy as np

from core.platform.config_env.environment import env
from domains.market_data.services.agent.polygon_minute_adapter import PolygonMinuteAdapter, MinuteBar

logger = logging.getLogger(__name__)


@dataclass
class IngestionConfig:
    """Configuration for minute data ingestion."""

    # Data source configuration
    symbols: List[str]
    start_date: datetime
    end_date: datetime

    # API configuration
    polygon_api_key: Optional[str] = None
    tiingo_api_key: Optional[str] = None

    # Performance configuration
    batch_size: int = 1000  # Bars per database insert
    max_concurrent_symbols: int = 5
    retry_attempts: int = 3
    retry_delay: int = 60  # seconds

    # Data quality configuration
    min_quality_score: float = 0.7
    max_gap_minutes: int = 5
    outlier_threshold: float = 0.1  # 10% price change

    # Processing configuration
    calculate_indicators: bool = True
    validate_data: bool = True
    store_metadata: bool = True


class MinuteDataPipeline:
    """
    High-performance pipeline for 1-minute data ingestion and processing.

    Designed to support TFT model training with real-time and historical data.
    """

    def __init__(self, pool: asyncpg.Pool, config: IngestionConfig):
        self.pool = pool
        self.config = config
        self.polygon_adapter = None

        # Statistics tracking
        self.stats = {
            'total_bars_processed': 0,
            'total_bars_stored': 0,
            'symbols_completed': 0,
            'symbols_failed': 0,
            'quality_rejections': 0,
            'processing_time': 0
        }

    async def __aenter__(self):
        """Async context manager entry."""
        self.polygon_adapter = PolygonMinuteAdapter(self.config.polygon_api_key)
        await self.polygon_adapter.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.polygon_adapter:
            await self.polygon_adapter.__aexit__(exc_type, exc_val, exc_tb)

    async def run_ingestion(self) -> Dict[str, Any]:
        """
        Execute complete data ingestion pipeline.

        Returns:
            Dictionary with ingestion statistics and results
        """
        start_time = datetime.now()
        logger.info(f"Starting minute data ingestion for {len(self.config.symbols)} symbols")

        try:
            # Process symbols in batches
            symbol_batches = [
                self.config.symbols[i:i + self.config.max_concurrent_symbols]
                for i in range(0, len(self.config.symbols), self.config.max_concurrent_symbols)
            ]

            for batch_idx, symbol_batch in enumerate(symbol_batches):
                logger.info(f"Processing batch {batch_idx + 1}/{len(symbol_batches)}: {symbol_batch}")

                # Process batch concurrently
                tasks = [
                    self._process_symbol(symbol)
                    for symbol in symbol_batch
                ]

                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                # Log batch results
                for symbol, result in zip(symbol_batch, batch_results):
                    if isinstance(result, Exception):
                        logger.error(f"Failed processing {symbol}: {result}")
                        self.stats['symbols_failed'] += 1
                    else:
                        logger.info(f"Completed {symbol}: {result['bars_stored']} bars stored")
                        self.stats['symbols_completed'] += 1
                        self.stats['total_bars_stored'] += result['bars_stored']

            # Final statistics
            self.stats['processing_time'] = (datetime.now() - start_time).total_seconds()

            logger.info(f"Ingestion completed: {self.stats}")
            return self.stats

        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise

    async def _process_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Process data ingestion for a single symbol.

        Args:
            symbol: Stock symbol to process

        Returns:
            Dictionary with processing results
        """
        symbol_stats = {
            'symbol': symbol,
            'bars_fetched': 0,
            'bars_processed': 0,
            'bars_stored': 0,
            'quality_score': 0.0
        }

        try:
            # Fetch minute bars from Polygon
            bars = await self.polygon_adapter.fetch_minute_bars_async(
                symbol, self.config.start_date, self.config.end_date
            )

            symbol_stats['bars_fetched'] = len(bars)

            if not bars:
                logger.warning(f"No data fetched for {symbol}")
                return symbol_stats

            # Process and validate bars
            processed_bars = await self._process_bars(symbol, bars)
            symbol_stats['bars_processed'] = len(processed_bars)

            if not processed_bars:
                logger.warning(f"No valid bars after processing for {symbol}")
                return symbol_stats

            # Store in database
            stored_count = await self._store_bars(processed_bars)
            symbol_stats['bars_stored'] = stored_count

            # Calculate overall quality score
            symbol_stats['quality_score'] = self._calculate_symbol_quality(processed_bars)

            return symbol_stats

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            raise

    async def _process_bars(self, symbol: str, bars: List[MinuteBar]) -> List[Dict[str, Any]]:
        """
        Process and validate minute bars for database storage.

        Args:
            symbol: Stock symbol
            bars: Raw minute bars from API

        Returns:
            List of processed bar dictionaries
        """
        if not bars:
            return []

        # Convert to DataFrame for processing
        df = pd.DataFrame([
            {
                'symbol': bar.symbol,
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'vwap': bar.vwap,
                'trade_count': bar.trade_count,
                'vendor': bar.vendor
            }
            for bar in bars
        ])

        # Data validation and cleaning
        df = await self._validate_and_clean_data(df)

        # Calculate technical indicators if configured
        if self.config.calculate_indicators:
            df = await self._calculate_technical_indicators(df)

        # Calculate quality scores
        if self.config.validate_data:
            df = await self._calculate_quality_scores(df)

            # Filter by minimum quality
            initial_count = len(df)
            df = df[df['quality_score'] >= self.config.min_quality_score]

            filtered_count = initial_count - len(df)
            if filtered_count > 0:
                logger.info(f"Filtered {filtered_count} low-quality bars for {symbol}")
                self.stats['quality_rejections'] += filtered_count

        return df.to_dict('records')

    async def _validate_and_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean minute bar data.

        Args:
            df: DataFrame with minute bars

        Returns:
            Cleaned DataFrame
        """
        # Remove invalid OHLC data
        df = df[
            (df['high'] >= df['low']) &
            (df['high'] >= df['open']) &
            (df['high'] >= df['close']) &
            (df['low'] <= df['open']) &
            (df['low'] <= df['close']) &
            (df['open'] > 0) &
            (df['high'] > 0) &
            (df['low'] > 0) &
            (df['close'] > 0)
        ]

        # Remove extreme outliers
        df = df.sort_values('timestamp')
        df['price_change'] = df['close'].pct_change().abs()
        df = df[df['price_change'] <= self.config.outlier_threshold]
        df = df.drop('price_change', axis=1)

        # Handle missing timestamps (interpolation for small gaps)
        df = df.set_index('timestamp').sort_index()

        # Fill small gaps (up to max_gap_minutes)
        full_index = pd.date_range(
            start=df.index.min(),
            end=df.index.max(),
            freq='1min'
        )

        # Only during market hours (9:30 AM - 4:00 PM ET)
        market_hours = full_index[
            (full_index.time >= pd.Timestamp('09:30').time()) &
            (full_index.time <= pd.Timestamp('16:00').time()) &
            (full_index.weekday < 5)  # Monday = 0, Friday = 4
        ]

        df = df.reindex(market_hours)

        # Forward fill small gaps only
        gap_mask = df.isnull().any(axis=1)
        gap_sizes = gap_mask.groupby((~gap_mask).cumsum()).sum()
        small_gaps = gap_sizes <= self.config.max_gap_minutes

        for gap_id, is_small in small_gaps.items():
            if is_small:
                gap_rows = gap_mask & ((~gap_mask).cumsum() == gap_id)
                df.loc[gap_rows] = df.loc[gap_rows].fillna(method='ffill')

        # Reset index and clean up
        df = df.reset_index()
        df = df.dropna()  # Remove any remaining gaps

        return df

    async def _calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate technical indicators for TFT model features.

        Args:
            df: DataFrame with OHLCV data

        Returns:
            DataFrame with technical indicators
        """
        if len(df) < 50:  # Need minimum data for indicators
            return df

        # Returns
        df['returns'] = df['close'].pct_change()

        # Simple moving averages
        df['sma_5'] = df['close'].rolling(window=5, min_periods=1).mean()
        df['sma_20'] = df['close'].rolling(window=20, min_periods=1).mean()

        # Exponential moving averages
        df['ema_12'] = df['close'].ewm(span=12, min_periods=1).mean()
        df['ema_26'] = df['close'].ewm(span=26, min_periods=1).mean()

        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9, min_periods=1).mean()

        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # Bollinger Bands
        df['bb_middle'] = df['close'].rolling(window=20, min_periods=1).mean()
        bb_std = df['close'].rolling(window=20, min_periods=1).std()
        df['bb_upper'] = df['bb_middle'] + (2 * bb_std)
        df['bb_lower'] = df['bb_middle'] - (2 * bb_std)

        # Volume indicators
        df['volume_sma'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma']

        # Volatility (annualized for minute data)
        df['volatility'] = df['returns'].rolling(window=20, min_periods=1).std() * np.sqrt(252 * 390)

        return df

    async def _calculate_quality_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate data quality scores for each bar.

        Args:
            df: DataFrame with minute bars

        Returns:
            DataFrame with quality scores
        """
        # Initialize quality score
        df['quality_score'] = 1.0

        # Penalize missing volume
        df.loc[df['volume'] == 0, 'quality_score'] *= 0.8

        # Penalize extreme price movements
        if 'returns' in df.columns:
            extreme_moves = df['returns'].abs() > 0.05  # 5% moves
            df.loc[extreme_moves, 'quality_score'] *= 0.9

        # Penalize narrow spreads (potential data issues)
        spread_ratio = (df['high'] - df['low']) / df['close']
        narrow_spreads = spread_ratio < 0.001  # Less than 0.1% spread
        df.loc[narrow_spreads, 'quality_score'] *= 0.7

        # Penalize missing VWAP
        df.loc[df['vwap'].isna(), 'quality_score'] *= 0.9

        # Bonus for high volume
        high_volume = df['volume'] > df['volume'].quantile(0.8)
        df.loc[high_volume, 'quality_score'] *= 1.1

        # Cap at 1.0
        df['quality_score'] = df['quality_score'].clip(upper=1.0)

        return df

    def _calculate_symbol_quality(self, bars: List[Dict[str, Any]]) -> float:
        """
        Calculate overall quality score for a symbol.

        Args:
            bars: List of processed bars

        Returns:
            Overall quality score (0.0 to 1.0)
        """
        if not bars:
            return 0.0

        quality_scores = [bar.get('quality_score', 0.5) for bar in bars]
        return np.mean(quality_scores)

    async def _store_bars(self, bars: List[Dict[str, Any]]) -> int:
        """
        Store processed bars in database.

        Args:
            bars: List of processed bar dictionaries

        Returns:
            Number of bars stored
        """
        if not bars:
            return 0

        # Prepare insert query
        table_name = env.get_table_name('minute_bars')

        insert_query = f"""
        INSERT INTO {table_name} (
            symbol, timestamp, open, high, low, close, volume, vwap, trade_count, vendor,
            returns, sma_5, sma_20, ema_12, ema_26, macd, macd_signal, rsi,
            bb_upper, bb_middle, bb_lower, volume_sma, volume_ratio, volatility,
            quality_score, is_validated, data_source_flags
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18,
            $19, $20, $21, $22, $23, $24,
            $25, $26, $27
        ) ON CONFLICT (symbol, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            vwap = EXCLUDED.vwap,
            trade_count = EXCLUDED.trade_count,
            returns = EXCLUDED.returns,
            sma_5 = EXCLUDED.sma_5,
            sma_20 = EXCLUDED.sma_20,
            ema_12 = EXCLUDED.ema_12,
            ema_26 = EXCLUDED.ema_26,
            macd = EXCLUDED.macd,
            macd_signal = EXCLUDED.macd_signal,
            rsi = EXCLUDED.rsi,
            bb_upper = EXCLUDED.bb_upper,
            bb_middle = EXCLUDED.bb_middle,
            bb_lower = EXCLUDED.bb_lower,
            volume_sma = EXCLUDED.volume_sma,
            volume_ratio = EXCLUDED.volume_ratio,
            volatility = EXCLUDED.volatility,
            quality_score = EXCLUDED.quality_score,
            is_validated = EXCLUDED.is_validated,
            data_source_flags = EXCLUDED.data_source_flags,
            updated_at = CURRENT_TIMESTAMP
        """

        stored_count = 0

        # Process in batches
        for i in range(0, len(bars), self.config.batch_size):
            batch = bars[i:i + self.config.batch_size]

            # Prepare batch data
            batch_data = []
            for bar in batch:
                batch_data.append((
                    bar.get('symbol'),
                    bar.get('timestamp'),
                    bar.get('open'),
                    bar.get('high'),
                    bar.get('low'),
                    bar.get('close'),
                    bar.get('volume'),
                    bar.get('vwap'),
                    bar.get('trade_count'),
                    bar.get('vendor', 'polygon'),
                    bar.get('returns'),
                    bar.get('sma_5'),
                    bar.get('sma_20'),
                    bar.get('ema_12'),
                    bar.get('ema_26'),
                    bar.get('macd'),
                    bar.get('macd_signal'),
                    bar.get('rsi'),
                    bar.get('bb_upper'),
                    bar.get('bb_middle'),
                    bar.get('bb_lower'),
                    bar.get('volume_sma'),
                    bar.get('volume_ratio'),
                    bar.get('volatility'),
                    bar.get('quality_score', 0.5),
                    True,  # is_validated
                    bar.get('data_source_flags', {})
                ))

            # Execute batch insert
            async with self.pool.acquire() as conn:
                await conn.executemany(insert_query, batch_data)
                stored_count += len(batch_data)

        return stored_count

    async def refresh_materialized_views(self):
        """Refresh materialized views for TFT training."""
        table_name = env.get_table_name('minute_bars_tft_training')

        async with self.pool.acquire() as conn:
            await conn.execute(f"REFRESH MATERIALIZED VIEW {table_name}")

        logger.info("Refreshed TFT training materialized view")


# Convenience functions
async def ingest_minute_data(
    pool: asyncpg.Pool,
    symbols: List[str],
    start_date: datetime,
    end_date: datetime,
    polygon_api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Convenience function for minute data ingestion.

    Args:
        pool: Database connection pool
        symbols: List of symbols to process
        start_date: Start date for data
        end_date: End date for data
        polygon_api_key: Polygon API key

    Returns:
        Ingestion statistics
    """
    config = IngestionConfig(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        polygon_api_key=polygon_api_key
    )

    async with MinuteDataPipeline(pool, config) as pipeline:
        return await pipeline.run_ingestion()


async def backfill_symbol_data(
    pool: asyncpg.Pool,
    symbol: str,
    days_back: int = 30,
    polygon_api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Backfill minute data for a single symbol.

    Args:
        pool: Database connection pool
        symbol: Symbol to backfill
        days_back: Number of days to backfill
        polygon_api_key: Polygon API key

    Returns:
        Backfill statistics
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    return await ingest_minute_data(
        pool, [symbol], start_date, end_date, polygon_api_key
    )