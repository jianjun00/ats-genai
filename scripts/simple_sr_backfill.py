#!/usr/bin/env python3
"""
Simplified S/R Event Backfill Script

This script directly loads parquet minute bar data and uses the S/R detector
to backfill historical support/resistance events for TSLA from 2025-07-01 to now.

Usage:
    python scripts/simple_sr_backfill.py --symbol TSLA --start-date 2025-07-01
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import traceback
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import existing S/R infrastructure
from domains.analytics.events.analysis.support_resistance_detector import (
    SupportResistanceDetector, SRLevel, SRTest, SREvent,
    SRType, SRLevelType, SRTestOutcome, Timeframe
)

# Import new event system components
from domains.analytics.events.producer import EventProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleSRBackfillProcessor:
    """Simplified S/R backfill processor that loads parquet files directly"""

    def __init__(self, data_path: str = '/mnt/d/ats-data/minute-bars/firstrate'):
        self.data_path = Path(data_path)

        # Initialize S/R detector
        self.sr_detector = SupportResistanceDetector({
            'pivot_lookback': 20,
            'cluster_epsilon': 0.02,
            'proximity_tolerance': 0.005,
            'break_threshold': 0.01,
            'psychological_levels': True,
            'volume_profile_levels': True,
        })

        # Initialize event producer (will try Redis, fallback to logging)
        self.event_producer = None

        # Processing stats
        self.stats = {
            'files_loaded': 0,
            'timeframes_processed': 0,
            'sr_levels_detected': 0,
            'sr_tests_detected': 0,
            'events_published': 0,
            'errors': 0
        }

    async def initialize(self):
        """Initialize Redis connection for event publishing"""
        logger.info("🚀 Initializing Simple S/R Event Backfill Processor...")

        import redis
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
        redis_client.ping()
        self.event_producer = EventProducer(redis_client)
        logger.info("✅ Connected to Redis for event publishing")
    def load_minute_data(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Load minute bar data from parquet files"""

        logger.info(f"📥 Loading minute bar data for {symbol} from {start_date.date()} to {end_date.date()}")

        all_data = []

        # Generate list of months to load
        current_date = start_date.replace(day=1)
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month

            # Build file path
            first_letter = symbol[0]
            file_path = self.data_path / first_letter / symbol / str(year) / f"{month:02d}" / f"{symbol}_{year}_{month:02d}.parquet"

            if file_path.exists():
                logger.debug(f"📄 Loading {file_path}")
                df = pd.read_parquet(file_path)

                # Filter to date range - handle timezone issues
                df['timestamp'] = pd.to_datetime(df['timestamp'])

                # Convert filter dates to pandas timestamps for consistent comparison
                start_ts = pd.Timestamp(start_date)
                end_ts = pd.Timestamp(end_date)

                # If data is timezone-aware, make filter dates timezone-aware too
                if df['timestamp'].dt.tz is not None:
                    if start_ts.tz is None:
                        start_ts = start_ts.tz_localize('UTC')
                    if end_ts.tz is None:
                        end_ts = end_ts.tz_localize('UTC')

                df = df[(df['timestamp'] >= start_ts) & (df['timestamp'] <= end_ts)]

                if not df.empty:
                    all_data.append(df)
                    self.stats['files_loaded'] += 1
                    logger.info(f"✅ Loaded {len(df)} rows from {file_path.name}")

                logger.warning(f"📭 File not found: {file_path}")

            # Move to next month
            if month == 12:
                current_date = current_date.replace(year=year + 1, month=1)
            else:
                current_date = current_date.replace(month=month + 1)

        if all_data:
            # Combine all data
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.sort_values('timestamp')
            combined_df.set_index('timestamp', inplace=True)

            logger.info(f"📊 Total data loaded: {len(combined_df)} minute bars for {symbol}")
            return combined_df
        else:
            logger.warning(f"📭 No data loaded for {symbol}")
            return pd.DataFrame()

    def aggregate_to_timeframe(self, minute_data: pd.DataFrame, timeframe: Timeframe) -> pd.DataFrame:
        """Aggregate minute data to specified timeframe"""

        if minute_data.empty:
            return minute_data

        # Define aggregation rules
        if timeframe == Timeframe.INTRADAY_1H:
            freq = '1H'
        elif timeframe == Timeframe.DAILY:
            freq = '1D'
        elif timeframe == Timeframe.WEEKLY:
            freq = '1W'
        else:
            return minute_data  # Return as-is for minute timeframes

        # Aggregate OHLCV data
        aggregated = minute_data.resample(freq).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

        logger.debug(f"📈 Aggregated to {timeframe.value}: {len(aggregated)} bars")
        return aggregated

    async def process_sr_events(self, symbol: str, market_data: pd.DataFrame,
                              timeframe: Timeframe, limit: Optional[int] = None) -> int:
        """Process S/R events for given market data and timeframe"""

        logger.info(f"🎯 Processing S/R events for {symbol} ({timeframe.value}) - {len(market_data)} bars")

        # Adjust minimum data requirement based on timeframe
        min_data_required = {
            Timeframe.DAILY: 30,      # 30 days minimum
            Timeframe.INTRADAY_1H: 100, # 100 hours minimum
            Timeframe.WEEKLY: 20,     # 20 weeks minimum
        }.get(timeframe, 50)

        if len(market_data) < min_data_required:
            logger.warning(f"⚠️  Insufficient data for S/R analysis: {len(market_data)} bars (need {min_data_required})")
            return 0

        events_processed = 0

        # Detect S/R levels
        sr_levels = await self.sr_detector.detect_sr_levels(symbol, market_data, timeframe)
        self.stats['sr_levels_detected'] += len(sr_levels)

        if not sr_levels:
            logger.info(f"📭 No S/R levels detected for {symbol} ({timeframe.value})")
            return 0

        logger.info(f"🎯 Detected {len(sr_levels)} S/R levels for {symbol} ({timeframe.value})")

        # Log some level details for debugging
        if sr_levels and logger.isEnabledFor(logging.DEBUG):
            for i, level in enumerate(sr_levels[:3]):  # Show first 3 levels
                logger.debug(f"  Level {i+1}: {level.sr_type.value} at ${level.price:.2f} "
                           f"(strength: {level.strength:.2f}, type: {level.level_type.value})")

        # Detect S/R tests using original detector
        sr_tests = await self.sr_detector.detect_sr_tests(symbol, market_data, sr_levels)
        self.stats['sr_tests_detected'] += len(sr_tests)

        # If no tests found with original detector, create synthetic tests from price interactions
        if len(sr_tests) == 0:
            logger.debug("🔍 No tests found with original detector, creating synthetic tests from price interactions")
            sr_tests = await self._create_synthetic_sr_tests(market_data, sr_levels)
            self.stats['sr_tests_detected'] += len(sr_tests)

        logger.info(f"📈 Detected {len(sr_tests)} S/R tests for {symbol} ({timeframe.value})")

        if len(sr_tests) == 0 and len(sr_levels) > 0:
            # Debug why no tests were found
            current_price = market_data['close'].iloc[-1] if not market_data.empty else 0
            price_range = (market_data['low'].min(), market_data['high'].max()) if not market_data.empty else (0, 0)
            logger.debug(f"  Debug: Current price: ${current_price:.2f}, "
                       f"Price range: ${price_range[0]:.2f} - ${price_range[1]:.2f}")

            # Check if any levels are within reasonable range
            nearby_levels = [l for l in sr_levels if price_range[0] * 0.9 <= l.price <= price_range[1] * 1.1]
            logger.debug(f"  Debug: {len(nearby_levels)} levels within price range")

        # Process and publish events
        for i, test in enumerate(sr_tests):
            if limit and events_processed >= limit:
                break

            # Find corresponding level
            level = next((l for l in sr_levels if self._level_matches_test(l, test)), None)
            if not level:
                continue

            # Check if test is significant
            if not self._is_significant_test(test, level):
                continue

            # Publish or log event
            success = await self._publish_sr_event(symbol, level, test, timeframe)
            if success:
                events_processed += 1
                self.stats['events_published'] += 1

                if events_processed % 5 == 0:
                    logger.info(f"📤 Processed {events_processed} S/R events for {symbol} ({timeframe.value})")

        return events_processed

    async def _create_synthetic_sr_tests(self, market_data: pd.DataFrame, sr_levels: List[SRLevel]) -> List[SRTest]:
        """Create synthetic S/R tests by analyzing price interactions with levels"""

        synthetic_tests = []

        if market_data.empty or not sr_levels:
            return synthetic_tests

        # Look for price interactions with each level
        for level in sr_levels:
            # Define proximity threshold (0.8% of level price)
            proximity_threshold = level.price * 0.008

            # Find bars where price came close to the level
            if level.sr_type == SRType.SUPPORT:
                # For support, check when low price came near the level
                close_interactions = market_data[
                    (market_data['low'] <= level.price + proximity_threshold) &
                    (market_data['low'] >= level.price - proximity_threshold)
                ]
            else:  # RESISTANCE
                # For resistance, check when high price came near the level
                close_interactions = market_data[
                    (market_data['high'] >= level.price - proximity_threshold) &
                    (market_data['high'] <= level.price + proximity_threshold)
                ]

            # Create tests for significant interactions
            for idx, row in close_interactions.iterrows():
                # Determine approach direction by looking at previous bars
                bar_position = market_data.index.get_loc(idx)
                if bar_position >= 3:  # Need some history
                    prev_bars = market_data.iloc[bar_position-3:bar_position]
                    avg_prev_price = prev_bars['close'].mean()

                    if level.sr_type == SRType.SUPPORT:
                        approach_direction = "from_above" if avg_prev_price > level.price else "from_below"
                        test_price = row['low']
                        penetration = max(0, level.price - row['low']) / level.price
                    else:  # RESISTANCE
                        approach_direction = "from_below" if avg_prev_price < level.price else "from_above"
                        test_price = row['high']
                        penetration = max(0, row['high'] - level.price) / level.price

                    # Determine test outcome based on subsequent price action
                    bars_after = market_data.iloc[bar_position:min(bar_position+5, len(market_data))]
                    outcome = self._determine_test_outcome(level, row, bars_after, penetration)

                    # Calculate volume spike if volume data available
                    volume_spike = 1.0
                    if 'volume' in market_data.columns:
                        avg_volume = market_data['volume'].rolling(20).mean().iloc[bar_position]
                        if avg_volume > 0:
                            volume_spike = row['volume'] / avg_volume

                    # Generate unique test ID
                    level_id = f"{level.sr_type.value}_{level.price:.2f}_{level.level_type.value}"

                    # Create synthetic test
                    test = SRTest(
                        level_id=level_id,
                        test_datetime=idx,
                        test_price=test_price,
                        approach_direction=approach_direction,
                        max_penetration=penetration,
                        hold_duration=timedelta(minutes=60),  # Simplified
                        volume_spike=volume_spike,
                        outcome=outcome,
                        confidence=self._calculate_test_confidence(level, penetration, volume_spike, outcome),
                        timeframe=level.timeframe
                    )

                    synthetic_tests.append(test)

        logger.debug(f"🔧 Created {len(synthetic_tests)} synthetic S/R tests")
        return synthetic_tests[:10]  # Limit to top 10 tests

    def _determine_test_outcome(self, level: SRLevel, test_bar: pd.Series, subsequent_bars: pd.DataFrame, penetration: float) -> SRTestOutcome:
        """Determine the outcome of an S/R test based on subsequent price action"""

        if subsequent_bars.empty:
            return SRTestOutcome.PENDING

        # Significant penetration usually means a break
        if penetration > 0.015:  # 1.5% penetration
            # Check if price stayed on the other side (clean break) or returned (false break)
            if level.sr_type == SRType.SUPPORT:
                stayed_broken = (subsequent_bars['close'] < level.price * 0.99).sum() >= len(subsequent_bars) * 0.6
            else:  # RESISTANCE
                stayed_broken = (subsequent_bars['close'] > level.price * 1.01).sum() >= len(subsequent_bars) * 0.6

            return SRTestOutcome.BREAK_CLEAN if stayed_broken else SRTestOutcome.BREAK_FALSE

        # Minor penetration
        elif penetration > 0.005:  # 0.5% penetration
            return SRTestOutcome.PENETRATION

        # Clean hold with strong bounce
        elif self._has_strong_bounce(level, test_bar, subsequent_bars):
            return SRTestOutcome.HOLD_STRONG

        # Weak hold
        else:
            return SRTestOutcome.HOLD_WEAK

    def _has_strong_bounce(self, level: SRLevel, test_bar: pd.Series, subsequent_bars: pd.DataFrame) -> bool:
        """Check if there was a strong bounce from the level"""

        if subsequent_bars.empty:
            return False

        # Look for price moving away from the level after test
        if level.sr_type == SRType.SUPPORT:
            # For support, look for upward movement
            price_move = subsequent_bars['close'].max() - test_bar['low']
            return price_move > level.price * 0.02  # 2% bounce
        else:  # RESISTANCE
            # For resistance, look for downward movement
            price_move = test_bar['high'] - subsequent_bars['close'].min()
            return price_move > level.price * 0.02  # 2% rejection

    def _calculate_test_confidence(self, level: SRLevel, penetration: float, volume_spike: float, outcome: SRTestOutcome) -> float:
        """Calculate confidence score for a synthetic test"""

        confidence = 0.5  # Base confidence for synthetic tests

        # Level strength contributes to confidence
        confidence += level.strength * 0.3

        # Volume confirmation
        if volume_spike > 1.5:
            confidence += 0.2
        elif volume_spike > 1.2:
            confidence += 0.1

        # Outcome clarity
        if outcome in [SRTestOutcome.BREAK_CLEAN, SRTestOutcome.HOLD_STRONG]:
            confidence += 0.2
        elif outcome in [SRTestOutcome.BREAK_FALSE, SRTestOutcome.PENETRATION]:
            confidence += 0.1

        return min(1.0, confidence)

    def _level_matches_test(self, level: SRLevel, test: SRTest) -> bool:
        """Check if level matches test (price proximity)"""
        price_diff = abs(level.price - test.test_price) / level.price
        return price_diff < 0.01  # Within 1%

    def _is_significant_test(self, test: SRTest, level: SRLevel) -> bool:
        """Check if test is significant enough to create event"""
        significant_outcomes = {
            SRTestOutcome.HOLD_STRONG,
            SRTestOutcome.HOLD_WEAK,     # Include weak holds
            SRTestOutcome.BREAK_CLEAN,
            SRTestOutcome.BREAK_FALSE,
            SRTestOutcome.PENETRATION
        }

        # Be more lenient on thresholds to catch more events
        return (test.outcome in significant_outcomes and
                test.confidence >= 0.3 and  # Lower confidence threshold
                level.strength >= 0.3)      # Lower strength threshold

    async def _publish_sr_event(self, symbol: str, level: SRLevel, test: SRTest, timeframe: Timeframe) -> bool:
        """Publish or log S/R event"""

        # Map test outcome to signal properties
        signal_type = self._get_signal_type(test.outcome, level.sr_type)
        signal_direction = self._get_signal_direction(test.outcome, level.sr_type)
        signal_strength = min(1.0, (level.strength * 0.6 + test.confidence * 0.4))

        if self.event_producer:
            # Publish via EventProducer
            indicator = f"SR_{level.sr_type.value.upper()}_{level.level_type.value}_{timeframe.value}"

            event_id = self.event_producer.publish_technical_signal_event(
                symbol=symbol,
                signal_type=signal_type,
                direction=signal_direction,
                strength=signal_strength,
                current_price=test.test_price,
                indicator=indicator,
                source="sr_backfill"
            )

            if event_id:
                logger.debug(f"📤 Published S/R event {event_id}")
                return True
            else:
                return False

            logger.info(f"📝 S/R EVENT: {symbol} {signal_type.upper()} {signal_direction.upper()} - "
                       f"{level.sr_type.value} at ${level.price:.2f} ({timeframe.value}) - "
                       f"outcome: {test.outcome.value} (strength: {signal_strength:.2f})")
            return True

    def _get_signal_type(self, outcome: SRTestOutcome, sr_type: SRType) -> str:
        """Map test outcome to signal type"""
        if outcome in [SRTestOutcome.BREAK_CLEAN, SRTestOutcome.BREAK_FALSE]:
            return "breakout" if sr_type == SRType.RESISTANCE else "breakdown"
        elif outcome in [SRTestOutcome.HOLD_STRONG, SRTestOutcome.HOLD_WEAK]:
            return "reversal"
        elif outcome == SRTestOutcome.PENETRATION:
            return "continuation"
        else:
            return "test"

    def _get_signal_direction(self, outcome: SRTestOutcome, sr_type: SRType) -> str:
        """Map test outcome to signal direction"""
        if outcome == SRTestOutcome.BREAK_CLEAN:
            return "bullish" if sr_type == SRType.RESISTANCE else "bearish"
        elif outcome in [SRTestOutcome.HOLD_STRONG, SRTestOutcome.HOLD_WEAK]:
            return "bullish" if sr_type == SRType.SUPPORT else "bearish"
        else:
            return "neutral"

    async def run_backfill(self, symbol: str, start_date: datetime,
                          end_date: datetime, limit: Optional[int] = None) -> Dict:
        """Run complete backfill process"""

        logger.info(f"🔄 Starting S/R backfill for {symbol}")
        logger.info(f"📅 Date range: {start_date.date()} to {end_date.date()}")
        if limit:
            logger.info(f"🔢 Event limit: {limit}")

        start_time = datetime.now()

        # Load minute data
        minute_data = self.load_minute_data(symbol, start_date, end_date)

        if minute_data.empty:
            return {
                'success': False,
                'error': 'No market data loaded',
                'events_processed': 0
            }

        # Process different timeframes
        timeframes = [Timeframe.DAILY, Timeframe.INTRADAY_1H]
        total_events = 0

        for timeframe in timeframes:
            if limit and total_events >= limit:
                break

            # Aggregate data to timeframe
            timeframe_data = self.aggregate_to_timeframe(minute_data, timeframe)

            if timeframe_data.empty:
                continue

            # Process S/R events for this timeframe
            remaining_limit = limit - total_events if limit else None
            events_processed = await self.process_sr_events(
                symbol, timeframe_data, timeframe, remaining_limit
            )

            total_events += events_processed
            self.stats['timeframes_processed'] += 1

            logger.info(f"✅ Processed {events_processed} events for {symbol} ({timeframe.value})")

        processing_time = (datetime.now() - start_time).total_seconds()

        logger.info(f"🎉 Backfill completed for {symbol}")
        logger.info(f"📊 Total events processed: {total_events}")
        logger.info(f"⏱️  Processing time: {processing_time:.1f} seconds")

        return {
            'success': True,
            'symbol': symbol,
            'events_processed': total_events,
            'processing_time_seconds': processing_time,
            'stats': self.stats
        }

async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Simple S/R Event Backfill")
    parser.add_argument('--symbol', type=str, default='TSLA', help='Symbol to process')
    parser.add_argument('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD, default: today)')
    parser.add_argument('--limit', type=int, help='Limit number of events')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')

    args = parser.parse_args()

    # Configure logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else datetime.now()
    processor = SimpleSRBackfillProcessor()

    await processor.initialize()
    result = await processor.run_backfill(args.symbol, start_date, end_date, args.limit)

    if result['success']:
        logger.info("🎉 Backfill completed successfully!")
        logger.info(f"📊 Final stats: {result['stats']}")
    else:
        logger.error(f"❌ Backfill failed: {result['error']}")

if __name__ == "__main__":
    asyncio.run(main())