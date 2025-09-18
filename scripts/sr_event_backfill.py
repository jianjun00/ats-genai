#!/usr/bin/env python3
"""
Support/Resistance Event System Backfill Script

This script integrates the existing S/R detection system with the new Protocol Buffer event system
to backfill historical support/resistance events. It processes historical minute bar data,
detects S/R events, and publishes them through the event system for storage and correlation.

Usage:
    python scripts/sr_event_backfill.py --symbol TSLA --start-date 2025-07-01 --end-date 2025-09-08
    python scripts/sr_event_backfill.py --symbol TSLA --start-date 2025-07-01 --limit 100
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import traceback

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import existing S/R infrastructure
from domains.analytics.events.analysis.support_resistance_detector import (
    SupportResistanceDetector, SRLevel, SRTest, SREvent,
    SRType, SRLevelType, SRTestOutcome, Timeframe
)
from domains.analytics.events.processors.support_resistance_processor import SupportResistanceProcessor

# Import new event system components
from domains.analytics.events.producer import EventProducer

# Import market data infrastructure
from core.shared.data_handling.utils.environment import Environment
from domains.market_data.services.core.minute.file_based_minute_market_data_manager import (
    FileBasedMinuteMarketDataManager
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SREventBackfillProcessor:
    """
    Backfill processor that bridges existing S/R detection with Protocol Buffer event system
    """

    def __init__(self):
        self.env = Environment()

        # Initialize existing S/R infrastructure
        self.sr_detector = SupportResistanceDetector({
            'pivot_lookback': 20,
            'cluster_epsilon': 0.02,
            'proximity_tolerance': 0.005,
            'break_threshold': 0.01,
            'psychological_levels': True,
            'volume_profile_levels': True,
            'timeframes': [
                Timeframe.INTRADAY_1H,
                Timeframe.DAILY,
                Timeframe.WEEKLY
            ]
        })

        # Initialize market data manager
        self.market_data_manager = None

        # Initialize event producer (will be set up with Redis connection)
        self.event_producer = None

        # Processing stats
        self.stats = {
            'symbols_processed': 0,
            'timeframes_processed': 0,
            'sr_levels_detected': 0,
            'sr_tests_detected': 0,
            'events_published': 0,
            'errors': 0,
            'processing_time_seconds': 0
        }

    async def initialize(self):
        """Initialize all components"""
        logger.info("🚀 Initializing S/R Event Backfill Processor...")

        try:
            # Initialize market data manager
            self.market_data_manager = FileBasedMinuteMarketDataManager(
                env=self.env,
                base_path='/mnt/d/ats-data/minute-bars/firstrate'
            )

            # Initialize event producer with Redis
            # Note: This would need Redis running - for now we'll mock it
            try:
                import redis
                redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
                redis_client.ping()  # Test connection
                self.event_producer = EventProducer(redis_client)
                logger.info("✅ Connected to Redis for event publishing")
            except Exception as e:
                logger.warning(f"⚠️  Could not connect to Redis: {e}")
                logger.warning("📝 Events will be logged instead of published to Redis")
                self.event_producer = None

            logger.info("✅ S/R Event Backfill Processor initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize processor: {e}")
            raise

    async def backfill_sr_events(self, symbol: str, start_date: datetime,
                                end_date: datetime, limit: Optional[int] = None) -> Dict:
        """
        Backfill S/R events for a symbol within date range

        Args:
            symbol: Stock symbol (e.g., 'TSLA')
            start_date: Start date for backfill
            end_date: End date for backfill
            limit: Optional limit on number of events to process

        Returns:
            Dictionary with processing statistics
        """
        logger.info(f"🔄 Starting S/R event backfill for {symbol}")
        logger.info(f"📅 Date range: {start_date.date()} to {end_date.date()}")
        if limit:
            logger.info(f"🔢 Processing limit: {limit} events")

        start_time = datetime.now()

        try:
            # Process each timeframe
            timeframes_to_process = [
                Timeframe.DAILY,      # Start with daily for major levels
                Timeframe.INTRADAY_1H, # Then hourly for more frequent events
                # Timeframe.WEEKLY    # Skip weekly for now - can be added later
            ]

            total_events_processed = 0

            for timeframe in timeframes_to_process:
                if limit and total_events_processed >= limit:
                    break

                logger.info(f"🎯 Processing {symbol} on {timeframe.value} timeframe...")

                # Get market data for this timeframe
                market_data = await self._get_market_data(symbol, timeframe, start_date, end_date)

                if market_data is None or market_data.empty:
                    logger.warning(f"⚠️  No market data for {symbol} ({timeframe.value})")
                    continue

                logger.info(f"📊 Retrieved {len(market_data)} data points for {symbol} ({timeframe.value})")

                # Detect S/R levels
                sr_levels = await self.sr_detector.detect_sr_levels(symbol, market_data, timeframe)
                self.stats['sr_levels_detected'] += len(sr_levels)

                if not sr_levels:
                    logger.info(f"📭 No S/R levels detected for {symbol} ({timeframe.value})")
                    continue

                logger.info(f"🎯 Detected {len(sr_levels)} S/R levels for {symbol} ({timeframe.value})")

                # Detect S/R tests (price interactions with levels)
                sr_tests = await self.sr_detector.detect_sr_tests(symbol, market_data, sr_levels)
                self.stats['sr_tests_detected'] += len(sr_tests)

                logger.info(f"📈 Detected {len(sr_tests)} S/R tests for {symbol} ({timeframe.value})")

                # Convert S/R events to Protocol Buffer events and publish
                events_processed = await self._process_sr_events(
                    symbol, sr_levels, sr_tests, timeframe, market_data,
                    limit - total_events_processed if limit else None
                )

                total_events_processed += events_processed
                self.stats['timeframes_processed'] += 1

                logger.info(f"✅ Processed {events_processed} events for {symbol} ({timeframe.value})")

            # Update final stats
            self.stats['symbols_processed'] += 1
            processing_time = (datetime.now() - start_time).total_seconds()
            self.stats['processing_time_seconds'] += processing_time

            logger.info(f"🎉 Completed backfill for {symbol}")
            logger.info(f"📊 Total events processed: {total_events_processed}")
            logger.info(f"⏱️  Processing time: {processing_time:.1f} seconds")

            return {
                'success': True,
                'symbol': symbol,
                'events_processed': total_events_processed,
                'processing_time_seconds': processing_time,
                'timeframes_processed': len(timeframes_to_process),
                'sr_levels_detected': len(sr_levels) if 'sr_levels' in locals() else 0,
                'sr_tests_detected': len(sr_tests) if 'sr_tests' in locals() else 0
            }

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"❌ Error during backfill for {symbol}: {e}")
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'symbol': symbol,
                'error': str(e),
                'events_processed': total_events_processed if 'total_events_processed' in locals() else 0
            }

    async def _get_market_data(self, symbol: str, timeframe: Timeframe,
                              start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """Get historical market data for the specified parameters"""

        if not self.market_data_manager:
            logger.error("Market data manager not initialized")
            return None

        try:
            # Convert S/R timeframe to minutes for market data manager
            timeframe_minutes = self._timeframe_to_minutes(timeframe)

            logger.debug(f"📥 Fetching {timeframe.value} data for {symbol} "
                        f"from {start_date.date()} to {end_date.date()}")

            # Get data batch
            data_batch = await self.market_data_manager.get_minute_ohlc_batch(
                symbols=[symbol],
                start=start_date,
                end=end_date,
                timeframe_minutes=timeframe_minutes
            )

            if symbol in data_batch and not data_batch[symbol].empty:
                df = data_batch[symbol]
                logger.debug(f"✅ Retrieved {len(df)} rows of {timeframe.value} data for {symbol}")
                return df
            else:
                logger.warning(f"📭 No data returned for {symbol} ({timeframe.value})")
                return None

        except Exception as e:
            logger.error(f"❌ Error fetching market data for {symbol} ({timeframe.value}): {e}")
            return None

    def _timeframe_to_minutes(self, timeframe: Timeframe) -> int:
        """Convert S/R Timeframe to minutes"""
        mapping = {
            Timeframe.INTRADAY_1M: 1,
            Timeframe.INTRADAY_5M: 5,
            Timeframe.INTRADAY_15M: 15,
            Timeframe.INTRADAY_1H: 60,
            Timeframe.DAILY: 1440,     # 24 * 60
            Timeframe.WEEKLY: 10080,   # 7 * 24 * 60
            Timeframe.MONTHLY: 43200,  # 30 * 24 * 60 (approximate)
        }
        return mapping.get(timeframe, 1440)  # Default to daily

    async def _process_sr_events(self, symbol: str, sr_levels: List[SRLevel],
                                sr_tests: List[SRTest], timeframe: Timeframe,
                                market_data: pd.DataFrame, limit: Optional[int] = None) -> int:
        """
        Convert S/R levels and tests to Protocol Buffer events and publish them
        """
        events_processed = 0

        # Create events for significant S/R tests
        for i, test in enumerate(sr_tests):
            if limit and events_processed >= limit:
                break

            try:
                # Find the corresponding level for this test
                level = next((l for l in sr_levels if self._level_matches_test(l, test)), None)
                if not level:
                    continue

                # Only create events for significant outcomes
                if not self._is_significant_test(test, level):
                    continue

                # Publish technical signal event directly via producer
                if self.event_producer:
                    success = await self._publish_sr_event_via_producer(
                        symbol, level, test, timeframe, market_data
                    )
                else:
                    success = await self._log_sr_event(symbol, level, test, timeframe)
                    if success:
                        events_processed += 1
                        self.stats['events_published'] += 1

                        if events_processed % 10 == 0:
                            logger.info(f"📤 Published {events_processed} events for {symbol} ({timeframe.value})")

            except Exception as e:
                logger.error(f"❌ Error processing S/R test {i} for {symbol}: {e}")
                self.stats['errors'] += 1
                continue

        return events_processed

    def _level_matches_test(self, level: SRLevel, test: SRTest) -> bool:
        """Check if a level matches a test (simplified matching)"""
        # Simple price proximity check
        price_diff = abs(level.price - test.test_price) / level.price
        return price_diff < 0.01  # Within 1%

    def _is_significant_test(self, test: SRTest, level: SRLevel) -> bool:
        """Determine if an S/R test is significant enough to create an event"""
        # Only create events for meaningful outcomes
        significant_outcomes = {
            SRTestOutcome.HOLD_STRONG,
            SRTestOutcome.BREAK_CLEAN,
            SRTestOutcome.BREAK_FALSE,
            SRTestOutcome.PENETRATION
        }

        if test.outcome not in significant_outcomes:
            return False

        # Must have reasonable confidence
        if test.confidence < 0.5:
            return False

        # Level must have reasonable strength
        if level.strength < 0.4:
            return False

        return True

    async def _publish_sr_event_via_producer(self, symbol: str, level: SRLevel,
                                            test: SRTest, timeframe: Timeframe,
                                            market_data: pd.DataFrame) -> bool:
        """Publish S/R event via EventProducer convenience method"""

        try:
            # Determine signal type based on S/R test outcome
            signal_type = self._get_signal_type(test.outcome, level.sr_type)

            # Determine signal direction
            signal_direction = self._get_signal_direction_string(test.outcome, level.sr_type)

            # Calculate signal strength (combine level strength and test confidence)
            signal_strength = min(1.0, (level.strength * 0.6 + test.confidence * 0.4))

            # Create indicator name that describes this S/R signal
            indicator = f"SR_{level.sr_type.value.upper()}_{level.level_type.value}_{timeframe.value}"

            # Publish via EventProducer
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
                logger.debug(f"📤 Published S/R event {event_id}: {symbol} {signal_type} {signal_direction} "
                           f"(strength: {signal_strength:.2f}) at ${test.test_price:.2f}")
                return True
            else:
                return False

        except Exception as e:
            logger.error(f"❌ Error publishing S/R event via producer: {e}")
            return False

    async def _log_sr_event(self, symbol: str, level: SRLevel, test: SRTest, timeframe: Timeframe) -> bool:
        """Log S/R event when Redis is not available"""

        signal_type = self._get_signal_type(test.outcome, level.sr_type)
        signal_direction = self._get_signal_direction_string(test.outcome, level.sr_type)
        signal_strength = min(1.0, (level.strength * 0.6 + test.confidence * 0.4))

        logger.info(f"📝 S/R EVENT: {symbol} {signal_type.upper()} {signal_direction.upper()} - "
                   f"{level.sr_type.value} at ${level.price:.2f} ({timeframe.value}) - "
                   f"outcome: {test.outcome.value} (strength: {signal_strength:.2f}, "
                   f"volume: {test.volume_spike:.1f}x, penetration: {test.max_penetration:.3f})")
        return True

    def _get_signal_type(self, outcome: SRTestOutcome, sr_type: SRType) -> str:
        """Map S/R test outcome to signal type"""
        if outcome in [SRTestOutcome.BREAK_CLEAN, SRTestOutcome.BREAK_FALSE]:
            return "breakout" if sr_type == SRType.RESISTANCE else "breakdown"
        elif outcome in [SRTestOutcome.HOLD_STRONG, SRTestOutcome.HOLD_WEAK]:
            return "reversal"
        elif outcome == SRTestOutcome.PENETRATION:
            return "continuation"
        else:
            return "test"

    def _get_signal_direction_string(self, outcome: SRTestOutcome, sr_type: SRType) -> str:
        """Map S/R test outcome to signal direction string"""
        if outcome == SRTestOutcome.BREAK_CLEAN:
            return "bullish" if sr_type == SRType.RESISTANCE else "bearish"
        elif outcome in [SRTestOutcome.HOLD_STRONG, SRTestOutcome.HOLD_WEAK]:
            return "bullish" if sr_type == SRType.SUPPORT else "bearish"
        else:
            return "neutral"

    def _get_price_context(self, market_data: pd.DataFrame, timestamp: datetime,
                          test_price: float) -> Dict:
        """Get price context around the test time"""
        try:
            # Find the closest data point to the test timestamp
            if hasattr(market_data.index, 'to_pydatetime'):
                data_timestamps = pd.to_datetime(market_data.index)
            else:
                data_timestamps = market_data.index

            closest_idx = (data_timestamps - timestamp).abs().argmin()
            closest_row = market_data.iloc[closest_idx]

            return {
                'open': float(closest_row['open']),
                'high': float(closest_row['high']),
                'low': float(closest_row['low']),
                'close': float(closest_row['close']),
                'volume': float(closest_row.get('volume', 0))
            }

        except Exception as e:
            logger.warning(f"Could not get price context: {e}")
            return {
                'open': test_price,
                'high': test_price,
                'low': test_price,
                'close': test_price,
                'volume': 0
            }


    def get_stats(self) -> Dict:
        """Get processing statistics"""
        return dict(self.stats)

    async def close(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up S/R Event Backfill Processor...")

        try:
            if self.market_data_manager:
                await self.market_data_manager.close()

            if self.event_producer and hasattr(self.event_producer, 'close'):
                await self.event_producer.close()

            logger.info("✅ S/R Event Backfill Processor closed")

        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")

async def main():
    """Main function for running S/R event backfill"""
    parser = argparse.ArgumentParser(description="Support/Resistance Event System Backfill")
    parser.add_argument('--symbol', type=str, default='TSLA',
                       help='Symbol to backfill (default: TSLA)')
    parser.add_argument('--start-date', type=str, required=True,
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str,
                       help='End date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of events to process')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')

    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')

        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        else:
            end_date = datetime.now()

    except ValueError as e:
        logger.error(f"❌ Invalid date format: {e}")
        logger.error("Use YYYY-MM-DD format (e.g., 2025-07-01)")
        return

    # Initialize and run processor
    processor = SREventBackfillProcessor()

    try:
        await processor.initialize()

        logger.info("🚀 Starting S/R Event Backfill Process")
        logger.info(f"🎯 Symbol: {args.symbol}")
        logger.info(f"📅 Date Range: {start_date.date()} to {end_date.date()}")

        # Run backfill
        result = await processor.backfill_sr_events(
            symbol=args.symbol,
            start_date=start_date,
            end_date=end_date,
            limit=args.limit
        )

        # Print results
        if result['success']:
            logger.info("🎉 Backfill completed successfully!")
            logger.info(f"📊 Events processed: {result['events_processed']}")
            logger.info(f"⏱️  Processing time: {result['processing_time_seconds']:.1f} seconds")
        else:
            logger.error(f"❌ Backfill failed: {result['error']}")

        # Print final statistics
        stats = processor.get_stats()
        logger.info("\n📈 Final Statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")

    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        logger.error(traceback.format_exc())
    finally:
        await processor.close()

if __name__ == "__main__":
    asyncio.run(main())