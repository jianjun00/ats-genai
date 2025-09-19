#!/usr/bin/env python3
"""
Support/Resistance Event Processor

Real-time processor that integrates with the existing market data infrastructure
to detect, track, and emit support/resistance events across multiple timeframes.

Features:
- Real-time S/R level detection and tracking
- Integration with existing market data feeds
- Event emission to main event system
- Cross-timeframe analysis and validation
- Performance optimization for high-frequency processing
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import pandas as pd
from decimal import Decimal
import json

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from domains.analytics.events.analysis.support_resistance_detector import (
    SupportResistanceDetector, SRLevel, SRTest, SREvent,
    SRType, SRLevelType, SRTestOutcome, Timeframe
)
from core.platform.config.environment import Environment
from domains.market_data.services.core.minute.file_based_minute_market_data_manager import (
    FileBasedMinuteMarketDataManager
)
# Optional logging import
try:
    from core.logging.logger_config import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

class SupportResistanceProcessor:
    """
    Real-time processor for support/resistance events

    Integrates with market data feeds and existing event system to provide
    comprehensive S/R analysis and event generation.
    """

    def __init__(self, config: Dict = None):
        self.config = config or self._default_config()
        self.env = Environment()

        # Initialize S/R detector
        self.detector = SupportResistanceDetector(self.config.get('detector_config', {}))

        # Processing state
        self.active_symbols: Set[str] = set()
        self.last_processed: Dict[str, Dict[Timeframe, datetime]] = {}
        self.processing_queue = asyncio.Queue()

        # Performance tracking
        self.stats = {
            'levels_detected': 0,
            'tests_identified': 0,
            'events_generated': 0,
            'processing_time_ms': 0,
            'symbols_processed': 0,
            'errors': 0
        }

        # Database connection pool
        self.db_pool = None

        # Market data manager (will be initialized in initialize())
        self.market_data_manager: Optional[FileBasedMinuteMarketDataManager] = None

        logger.info("SupportResistanceProcessor initialized")

    def _default_config(self) -> Dict:
        """Default processor configuration"""
        return {
            'processing_interval_seconds': 300,  # 5 minutes
            'batch_size': 100,
            'max_concurrent_symbols': 50,
            'enable_cross_timeframe_validation': True,
            'min_data_points': 100,
            'minute_bars_path': '/mnt/d/ats-data/minute-bars/firstrate',  # Market data path
            'alert_thresholds': {
                'strong_level_test': 0.8,
                'level_break': 0.7,
                'confluence_level': 0.9
            },
            'timeframe_priorities': [
                Timeframe.DAILY,
                Timeframe.INTRADAY_1H,
                Timeframe.WEEKLY,
                Timeframe.INTRADAY_15M
            ],
            'detector_config': {
                'pivot_lookback': 20,
                'cluster_epsilon': 0.02,
                'proximity_tolerance': 0.005,
                'break_threshold': 0.01,
                'psychological_levels': True,
                'volume_profile_levels': True
            }
        }

    async def initialize(self):
        """Initialize database connections and processing state"""
        logger.info("Initializing SupportResistanceProcessor...")

        try:
            # Initialize database connection pool
            self.db_pool = await self.env.database.create_pool_with_retry(max_retries=3)

            # Initialize market data manager
            self.market_data_manager = FileBasedMinuteMarketDataManager(
                env=self.env,
                base_path=self.config.get('minute_bars_path', '/mnt/d/ats-data/minute-bars')
            )

            # Load active symbols from database
            await self._load_active_symbols()

            # Initialize processing state
            await self._initialize_processing_state()

            logger.info(f"Processor initialized with {len(self.active_symbols)} active symbols")

        except Exception as e:
            logger.error(f"Failed to initialize processor: {e}")
            raise

    async def _load_active_symbols(self):
        """Load active symbols from instruments table"""
        query = f"""
        SELECT DISTINCT symbol
        FROM {self.env.get_table_name('instruments')}
        WHERE active = true
          AND symbol IS NOT NULL
          AND symbol ~ '^[A-Z]{{1,5}}$'
        ORDER BY symbol
        LIMIT 1000
        """

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query)
            self.active_symbols = {row['symbol'] for row in rows}

        logger.info(f"Loaded {len(self.active_symbols)} active symbols")

    async def _initialize_processing_state(self):
        """Initialize processing timestamps for all symbols"""
        for symbol in self.active_symbols:
            self.last_processed[symbol] = {}
            for timeframe in self.config['timeframe_priorities']:
                # Start processing from 1 day ago
                self.last_processed[symbol][timeframe] = datetime.now() - timedelta(days=1)

    async def process_market_data_update(self, symbol: str, ohlcv_data: pd.DataFrame, timeframe: Timeframe):
        """
        Process new market data and detect S/R events

        Args:
            symbol: Stock symbol
            ohlcv_data: OHLCV price data
            timeframe: Data timeframe
        """
        if symbol not in self.active_symbols:
            return

        start_time = datetime.now()

        try:
            # Ensure sufficient data for analysis
            if len(ohlcv_data) < self.config['min_data_points']:
                logger.debug(f"Insufficient data for {symbol} ({timeframe.value}): {len(ohlcv_data)} points")
                return

            # Detect S/R levels
            levels = await self.detector.detect_sr_levels(symbol, ohlcv_data, timeframe)
            self.stats['levels_detected'] += len(levels)

            if not levels:
                return

            # Store/update levels in database
            stored_levels = await self._store_sr_levels(symbol, levels, timeframe)

            # Detect tests of existing levels
            tests = await self.detector.detect_sr_tests(symbol, ohlcv_data, levels)
            self.stats['tests_identified'] += len(tests)

            if tests:
                # Store tests and generate events
                await self._process_sr_tests(symbol, tests, timeframe)

            # Update processing timestamp
            self.last_processed[symbol][timeframe] = datetime.now()

            # Track performance
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            self.stats['processing_time_ms'] += processing_time
            self.stats['symbols_processed'] += 1

            logger.debug(f"Processed {symbol} ({timeframe.value}): {len(levels)} levels, {len(tests)} tests in {processing_time:.1f}ms")

        except Exception as e:
            logger.error(f"Error processing {symbol} ({timeframe.value}): {e}")
            self.stats['errors'] += 1
            raise

    async def _store_sr_levels(self, symbol: str, levels: List[SRLevel], timeframe: Timeframe) -> List[int]:
        """Store S/R levels in database"""
        if not levels:
            return []

        stored_ids = []

        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                for level in levels:
                    # Generate unique level ID
                    level_id = f"{symbol}_{timeframe.value}_{level.sr_type.value}_{level.price:.6f}_{int(level.first_established.timestamp())}"

                    # Insert or update level
                    query = """
                    INSERT INTO dev_sr_levels (
                        level_id, symbol, price, sr_type, level_type, timeframe,
                        strength, confidence, first_established, last_tested,
                        test_count, hold_count, break_count, volume_confirmation,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (level_id) DO UPDATE SET
                        strength = EXCLUDED.strength,
                        confidence = EXCLUDED.confidence,
                        last_tested = GREATEST(dev_sr_levels.last_tested, EXCLUDED.last_tested),
                        test_count = GREATEST(dev_sr_levels.test_count, EXCLUDED.test_count),
                        hold_count = GREATEST(dev_sr_levels.hold_count, EXCLUDED.hold_count),
                        break_count = GREATEST(dev_sr_levels.break_count, EXCLUDED.break_count),
                        volume_confirmation = EXCLUDED.volume_confirmation OR dev_sr_levels.volume_confirmation,
                        metadata = dev_sr_levels.metadata || EXCLUDED.metadata,
                        updated_at = NOW()
                    RETURNING id
                    """

                    result = await conn.fetchrow(
                        query,
                        level_id, symbol, Decimal(str(level.price)),
                        level.sr_type.value, level.level_type.value, timeframe.value,
                        Decimal(str(level.strength)), Decimal(str(level.confidence)),
                        level.first_established, level.last_tested,
                        level.test_count, level.hold_count, level.break_count,
                        level.volume_confirmation, json.dumps(level.metadata)
                    )

                    if result:
                        stored_ids.append(result['id'])

        logger.debug(f"Stored {len(stored_ids)} S/R levels for {symbol}")
        return stored_ids

    async def _process_sr_tests(self, symbol: str, tests: List[SRTest], timeframe: Timeframe):
        """Process S/R tests and generate events"""
        for test in tests:
            try:
                # Store test in database
                test_db_id = await self._store_sr_test(symbol, test, timeframe)

                if test_db_id:
                    # Generate event based on test outcome
                    event = await self._create_sr_event(symbol, test, test_db_id, timeframe)

                    if event:
                        # Emit event to main event system
                        await self._emit_sr_event(event)
                        self.stats['events_generated'] += 1

            except Exception as e:
                logger.error(f"Error processing test for {symbol}: {e}")
                continue

    async def _store_sr_test(self, symbol: str, test: SRTest, timeframe: Timeframe) -> Optional[int]:
        """Store S/R test in database"""
        try:
            async with self.db_pool.acquire() as conn:
                # Find the level ID
                level_query = """
                SELECT id FROM dev_sr_levels
                WHERE level_id = $1 AND symbol = $2 AND timeframe = $3
                LIMIT 1
                """

                level_result = await conn.fetchrow(level_query, test.level_id, symbol, timeframe.value)
                if not level_result:
                    logger.warning(f"Could not find level {test.level_id} for test storage")
                    return None

                level_db_id = level_result['id']

                # Generate unique test ID
                test_id = f"{test.level_id}_{int(test.test_datetime.timestamp())}"

                # Insert test
                query = """
                INSERT INTO dev_sr_tests (
                    test_id, level_id, symbol, sr_level_id, test_datetime,
                    test_price, approach_direction, timeframe, max_penetration,
                    hold_duration, volume_spike, outcome, outcome_confidence,
                    metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (test_id) DO UPDATE SET
                    outcome = EXCLUDED.outcome,
                    outcome_confidence = EXCLUDED.outcome_confidence,
                    updated_at = NOW()
                RETURNING id
                """

                result = await conn.fetchrow(
                    query,
                    test_id, test.level_id, symbol, level_db_id, test.test_datetime,
                    Decimal(str(test.test_price)), test.approach_direction, timeframe.value,
                    Decimal(str(test.max_penetration)), test.hold_duration,
                    Decimal(str(test.volume_spike)), test.outcome.value,
                    Decimal(str(test.confidence)), json.dumps({})
                )

                return result['id'] if result else None

        except Exception as e:
            logger.error(f"Error storing S/R test: {e}")
            return None

    async def _create_sr_event(self, symbol: str, test: SRTest, test_db_id: int, timeframe: Timeframe) -> Optional[SREvent]:
        """Create S/R event based on test outcome"""

        # Determine event significance based on test outcome and level characteristics
        significance_score = self._calculate_event_significance(test)

        # Only create events for significant outcomes
        if significance_score < 0.5:
            return None

        # Get level information
        async with self.db_pool.acquire() as conn:
            level_query = """
            SELECT * FROM dev_sr_levels
            WHERE symbol = $1 AND level_id = $2
            LIMIT 1
            """
            level_row = await conn.fetchrow(level_query, symbol, test.level_id)

            if not level_row:
                return None

            # Create SRLevel object from database row
            level = SRLevel(
                price=float(level_row['price']),
                sr_type=SRType(level_row['sr_type']),
                level_type=SRLevelType(level_row['level_type']),
                timeframe=Timeframe(level_row['timeframe']),
                strength=float(level_row['strength']),
                first_established=level_row['first_established'],
                last_tested=level_row['last_tested'],
                test_count=level_row['test_count'],
                hold_count=level_row['hold_count'],
                break_count=level_row['break_count'],
                confidence=float(level_row['confidence']),
                volume_confirmation=level_row['volume_confirmation'],
                metadata=level_row['metadata'] or {}
            )

        # Generate event ID
        event_id = f"sr_{symbol}_{timeframe.value}_{test.outcome.value}_{int(test.test_datetime.timestamp())}"

        # Create event
        event = SREvent(
            event_id=event_id,
            symbol=symbol,
            level=level,
            test=test,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        return event

    def _calculate_event_significance(self, test: SRTest) -> float:
        """Calculate significance score for S/R test event"""
        significance = 0.5  # Base significance

        # Outcome-based significance
        outcome_weights = {
            SRTestOutcome.HOLD_STRONG: 0.9,
            SRTestOutcome.BREAK_CLEAN: 0.9,
            SRTestOutcome.HOLD_WEAK: 0.6,
            SRTestOutcome.PENETRATION: 0.7,
            SRTestOutcome.BREAK_FALSE: 0.8,  # False breaks are significant
            SRTestOutcome.PENDING: 0.3
        }

        significance *= outcome_weights.get(test.outcome, 0.5)

        # Volume confirmation boost
        if test.volume_spike > 2.0:  # 2x average volume
            significance += 0.2
        elif test.volume_spike > 1.5:
            significance += 0.1

        # Penetration factor
        if test.max_penetration > 0.01:  # 1% penetration
            significance += 0.1

        # Confidence factor
        significance *= test.confidence

        return min(1.0, significance)

    async def _emit_sr_event(self, event: SREvent):
        """Emit S/R event to main event system"""
        try:
            # Store in S/R events table
            await self._store_sr_event(event)

            # Emit to main financial events system (if configured)
            if self.config.get('emit_to_main_events', True):
                await self._emit_to_financial_events(event)

            # Send alerts for high-significance events
            if self._should_alert(event):
                await self._send_sr_alert(event)

            logger.debug(f"Emitted S/R event: {event.event_id}")

        except Exception as e:
            logger.error(f"Error emitting S/R event: {e}")
            raise

    async def _store_sr_event(self, event: SREvent):
        """Store S/R event in database"""
        async with self.db_pool.acquire() as conn:
            # Get level and test database IDs
            level_query = "SELECT id FROM dev_sr_levels WHERE level_id = $1 AND symbol = $2"
            level_result = await conn.fetchrow(level_query, event.test.level_id, event.symbol)

            if not level_result:
                logger.warning(f"Could not find level for event {event.event_id}")
                return

            test_query = "SELECT id FROM dev_sr_tests WHERE test_id LIKE $1 AND symbol = $2"
            test_pattern = f"{event.test.level_id}_%"
            test_result = await conn.fetchrow(test_query, test_pattern, event.symbol)

            if not test_result:
                logger.warning(f"Could not find test for event {event.event_id}")
                return

            # Determine event subtype
            event_subtype = self._get_event_subtype(event.test.outcome)

            # Calculate scores
            significance_score = self._calculate_event_significance(event.test)
            impact_score = min(1.0, event.level.strength * significance_score)

            # Insert event
            query = """
            INSERT INTO dev_sr_events (
                event_id, symbol, sr_level_id, sr_test_id, event_type, event_subtype,
                event_datetime, market_datetime, timeframe, significance_score,
                impact_score, price_at_event, event_data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (event_id) DO NOTHING
            """

            event_data = {
                'test_outcome': event.test.outcome.value,
                'level_strength': event.level.strength,
                'level_type': event.level.level_type.value,
                'approach_direction': event.test.approach_direction,
                'max_penetration': event.test.max_penetration,
                'volume_spike': event.test.volume_spike
            }

            await conn.execute(
                query,
                event.event_id, event.symbol, level_result['id'], test_result['id'],
                'support_resistance', event_subtype, event.test.test_datetime,
                event.test.test_datetime, event.level.timeframe.value,
                Decimal(str(significance_score)), Decimal(str(impact_score)),
                Decimal(str(event.test.test_price)), json.dumps(event_data)
            )

    def _get_event_subtype(self, outcome: SRTestOutcome) -> str:
        """Get event subtype based on test outcome"""
        subtype_map = {
            SRTestOutcome.HOLD_STRONG: 'level_held_strong',
            SRTestOutcome.HOLD_WEAK: 'level_held_weak',
            SRTestOutcome.BREAK_CLEAN: 'level_broken_clean',
            SRTestOutcome.BREAK_FALSE: 'level_broken_false',
            SRTestOutcome.PENETRATION: 'level_penetrated',
            SRTestOutcome.PENDING: 'level_test_pending'
        }
        return subtype_map.get(outcome, 'level_tested')

    async def _emit_to_financial_events(self, event: SREvent):
        """Emit S/R event to main financial events system"""
        # This would integrate with the existing financial events system
        # For now, we'll just log the integration point
        logger.debug(f"Would emit to financial events: {event.event_id}")

    def _should_alert(self, event: SREvent) -> bool:
        """Determine if event should trigger an alert"""
        thresholds = self.config['alert_thresholds']

        # Strong level test with high volume
        if (event.test.outcome in [SRTestOutcome.HOLD_STRONG, SRTestOutcome.BREAK_CLEAN] and
            event.level.strength >= thresholds['strong_level_test'] and
            event.test.volume_spike > 2.0):
            return True

        # Level break
        if (event.test.outcome == SRTestOutcome.BREAK_CLEAN and
            event.level.strength >= thresholds['level_break']):
            return True

        # Confluence level test
        if (event.level.level_type == SRLevelType.CONFLUENCE and
            event.level.strength >= thresholds['confluence_level']):
            return True

        return False

    async def _send_sr_alert(self, event: SREvent):
        """Send alert for significant S/R event"""
        alert_message = self._format_alert_message(event)
        logger.info(f"S/R ALERT: {alert_message}")

        # TODO: Integrate with actual alerting system (Slack, email, etc.)

    def _format_alert_message(self, event: SREvent) -> str:
        """Format S/R event alert message"""
        level_type = event.level.sr_type.value.upper()
        outcome = event.test.outcome.value.replace('_', ' ').title()

        return (f"{event.symbol} {level_type} at ${event.level.price:.2f} "
                f"({event.level.timeframe.value}) - {outcome} "
                f"(Strength: {event.level.strength:.2f}, "
                f"Volume: {event.test.volume_spike:.1f}x)")

    async def run_batch_processing(self):
        """Run batch processing for all active symbols"""
        logger.info("Starting batch S/R processing...")

        processed_count = 0
        error_count = 0

        # Process symbols in batches
        symbol_list = list(self.active_symbols)
        batch_size = self.config['batch_size']

        for i in range(0, len(symbol_list), batch_size):
            batch = symbol_list[i:i + batch_size]

            # Process batch concurrently
            tasks = []
            for symbol in batch:
                for timeframe in self.config['timeframe_priorities']:
                    tasks.append(self._process_symbol_timeframe(symbol, timeframe))

            # Limit concurrency
            max_concurrent = self.config['max_concurrent_symbols']
            semaphore = asyncio.Semaphore(max_concurrent)

            async def process_with_semaphore(task):
                async with semaphore:
                    return await task

            # Execute batch
            results = await asyncio.gather(
                *[process_with_semaphore(task) for task in tasks],
                return_exceptions=True
            )

            # Count results
            for result in results:
                if isinstance(result, Exception):
                    error_count += 1
                else:
                    processed_count += 1

        logger.info(f"Batch processing completed: {processed_count} processed, {error_count} errors")

    async def _process_symbol_timeframe(self, symbol: str, timeframe: Timeframe):
        """Process single symbol/timeframe combination"""
        try:
            # Get market data for this symbol/timeframe
            ohlcv_data = await self._get_market_data(symbol, timeframe)

            if ohlcv_data is not None and len(ohlcv_data) >= self.config['min_data_points']:
                await self.process_market_data_update(symbol, ohlcv_data, timeframe)

        except Exception as e:
            logger.error(f"Error processing {symbol}/{timeframe.value}: {e}")
            raise

    def _timeframe_to_minutes(self, timeframe: Timeframe) -> int:
        """Convert S/R Timeframe enum to minutes for market data manager"""
        mapping = {
            Timeframe.INTRADAY_1M: 1,
            Timeframe.INTRADAY_5M: 5,
            Timeframe.INTRADAY_15M: 15,
            Timeframe.INTRADAY_1H: 60,
            Timeframe.DAILY: 1440,     # 24 * 60
            Timeframe.WEEKLY: 10080,   # 7 * 24 * 60
            Timeframe.MONTHLY: 43200,  # 30 * 24 * 60 (approximate)
            Timeframe.QUARTERLY: 129600, # 90 * 24 * 60 (approximate)
            Timeframe.YEARLY: 525600   # 365 * 24 * 60 (approximate)
        }
        return mapping.get(timeframe, 1440)  # Default to daily

    def _get_data_lookback_period(self, timeframe: Timeframe) -> timedelta:
        """Get appropriate lookback period for sufficient S/R analysis data"""
        lookback_periods = {
            Timeframe.INTRADAY_1M: timedelta(days=5),      # 5 days of 1m data
            Timeframe.INTRADAY_5M: timedelta(days=10),     # 10 days of 5m data
            Timeframe.INTRADAY_15M: timedelta(days=20),    # 20 days of 15m data
            Timeframe.INTRADAY_1H: timedelta(days=60),     # 60 days of hourly data
            Timeframe.DAILY: timedelta(days=252),          # ~1 year of daily data
            Timeframe.WEEKLY: timedelta(days=1260),        # ~3.5 years of weekly data
            Timeframe.MONTHLY: timedelta(days=2520),       # ~7 years of monthly data
            Timeframe.QUARTERLY: timedelta(days=3650),     # ~10 years quarterly
            Timeframe.YEARLY: timedelta(days=7300)         # ~20 years yearly
        }
        return lookback_periods.get(timeframe, timedelta(days=252))

    async def _get_market_data(self, symbol: str, timeframe: Timeframe) -> Optional[pd.DataFrame]:
        """Get market data for symbol/timeframe"""
        if not self.market_data_manager:
            logger.error("Market data manager not initialized")
            return None

        try:
            # Calculate date range for sufficient historical data
            end_date = datetime.now()
            start_date = end_date - self._get_data_lookback_period(timeframe)

            # Convert timeframe to minutes for the manager
            timeframe_minutes = self._timeframe_to_minutes(timeframe)

            logger.debug(f"Getting {timeframe.value} data for {symbol} from {start_date.date()} to {end_date.date()}")

            # Query market data
            data_batch = await self.market_data_manager.get_minute_ohlc_batch(
                symbols=[symbol],
                start=start_date,
                end=end_date,
                timeframe_minutes=timeframe_minutes
            )

            # Extract data for the symbol
            if symbol in data_batch:
                df = data_batch[symbol]
                if not df.empty:
                    logger.debug(f"Retrieved {len(df)} data points for {symbol} ({timeframe.value})")
                    return df
                else:
                    logger.warning(f"Empty dataset returned for {symbol} ({timeframe.value})")
            else:
                logger.warning(f"No data returned for {symbol} ({timeframe.value})")

            return None

        except Exception as e:
            logger.error(f"Error getting market data for {symbol} ({timeframe.value}): {e}")
            return None

    def get_processing_stats(self) -> Dict:
        """Get current processing statistics"""
        return {
            **self.stats,
            'active_symbols': len(self.active_symbols),
            'avg_processing_time_ms': (
                self.stats['processing_time_ms'] / max(1, self.stats['symbols_processed'])
            )
        }

    async def close(self):
        """Clean up processor resources"""
        logger.info("Closing SupportResistanceProcessor...")

        try:
            # Close market data manager
            if self.market_data_manager:
                await self.market_data_manager.close()
                self.market_data_manager = None

            # Close database pool
            if self.db_pool:
                await self.db_pool.close()
                self.db_pool = None

            # Clear processing state
            self.active_symbols.clear()
            self.last_processed.clear()

            logger.info("SupportResistanceProcessor closed successfully")

        except Exception as e:
            logger.error(f"Error closing processor: {e}")
            raise

async def main():
    """Main function for testing and standalone operation"""
    import argparse

    parser = argparse.ArgumentParser(description="Support/Resistance Event Processor")
    parser.add_argument('--batch', action='store_true', help='Run batch processing')
    parser.add_argument('--symbols', type=str, help='Comma-separated symbols to process')
    parser.add_argument('--timeframe', type=str, default='1d', help='Timeframe to process')

    args = parser.parse_args()

    # Initialize processor
    processor = SupportResistanceProcessor()
    await processor.initialize()

    if args.batch:
        await processor.run_batch_processing()
    else:
        # Interactive mode - process specific symbols
        symbols = args.symbols.split(',') if args.symbols else ['AAPL', 'MSFT', 'GOOGL']
        timeframe = Timeframe(args.timeframe)

        for symbol in symbols:
            logger.info(f"Processing {symbol} ({timeframe.value})...")
            # Would need actual market data here
            # await processor.process_market_data_update(symbol, data, timeframe)

    # Print stats
    stats = processor.get_processing_stats()
    logger.info(f"Processing completed: {stats}")

if __name__ == "__main__":
    asyncio.run(main())