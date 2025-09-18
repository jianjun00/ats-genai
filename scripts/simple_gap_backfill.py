#!/usr/bin/env python3
"""
Simple Gap Event Backfill for Stocks

Detect price gaps (opening price significantly different from previous close)
and store them as events in the database with Protocol Buffer serialization.

Usage:
    python scripts/simple_gap_backfill.py --symbol TSLA --start-date 2025-07-01 --end-date 2025-07-31
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np
from dataclasses import dataclass
import json

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from infrastructure.storage.file_based_minute_manager import FileBasedMinuteManager
from infrastructure.storage.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
from domains.analytics.events.proto.events_pb2 import Event, create_gap_event
from infrastructure.database.connections import get_database_url
from infrastructure.database.base_service import BaseService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class GapEvent:
    """Data class for price gap events"""
    symbol: str
    gap_date: datetime
    gap_points: float
    gap_percentage: float
    gap_size_class: str
    direction: str
    prev_close: float
    open_price: float
    volume: int
    avg_volume: float
    volume_confirmed: bool
    significance_score: float
    gap_context: str = "market"


@dataclass
class GapFillEvent:
    """Data class for gap fill tracking"""
    gap_id: int
    fill_date: datetime
    days_to_fill: int
    fill_percentage: float
    fill_type: str


class SimpleGapBackfillProcessor:
    """Simple gap detection and backfill processor"""

    def __init__(self):
        self.data_manager = None
        self.db_service = None
        self.events_processed = 0
        self.gaps_detected = 0
        self.gaps_filled = 0

    async def initialize(self):
        """Initialize data manager and database connection"""
        try:
            # Initialize data manager
            minute_manager = FileBasedMinuteManager()
            self.data_manager = FileBasedMinuteMarketDataManager(minute_manager)

            # Initialize database connection
            db_url = get_database_url()
            self.db_service = BaseService()
            await self.db_service.connect(db_url)

            logger.info("✅ Gap processor initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize gap processor: {e}")
            raise

    async def close(self):
        """Clean up resources"""
        try:
            if self.db_service:
                await self.db_service.close()
            logger.info("✅ Gap processor closed successfully")
        except Exception as e:
            logger.error(f"❌ Error closing gap processor: {e}")

    def classify_gap_size(self, gap_pct: float) -> str:
        """Classify gap size based on percentage"""
        gap_size = abs(gap_pct)

        if gap_size >= 5.0:
            return "extreme"
        elif gap_size >= 2.5:
            return "large"
        elif gap_size >= 1.0:
            return "medium"
        elif gap_size >= 0.5:
            return "small"
        else:
            return "micro"

    def detect_price_gap(self, open_price: float, prev_close: float,
                        volume: int, avg_volume: float, symbol: str,
                        gap_date: datetime) -> Optional[GapEvent]:
        """
        Detect and classify a price gap

        Args:
            open_price: Opening price of current session
            prev_close: Closing price of previous session
            volume: Opening volume
            avg_volume: Average volume for validation
            symbol: Stock symbol
            gap_date: Date of the gap

        Returns:
            GapEvent if significant gap detected, None otherwise
        """

        # Calculate gap metrics
        gap_points = open_price - prev_close
        gap_pct = (gap_points / prev_close) * 100
        gap_size = abs(gap_pct)

        # Minimum threshold for gap detection (0.2%)
        if gap_size < 0.2:
            return None

        # Classify gap size
        size_class = self.classify_gap_size(gap_pct)

        # Determine direction
        direction = "gap_up" if gap_pct > 0 else "gap_down"

        # Volume confirmation (gaps with high volume more significant)
        volume_ratio = volume / avg_volume if avg_volume > 0 else 1.0
        volume_confirmed = volume_ratio > 1.5

        # Gap significance score (size + volume factor)
        significance = gap_size * (1.0 + min(volume_ratio - 1.0, 2.0))

        return GapEvent(
            symbol=symbol,
            gap_date=gap_date,
            gap_points=gap_points,
            gap_percentage=gap_pct,
            gap_size_class=size_class,
            direction=direction,
            prev_close=prev_close,
            open_price=open_price,
            volume=volume,
            avg_volume=avg_volume,
            volume_confirmed=volume_confirmed,
            significance_score=significance
        )

    def detect_gap_fill(self, gap_event: GapEvent, subsequent_data: pd.DataFrame,
                       max_days: int = 10) -> Optional[GapFillEvent]:
        """
        Detect if and when a gap gets filled

        Args:
            gap_event: Original gap event
            subsequent_data: OHLC data for days after gap
            max_days: Maximum days to track for gap fill

        Returns:
            GapFillEvent if gap gets filled, None otherwise
        """

        gap_level = gap_event.prev_close

        # Group by date to get daily OHLC
        daily_data = subsequent_data.groupby(subsequent_data['timestamp'].dt.date).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }).reset_index()

        for day_num, (_, day_data) in enumerate(daily_data.head(max_days).iterrows(), 1):

            if gap_event.direction == "gap_up":
                # For gap up, check if price goes back down to gap level
                if day_data['low'] <= gap_level:
                    return GapFillEvent(
                        gap_id=0,  # Will be set after database insert
                        fill_date=day_data['timestamp'],
                        days_to_fill=day_num,
                        fill_percentage=100.0,
                        fill_type="full"
                    )
                elif day_data['low'] <= gap_level + (gap_event.gap_points * 0.5):
                    # Partial fill (50%+ retracement)
                    fill_pct = ((gap_event.open_price - day_data['low']) / gap_event.gap_points) * 100
                    if fill_pct >= 50:
                        return GapFillEvent(
                            gap_id=0,
                            fill_date=day_data['timestamp'],
                            days_to_fill=day_num,
                            fill_percentage=fill_pct,
                            fill_type="partial"
                        )

            else:  # gap_down
                # For gap down, check if price goes back up to gap level
                if day_data['high'] >= gap_level:
                    return GapFillEvent(
                        gap_id=0,
                        fill_date=day_data['timestamp'],
                        days_to_fill=day_num,
                        fill_percentage=100.0,
                        fill_type="full"
                    )
                elif day_data['high'] >= gap_level - (gap_event.gap_points * 0.5):
                    # Partial fill
                    fill_pct = ((day_data['high'] - gap_event.open_price) / abs(gap_event.gap_points)) * 100
                    if fill_pct >= 50:
                        return GapFillEvent(
                            gap_id=0,
                            fill_date=day_data['timestamp'],
                            days_to_fill=day_num,
                            fill_percentage=fill_pct,
                            fill_type="partial"
                        )

        return None  # Gap unfilled within tracking period

    async def store_gap_event(self, gap_event: GapEvent, fill_event: Optional[GapFillEvent] = None) -> int:
        """Store gap event in database and return gap ID"""

        try:
            # Create Protocol Buffer event
            proto_event = create_gap_event(
                symbol=gap_event.symbol,
                gap_points=gap_event.gap_points,
                gap_percentage=gap_event.gap_percentage,
                direction=gap_event.direction,
                prev_close=gap_event.prev_close,
                open_price=gap_event.open_price,
                volume=gap_event.volume,
                significance=gap_event.significance_score
            )

            # Serialize to bytes
            event_data = proto_event.SerializeToString()

            # Store in gap_events table
            query = """
                INSERT INTO gap_events (
                    symbol, gap_date, gap_points, gap_percentage,
                    gap_size_class, direction, prev_close, open_price,
                    volume, avg_volume, volume_confirmed, significance_score,
                    gap_context, fill_date, days_to_fill, fill_percentage,
                    fill_type, event_data
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
                ) RETURNING id
            """

            gap_id = await self.db_service.fetch_value(
                query,
                gap_event.symbol,
                gap_event.gap_date.date(),
                gap_event.gap_points,
                gap_event.gap_percentage,
                gap_event.gap_size_class,
                gap_event.direction,
                gap_event.prev_close,
                gap_event.open_price,
                gap_event.volume,
                gap_event.avg_volume,
                gap_event.volume_confirmed,
                gap_event.significance_score,
                gap_event.gap_context,
                fill_event.fill_date.date() if fill_event else None,
                fill_event.days_to_fill if fill_event else None,
                fill_event.fill_percentage if fill_event else None,
                fill_event.fill_type if fill_event else None,
                event_data
            )

            self.events_processed += 1
            return gap_id

        except Exception as e:
            logger.error(f"❌ Failed to store gap event: {e}")
            raise

    async def run_backfill(self, symbol: str, start_date: datetime,
                          end_date: datetime, limit: Optional[int] = None) -> Dict:
        """
        Run gap detection backfill for a symbol

        Args:
            symbol: Stock symbol to process
            start_date: Start date for gap detection
            end_date: End date for gap detection
            limit: Maximum number of gaps to process

        Returns:
            Dictionary with processing results
        """

        try:
            logger.info(f"🔍 Starting gap detection for {symbol} from {start_date.date()} to {end_date.date()}")

            # Get market data
            market_data = await self.data_manager.get_minute_bars(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )

            if market_data.empty:
                return {
                    'success': False,
                    'error': f'No market data found for {symbol}',
                    'events_processed': 0
                }

            logger.info(f"📊 Loaded {len(market_data)} minute bars for {symbol}")

            # Group by date to get daily OHLC
            market_data['date'] = pd.to_datetime(market_data['timestamp']).dt.date
            daily_data = market_data.groupby('date').agg({
                'timestamp': 'first',
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).reset_index()

            # Calculate average volume for context
            avg_volume = daily_data['volume'].rolling(window=20, min_periods=5).mean()
            daily_data['avg_volume'] = avg_volume.fillna(daily_data['volume'].mean())

            # Detect gaps
            gaps_found = []

            for i in range(1, len(daily_data)):
                current_day = daily_data.iloc[i]
                prev_day = daily_data.iloc[i-1]

                gap_event = self.detect_price_gap(
                    open_price=current_day['open'],
                    prev_close=prev_day['close'],
                    volume=current_day['volume'],
                    avg_volume=current_day['avg_volume'],
                    symbol=symbol,
                    gap_date=pd.to_datetime(current_day['timestamp'])
                )

                if gap_event:
                    # Look for gap fill in subsequent days
                    subsequent_data = market_data[
                        market_data['timestamp'] > current_day['timestamp']
                    ]

                    fill_event = None
                    if not subsequent_data.empty:
                        fill_event = self.detect_gap_fill(gap_event, subsequent_data)

                    gaps_found.append((gap_event, fill_event))

                    if limit and len(gaps_found) >= limit:
                        break

            # Store gap events
            gaps_stored = 0
            for gap_event, fill_event in gaps_found:
                try:
                    gap_id = await self.store_gap_event(gap_event, fill_event)
                    gaps_stored += 1

                    gap_type = gap_event.direction.replace('_', ' ').title()
                    fill_info = ""
                    if fill_event:
                        fill_info = f" (FILLED {fill_event.fill_type} in {fill_event.days_to_fill} days)"
                        self.gaps_filled += 1

                    logger.info(f"💰 {gap_event.gap_date.date()}: {gap_type} {gap_event.gap_percentage:+.2f}% "
                              f"({gap_event.gap_size_class}){fill_info}")

                except Exception as e:
                    logger.error(f"❌ Failed to store gap for {gap_event.gap_date}: {e}")

            self.gaps_detected += gaps_stored

            return {
                'success': True,
                'events_processed': gaps_stored,
                'gaps_detected': len(gaps_found),
                'gaps_filled': self.gaps_filled,
                'symbol': symbol,
                'date_range': f"{start_date.date()} to {end_date.date()}"
            }

        except Exception as e:
            logger.error(f"❌ Gap backfill failed for {symbol}: {e}")
            return {
                'success': False,
                'error': str(e),
                'events_processed': 0
            }


async def main():
    """Main function for gap backfill"""

    parser = argparse.ArgumentParser(description="Simple Gap Event Backfill")
    parser.add_argument('--symbol', type=str, required=True, help='Stock symbol')
    parser.add_argument('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD, default: today)')
    parser.add_argument('--limit', type=int, help='Maximum number of gaps to process')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Configure logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else datetime.now()
    except ValueError as e:
        logger.error(f"❌ Invalid date format: {e}")
        return

    processor = SimpleGapBackfillProcessor()

    try:
        await processor.initialize()

        result = await processor.run_backfill(
            symbol=args.symbol,
            start_date=start_date,
            end_date=end_date,
            limit=args.limit
        )

        if result['success']:
            logger.info(f"✅ Gap backfill completed for {args.symbol}")
            logger.info(f"📊 Events processed: {result['events_processed']}")
            logger.info(f"🔍 Gaps detected: {result['gaps_detected']}")
            logger.info(f"💰 Gaps filled: {result.get('gaps_filled', 0)}")
        else:
            logger.error(f"❌ Gap backfill failed: {result['error']}")

    except Exception as e:
        logger.error(f"❌ Process failed: {e}")

    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main())