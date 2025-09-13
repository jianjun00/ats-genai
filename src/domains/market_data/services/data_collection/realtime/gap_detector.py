#!/usr/bin/env python3
"""
Real-Time Data Gap Detection and Backfill

Detects gaps in real-time data collection and triggers intelligent backfill
processes to maintain data completeness.
"""

import asyncio
import asyncpg
import aiohttp
import logging
import os
from datetime import datetime
from typing import Dict, List
from dataclasses import dataclass
import pytz

from shared.utils.environment import Environment

logger = logging.getLogger(__name__)

@dataclass
class DataGap:
    """Represents a detected data gap"""
    vendor: str
    symbol: str
    gap_start: datetime
    gap_end: datetime
    gap_duration_minutes: int
    missing_bars_count: int
    gap_type: str
    severity: str
    detection_method: str = 'realtime'

class GapDetectionEngine:
    """
    Real-time gap detection and intelligent backfill engine.
    """

    def __init__(self):
        self.env = Environment()
        self.pool = None

        # Configuration
        self.gap_threshold_minutes = int(os.getenv('GAP_THRESHOLD_MINUTES', '5'))
        self.critical_gap_minutes = int(os.getenv('CRITICAL_GAP_MINUTES', '15'))
        self.max_backfill_symbols = int(os.getenv('MAX_BACKFILL_SYMBOLS', '100'))
        self.enable_auto_backfill = os.getenv('ENABLE_AUTO_BACKFILL', 'true').lower() == 'true'
        self.market_hours_only = os.getenv('MARKET_HOURS_ONLY', 'true').lower() == 'true'

        # API credentials for backfill
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        self.tiingo_api_key = os.getenv('TIINGO_API_KEY')
        self.fmp_api_key = os.getenv('FMP_API_KEY')

        # Statistics
        self.gaps_detected = 0
        self.gaps_backfilled = 0
        self.backfill_errors = 0

        self.eastern_tz = pytz.timezone('US/Eastern')

    async def initialize(self):
        """Initialize database connection"""
        self.pool = await asyncpg.create_pool(self.env.get_database_url())
        logger.info("✅ Connected to database for gap detection")

    async def run_gap_detection(self):
        """Run complete gap detection and backfill process"""
        logger.info("🔍 Starting gap detection and backfill process")

        try:
            # Detect gaps across all vendors
            detected_gaps = await self._detect_all_gaps()
            logger.info(f"📉 Detected {len(detected_gaps)} data gaps")

            # Store gap records
            await self._store_gaps(detected_gaps)

            # Prioritize gaps for backfill
            priority_gaps = self._prioritize_gaps(detected_gaps)

            # Trigger backfills for priority gaps
            if self.enable_auto_backfill and priority_gaps:
                await self._execute_backfills(priority_gaps)

            # Update collection status for gaps
            await self._update_collection_status(detected_gaps)

            logger.info(f"✅ Gap detection completed: {self.gaps_detected} detected, {self.gaps_backfilled} backfilled")

        except Exception as e:
            logger.error(f"💥 Gap detection failed: {e}")
            raise

    async def _detect_all_gaps(self) -> List[DataGap]:
        """Detect gaps across all vendors and active symbols"""
        all_gaps = []
        vendors = ['polygon', 'tiingo', 'fmp']

        for vendor in vendors:
            vendor_gaps = await self._detect_vendor_gaps(vendor)
            all_gaps.extend(vendor_gaps)

        return all_gaps

    async def _detect_vendor_gaps(self, vendor: str) -> List[DataGap]:
        """Detect gaps for a specific vendor"""
        # Get active symbols for this vendor
        active_symbols = await self._get_active_symbols(vendor)

        gaps = []
        for symbol in active_symbols:
            symbol_gaps = await self._detect_symbol_gaps(vendor, symbol)
            gaps.extend(symbol_gaps)

        return gaps

    async def _get_active_symbols(self, vendor: str) -> List[str]:
        """Get symbols that should have recent data for this vendor"""
        query = """
            SELECT DISTINCT symbol
            FROM dev_realtime_collection_status
            WHERE vendor = $1
              AND is_active = true
              AND collection_health_score > 0.3
            ORDER BY collection_health_score DESC
            LIMIT $2
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, vendor, self.max_backfill_symbols)
            return [row['symbol'] for row in rows]

    async def _detect_symbol_gaps(self, vendor: str, symbol: str) -> List[DataGap]:
        """Detect gaps for a specific vendor/symbol combination"""
        table_name = f"dev_one_minute_live_{vendor}"

        # Get recent data points to analyze for gaps
        query = f"""
            WITH time_series AS (
                SELECT
                    timestamp,
                    LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp,
                    EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY timestamp))) / 60 as gap_minutes
                FROM {table_name}
                WHERE symbol = $1
                  AND timestamp >= now() - INTERVAL '4 hours'
                  AND timestamp < now() - INTERVAL '5 minutes'  -- Exclude very recent data
                ORDER BY timestamp
            )
            SELECT
                prev_timestamp,
                timestamp,
                gap_minutes
            FROM time_series
            WHERE gap_minutes > $2
              AND gap_minutes IS NOT NULL
            ORDER BY gap_minutes DESC
        """

        gaps = []

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, symbol, self.gap_threshold_minutes)

            for row in rows:
                gap_start = row['prev_timestamp']
                gap_end = row['timestamp']
                gap_minutes = int(row['gap_minutes'])

                # Skip gaps outside market hours if configured
                if self.market_hours_only and not self._is_market_hours_gap(gap_start, gap_end):
                    continue

                # Determine gap type and severity
                gap_type = self._classify_gap_type(gap_start, gap_end, gap_minutes)
                severity = self._determine_gap_severity(gap_minutes)

                gap = DataGap(
                    vendor=vendor,
                    symbol=symbol,
                    gap_start=gap_start,
                    gap_end=gap_end,
                    gap_duration_minutes=gap_minutes,
                    missing_bars_count=gap_minutes,  # Approximate for 1-minute bars
                    gap_type=gap_type,
                    severity=severity
                )

                gaps.append(gap)

        return gaps

    def _is_market_hours_gap(self, gap_start: datetime, gap_end: datetime) -> bool:
        """Check if gap occurs during market hours"""
        # Convert to Eastern time
        start_et = gap_start.astimezone(self.eastern_tz)
        end_et = gap_end.astimezone(self.eastern_tz)

        # Check if gap overlaps with market hours (9:30 AM - 4:00 PM ET)
        market_start = start_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_end = start_et.replace(hour=16, minute=0, second=0, microsecond=0)

        # Gap is relevant if it overlaps with market hours
        return not (end_et <= market_start or start_et >= market_end)

    def _classify_gap_type(self, gap_start: datetime, gap_end: datetime, gap_minutes: int) -> str:
        """Classify the type of gap"""
        if gap_minutes > 60:
            return 'connection_loss'
        elif gap_minutes > 30:
            return 'api_error'
        elif gap_minutes > 10:
            return 'rate_limit'
        else:
            return 'temporary_delay'

    def _determine_gap_severity(self, gap_minutes: int) -> str:
        """Determine gap severity level"""
        if gap_minutes >= self.critical_gap_minutes:
            return 'critical'
        elif gap_minutes >= 10:
            return 'high'
        elif gap_minutes >= 5:
            return 'medium'
        else:
            return 'low'

    async def _store_gaps(self, gaps: List[DataGap]):
        """Store detected gaps in database"""
        query = """
            INSERT INTO dev_realtime_gaps (
                vendor, symbol, gap_start_timestamp, gap_end_timestamp,
                gap_duration_minutes, missing_bars_count, gap_type,
                detection_method, gap_severity
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT DO NOTHING
        """

        async with self.pool.acquire() as conn:
            for gap in gaps:
                await conn.execute(
                    query,
                    gap.vendor, gap.symbol, gap.gap_start, gap.gap_end,
                    gap.gap_duration_minutes, gap.missing_bars_count,
                    gap.gap_type, gap.detection_method, gap.severity
                )

        self.gaps_detected = len(gaps)
        logger.info(f"📊 Stored {len(gaps)} gap records")

    def _prioritize_gaps(self, gaps: List[DataGap]) -> List[DataGap]:
        """Prioritize gaps for backfill based on severity and impact"""
        # Sort by severity and duration
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}

        priority_gaps = sorted(gaps, key=lambda g: (
            severity_order.get(g.severity, 4),
            -g.gap_duration_minutes,
            g.vendor,  # Prefer certain vendors
            g.symbol
        ))

        # Limit to manageable number for backfill
        return priority_gaps[:20]

    async def _execute_backfills(self, priority_gaps: List[DataGap]):
        """Execute backfill operations for priority gaps"""
        logger.info(f"🔄 Starting backfill for {len(priority_gaps)} priority gaps")

        for gap in priority_gaps:
            try:
                success = await self._backfill_gap(gap)

                if success:
                    self.gaps_backfilled += 1
                    await self._mark_gap_backfilled(gap)
                else:
                    self.backfill_errors += 1
                    await self._mark_gap_failed(gap)

                # Rate limiting between backfills
                await asyncio.sleep(1)

            except Exception as e:
                logger.warning(f"Backfill error for {gap.vendor}/{gap.symbol}: {e}")
                self.backfill_errors += 1
                await self._mark_gap_failed(gap, str(e))

    async def _backfill_gap(self, gap: DataGap) -> bool:
        """Backfill a specific gap using vendor API"""
        if gap.vendor == 'polygon':
            return await self._backfill_polygon_gap(gap)
        elif gap.vendor == 'tiingo':
            return await self._backfill_tiingo_gap(gap)
        elif gap.vendor == 'fmp':
            return await self._backfill_fmp_gap(gap)
        else:
            logger.warning(f"Unknown vendor for backfill: {gap.vendor}")
            return False

    async def _backfill_polygon_gap(self, gap: DataGap) -> bool:
        """Backfill gap using Polygon API"""
        if not self.polygon_api_key:
            return False

        start_date = gap.gap_start.strftime('%Y-%m-%d')
        end_date = gap.gap_end.strftime('%Y-%m-%d')

        url = f"https://api.polygon.io/v2/aggs/ticker/{gap.symbol}/range/1/minute/{start_date}/{end_date}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,
            'apiKey': self.polygon_api_key
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = data.get('results', [])

                        if results:
                            await self._store_backfilled_data('polygon', gap.symbol, results)
                            logger.info(f"✅ Backfilled {len(results)} bars for {gap.vendor}/{gap.symbol}")
                            return True
                    else:
                        logger.warning(f"Polygon backfill error for {gap.symbol}: {response.status}")

            except Exception as e:
                logger.warning(f"Error in Polygon backfill for {gap.symbol}: {e}")

        return False

    async def _backfill_tiingo_gap(self, gap: DataGap) -> bool:
        """Backfill gap using Tiingo API"""
        if not self.tiingo_api_key:
            return False

        url = f"https://api.tiingo.com/iex/{gap.symbol}/prices"
        params = {
            'token': self.tiingo_api_key,
            'startDate': gap.gap_start.strftime('%Y-%m-%d'),
            'endDate': gap.gap_end.strftime('%Y-%m-%d'),
            'resampleFreq': '1min',
            'format': 'json'
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        if data:
                            await self._store_backfilled_tiingo_data(gap.symbol, data)
                            logger.info(f"✅ Backfilled {len(data)} bars for {gap.vendor}/{gap.symbol}")
                            return True
                    else:
                        logger.warning(f"Tiingo backfill error for {gap.symbol}: {response.status}")

            except Exception as e:
                logger.warning(f"Error in Tiingo backfill for {gap.symbol}: {e}")

        return False

    async def _backfill_fmp_gap(self, gap: DataGap) -> bool:
        """Backfill gap using FMP API"""
        if not self.fmp_api_key:
            return False

        url = f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{gap.symbol}"
        params = {
            'apikey': self.fmp_api_key,
            'from': gap.gap_start.strftime('%Y-%m-%d'),
            'to': gap.gap_end.strftime('%Y-%m-%d')
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        if data:
                            await self._store_backfilled_fmp_data(gap.symbol, data)
                            logger.info(f"✅ Backfilled {len(data)} bars for {gap.vendor}/{gap.symbol}")
                            return True
                    else:
                        logger.warning(f"FMP backfill error for {gap.symbol}: {response.status}")

            except Exception as e:
                logger.warning(f"Error in FMP backfill for {gap.symbol}: {e}")

        return False

    async def _store_backfilled_data(self, vendor: str, symbol: str, results: List[Dict]):
        """Store backfilled data in vendor-specific table"""
        table_name = f"dev_one_minute_live_{vendor}"

        # Get instrument_id
        instrument_id = await self._get_instrument_id(symbol)

        query = f"""
            INSERT INTO {table_name} (
                instrument_id, symbol, timestamp, open_price, high_price,
                low_price, close_price, volume, vwap, trade_count,
                received_at, data_latency_ms, collection_method,
                is_realtime, quality_score, validation_status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (instrument_id, timestamp) DO NOTHING
        """

        async with self.pool.acquire() as conn:
            for result in results:
                timestamp = datetime.fromtimestamp(result['t'] / 1000)

                await conn.execute(
                    query,
                    instrument_id, symbol, timestamp,
                    float(result['o']), float(result['h']), float(result['l']), float(result['c']),
                    int(result['v']), result.get('vw'), result.get('n'),
                    datetime.now(), None, 'backfill', False, 0.9, 'backfilled'
                )

    async def _store_backfilled_tiingo_data(self, symbol: str, data: List[Dict]):
        """Store backfilled Tiingo data"""
        instrument_id = await self._get_instrument_id(symbol)

        query = """
            INSERT INTO dev_one_minute_live_tiingo (
                instrument_id, symbol, timestamp, open_price, high_price,
                low_price, close_price, volume, received_at, data_latency_ms,
                collection_method, is_realtime, quality_score, validation_status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (instrument_id, timestamp) DO NOTHING
        """

        async with self.pool.acquire() as conn:
            for result in data:
                timestamp = datetime.fromisoformat(result['date'].replace('Z', '+00:00'))

                await conn.execute(
                    query,
                    instrument_id, symbol, timestamp,
                    float(result['open']), float(result['high']),
                    float(result['low']), float(result['close']),
                    int(result['volume']), datetime.now(), None,
                    'backfill', False, 0.9, 'backfilled'
                )

    async def _store_backfilled_fmp_data(self, symbol: str, data: List[Dict]):
        """Store backfilled FMP data"""
        instrument_id = await self._get_instrument_id(symbol)

        query = """
            INSERT INTO dev_one_minute_live_fmp (
                instrument_id, symbol, timestamp, open_price, high_price,
                low_price, close_price, volume, received_at, data_latency_ms,
                collection_method, is_realtime, quality_score, validation_status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (instrument_id, timestamp) DO NOTHING
        """

        async with self.pool.acquire() as conn:
            for result in data:
                timestamp = datetime.fromisoformat(result['date'].replace('Z', '+00:00'))

                await conn.execute(
                    query,
                    instrument_id, symbol, timestamp,
                    float(result['open']), float(result['high']),
                    float(result['low']), float(result['close']),
                    int(result['volume']), datetime.now(), None,
                    'backfill', False, 0.9, 'backfilled'
                )

    async def _get_instrument_id(self, symbol: str) -> int:
        """Get instrument_id for symbol"""
        query = "SELECT id FROM dev_instrument WHERE symbol = $1 LIMIT 1"

        async with self.pool.acquire() as conn:
            result = await conn.fetchval(query, symbol)
            return result or 0

    async def _mark_gap_backfilled(self, gap: DataGap):
        """Mark gap as successfully backfilled"""
        query = """
            UPDATE dev_realtime_gaps
            SET
                backfill_status = 'completed',
                backfill_method = 'api_backfill',
                backfill_completed_at = now(),
                updated_at = now()
            WHERE vendor = $1 AND symbol = $2
              AND gap_start_timestamp = $3 AND gap_end_timestamp = $4
        """

        async with self.pool.acquire() as conn:
            await conn.execute(query, gap.vendor, gap.symbol, gap.gap_start, gap.gap_end)

    async def _mark_gap_failed(self, gap: DataGap, error_message: str = None):
        """Mark gap backfill as failed"""
        query = """
            UPDATE dev_realtime_gaps
            SET
                backfill_status = 'failed',
                backfill_error_message = $5,
                updated_at = now()
            WHERE vendor = $1 AND symbol = $2
              AND gap_start_timestamp = $3 AND gap_end_timestamp = $4
        """

        async with self.pool.acquire() as conn:
            await conn.execute(query, gap.vendor, gap.symbol, gap.gap_start, gap.gap_end, error_message)

    async def _update_collection_status(self, gaps: List[DataGap]):
        """Update collection status based on detected gaps"""
        symbol_vendor_gaps = {}

        # Group gaps by symbol/vendor
        for gap in gaps:
            key = (gap.vendor, gap.symbol)
            if key not in symbol_vendor_gaps:
                symbol_vendor_gaps[key] = []
            symbol_vendor_gaps[key].append(gap)

        # Update collection status
        async with self.pool.acquire() as conn:
            for (vendor, symbol), vendor_gaps in symbol_vendor_gaps.items():
                total_gap_minutes = sum(g.gap_duration_minutes for g in vendor_gaps)
                max_gap_minutes = max(g.gap_duration_minutes for g in vendor_gaps)

                # Reduce health score based on gaps
                health_penalty = min(0.5, total_gap_minutes / 60)  # Max 50% penalty

                await conn.execute("""
                    UPDATE dev_realtime_collection_status
                    SET
                        consecutive_missing_bars = consecutive_missing_bars + $3,
                        collection_health_score = GREATEST(0.0, collection_health_score - $4),
                        last_error_message = $5,
                        last_error_at = now(),
                        updated_at = now()
                    WHERE vendor = $1 AND symbol = $2
                """, vendor, symbol, len(vendor_gaps), health_penalty,
                f"{len(vendor_gaps)} gaps detected, max {max_gap_minutes} minutes")

    async def shutdown(self):
        """Cleanup resources"""
        if self.pool:
            await self.pool.close()

async def main():
    """Main entry point for gap detection"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    detector = GapDetectionEngine()

    try:
        await detector.initialize()
        await detector.run_gap_detection()
    except Exception as e:
        logger.error(f"💥 Gap detection failed: {e}")
        raise
    finally:
        await detector.shutdown()

if __name__ == "__main__":
    asyncio.run(main())