#!/usr/bin/env python3
"""
Real-time vs Batch Data Validation Service

Daily CronJob that compares real-time collected data against batch API results
to identify discrepancies, delays, and data quality issues. Runs after market close.
"""

import asyncio
import asyncpg
import logging
import os
import json
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import statistics
import aiohttp

from shared.utils.environment import Environment

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Validation result for a symbol/vendor combination"""
    vendor: str
    symbol: str
    validation_date: date
    realtime_bars_count: int
    batch_bars_count: int
    missing_realtime_bars: int
    discrepant_prices: int
    avg_price_difference: float
    max_price_difference: float
    avg_data_latency_minutes: float
    max_data_latency_minutes: float
    late_bars_count: int
    realtime_quality_score: float
    batch_quality_score: float
    overall_accuracy_score: float
    validation_status: str
    validation_notes: List[str]

class RealtimeBatchValidator:
    """
    Validates real-time data collection against batch API results.
    Runs as daily CronJob after market close.
    """

    def __init__(self):
        self.env = Environment()
        self.pool = None
        self.validation_date = date.today()

        # API credentials for batch validation
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        self.tiingo_api_key = os.getenv('TIINGO_API_KEY')
        self.fmp_api_key = os.getenv('FMP_API_KEY')

        # Validation thresholds
        self.max_acceptable_latency_minutes = float(os.getenv('MAX_LATENCY_MINUTES', '5.0'))
        self.max_price_difference_pct = float(os.getenv('MAX_PRICE_DIFF_PCT', '0.5'))
        self.min_accuracy_score = float(os.getenv('MIN_ACCURACY_SCORE', '0.95'))
        self.min_data_completeness = float(os.getenv('MIN_COMPLETENESS', '0.90'))

        # Batch size for processing
        self.batch_size = int(os.getenv('VALIDATION_BATCH_SIZE', '50'))

        # Override validation date if provided
        if os.getenv('VALIDATION_DATE'):
            self.validation_date = date.fromisoformat(os.getenv('VALIDATION_DATE'))

    async def initialize(self):
        """Initialize database connection"""
        self.pool = await asyncpg.create_pool(self.env.get_database_url())
        logger.info("✅ Connected to database for validation")

    async def run_daily_validation(self):
        """Run complete daily validation process"""
        logger.info(f"🔍 Starting daily validation for {self.validation_date}")

        try:
            # Get symbols to validate
            symbols = await self._get_validation_symbols()
            logger.info(f"📊 Validating {len(symbols)} symbols")

            # Validate each vendor
            vendors = ['polygon', 'tiingo', 'fmp']
            all_results = []

            for vendor in vendors:
                if not self._has_api_key(vendor):
                    logger.warning(f"⚠️ Skipping {vendor} - no API key configured")
                    continue

                logger.info(f"🔍 Validating {vendor} data...")
                vendor_results = await self._validate_vendor(vendor, symbols)
                all_results.extend(vendor_results)

            # Store validation results
            await self._store_validation_results(all_results)

            # Generate summary report
            await self._generate_validation_report(all_results)

            logger.info(f"✅ Daily validation completed - {len(all_results)} results")

        except Exception as e:
            logger.error(f"💥 Validation failed: {e}")
            raise

    async def _get_validation_symbols(self) -> List[Tuple[str, int]]:
        """Get symbols that had real-time collection yesterday"""
        query = """
            SELECT DISTINCT symbol,
                   COALESCE(i.id, 0) as instrument_id
            FROM (
                SELECT DISTINCT symbol FROM dev_one_minute_live_polygon
                WHERE timestamp::date = $1
                UNION
                SELECT DISTINCT symbol FROM dev_one_minute_live_tiingo
                WHERE timestamp::date = $1
                UNION
                SELECT DISTINCT symbol FROM dev_one_minute_live_fmp
                WHERE timestamp::date = $1
            ) symbols
            LEFT JOIN dev_instrument i ON symbols.symbol = i.symbol
            ORDER BY symbol
            LIMIT 500  -- Limit for processing time
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, self.validation_date)
            return [(row['symbol'], row['instrument_id']) for row in rows]

    def _has_api_key(self, vendor: str) -> bool:
        """Check if API key is configured for vendor"""
        if vendor == 'polygon':
            return bool(self.polygon_api_key)
        elif vendor == 'tiingo':
            return bool(self.tiingo_api_key)
        elif vendor == 'fmp':
            return bool(self.fmp_api_key)
        return False

    async def _validate_vendor(self, vendor: str, symbols: List[Tuple[str, int]]) -> List[ValidationResult]:
        """Validate a specific vendor's data"""
        results = []

        # Process symbols in batches
        for i in range(0, len(symbols), self.batch_size):
            batch = symbols[i:i + self.batch_size]
            batch_results = await self._validate_symbol_batch(vendor, batch)
            results.extend(batch_results)

            logger.info(f"📊 {vendor}: Processed batch {i//self.batch_size + 1}/{(len(symbols)-1)//self.batch_size + 1}")

            # Rate limiting
            await asyncio.sleep(1)

        return results

    async def _validate_symbol_batch(self, vendor: str, symbols: List[Tuple[str, int]]) -> List[ValidationResult]:
        """Validate a batch of symbols for a vendor"""
        results = []

        for symbol, instrument_id in symbols:
            try:
                result = await self._validate_single_symbol(vendor, symbol, instrument_id)
                if result:
                    results.append(result)

            except Exception as e:
                logger.debug(f"Validation error for {vendor}/{symbol}: {e}")

        return results

    async def _validate_single_symbol(self, vendor: str, symbol: str, instrument_id: int) -> Optional[ValidationResult]:
        """Validate a single symbol for a vendor"""
        try:
            # Get real-time data for the day
            realtime_data = await self._get_realtime_data(vendor, symbol, self.validation_date)

            # Get batch data from API
            batch_data = await self._get_batch_data(vendor, symbol, self.validation_date)

            # Compare and analyze
            return await self._compare_data(vendor, symbol, realtime_data, batch_data)

        except Exception as e:
            logger.debug(f"Symbol validation error {vendor}/{symbol}: {e}")
            return None

    async def _get_realtime_data(self, vendor: str, symbol: str, target_date: date) -> List[Dict]:
        """Get real-time data for a symbol/vendor/date"""
        table_name = f"dev_one_minute_live_{vendor}"

        query = f"""
            SELECT
                timestamp,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                received_at,
                data_latency_ms,
                quality_score
            FROM {table_name}
            WHERE symbol = $1
              AND timestamp::date = $2
              AND is_realtime = true
            ORDER BY timestamp
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, symbol, target_date)
            return [dict(row) for row in rows]

    async def _get_batch_data(self, vendor: str, symbol: str, target_date: date) -> List[Dict]:
        """Get batch data from vendor API for comparison"""
        if vendor == 'polygon':
            return await self._get_polygon_batch_data(symbol, target_date)
        elif vendor == 'tiingo':
            return await self._get_tiingo_batch_data(symbol, target_date)
        elif vendor == 'fmp':
            return await self._get_fmp_batch_data(symbol, target_date)
        return []

    async def _get_polygon_batch_data(self, symbol: str, target_date: date) -> List[Dict]:
        """Get Polygon batch minute data via API"""
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{target_date}/{target_date}"
            params = {
                'apikey': self.polygon_api_key,
                'adjusted': 'true',
                'sort': 'asc',
                'limit': 50000
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = data.get('results', [])

                        return [{
                            'timestamp': datetime.fromtimestamp(bar['t'] / 1000, tz=timezone.utc),
                            'open_price': float(bar['o']),
                            'high_price': float(bar['h']),
                            'low_price': float(bar['l']),
                            'close_price': float(bar['c']),
                            'volume': int(bar['v'])
                        } for bar in results]

        except Exception as e:
            logger.debug(f"Polygon batch data error for {symbol}: {e}")

        return []

    async def _get_tiingo_batch_data(self, symbol: str, target_date: date) -> List[Dict]:
        """Get Tiingo batch minute data via API"""
        try:
            url = f"https://api.tiingo.com/iex/{symbol}/prices"
            params = {
                'token': self.tiingo_api_key,
                'startDate': target_date.isoformat(),
                'endDate': target_date.isoformat(),
                'resampleFreq': '1min',
                'format': 'json',
                'columns': 'open,high,low,close,volume'
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        return [{
                            'timestamp': datetime.fromisoformat(bar['date'].replace('Z', '+00:00')),
                            'open_price': float(bar['open']),
                            'high_price': float(bar['high']),
                            'low_price': float(bar['low']),
                            'close_price': float(bar['close']),
                            'volume': int(bar['volume'])
                        } for bar in data if bar]

        except Exception as e:
            logger.debug(f"Tiingo batch data error for {symbol}: {e}")

        return []

    async def _get_fmp_batch_data(self, symbol: str, target_date: date) -> List[Dict]:
        """Get FMP batch minute data via API"""
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{symbol}"
            params = {
                'apikey': self.fmp_api_key,
                'from': target_date.isoformat(),
                'to': target_date.isoformat()
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        return [{
                            'timestamp': datetime.fromisoformat(bar['date'].replace('Z', '+00:00')),
                            'open_price': float(bar['open']),
                            'high_price': float(bar['high']),
                            'low_price': float(bar['low']),
                            'close_price': float(bar['close']),
                            'volume': int(bar['volume'])
                        } for bar in data if bar]

        except Exception as e:
            logger.debug(f"FMP batch data error for {symbol}: {e}")

        return []

    async def _compare_data(self, vendor: str, symbol: str, realtime_data: List[Dict], batch_data: List[Dict]) -> ValidationResult:
        """Compare real-time vs batch data and generate validation result"""

        # Create timestamp lookup for batch data
        batch_lookup = {bar['timestamp']: bar for bar in batch_data}

        # Analysis variables
        price_differences = []
        latency_minutes = []
        missing_realtime = 0
        discrepant_prices = 0
        late_bars = 0
        validation_notes = []

        # Compare each real-time bar with batch equivalent
        for rt_bar in realtime_data:
            rt_timestamp = rt_bar['timestamp']

            # Find matching batch bar (within 1 minute tolerance)
            batch_bar = None
            for tolerance in [0, 1, -1]:  # Try exact match, then ±1 minute
                lookup_time = rt_timestamp + timedelta(minutes=tolerance)
                if lookup_time in batch_lookup:
                    batch_bar = batch_lookup[lookup_time]
                    break

            if not batch_bar:
                missing_realtime += 1
                continue

            # Compare prices
            rt_close = rt_bar['close_price']
            batch_close = batch_bar['close_price']

            if rt_close and batch_close and rt_close > 0:
                price_diff_pct = abs((rt_close - batch_close) / batch_close) * 100
                price_differences.append(price_diff_pct)

                if price_diff_pct > self.max_price_difference_pct:
                    discrepant_prices += 1

            # Analyze latency
            if rt_bar.get('data_latency_ms'):
                latency_min = rt_bar['data_latency_ms'] / 60000  # Convert ms to minutes
                latency_minutes.append(latency_min)

                if latency_min > self.max_acceptable_latency_minutes:
                    late_bars += 1

        # Check for missing batch bars
        realtime_timestamps = {bar['timestamp'] for bar in realtime_data}
        sum(1 for bar in batch_data if bar['timestamp'] not in realtime_timestamps)

        # Calculate quality scores
        realtime_quality = statistics.mean([bar.get('quality_score', 0.8) for bar in realtime_data]) if realtime_data else 0.0
        batch_quality = 1.0  # Assume batch data is high quality

        # Calculate overall accuracy
        total_expected_bars = len(batch_data)
        accuracy_score = 1.0

        if total_expected_bars > 0:
            completeness = len(realtime_data) / total_expected_bars
            accuracy_score *= completeness

        if price_differences:
            avg_price_error = statistics.mean(price_differences)
            accuracy_score *= max(0.0, 1.0 - (avg_price_error / 100))

        if latency_minutes:
            avg_latency = statistics.mean(latency_minutes)
            latency_penalty = min(0.5, avg_latency / self.max_acceptable_latency_minutes * 0.2)
            accuracy_score *= (1.0 - latency_penalty)

        # Determine validation status
        if accuracy_score >= self.min_accuracy_score:
            status = 'valid'
        elif accuracy_score >= 0.8:
            status = 'warning'
        else:
            status = 'failed'

        # Generate notes
        if missing_realtime > 0:
            validation_notes.append(f"Missing {missing_realtime} real-time bars")
        if discrepant_prices > 0:
            validation_notes.append(f"{discrepant_prices} price discrepancies > {self.max_price_difference_pct}%")
        if late_bars > 0:
            validation_notes.append(f"{late_bars} bars with latency > {self.max_acceptable_latency_minutes} minutes")

        return ValidationResult(
            vendor=vendor,
            symbol=symbol,
            validation_date=self.validation_date,
            realtime_bars_count=len(realtime_data),
            batch_bars_count=len(batch_data),
            missing_realtime_bars=missing_realtime,
            discrepant_prices=discrepant_prices,
            avg_price_difference=statistics.mean(price_differences) if price_differences else 0.0,
            max_price_difference=max(price_differences) if price_differences else 0.0,
            avg_data_latency_minutes=statistics.mean(latency_minutes) if latency_minutes else 0.0,
            max_data_latency_minutes=max(latency_minutes) if latency_minutes else 0.0,
            late_bars_count=late_bars,
            realtime_quality_score=realtime_quality,
            batch_quality_score=batch_quality,
            overall_accuracy_score=accuracy_score,
            validation_status=status,
            validation_notes=validation_notes
        )

    async def _store_validation_results(self, results: List[ValidationResult]):
        """Store validation results in database"""
        if not results:
            return

        async with self.pool.acquire() as conn:
            for result in results:
                await conn.execute("""
                    INSERT INTO dev_realtime_batch_validation (
                        symbol, validation_date, vendor, realtime_bars_count,
                        batch_bars_count, missing_realtime_bars, discrepant_prices,
                        avg_price_difference, max_price_difference, avg_data_latency_minutes,
                        max_data_latency_minutes, late_bars_count, realtime_quality_score,
                        batch_quality_score, overall_accuracy_score, validation_status,
                        validation_notes
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                    ON CONFLICT (symbol, validation_date, vendor)
                    DO UPDATE SET
                        realtime_bars_count = EXCLUDED.realtime_bars_count,
                        batch_bars_count = EXCLUDED.batch_bars_count,
                        missing_realtime_bars = EXCLUDED.missing_realtime_bars,
                        discrepant_prices = EXCLUDED.discrepant_prices,
                        avg_price_difference = EXCLUDED.avg_price_difference,
                        max_price_difference = EXCLUDED.max_price_difference,
                        avg_data_latency_minutes = EXCLUDED.avg_data_latency_minutes,
                        max_data_latency_minutes = EXCLUDED.max_data_latency_minutes,
                        late_bars_count = EXCLUDED.late_bars_count,
                        realtime_quality_score = EXCLUDED.realtime_quality_score,
                        batch_quality_score = EXCLUDED.batch_quality_score,
                        overall_accuracy_score = EXCLUDED.overall_accuracy_score,
                        validation_status = EXCLUDED.validation_status,
                        validation_notes = EXCLUDED.validation_notes,
                        created_at = now()
                """,
                result.symbol, result.validation_date, result.vendor,
                result.realtime_bars_count, result.batch_bars_count,
                result.missing_realtime_bars, result.discrepant_prices,
                result.avg_price_difference, result.max_price_difference,
                result.avg_data_latency_minutes, result.max_data_latency_minutes,
                result.late_bars_count, result.realtime_quality_score,
                result.batch_quality_score, result.overall_accuracy_score,
                result.validation_status, json.dumps(result.validation_notes)
                )

        logger.info(f"📊 Stored {len(results)} validation results")

    async def _generate_validation_report(self, results: List[ValidationResult]):
        """Generate summary validation report"""
        if not results:
            return

        # Calculate summary statistics
        total_validations = len(results)
        valid_count = sum(1 for r in results if r.validation_status == 'valid')
        warning_count = sum(1 for r in results if r.validation_status == 'warning')
        failed_count = sum(1 for r in results if r.validation_status == 'failed')

        avg_accuracy = statistics.mean([r.overall_accuracy_score for r in results])
        avg_latency = statistics.mean([r.avg_data_latency_minutes for r in results if r.avg_data_latency_minutes > 0])

        # Group by vendor
        vendor_stats = {}
        for result in results:
            vendor = result.vendor
            if vendor not in vendor_stats:
                vendor_stats[vendor] = {'count': 0, 'accuracy': [], 'latency': []}

            vendor_stats[vendor]['count'] += 1
            vendor_stats[vendor]['accuracy'].append(result.overall_accuracy_score)
            if result.avg_data_latency_minutes > 0:
                vendor_stats[vendor]['latency'].append(result.avg_data_latency_minutes)

        # Log summary
        logger.info(f"📋 Validation Summary for {self.validation_date}")
        logger.info(f"   Total validations: {total_validations}")
        logger.info(f"   Valid: {valid_count} ({valid_count/total_validations*100:.1f}%)")
        logger.info(f"   Warning: {warning_count} ({warning_count/total_validations*100:.1f}%)")
        logger.info(f"   Failed: {failed_count} ({failed_count/total_validations*100:.1f}%)")
        logger.info(f"   Average accuracy: {avg_accuracy:.3f}")
        logger.info(f"   Average latency: {avg_latency:.1f} minutes")

        for vendor, stats in vendor_stats.items():
            vendor_accuracy = statistics.mean(stats['accuracy'])
            vendor_latency = statistics.mean(stats['latency']) if stats['latency'] else 0
            logger.info(f"   {vendor}: {stats['count']} symbols, {vendor_accuracy:.3f} accuracy, {vendor_latency:.1f}min latency")

    async def shutdown(self):
        """Graceful shutdown"""
        if self.pool:
            await self.pool.close()

async def main():
    """Main entry point for CronJob"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    validator = RealtimeBatchValidator()

    try:
        await validator.initialize()
        await validator.run_daily_validation()
        logger.info("✅ Daily validation completed successfully")
    except Exception as e:
        logger.error(f"💥 Validation failed: {e}")
        raise
    finally:
        await validator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())