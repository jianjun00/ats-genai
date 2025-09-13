#!/usr/bin/env python3
"""
Daily Real-Time vs Batch Data Validation

Compares real-time data collected during market hours with authoritative
batch API data to ensure accuracy and detect quality issues.
"""

import asyncio
import asyncpg
import aiohttp
import logging
import os
from datetime import datetime, timedelta, date
from typing import Dict, List
from dataclasses import dataclass
import pytz

from shared.utils.environment import Environment
from core.business.calendars.market_calendar_utils import get_previous_trading_day

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Validation result for a symbol/vendor combination"""
    symbol: str
    vendor: str
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
    validation_notes: str

class DailyValidationEngine:
    """
    Daily validation engine that compares real-time vs batch data quality.
    """

    def __init__(self):
        self.env = Environment()
        self.pool = None

        # Configuration
        self.validation_date = self._get_validation_date()
        self.max_symbols = int(os.getenv('MAX_VALIDATION_SYMBOLS', '2000'))
        self.price_tolerance = float(os.getenv('PRICE_TOLERANCE_PERCENT', '0.01'))
        self.enable_alerts = os.getenv('ENABLE_SLACK_ALERTS', 'false').lower() == 'true'

        # API credentials
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        self.tiingo_api_key = os.getenv('TIINGO_API_KEY')
        self.fmp_api_key = os.getenv('FMP_API_KEY')

        # Validation metrics
        self.validation_results = []
        self.validation_summary = {
            'total_symbols': 0,
            'total_vendors': 0,
            'passed_validations': 0,
            'failed_validations': 0,
            'avg_accuracy_score': 0.0,
            'critical_issues': []
        }

        self.eastern_tz = pytz.timezone('US/Eastern')

    def _get_validation_date(self) -> date:
        """Get the date to validate (previous trading day)"""
        validation_date_str = os.getenv('VALIDATION_DATE', 'yesterday')

        if validation_date_str == 'yesterday':
            return get_previous_trading_day(date.today())
        else:
            return datetime.strptime(validation_date_str, '%Y-%m-%d').date()

    async def initialize(self):
        """Initialize database connection"""
        self.pool = await asyncpg.create_pool(self.env.get_database_url())
        logger.info("✅ Connected to database for validation")

    async def run_daily_validation(self):
        """Run complete daily validation process"""
        logger.info(f"🔍 Starting daily validation for {self.validation_date}")

        try:
            # Get active symbols with real-time data
            active_symbols = await self._get_active_symbols()
            logger.info(f"📊 Validating {len(active_symbols)} symbols")

            # Validate each vendor
            vendors = ['polygon', 'tiingo', 'fmp']

            for vendor in vendors:
                if not self._has_api_key(vendor):
                    logger.warning(f"⚠️ Skipping {vendor} - no API key configured")
                    continue

                logger.info(f"🔬 Validating {vendor} data...")
                await self._validate_vendor(vendor, active_symbols)

            # Store validation results
            await self._store_validation_results()

            # Generate summary report
            await self._generate_validation_summary()

            # Send alerts if enabled
            if self.enable_alerts:
                await self._send_validation_alerts()

            logger.info("✅ Daily validation completed successfully")

        except Exception as e:
            logger.error(f"💥 Daily validation failed: {e}")
            raise

    async def _get_active_symbols(self) -> List[str]:
        """Get symbols that have real-time data for validation date"""
        query = """
            WITH realtime_symbols AS (
                SELECT DISTINCT symbol
                FROM (
                    SELECT symbol FROM dev_one_minute_live_polygon
                    WHERE DATE(timestamp) = $1
                    UNION
                    SELECT symbol FROM dev_one_minute_live_tiingo
                    WHERE DATE(timestamp) = $1
                    UNION
                    SELECT symbol FROM dev_one_minute_live_fmp
                    WHERE DATE(timestamp) = $1
                ) combined
            ),
            ranked_symbols AS (
                SELECT
                    rs.symbol,
                    COUNT(DISTINCT p.date) + COUNT(DISTINCT t.date) + COUNT(DISTINCT f.date) as data_days,
                    AVG(COALESCE(p.volume, t.volume, f.volume, 0)) as avg_volume
                FROM realtime_symbols rs
                LEFT JOIN dev_daily_price_polygon p ON rs.symbol = p.symbol
                    AND p.date >= $1 - INTERVAL '30 days'
                LEFT JOIN dev_daily_price_tiingo t ON rs.symbol = t.symbol
                    AND t.date >= $1 - INTERVAL '30 days'
                LEFT JOIN dev_daily_prices_fmp f ON rs.symbol = f.symbol
                    AND f.date >= $1 - INTERVAL '30 days'
                GROUP BY rs.symbol
                HAVING COUNT(DISTINCT p.date) + COUNT(DISTINCT t.date) + COUNT(DISTINCT f.date) >= 5
                ORDER BY data_days DESC, avg_volume DESC NULLS LAST
                LIMIT $2
            )
            SELECT symbol FROM ranked_symbols
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, self.validation_date, self.max_symbols)
            return [row['symbol'] for row in rows]

    def _has_api_key(self, vendor: str) -> bool:
        """Check if API key is available for vendor"""
        key_map = {
            'polygon': self.polygon_api_key,
            'tiingo': self.tiingo_api_key,
            'fmp': self.fmp_api_key
        }
        return key_map.get(vendor) is not None

    async def _validate_vendor(self, vendor: str, symbols: List[str]):
        """Validate real-time data against batch data for a vendor"""
        for symbol in symbols[:50]:  # Limit to avoid API rate limits
            try:
                # Get real-time data
                realtime_data = await self._get_realtime_data(vendor, symbol)

                # Get batch data from vendor API
                batch_data = await self._get_batch_data(vendor, symbol)

                # Compare and generate validation result
                validation_result = self._compare_data(vendor, symbol, realtime_data, batch_data)
                self.validation_results.append(validation_result)

                # Small delay to respect API rate limits
                await asyncio.sleep(0.2)

            except Exception as e:
                logger.warning(f"Validation error for {vendor}/{symbol}: {e}")
                continue

    async def _get_realtime_data(self, vendor: str, symbol: str) -> List[Dict]:
        """Get real-time data for symbol/vendor/date"""
        table_name = f"dev_one_minute_live_{vendor}"

        query = f"""
            SELECT
                timestamp,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                data_latency_ms,
                quality_score
            FROM {table_name}
            WHERE symbol = $1
              AND DATE(timestamp) = $2
            ORDER BY timestamp
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, symbol, self.validation_date)
            return [dict(row) for row in rows]

    async def _get_batch_data(self, vendor: str, symbol: str) -> List[Dict]:
        """Get batch data from vendor API for comparison"""
        if vendor == 'polygon':
            return await self._get_polygon_batch_data(symbol)
        elif vendor == 'tiingo':
            return await self._get_tiingo_batch_data(symbol)
        elif vendor == 'fmp':
            return await self._get_fmp_batch_data(symbol)
        else:
            return []

    async def _get_polygon_batch_data(self, symbol: str) -> List[Dict]:
        """Get batch minute data from Polygon API"""
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{self.validation_date}/{self.validation_date}"
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

                        return [{
                            'timestamp': datetime.fromtimestamp(r['t'] / 1000),
                            'open_price': float(r['o']),
                            'high_price': float(r['h']),
                            'low_price': float(r['l']),
                            'close_price': float(r['c']),
                            'volume': int(r['v'])
                        } for r in results]
                    else:
                        logger.warning(f"Polygon API error for {symbol}: {response.status}")
                        return []
            except Exception as e:
                logger.warning(f"Error fetching Polygon batch data for {symbol}: {e}")
                return []

    async def _get_tiingo_batch_data(self, symbol: str) -> List[Dict]:
        """Get batch minute data from Tiingo API"""
        url = f"https://api.tiingo.com/iex/{symbol}/prices"
        params = {
            'token': self.tiingo_api_key,
            'startDate': self.validation_date.strftime('%Y-%m-%d'),
            'endDate': self.validation_date.strftime('%Y-%m-%d'),
            'resampleFreq': '1min',
            'format': 'json'
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        return [{
                            'timestamp': datetime.fromisoformat(r['date'].replace('Z', '+00:00')),
                            'open_price': float(r['open']),
                            'high_price': float(r['high']),
                            'low_price': float(r['low']),
                            'close_price': float(r['close']),
                            'volume': int(r['volume'])
                        } for r in data if r]
                    else:
                        logger.warning(f"Tiingo API error for {symbol}: {response.status}")
                        return []
            except Exception as e:
                logger.warning(f"Error fetching Tiingo batch data for {symbol}: {e}")
                return []

    async def _get_fmp_batch_data(self, symbol: str) -> List[Dict]:
        """Get batch minute data from FMP API"""
        url = f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{symbol}"
        params = {
            'apikey': self.fmp_api_key,
            'from': self.validation_date.strftime('%Y-%m-%d'),
            'to': self.validation_date.strftime('%Y-%m-%d')
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        return [{
                            'timestamp': datetime.fromisoformat(r['date'].replace('Z', '+00:00')),
                            'open_price': float(r['open']),
                            'high_price': float(r['high']),
                            'low_price': float(r['low']),
                            'close_price': float(r['close']),
                            'volume': int(r['volume'])
                        } for r in data if r]
                    else:
                        logger.warning(f"FMP API error for {symbol}: {response.status}")
                        return []
            except Exception as e:
                logger.warning(f"Error fetching FMP batch data for {symbol}: {e}")
                return []

    def _compare_data(self, vendor: str, symbol: str, realtime_data: List[Dict], batch_data: List[Dict]) -> ValidationResult:
        """Compare real-time vs batch data and generate validation result"""
        # Create timestamp-based lookup for batch data
        batch_lookup = {r['timestamp']: r for r in batch_data}

        discrepant_prices = 0
        price_differences = []
        latency_values = []
        late_bars = 0

        for rt_bar in realtime_data:
            rt_timestamp = rt_bar['timestamp']

            # Find matching batch bar (allowing small time tolerance)
            batch_bar = None
            for offset in [0, 60, -60]:  # Check exact time, +/-1 minute
                lookup_time = rt_timestamp + timedelta(seconds=offset)
                if lookup_time in batch_lookup:
                    batch_bar = batch_lookup[lookup_time]
                    break

            if batch_bar:
                # Compare prices
                price_diff = abs(rt_bar['close_price'] - batch_bar['close_price']) / batch_bar['close_price']
                price_differences.append(price_diff)

                if price_diff > self.price_tolerance:
                    discrepant_prices += 1

            # Check latency
            if rt_bar.get('data_latency_ms'):
                latency_minutes = rt_bar['data_latency_ms'] / 60000
                latency_values.append(latency_minutes)

                if latency_minutes > 5:  # More than 5 minutes late
                    late_bars += 1

        # Calculate metrics
        avg_price_diff = sum(price_differences) / len(price_differences) if price_differences else 0
        max_price_diff = max(price_differences) if price_differences else 0
        avg_latency = sum(latency_values) / len(latency_values) if latency_values else 0
        max_latency = max(latency_values) if latency_values else 0

        # Calculate quality scores
        realtime_quality = sum(r.get('quality_score', 0.8) for r in realtime_data) / len(realtime_data) if realtime_data else 0
        batch_quality = 1.0  # Assume batch data is authoritative

        # Calculate overall accuracy
        accuracy_score = 1.0 - (discrepant_prices / len(realtime_data)) if realtime_data else 0

        # Determine validation status
        if accuracy_score >= 0.99 and avg_latency <= 2:
            status = 'passed'
        elif accuracy_score >= 0.95:
            status = 'warning'
        else:
            status = 'failed'

        notes = f"Compared {len(realtime_data)} RT bars with {len(batch_data)} batch bars"

        return ValidationResult(
            symbol=symbol,
            vendor=vendor,
            validation_date=self.validation_date,
            realtime_bars_count=len(realtime_data),
            batch_bars_count=len(batch_data),
            missing_realtime_bars=max(0, len(batch_data) - len(realtime_data)),
            discrepant_prices=discrepant_prices,
            avg_price_difference=avg_price_diff,
            max_price_difference=max_price_diff,
            avg_data_latency_minutes=avg_latency,
            max_data_latency_minutes=max_latency,
            late_bars_count=late_bars,
            realtime_quality_score=realtime_quality,
            batch_quality_score=batch_quality,
            overall_accuracy_score=accuracy_score,
            validation_status=status,
            validation_notes=notes
        )

    async def _store_validation_results(self):
        """Store validation results in database"""
        query = """
            INSERT INTO dev_realtime_batch_validation (
                symbol, validation_date, vendor, realtime_bars_count, batch_bars_count,
                missing_realtime_bars, discrepant_prices, avg_price_difference, max_price_difference,
                avg_data_latency_minutes, max_data_latency_minutes, late_bars_count,
                realtime_quality_score, batch_quality_score, overall_accuracy_score,
                validation_status, validation_notes
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
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
        """

        async with self.pool.acquire() as conn:
            for result in self.validation_results:
                await conn.execute(
                    query,
                    result.symbol, result.validation_date, result.vendor,
                    result.realtime_bars_count, result.batch_bars_count,
                    result.missing_realtime_bars, result.discrepant_prices,
                    result.avg_price_difference, result.max_price_difference,
                    result.avg_data_latency_minutes, result.max_data_latency_minutes,
                    result.late_bars_count, result.realtime_quality_score,
                    result.batch_quality_score, result.overall_accuracy_score,
                    result.validation_status, result.validation_notes
                )

        logger.info(f"📊 Stored {len(self.validation_results)} validation results")

    async def _generate_validation_summary(self):
        """Generate validation summary"""
        total_validations = len(self.validation_results)
        passed = sum(1 for r in self.validation_results if r.validation_status == 'passed')
        failed = sum(1 for r in self.validation_results if r.validation_status == 'failed')
        avg_accuracy = sum(r.overall_accuracy_score for r in self.validation_results) / total_validations if total_validations else 0

        self.validation_summary = {
            'validation_date': self.validation_date.isoformat(),
            'total_validations': total_validations,
            'passed_validations': passed,
            'failed_validations': failed,
            'warning_validations': total_validations - passed - failed,
            'success_rate': passed / total_validations if total_validations else 0,
            'avg_accuracy_score': avg_accuracy,
            'critical_issues': [r for r in self.validation_results if r.validation_status == 'failed']
        }

        logger.info(f"📈 Validation Summary for {self.validation_date}:")
        logger.info(f"   Total: {total_validations}, Passed: {passed}, Failed: {failed}")
        logger.info(f"   Success Rate: {self.validation_summary['success_rate']:.2%}")
        logger.info(f"   Avg Accuracy: {avg_accuracy:.4f}")

    async def _send_validation_alerts(self):
        """Send alerts for validation results"""
        # Implementation would depend on alerting system (Slack, email, etc.)
        critical_issues = self.validation_summary['critical_issues']

        if critical_issues:
            logger.warning(f"🚨 {len(critical_issues)} critical validation issues detected!")
            for issue in critical_issues[:5]:  # Show first 5
                logger.warning(f"   {issue.vendor}/{issue.symbol}: {issue.overall_accuracy_score:.2%} accuracy")

    async def shutdown(self):
        """Cleanup resources"""
        if self.pool:
            await self.pool.close()

async def main():
    """Main entry point for daily validation"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    validator = DailyValidationEngine()

    try:
        await validator.initialize()
        await validator.run_daily_validation()
    except Exception as e:
        logger.error(f"💥 Daily validation failed: {e}")
        raise
    finally:
        await validator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())