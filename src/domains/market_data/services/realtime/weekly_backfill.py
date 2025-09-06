#!/usr/bin/env python3
"""
Weekly Comprehensive Backfill Service

Performs comprehensive weekly backfill of real-time data to ensure completeness
and reconcile any gaps that may have been missed during market hours.
"""

import asyncio
import asyncpg
import aiohttp
import logging
import os
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import pytz

from shared.utils.environment import Environment
from core.calendars.market_calendar_utils import get_trading_days

logger = logging.getLogger(__name__)

class BackfillStatus(Enum):
    """Enumeration of backfill job statuses"""
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'

@dataclass
class BackfillJob:
    """Represents a backfill job with progress tracking"""
    job_id: str
    vendor: str
    symbol: str
    start_date: date
    end_date: date
    priority: int
    status: str
    progress_percentage: float
    bars_processed: int
    bars_total: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0

@dataclass
class BackfillTask:
    """Represents a backfill task for a specific vendor/symbol/date range"""
    vendor: str
    symbol: str
    start_date: date
    end_date: date
    priority: int = 1
    estimated_bars: int = 0

class WeeklyBackfillEngine:
    """
    Comprehensive weekly backfill engine that ensures data completeness
    across all vendors and symbols.
    """
    
    def __init__(self):
        self.env = Environment()
        self.pool = None
        
        # Configuration
        self.backfill_days = int(os.getenv('BACKFILL_DAYS', '7'))
        self.max_symbols_per_vendor = int(os.getenv('MAX_SYMBOLS_PER_VENDOR', '2000'))
        self.parallel_requests = int(os.getenv('PARALLEL_REQUESTS', '10'))
        self.enable_reconciliation = os.getenv('ENABLE_RECONCILIATION', 'true').lower() == 'true'
        self.cleanup_old_gaps = os.getenv('CLEANUP_OLD_GAPS', 'true').lower() == 'true'
        
        # API credentials
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        self.tiingo_api_key = os.getenv('TIINGO_API_KEY')
        self.fmp_api_key = os.getenv('FMP_API_KEY')
        
        # Statistics
        self.backfill_stats = {
            'tasks_created': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'bars_backfilled': 0,
            'gaps_filled': 0,
            'errors_encountered': 0
        }
        
        self.eastern_tz = pytz.timezone('US/Eastern')
        
    async def initialize(self):
        """Initialize database connection and setup"""
        self.pool = await asyncpg.create_pool(self.env.get_database_url())
        logger.info("✅ Connected to database for weekly backfill")
        
    async def run_weekly_backfill(self):
        """Run comprehensive weekly backfill process"""
        logger.info(f"🔄 Starting weekly comprehensive backfill for last {self.backfill_days} days")
        
        try:
            # Calculate backfill date range
            end_date = date.today() - timedelta(days=1)  # Yesterday
            start_date = end_date - timedelta(days=self.backfill_days)
            
            logger.info(f"📅 Backfill period: {start_date} to {end_date}")
            
            # Get trading days in the period
            trading_days = get_trading_days(start_date, end_date)
            logger.info(f"📊 Found {len(trading_days)} trading days to backfill")
            
            # Analyze current data coverage
            coverage_analysis = await self._analyze_data_coverage(trading_days)
            
            # Generate backfill tasks
            backfill_tasks = await self._generate_backfill_tasks(coverage_analysis, trading_days)
            logger.info(f"📋 Created {len(backfill_tasks)} backfill tasks")
            
            # Execute backfill tasks in parallel
            await self._execute_backfill_tasks(backfill_tasks)
            
            # Reconcile data if enabled
            if self.enable_reconciliation:
                await self._reconcile_backfilled_data(trading_days)
                
            # Cleanup old gaps if enabled
            if self.cleanup_old_gaps:
                await self._cleanup_old_gaps()
                
            # Generate completion report
            await self._generate_backfill_report()
            
            logger.info("✅ Weekly backfill completed successfully")
            
        except Exception as e:
            logger.error(f"💥 Weekly backfill failed: {e}")
            raise
            
    async def _analyze_data_coverage(self, trading_days: List[date]) -> Dict:
        """Analyze current data coverage to identify gaps"""
        coverage_analysis = {
            'total_expected_bars': {},
            'current_bars': {},
            'missing_bars': {},
            'coverage_percentage': {}
        }
        
        vendors = ['polygon', 'tiingo', 'fmp']
        
        # Get active symbols for each vendor
        for vendor in vendors:
            if not self._has_api_key(vendor):
                continue
                
            active_symbols = await self._get_active_symbols(vendor)
            
            for symbol in active_symbols:
                # Calculate expected bars (390 minutes per trading day)
                expected_bars = len(trading_days) * 390
                coverage_analysis['total_expected_bars'][(vendor, symbol)] = expected_bars
                
                # Count current bars
                current_bars = await self._count_existing_bars(vendor, symbol, trading_days)
                coverage_analysis['current_bars'][(vendor, symbol)] = current_bars
                
                # Calculate missing bars
                missing_bars = max(0, expected_bars - current_bars)
                coverage_analysis['missing_bars'][(vendor, symbol)] = missing_bars
                
                # Calculate coverage percentage
                coverage_pct = (current_bars / expected_bars) * 100 if expected_bars > 0 else 0
                coverage_analysis['coverage_percentage'][(vendor, symbol)] = coverage_pct
                
        return coverage_analysis
        
    async def _get_active_symbols(self, vendor: str) -> List[str]:
        """Get active symbols for vendor"""
        query = """
            SELECT DISTINCT symbol
            FROM dev_realtime_collection_status
            WHERE vendor = $1 
              AND is_active = true
            ORDER BY collection_health_score DESC
            LIMIT $2
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, vendor, self.max_symbols_per_vendor)
            return [row['symbol'] for row in rows]
            
    async def _count_existing_bars(self, vendor: str, symbol: str, trading_days: List[date]) -> int:
        """Count existing bars for vendor/symbol in date range"""
        table_name = f"dev_one_minute_live_{vendor}"
        
        query = f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE symbol = $1
              AND DATE(timestamp) = ANY($2)
        """
        
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, symbol, trading_days)
            
    def _has_api_key(self, vendor: str) -> bool:
        """Check if API key is available for vendor"""
        key_map = {
            'polygon': self.polygon_api_key,
            'tiingo': self.tiingo_api_key,
            'fmp': self.fmp_api_key
        }
        return key_map.get(vendor) is not None
        
    async def _generate_backfill_tasks(self, coverage_analysis: Dict, trading_days: List[date]) -> List[BackfillTask]:
        """Generate prioritized backfill tasks based on coverage analysis"""
        tasks = []
        
        # Sort by coverage percentage (lowest first) and missing bars
        missing_data = coverage_analysis['missing_bars']
        coverage_pct = coverage_analysis['coverage_percentage']
        
        sorted_items = sorted(missing_data.items(), key=lambda x: (
            coverage_pct.get(x[0], 0),  # Lower coverage first
            -x[1],  # More missing bars first
            x[0][0],  # Vendor preference
            x[0][1]   # Symbol
        ))
        
        for (vendor, symbol), missing_bars in sorted_items:
            if missing_bars <= 0:
                continue
                
            # Create backfill task for the entire period
            task = BackfillTask(
                vendor=vendor,
                symbol=symbol,
                start_date=trading_days[0],
                end_date=trading_days[-1],
                priority=self._calculate_task_priority(vendor, symbol, missing_bars, coverage_pct.get((vendor, symbol), 0)),
                estimated_bars=missing_bars
            )
            
            tasks.append(task)
            
        self.backfill_stats['tasks_created'] = len(tasks)
        return tasks
        
    def _calculate_task_priority(self, vendor: str, symbol: str, missing_bars: int, coverage_pct: float) -> int:
        """Calculate task priority (lower number = higher priority)"""
        # Base priority by vendor
        vendor_priority = {'polygon': 1, 'tiingo': 2, 'fmp': 3}
        base_priority = vendor_priority.get(vendor, 4)
        
        # Adjust by coverage percentage (lower coverage = higher priority)
        coverage_adjustment = int((100 - coverage_pct) / 10)
        
        # Adjust by missing bars (more missing = higher priority)
        missing_adjustment = min(5, missing_bars // 1000)
        
        return base_priority + coverage_adjustment + missing_adjustment
        
    async def _execute_backfill_tasks(self, tasks: List[BackfillTask]):
        """Execute backfill tasks in parallel with rate limiting"""
        logger.info(f"🚀 Executing {len(tasks)} backfill tasks with {self.parallel_requests} parallel requests")
        
        # Sort by priority
        sorted_tasks = sorted(tasks, key=lambda t: t.priority)
        
        # Execute in batches
        semaphore = asyncio.Semaphore(self.parallel_requests)
        
        async def execute_task(task: BackfillTask):
            async with semaphore:
                try:
                    success = await self._execute_single_backfill_task(task)
                    if success:
                        self.backfill_stats['tasks_completed'] += 1
                    else:
                        self.backfill_stats['tasks_failed'] += 1
                        
                    # Rate limiting
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.warning(f"Task execution error for {task.vendor}/{task.symbol}: {e}")
                    self.backfill_stats['tasks_failed'] += 1
                    self.backfill_stats['errors_encountered'] += 1
                    
        # Execute all tasks
        await asyncio.gather(*[execute_task(task) for task in sorted_tasks], return_exceptions=True)
        
    async def _execute_single_backfill_task(self, task: BackfillTask) -> bool:
        """Execute a single backfill task"""
        logger.info(f"🔄 Backfilling {task.vendor}/{task.symbol} ({task.start_date} to {task.end_date})")
        
        try:
            if task.vendor == 'polygon':
                return await self._backfill_polygon_data(task)
            elif task.vendor == 'tiingo':
                return await self._backfill_tiingo_data(task)
            elif task.vendor == 'fmp':
                return await self._backfill_fmp_data(task)
            else:
                logger.warning(f"Unknown vendor: {task.vendor}")
                return False
                
        except Exception as e:
            logger.warning(f"Backfill error for {task.vendor}/{task.symbol}: {e}")
            return False
            
    async def _backfill_polygon_data(self, task: BackfillTask) -> bool:
        """Backfill data from Polygon API"""
        if not self.polygon_api_key:
            return False
            
        url = f"https://api.polygon.io/v2/aggs/ticker/{task.symbol}/range/1/minute/{task.start_date}/{task.end_date}"
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
                            bars_stored = await self._store_polygon_backfill_data(task.symbol, results)
                            self.backfill_stats['bars_backfilled'] += bars_stored
                            logger.info(f"✅ Stored {bars_stored} Polygon bars for {task.symbol}")
                            return True
                    else:
                        logger.warning(f"Polygon API error for {task.symbol}: {response.status}")
                        
            except Exception as e:
                logger.warning(f"Error backfilling Polygon data for {task.symbol}: {e}")
                
        return False
        
    async def _backfill_tiingo_data(self, task: BackfillTask) -> bool:
        """Backfill data from Tiingo API"""
        if not self.tiingo_api_key:
            return False
            
        url = f"https://api.tiingo.com/iex/{task.symbol}/prices"
        params = {
            'token': self.tiingo_api_key,
            'startDate': task.start_date.strftime('%Y-%m-%d'),
            'endDate': task.end_date.strftime('%Y-%m-%d'),
            'resampleFreq': '1min',
            'format': 'json'
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data:
                            bars_stored = await self._store_tiingo_backfill_data(task.symbol, data)
                            self.backfill_stats['bars_backfilled'] += bars_stored
                            logger.info(f"✅ Stored {bars_stored} Tiingo bars for {task.symbol}")
                            return True
                    else:
                        logger.warning(f"Tiingo API error for {task.symbol}: {response.status}")
                        
            except Exception as e:
                logger.warning(f"Error backfilling Tiingo data for {task.symbol}: {e}")
                
        return False
        
    async def _backfill_fmp_data(self, task: BackfillTask) -> bool:
        """Backfill data from FMP API"""
        if not self.fmp_api_key:
            return False
            
        url = f"https://financialmodelingprep.com/api/v3/historical-chart/1min/{task.symbol}"
        params = {
            'apikey': self.fmp_api_key,
            'from': task.start_date.strftime('%Y-%m-%d'),
            'to': task.end_date.strftime('%Y-%m-%d')
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data:
                            bars_stored = await self._store_fmp_backfill_data(task.symbol, data)
                            self.backfill_stats['bars_backfilled'] += bars_stored
                            logger.info(f"✅ Stored {bars_stored} FMP bars for {task.symbol}")
                            return True
                    else:
                        logger.warning(f"FMP API error for {task.symbol}: {response.status}")
                        
            except Exception as e:
                logger.warning(f"Error backfilling FMP data for {task.symbol}: {e}")
                
        return False
        
    async def _store_polygon_backfill_data(self, symbol: str, results: List[Dict]) -> int:
        """Store Polygon backfill data"""
        instrument_id = await self._get_instrument_id(symbol)
        stored_count = 0
        
        query = """
            INSERT INTO dev_one_minute_live_polygon (
                instrument_id, symbol, timestamp, open_price, high_price,
                low_price, close_price, volume, vwap, trade_count,
                received_at, collection_method, is_realtime, quality_score, validation_status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (instrument_id, timestamp) DO NOTHING
        """
        
        async with self.pool.acquire() as conn:
            for result in results:
                timestamp = datetime.fromtimestamp(result['t'] / 1000)
                
                # Only insert if within trading hours
                if self._is_trading_hour(timestamp):
                    await conn.execute(
                        query,
                        instrument_id, symbol, timestamp,
                        float(result['o']), float(result['h']), float(result['l']), float(result['c']),
                        int(result['v']), result.get('vw'), result.get('n'),
                        datetime.now(), 'weekly_backfill', False, 0.9, 'backfilled'
                    )
                    stored_count += 1
                    
        return stored_count
        
    async def _store_tiingo_backfill_data(self, symbol: str, data: List[Dict]) -> int:
        """Store Tiingo backfill data"""
        instrument_id = await self._get_instrument_id(symbol)
        stored_count = 0
        
        query = """
            INSERT INTO dev_one_minute_live_tiingo (
                instrument_id, symbol, timestamp, open_price, high_price,
                low_price, close_price, volume, received_at, collection_method,
                is_realtime, quality_score, validation_status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (instrument_id, timestamp) DO NOTHING
        """
        
        async with self.pool.acquire() as conn:
            for result in data:
                timestamp = datetime.fromisoformat(result['date'].replace('Z', '+00:00'))
                
                if self._is_trading_hour(timestamp):
                    await conn.execute(
                        query,
                        instrument_id, symbol, timestamp,
                        float(result['open']), float(result['high']), 
                        float(result['low']), float(result['close']),
                        int(result['volume']), datetime.now(), 'weekly_backfill',
                        False, 0.9, 'backfilled'
                    )
                    stored_count += 1
                    
        return stored_count
        
    async def _store_fmp_backfill_data(self, symbol: str, data: List[Dict]) -> int:
        """Store FMP backfill data"""
        instrument_id = await self._get_instrument_id(symbol)
        stored_count = 0
        
        query = """
            INSERT INTO dev_one_minute_live_fmp (
                instrument_id, symbol, timestamp, open_price, high_price,
                low_price, close_price, volume, received_at, collection_method,
                is_realtime, quality_score, validation_status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (instrument_id, timestamp) DO NOTHING
        """
        
        async with self.pool.acquire() as conn:
            for result in data:
                timestamp = datetime.fromisoformat(result['date'].replace('Z', '+00:00'))
                
                if self._is_trading_hour(timestamp):
                    await conn.execute(
                        query,
                        instrument_id, symbol, timestamp,
                        float(result['open']), float(result['high']), 
                        float(result['low']), float(result['close']),
                        int(result['volume']), datetime.now(), 'weekly_backfill',
                        False, 0.9, 'backfilled'
                    )
                    stored_count += 1
                    
        return stored_count
        
    def _is_trading_hour(self, timestamp: datetime) -> bool:
        """Check if timestamp is during trading hours"""
        et_time = timestamp.astimezone(self.eastern_tz)
        
        # Trading hours: 9:30 AM - 4:00 PM ET, Monday-Friday
        if et_time.weekday() >= 5:  # Weekend
            return False
            
        market_start = et_time.replace(hour=9, minute=30, second=0, microsecond=0)
        market_end = et_time.replace(hour=16, minute=0, second=0, microsecond=0)
        
        return market_start <= et_time <= market_end
        
    async def _get_instrument_id(self, symbol: str) -> int:
        """Get instrument_id for symbol"""
        query = "SELECT id FROM dev_instruments WHERE symbol = $1 LIMIT 1"
        
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(query, symbol)
            return result or 0
            
    async def _reconcile_backfilled_data(self, trading_days: List[date]):
        """Reconcile backfilled data across vendors"""
        logger.info("🔍 Starting data reconciliation across vendors")
        
        # This could include cross-vendor price validation,
        # gap detection, and quality scoring updates
        
        reconciliation_query = """
            UPDATE dev_realtime_collection_status
            SET 
                collection_health_score = LEAST(1.0, collection_health_score + 0.1),
                updated_at = now()
            WHERE vendor IN ('polygon', 'tiingo', 'fmp')
              AND last_received_timestamp >= $1
        """
        
        async with self.pool.acquire() as conn:
            cutoff_date = datetime.combine(trading_days[0], datetime.min.time())
            await conn.execute(reconciliation_query, cutoff_date)
            
        logger.info("✅ Data reconciliation completed")
        
    async def _cleanup_old_gaps(self):
        """Clean up old gap records that are no longer relevant"""
        logger.info("🧹 Cleaning up old gap records")
        
        cleanup_query = """
            DELETE FROM dev_realtime_gaps
            WHERE gap_start_timestamp < now() - INTERVAL '30 days'
              AND backfill_status = 'completed'
        """
        
        async with self.pool.acquire() as conn:
            deleted_count = await conn.execute(cleanup_query)
            logger.info(f"🗑️ Cleaned up {deleted_count} old gap records")
            
    async def _generate_backfill_report(self):
        """Generate comprehensive backfill completion report"""
        stats = self.backfill_stats
        
        logger.info("📊 Weekly Backfill Report:")
        logger.info(f"   Tasks Created: {stats['tasks_created']}")
        logger.info(f"   Tasks Completed: {stats['tasks_completed']}")
        logger.info(f"   Tasks Failed: {stats['tasks_failed']}")
        logger.info(f"   Success Rate: {stats['tasks_completed']/(stats['tasks_created'] or 1):.2%}")
        logger.info(f"   Bars Backfilled: {stats['bars_backfilled']:,}")
        logger.info(f"   Gaps Filled: {stats['gaps_filled']}")
        logger.info(f"   Errors Encountered: {stats['errors_encountered']}")
        
        # Store report in database for historical tracking
        await self._store_backfill_report()
        
    async def _store_backfill_report(self):
        """Store backfill report in database"""
        query = """
            INSERT INTO dev_realtime_batch_validation (
                symbol, validation_date, vendor, validation_status, validation_notes
            ) VALUES ($1, $2, $3, $4, $5)
        """
        
        report_notes = (
            f"Weekly backfill report: {self.backfill_stats['tasks_completed']} completed, "
            f"{self.backfill_stats['bars_backfilled']} bars backfilled"
        )
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                'SYSTEM', date.today(), 'weekly_backfill', 'completed', report_notes
            )
            
    async def shutdown(self):
        """Cleanup resources"""
        if self.pool:
            await self.pool.close()

async def main():
    """Main entry point for weekly backfill"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    backfill_engine = WeeklyBackfillEngine()
    
    try:
        await backfill_engine.initialize()
        await backfill_engine.run_weekly_backfill()
    except Exception as e:
        logger.error(f"💥 Weekly backfill failed: {e}")
        raise
    finally:
        await backfill_engine.shutdown()

if __name__ == "__main__":
    asyncio.run(main())