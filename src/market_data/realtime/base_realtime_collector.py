#!/usr/bin/env python3
"""
Base Real-Time Market Data Collector

Foundation for vendor-specific real-time 1-minute bar collection with:
- Gap detection and overlap handling
- Database storage with conflict resolution
- Quality scoring and latency tracking
- Market hours intelligence
- Universe management
"""

import asyncio
import logging
from datetime import datetime, timedelta, time
from typing import List, Dict, Any, Optional, Set
from abc import ABC, abstractmethod
import pytz
from dataclasses import dataclass
import json

import asyncpg
from config.environment import Environment

@dataclass
class MinuteBar:
    """Standardized minute bar structure for real-time collection."""
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    
    # Optional vendor-specific fields
    vwap: Optional[float] = None
    trade_count: Optional[int] = None
    adj_close_price: Optional[float] = None
    
    # Real-time metadata
    received_at: datetime = None
    data_latency_ms: Optional[int] = None
    collection_method: str = 'unknown'
    quality_score: float = 0.8
    data_source_metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.received_at is None:
            self.received_at = datetime.now(pytz.UTC)
        if self.data_source_metadata is None:
            self.data_source_metadata = {}
            
        # Calculate latency if possible
        if self.data_latency_ms is None and self.timestamp and self.received_at:
            # Ensure both timestamps are timezone-aware
            if self.timestamp.tzinfo is None:
                # Assume market data is in Eastern time
                eastern = pytz.timezone('US/Eastern')
                self.timestamp = eastern.localize(self.timestamp)
            
            if self.received_at.tzinfo is None:
                self.received_at = pytz.UTC.localize(self.received_at)
            
            # Calculate latency in milliseconds
            latency_delta = self.received_at - self.timestamp
            self.data_latency_ms = int(latency_delta.total_seconds() * 1000)

@dataclass
class CollectionStatus:
    """Real-time collection status for monitoring."""
    vendor: str
    symbol: str
    last_received_timestamp: Optional[datetime] = None
    expected_timestamp: Optional[datetime] = None
    data_delay_minutes: int = 0
    consecutive_missing_bars: int = 0
    total_bars_today: int = 0
    successful_collections: int = 0
    failed_collections: int = 0
    avg_latency_ms: float = 0.0
    collection_health_score: float = 1.0
    is_active: bool = True
    last_error_message: Optional[str] = None
    last_error_at: Optional[datetime] = None

class MarketHours:
    """Market hours intelligence for real-time collection."""
    
    def __init__(self):
        self.eastern = pytz.timezone('US/Eastern')
        
        # Standard market hours
        self.market_open = time(9, 30)  # 9:30 AM ET
        self.market_close = time(16, 0)  # 4:00 PM ET
        
        # Extended hours
        self.premarket_open = time(4, 0)   # 4:00 AM ET
        self.afterhours_close = time(20, 0)  # 8:00 PM ET
        
    def is_market_hours(self, dt: Optional[datetime] = None) -> bool:
        """Check if given time is during regular market hours."""
        if dt is None:
            dt = datetime.now(self.eastern)
        elif dt.tzinfo is None:
            dt = self.eastern.localize(dt)
        else:
            dt = dt.astimezone(self.eastern)
        
        # Check if weekend
        if dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        current_time = dt.time()
        return self.market_open <= current_time <= self.market_close
    
    def is_extended_hours(self, dt: Optional[datetime] = None) -> bool:
        """Check if given time is during extended trading hours."""
        if dt is None:
            dt = datetime.now(self.eastern)
        elif dt.tzinfo is None:
            dt = self.eastern.localize(dt)
        else:
            dt = dt.astimezone(self.eastern)
        
        # Check if weekend
        if dt.weekday() >= 5:
            return False
        
        current_time = dt.time()
        return (self.premarket_open <= current_time < self.market_open or 
                self.market_close < current_time <= self.afterhours_close)
    
    def is_trading_day(self, dt: Optional[datetime] = None) -> bool:
        """Check if given day is a trading day (no weekends, basic holiday check)."""
        if dt is None:
            dt = datetime.now(self.eastern)
        elif dt.tzinfo is None:
            dt = self.eastern.localize(dt)
        else:
            dt = dt.astimezone(self.eastern)
        
        # Basic weekend check (full holiday calendar would be more comprehensive)
        return dt.weekday() < 5
    
    def next_market_open(self, dt: Optional[datetime] = None) -> datetime:
        """Get next market open time."""
        if dt is None:
            dt = datetime.now(self.eastern)
        elif dt.tzinfo is None:
            dt = self.eastern.localize(dt)
        else:
            dt = dt.astimezone(self.eastern)
        
        # If before market open today and it's a trading day
        if (dt.time() < self.market_open and self.is_trading_day(dt)):
            return dt.replace(hour=9, minute=30, second=0, microsecond=0)
        
        # Otherwise, next trading day at market open
        next_day = dt + timedelta(days=1)
        while not self.is_trading_day(next_day):
            next_day += timedelta(days=1)
        
        return next_day.replace(hour=9, minute=30, second=0, microsecond=0)

class BaseRealtimeCollector(ABC):
    """Base class for vendor-specific real-time collectors."""
    
    def __init__(self, vendor_name: str, environment: Environment):
        self.vendor_name = vendor_name.lower()
        self.env = environment
        self.logger = logging.getLogger(f"realtime.{self.vendor_name}")
        
        self.market_hours = MarketHours()
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Collection state
        self.active_symbols: Set[str] = set()
        self.collection_stats: Dict[str, CollectionStatus] = {}
        self.is_running = False
        self.collection_task: Optional[asyncio.Task] = None
        
        # Configuration
        self.max_concurrent_symbols = 50
        self.collection_interval_seconds = 60  # 1 minute
        self.quality_threshold = 0.7
        self.max_data_latency_minutes = 5
        
        self.logger.info(f"Initialized {vendor_name} real-time collector")
    
    async def initialize(self):
        """Initialize database connections and load universe."""
        try:
            # Initialize database pool
            db_url = self.env.get_database_url()
            self.db_pool = await asyncpg.create_pool(
                db_url,
                min_size=2,
                max_size=10,
                command_timeout=30
            )
            
            # Load active universe
            await self.load_active_universe()
            
            self.logger.info(f"✅ {self.vendor_name.upper()} collector initialized with {len(self.active_symbols)} symbols")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize {self.vendor_name} collector: {e}")
            raise
    
    async def load_active_universe(self):
        """Load active trading universe from database."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get active instruments from universe membership
                query = """
                    SELECT DISTINCT i.symbol
                    FROM dev_instruments i
                    INNER JOIN dev_universe_membership um ON i.id = um.instrument_id  
                    WHERE um.is_active = true
                      AND i.symbol IS NOT NULL
                      AND LENGTH(i.symbol) <= 5
                    ORDER BY i.symbol
                    LIMIT $1
                """
                
                rows = await conn.fetch(query, 2000)  # Limit to top 2000 symbols
                self.active_symbols = {row['symbol'] for row in rows}
                
                # Initialize collection status for each symbol
                for symbol in self.active_symbols:
                    self.collection_stats[symbol] = CollectionStatus(
                        vendor=self.vendor_name,
                        symbol=symbol
                    )
                
                self.logger.info(f"Loaded {len(self.active_symbols)} symbols from active universe")
                
        except Exception as e:
            self.logger.error(f"Failed to load universe: {e}")
            # Fallback to popular symbols
            self.active_symbols = {
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'CRM', 'ADBE'
            }
            for symbol in self.active_symbols:
                self.collection_stats[symbol] = CollectionStatus(
                    vendor=self.vendor_name,
                    symbol=symbol
                )
            self.logger.warning(f"Using fallback symbols: {len(self.active_symbols)} symbols")
    
    async def store_minute_bar(self, bar: MinuteBar) -> bool:
        """Store minute bar in vendor-specific database table."""
        try:
            table_name = f"dev_one_minute_live_{self.vendor_name}"
            
            async with self.db_pool.acquire() as conn:
                # Get instrument_id for symbol
                instrument_query = """
                    SELECT id FROM dev_instruments WHERE symbol = $1 LIMIT 1
                """
                instrument_row = await conn.fetchrow(instrument_query, bar.symbol)
                
                if not instrument_row:
                    self.logger.warning(f"No instrument found for symbol {bar.symbol}")
                    return False
                
                instrument_id = instrument_row['id']
                
                # Insert or update minute bar (handle overlaps)
                upsert_query = f"""
                    INSERT INTO {table_name} (
                        instrument_id, symbol, timestamp, open_price, high_price, 
                        low_price, close_price, volume, vwap, trade_count, 
                        adj_close_price, received_at, data_latency_ms, 
                        collection_method, is_realtime, quality_score, 
                        validation_status, data_source_metadata
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
                    )
                    ON CONFLICT (instrument_id, timestamp) 
                    DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        vwap = EXCLUDED.vwap,
                        trade_count = EXCLUDED.trade_count,
                        adj_close_price = EXCLUDED.adj_close_price,
                        received_at = EXCLUDED.received_at,
                        data_latency_ms = EXCLUDED.data_latency_ms,
                        collection_method = EXCLUDED.collection_method,
                        quality_score = EXCLUDED.quality_score,
                        data_source_metadata = EXCLUDED.data_source_metadata,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE {table_name}.updated_at < EXCLUDED.received_at
                """
                
                await conn.execute(
                    upsert_query,
                    instrument_id, bar.symbol, bar.timestamp, bar.open_price,
                    bar.high_price, bar.low_price, bar.close_price, bar.volume,
                    bar.vwap, bar.trade_count, bar.adj_close_price, bar.received_at,
                    bar.data_latency_ms, bar.collection_method, True, bar.quality_score,
                    'pending', json.dumps(bar.data_source_metadata)
                )
                
                # Update collection status
                await self.update_collection_status(bar.symbol, bar, success=True)
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to store minute bar for {bar.symbol}: {e}")
            await self.update_collection_status(bar.symbol, bar, success=False, error=str(e))
            return False
    
    async def update_collection_status(self, symbol: str, bar: Optional[MinuteBar] = None, 
                                     success: bool = True, error: Optional[str] = None):
        """Update collection status for symbol."""
        try:
            status = self.collection_stats.get(symbol)
            if not status:
                return
            
            if success and bar:
                status.last_received_timestamp = bar.timestamp
                status.successful_collections += 1
                status.total_bars_today += 1
                status.consecutive_missing_bars = 0
                
                # Update latency statistics
                if bar.data_latency_ms is not None:
                    if status.avg_latency_ms == 0:
                        status.avg_latency_ms = bar.data_latency_ms
                    else:
                        # Running average
                        status.avg_latency_ms = (status.avg_latency_ms * 0.9 + bar.data_latency_ms * 0.1)
                
            else:
                status.failed_collections += 1
                status.consecutive_missing_bars += 1
                if error:
                    status.last_error_message = error
                    status.last_error_at = datetime.now(pytz.UTC)
            
            # Calculate health score
            status.collection_health_score = self.calculate_health_score(status)
            
            # Store in database
            await self.store_collection_status(status)
            
        except Exception as e:
            self.logger.error(f"Failed to update collection status for {symbol}: {e}")
    
    def calculate_health_score(self, status: CollectionStatus) -> float:
        """Calculate collection health score (0.0 to 1.0)."""
        score = 1.0
        
        # Penalize for consecutive missing bars
        if status.consecutive_missing_bars > 0:
            score -= min(0.5, status.consecutive_missing_bars * 0.1)
        
        # Penalize for high latency
        if status.avg_latency_ms > 60000:  # > 1 minute
            score -= min(0.3, (status.avg_latency_ms - 60000) / 300000)  # 5 minutes = 0.3 penalty
        
        # Penalize for recent failures
        if status.failed_collections > 0:
            failure_rate = status.failed_collections / max(1, status.successful_collections + status.failed_collections)
            score -= min(0.4, failure_rate * 0.8)
        
        return max(0.0, min(1.0, score))
    
    async def store_collection_status(self, status: CollectionStatus):
        """Store collection status in database."""
        try:
            async with self.db_pool.acquire() as conn:
                upsert_query = """
                    INSERT INTO dev_realtime_collection_status (
                        vendor, symbol, last_received_timestamp, expected_timestamp,
                        data_delay_minutes, consecutive_missing_bars, total_bars_today,
                        successful_collections, failed_collections, avg_latency_ms,
                        collection_health_score, is_active, last_error_message, last_error_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (vendor, symbol)
                    DO UPDATE SET
                        last_received_timestamp = EXCLUDED.last_received_timestamp,
                        expected_timestamp = EXCLUDED.expected_timestamp,
                        data_delay_minutes = EXCLUDED.data_delay_minutes,
                        consecutive_missing_bars = EXCLUDED.consecutive_missing_bars,
                        total_bars_today = EXCLUDED.total_bars_today,
                        successful_collections = EXCLUDED.successful_collections,
                        failed_collections = EXCLUDED.failed_collections,
                        avg_latency_ms = EXCLUDED.avg_latency_ms,
                        collection_health_score = EXCLUDED.collection_health_score,
                        is_active = EXCLUDED.is_active,
                        last_error_message = EXCLUDED.last_error_message,
                        last_error_at = EXCLUDED.last_error_at,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                await conn.execute(
                    upsert_query,
                    status.vendor, status.symbol, status.last_received_timestamp,
                    status.expected_timestamp, status.data_delay_minutes,
                    status.consecutive_missing_bars, status.total_bars_today,
                    status.successful_collections, status.failed_collections,
                    status.avg_latency_ms, status.collection_health_score,
                    status.is_active, status.last_error_message, status.last_error_at
                )
                
        except Exception as e:
            self.logger.error(f"Failed to store collection status: {e}")
    
    async def detect_gaps(self) -> List[Dict[str, Any]]:
        """Detect data gaps for the current vendor."""
        gaps = []
        
        try:
            async with self.db_pool.acquire() as conn:
                table_name = f"dev_one_minute_live_{self.vendor_name}"
                
                # Find symbols with missing recent data
                gap_query = f"""
                    WITH expected_bars AS (
                        SELECT 
                            generate_series(
                                date_trunc('minute', NOW() - INTERVAL '2 hours'),
                                date_trunc('minute', NOW()),
                                '1 minute'::INTERVAL
                            ) AS expected_timestamp
                    ),
                    symbol_coverage AS (
                        SELECT DISTINCT symbol FROM {table_name}
                        WHERE timestamp >= NOW() - INTERVAL '2 hours'
                    )
                    SELECT 
                        sc.symbol,
                        eb.expected_timestamp,
                        CASE WHEN bars.timestamp IS NULL THEN 'missing' ELSE 'present' END as status
                    FROM symbol_coverage sc
                    CROSS JOIN expected_bars eb
                    LEFT JOIN {table_name} bars ON sc.symbol = bars.symbol 
                        AND bars.timestamp = eb.expected_timestamp
                    WHERE bars.timestamp IS NULL
                        AND EXTRACT(HOUR FROM eb.expected_timestamp AT TIME ZONE 'US/Eastern') BETWEEN 9 AND 16
                        AND EXTRACT(DOW FROM eb.expected_timestamp AT TIME ZONE 'US/Eastern') BETWEEN 1 AND 5
                    ORDER BY sc.symbol, eb.expected_timestamp
                """
                
                gap_rows = await conn.fetch(gap_query)
                
                # Group consecutive missing bars into gaps
                current_gap = None
                for row in gap_rows:
                    symbol = row['symbol']
                    timestamp = row['expected_timestamp']
                    
                    if current_gap and current_gap['symbol'] == symbol:
                        # Extend current gap
                        if timestamp == current_gap['gap_end_timestamp'] + timedelta(minutes=1):
                            current_gap['gap_end_timestamp'] = timestamp
                            current_gap['missing_bars_count'] += 1
                        else:
                            # Gap in the gap - finalize current and start new
                            gaps.append(current_gap)
                            current_gap = {
                                'symbol': symbol,
                                'gap_start_timestamp': timestamp,
                                'gap_end_timestamp': timestamp,
                                'missing_bars_count': 1
                            }
                    else:
                        # Start new gap
                        if current_gap:
                            gaps.append(current_gap)
                        current_gap = {
                            'symbol': symbol,
                            'gap_start_timestamp': timestamp,
                            'gap_end_timestamp': timestamp,
                            'missing_bars_count': 1
                        }
                
                if current_gap:
                    gaps.append(current_gap)
                
                # Store gaps in database
                for gap in gaps:
                    await self.store_detected_gap(gap)
                
                self.logger.info(f"Detected {len(gaps)} gaps for {self.vendor_name}")
                
        except Exception as e:
            self.logger.error(f"Failed to detect gaps: {e}")
        
        return gaps
    
    async def store_detected_gap(self, gap: Dict[str, Any]):
        """Store detected gap in database."""
        try:
            async with self.db_pool.acquire() as conn:
                duration_minutes = int((gap['gap_end_timestamp'] - gap['gap_start_timestamp']).total_seconds() / 60) + 1
                
                # Determine gap severity
                if duration_minutes <= 5:
                    severity = 'low'
                elif duration_minutes <= 15:
                    severity = 'medium'
                elif duration_minutes <= 60:
                    severity = 'high'
                else:
                    severity = 'critical'
                
                insert_query = """
                    INSERT INTO dev_realtime_gaps (
                        vendor, symbol, gap_start_timestamp, gap_end_timestamp,
                        gap_duration_minutes, missing_bars_count, gap_type,
                        detection_method, gap_severity, backfill_status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (vendor, symbol, gap_start_timestamp)
                    DO UPDATE SET
                        gap_end_timestamp = EXCLUDED.gap_end_timestamp,
                        gap_duration_minutes = EXCLUDED.gap_duration_minutes,
                        missing_bars_count = EXCLUDED.missing_bars_count,
                        gap_severity = EXCLUDED.gap_severity,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                await conn.execute(
                    insert_query,
                    self.vendor_name, gap['symbol'], gap['gap_start_timestamp'],
                    gap['gap_end_timestamp'], duration_minutes, gap['missing_bars_count'],
                    'realtime_missing', 'realtime', severity, 'pending'
                )
                
        except Exception as e:
            self.logger.error(f"Failed to store gap: {e}")
    
    @abstractmethod
    async def collect_realtime_data(self) -> AsyncGenerator[MinuteBar, None]:
        """Collect real-time data from vendor. Must be implemented by subclasses."""
        pass
    
    async def start_collection(self):
        """Start real-time data collection."""
        if self.is_running:
            self.logger.warning("Collection is already running")
            return
        
        self.is_running = True
        self.collection_task = asyncio.create_task(self._collection_loop())
        self.logger.info(f"🚀 Started {self.vendor_name.upper()} real-time collection")
    
    async def stop_collection(self):
        """Stop real-time data collection."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.collection_task:
            self.collection_task.cancel()
            try:
                await self.collection_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info(f"🛑 Stopped {self.vendor_name.upper()} real-time collection")
    
    async def _collection_loop(self):
        """Main collection loop."""
        try:
            while self.is_running:
                if self.market_hours.is_market_hours():
                    self.logger.debug("Market is open - collecting data")
                    await self._collect_batch()
                elif self.market_hours.is_extended_hours():
                    self.logger.debug("Extended hours - reduced collection")
                    await self._collect_batch()
                    await asyncio.sleep(300)  # 5 minute intervals in extended hours
                else:
                    self.logger.debug("Market closed - waiting for next open")
                    next_open = self.market_hours.next_market_open()
                    sleep_seconds = (next_open - datetime.now(self.market_hours.eastern)).total_seconds()
                    sleep_seconds = min(sleep_seconds, 3600)  # Max 1 hour sleep
                    await asyncio.sleep(sleep_seconds)
                
                # Regular gap detection during market hours
                if self.market_hours.is_market_hours():
                    await self.detect_gaps()
                
                await asyncio.sleep(60)  # 1 minute base interval
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"Collection loop error: {e}")
            if self.is_running:
                await asyncio.sleep(60)  # Wait before retrying
                asyncio.create_task(self._collection_loop())
    
    async def _collect_batch(self):
        """Collect a batch of data from all symbols."""
        try:
            async for bar in self.collect_realtime_data():
                await self.store_minute_bar(bar)
                
        except Exception as e:
            self.logger.error(f"Batch collection error: {e}")
    
    async def cleanup(self):
        """Cleanup resources."""
        await self.stop_collection()
        
        if self.db_pool:
            await self.db_pool.close()
            
        self.logger.info(f"✅ {self.vendor_name.upper()} collector cleaned up")