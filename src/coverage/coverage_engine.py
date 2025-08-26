"""
Data Coverage Analytics Engine

Provides comprehensive coverage analysis, statistics computation,
and gap detection for massive-scale price data (100M-2B rows).

Key Features:
- Real-time coverage computation and tracking
- Hierarchical aggregation (minute → hour → day → week → month)
- Intelligent gap detection and classification
- Performance-optimized queries for massive datasets
- Vendor comparison and SLA monitoring
"""

import asyncio
import logging
import json
import uuid
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date, time
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, asdict
from enum import Enum
import asyncpg
from decimal import Decimal

# Configure logging
logger = logging.getLogger(__name__)

# =====================================================
# Data Models and Enums
# =====================================================

class CoverageStatus(Enum):
    ACTIVE = "active"
    STALE = "stale"
    MISSING = "missing"
    DEGRADED = "degraded"

class GapSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class GapType(Enum):
    MISSING = "missing"
    PARTIAL = "partial"
    LOW_QUALITY = "low_quality"
    OUTLIER = "outlier"
    MINOR = "minor"
    OFF_HOURS = "off_hours"

class AggregationLevel(Enum):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"

@dataclass
class CoverageInterval:
    """Represents a contiguous period of data coverage"""
    symbol: str
    vendor: str
    data_type: str
    start_time: datetime
    end_time: datetime
    record_count: int
    expected_count: int
    completeness_ratio: float
    avg_quality_score: Optional[float] = None
    has_gaps: bool = False
    gap_count: int = 0
    total_gap_duration_minutes: int = 0

@dataclass
class CoverageStats:
    """Pre-computed coverage statistics for fast queries"""
    symbol: str
    vendor: str
    data_type: str
    aggregation_level: str
    period_start: datetime
    period_end: datetime
    total_expected: int
    total_actual: int
    coverage_percentage: float
    completeness_score: float
    avg_quality_score: Optional[float] = None
    gap_count: int = 0
    total_gap_duration_minutes: int = 0
    first_record_time: Optional[datetime] = None
    last_record_time: Optional[datetime] = None

@dataclass
class CoverageGap:
    """Represents a detected gap in data coverage"""
    symbol: str
    vendor: str
    data_type: str
    gap_start: datetime
    gap_end: datetime
    gap_duration_minutes: int
    expected_records: int
    gap_type: str
    gap_severity: str
    trading_day: date
    is_market_hours: bool
    detection_method: str
    detection_confidence: float
    is_resolved: bool = False

@dataclass
class CoverageQuery:
    """Query specification for coverage analysis"""
    symbols: Optional[List[str]] = None
    vendors: Optional[List[str]] = None
    data_types: Optional[List[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    aggregation_level: Optional[str] = None
    min_coverage_percentage: Optional[float] = None
    include_gaps: bool = True

@dataclass
class CoverageSummary:
    """Real-time coverage summary for dashboards"""
    symbol: str
    vendor: str
    data_type: str
    current_status: str
    coverage_24h: float
    quality_24h: Optional[float]
    gaps_24h: int
    records_24h: int
    coverage_7d: float
    coverage_30d: float
    latest_data_time: Optional[datetime]
    hours_since_update: float
    coverage_trend: str
    quality_trend: str

# =====================================================
# Core Coverage Engine
# =====================================================

class CoverageAnalyticsEngine:
    """
    High-performance coverage analytics engine optimized for massive datasets
    """
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
        
        # Configuration
        self.trading_hours_per_day = 6.5  # US market: 9:30 AM - 4:00 PM EST
        self.trading_days_per_week = 5
        self.trading_days_per_month = 22  # Average
        
        # Market hours (UTC time - EST is UTC-5, EDT is UTC-4)
        self.market_open_utc = time(13, 30)  # 9:30 AM EST
        self.market_close_utc = time(20, 0)   # 4:00 PM EST
        
        # Cache for frequently accessed data
        self._sla_cache = {}
        self._vendor_config_cache = {}
        
    async def initialize(self):
        """Initialize the coverage engine"""
        await self._load_sla_configurations()
        logger.info("✅ Coverage Analytics Engine initialized")
    
    # =====================================================
    # Coverage Computation and Statistics
    # =====================================================
    
    async def compute_coverage_stats(
        self, 
        symbol: str, 
        vendor: str, 
        data_type: str,
        start_time: datetime,
        end_time: datetime,
        aggregation_level: AggregationLevel = AggregationLevel.HOUR
    ) -> CoverageStats:
        """
        Compute coverage statistics for a specific time period
        """
        
        # Calculate expected records based on aggregation level
        time_delta = end_time - start_time
        expected_count = self._calculate_expected_records(
            data_type, aggregation_level, time_delta
        )
        
        # Query actual data
        if data_type == 'minute':
            actual_count, avg_quality, first_time, last_time = await self._query_minute_stats(
                symbol, vendor, start_time, end_time
            )
        else:
            actual_count, avg_quality, first_time, last_time = await self._query_daily_stats(
                symbol, vendor, start_time, end_time
            )
        
        # Calculate coverage metrics
        coverage_percentage = (actual_count / max(expected_count, 1)) * 100.0
        completeness_score = actual_count / max(expected_count, 1)
        
        # Count gaps in the period
        gap_count, total_gap_minutes = await self._count_gaps_in_period(
            symbol, vendor, data_type, start_time, end_time
        )
        
        return CoverageStats(
            symbol=symbol,
            vendor=vendor,
            data_type=data_type,
            aggregation_level=aggregation_level.value,
            period_start=start_time,
            period_end=end_time,
            total_expected=expected_count,
            total_actual=actual_count,
            coverage_percentage=min(coverage_percentage, 100.0),
            completeness_score=min(completeness_score, 1.0),
            avg_quality_score=avg_quality,
            gap_count=gap_count,
            total_gap_duration_minutes=total_gap_minutes,
            first_record_time=first_time,
            last_record_time=last_time
        )
    
    async def compute_hierarchical_aggregations(
        self,
        symbol: str,
        vendor: str,
        data_type: str,
        base_date: date
    ) -> List[CoverageStats]:
        """
        Compute coverage statistics at multiple aggregation levels
        """
        
        aggregations = []
        base_datetime = datetime.combine(base_date, time.min)
        
        # Define aggregation periods
        periods = [
            (AggregationLevel.HOUR, base_datetime, base_datetime + timedelta(hours=1)),
            (AggregationLevel.DAY, base_datetime, base_datetime + timedelta(days=1)),
            (AggregationLevel.WEEK, base_datetime - timedelta(days=base_datetime.weekday()), 
             base_datetime - timedelta(days=base_datetime.weekday()) + timedelta(weeks=1)),
            (AggregationLevel.MONTH, base_datetime.replace(day=1),
             (base_datetime.replace(day=1) + timedelta(days=32)).replace(day=1))
        ]
        
        for level, start_time, end_time in periods:
            stats = await self.compute_coverage_stats(
                symbol, vendor, data_type, start_time, end_time, level
            )
            aggregations.append(stats)
        
        return aggregations
    
    async def update_coverage_stats_incremental(
        self,
        symbol: str,
        vendor: str,
        data_type: str,
        affected_timestamps: List[datetime]
    ):
        """
        Update coverage statistics incrementally for affected time periods
        """
        
        if not affected_timestamps:
            return
        
        # Group timestamps by aggregation periods
        affected_periods = self._calculate_affected_periods(affected_timestamps)
        
        # Update each affected period
        for level, periods in affected_periods.items():
            for period_start in periods:
                await self._update_single_period_stats(
                    symbol, vendor, data_type, level, period_start
                )
    
    # =====================================================
    # Gap Detection and Analysis
    # =====================================================
    
    async def detect_gaps_realtime(
        self,
        symbol: str,
        vendor: str,
        data_type: str,
        new_timestamp: datetime
    ) -> List[CoverageGap]:
        """
        Detect gaps in real-time as new data arrives
        """
        
        gaps = []
        
        # Get the previous timestamp
        previous_timestamp = await self._get_previous_timestamp(
            symbol, vendor, data_type, new_timestamp
        )
        
        if previous_timestamp:
            gap = self._analyze_potential_gap(
                symbol, vendor, data_type, previous_timestamp, new_timestamp
            )
            if gap:
                gaps.append(gap)
        
        return gaps
    
    async def detect_gaps_batch(
        self,
        symbol: str,
        vendor: str,
        data_type: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[CoverageGap]:
        """
        Batch gap detection for historical data analysis
        """
        
        gaps = []
        
        # Get all timestamps in the range
        timestamps = await self._get_timestamps_in_range(
            symbol, vendor, data_type, start_time, end_time
        )
        
        if len(timestamps) < 2:
            return gaps
        
        # Analyze gaps between consecutive timestamps
        for i in range(len(timestamps) - 1):
            current_time = timestamps[i]
            next_time = timestamps[i + 1]
            
            gap = self._analyze_potential_gap(
                symbol, vendor, data_type, current_time, next_time
            )
            if gap:
                gaps.append(gap)
        
        return gaps
    
    async def heal_gaps_from_backfill(
        self,
        symbol: str,
        vendor: str,
        data_type: str,
        backfill_timestamps: List[datetime]
    ) -> int:
        """
        Mark gaps as healed when backfill data arrives
        """
        
        healed_count = 0
        
        async with self.db_pool.acquire() as conn:
            for timestamp in backfill_timestamps:
                # Find gaps that this timestamp might heal
                healed_gaps = await conn.fetch("""
                    UPDATE coverage_gaps
                    SET is_resolved = TRUE,
                        resolution_method = 'backfill_healing',
                        resolved_at = NOW(),
                        resolution_notes = 'Gap healed by backfill data'
                    WHERE symbol = $1 AND vendor = $2 AND data_type = $3
                        AND gap_start <= $4 AND gap_end >= $4
                        AND is_resolved = FALSE
                    RETURNING gap_id
                """, symbol, vendor, data_type, timestamp)
                
                healed_count += len(healed_gaps)
                
                if healed_gaps:
                    logger.info(f"Healed {len(healed_gaps)} gaps for {symbol}/{vendor} at {timestamp}")
        
        return healed_count
    
    # =====================================================
    # Coverage Queries and Analysis
    # =====================================================
    
    async def query_coverage_summary(
        self,
        query: CoverageQuery
    ) -> List[CoverageSummary]:
        """
        Query coverage summary with filtering
        """
        
        where_conditions = []
        params = []
        param_count = 0
        
        # Build WHERE clause
        if query.symbols:
            param_count += 1
            where_conditions.append(f"symbol = ANY(${param_count})")
            params.append(query.symbols)
        
        if query.vendors:
            param_count += 1
            where_conditions.append(f"vendor = ANY(${param_count})")
            params.append(query.vendors)
        
        if query.data_types:
            param_count += 1
            where_conditions.append(f"data_type = ANY(${param_count})")
            params.append(query.data_types)
        
        if query.min_coverage_percentage:
            param_count += 1
            where_conditions.append(f"coverage_24h >= ${param_count}")
            params.append(query.min_coverage_percentage)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch(f"""
                SELECT 
                    symbol, vendor, data_type, current_status,
                    coverage_24h, quality_24h, records_24h,
                    coverage_7d, coverage_30d,
                    latest_data_time, hours_since_update,
                    coverage_trend, quality_trend,
                    COALESCE((
                        SELECT COUNT(*) FROM coverage_gaps g
                        WHERE g.symbol = cs.symbol 
                            AND g.vendor = cs.vendor 
                            AND g.data_type = cs.data_type
                            AND g.gap_start >= NOW() - INTERVAL '24 hours'
                            AND g.is_resolved = FALSE
                    ), 0) as gaps_24h
                FROM coverage_summary cs
                {where_clause}
                ORDER BY symbol, vendor, data_type
            """, *params)
            
            return [CoverageSummary(**dict(record)) for record in records]
    
    async def get_vendor_comparison(
        self,
        symbol: str,
        data_type: str = 'minute',
        time_period: str = '24h'
    ) -> Dict[str, Any]:
        """
        Compare coverage across vendors for a specific symbol
        """
        
        time_filter = {
            '24h': 'coverage_24h',
            '7d': 'coverage_7d',
            '30d': 'coverage_30d'
        }.get(time_period, 'coverage_24h')
        
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch(f"""
                SELECT 
                    vendor,
                    {time_filter} as coverage_percentage,
                    quality_{time_period.replace('h', 'h').replace('d', 'd')} as quality_score,
                    current_status,
                    latest_data_time,
                    hours_since_update
                FROM coverage_summary
                WHERE symbol = $1 AND data_type = $2
                ORDER BY {time_filter} DESC
            """, symbol, data_type)
            
            vendors = []
            for record in records:
                vendors.append({
                    'vendor': record['vendor'],
                    'coverage_percentage': float(record['coverage_percentage'] or 0),
                    'quality_score': float(record['quality_score'] or 0),
                    'status': record['current_status'],
                    'latest_data_time': record['latest_data_time'],
                    'hours_since_update': float(record['hours_since_update'] or 0)
                })
            
            # Calculate comparison metrics
            if vendors:
                best_vendor = vendors[0]
                worst_vendor = vendors[-1]
                avg_coverage = np.mean([v['coverage_percentage'] for v in vendors])
                coverage_variance = np.var([v['coverage_percentage'] for v in vendors])
            else:
                best_vendor = worst_vendor = None
                avg_coverage = coverage_variance = 0
            
            return {
                'symbol': symbol,
                'data_type': data_type,
                'time_period': time_period,
                'vendors': vendors,
                'best_vendor': best_vendor,
                'worst_vendor': worst_vendor,
                'average_coverage': avg_coverage,
                'coverage_variance': coverage_variance,
                'vendor_count': len(vendors)
            }
    
    async def get_coverage_trends(
        self,
        symbol: str,
        vendor: str,
        data_type: str,
        days_back: int = 30
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get coverage trends over time for trend analysis
        """
        
        start_date = datetime.now() - timedelta(days=days_back)
        
        async with self.db_pool.acquire() as conn:
            # Get daily coverage trends
            daily_trends = await conn.fetch("""
                SELECT 
                    period_start::DATE as date,
                    coverage_percentage,
                    avg_quality_score,
                    gap_count,
                    total_gap_duration_minutes
                FROM coverage_stats
                WHERE symbol = $1 AND vendor = $2 AND data_type = $3
                    AND aggregation_level = 'day'
                    AND period_start >= $4
                ORDER BY period_start
            """, symbol, vendor, data_type, start_date)
            
            # Get hourly trends for recent data
            hourly_trends = await conn.fetch("""
                SELECT 
                    period_start,
                    coverage_percentage,
                    avg_quality_score,
                    gap_count
                FROM coverage_stats
                WHERE symbol = $1 AND vendor = $2 AND data_type = $3
                    AND aggregation_level = 'hour'
                    AND period_start >= NOW() - INTERVAL '7 days'
                ORDER BY period_start
            """, symbol, vendor, data_type)
            
            return {
                'daily_trends': [dict(record) for record in daily_trends],
                'hourly_trends': [dict(record) for record in hourly_trends],
                'symbol': symbol,
                'vendor': vendor,
                'data_type': data_type,
                'period_days': days_back
            }
    
    # =====================================================
    # SLA Monitoring and Alerting
    # =====================================================
    
    async def check_sla_compliance(
        self,
        symbol: str = None,
        vendor: str = None
    ) -> List[Dict[str, Any]]:
        """
        Check SLA compliance across symbols and vendors
        """
        
        where_conditions = []
        params = []
        param_count = 0
        
        if symbol:
            param_count += 1
            where_conditions.append(f"cs.symbol = ${param_count}")
            params.append(symbol)
        
        if vendor:
            param_count += 1
            where_conditions.append(f"cs.vendor = ${param_count}")
            params.append(vendor)
        
        where_clause = "AND " + " AND ".join(where_conditions) if where_conditions else ""
        
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch(f"""
                SELECT 
                    cs.symbol,
                    cs.vendor,
                    cs.data_type,
                    cs.coverage_24h,
                    cs.quality_24h,
                    sla.min_coverage_percentage,
                    sla.warning_threshold,
                    sla.critical_threshold,
                    CASE 
                        WHEN cs.coverage_24h >= sla.min_coverage_percentage THEN 'compliant'
                        WHEN cs.coverage_24h >= sla.warning_threshold THEN 'warning'
                        WHEN cs.coverage_24h >= sla.critical_threshold THEN 'critical'
                        ELSE 'violation'
                    END as compliance_status,
                    cs.coverage_24h - sla.min_coverage_percentage as coverage_gap
                FROM coverage_summary cs
                LEFT JOIN coverage_sla sla ON 
                    sla.vendor = cs.vendor 
                    AND sla.data_type = cs.data_type 
                    AND (sla.symbol = cs.symbol OR sla.symbol IS NULL)
                WHERE sla.sla_id IS NOT NULL
                {where_clause}
                ORDER BY compliance_status DESC, coverage_gap ASC
            """, *params)
            
            compliance_results = []
            for record in records:
                compliance_results.append({
                    'symbol': record['symbol'],
                    'vendor': record['vendor'],
                    'data_type': record['data_type'],
                    'current_coverage': float(record['coverage_24h'] or 0),
                    'required_coverage': float(record['min_coverage_percentage']),
                    'compliance_status': record['compliance_status'],
                    'coverage_gap': float(record['coverage_gap'] or 0),
                    'quality_score': float(record['quality_24h'] or 0)
                })
            
            return compliance_results
    
    # =====================================================
    # Helper Methods
    # =====================================================
    
    def _calculate_expected_records(
        self,
        data_type: str,
        aggregation_level: AggregationLevel,
        time_delta: timedelta
    ) -> int:
        """Calculate expected number of records for a time period"""
        
        if data_type == 'minute':
            if aggregation_level == AggregationLevel.HOUR:
                return 60
            elif aggregation_level == AggregationLevel.DAY:
                return int(self.trading_hours_per_day * 60)
            elif aggregation_level == AggregationLevel.WEEK:
                return int(self.trading_hours_per_day * 60 * self.trading_days_per_week)
            elif aggregation_level == AggregationLevel.MONTH:
                return int(self.trading_hours_per_day * 60 * self.trading_days_per_month)
        else:  # daily data
            if aggregation_level == AggregationLevel.DAY:
                return 1
            elif aggregation_level == AggregationLevel.WEEK:
                return self.trading_days_per_week
            elif aggregation_level == AggregationLevel.MONTH:
                return self.trading_days_per_month
        
        # Fallback calculation based on time delta
        minutes = time_delta.total_seconds() / 60
        if data_type == 'minute':
            return int(minutes)
        else:
            return max(1, int(time_delta.days))
    
    async def _query_minute_stats(
        self,
        symbol: str,
        vendor: str,
        start_time: datetime,
        end_time: datetime
    ) -> Tuple[int, Optional[float], Optional[datetime], Optional[datetime]]:
        """Query minute bars statistics"""
        
        async with self.db_pool.acquire() as conn:
            result = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as count,
                    AVG(quality_score) as avg_quality,
                    MIN(timestamp) as first_time,
                    MAX(timestamp) as last_time
                FROM minute_bars
                WHERE symbol = $1 AND vendor = $2
                    AND timestamp >= $3 AND timestamp < $4
            """, symbol, vendor, start_time, end_time)
            
            return (
                result['count'],
                float(result['avg_quality']) if result['avg_quality'] else None,
                result['first_time'],
                result['last_time']
            )
    
    async def _query_daily_stats(
        self,
        symbol: str,
        vendor: str,
        start_time: datetime,
        end_time: datetime
    ) -> Tuple[int, Optional[float], Optional[datetime], Optional[datetime]]:
        """Query daily prices statistics"""
        
        async with self.db_pool.acquire() as conn:
            result = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as count,
                    NULL as avg_quality,
                    MIN(date)::TIMESTAMPTZ as first_time,
                    MAX(date)::TIMESTAMPTZ as last_time
                FROM daily_prices
                WHERE symbol = $1 AND source = $2
                    AND date >= $3::DATE AND date < $4::DATE
            """, symbol, vendor, start_time, end_time)
            
            return (
                result['count'],
                None,  # No quality score for daily prices yet
                result['first_time'],
                result['last_time']
            )
    
    async def _load_sla_configurations(self):
        """Load SLA configurations into cache"""
        
        async with self.db_pool.acquire() as conn:
            records = await conn.fetch("SELECT * FROM coverage_sla")
            
            for record in records:
                key = (record['symbol'], record['vendor'], record['data_type'])
                self._sla_cache[key] = dict(record)
        
        logger.info(f"Loaded {len(self._sla_cache)} SLA configurations")
    
    def _calculate_affected_periods(self, timestamps: List[datetime]) -> Dict[str, Set[datetime]]:
        """Calculate which aggregation periods are affected by timestamp changes"""
        
        affected_periods = {
            'hour': set(),
            'day': set(),
            'week': set(),
            'month': set()
        }
        
        for timestamp in timestamps:
            affected_periods['hour'].add(timestamp.replace(minute=0, second=0, microsecond=0))
            affected_periods['day'].add(timestamp.replace(hour=0, minute=0, second=0, microsecond=0))
            
            # Week starts on Monday
            week_start = timestamp - timedelta(days=timestamp.weekday())
            affected_periods['week'].add(week_start.replace(hour=0, minute=0, second=0, microsecond=0))
            
            # Month start
            affected_periods['month'].add(timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0))
        
        return affected_periods
    
    async def _count_gaps_in_period(
        self, symbol: str, vendor: str, data_type: str, 
        start_time: datetime, end_time: datetime
    ) -> Tuple[int, int]:
        """Count gaps and total gap duration in a time period"""
        async with self.db_pool.acquire() as conn:
            result = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as gap_count,
                    COALESCE(SUM(gap_duration_minutes), 0) as total_gap_minutes
                FROM coverage_gaps
                WHERE symbol = $1 AND vendor = $2 AND data_type = $3
                    AND gap_start >= $4 AND gap_end <= $5
            """, symbol, vendor, data_type, start_time, end_time)
            
            return result['gap_count'], int(result['total_gap_minutes'])
    
    async def _get_previous_timestamp(
        self, symbol: str, vendor: str, data_type: str, current_time: datetime
    ) -> Optional[datetime]:
        """Get the previous timestamp for gap detection"""
        table_name = f"{data_type}_bars" if data_type == 'minute' else 'daily_prices'
        
        async with self.db_pool.acquire() as conn:
            result = await conn.fetchrow(f"""
                SELECT timestamp FROM {table_name}
                WHERE symbol = $1 AND vendor = $2 AND timestamp < $3
                ORDER BY timestamp DESC
                LIMIT 1
            """, symbol, vendor, current_time)
            
            return result['timestamp'] if result else None
    
    async def _get_timestamps_in_range(
        self, symbol: str, vendor: str, data_type: str,
        start_time: datetime, end_time: datetime
    ) -> List[datetime]:
        """Get all timestamps in a range for batch gap detection"""
        table_name = f"{data_type}_bars" if data_type == 'minute' else 'daily_prices'
        
        async with self.db_pool.acquire() as conn:
            results = await conn.fetch(f"""
                SELECT timestamp FROM {table_name}
                WHERE symbol = $1 AND vendor = $2 
                    AND timestamp >= $3 AND timestamp <= $4
                ORDER BY timestamp
            """, symbol, vendor, start_time, end_time)
            
            return [row['timestamp'] for row in results]
    
    def _analyze_potential_gap(
        self, symbol: str, vendor: str, data_type: str,
        gap_start: datetime, gap_end: datetime, detection_method: str = 'realtime'
    ) -> CoverageGap:
        """Analyze a potential gap and classify it"""
        gap_duration = int((gap_end - gap_start).total_seconds() / 60)
        expected_records = gap_duration if data_type == 'minute' else 1
        
        # Determine severity based on duration
        if gap_duration <= 2:
            severity = GapSeverity.LOW
        elif gap_duration <= 10:
            severity = GapSeverity.MEDIUM
        elif gap_duration <= 30:
            severity = GapSeverity.HIGH
        else:
            severity = GapSeverity.CRITICAL
        
        return CoverageGap(
            symbol=symbol,
            vendor=vendor,
            data_type=data_type,
            gap_start=gap_start,
            gap_end=gap_end,
            gap_duration_minutes=gap_duration,
            expected_records=expected_records,
            gap_type=GapType.MISSING,
            gap_severity=severity,
            trading_day=gap_start.date(),
            is_market_hours=True,  # Simplified
            detection_method=detection_method,
            detection_confidence=0.95
        )

    # =====================================================
    # Quality Scoring and Multi-Vendor Reconciliation
    # =====================================================
    
    async def compute_quality_scores(self, symbols: List[str], date_range: Tuple[date, date]) -> Dict[str, Dict[str, float]]:
        """
        Compute quality scores for symbols across all vendors
        
        Returns:
            {symbol: {vendor: quality_score}} where quality_score is 0-100
        """
        start_date, end_date = date_range
        quality_scores = {}
        
        for symbol in symbols:
            quality_scores[symbol] = {}
            
            # Get data from all vendors for this symbol
            vendor_data = await self._get_multi_vendor_data(symbol, start_date, end_date)
            
            for vendor, data in vendor_data.items():
                quality_score = await self._calculate_vendor_quality(vendor, data, symbol, start_date, end_date)
                quality_scores[symbol][vendor] = quality_score
                
        return quality_scores
    
    async def _get_multi_vendor_data(self, symbol: str, start_date: date, end_date: date) -> Dict[str, List[Dict]]:
        """Get price data for a symbol from all vendors"""
        vendors = ['polygon', 'tiingo', 'eodhd']
        vendor_data = {}
        
        async with self.db_pool.acquire() as conn:
            for vendor in vendors:
                table_name = f"dev_{vendor}_prices"
                try:
                    results = await conn.fetch(f"""
                        SELECT price_date, open_price, high_price, low_price, close_price, volume
                        FROM {table_name}
                        WHERE symbol = $1 AND price_date >= $2 AND price_date <= $3
                        ORDER BY price_date
                    """, symbol, start_date, end_date)
                    
                    vendor_data[vendor] = [dict(row) for row in results]
                except Exception as e:
                    logger.warning(f"Failed to fetch {vendor} data for {symbol}: {e}")
                    vendor_data[vendor] = []
        
        return vendor_data
    
    async def _calculate_vendor_quality(self, vendor: str, data: List[Dict], symbol: str, start_date: date, end_date: date) -> float:
        """Calculate quality score (0-100) for a vendor's data"""
        if not data:
            return 0.0
            
        total_score = 0.0
        factors = []
        
        # Factor 1: Completeness (40% weight)
        expected_days = (end_date - start_date).days + 1
        actual_days = len(data)
        completeness = min(actual_days / expected_days, 1.0) if expected_days > 0 else 0.0
        factors.append(('completeness', completeness, 0.4))
        
        # Factor 2: Data Integrity (30% weight) 
        integrity_score = self._assess_data_integrity(data)
        factors.append(('integrity', integrity_score, 0.3))
        
        # Factor 3: Freshness (20% weight)
        freshness_score = self._assess_data_freshness(data, end_date)
        factors.append(('freshness', freshness_score, 0.2))
        
        # Factor 4: Consistency (10% weight)
        consistency_score = self._assess_price_consistency(data)
        factors.append(('consistency', consistency_score, 0.1))
        
        # Calculate weighted score
        for factor_name, score, weight in factors:
            total_score += score * weight
        
        return round(total_score * 100, 2)  # Convert to 0-100 scale
    
    def _assess_data_integrity(self, data: List[Dict]) -> float:
        """Assess data integrity (missing values, invalid prices, etc.)"""
        if not data:
            return 0.0
        
        valid_records = 0
        total_records = len(data)
        
        for record in data:
            # Check for required fields and valid values
            required_fields = ['open_price', 'high_price', 'low_price', 'close_price']
            has_all_fields = all(record.get(field) is not None for field in required_fields)
            
            if has_all_fields:
                # Check price validity (positive values, high >= low, etc.)
                prices = [float(record[field]) for field in required_fields]
                if all(p > 0 for p in prices) and prices[1] >= prices[2]:  # high >= low
                    valid_records += 1
        
        return valid_records / total_records if total_records > 0 else 0.0
    
    def _assess_data_freshness(self, data: List[Dict], end_date: date) -> float:
        """Assess how fresh the data is relative to end_date"""
        if not data:
            return 0.0
            
        # Get the latest date in the data
        latest_date = max(record['price_date'] for record in data)
        days_old = (end_date - latest_date).days
        
        # Score based on recency (100% if same day, declining over time)
        if days_old <= 1:
            return 1.0
        elif days_old <= 3:
            return 0.8
        elif days_old <= 7:
            return 0.6
        elif days_old <= 30:
            return 0.4
        else:
            return 0.2
    
    def _assess_price_consistency(self, data: List[Dict]) -> float:
        """Assess consistency of price data (no extreme outliers)"""
        if len(data) < 2:
            return 1.0
            
        # Calculate daily returns to detect outliers
        returns = []
        for i in range(1, len(data)):
            prev_close = float(data[i-1]['close_price'])
            curr_close = float(data[i]['close_price'])
            if prev_close > 0:
                daily_return = abs(curr_close - prev_close) / prev_close
                returns.append(daily_return)
        
        if not returns:
            return 1.0
            
        # Flag extreme moves (>50% daily change as potential data errors)
        extreme_moves = sum(1 for r in returns if r > 0.5)
        consistency_score = 1.0 - (extreme_moves / len(returns))
        
        return max(consistency_score, 0.0)
    
    async def multi_vendor_reconciliation(self, symbols: List[str], date_range: Tuple[date, date]) -> Dict[str, Dict]:
        """
        Perform multi-vendor reconciliation analysis
        
        Returns detailed comparison and recommended data sources
        """
        start_date, end_date = date_range
        reconciliation_results = {}
        
        for symbol in symbols:
            # Get vendor data and quality scores
            vendor_data = await self._get_multi_vendor_data(symbol, start_date, end_date)
            quality_scores = {}
            
            for vendor, data in vendor_data.items():
                if data:  # Only score vendors with data
                    quality_scores[vendor] = await self._calculate_vendor_quality(vendor, data, symbol, start_date, end_date)
            
            # Perform reconciliation analysis
            reconciliation_results[symbol] = {
                'vendor_coverage': {vendor: len(data) for vendor, data in vendor_data.items()},
                'quality_scores': quality_scores,
                'recommended_primary': self._select_primary_vendor(quality_scores, vendor_data),
                'data_conflicts': await self._detect_price_conflicts(vendor_data),
                'coverage_gaps': self._identify_coverage_gaps(vendor_data, start_date, end_date)
            }
        
        return reconciliation_results
    
    def _select_primary_vendor(self, quality_scores: Dict[str, float], vendor_data: Dict[str, List]) -> str:
        """Select the best primary vendor based on quality scores and coverage"""
        if not quality_scores:
            return None
            
        # Weight by both quality score and data coverage
        vendor_scores = {}
        for vendor, quality in quality_scores.items():
            coverage = len(vendor_data.get(vendor, []))
            # Combined score: 70% quality, 30% coverage
            vendor_scores[vendor] = (quality * 0.7) + (min(coverage/252, 1.0) * 30 * 0.3)  # Assume 252 trading days/year
        
        return max(vendor_scores.items(), key=lambda x: x[1])[0] if vendor_scores else None
    
    async def _detect_price_conflicts(self, vendor_data: Dict[str, List]) -> List[Dict]:
        """Detect significant price differences between vendors on same dates"""
        conflicts = []
        
        # Group by date across vendors
        date_data = {}
        for vendor, data in vendor_data.items():
            for record in data:
                date_key = record['price_date']
                if date_key not in date_data:
                    date_data[date_key] = {}
                date_data[date_key][vendor] = record
        
        # Check for conflicts on shared dates
        for date_key, vendor_records in date_data.items():
            if len(vendor_records) > 1:
                # Compare close prices across vendors
                vendors = list(vendor_records.keys())
                prices = [float(vendor_records[v]['close_price']) for v in vendors]
                
                # Calculate coefficient of variation
                if len(prices) > 1:
                    mean_price = np.mean(prices)
                    std_price = np.std(prices)
                    cv = std_price / mean_price if mean_price > 0 else 0
                    
                    # Flag as conflict if CV > 2% (significant disagreement)
                    if cv > 0.02:
                        conflicts.append({
                            'date': date_key,
                            'vendors': vendors,
                            'prices': prices,
                            'coefficient_of_variation': cv,
                            'max_deviation_pct': (max(prices) - min(prices)) / mean_price * 100
                        })
        
        return conflicts
    
    def _identify_coverage_gaps(self, vendor_data: Dict[str, List], start_date: date, end_date: date) -> Dict[str, List]:
        """Identify date ranges where each vendor has missing data"""
        gaps = {}
        
        # Create complete date range
        current_date = start_date
        expected_dates = set()
        while current_date <= end_date:
            # Skip weekends (simplified - doesn't account for holidays)
            if current_date.weekday() < 5:
                expected_dates.add(current_date)
            current_date += timedelta(days=1)
        
        for vendor, data in vendor_data.items():
            vendor_dates = set(record['price_date'] for record in data)
            missing_dates = expected_dates - vendor_dates
            
            if missing_dates:
                gaps[vendor] = sorted(list(missing_dates))
        
        return gaps

    # =====================================================
    # Performance Monitoring and Storage Metrics
    # =====================================================
    
    async def get_performance_metrics(self, time_window_hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive performance and storage metrics"""
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        
        async with self.db_pool.acquire() as conn:
            # Database size and table statistics
            db_stats = await conn.fetchrow("""
                SELECT 
                    pg_database_size(current_database()) as total_db_size_bytes,
                    (SELECT COUNT(*) FROM pg_stat_user_tables) as total_tables
            """)
            
            # Price table statistics
            price_table_stats = await conn.fetch("""
                SELECT 
                    schemaname,
                    tablename,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes,
                    n_live_tup as live_rows,
                    n_dead_tup as dead_rows
                FROM pg_stat_user_tables 
                WHERE tablename LIKE '%price%'
                ORDER BY n_live_tup DESC
            """)
            
            # Query performance stats (if available)
            query_stats = await conn.fetch("""
                SELECT 
                    query,
                    calls,
                    total_exec_time,
                    mean_exec_time,
                    rows
                FROM pg_stat_statements 
                WHERE query LIKE '%price%'
                ORDER BY total_exec_time DESC
                LIMIT 10
            """) if await self._check_pg_stat_statements_available(conn) else []
            
            # Storage metrics by vendor
            vendor_storage = await conn.fetch("""
                SELECT 
                    'dev_polygon_prices' as table_name,
                    COUNT(*) as row_count,
                    pg_total_relation_size('dev_polygon_prices') as size_bytes
                FROM dev_polygon_prices
                UNION ALL
                SELECT 
                    'dev_tiingo_prices' as table_name,
                    COUNT(*) as row_count,
                    pg_total_relation_size('dev_tiingo_prices') as size_bytes
                FROM dev_tiingo_prices
                UNION ALL
                SELECT 
                    'dev_eodhd_prices' as table_name,
                    COUNT(*) as row_count,
                    pg_total_relation_size('dev_eodhd_prices') as size_bytes
                FROM dev_eodhd_prices
            """)
        
        # Format results
        total_price_records = sum(row['row_count'] for row in vendor_storage)
        total_price_storage_mb = sum(row['size_bytes'] for row in vendor_storage) / (1024 * 1024)
        
        performance_metrics = {
            'timestamp': datetime.now().isoformat(),
            'time_window_hours': time_window_hours,
            'database': {
                'total_size_mb': db_stats['total_db_size_bytes'] / (1024 * 1024),
                'total_tables': db_stats['total_tables']
            },
            'price_data': {
                'total_records': total_price_records,
                'total_storage_mb': total_price_storage_mb,
                'records_per_mb': total_price_records / total_price_storage_mb if total_price_storage_mb > 0 else 0,
                'vendor_breakdown': {
                    row['table_name']: {
                        'records': row['row_count'],
                        'size_mb': row['size_bytes'] / (1024 * 1024),
                        'avg_bytes_per_record': row['size_bytes'] / row['row_count'] if row['row_count'] > 0 else 0
                    }
                    for row in vendor_storage
                }
            },
            'table_activity': [
                {
                    'table': row['tablename'],
                    'live_rows': row['live_rows'],
                    'dead_rows': row['dead_rows'],
                    'recent_inserts': row['inserts'],
                    'recent_updates': row['updates']
                }
                for row in price_table_stats
            ],
            'query_performance': [
                {
                    'query_snippet': row['query'][:100] + '...' if len(row['query']) > 100 else row['query'],
                    'total_calls': row['calls'],
                    'avg_execution_time_ms': float(row['mean_exec_time']),
                    'total_execution_time_ms': float(row['total_exec_time'])
                }
                for row in query_stats
            ] if query_stats else [],
            'performance_summary': {
                'storage_efficiency_score': self._calculate_storage_efficiency(total_price_records, total_price_storage_mb),
                'data_growth_rate': await self._estimate_data_growth_rate(conn),
                'projected_30day_size_mb': total_price_storage_mb * 1.1  # Simple 10% growth assumption
            }
        }
        
        return performance_metrics
    
    async def _check_pg_stat_statements_available(self, conn) -> bool:
        """Check if pg_stat_statements extension is available"""
        try:
            result = await conn.fetchval("SELECT 1 FROM pg_stat_statements LIMIT 1")
            return True
        except:
            return False
    
    def _calculate_storage_efficiency(self, total_records: int, total_mb: float) -> float:
        """Calculate storage efficiency score (0-100)"""
        if total_mb <= 0 or total_records <= 0:
            return 0.0
            
        # Benchmark: ~100 bytes per price record is efficient
        bytes_per_record = (total_mb * 1024 * 1024) / total_records
        efficiency_score = min(100 / bytes_per_record * 100, 100.0) if bytes_per_record > 0 else 0.0
        
        return round(efficiency_score, 2)
    
    async def _estimate_data_growth_rate(self, conn) -> float:
        """Estimate daily data growth rate based on recent activity"""
        try:
            # Get data from last 7 days to estimate growth
            result = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as recent_records
                FROM (
                    SELECT price_date FROM dev_polygon_prices WHERE price_date > CURRENT_DATE - INTERVAL '7 days'
                    UNION ALL
                    SELECT price_date FROM dev_tiingo_prices WHERE price_date > CURRENT_DATE - INTERVAL '7 days'  
                    UNION ALL
                    SELECT price_date FROM dev_eodhd_prices WHERE price_date > CURRENT_DATE - INTERVAL '7 days'
                ) recent_data
            """)
            
            daily_growth = result['recent_records'] / 7.0 if result else 0.0
            return round(daily_growth, 2)
            
        except Exception as e:
            logger.warning(f"Failed to estimate growth rate: {e}")
            return 0.0