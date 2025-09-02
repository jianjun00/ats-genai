#!/usr/bin/env python3
"""
Universe Analytics Service

Provides analytics and visualization capabilities for universe membership,
qualification metrics, and historical composition analysis.
"""

import asyncio
import asyncpg
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import json

from config.environment import Environment


@dataclass
class UniverseMembershipRecord:
    """Individual universe membership record"""
    universe_id: int
    symbol: str
    start_at: date
    end_at: Optional[date]
    active: bool
    removal_reason: Optional[str] = None


@dataclass
class UniverseMetrics:
    """Universe-level metrics and statistics"""
    universe_id: int
    universe_name: str
    total_members: int
    active_members: int
    avg_market_cap: Optional[float]
    total_market_cap: Optional[float]
    avg_dollar_volume: Optional[float]
    total_dollar_volume: Optional[float]
    warning_count: int
    grace_period_count: int
    calculation_date: date


@dataclass
class QualificationDistribution:
    """Distribution of qualification metrics"""
    market_cap_buckets: Dict[str, int]
    volume_buckets: Dict[str, int]
    sector_distribution: Dict[str, int]
    qualification_scatter: List[Dict[str, Any]]


@dataclass
class UniverseTimeSeriesData:
    """Historical universe size and composition"""
    dates: List[str]
    member_counts: List[int]
    market_cap_totals: List[float]
    volume_totals: List[float]
    entry_counts: List[int]
    exit_counts: List[int]


class UniverseAnalyticsService:
    """
    Service for universe analytics and visualization data.
    
    Provides methods to query universe membership, qualification metrics,
    historical composition, and warning indicators for dashboard display.
    """
    
    def __init__(self, env: Environment):
        self.env = env
        self.logger = logging.getLogger(__name__)
        self.db_pool = None
    
    async def initialize(self):
        """Initialize database connections"""
        try:
            db_config = self.env.get_database_config()
            
            # Filter to asyncpg-compatible parameters
            asyncpg_compatible_keys = {'host', 'port', 'user', 'password', 'database'}
            asyncpg_config = {
                k: v for k, v in db_config.items() 
                if k in asyncpg_compatible_keys and v is not None
            }
            
            self.db_pool = await asyncpg.create_pool(**asyncpg_config, min_size=1, max_size=10)
            self.logger.info("Universe analytics service initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize universe analytics service: {e}")
            raise
    
    async def close(self):
        """Close database connections"""
        if self.db_pool:
            await self.db_pool.close()
    
    async def get_all_universes(self) -> List[Dict[str, Any]]:
        """Get list of all available universes"""
        async with self.db_pool.acquire() as conn:
            query = f"""
            SELECT u.id, u.name, u.description, u.created_at,
                   COUNT(um.symbol) as total_members,
                   COUNT(CASE WHEN um.end_at IS NULL THEN 1 END) as active_members,
                   MIN(um.start_at) as first_entry,
                   MAX(COALESCE(um.end_at, um.start_at)) as last_activity
            FROM {self.env.get_table_name('universe')} u
            LEFT JOIN {self.env.get_table_name('universe_membership')} um ON u.id = um.universe_id
            GROUP BY u.id, u.name, u.description, u.created_at
            ORDER BY u.created_at DESC
            """
            
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
    
    async def get_universe_metrics(self, universe_id: int, as_of_date: date) -> UniverseMetrics:
        """Get comprehensive metrics for a universe as of a specific date"""
        async with self.db_pool.acquire() as conn:
            # Get basic universe info
            universe_query = f"""
            SELECT id, name FROM {self.env.get_table_name('universe')} WHERE id = $1
            """
            universe_row = await conn.fetchrow(universe_query, universe_id)
            
            if not universe_row:
                raise ValueError(f"Universe {universe_id} not found")
            
            # Get membership metrics
            membership_query = f"""
            SELECT 
                COUNT(*) as total_members,
                COUNT(CASE WHEN (start_at <= $2 AND (end_at IS NULL OR end_at > $2)) THEN 1 END) as active_members
            FROM {self.env.get_table_name('universe_membership')} 
            WHERE universe_id = $1
            """
            membership_row = await conn.fetchrow(membership_query, universe_id, as_of_date)
            
            # Get financial metrics from universe tracking (if available)
            tracking_query = f"""
            SELECT 
                AVG(avg_market_cap) as avg_market_cap,
                SUM(avg_market_cap) as total_market_cap,
                AVG(avg_dollar_volume) as avg_dollar_volume,
                SUM(avg_dollar_volume) as total_dollar_volume
            FROM {self.env.get_table_name('universe_tracking')} 
            WHERE universe_name LIKE (SELECT name FROM {self.env.get_table_name('universe')} WHERE id = $1)
              AND last_update = $2
            """
            
            try:
                tracking_row = await conn.fetchrow(tracking_query, universe_id, as_of_date)
            except:
                # Universe tracking table might not exist or be populated
                tracking_row = {
                    'avg_market_cap': None,
                    'total_market_cap': None,
                    'avg_dollar_volume': None,
                    'total_dollar_volume': None
                }
            
            # Get warning indicators (simplified - would need more complex logic for real implementation)
            warning_count = 0  # Placeholder
            grace_period_count = 0  # Placeholder
            
            return UniverseMetrics(
                universe_id=universe_id,
                universe_name=universe_row['name'],
                total_members=membership_row['total_members'] or 0,
                active_members=membership_row['active_members'] or 0,
                avg_market_cap=tracking_row.get('avg_market_cap') if tracking_row else None,
                total_market_cap=tracking_row.get('total_market_cap') if tracking_row else None,
                avg_dollar_volume=tracking_row.get('avg_dollar_volume') if tracking_row else None,
                total_dollar_volume=tracking_row.get('total_dollar_volume') if tracking_row else None,
                warning_count=warning_count,
                grace_period_count=grace_period_count,
                calculation_date=as_of_date
            )
    
    async def get_membership_table(self, universe_id: int, as_of_date: date, 
                                 limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get paginated membership table for a specific date"""
        async with self.db_pool.acquire() as conn:
            query = f"""
            SELECT 
                um.symbol,
                um.start_at,
                um.end_at,
                CASE WHEN um.start_at <= $2 AND (um.end_at IS NULL OR um.end_at > $2) 
                     THEN true ELSE false END as active,
                COALESCE(ut.avg_market_cap, 0) as market_cap,
                COALESCE(ut.avg_dollar_volume, 0) as dollar_volume,
                ut.removal_reason
            FROM {self.env.get_table_name('universe_membership')} um
            LEFT JOIN {self.env.get_table_name('universe_tracking')} ut 
                ON um.symbol = ut.instrument_id::text AND ut.last_update <= $2
            WHERE um.universe_id = $1
              AND um.start_at <= $2
            ORDER BY um.start_at DESC
            LIMIT $3 OFFSET $4
            """
            
            try:
                rows = await conn.fetch(query, universe_id, as_of_date, limit, offset)
                return [dict(row) for row in rows]
            except Exception as e:
                # Fallback query if universe_tracking doesn't exist
                simple_query = f"""
                SELECT 
                    symbol,
                    start_at,
                    end_at,
                    CASE WHEN start_at <= $2 AND (end_at IS NULL OR end_at > $2) 
                         THEN true ELSE false END as active,
                    NULL as market_cap,
                    NULL as dollar_volume,
                    NULL as removal_reason
                FROM {self.env.get_table_name('universe_membership')} 
                WHERE universe_id = $1 AND start_at <= $2
                ORDER BY start_at DESC
                LIMIT $3 OFFSET $4
                """
                rows = await conn.fetch(simple_query, universe_id, as_of_date, limit, offset)
                return [dict(row) for row in rows]
    
    async def get_qualification_scatter_data(self, universe_id: int, as_of_date: date) -> List[Dict[str, Any]]:
        """Get market cap vs volume scatter plot data for universe members"""
        async with self.db_pool.acquire() as conn:
            query = f"""
            SELECT 
                um.symbol,
                COALESCE(ut.avg_market_cap / 1000000.0, 0) as market_cap_millions,
                COALESCE(ut.avg_dollar_volume / 1000000.0, 0) as volume_millions,
                CASE WHEN um.end_at IS NULL OR um.end_at > $2 THEN 'Active' ELSE 'Inactive' END as status
            FROM {self.env.get_table_name('universe_membership')} um
            LEFT JOIN {self.env.get_table_name('universe_tracking')} ut 
                ON um.symbol = ut.instrument_id::text AND ut.last_update <= $2
            WHERE um.universe_id = $1
              AND um.start_at <= $2
            ORDER BY market_cap_millions DESC
            LIMIT 1000
            """
            
            try:
                rows = await conn.fetch(query, universe_id, as_of_date)
                return [dict(row) for row in rows]
            except Exception as e:
                # Return empty data if tracking table doesn't exist
                self.logger.warning(f"Could not fetch qualification scatter data: {e}")
                return []
    
    async def get_universe_time_series(self, universe_id: int, 
                                     start_date: date, end_date: date) -> UniverseTimeSeriesData:
        """Get time series data for universe size and composition"""
        async with self.db_pool.acquire() as conn:
            # Generate date series
            query = f"""
            WITH date_series AS (
                SELECT generate_series($2::date, $3::date, '1 day'::interval)::date as date
            ),
            daily_membership AS (
                SELECT 
                    ds.date,
                    COUNT(um.symbol) as member_count,
                    COUNT(CASE WHEN um.start_at = ds.date THEN 1 END) as entries,
                    COUNT(CASE WHEN um.end_at = ds.date THEN 1 END) as exits
                FROM date_series ds
                LEFT JOIN {self.env.get_table_name('universe_membership')} um 
                    ON um.universe_id = $1 
                    AND um.start_at <= ds.date 
                    AND (um.end_at IS NULL OR um.end_at > ds.date)
                GROUP BY ds.date
                ORDER BY ds.date
            )
            SELECT * FROM daily_membership
            """
            
            rows = await conn.fetch(query, universe_id, start_date, end_date)
            
            dates = [row['date'].isoformat() for row in rows]
            member_counts = [row['member_count'] for row in rows]
            entry_counts = [row['entries'] for row in rows]
            exit_counts = [row['exits'] for row in rows]
            
            return UniverseTimeSeriesData(
                dates=dates,
                member_counts=member_counts,
                market_cap_totals=[0] * len(dates),  # Placeholder
                volume_totals=[0] * len(dates),      # Placeholder
                entry_counts=entry_counts,
                exit_counts=exit_counts
            )
    
    async def get_universe_warnings(self, universe_id: int) -> List[Dict[str, Any]]:
        """Get current warning indicators for universe members"""
        # This would query the universe_tracking table for stocks in grace period
        # Placeholder implementation
        return [
            {
                "symbol": "EXAMPLE",
                "warning_type": "Market Cap Below Threshold", 
                "current_value": 350000000,
                "threshold": 400000000,
                "grace_period_days_remaining": 5
            }
        ]