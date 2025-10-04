#!/usr/bin/env python3
"""
Data Access Object for Economic Events.
Handles database operations for economic events from multiple vendors.
"""

import asyncpg
import json
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from decimal import Decimal

from core.platform.config_env.environment import Environment


@dataclass
class EconomicEventType:
    """Economic event type data model."""
    id: Optional[int]
    name: str
    description: Optional[str] = None
    category: Optional[str] = None
    country: Optional[str] = None
    importance_level: Optional[int] = None
    frequency: Optional[str] = None
    typical_release_time: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class EconomicEvent:
    """Economic event data model."""
    id: Optional[int]
    event_type_id: int
    date: date
    release_time: Optional[datetime] = None
    estimate: Optional[Decimal] = None
    actual: Optional[Decimal] = None
    previous: Optional[Decimal] = None
    revised: Optional[Decimal] = None
    unit: Optional[str] = None
    currency: Optional[str] = None
    source_vendor: str = None
    source_event_id: Optional[str] = None
    is_preliminary: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class EconomicEventVendorData:
    """Vendor-specific economic event data."""
    id: Optional[int]
    economic_event_id: int
    vendor_event_id: Optional[str] = None
    vendor_specific_data: Optional[Dict[str, Any]] = None
    raw_data: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None


class EconomicEventsDAO:
    """Data Access Object for Economic Events."""

    def __init__(self, connection_pool: asyncpg.Pool, env: Environment):
        self.pool = connection_pool
        self.env = env

    # Event Types Operations

    async def create_event_type(self, event_type: EconomicEventType) -> int:
        """Create a new economic event type."""
        table_name = self.env.get_table_name("economic_event_types")

        async with self.pool.acquire() as conn:
            return await conn.fetchval(f"""
                INSERT INTO {table_name}
                (name, description, category, country, importance_level, frequency, typical_release_time)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
            """, event_type.name, event_type.description, event_type.category,
                event_type.country, event_type.importance_level, event_type.frequency,
                event_type.typical_release_time)

    async def get_event_type_by_name(self, name: str) -> Optional[EconomicEventType]:
        """Get event type by name."""
        table_name = self.env.get_table_name("economic_event_types")

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(f"""
                SELECT * FROM {table_name} WHERE name = $1
            """, name)

            return EconomicEventType(**dict(row)) if row else None

    async def get_event_types_by_importance(self, min_importance: int = 1) -> List[EconomicEventType]:
        """Get event types by minimum importance level."""
        table_name = self.env.get_table_name("economic_event_types")

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT * FROM {table_name}
                WHERE importance_level >= $1
                ORDER BY importance_level DESC, name
            """, min_importance)

            return [EconomicEventType(**dict(row)) for row in rows]

    async def get_event_types_by_country(self, country: str) -> List[EconomicEventType]:
        """Get event types by country."""
        table_name = self.env.get_table_name("economic_event_types")

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT * FROM {table_name}
                WHERE country = $1
                ORDER BY importance_level DESC, name
            """, country)

            return [EconomicEventType(**dict(row)) for row in rows]

    # Economic Events Operations

    async def create_economic_event(self, event: EconomicEvent) -> int:
        """Create a new economic event."""
        table_name = self.env.get_table_name("economic_events")

        async with self.pool.acquire() as conn:
            return await conn.fetchval(f"""
                INSERT INTO {table_name}
                (event_type_id, date, release_time, estimate, actual, previous, revised,
                 unit, currency, source_vendor, source_event_id, is_preliminary)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (event_type_id, date, source_vendor) DO UPDATE SET
                release_time = EXCLUDED.release_time,
                estimate = EXCLUDED.estimate,
                actual = EXCLUDED.actual,
                previous = EXCLUDED.previous,
                revised = EXCLUDED.revised,
                unit = EXCLUDED.unit,
                currency = EXCLUDED.currency,
                source_event_id = EXCLUDED.source_event_id,
                is_preliminary = EXCLUDED.is_preliminary,
                updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, event.event_type_id, event.date, event.release_time, event.estimate,
                event.actual, event.previous, event.revised, event.unit, event.currency,
                event.source_vendor, event.source_event_id, event.is_preliminary)

    async def get_economic_events_by_date_range(self, start_date: date, end_date: date,
                                              vendor: Optional[str] = None) -> List[EconomicEvent]:
        """Get economic events within date range."""
        table_name = self.env.get_table_name("economic_events")

        if vendor:
            where_clause = "WHERE date BETWEEN $1 AND $2 AND source_vendor = $3"
            params = [start_date, end_date, vendor]
        else:
            where_clause = "WHERE date BETWEEN $1 AND $2"
            params = [start_date, end_date]

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT * FROM {table_name}
                {where_clause}
                ORDER BY date DESC, release_time DESC
            """, *params)

            return [EconomicEvent(**dict(row)) for row in rows]

    async def get_economic_events_with_types(self, start_date: date, end_date: date,
                                           min_importance: int = 1) -> List[Dict[str, Any]]:
        """Get economic events with their type information."""
        events_table = self.env.get_table_name("economic_events")
        types_table = self.env.get_table_name("economic_event_types")

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT
                    e.*,
                    et.name as event_name,
                    et.description as event_description,
                    et.category,
                    et.country,
                    et.importance_level,
                    et.frequency
                FROM {events_table} e
                JOIN {types_table} et ON e.event_type_id = et.id
                WHERE e.date BETWEEN $1 AND $2
                AND et.importance_level >= $3
                ORDER BY e.date DESC, et.importance_level DESC, e.release_time DESC
            """, start_date, end_date, min_importance)

            return [dict(row) for row in rows]

    async def get_upcoming_events(self, days_ahead: int = 7,
                                min_importance: int = 3) -> List[Dict[str, Any]]:
        """Get upcoming high-importance economic events."""
        events_table = self.env.get_table_name("economic_events")
        types_table = self.env.get_table_name("economic_event_types")

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT
                    e.*,
                    et.name as event_name,
                    et.description as event_description,
                    et.category,
                    et.country,
                    et.importance_level,
                    et.frequency
                FROM {events_table} e
                JOIN {types_table} et ON e.event_type_id = et.id
                WHERE e.date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '{days_ahead} days'
                AND et.importance_level >= $1
                AND e.actual IS NULL  -- Only events that haven't occurred yet
                ORDER BY e.date ASC, et.importance_level DESC, e.release_time ASC
            """, min_importance)

            return [dict(row) for row in rows]

    # Vendor-specific operations

    async def create_polygon_event_data(self, event_data: EconomicEventVendorData) -> int:
        """Create Polygon-specific event data."""
        table_name = self.env.get_table_name("economic_events_polygon")

        async with self.pool.acquire() as conn:
            return await conn.fetchval(f"""
                INSERT INTO {table_name}
                (economic_event_id, polygon_event_id, name, country, importance,
                 actual_change_percent, estimated_change_percent, previous_change_percent, raw_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING id
            """, event_data.economic_event_id, event_data.vendor_event_id,
                event_data.vendor_specific_data.get('name'),
                event_data.vendor_specific_data.get('country'),
                event_data.vendor_specific_data.get('importance'),
                event_data.vendor_specific_data.get('actual_change_percent'),
                event_data.vendor_specific_data.get('estimated_change_percent'),
                event_data.vendor_specific_data.get('previous_change_percent'),
                event_data.raw_data)

    async def create_tiingo_event_data(self, event_data: EconomicEventVendorData) -> int:
        """Create Tiingo-specific event data."""
        table_name = self.env.get_table_name("economic_events_tiingo")

        async with self.pool.acquire() as conn:
            return await conn.fetchval(f"""
                INSERT INTO {table_name}
                (economic_event_id, tiingo_event_id, description, source_url, tags, raw_data)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
            """, event_data.economic_event_id, event_data.vendor_event_id,
                event_data.vendor_specific_data.get('description'),
                event_data.vendor_specific_data.get('source_url'),
                event_data.vendor_specific_data.get('tags'),
                event_data.raw_data)

    async def create_alpha_vantage_event_data(self, event_data: EconomicEventVendorData) -> int:
        """Create Alpha Vantage-specific event data."""
        table_name = self.env.get_table_name("economic_events_alpha_vantage")

        async with self.pool.acquire() as conn:
            return await conn.fetchval(f"""
                INSERT INTO {table_name}
                (economic_event_id, alpha_vantage_event_id, function_name, interval_period, raw_data)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
            """, event_data.economic_event_id, event_data.vendor_event_id,
                event_data.vendor_specific_data.get('function_name'),
                event_data.vendor_specific_data.get('interval_period'),
                event_data.raw_data)

    async def create_eodhd_event_data(self, event_data: EconomicEventVendorData) -> int:
        """Create EODHD-specific event data."""
        table_name = self.env.get_table_name("economic_events_eodhd")

        async with self.pool.acquire() as conn:
            return await conn.fetchval(f"""
                INSERT INTO {table_name}
                (economic_event_id, eodhd_event_id, event_name, country, importance,
                 period, reference, source, raw_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING id
            """, event_data.economic_event_id, event_data.vendor_event_id,
                event_data.vendor_specific_data.get('event_name'),
                event_data.vendor_specific_data.get('country'),
                event_data.vendor_specific_data.get('importance_text'),
                event_data.vendor_specific_data.get('period'),
                event_data.vendor_specific_data.get('reference'),
                event_data.vendor_specific_data.get('source'),
                json.dumps(event_data.raw_data) if event_data.raw_data else None)

    async def create_fred_event_data(self, event_data: EconomicEventVendorData) -> int:
        """Create FRED-specific event data."""
        table_name = self.env.get_table_name("economic_events_fred")

        async with self.pool.acquire() as conn:
            return await conn.fetchval(f"""
                INSERT INTO {table_name}
                (economic_event_id, fred_series_id, fred_observation_date, series_title,
                 series_units, seasonal_adjustment, frequency, raw_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id
            """, event_data.economic_event_id, event_data.vendor_event_id,
                event_data.vendor_specific_data.get('observation_date'),
                event_data.vendor_specific_data.get('series_title'),
                event_data.vendor_specific_data.get('series_units'),
                event_data.vendor_specific_data.get('seasonal_adjustment'),
                event_data.vendor_specific_data.get('frequency'),
                event_data.raw_data)

    # Analytics and aggregation

    async def get_event_statistics(self) -> Dict[str, Any]:
        """Get statistics about economic events data."""
        events_table = self.env.get_table_name("economic_events")
        types_table = self.env.get_table_name("economic_event_types")

        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow(f"""
                SELECT
                    COUNT(*) as total_events,
                    COUNT(DISTINCT event_type_id) as unique_event_types,
                    COUNT(DISTINCT source_vendor) as unique_vendors,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date,
                    COUNT(CASE WHEN actual IS NOT NULL THEN 1 END) as events_with_actual,
                    COUNT(CASE WHEN estimate IS NOT NULL THEN 1 END) as events_with_estimate
                FROM {events_table}
            """)

            vendor_stats = await conn.fetch(f"""
                SELECT
                    source_vendor,
                    COUNT(*) as event_count,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date
                FROM {events_table}
                GROUP BY source_vendor
                ORDER BY event_count DESC
            """)

            importance_stats = await conn.fetch(f"""
                SELECT
                    et.importance_level,
                    COUNT(*) as event_count
                FROM {events_table} e
                JOIN {types_table} et ON e.event_type_id = et.id
                GROUP BY et.importance_level
                ORDER BY et.importance_level DESC
            """)

            return {
                "overall": dict(stats),
                "by_vendor": [dict(row) for row in vendor_stats],
                "by_importance": [dict(row) for row in importance_stats]
            }