#!/usr/bin/env python3
"""
Missing Data Detection and Backfill System.
Identifies gaps in daily price data and automatically backfills from available sources.
"""

import asyncio
import asyncpg
import logging
from datetime import date, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import aiohttp

from core.platform.config.environment import Environment

logger = logging.getLogger(__name__)


@dataclass
class DataGap:
    """Represents a gap in price data."""
    instrument_id: int
    symbol: str
    vendor: str
    missing_dates: List[date]
    gap_start: date
    gap_end: date
    trading_days_missing: int
    backfill_priority: int  # 1=critical, 5=low


@dataclass
class BackfillResult:
    """Result of a backfill operation."""
    instrument_id: int
    symbol: str
    vendor: str
    target_date: date
    success: bool
    records_added: int
    error_message: Optional[str] = None


class MissingDataHandler:
    """Handles detection and backfilling of missing daily price data."""

    def __init__(self, connection_pool: asyncpg.Pool, env: Environment, api_keys: Dict[str, str]):
        self.pool = connection_pool
        self.env = env
        self.api_keys = api_keys

        # API configurations
        self.api_configs = {
            "polygon": {
                "base_url": "https://api.polygon.io/v2/aggs/ticker",
                "rate_limit": 0.1  # 100ms between calls
            },
            "tiingo": {
                "base_url": "https://api.tiingo.com/tiingo/daily",
                "rate_limit": 0.5  # 500ms between calls
            }
        }

    async def detect_missing_data(self, start_date: date, end_date: date,
                                priority_symbols: Optional[List[str]] = None) -> List[DataGap]:
        """
        Detect missing data gaps for a date range.

        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
            priority_symbols: Optional list of high-priority symbols to check first

        Returns:
            List of data gaps found
        """
        logger.info(f"Detecting missing data from {start_date} to {end_date}")

        gaps = []

        # Get all active instruments
        instruments = await self._get_active_instruments(priority_symbols)

        # Generate list of expected trading days
        trading_days = self._get_trading_days(start_date, end_date)

        for vendor in ["polygon", "tiingo"]:
            vendor_gaps = await self._detect_vendor_gaps(
                vendor, instruments, trading_days
            )
            gaps.extend(vendor_gaps)

        # Sort by priority (critical gaps first)
        gaps.sort(key=lambda g: (g.backfill_priority, -g.trading_days_missing))

        logger.info(f"Found {len(gaps)} data gaps across {len(instruments)} instruments")
        return gaps

    async def _get_active_instruments(self, priority_symbols: Optional[List[str]]) -> List[Dict[str, Any]]:
        """Get active instruments, optionally filtered by priority symbols."""
        instruments_table = self.env.get_table_name("instruments")

        async with self.pool.acquire() as conn:
            if priority_symbols:
                placeholders = ",".join([f"${i+1}" for i in range(len(priority_symbols))])
                query = f"""
                    SELECT id, symbol, name FROM {instruments_table}
                    WHERE is_active = true AND symbol IN ({placeholders})
                    ORDER BY symbol
                """
                rows = await conn.fetch(query, *priority_symbols)
            else:
                query = f"""
                    SELECT id, symbol, name FROM {instruments_table}
                    WHERE is_active = true
                    ORDER BY symbol
                    LIMIT 1000  -- Limit for performance
                """
                rows = await conn.fetch(query)

        return [dict(row) for row in rows]

    def _get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """Generate list of trading days (excludes weekends, basic holiday handling)."""
        trading_days = []
        current_date = start_date

        # Basic US market holidays (simplified)
        holidays = {
            # Add major holidays - in production, use market calendar library
            date(2024, 1, 1),   # New Year's Day
            date(2024, 7, 4),   # Independence Day
            date(2024, 12, 25), # Christmas
            # Add more holidays as needed
        }

        while current_date <= end_date:
            # Skip weekends and holidays
            if current_date.weekday() < 5 and current_date not in holidays:
                trading_days.append(current_date)
            current_date += timedelta(days=1)

        return trading_days

    async def _detect_vendor_gaps(self, vendor: str, instruments: List[Dict[str, Any]],
                                trading_days: List[date]) -> List[DataGap]:
        """Detect gaps for a specific vendor."""
        gaps = []
        table_name = self.env.get_table_name(f"daily_price_polygon_{vendor}")

        async with self.pool.acquire() as conn:
            for instrument in instruments:
                instrument_id = instrument["id"]
                symbol = instrument["symbol"]

                # Get existing dates for this instrument
                existing_dates = await conn.fetch(f"""
                    SELECT DISTINCT date FROM {table_name}
                    WHERE instrument_id = $1 AND date >= $2 AND date <= $3
                    ORDER BY date
                """, instrument_id, trading_days[0], trading_days[-1])

                existing_date_set = {row["date"] for row in existing_dates}
                missing_dates = [d for d in trading_days if d not in existing_date_set]

                if missing_dates:
                    # Group consecutive missing dates into gaps
                    gap_groups = self._group_consecutive_dates(missing_dates)

                    for gap_dates in gap_groups:
                        priority = self._calculate_priority(symbol, len(gap_dates))

                        gap = DataGap(
                            instrument_id=instrument_id,
                            symbol=symbol,
                            vendor=vendor,
                            missing_dates=gap_dates,
                            gap_start=gap_dates[0],
                            gap_end=gap_dates[-1],
                            trading_days_missing=len(gap_dates),
                            backfill_priority=priority
                        )
                        gaps.append(gap)

        return gaps

    def _group_consecutive_dates(self, dates: List[date]) -> List[List[date]]:
        """Group consecutive dates into separate gaps."""
        if not dates:
            return []

        sorted_dates = sorted(dates)
        groups = []
        current_group = [sorted_dates[0]]

        for i in range(1, len(sorted_dates)):
            current_date = sorted_dates[i]
            prev_date = sorted_dates[i-1]

            # Check if dates are consecutive (considering weekends)
            days_diff = (current_date - prev_date).days
            if days_diff <= 3:  # Allow for weekends
                current_group.append(current_date)
            else:
                groups.append(current_group)
                current_group = [current_date]

        groups.append(current_group)
        return groups

    def _calculate_priority(self, symbol: str, missing_days: int) -> int:
        """Calculate backfill priority (1=highest, 5=lowest)."""
        # High-priority symbols (major indices, popular stocks)
        high_priority_symbols = {
            "SPY", "QQQ", "IWM", "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
            "NVDA", "META", "BRK.B", "JPM", "JNJ", "V", "PG"
        }

        if symbol in high_priority_symbols:
            if missing_days >= 5:
                return 1  # Critical
            elif missing_days >= 3:
                return 2  # High
            else:
                return 3  # Medium
        else:
            if missing_days >= 10:
                return 2  # High for any symbol with many missing days
            elif missing_days >= 5:
                return 3  # Medium
            else:
                return 4  # Low

    async def backfill_missing_data(self, gaps: List[DataGap],
                                  max_concurrent: int = 5,
                                  priority_threshold: int = 3) -> List[BackfillResult]:
        """
        Backfill missing data for detected gaps.

        Args:
            gaps: List of data gaps to backfill
            max_concurrent: Maximum concurrent API requests
            priority_threshold: Only backfill gaps with priority <= threshold

        Returns:
            List of backfill results
        """
        # Filter by priority
        priority_gaps = [g for g in gaps if g.backfill_priority <= priority_threshold]

        logger.info(f"Backfilling {len(priority_gaps)} high-priority gaps")

        results = []
        semaphore = asyncio.Semaphore(max_concurrent)

        # Group gaps by vendor for efficient processing
        gaps_by_vendor = {}
        for gap in priority_gaps:
            if gap.vendor not in gaps_by_vendor:
                gaps_by_vendor[gap.vendor] = []
            gaps_by_vendor[gap.vendor].append(gap)

        # Process each vendor's gaps
        tasks = []
        for vendor, vendor_gaps in gaps_by_vendor.items():
            for gap in vendor_gaps:
                task = asyncio.create_task(
                    self._backfill_single_gap(semaphore, gap)
                )
                tasks.append(task)

        # Wait for all backfill tasks to complete
        backfill_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for result in backfill_results:
            if isinstance(result, Exception):
                logger.error(f"Backfill task failed: {result}")
            else:
                results.extend(result)

        # Log summary
        successful = sum(1 for r in results if r.success)
        total_records = sum(r.records_added for r in results)

        logger.info(f"Backfill completed: {successful}/{len(results)} successful, "
                   f"{total_records} records added")

        return results

    async def _backfill_single_gap(self, semaphore: asyncio.Semaphore,
                                 gap: DataGap) -> List[BackfillResult]:
        """Backfill a single data gap."""
        async with semaphore:
            results = []

            # Backfill each missing date in the gap
            for missing_date in gap.missing_dates:
                try:
                    result = await self._fetch_and_store_price_data(
                        gap.vendor, gap.instrument_id, gap.symbol, missing_date
                    )
                    results.append(result)

                    # Rate limiting
                    rate_limit = self.api_configs[gap.vendor]["rate_limit"]
                    await asyncio.sleep(rate_limit)

                except Exception as e:
                    logger.error(f"Error backfilling {gap.symbol} {missing_date}: {e}")
                    results.append(BackfillResult(
                        instrument_id=gap.instrument_id,
                        symbol=gap.symbol,
                        vendor=gap.vendor,
                        target_date=missing_date,
                        success=False,
                        records_added=0,
                        error_message=str(e)
                    ))

            return results

    async def _fetch_and_store_price_data(self, vendor: str, instrument_id: int,
                                        symbol: str, target_date: date) -> BackfillResult:
        """Fetch price data from API and store in database."""
        try:
            # Fetch data from API
            price_data = await self._fetch_price_from_api(vendor, symbol, target_date)

            if not price_data:
                return BackfillResult(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    vendor=vendor,
                    target_date=target_date,
                    success=False,
                    records_added=0,
                    error_message="No data returned from API"
                )

            # Store in database
            records_added = await self._store_price_data(vendor, instrument_id, price_data)

            return BackfillResult(
                instrument_id=instrument_id,
                symbol=symbol,
                vendor=vendor,
                target_date=target_date,
                success=True,
                records_added=records_added
            )

        except Exception as e:
            return BackfillResult(
                instrument_id=instrument_id,
                symbol=symbol,
                vendor=vendor,
                target_date=target_date,
                success=False,
                records_added=0,
                error_message=str(e)
            )

    async def _fetch_price_from_api(self, vendor: str, symbol: str,
                                  target_date: date) -> Optional[Dict[str, Any]]:
        """Fetch price data from vendor API."""
        if vendor not in self.api_keys or not self.api_keys[vendor]:
            raise ValueError(f"API key not configured for {vendor}")

        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            if vendor == "polygon":
                return await self._fetch_polygon_price(session, symbol, target_date)
            elif vendor == "tiingo":
                return await self._fetch_tiingo_price(session, symbol, target_date)
            else:
                raise ValueError(f"Unsupported vendor: {vendor}")

    async def _fetch_polygon_price(self, session: aiohttp.ClientSession,
                                 symbol: str, target_date: date) -> Optional[Dict[str, Any]]:
        """Fetch price data from Polygon API."""
        base_url = self.api_configs["polygon"]["base_url"]
        url = f"{base_url}/{symbol}/range/1/day/{target_date}/{target_date}"

        params = {"apikey": self.api_keys["polygon"]}

        async with session.get(url, params=params) as response:
            if response.status == 429:
                await asyncio.sleep(12)  # Rate limit backoff
                return None
            elif response.status != 200:
                logger.warning(f"Polygon API error {response.status} for {symbol} {target_date}")
                return None

            data = await response.json()
            results = data.get("results", [])

            if results:
                result = results[0]
                return {
                    "date": target_date,
                    "open": result.get("o"),
                    "high": result.get("h"),
                    "low": result.get("l"),
                    "close": result.get("c"),
                    "volume": result.get("v")
                }

            return None

    async def _fetch_tiingo_price(self, session: aiohttp.ClientSession,
                                symbol: str, target_date: date) -> Optional[Dict[str, Any]]:
        """Fetch price data from Tiingo API."""
        base_url = self.api_configs["tiingo"]["base_url"]
        url = f"{base_url}/{symbol}/prices"

        params = {
            "startDate": target_date.strftime("%Y-%m-%d"),
            "endDate": target_date.strftime("%Y-%m-%d"),
            "format": "json",
            "token": self.api_keys["tiingo"]
        }

        async with session.get(url, params=params) as response:
            if response.status == 429:
                await asyncio.sleep(5)  # Rate limit backoff
                return None
            elif response.status != 200:
                logger.warning(f"Tiingo API error {response.status} for {symbol} {target_date}")
                return None

            data = await response.json()

            if data:
                result = data[0]
                return {
                    "date": target_date,
                    "open": result.get("open"),
                    "high": result.get("high"),
                    "low": result.get("low"),
                    "close": result.get("close"),
                    "adjclose": result.get("adjClose"),
                    "volume": result.get("volume")
                }

            return None

    async def _store_price_data(self, vendor: str, instrument_id: int,
                              price_data: Dict[str, Any]) -> int:
        """Store price data in database."""
        table_name = self.env.get_table_name(f"daily_price_polygon_{vendor}")

        async with self.pool.acquire() as conn:
            if vendor == "polygon":
                await conn.execute(f"""
                    INSERT INTO {table_name}
                    (date, instrument_id, open, high, low, close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (instrument_id, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    updated_at = CURRENT_TIMESTAMP
                """,
                    price_data["date"], instrument_id,
                    price_data["open"], price_data["high"],
                    price_data["low"], price_data["close"],
                    price_data["volume"]
                )
            elif vendor == "tiingo":
                await conn.execute(f"""
                    INSERT INTO {table_name}
                    (date, instrument_id, open, high, low, close, adjclose, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (instrument_id, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adjclose = EXCLUDED.adjclose,
                    volume = EXCLUDED.volume,
                    updated_at = CURRENT_TIMESTAMP
                """,
                    price_data["date"], instrument_id,
                    price_data["open"], price_data["high"],
                    price_data["low"], price_data["close"],
                    price_data["adjclose"], price_data["volume"]
                )

        return 1  # One record added

    async def generate_missing_data_report(self, start_date: date,
                                         end_date: date) -> Dict[str, Any]:
        """Generate comprehensive missing data report."""
        gaps = await self.detect_missing_data(start_date, end_date)

        # Analyze gaps
        gaps_by_vendor = {}
        gaps_by_priority = {}
        gaps_by_symbol = {}

        for gap in gaps:
            # By vendor
            if gap.vendor not in gaps_by_vendor:
                gaps_by_vendor[gap.vendor] = []
            gaps_by_vendor[gap.vendor].append(gap)

            # By priority
            priority = gap.backfill_priority
            if priority not in gaps_by_priority:
                gaps_by_priority[priority] = []
            gaps_by_priority[priority].append(gap)

            # By symbol
            if gap.symbol not in gaps_by_symbol:
                gaps_by_symbol[gap.symbol] = []
            gaps_by_symbol[gap.symbol].append(gap)

        # Calculate statistics
        total_missing_days = sum(gap.trading_days_missing for gap in gaps)
        avg_gap_size = total_missing_days / len(gaps) if gaps else 0

        # Get symbols with most issues
        symbol_issues = [(symbol, len(symbol_gaps), sum(g.trading_days_missing for g in symbol_gaps))
                        for symbol, symbol_gaps in gaps_by_symbol.items()]
        symbol_issues.sort(key=lambda x: x[2], reverse=True)  # Sort by total missing days

        return {
            "period": {"start": start_date, "end": end_date},
            "summary": {
                "total_gaps": len(gaps),
                "total_missing_days": total_missing_days,
                "avg_gap_size": round(avg_gap_size, 1),
                "vendors_affected": list(gaps_by_vendor.keys()),
                "symbols_affected": len(gaps_by_symbol)
            },
            "by_vendor": {
                vendor: {
                    "gap_count": len(vendor_gaps),
                    "missing_days": sum(g.trading_days_missing for g in vendor_gaps)
                } for vendor, vendor_gaps in gaps_by_vendor.items()
            },
            "by_priority": {
                f"priority_{priority}": {
                    "gap_count": len(priority_gaps),
                    "missing_days": sum(g.trading_days_missing for g in priority_gaps)
                } for priority, priority_gaps in gaps_by_priority.items()
            },
            "top_problem_symbols": symbol_issues[:10],
            "critical_gaps": [
                {
                    "symbol": gap.symbol,
                    "vendor": gap.vendor,
                    "missing_days": gap.trading_days_missing,
                    "gap_start": gap.gap_start.isoformat(),
                    "gap_end": gap.gap_end.isoformat()
                } for gap in gaps if gap.backfill_priority <= 2
            ]
        }