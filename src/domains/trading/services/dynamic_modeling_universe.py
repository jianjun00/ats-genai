"""
Dynamic Modeling Universe Creator

Creates and maintains a dynamic universe with:
- Entry: >$400M market cap AND >$100M avg trading volume (52 days)
- Exit: Stock removed after 1 week if it no longer meets criteria
- Re-entry: Must meet criteria again AND 1 year must pass since removal
- Daily monitoring and automatic updates

Usage:
    PYTHONPATH=src python src/universe/dynamic_modeling_universe.py --update-daily
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import argparse
import json

from shared.utils.environment import Environment


@dataclass
class UniverseStock:
    """Stock in the dynamic universe with tracking info"""
    instrument_id: int
    symbol: str
    entry_date: date
    last_qualifying_date: date
    warning_date: Optional[date] = None  # Date when stock first failed criteria
    removal_date: Optional[date] = None
    removal_reason: Optional[str] = None
    avg_market_cap: Optional[float] = None
    avg_dollar_volume: Optional[float] = None
    last_update: date = None


@dataclass
class QualificationMetrics:
    """Current qualification metrics for a stock"""
    symbol: str
    instrument_id: int
    avg_market_cap_millions: float
    avg_dollar_volume_millions: float
    trading_days_count: int
    meets_market_cap: bool
    meets_volume: bool
    qualifies: bool
    last_price: float = None
    calculation_date: date = None


class DynamicModelingUniverse:
    """
    Dynamic universe that automatically adds/removes stocks based on criteria
    
    Rules:
    1. Entry: Market cap > $400M AND volume > $100M (52-day average)
    2. Grace period: 1 week after failing criteria before removal
    3. Re-entry restriction: Must wait 1 year after removal
    4. Daily monitoring and updates
    """
    
    def __init__(self, env: Environment):
        self.env = env
        self.logger = logging.getLogger(__name__)
        
        # Configuration
        self.universe_name = "dynamic_modeling_400m_100m"
        self.min_market_cap_millions = 400
        self.min_dollar_volume_millions = 100
        self.lookback_days = 52  # ~2.5 months of trading
        self.min_trading_days = 40  # Minimum trading days in lookback
        self.grace_period_days = 7  # 1 week grace period
        self.reentry_restriction_days = 365  # 1 year restriction
        
        # Database connection
        self.db_pool = None
    
    async def initialize(self):
        """Initialize database connection and universe"""
        # Get database configuration from environment
        try:
            db_config = self.env.get_database_config()
            self.logger.debug(f"Raw database config: {db_config}")
            
            # Filter to only asyncpg-compatible parameters (based on test findings)
            asyncpg_compatible_keys = {'host', 'port', 'user', 'password', 'database'}
            asyncpg_config = {
                k: v for k, v in db_config.items() 
                if k in asyncpg_compatible_keys and v is not None
            }
            
            # Log filtered config (without password)
            safe_config = {k: v if k != 'password' else '*****' for k, v in asyncpg_config.items()}
            self.logger.info(f"Using filtered database config: {safe_config}")
            
            self.db_pool = await asyncpg.create_pool(**asyncpg_config)
            self.logger.info("Database connection pool created successfully")
            
        except (AttributeError, Exception) as e:
            self.logger.warning(f"Failed with get_database_config(): {e}")
            # Fallback for different environment API or connection issues
            try:
                db_url = self.env.get_database_url()
                self.logger.info(f"Trying database URL fallback")
                self.db_pool = await asyncpg.create_pool(db_url)
                self.logger.info("Database connection successful with URL fallback")
            except Exception as fallback_e:
                self.logger.error(f"All database connection methods failed: {fallback_e}")
                raise
        
        # Ensure universe exists
        await self._ensure_universe_exists()
        
        self.logger.info(f"Dynamic universe '{self.universe_name}' initialized")
    
    async def close(self):
        """Close database connections"""
        if self.db_pool:
            await self.db_pool.close()
    
    async def run_daily_update(self, update_date: Optional[date] = None) -> Dict[str, any]:
        """
        Run daily universe update
        
        Returns:
            Summary of changes made
        """
        if update_date is None:
            update_date = date.today()
        
        self.logger.info(f"Running daily universe update for {update_date}")
        
        # Get current universe stocks
        current_stocks = await self._get_current_universe_stocks()
        self.logger.info(f"Current universe has {len(current_stocks)} stocks")
        
        # Get all qualifying stocks based on current criteria
        qualifying_metrics = await self._get_qualifying_stocks(update_date)
        self.logger.info(f"Found {len(qualifying_metrics)} stocks meeting criteria")
        
        # Process additions and removals
        summary = {
            "update_date": update_date,
            "current_count": len(current_stocks),
            "qualifying_count": len(qualifying_metrics),
            "added": [],
            "removed": [],
            "warned": [],
            "updated_metrics": [],
            "errors": []
        }
        
        try:
            # Process potential additions
            await self._process_additions(qualifying_metrics, current_stocks, update_date, summary)
            
            # Process potential removals and warnings
            await self._process_removals_and_warnings(qualifying_metrics, current_stocks, update_date, summary)
            
            # Update metrics for existing stocks
            await self._update_stock_metrics(current_stocks, qualifying_metrics, update_date, summary)
            
            # Log summary
            self._log_update_summary(summary)
            
        except Exception as e:
            self.logger.error(f"Error in daily update: {e}")
            summary["errors"].append(str(e))
            raise
        
        return summary
    
    async def _get_qualifying_stocks(self, calculation_date: date) -> List[QualificationMetrics]:
        """Get all stocks that currently meet the criteria"""
        
        # Calculate date range for lookback
        end_date = calculation_date
        start_date = calculation_date - timedelta(days=self.lookback_days * 2)  # Extra buffer for weekends
        
        query = """
        WITH price_data AS (
            SELECT 
                p.instrument_id,
                x.vendor_symbol as symbol,
                p.date,
                p.close,
                p.volume,
                (p.close * p.volume) as dollar_volume,
                mc.market_cap
            FROM {prices_table} p
            JOIN {xrefs_table} x ON p.instrument_id = x.instrument_id
            JOIN {vendors_table} v ON x.vendor_id = v.id
            LEFT JOIN {market_cap_table} mc ON p.instrument_id = mc.instrument_id 
                                              AND p.date = mc.date
            WHERE p.date BETWEEN $1 AND $2
              AND x.vendor_id = 3  -- Ticker vendor
              AND p.close > 1.0  -- Basic price filter
              AND p.volume > 1000      -- Basic volume filter
              AND x.vendor_symbol ~ '^[A-Z]+$'  -- Valid ticker format
        ),
        recent_data AS (
            SELECT 
                instrument_id,
                symbol,
                COUNT(*) as trading_days,
                AVG(COALESCE(market_cap, close * volume * 0.0001)) / 1000000.0 as avg_market_cap_millions,
                AVG(dollar_volume) / 1000000.0 as avg_dollar_volume_millions,
                MAX(close) as last_price,
                MAX(date) as last_date
            FROM price_data
            WHERE date >= $3  -- Only recent data for calculation
            GROUP BY instrument_id, symbol
            HAVING COUNT(*) >= $4  -- Minimum trading days
        )
        SELECT 
            instrument_id,
            symbol,
            trading_days,
            avg_market_cap_millions,
            avg_dollar_volume_millions,
            last_price,
            last_date,
            CASE WHEN avg_market_cap_millions >= $5 THEN true ELSE false END as meets_market_cap,
            CASE WHEN avg_dollar_volume_millions >= $6 THEN true ELSE false END as meets_volume,
            CASE WHEN avg_market_cap_millions >= $5 AND avg_dollar_volume_millions >= $6 
                 THEN true ELSE false END as qualifies
        FROM recent_data
        ORDER BY avg_dollar_volume_millions DESC
        """.format(
            prices_table=self.env.get_table_name("daily_prices_polygon"),
            xrefs_table=self.env.get_table_name("instrument_xrefs"),
            vendors_table=self.env.get_table_name("vendors"),
            market_cap_table=self.env.get_table_name("daily_market_cap")
        )
        
        # Date parameters
        recent_start_date = calculation_date - timedelta(days=int(self.lookback_days * 1.4))  # Allow for weekends
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                query,
                start_date,
                end_date,
                recent_start_date,
                self.min_trading_days,
                self.min_market_cap_millions,
                self.min_dollar_volume_millions
            )
        
        metrics = []
        for row in rows:
            metric = QualificationMetrics(
                symbol=row['symbol'],
                instrument_id=row['instrument_id'],
                avg_market_cap_millions=float(row['avg_market_cap_millions']),
                avg_dollar_volume_millions=float(row['avg_dollar_volume_millions']),
                trading_days_count=int(row['trading_days']),
                meets_market_cap=row['meets_market_cap'],
                meets_volume=row['meets_volume'],
                qualifies=row['qualifies'],
                last_price=float(row['last_price']) if row['last_price'] else None,
                calculation_date=calculation_date
            )
            metrics.append(metric)
        
        return metrics
    
    async def _process_additions(self,
                               qualifying_metrics: List[QualificationMetrics],
                               current_stocks: List[UniverseStock],
                               update_date: date,
                               summary: Dict) -> None:
        """Process potential stock additions to universe"""
        
        current_instrument_ids = {stock.instrument_id for stock in current_stocks}
        qualifying_by_id = {m.instrument_id: m for m in qualifying_metrics if m.qualifies}
        
        # Find stocks that qualify but aren't in universe
        potential_additions = []
        for instrument_id, metrics in qualifying_by_id.items():
            if instrument_id not in current_instrument_ids:
                potential_additions.append(metrics)
        
        # Check re-entry restrictions
        for metrics in potential_additions:
            can_add = await self._check_reentry_eligibility(metrics.instrument_id, update_date)
            
            if can_add:
                await self._add_stock_to_universe(metrics, update_date)
                summary["added"].append({
                    "symbol": metrics.symbol,
                    "instrument_id": metrics.instrument_id,
                    "market_cap": metrics.avg_market_cap_millions,
                    "volume": metrics.avg_dollar_volume_millions
                })
                self.logger.info(
                    f"Added {metrics.symbol} to universe "
                    f"(Cap: ${metrics.avg_market_cap_millions:.0f}M, "
                    f"Vol: ${metrics.avg_dollar_volume_millions:.0f}M)"
                )
            else:
                self.logger.debug(
                    f"Skipped {metrics.symbol} - still in re-entry restriction period"
                )
    
    async def _process_removals_and_warnings(self,
                                           qualifying_metrics: List[QualificationMetrics],
                                           current_stocks: List[UniverseStock],
                                           update_date: date,
                                           summary: Dict) -> None:
        """Process potential warnings and removals"""
        
        qualifying_by_id = {m.instrument_id: m for m in qualifying_metrics}
        
        for stock in current_stocks:
            metrics = qualifying_by_id.get(stock.instrument_id)
            
            if metrics is None or not metrics.qualifies:
                # Stock no longer qualifies
                if stock.warning_date is None:
                    # First time failing - start grace period
                    await self._set_warning_date(stock, update_date)
                    reason = self._get_failure_reason(metrics) if metrics else "No recent data"
                    summary["warned"].append({
                        "symbol": stock.symbol,
                        "instrument_id": stock.instrument_id,
                        "reason": reason,
                        "grace_period_ends": update_date + timedelta(days=self.grace_period_days)
                    })
                    self.logger.info(
                        f"Warning issued for {stock.symbol}: {reason} "
                        f"(Grace period ends {update_date + timedelta(days=self.grace_period_days)})"
                    )
                
                elif (update_date - stock.warning_date).days >= self.grace_period_days:
                    # Grace period expired - remove stock
                    reason = self._get_failure_reason(metrics) if metrics else "No recent data"
                    await self._remove_stock_from_universe(stock, update_date, reason)
                    summary["removed"].append({
                        "symbol": stock.symbol,
                        "instrument_id": stock.instrument_id,
                        "reason": reason,
                        "warning_date": stock.warning_date,
                        "days_warned": (update_date - stock.warning_date).days
                    })
                    self.logger.info(
                        f"Removed {stock.symbol} from universe: {reason} "
                        f"(Warned for {(update_date - stock.warning_date).days} days)"
                    )
            
            else:
                # Stock qualifies again - clear warning if exists
                if stock.warning_date is not None:
                    await self._clear_warning_date(stock, update_date)
                    self.logger.info(f"Cleared warning for {stock.symbol} - now qualifies")
    
    async def _update_stock_metrics(self,
                                  current_stocks: List[UniverseStock],
                                  qualifying_metrics: List[QualificationMetrics],
                                  update_date: date,
                                  summary: Dict) -> None:
        """Update metrics for existing stocks"""
        
        qualifying_by_id = {m.instrument_id: m for m in qualifying_metrics}
        
        for stock in current_stocks:
            metrics = qualifying_by_id.get(stock.instrument_id)
            if metrics:
                await self._update_stock_metrics_in_db(stock, metrics, update_date)
                summary["updated_metrics"].append({
                    "symbol": stock.symbol,
                    "market_cap": metrics.avg_market_cap_millions,
                    "volume": metrics.avg_dollar_volume_millions,
                    "qualifies": metrics.qualifies
                })
    
    def _get_failure_reason(self, metrics: Optional[QualificationMetrics]) -> str:
        """Get human-readable reason for stock failure"""
        if metrics is None:
            return "No recent data available"
        
        reasons = []
        if not metrics.meets_market_cap:
            reasons.append(f"Market cap ${metrics.avg_market_cap_millions:.0f}M < ${self.min_market_cap_millions}M")
        if not metrics.meets_volume:
            reasons.append(f"Volume ${metrics.avg_dollar_volume_millions:.0f}M < ${self.min_dollar_volume_millions}M")
        
        return "; ".join(reasons) if reasons else "Unknown criteria failure"
    
    async def _check_reentry_eligibility(self, instrument_id: int, current_date: date) -> bool:
        """Check if stock is eligible for re-entry (1 year restriction)"""
        
        query = """
        SELECT MAX(removal_date) as last_removal_date
        FROM {universe_tracking_table}
        WHERE universe_name = $1 
          AND instrument_id = $2
          AND removal_date IS NOT NULL
        """.format(universe_tracking_table=self.env.get_table_name("universe_tracking"))
        
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(query, self.universe_name, instrument_id)
            
            if row and row['last_removal_date']:
                days_since_removal = (current_date - row['last_removal_date']).days
                return days_since_removal >= self.reentry_restriction_days
            
            return True  # Never been removed, so eligible
    
    async def _ensure_universe_exists(self):
        """Ensure universe and tracking table exist"""
        
        async with self.db_pool.acquire() as conn:
            # Check if updated_at column exists in universe table
            updated_at_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = $1 AND column_name = 'updated_at'
                );
            """, self.env.get_table_name("universe"))
            
            # Create tracking table first
            tracking_table_query = """
            CREATE TABLE IF NOT EXISTS {universe_tracking_table} (
                id SERIAL PRIMARY KEY,
                universe_name VARCHAR(100) NOT NULL,
                instrument_id INTEGER NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                entry_date DATE NOT NULL,
                last_qualifying_date DATE,
                warning_date DATE,
                removal_date DATE,
                removal_reason TEXT,
                avg_market_cap DECIMAL(15,2),
                avg_dollar_volume DECIMAL(15,2),
                last_update DATE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(universe_name, instrument_id, entry_date)
            )
            """.format(universe_tracking_table=self.env.get_table_name("universe_tracking"))
            
            await conn.execute(tracking_table_query)
            
            # Define universe query based on schema
            if updated_at_exists:
                # Use modern schema with updated_at
                universe_query = """
                INSERT INTO {universe_table} (name, description, created_at, updated_at)
                VALUES ($1, $2, $3, $3)
                ON CONFLICT (name) DO UPDATE SET
                    updated_at = $3,
                    description = $2
                RETURNING id
                """.format(universe_table=self.env.get_table_name("universe"))
            else:
                # Use legacy schema without updated_at  
                universe_query = """
                INSERT INTO {universe_table} (name, description, created_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (name) DO UPDATE SET
                    description = $2
                RETURNING id
                """.format(universe_table=self.env.get_table_name("universe"))
            
            description = (
                f"Dynamic modeling universe: Market cap >${self.min_market_cap_millions}M, "
                f"Volume >${self.min_dollar_volume_millions}M ({self.lookback_days}d avg), "
                f"{self.grace_period_days}d grace period, {self.reentry_restriction_days}d re-entry restriction"
            )
            
            # Create or update universe
            universe_id = await conn.fetchval(
                universe_query,
                self.universe_name,
                description,
                datetime.now()
            )
            
            self.logger.info(f"Universe '{self.universe_name}' initialized with ID {universe_id} (schema compatible: updated_at_exists={updated_at_exists})")
    
    async def _get_current_universe_stocks(self) -> List[UniverseStock]:
        """Get current stocks in the universe"""
        
        query = """
        SELECT 
            instrument_id,
            symbol,
            entry_date,
            last_qualifying_date,
            warning_date,
            removal_date,
            removal_reason,
            avg_market_cap,
            avg_dollar_volume,
            last_update
        FROM {universe_tracking_table}
        WHERE universe_name = $1
          AND removal_date IS NULL  -- Only active stocks
        ORDER BY entry_date
        """.format(universe_tracking_table=self.env.get_table_name("universe_tracking"))
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, self.universe_name)
        
        stocks = []
        for row in rows:
            stock = UniverseStock(
                instrument_id=row['instrument_id'],
                symbol=row['symbol'],
                entry_date=row['entry_date'],
                last_qualifying_date=row['last_qualifying_date'],
                warning_date=row['warning_date'],
                removal_date=row['removal_date'],
                removal_reason=row['removal_reason'],
                avg_market_cap=float(row['avg_market_cap']) if row['avg_market_cap'] else None,
                avg_dollar_volume=float(row['avg_dollar_volume']) if row['avg_dollar_volume'] else None,
                last_update=row['last_update']
            )
            stocks.append(stock)
        
        return stocks
    
    async def _add_stock_to_universe(self, metrics: QualificationMetrics, entry_date: date):
        """Add stock to universe and tracking"""
        
        # Add to tracking table
        tracking_query = """
        INSERT INTO {universe_tracking_table} 
            (universe_name, instrument_id, symbol, entry_date, last_qualifying_date, 
             avg_market_cap, avg_dollar_volume, last_update)
        VALUES ($1, $2, $3, $4, $4, $5, $6, $4)
        """.format(universe_tracking_table=self.env.get_table_name("universe_tracking"))
        
        # Add to universe membership (using symbol, not instrument_id to match existing schema)
        membership_query = """
        INSERT INTO {universe_membership_table} (universe_id, symbol, start_at)
        SELECT u.id, $2, $3
        FROM {universe_table} u
        WHERE u.name = $1
        ON CONFLICT (universe_id, symbol, start_at) DO NOTHING
        """.format(
            universe_membership_table=self.env.get_table_name("universe_membership"),
            universe_table=self.env.get_table_name("universe")
        )
        
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                tracking_query,
                self.universe_name,
                metrics.instrument_id,
                metrics.symbol,
                entry_date,
                metrics.avg_market_cap_millions,
                metrics.avg_dollar_volume_millions
            )
            
            await conn.execute(
                membership_query,
                self.universe_name,
                metrics.symbol,
                entry_date
            )
    
    async def _remove_stock_from_universe(self, stock: UniverseStock, removal_date: date, reason: str):
        """Remove stock from universe"""
        
        # Update tracking table
        tracking_query = """
        UPDATE {universe_tracking_table}
        SET removal_date = $3,
            removal_reason = $4,
            last_update = $3,
            updated_at = NOW()
        WHERE universe_name = $1 AND instrument_id = $2
        """.format(universe_tracking_table=self.env.get_table_name("universe_tracking"))
        
        # Remove from universe membership (update end_at instead of deleting)
        membership_query = """
        UPDATE {universe_membership_table}
        SET end_at = $3
        WHERE universe_id = (
            SELECT id FROM {universe_table} WHERE name = $1
        ) AND symbol = $2 AND end_at IS NULL
        """.format(
            universe_membership_table=self.env.get_table_name("universe_membership"),
            universe_table=self.env.get_table_name("universe")
        )
        
        async with self.db_pool.acquire() as conn:
            await conn.execute(tracking_query, self.universe_name, stock.instrument_id, removal_date, reason)
            await conn.execute(membership_query, self.universe_name, stock.symbol, removal_date)
    
    async def _set_warning_date(self, stock: UniverseStock, warning_date: date):
        """Set warning date for stock"""
        
        query = """
        UPDATE {universe_tracking_table}
        SET warning_date = $3,
            last_update = $3,
            updated_at = NOW()
        WHERE universe_name = $1 AND instrument_id = $2
        """.format(universe_tracking_table=self.env.get_table_name("universe_tracking"))
        
        async with self.db_pool.acquire() as conn:
            await conn.execute(query, self.universe_name, stock.instrument_id, warning_date)
    
    async def _clear_warning_date(self, stock: UniverseStock, update_date: date):
        """Clear warning date for stock"""
        
        query = """
        UPDATE {universe_tracking_table}
        SET warning_date = NULL,
            last_qualifying_date = $3,
            last_update = $3,
            updated_at = NOW()
        WHERE universe_name = $1 AND instrument_id = $2
        """.format(universe_tracking_table=self.env.get_table_name("universe_tracking"))
        
        async with self.db_pool.acquire() as conn:
            await conn.execute(query, self.universe_name, stock.instrument_id, update_date)
    
    async def _update_stock_metrics_in_db(self, stock: UniverseStock, metrics: QualificationMetrics, update_date: date):
        """Update stock metrics in database"""
        
        query = """
        UPDATE {universe_tracking_table}
        SET avg_market_cap = $3,
            avg_dollar_volume = $4,
            last_qualifying_date = CASE WHEN $5 THEN $6 ELSE last_qualifying_date END,
            last_update = $6,
            updated_at = NOW()
        WHERE universe_name = $1 AND instrument_id = $2
        """.format(universe_tracking_table=self.env.get_table_name("universe_tracking"))
        
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                query,
                self.universe_name,
                stock.instrument_id,
                metrics.avg_market_cap_millions,
                metrics.avg_dollar_volume_millions,
                metrics.qualifies,
                update_date
            )
    
    def _log_update_summary(self, summary: Dict):
        """Log update summary"""
        
        self.logger.info("=" * 60)
        self.logger.info(f"DAILY UNIVERSE UPDATE SUMMARY - {summary['update_date']}")
        self.logger.info("=" * 60)
        self.logger.info(f"Current stocks in universe: {summary['current_count']}")
        self.logger.info(f"Stocks meeting criteria: {summary['qualifying_count']}")
        self.logger.info(f"Stocks added: {len(summary['added'])}")
        self.logger.info(f"Stocks removed: {len(summary['removed'])}")
        self.logger.info(f"Stocks warned: {len(summary['warned'])}")
        self.logger.info(f"Metrics updated: {len(summary['updated_metrics'])}")
        
        if summary['added']:
            self.logger.info("ADDITIONS:")
            for add in summary['added']:
                self.logger.info(f"  + {add['symbol']}: Cap ${add['market_cap']:.0f}M, Vol ${add['volume']:.0f}M")
        
        if summary['removed']:
            self.logger.info("REMOVALS:")
            for removal in summary['removed']:
                self.logger.info(f"  - {removal['symbol']}: {removal['reason']}")
        
        if summary['warned']:
            self.logger.info("WARNINGS:")
            for warning in summary['warned']:
                self.logger.info(f"  ⚠ {warning['symbol']}: {warning['reason']}")
        
        if summary['errors']:
            self.logger.error("ERRORS:")
            for error in summary['errors']:
                self.logger.error(f"  ❌ {error}")
    
    async def get_current_universe_report(self) -> str:
        """Generate current universe status report"""
        
        current_stocks = await self._get_current_universe_stocks()
        
        # Get latest metrics
        latest_metrics = await self._get_qualifying_stocks(date.today())
        metrics_by_id = {m.instrument_id: m for m in latest_metrics}
        
        report_lines = [
            f"# Dynamic Modeling Universe Report",
            f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Universe**: {self.universe_name}",
            "",
            f"## Criteria",
            f"- **Market Cap**: > ${self.min_market_cap_millions}M (52-day average)",
            f"- **Trading Volume**: > ${self.min_dollar_volume_millions}M (52-day average)",
            f"- **Grace Period**: {self.grace_period_days} days after failing criteria",
            f"- **Re-entry Restriction**: {self.reentry_restriction_days} days after removal",
            "",
            f"## Current Universe ({len(current_stocks)} stocks)",
            "",
            "| Symbol | Entry Date | Market Cap ($M) | Volume ($M) | Status | Warning Date |",
            "|--------|------------|----------------|-------------|--------|--------------|"
        ]
        
        for stock in sorted(current_stocks, key=lambda x: x.entry_date):
            metrics = metrics_by_id.get(stock.instrument_id)
            
            if metrics:
                market_cap = f"{metrics.avg_market_cap_millions:.0f}"
                volume = f"{metrics.avg_dollar_volume_millions:.0f}"
                status = "✅ Qualifying" if metrics.qualifies else "⚠️ Failing"
            else:
                market_cap = f"{stock.avg_market_cap:.0f}" if stock.avg_market_cap else "N/A"
                volume = f"{stock.avg_dollar_volume:.0f}" if stock.avg_dollar_volume else "N/A"
                status = "❓ No Data"
            
            warning_date = stock.warning_date.strftime('%m/%d/%Y') if stock.warning_date else ""
            
            report_lines.append(
                f"| {stock.symbol} | {stock.entry_date.strftime('%m/%d/%Y')} | "
                f"{market_cap} | {volume} | {status} | {warning_date} |"
            )
        
        # Summary statistics
        if current_stocks:
            total_market_cap = sum(
                metrics_by_id[s.instrument_id].avg_market_cap_millions 
                for s in current_stocks 
                if s.instrument_id in metrics_by_id
            )
            total_volume = sum(
                metrics_by_id[s.instrument_id].avg_dollar_volume_millions 
                for s in current_stocks 
                if s.instrument_id in metrics_by_id
            )
            
            report_lines.extend([
                "",
                f"## Summary Statistics",
                f"- **Total Market Cap**: ${total_market_cap:,.0f}M",
                f"- **Total Daily Volume**: ${total_volume:,.0f}M",
                f"- **Average Market Cap**: ${total_market_cap/len(current_stocks):,.0f}M",
                f"- **Average Daily Volume**: ${total_volume/len(current_stocks):,.0f}M"
            ])
        
        return "\n".join(report_lines)


async def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Dynamic Modeling Universe Management")
    parser.add_argument("--update-daily", action="store_true", 
                       help="Run daily universe update")
    parser.add_argument("--report", action="store_true",
                       help="Generate current universe report")
    parser.add_argument("--date", type=str, 
                       help="Specific date for update (YYYY-MM-DD), defaults to today")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # Parse date
    update_date = None
    if args.date:
        try:
            update_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            return 1
    
    try:
        # Initialize
        env = Environment()
        universe = DynamicModelingUniverse(env)
        await universe.initialize()
        
        if args.update_daily:
            logger.info("Running daily universe update...")
            summary = await universe.run_daily_update(update_date)
            
            # Save summary to file
            summary_file = f"universe_update_{summary['update_date']}.json"
            with open(summary_file, 'w') as f:
                # Convert date objects to strings for JSON serialization
                json_summary = json.loads(json.dumps(summary, default=str))
                json.dump(json_summary, f, indent=2)
            
            logger.info(f"Update summary saved to: {summary_file}")
        
        if args.report:
            logger.info("Generating universe report...")
            report = await universe.get_current_universe_report()
            
            report_file = f"universe_report_{date.today().strftime('%Y%m%d')}.md"
            with open(report_file, 'w') as f:
                f.write(report)
            
            print(report)
            logger.info(f"Report saved to: {report_file}")
        
        if not args.update_daily and not args.report:
            logger.info("No action specified. Use --update-daily or --report")
            parser.print_help()
        
        await universe.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)