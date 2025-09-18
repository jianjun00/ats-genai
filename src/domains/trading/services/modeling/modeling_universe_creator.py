"""
Modeling Universe Creator

Creates universes for modeling based on specific market cap and volume criteria.
Filters stocks by average market cap > 400M and average dollar trading volume > 100M
over the past 20 trading days to ensure adequate liquidity and size for modeling.

Uses proper database joins: dev_daily_price_polygon -> dev_instrument_xrefs -> vendor_symbol
"""

import asyncio
import asyncpg
import logging
from datetime import date, datetime, timedelta
from typing import List, Optional
from dataclasses import dataclass
from src.core.shared.utils.environment import Environment
import gin

@dataclass
class ModelingStock:
    """Represents a stock qualifying for modeling universe"""
    symbol: str
    instrument_id: int
    avg_market_cap: Optional[float]  # 20-day average from market cap table
    avg_dollar_volume: Optional[float]  # 20-day average
    avg_volume: Optional[float]  # 20-day average
    avg_price: Optional[float]  # 20-day average
    trading_days: int
    first_date: date
    last_date: date

@gin.configurable
class ModelingUniverseCreator:
    """
    Creates universes for modeling based on market cap and liquidity criteria.

    Filters stocks based on:
    - Average market cap > 400M over past 20 trading days (when available)
    - Average dollar trading volume > 100M over past 20 trading days
    - Minimum number of trading days for data completeness
    """

    def __init__(self, env: Environment = None):
        self.env = env or Environment()
        self.logger = logging.getLogger(__name__)

    async def create_modeling_universe(
        self,
        universe_name: str,
        min_market_cap_millions: float = 400,  # $400M minimum
        min_dollar_volume_millions: float = 100,  # $100M daily minimum
        min_trading_days: int = 20,  # At least 20 trading days
        lookback_days: int = 30,  # Look back 30 calendar days for data
        max_stocks: Optional[int] = None  # Optional limit on universe size
    ) -> int:
        """
        Create a modeling universe based on market cap and volume criteria.

        Args:
            universe_name: Name for the new universe
            min_market_cap_millions: Minimum average market cap in millions USD
            min_dollar_volume_millions: Minimum average dollar volume in millions USD
            min_trading_days: Minimum trading days in lookback period
            lookback_days: Number of calendar days to look back for data
            max_stocks: Optional maximum number of stocks to include

        Returns:
            Universe ID of the created universe
        """
        self.logger.info(f"Creating modeling universe: {universe_name}")
        self.logger.info(f"Criteria: market_cap>${min_market_cap_millions}M, "
                        f"dollar_volume>${min_dollar_volume_millions}M, "
                        f"min_trading_days={min_trading_days}")

        # Get qualifying stocks
        qualifying_stocks = await self.get_qualifying_stocks(
            min_market_cap_millions, min_dollar_volume_millions,
            min_trading_days, lookback_days
        )

        self.logger.info(f"Found {len(qualifying_stocks)} qualifying stocks")

        # Apply size limit if specified
        if max_stocks and len(qualifying_stocks) > max_stocks:
            # Sort by combined market cap and dollar volume score
            qualifying_stocks = self._rank_stocks_for_modeling(qualifying_stocks)[:max_stocks]
            self.logger.info(f"Limited to top {max_stocks} stocks by modeling score")

        # Create universe and populate
        universe_id = await self._create_universe_with_stocks(
            universe_name, qualifying_stocks, min_market_cap_millions,
            min_dollar_volume_millions
        )

        return universe_id

    async def get_qualifying_stocks(
        self,
        min_market_cap_millions: float,
        min_dollar_volume_millions: float,
        min_trading_days: int,
        lookback_days: int
    ) -> List[ModelingStock]:
        """
        Get stocks that meet the modeling criteria based on recent trading data.
        """
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                return await self._query_qualifying_stocks(
                    conn, min_market_cap_millions, min_dollar_volume_millions,
                    min_trading_days, lookback_days
                )
        finally:
            await pool.close()

    async def _query_qualifying_stocks(
        self,
        conn,
        min_market_cap_millions: float,
        min_dollar_volume_millions: float,
        min_trading_days: int,
        lookback_days: int
    ) -> List[ModelingStock]:
        """Query for stocks meeting modeling criteria using proper database joins"""

        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)

        # Query combines price/volume data with market cap data using proper joins
        query = f"""
        WITH recent_trading AS (
            SELECT
                dp.instrument_id,
                xr.vendor_symbol as symbol,
                dp.date,
                dp.close,
                dp.volume,
                dp.close * dp.volume as dollar_volume
            FROM {self.env.get_table_name('daily_price_polygon')} dp
            JOIN {self.env.get_table_name('instrument_xrefs')} xr ON dp.instrument_id = xr.instrument_id
            WHERE xr.vendor_id = 3  -- ticker vendor
              AND dp.date >= $1 AND dp.date <= $2
              AND dp.volume > 0
              AND dp.close > 0
              AND xr.vendor_symbol IS NOT NULL
              AND xr.vendor_symbol ~ '^[A-Z]{{1,5}}$'
        ),
        market_cap_data AS (
            SELECT
                mc.instrument_id,
                mc.date,
                mc.market_cap
            FROM {self.env.get_table_name('daily_market_cap')} mc
            WHERE mc.date >= $1 AND mc.date <= $2
              AND mc.market_cap > 0
        ),
        combined_data AS (
            SELECT
                rt.instrument_id,
                rt.symbol,
                rt.date,
                rt.close,
                rt.volume,
                rt.dollar_volume,
                COALESCE(mc.market_cap, rt.close * rt.volume * 0.0001) as estimated_market_cap
            FROM recent_trading rt
            LEFT JOIN market_cap_data mc ON rt.instrument_id = mc.instrument_id AND rt.date = mc.date
        ),
        stock_metrics AS (
            SELECT
                instrument_id,
                symbol,
                COUNT(*) as trading_days,
                AVG(close) as avg_price,
                AVG(volume) as avg_volume,
                AVG(dollar_volume) as avg_dollar_volume,
                AVG(estimated_market_cap) as avg_market_cap,
                MIN(date) as first_date,
                MAX(date) as last_date
            FROM combined_data
            GROUP BY instrument_id, symbol
        )
        SELECT
            instrument_id,
            symbol,
            avg_market_cap,
            avg_dollar_volume,
            avg_volume,
            avg_price,
            trading_days,
            first_date,
            last_date
        FROM stock_metrics
        WHERE trading_days >= $3
          AND avg_market_cap >= $4  -- Market cap filter
          AND avg_dollar_volume >= $5  -- Dollar volume filter
          AND avg_price > 1.0  -- Filter out penny stocks
          AND avg_price < 5000  -- Filter out extreme prices
        ORDER BY avg_market_cap DESC, avg_dollar_volume DESC
        """

        min_market_cap = min_market_cap_millions * 1_000_000
        min_dollar_volume = min_dollar_volume_millions * 1_000_000

        try:
            rows = await conn.fetch(
                query, start_date, end_date, min_trading_days,
                min_market_cap, min_dollar_volume
            )

            stocks = []
            for row in rows:
                stocks.append(ModelingStock(
                    symbol=row['symbol'],
                    instrument_id=row['instrument_id'],
                    avg_market_cap=row['avg_market_cap'],
                    avg_dollar_volume=row['avg_dollar_volume'],
                    avg_volume=row['avg_volume'],
                    avg_price=row['avg_price'],
                    trading_days=row['trading_days'],
                    first_date=row['first_date'],
                    last_date=row['last_date']
                ))

            return stocks

        except Exception as e:
            self.logger.error(f"Error querying qualifying stocks: {e}")
            return []

    def _rank_stocks_for_modeling(self, stocks: List[ModelingStock]) -> List[ModelingStock]:
        """
        Rank stocks by modeling suitability based on market cap and liquidity.
        """
        def modeling_score(stock):
            market_cap_score = (stock.avg_market_cap or 0) / 1_000_000  # Scale to millions
            dollar_volume_score = (stock.avg_dollar_volume or 0) / 1_000_000  # Scale to millions

            # Combined score: 60% market cap, 40% liquidity
            return 0.6 * market_cap_score + 0.4 * dollar_volume_score

        return sorted(stocks, key=modeling_score, reverse=True)

    async def _create_universe_with_stocks(
        self,
        universe_name: str,
        stocks: List[ModelingStock],
        min_market_cap_millions: float,
        min_dollar_volume_millions: float
    ) -> int:
        """Create universe and populate with modeling stocks"""

        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                # Create universe
                universe_id = await self._create_universe(
                    conn, universe_name, min_market_cap_millions, min_dollar_volume_millions
                )

                # Add members
                await self._add_modeling_members(conn, universe_id, stocks)

                return universe_id
        finally:
            await pool.close()

    async def _create_universe(
        self,
        conn,
        universe_name: str,
        min_market_cap_millions: float,
        min_dollar_volume_millions: float
    ) -> int:
        """Create a new modeling universe"""

        description = (f"Modeling universe: market cap > ${min_market_cap_millions}M, "
                      f"dollar volume > ${min_dollar_volume_millions}M "
                      f"(created {date.today()})")

        # Use unique name to avoid conflicts
        unique_name = f"{universe_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        query = f"""
        INSERT INTO {self.env.get_table_name('universe')} (name, description)
        VALUES ($1, $2)
        RETURNING id
        """

        row = await conn.fetchrow(query, unique_name, description)
        universe_id = row['id']

        self.logger.info(f"Created universe '{unique_name}' with ID: {universe_id}")
        return universe_id

    async def _add_modeling_members(
        self,
        conn,
        universe_id: int,
        stocks: List[ModelingStock]
    ) -> None:
        """Add modeling stocks to universe membership"""

        membership_table = self.env.get_table_name('universe_membership')
        membership_start = date.today()

        for stock in stocks:
            query = f"""
            INSERT INTO {membership_table} (universe_id, symbol, start_at, end_at)
            VALUES ($1, $2, $3, NULL)
            ON CONFLICT (universe_id, symbol, start_at) DO NOTHING
            """

            await conn.execute(query, universe_id, stock.symbol, membership_start)

        self.logger.info(f"Added {len(stocks)} members to universe {universe_id}")

    async def generate_modeling_report(
        self,
        stocks: List[ModelingStock],
        min_market_cap_millions: float,
        min_dollar_volume_millions: float,
        output_file: str = None
    ) -> str:
        """Generate a report of the modeling universe selection"""

        total_market_cap = sum(s.avg_market_cap or 0 for s in stocks)
        avg_market_cap = total_market_cap / len(stocks) if stocks else 0

        total_dollar_volume = sum(s.avg_dollar_volume or 0 for s in stocks)
        avg_dollar_volume = total_dollar_volume / len(stocks) if stocks else 0

        report_lines = [
            f"# Modeling Universe Report",
            f"Generated: {datetime.now().isoformat()}",
            "",
            "## Selection Criteria",
            f"- Minimum average market cap: ${min_market_cap_millions:,.0f}M",
            f"- Minimum average dollar volume: ${min_dollar_volume_millions:,.0f}M",
            f"- Based on past 20 trading days",
            "",
            "## Summary Statistics",
            f"- Total stocks selected: {len(stocks)}",
            f"- Average market cap: ${avg_market_cap/1_000_000:,.0f}M",
            f"- Total market cap: ${total_market_cap/1_000_000:,.0f}M",
            f"- Average daily dollar volume: ${avg_dollar_volume/1_000_000:,.0f}M",
            f"- Total daily dollar volume: ${total_dollar_volume/1_000_000:,.0f}M",
            "",
            "## Market Cap Distribution",
        ]

        # Add market cap distribution
        market_caps = [s.avg_market_cap/1_000_000 for s in stocks if s.avg_market_cap]
        if market_caps:
            market_caps.sort(reverse=True)
            report_lines.extend([
                f"- Largest: ${market_caps[0]:,.0f}M",
                f"- Median: ${market_caps[len(market_caps)//2]:,.0f}M",
                f"- Smallest: ${market_caps[-1]:,.0f}M",
                ""
            ])

        report_lines.extend([
            "## Selected Stocks",
            "| Symbol | Instrument ID | Market Cap ($M) | Dollar Volume ($M) | Avg Price | Trading Days |",
            "|--------|---------------|-----------------|--------------------|-----------| ------------|"
        ])

        # Sort by market cap for display
        sorted_stocks = sorted(stocks,
                             key=lambda s: s.avg_market_cap or 0,
                             reverse=True)

        for stock in sorted_stocks:
            market_cap_str = f"${stock.avg_market_cap/1_000_000:,.0f}" if stock.avg_market_cap else "N/A"
            dollar_volume_str = f"${stock.avg_dollar_volume/1_000_000:,.0f}" if stock.avg_dollar_volume else "N/A"
            price_str = f"${stock.avg_price:.2f}" if stock.avg_price else "N/A"

            report_lines.append(
                f"| {stock.symbol} | {stock.instrument_id} | {market_cap_str} | {dollar_volume_str} | "
                f"{price_str} | {stock.trading_days} |"
            )

        report = "\n".join(report_lines)

        if output_file:
            with open(output_file, 'w') as f:
                f.write(report)
            self.logger.info(f"Modeling report saved to {output_file}")

        return report


async def main():
    """Main function to create modeling universe"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Create universe for modeling based on market cap and volume criteria"
    )
    parser.add_argument("--universe-name", default="modeling_400m_100m",
                       help="Name for the new universe")
    parser.add_argument("--min-market-cap", type=float, default=400,
                       help="Minimum market cap in millions USD (default: 400)")
    parser.add_argument("--min-dollar-volume", type=float, default=100,
                       help="Minimum daily dollar volume in millions USD (default: 100)")
    parser.add_argument("--min-trading-days", type=int, default=20,
                       help="Minimum trading days in lookback period (default: 20)")
    parser.add_argument("--lookback-days", type=int, default=30,
                       help="Calendar days to look back for data (default: 30)")
    parser.add_argument("--max-stocks", type=int,
                       help="Maximum number of stocks to include")
    parser.add_argument("--report-file", help="Output file for selection report")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create modeling universe creator
    creator = ModelingUniverseCreator()

    # Create universe
    universe_id = await creator.create_modeling_universe(
        universe_name=args.universe_name,
        min_market_cap_millions=args.min_market_cap,
        min_dollar_volume_millions=args.min_dollar_volume,
        min_trading_days=args.min_trading_days,
        lookback_days=args.lookback_days,
        max_stocks=args.max_stocks
    )

    print(f"Created modeling universe '{args.universe_name}' with ID: {universe_id}")

    # Generate report if requested
    if args.report_file:
        stocks = await creator.get_qualifying_stocks(
            args.min_market_cap, args.min_dollar_volume,
            args.min_trading_days, args.lookback_days
        )

        if args.max_stocks and len(stocks) > args.max_stocks:
            stocks = creator._rank_stocks_for_modeling(stocks)[:args.max_stocks]

        report = await creator.generate_modeling_report(
            stocks, args.min_market_cap, args.min_dollar_volume, args.report_file
        )
        print(f"Report saved to: {args.report_file}")


if __name__ == "__main__":
    asyncio.run(main())