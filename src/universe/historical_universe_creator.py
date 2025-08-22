"""
Historical Universe Creator

Creates universes based on historical point-in-time criteria to avoid survivorship bias.
Uses only information that would have been available at the specified time period.
"""

import asyncio
import asyncpg
import logging
import random
from datetime import date, datetime
from typing import List, Dict, Set, Optional
from dataclasses import dataclass
from config.environment import Environment
import gin

@dataclass
class HistoricalStock:
    """Represents a stock with historical point-in-time data"""
    symbol: str
    instrument_id: Optional[int]
    market_cap: Optional[float]
    avg_volume: Optional[float]
    avg_price: Optional[float]
    trading_days: int
    first_date: date
    last_date: date
    
@gin.configurable
class HistoricalUniverseCreator:
    """
    Creates universes based on historical point-in-time criteria.
    
    This approach avoids survivorship bias by using only information
    that would have been available at the specified historical period.
    """
    
    def __init__(self, env: Environment = None):
        self.env = env or Environment()
        self.logger = logging.getLogger(__name__)
        
    async def create_historical_sample_universe(
        self, 
        universe_name: str,
        sample_year: int = 2020,
        sample_size: int = 50,
        min_market_cap_millions: float = 1000,  # $1B minimum
        min_avg_volume: int = 100000,  # 100k shares daily
        min_trading_days: int = 200,  # Active for most of the year
        seed: Optional[int] = 42
    ) -> int:
        """
        Create a universe by sampling stocks that were available in a historical year.
        
        Args:
            universe_name: Name for the new universe
            sample_year: Year to use for sampling criteria (default 2020)
            sample_size: Number of stocks to sample (default 50)
            min_market_cap_millions: Minimum market cap in millions USD
            min_avg_volume: Minimum average daily volume
            min_trading_days: Minimum trading days in the sample year
            seed: Random seed for reproducible sampling
            
        Returns:
            Universe ID of the created universe
        """
        self.logger.info(f"Creating historical sample universe: {universe_name}")
        self.logger.info(f"Sample criteria: year={sample_year}, size={sample_size}, "
                        f"min_market_cap=${min_market_cap_millions}M, "
                        f"min_volume={min_avg_volume:,}")
        
        if seed is not None:
            random.seed(seed)
            
        # Get stocks that were active in the sample year
        active_stocks = await self.get_active_stocks_in_year(
            sample_year, min_market_cap_millions, min_avg_volume, min_trading_days
        )
        
        self.logger.info(f"Found {len(active_stocks)} stocks meeting criteria in {sample_year}")
        
        if len(active_stocks) < sample_size:
            self.logger.warning(f"Only {len(active_stocks)} stocks available, "
                               f"sampling all instead of {sample_size}")
            sampled_stocks = active_stocks
        else:
            # Sample stocks based on market cap weighting
            sampled_stocks = self._sample_stocks_by_market_cap(active_stocks, sample_size)
        
        self.logger.info(f"Sampled {len(sampled_stocks)} stocks for universe")
        
        # Create universe and populate
        universe_id = await self._create_universe_with_historical_members(
            universe_name, sampled_stocks, sample_year
        )
        
        return universe_id
    
    async def get_active_stocks_in_year(
        self,
        year: int,
        min_market_cap_millions: float,
        min_avg_volume: int,
        min_trading_days: int
    ) -> List[HistoricalStock]:
        """
        Get stocks that were actively trading in the specified year.
        
        Uses only data from that year to avoid survivorship bias.
        """
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                return await self._query_active_stocks_in_year(
                    conn, year, min_market_cap_millions, min_avg_volume, min_trading_days
                )
        finally:
            await pool.close()
    
    async def _query_active_stocks_in_year(
        self, 
        conn, 
        year: int,
        min_market_cap_millions: float,
        min_avg_volume: int,
        min_trading_days: int
    ) -> List[HistoricalStock]:
        """Query for active stocks in the specified year"""
        
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        
        # Query daily prices with volume and market data
        # We'll try multiple sources and take the best data available
        query = f"""
        WITH yearly_stats AS (
            SELECT 
                symbol,
                COUNT(*) as trading_days,
                AVG(volume) as avg_volume,
                AVG(close) as avg_price,
                AVG(close * volume) as avg_dollar_volume,
                MIN(date) as first_date,
                MAX(date) as last_date,
                -- Estimate market cap using close price and shares outstanding proxy
                -- We'll approximate shares outstanding from volume patterns
                AVG(close * volume / NULLIF(volume, 0) * 1000000) as estimated_market_cap
            FROM (
                -- Try polygon data first
                SELECT symbol, date, close, volume
                FROM {self.env.get_table_name('daily_prices_polygon')}
                WHERE date >= $1 AND date <= $2
                  AND volume > 0 
                  AND close > 0
                
                UNION ALL
                
                -- Then tiingo data
                SELECT symbol, date, close, volume
                FROM {self.env.get_table_name('daily_prices_tiingo')}
                WHERE date >= $1 AND date <= $2
                  AND volume > 0 
                  AND close > 0
                  AND symbol NOT IN (
                      SELECT DISTINCT symbol 
                      FROM {self.env.get_table_name('daily_prices_polygon')}
                      WHERE date >= $1 AND date <= $2
                  )
                
                UNION ALL
                
                -- Fallback to unified daily prices
                SELECT symbol, date, close, volume
                FROM {self.env.get_table_name('daily_prices')}
                WHERE date >= $1 AND date <= $2
                  AND volume > 0 
                  AND close > 0
                  AND symbol NOT IN (
                      SELECT DISTINCT symbol 
                      FROM {self.env.get_table_name('daily_prices_polygon')}
                      WHERE date >= $1 AND date <= $2
                      UNION
                      SELECT DISTINCT symbol 
                      FROM {self.env.get_table_name('daily_prices_tiingo')}
                      WHERE date >= $1 AND date <= $2
                  )
            ) combined_data
            GROUP BY symbol
        ),
        instruments AS (
            SELECT id, symbol 
            FROM {self.env.get_table_name('instruments')}
        )
        SELECT 
            ys.symbol,
            i.id as instrument_id,
            ys.avg_volume,
            ys.avg_price,
            ys.trading_days,
            ys.first_date,
            ys.last_date,
            -- Simple market cap estimation based on price and volume patterns
            CASE 
                WHEN ys.avg_price > 0 AND ys.avg_volume > 0 
                THEN ys.avg_price * (ys.avg_volume * 500)  -- Rough approximation
                ELSE NULL 
            END as estimated_market_cap
        FROM yearly_stats ys
        LEFT JOIN instruments i ON ys.symbol = i.symbol
        WHERE ys.trading_days >= $3
          AND ys.avg_volume >= $4
          AND ys.avg_price > 1.0  -- Filter out penny stocks
          AND ys.avg_price < 2000  -- Filter out extreme prices
          AND ys.symbol ~ '^[A-Z]{{1,5}}$'  -- Simple symbol format filter
        ORDER BY ys.avg_volume DESC, ys.trading_days DESC
        """
        
        try:
            rows = await conn.fetch(
                query, start_date, end_date, min_trading_days, min_avg_volume
            )
            
            stocks = []
            for row in rows:
                # Apply market cap filter if we have estimated market cap
                estimated_market_cap = row['estimated_market_cap']
                if (estimated_market_cap is None or 
                    estimated_market_cap >= min_market_cap_millions * 1_000_000):
                    
                    stocks.append(HistoricalStock(
                        symbol=row['symbol'],
                        instrument_id=row['instrument_id'],
                        market_cap=estimated_market_cap,
                        avg_volume=row['avg_volume'],
                        avg_price=row['avg_price'],
                        trading_days=row['trading_days'],
                        first_date=row['first_date'],
                        last_date=row['last_date']
                    ))
            
            return stocks
            
        except Exception as e:
            self.logger.error(f"Error querying active stocks for {year}: {e}")
            return []
    
    def _sample_stocks_by_market_cap(
        self, 
        stocks: List[HistoricalStock], 
        sample_size: int
    ) -> List[HistoricalStock]:
        """
        Sample stocks with bias toward larger market cap.
        
        This gives more weight to larger companies while still including
        some smaller ones for diversity.
        """
        # Sort by market cap (use volume as proxy if market cap not available)
        def sort_key(stock):
            if stock.market_cap:
                return stock.market_cap
            else:
                # Use volume * price as proxy for market cap
                return (stock.avg_volume or 0) * (stock.avg_price or 0)
        
        stocks_sorted = sorted(stocks, key=sort_key, reverse=True)
        
        # Use weighted sampling: top 25% get higher weight
        top_25_percent = len(stocks_sorted) // 4
        weights = []
        
        for i, stock in enumerate(stocks_sorted):
            if i < top_25_percent:
                weight = 3.0  # Higher weight for large cap
            elif i < len(stocks_sorted) // 2:
                weight = 2.0  # Medium weight for mid cap
            else:
                weight = 1.0  # Base weight for smaller cap
            weights.append(weight)
        
        # Weighted random sampling without replacement
        sampled_indices = []
        remaining_indices = list(range(len(stocks_sorted)))
        remaining_weights = weights.copy()
        
        for _ in range(min(sample_size, len(stocks_sorted))):
            # Calculate cumulative weights
            total_weight = sum(remaining_weights)
            if total_weight == 0:
                break
                
            # Random selection based on weights
            rand_val = random.random() * total_weight
            cumulative = 0
            
            for j, weight in enumerate(remaining_weights):
                cumulative += weight
                if cumulative >= rand_val:
                    selected_idx = remaining_indices[j]
                    sampled_indices.append(selected_idx)
                    remaining_indices.pop(j)
                    remaining_weights.pop(j)
                    break
        
        return [stocks_sorted[i] for i in sampled_indices]
    
    async def _create_universe_with_historical_members(
        self,
        universe_name: str,
        stocks: List[HistoricalStock],
        sample_year: int
    ) -> int:
        """Create universe and populate with historical stock members"""
        
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                # Create universe
                universe_id = await self._create_universe(conn, universe_name, sample_year)
                
                # Add members
                await self._add_historical_members(conn, universe_id, stocks, sample_year)
                
                return universe_id
        finally:
            await pool.close()
    
    async def _create_universe(self, conn, universe_name: str, sample_year: int) -> int:
        """Create a new universe"""
        
        description = (f"Historical sample universe from {sample_year} "
                      f"(created {date.today()}, avoids survivorship bias)")
        
        query = f"""
        INSERT INTO {self.env.get_table_name('universe')} (name, description)
        VALUES ($1, $2)
        ON CONFLICT (name) DO UPDATE SET 
            description = EXCLUDED.description,
            updated_at = NOW()
        RETURNING id
        """
        
        row = await conn.fetchrow(query, universe_name, description)
        universe_id = row['id']
        
        self.logger.info(f"Created universe '{universe_name}' with ID: {universe_id}")
        return universe_id
    
    async def _add_historical_members(
        self, 
        conn, 
        universe_id: int,
        stocks: List[HistoricalStock],
        sample_year: int
    ) -> None:
        """Add historical stocks to universe membership"""
        
        membership_table = self.env.get_table_name('universe_membership')
        
        # Use January 1st of the sample year as the membership start date
        membership_start = date(sample_year, 1, 1)
        
        for stock in stocks:
            query = f"""
            INSERT INTO {membership_table} (universe_id, symbol, start_at, end_at)
            VALUES ($1, $2, $3, NULL)
            ON CONFLICT (universe_id, symbol, start_at) DO NOTHING
            """
            
            await conn.execute(query, universe_id, stock.symbol, membership_start)
        
        self.logger.info(f"Added {len(stocks)} members to universe {universe_id}")
    
    async def generate_historical_report(
        self, 
        stocks: List[HistoricalStock],
        sample_year: int,
        output_file: str = None
    ) -> str:
        """Generate a report of the historical universe selection"""
        
        total_market_cap = sum(s.market_cap or 0 for s in stocks if s.market_cap)
        avg_market_cap = total_market_cap / len([s for s in stocks if s.market_cap]) if stocks else 0
        
        report_lines = [
            f"# Historical Universe Report - {sample_year}",
            f"Generated: {datetime.now().isoformat()}",
            "",
            "## Summary",
            f"- Sample year: {sample_year}",
            f"- Total stocks selected: {len(stocks)}",
            f"- Average estimated market cap: ${avg_market_cap/1_000_000:,.0f}M",
            f"- Total estimated market cap: ${total_market_cap/1_000_000:,.0f}M",
            "",
            "## Methodology",
            "- Uses only information available in the sample year",
            "- Avoids survivorship bias by not using future data",
            "- Weighted sampling favoring larger market cap stocks",
            "- Filters for active trading and reasonable liquidity",
            "",
            "## Selected Stocks",
            "| Symbol | Est. Market Cap ($M) | Avg Volume | Avg Price | Trading Days |",
            "|--------|---------------------|------------|-----------|--------------|"
        ]
        
        # Sort by market cap for display
        sorted_stocks = sorted(stocks, 
                             key=lambda s: s.market_cap or 0, 
                             reverse=True)
        
        for stock in sorted_stocks:
            market_cap_str = f"${stock.market_cap/1_000_000:,.0f}" if stock.market_cap else "N/A"
            volume_str = f"{stock.avg_volume:,.0f}" if stock.avg_volume else "N/A"
            price_str = f"${stock.avg_price:.2f}" if stock.avg_price else "N/A"
            
            report_lines.append(
                f"| {stock.symbol} | {market_cap_str} | {volume_str} | "
                f"{price_str} | {stock.trading_days} |"
            )
        
        report = "\n".join(report_lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report)
            self.logger.info(f"Historical report saved to {output_file}")
        
        return report


async def main():
    """Main function to create historical sample universe"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Create universe by sampling stocks from historical period"
    )
    parser.add_argument("--universe-name", default="test_sample_2020_50", 
                       help="Name for the new universe")
    parser.add_argument("--sample-year", type=int, default=2020,
                       help="Year to sample from (default: 2020)")
    parser.add_argument("--sample-size", type=int, default=50,
                       help="Number of stocks to sample (default: 50)")
    parser.add_argument("--min-market-cap", type=float, default=1000,
                       help="Minimum market cap in millions USD (default: 1000)")
    parser.add_argument("--min-volume", type=int, default=100000,
                       help="Minimum average daily volume (default: 100,000)")
    parser.add_argument("--min-trading-days", type=int, default=200,
                       help="Minimum trading days in sample year (default: 200)")
    parser.add_argument("--seed", type=int, default=42,
                       help="Random seed for reproducible sampling")
    parser.add_argument("--report-file", help="Output file for selection report")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create historical universe creator
    creator = HistoricalUniverseCreator()
    
    # Create universe
    universe_id = await creator.create_historical_sample_universe(
        universe_name=args.universe_name,
        sample_year=args.sample_year,
        sample_size=args.sample_size,
        min_market_cap_millions=args.min_market_cap,
        min_avg_volume=args.min_volume,
        min_trading_days=args.min_trading_days,
        seed=args.seed
    )
    
    print(f"Created historical universe '{args.universe_name}' with ID: {universe_id}")
    
    # Generate report if requested
    if args.report_file:
        stocks = await creator.get_active_stocks_in_year(
            args.sample_year, args.min_market_cap, args.min_volume, args.min_trading_days
        )
        report = await creator.generate_historical_report(
            stocks[:args.sample_size], args.sample_year, args.report_file
        )
        print(f"Report saved to: {args.report_file}")


if __name__ == "__main__":
    asyncio.run(main())