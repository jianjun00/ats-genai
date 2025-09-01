"""
Data Complete Universe Creator

Creates a universe with instruments that have complete 5-year daily and 1-minute data.
This ensures we have high-quality data for backtesting and model training.
"""

import asyncio
import asyncpg
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Set, Optional
from dataclasses import dataclass
from config.environment import Environment
import gin

@dataclass
class DataCompleteness:
    """Tracks data completeness metrics for an instrument"""
    symbol: str
    instrument_id: Optional[int]
    daily_start_date: Optional[date]
    daily_end_date: Optional[date]
    daily_count: int
    minute_start_date: Optional[datetime]
    minute_end_date: Optional[datetime]
    minute_count: int
    minute_trading_days: int
    expected_daily_count: int
    expected_minute_count: int
    daily_completeness_ratio: float
    minute_completeness_ratio: float
    overall_quality_score: float

@gin.configurable
class DataCompleteUniverseCreator:
    """
    Creates and manages universes based on data completeness criteria.
    
    This class analyzes existing market data to identify instruments with
    sufficient historical data quality for reliable backtesting and modeling.
    """
    
    def __init__(self, env: Environment = None):
        self.env = env or Environment()
        self.logger = logging.getLogger(__name__)
        self.min_years = 5
        self.min_daily_completeness = 0.95  # 95% of trading days
        self.min_minute_completeness = 0.85  # 85% of expected minute bars
        self.min_overall_quality = 0.80     # Combined quality score
        
    async def analyze_data_completeness(self) -> List[DataCompleteness]:
        """
        Analyze data completeness for all instruments across daily and minute data.
        
        Returns:
            List of DataCompleteness objects with quality metrics
        """
        self.logger.info("Starting data completeness analysis...")
        
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                # Get all symbols that have both daily and minute data
                symbols_with_data = await self._get_symbols_with_both_datasets(conn)
                self.logger.info(f"Found {len(symbols_with_data)} symbols with both daily and minute data")
                
                completeness_results = []
                for symbol in symbols_with_data:
                    completeness = await self._analyze_symbol_completeness(conn, symbol)
                    if completeness:
                        completeness_results.append(completeness)
                
                return completeness_results
        finally:
            await pool.close()
    
    async def _get_symbols_with_both_datasets(self, conn) -> Set[str]:
        """Get symbols that exist in both daily and minute data tables"""
        
        # Get symbols from daily data (checking multiple sources)
        daily_symbols_query = """
        SELECT DISTINCT symbol FROM (
            SELECT symbol FROM daily_prices_polygon 
            WHERE date >= $1
            UNION 
            SELECT symbol FROM daily_prices_tiingo 
            WHERE date >= $1
            UNION
            SELECT symbol FROM daily_prices
            WHERE date >= $1
        ) daily_symbols
        """
        
        # Get symbols from minute data
        minute_symbols_query = f"""
        SELECT DISTINCT symbol 
        FROM {self.env.get_table_name('minute_bars')}
        WHERE timestamp >= $1
        """
        
        cutoff_date = date.today() - timedelta(days=self.min_years * 365 + 100)  # Add buffer
        
        daily_symbols = set()
        minute_symbols = set()
        
        try:
            # Get daily symbols
            daily_rows = await conn.fetch(daily_symbols_query, cutoff_date)
            daily_symbols = {row['symbol'] for row in daily_rows}
            
            # Get minute symbols
            minute_rows = await conn.fetch(minute_symbols_query, cutoff_date)
            minute_symbols = {row['symbol'] for row in minute_rows}
            
        except Exception as e:
            self.logger.warning(f"Error querying symbols: {e}")
            return set()
        
        # Return intersection
        common_symbols = daily_symbols.intersection(minute_symbols)
        self.logger.info(f"Daily symbols: {len(daily_symbols)}, Minute symbols: {len(minute_symbols)}, Common: {len(common_symbols)}")
        
        return common_symbols
    
    async def _analyze_symbol_completeness(self, conn, symbol: str) -> Optional[DataCompleteness]:
        """Analyze data completeness for a single symbol"""
        try:
            # Get instrument_id if available
            instrument_id = await self._get_instrument_id(conn, symbol)
            
            # Analyze daily data completeness
            daily_stats = await self._analyze_daily_completeness(conn, symbol)
            
            # Analyze minute data completeness
            minute_stats = await self._analyze_minute_completeness(conn, symbol)
            
            if not daily_stats or not minute_stats:
                return None
            
            # Calculate expected counts and completeness ratios
            expected_daily = self._calculate_expected_trading_days(
                daily_stats['start_date'], daily_stats['end_date']
            )
            
            expected_minute = self._calculate_expected_minute_bars(
                minute_stats['trading_days']
            )
            
            daily_completeness = daily_stats['count'] / max(expected_daily, 1)
            minute_completeness = minute_stats['count'] / max(expected_minute, 1)
            
            # Calculate overall quality score
            quality_score = self._calculate_quality_score(
                daily_completeness, minute_completeness, 
                daily_stats['count'], minute_stats['count']
            )
            
            return DataCompleteness(
                symbol=symbol,
                instrument_id=instrument_id,
                daily_start_date=daily_stats['start_date'],
                daily_end_date=daily_stats['end_date'],
                daily_count=daily_stats['count'],
                minute_start_date=minute_stats['start_datetime'],
                minute_end_date=minute_stats['end_datetime'],
                minute_count=minute_stats['count'],
                minute_trading_days=minute_stats['trading_days'],
                expected_daily_count=expected_daily,
                expected_minute_count=expected_minute,
                daily_completeness_ratio=daily_completeness,
                minute_completeness_ratio=minute_completeness,
                overall_quality_score=quality_score
            )
            
        except Exception as e:
            self.logger.warning(f"Error analyzing symbol {symbol}: {e}")
            return None
    
    async def _get_instrument_id(self, conn, symbol: str) -> Optional[int]:
        """Get instrument_id for a symbol"""
        query = f"""
        SELECT id FROM {self.env.get_table_name('instruments')} 
        WHERE symbol = $1 LIMIT 1
        """
        try:
            row = await conn.fetchrow(query, symbol)
            return row['id'] if row else None
        except:
            return None
    
    async def _analyze_daily_completeness(self, conn, symbol: str) -> Optional[Dict]:
        """Analyze daily data completeness for a symbol"""
        
        # Try multiple daily data sources
        queries = [
            f"SELECT MIN(date) as start_date, MAX(date) as end_date, COUNT(*) as count FROM {self.env.get_table_name('daily_prices_polygon')} WHERE symbol = $1",
            f"SELECT MIN(date) as start_date, MAX(date) as end_date, COUNT(*) as count FROM {self.env.get_table_name('daily_prices_tiingo')} WHERE symbol = $1",
            f"SELECT MIN(date) as start_date, MAX(date) as end_date, COUNT(*) as count FROM {self.env.get_table_name('daily_prices')} WHERE symbol = $1"
        ]
        
        best_result = None
        max_count = 0
        
        for query in queries:
            try:
                row = await conn.fetchrow(query, symbol)
                if row and row['count'] and row['count'] > max_count:
                    max_count = row['count']
                    best_result = {
                        'start_date': row['start_date'],
                        'end_date': row['end_date'],
                        'count': row['count']
                    }
            except Exception:
                continue
        
        return best_result
    
    async def _analyze_minute_completeness(self, conn, symbol: str) -> Optional[Dict]:
        """Analyze minute data completeness for a symbol"""
        
        query = f"""
        SELECT 
            MIN(timestamp) as start_datetime,
            MAX(timestamp) as end_datetime,
            COUNT(*) as count,
            COUNT(DISTINCT DATE(timestamp)) as trading_days
        FROM {self.env.get_table_name('minute_bars')}
        WHERE symbol = $1
        """
        
        try:
            row = await conn.fetchrow(query, symbol)
            if row and row['count']:
                return {
                    'start_datetime': row['start_datetime'],
                    'end_datetime': row['end_datetime'],
                    'count': row['count'],
                    'trading_days': row['trading_days']
                }
        except Exception as e:
            self.logger.warning(f"Error analyzing minute data for {symbol}: {e}")
        
        return None
    
    def _calculate_expected_trading_days(self, start_date: date, end_date: date) -> int:
        """Calculate expected number of trading days between two dates"""
        if not start_date or not end_date:
            return 0
        
        total_days = (end_date - start_date).days + 1
        # Rough approximation: ~252 trading days per year, ~70% of calendar days
        expected_trading_days = int(total_days * 0.70)
        return max(expected_trading_days, 1)
    
    def _calculate_expected_minute_bars(self, trading_days: int) -> int:
        """Calculate expected number of minute bars for given trading days"""
        # US market: 6.5 hours * 60 minutes = 390 minutes per trading day
        minutes_per_trading_day = 390
        return trading_days * minutes_per_trading_day
    
    def _calculate_quality_score(self, daily_completeness: float, minute_completeness: float, 
                               daily_count: int, minute_count: int) -> float:
        """Calculate overall data quality score"""
        
        # Base score from completeness ratios
        base_score = (daily_completeness * 0.3 + minute_completeness * 0.7)
        
        # Bonus for having substantial data
        data_volume_bonus = 0
        if daily_count > 1000:  # ~4 years of daily data
            data_volume_bonus += 0.05
        if minute_count > 500000:  # ~3+ years of minute data
            data_volume_bonus += 0.05
        
        return min(base_score + data_volume_bonus, 1.0)
    
    async def create_data_complete_universe(self, universe_name: str = "data_complete_5y") -> int:
        """
        Create a new universe with instruments that meet data completeness criteria.
        
        Args:
            universe_name: Name for the new universe
            
        Returns:
            Universe ID of the created universe
        """
        self.logger.info(f"Creating data complete universe: {universe_name}")
        
        # Analyze data completeness
        completeness_results = await self.analyze_data_completeness()
        
        # Filter for high-quality instruments
        qualified_instruments = self._filter_qualified_instruments(completeness_results)
        
        self.logger.info(f"Found {len(qualified_instruments)} qualified instruments")
        
        # Create universe and populate membership
        universe_id = await self._create_universe_with_members(universe_name, qualified_instruments)
        
        return universe_id
    
    def _filter_qualified_instruments(self, completeness_results: List[DataCompleteness]) -> List[DataCompleteness]:
        """Filter instruments that meet quality criteria"""
        
        qualified = []
        for result in completeness_results:
            # Check minimum data age (5 years)
            has_sufficient_history = (
                result.daily_start_date and 
                result.daily_start_date <= date.today() - timedelta(days=self.min_years * 365)
            )
            
            # Check completeness ratios
            meets_daily_threshold = result.daily_completeness_ratio >= self.min_daily_completeness
            meets_minute_threshold = result.minute_completeness_ratio >= self.min_minute_completeness
            meets_quality_threshold = result.overall_quality_score >= self.min_overall_quality
            
            if (has_sufficient_history and meets_daily_threshold and 
                meets_minute_threshold and meets_quality_threshold):
                qualified.append(result)
        
        # Sort by quality score descending
        qualified.sort(key=lambda x: x.overall_quality_score, reverse=True)
        
        return qualified
    
    async def _create_universe_with_members(self, universe_name: str, 
                                          qualified_instruments: List[DataCompleteness]) -> int:
        """Create universe and populate with qualified members"""
        
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                # Create universe
                universe_id = await self._create_universe(conn, universe_name)
                
                # Add members
                await self._add_universe_members(conn, universe_id, qualified_instruments)
                
                return universe_id
        finally:
            await pool.close()
    
    async def _create_universe(self, conn, universe_name: str) -> int:
        """Create a new universe"""
        
        description = f"Universe with instruments having complete 5-year daily and 1-minute data (created {date.today()})"
        
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
    
    async def _add_universe_members(self, conn, universe_id: int, 
                                  qualified_instruments: List[DataCompleteness]) -> None:
        """Add qualified instruments to universe membership"""
        
        membership_table = self.env.get_table_name('universe_membership')
        
        for instrument in qualified_instruments:
            # Use earliest start date as membership start
            start_date = min(
                instrument.daily_start_date or date.today(),
                instrument.minute_start_date.date() if instrument.minute_start_date else date.today()
            )
            
            query = f"""
            INSERT INTO {membership_table} (universe_id, symbol, start_at, end_at)
            VALUES ($1, $2, $3, NULL)
            ON CONFLICT (universe_id, symbol, start_at) DO NOTHING
            """
            
            await conn.execute(query, universe_id, instrument.symbol, start_date)
        
        self.logger.info(f"Added {len(qualified_instruments)} members to universe {universe_id}")
    
    async def generate_quality_report(self, output_file: str = None) -> str:
        """Generate a comprehensive data quality report"""
        
        completeness_results = await self.analyze_data_completeness()
        qualified = self._filter_qualified_instruments(completeness_results)
        
        report_lines = [
            "# Data Completeness Analysis Report",
            f"Generated: {datetime.now().isoformat()}",
            "",
            "## Summary",
            f"- Total symbols analyzed: {len(completeness_results)}",
            f"- Qualified instruments: {len(qualified)}",
            f"- Qualification rate: {len(qualified)/max(len(completeness_results), 1)*100:.1f}%",
            "",
            "## Quality Criteria",
            f"- Minimum history: {self.min_years} years",
            f"- Daily completeness: {self.min_daily_completeness*100:.0f}%",
            f"- Minute completeness: {self.min_minute_completeness*100:.0f}%",
            f"- Overall quality: {self.min_overall_quality*100:.0f}%",
            "",
            "## Top Qualified Instruments",
            "| Symbol | Daily Complete | Minute Complete | Quality Score | Daily Count | Minute Count |",
            "|--------|----------------|-----------------|---------------|-------------|--------------|"
        ]
        
        # Add top 20 qualified instruments
        for instrument in qualified[:20]:
            report_lines.append(
                f"| {instrument.symbol} | "
                f"{instrument.daily_completeness_ratio*100:.1f}% | "
                f"{instrument.minute_completeness_ratio*100:.1f}% | "
                f"{instrument.overall_quality_score:.3f} | "
                f"{instrument.daily_count:,} | "
                f"{instrument.minute_count:,} |"
            )
        
        report = "\n".join(report_lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report)
            self.logger.info(f"Quality report saved to {output_file}")
        
        return report


async def main():
    """Main function to create data complete universe"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Create universe with complete 5-year data")
    parser.add_argument("--universe-name", default="data_complete_5y", 
                       help="Name for the new universe")
    parser.add_argument("--report-only", action="store_true",
                       help="Only generate quality report, don't create universe")
    parser.add_argument("--report-file", help="Output file for quality report")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create universe creator
    creator = DataCompleteUniverseCreator()
    
    if args.report_only:
        # Generate quality report only
        report = await creator.generate_quality_report(args.report_file)
        print(report)
    else:
        # Create universe and generate report
        universe_id = await creator.create_data_complete_universe(args.universe_name)
        print(f"Created universe '{args.universe_name}' with ID: {universe_id}")
        
        # Also generate report
        report = await creator.generate_quality_report(args.report_file)
        print("\n" + report)


if __name__ == "__main__":
    asyncio.run(main())