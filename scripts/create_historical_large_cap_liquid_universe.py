#!/usr/bin/env python3
"""
Create Historical Large Cap Liquid Universe (1995-Present)

Uses DynamicModelingUniverse to build a comprehensive historical universe of large cap,
liquid stocks from 1995 to present day. This provides a realistic universe for
backtesting that accounts for:
- Entry/exit criteria based on market cap ($400M+) and trading volume ($100M+)
- Historical point-in-time membership to avoid survivorship bias
- Grace periods and re-entry restrictions for realistic universe dynamics

Usage:
    python3 scripts/create_historical_large_cap_liquid_universe.py --start-year 1995 --end-year 2025
    python3 scripts/create_historical_large_cap_liquid_universe.py --daily-updates --parallel-processing
    python3 scripts/create_historical_large_cap_liquid_universe.py --export-summary
"""

import asyncio
import asyncpg
import logging
import os
import sys
import argparse
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set
import json
import pandas as pd
from pathlib import Path
import time

# Add src to Python path
sys.path.insert(0, '/workspace/src')

from universe.dynamic_modeling_universe import DynamicModelingUniverse
from config.environment import Environment
from core.run_aware_logging import setup_run_aware_logging

logger = logging.getLogger(__name__)

class HistoricalUniverseBuilder:
    """
    Builds historical large cap liquid universe using DynamicModelingUniverse.

    Creates a complete historical universe from 1995 to present with realistic
    entry/exit dynamics based on market cap and trading volume criteria.
    """

    def __init__(self, environment: str = "dev"):
        from config.environment import EnvironmentType

        # Create simple environment for database connection
        os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'  # Skip gin config loading

        # Set environment type and database connection
        env_type_map = {
            "dev": EnvironmentType.DEV,
            "intg": EnvironmentType.INTEGRATION,
            "test": EnvironmentType.TEST,
            "prod": EnvironmentType.PRODUCTION
        }

        env_type = env_type_map.get(environment, EnvironmentType.DEV)

        # Set up database connection directly
        if environment == "dev":
            db_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"
        elif environment == "intg":
            db_url = "postgresql://postgres:intg_password@localhost:4432/intg_db"
        else:
            db_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"

        # Pass both env_type and db_url to ensure correct environment detection
        self.env = Environment(env_type=env_type, db_url=db_url)
        self.universe_builder = DynamicModelingUniverse(self.env)

        # Override universe name for historical version
        self.universe_builder.universe_name = "large_cap_liquid_historical_1995_2025"

        # Configuration for historical build
        self.start_year = 1995
        self.end_year = datetime.now().year
        self.batch_size = 30  # Days to process at once
        self.progress_checkpoint_days = 90  # Save progress every 90 days

        # Statistics tracking
        self.stats = {
            'start_time': datetime.now(),
            'total_days_processed': 0,
            'entries_processed': 0,
            'exits_processed': 0,
            'universe_size_by_year': {},
            'processing_errors': [],
            'checkpoints_saved': 0
        }

    async def create_historical_universe(
        self,
        start_year: int = 1995,
        end_year: Optional[int] = None,
        daily_processing: bool = False,
        parallel_batches: bool = False
    ) -> Dict:
        """
        Create complete historical universe from start_year to end_year.

        Args:
            start_year: Start year for universe creation (default: 1995)
            end_year: End year (default: current year)
            daily_processing: Process each day individually vs batches
            parallel_batches: Use parallel processing for batches

        Returns:
            Dictionary with universe statistics and metadata
        """

        if end_year is None:
            end_year = datetime.now().year

        self.start_year = start_year
        self.end_year = end_year

        logger.info(f"🚀 Creating historical large cap liquid universe: {start_year}-{end_year}")
        logger.info(f"📊 Criteria: Market cap >${self.universe_builder.min_market_cap_millions}M, "
                   f"Volume >${self.universe_builder.min_dollar_volume_millions}M")

        # Initialize universe builder
        await self.universe_builder.initialize()

        try:
            # Process historical data year by year
            if daily_processing:
                await self._process_daily_sequential(start_year, end_year)
            elif parallel_batches:
                await self._process_parallel_batches(start_year, end_year)
            else:
                await self._process_monthly_batches(start_year, end_year)

            # Generate final statistics
            final_stats = await self._generate_final_statistics()

            logger.info(f"✅ Historical universe creation completed!")
            logger.info(f"📊 Total days processed: {self.stats['total_days_processed']:,}")
            logger.info(f"📈 Entries processed: {self.stats['entries_processed']:,}")
            logger.info(f"📉 Exits processed: {self.stats['exits_processed']:,}")

            return final_stats

        except Exception as e:
            logger.error(f"❌ Historical universe creation failed: {e}")
            raise
        finally:
            await self.universe_builder.close()

    async def _process_monthly_batches(self, start_year: int, end_year: int):
        """Process universe updates in monthly batches for efficiency."""

        current_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)

        batch_count = 0

        while current_date <= end_date:
            # Process one month at a time
            month_end = min(
                current_date.replace(day=28) + timedelta(days=4),  # Next month
                end_date
            )
            month_end = month_end.replace(day=1) - timedelta(days=1)  # Last day of current month

            logger.info(f"📅 Processing batch {batch_count + 1}: {current_date} to {month_end}")

            try:
                # Process all trading days in the month
                trading_days = await self._get_trading_days(current_date, month_end)

                for trading_day in trading_days:
                    # Update universe for this day
                    daily_changes = await self.universe_builder.run_daily_update(trading_day)

                    # Track statistics
                    if daily_changes:
                        self.stats['entries_processed'] += len(daily_changes.get('added', []))
                        self.stats['exits_processed'] += len(daily_changes.get('removed', []))

                    self.stats['total_days_processed'] += 1

                    # Progress logging
                    if self.stats['total_days_processed'] % 100 == 0:
                        logger.info(f"📊 Progress: {self.stats['total_days_processed']:,} days processed")

                # Save checkpoint periodically
                if batch_count % 3 == 0:  # Every 3 months
                    await self._save_progress_checkpoint(current_date)

                # Track yearly statistics
                if current_date.month == 12:
                    universe_size = await self._get_universe_size_at_date(month_end)
                    self.stats['universe_size_by_year'][current_date.year] = universe_size
                    logger.info(f"📈 {current_date.year} year-end universe size: {universe_size}")

            except Exception as e:
                error_msg = f"Batch {batch_count + 1} ({current_date} to {month_end}): {str(e)}"
                self.stats['processing_errors'].append(error_msg)
                logger.error(f"❌ {error_msg}")

            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

            batch_count += 1

            # Brief pause to avoid overwhelming the system
            await asyncio.sleep(0.1)

    async def _process_daily_sequential(self, start_year: int, end_year: int):
        """Process universe updates day by day sequentially."""

        current_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)

        logger.info(f"📅 Processing daily sequential: {current_date} to {end_date}")

        day_count = 0

        while current_date <= end_date:
            # Skip weekends (basic filter - could be enhanced with trading calendar)
            if current_date.weekday() < 5:  # Monday=0, Friday=4
                try:
                    # Update universe for this trading day
                    daily_changes = await self.universe_builder.run_daily_update(current_date)

                    # Track statistics
                    if daily_changes:
                        self.stats['entries_processed'] += len(daily_changes.get('added', []))
                        self.stats['exits_processed'] += len(daily_changes.get('removed', []))

                    self.stats['total_days_processed'] += 1

                    # Progress logging
                    if day_count % 250 == 0:  # Approximately yearly
                        universe_size = await self._get_universe_size_at_date(current_date)
                        logger.info(f"📊 {current_date}: {universe_size} stocks in universe "
                                   f"({self.stats['total_days_processed']:,} days processed)")

                    # Save checkpoint periodically
                    if day_count % self.progress_checkpoint_days == 0:
                        await self._save_progress_checkpoint(current_date)

                except Exception as e:
                    error_msg = f"Day {current_date}: {str(e)}"
                    self.stats['processing_errors'].append(error_msg)
                    logger.error(f"❌ {error_msg}")

                day_count += 1

            # Move to next day
            current_date += timedelta(days=1)

    async def _process_parallel_batches(self, start_year: int, end_year: int):
        """Process universe updates using parallel batch processing."""

        # Split time period into quarters for parallel processing
        quarters = []

        for year in range(start_year, end_year + 1):
            for quarter in range(1, 5):
                if quarter == 1:
                    start_date = date(year, 1, 1)
                    end_date = date(year, 3, 31)
                elif quarter == 2:
                    start_date = date(year, 4, 1)
                    end_date = date(year, 6, 30)
                elif quarter == 3:
                    start_date = date(year, 7, 1)
                    end_date = date(year, 9, 30)
                else:  # quarter == 4
                    start_date = date(year, 10, 1)
                    end_date = date(year, 12, 31)

                quarters.append((start_date, end_date))

        logger.info(f"🔄 Processing {len(quarters)} quarters in parallel batches")

        # Process quarters in groups to avoid overwhelming the system
        batch_size = 4  # Process 4 quarters (1 year) at a time

        for i in range(0, len(quarters), batch_size):
            batch_quarters = quarters[i:i + batch_size]

            # Create tasks for parallel processing
            tasks = [
                self._process_quarter(start_date, end_date)
                for start_date, end_date in batch_quarters
            ]

            # Execute batch in parallel
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for j, result in enumerate(batch_results):
                start_date, end_date = batch_quarters[j]
                if isinstance(result, Exception):
                    error_msg = f"Quarter {start_date} to {end_date}: {str(result)}"
                    self.stats['processing_errors'].append(error_msg)
                    logger.error(f"❌ {error_msg}")
                else:
                    logger.info(f"✅ Quarter {start_date} to {end_date}: {result['days_processed']} days")

            logger.info(f"📊 Completed batch {i//batch_size + 1}/{(len(quarters)-1)//batch_size + 1}")

    async def _process_quarter(self, start_date: date, end_date: date) -> Dict:
        """Process a single quarter of data."""

        days_processed = 0
        entries = 0
        exits = 0

        current_date = start_date

        while current_date <= end_date:
            if current_date.weekday() < 5:  # Trading days only
                try:
                    daily_changes = await self.universe_builder.run_daily_update(current_date)

                    if daily_changes:
                        entries += len(daily_changes.get('added', []))
                        exits += len(daily_changes.get('removed', []))

                    days_processed += 1

                except Exception as e:
                    logger.error(f"❌ Error processing {current_date}: {e}")

            current_date += timedelta(days=1)

        return {
            'days_processed': days_processed,
            'entries': entries,
            'exits': exits,
            'start_date': start_date,
            'end_date': end_date
        }

    async def _get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """Get list of trading days between start and end date."""

        trading_days = []
        current_date = start_date

        while current_date <= end_date:
            # Simple weekday filter (could be enhanced with trading calendar)
            if current_date.weekday() < 5:
                trading_days.append(current_date)
            current_date += timedelta(days=1)

        return trading_days

    async def _get_universe_size_at_date(self, target_date: date) -> int:
        """Get universe size at a specific date."""

        try:
            query = f"""
            SELECT COUNT(*) as universe_size
            FROM {self.env.get_table_name('universe_tracking')}
            WHERE universe_name = $1
              AND entry_date <= $2
              AND (removal_date IS NULL OR removal_date > $2)
            """

            async with self.universe_builder.db_pool.acquire() as conn:
                result = await conn.fetchval(query, self.universe_builder.universe_name, target_date)
                return result or 0

        except Exception as e:
            logger.error(f"❌ Error getting universe size for {target_date}: {e}")
            return 0

    async def _save_progress_checkpoint(self, current_date: date):
        """Save progress checkpoint to file."""

        try:
            checkpoint_data = {
                'universe_name': self.universe_builder.universe_name,
                'current_date': current_date.isoformat(),
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }

            checkpoint_file = Path(f"/tmp/universe_checkpoint_{current_date.strftime('%Y%m%d')}.json")

            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2, default=str)

            self.stats['checkpoints_saved'] += 1
            logger.info(f"💾 Saved progress checkpoint: {checkpoint_file}")

        except Exception as e:
            logger.error(f"❌ Error saving checkpoint: {e}")

    async def _generate_final_statistics(self) -> Dict:
        """Generate comprehensive final statistics."""

        try:
            # Get current universe size
            current_size = await self._get_universe_size_at_date(date.today())

            # Get universe composition by year
            yearly_stats = {}
            for year in range(self.start_year, self.end_year + 1):
                year_end = date(year, 12, 31)
                if year_end <= date.today():
                    size = await self._get_universe_size_at_date(year_end)
                    yearly_stats[year] = size

            # Calculate processing duration
            duration = datetime.now() - self.stats['start_time']

            final_stats = {
                'universe_name': self.universe_builder.universe_name,
                'criteria': {
                    'min_market_cap_millions': self.universe_builder.min_market_cap_millions,
                    'min_dollar_volume_millions': self.universe_builder.min_dollar_volume_millions,
                    'lookback_days': self.universe_builder.lookback_days,
                    'grace_period_days': self.universe_builder.grace_period_days
                },
                'time_period': {
                    'start_year': self.start_year,
                    'end_year': self.end_year,
                    'start_date': f"{self.start_year}-01-01",
                    'end_date': f"{self.end_year}-12-31"
                },
                'processing_stats': {
                    'total_days_processed': self.stats['total_days_processed'],
                    'entries_processed': self.stats['entries_processed'],
                    'exits_processed': self.stats['exits_processed'],
                    'processing_errors': len(self.stats['processing_errors']),
                    'checkpoints_saved': self.stats['checkpoints_saved'],
                    'processing_duration': str(duration)
                },
                'universe_stats': {
                    'current_size': current_size,
                    'yearly_sizes': yearly_stats,
                    'avg_yearly_size': sum(yearly_stats.values()) / len(yearly_stats) if yearly_stats else 0
                },
                'completion_timestamp': datetime.now().isoformat()
            }

            return final_stats

        except Exception as e:
            logger.error(f"❌ Error generating final statistics: {e}")
            return {'error': str(e)}

async def export_universe_summary(builder: HistoricalUniverseBuilder, output_file: str):
    """Export universe composition summary."""

    try:
        # Get current universe composition
        query = f"""
        SELECT
            t.symbol,
            t.entry_date,
            t.removal_date,
            t.avg_market_cap / 1000000 as avg_market_cap_millions,
            t.avg_dollar_volume / 1000000 as avg_dollar_volume_millions,
            CASE
                WHEN t.removal_date IS NULL THEN 'Active'
                ELSE 'Removed'
            END as status
        FROM {builder.env.get_table_name('universe_tracking')} t
        WHERE t.universe_name = $1
        ORDER BY t.entry_date DESC, t.symbol
        """

        async with builder.universe_builder.db_pool.acquire() as conn:
            rows = await conn.fetch(query, builder.universe_builder.universe_name)

        # Convert to DataFrame and export
        df = pd.DataFrame(rows)

        if not df.empty:
            df.to_csv(output_file, index=False)
            logger.info(f"📊 Exported universe summary to {output_file}")
            logger.info(f"📈 Total universe history: {len(df):,} entries")
            logger.info(f"📈 Currently active: {len(df[df['status'] == 'Active']):,} stocks")
        else:
            logger.warning("⚠️ No universe data found to export")

    except Exception as e:
        logger.error(f"❌ Error exporting universe summary: {e}")

async def main():
    """Main function for historical universe creation."""

    parser = argparse.ArgumentParser(description='Create Historical Large Cap Liquid Universe')
    parser.add_argument('--start-year', type=int, default=1995, help='Start year for universe creation')
    parser.add_argument('--end-year', type=int, help='End year (default: current year)')
    parser.add_argument('--environment', choices=['dev', 'intg'], default='dev', help='Database environment')
    parser.add_argument('--daily-updates', action='store_true', help='Process each day individually')
    parser.add_argument('--parallel-processing', action='store_true', help='Use parallel batch processing')
    parser.add_argument('--export-summary', action='store_true', help='Export universe summary to CSV')
    parser.add_argument('--output-file', default='large_cap_liquid_universe_summary.csv', help='Output CSV file')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Configure logging
    log_level = "DEBUG" if args.debug else "INFO"
    setup_run_aware_logging(log_level=log_level)

    logger.info("="*80)
    logger.info("HISTORICAL LARGE CAP LIQUID UNIVERSE CREATION")
    logger.info("="*80)

    # Initialize builder
    builder = HistoricalUniverseBuilder(environment=args.environment)

    try:
        # Create historical universe
        final_stats = await builder.create_historical_universe(
            start_year=args.start_year,
            end_year=args.end_year,
            daily_processing=args.daily_updates,
            parallel_batches=args.parallel_processing
        )

        # Display final results
        logger.info("="*60)
        logger.info("UNIVERSE CREATION COMPLETED")
        logger.info("="*60)
        logger.info(f"📊 Universe: {final_stats.get('universe_name', 'Unknown')}")
        logger.info(f"📅 Period: {final_stats.get('time_period', {}).get('start_date', 'Unknown')} to "
                   f"{final_stats.get('time_period', {}).get('end_date', 'Unknown')}")
        logger.info(f"📈 Current size: {final_stats.get('universe_stats', {}).get('current_size', 0):,} stocks")
        logger.info(f"📊 Total entries: {final_stats.get('processing_stats', {}).get('entries_processed', 0):,}")
        logger.info(f"📉 Total exits: {final_stats.get('processing_stats', {}).get('exits_processed', 0):,}")
        logger.info(f"⏱️ Processing time: {final_stats.get('processing_stats', {}).get('processing_duration', 'Unknown')}")

        # Export summary if requested
        if args.export_summary:
            await export_universe_summary(builder, args.output_file)

        # Save final statistics
        stats_file = f"universe_creation_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(stats_file, 'w') as f:
            json.dump(final_stats, f, indent=2, default=str)
        logger.info(f"💾 Final statistics saved to {stats_file}")

    except KeyboardInterrupt:
        logger.info("📤 Received keyboard interrupt")
    except Exception as e:
        logger.error(f"❌ Universe creation failed: {e}")
        raise
    finally:
        logger.info("✅ Historical universe creation process completed")

if __name__ == "__main__":
    asyncio.run(main())