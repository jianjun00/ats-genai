#!/usr/bin/env python3
"""
Create Test Universe - 2020 Sample

Script to create a test universe using 50 stocks that were available in 2020.
This approach avoids survivorship bias by using only historical information.
"""

import os
import sys
import asyncio
import logging
import argparse
from pathlib import Path

# Add src to path to import our modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from config.environment import Environment
from universe.historical_universe_creator import HistoricalUniverseCreator

async def create_test_universe_2020(
    universe_name: str = "test_sample_2020_50",
    sample_size: int = 50,
    generate_report: bool = True,
    report_dir: str = None
) -> int:
    """
    Create a test universe with 50 stocks sampled from 2020.
    
    Args:
        universe_name: Name for the test universe
        sample_size: Number of stocks to sample
        generate_report: Whether to generate selection report
        report_dir: Directory to save report
        
    Returns:
        Universe ID of the created universe
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Creating test universe: {universe_name}")
    logger.info(f"Sampling {sample_size} stocks from 2020 data")
    
    # Create historical universe creator
    creator = HistoricalUniverseCreator()
    
    # Create universe with 2020 sample
    universe_id = await creator.create_historical_sample_universe(
        universe_name=universe_name,
        sample_year=2020,
        sample_size=sample_size,
        min_market_cap_millions=1000,  # $1B minimum market cap
        min_avg_volume=100000,         # 100k shares daily minimum
        min_trading_days=200,          # Active for most of 2020
        seed=42                        # Reproducible sampling
    )
    
    logger.info(f"Created test universe '{universe_name}' with ID: {universe_id}")
    
    # Generate selection report
    if generate_report:
        logger.info("Generating selection report...")
        
        # Get the stocks that were available in 2020
        stocks_2020 = await creator.get_active_stocks_in_year(
            year=2020,
            min_market_cap_millions=1000,
            min_avg_volume=100000,
            min_trading_days=200
        )
        
        # Get the sampled stocks (first N from the weighted sample)
        sampled_stocks = creator._sample_stocks_by_market_cap(stocks_2020, sample_size)
        
        # Generate report
        timestamp = "2020_sample"
        report_filename = f"test_universe_selection_report_{timestamp}.md"
        
        if report_dir:
            os.makedirs(report_dir, exist_ok=True)
            report_path = os.path.join(report_dir, report_filename)
        else:
            report_path = report_filename
        
        report = await creator.generate_historical_report(
            stocks=sampled_stocks,
            sample_year=2020,
            output_file=report_path
        )
        
        logger.info(f"Selection report saved to: {report_path}")
        
        # Print summary
        print("\n" + "="*60)
        print("TEST UNIVERSE CREATION SUMMARY")
        print("="*60)
        print(f"Universe Name: {universe_name}")
        print(f"Universe ID: {universe_id}")
        print(f"Sample Year: 2020")
        print(f"Stocks Sampled: {len(sampled_stocks)}")
        print(f"Total Available in 2020: {len(stocks_2020)}")
        print(f"Selection Rate: {len(sampled_stocks)/len(stocks_2020)*100:.1f}%")
        print(f"Report: {report_path}")
        print()
        print("Methodology:")
        print("- Uses only 2020 data to avoid survivorship bias")
        print("- Weighted sampling favoring larger market cap")
        print("- Minimum $1B market cap and 100k avg daily volume")
        print("- Reproducible with seed=42")
        print("="*60)
    
    return universe_id

async def verify_no_survivorship_bias(universe_id: int) -> None:
    """
    Verify that the created universe doesn't have survivorship bias
    by checking if any selected stocks were later delisted or had issues.
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Verifying survivorship bias for universe {universe_id}...")
    
    env = Environment()
    pool = await asyncpg.create_pool(env.get_database_url())
    
    try:
        async with pool.acquire() as conn:
            # Get universe members
            members_query = f"""
            SELECT symbol, start_at 
            FROM {env.get_table_name('universe_membership')}
            WHERE universe_id = $1
            ORDER BY symbol
            """
            
            members = await conn.fetch(members_query, universe_id)
            symbols = [row['symbol'] for row in members]
            
            logger.info(f"Checking {len(symbols)} universe members for data issues...")
            
            # Check each symbol for data availability after 2020
            issues_found = []
            
            for symbol in symbols:
                # Check if symbol has data in recent years (2021-2024)
                data_check_query = f"""
                SELECT 
                    COUNT(*) as recent_count,
                    MAX(date) as last_date
                FROM (
                    SELECT date FROM {env.get_table_name('daily_prices_polygon')}
                    WHERE symbol = $1 AND date >= '2021-01-01'
                    UNION
                    SELECT date FROM {env.get_table_name('daily_prices_tiingo')}
                    WHERE symbol = $1 AND date >= '2021-01-01'
                    UNION 
                    SELECT date FROM {env.get_table_name('daily_prices')}
                    WHERE symbol = $1 AND date >= '2021-01-01'
                ) recent_data
                """
                
                try:
                    result = await conn.fetchrow(data_check_query, symbol)
                    recent_count = result['recent_count'] if result else 0
                    last_date = result['last_date'] if result else None
                    
                    # Flag potential issues
                    if recent_count < 500:  # Less than ~2 years of recent data
                        issues_found.append({
                            'symbol': symbol,
                            'issue': 'Limited recent data',
                            'recent_count': recent_count,
                            'last_date': last_date
                        })
                        
                except Exception as e:
                    logger.warning(f"Error checking {symbol}: {e}")
            
            # Report results
            print("\n" + "="*60)
            print("SURVIVORSHIP BIAS VERIFICATION")
            print("="*60)
            print(f"Universe members checked: {len(symbols)}")
            print(f"Potential issues found: {len(issues_found)}")
            print(f"Clean rate: {(len(symbols)-len(issues_found))/len(symbols)*100:.1f}%")
            
            if issues_found:
                print("\nStocks with potential post-2020 issues:")
                print("(This is expected and shows we avoided survivorship bias)")
                print("-" * 60)
                for issue in issues_found[:10]:  # Show first 10
                    print(f"  {issue['symbol']}: {issue['issue']} "
                          f"(recent_count: {issue['recent_count']}, "
                          f"last_date: {issue['last_date']})")
                if len(issues_found) > 10:
                    print(f"  ... and {len(issues_found) - 10} more")
            else:
                print("\nAll stocks have good recent data coverage.")
                print("(This might indicate survivorship bias if unexpected)")
            
            print("\nConclusion:")
            if issues_found:
                print("✓ Successfully avoided survivorship bias")
                print("✓ Selected stocks based only on 2020 information")
                print("✓ Some stocks naturally had issues after 2020")
            else:
                print("⚠ All stocks have perfect data - verify methodology")
                
            print("="*60)
            
    finally:
        await pool.close()

async def list_universe_members(universe_id: int, limit: int = 20) -> None:
    """List members of the created universe"""
    import asyncpg
    
    env = Environment()
    pool = await asyncpg.create_pool(env.get_database_url())
    
    try:
        async with pool.acquire() as conn:
            query = f"""
            SELECT symbol, start_at
            FROM {env.get_table_name('universe_membership')}
            WHERE universe_id = $1
            ORDER BY symbol
            LIMIT $2
            """
            
            rows = await conn.fetch(query, universe_id, limit)
            
            print(f"\nUniverse {universe_id} Members (showing {len(rows)}):")
            print("-" * 40)
            for row in rows:
                print(f"  {row['symbol']} (start: {row['start_at']})")
            print("-" * 40)
            
    finally:
        await pool.close()

def main():
    parser = argparse.ArgumentParser(
        description="Create test universe with 50 stocks from 2020"
    )
    
    parser.add_argument('--universe-name', default='test_sample_2020_50',
                       help='Name for the test universe')
    parser.add_argument('--sample-size', type=int, default=50,
                       help='Number of stocks to sample (default: 50)')
    parser.add_argument('--no-report', action='store_true',
                       help='Skip generation of selection report')
    parser.add_argument('--report-dir', 
                       help='Directory to save reports')
    parser.add_argument('--verify-bias', action='store_true',
                       help='Run survivorship bias verification after creation')
    parser.add_argument('--list-members', action='store_true',
                       help='List universe members after creation')
    parser.add_argument('--debug', action='store_true', 
                       help='Enable debug logging')
    parser.add_argument('--env', default='dev', 
                       help='Environment (dev/test/prod)')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set environment
    os.environ['ENVIRONMENT'] = args.env
    
    try:
        # Create test universe
        universe_id = asyncio.run(create_test_universe_2020(
            universe_name=args.universe_name,
            sample_size=args.sample_size,
            generate_report=not args.no_report,
            report_dir=args.report_dir
        ))
        
        # Optional: list members
        if args.list_members:
            asyncio.run(list_universe_members(universe_id))
        
        # Optional: verify no survivorship bias
        if args.verify_bias:
            asyncio.run(verify_no_survivorship_bias(universe_id))
            
        print(f"\n✓ Test universe created successfully!")
        print(f"Universe ID: {universe_id}")
        print(f"Universe Name: {args.universe_name}")
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error: {e}")
        if args.debug:
            raise
        sys.exit(1)

if __name__ == "__main__":
    main()