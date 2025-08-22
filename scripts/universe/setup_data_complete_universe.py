#!/usr/bin/env python3
"""
Setup Data Complete Universe

Script to create and validate a universe with instruments that have complete
5-year daily and 1-minute data coverage for reliable backtesting and modeling.
"""

import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Add src to path to import our modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from config.environment import Environment
from universe.data_complete_universe_creator import DataCompleteUniverseCreator
from universe.data_quality_validator import DataQualityValidator

async def setup_universe(universe_name: str, validate: bool = True, 
                        report_dir: str = None) -> None:
    """
    Setup complete data universe with validation
    
    Args:
        universe_name: Name for the universe
        validate: Whether to run validation after creation
        report_dir: Directory to save reports
    """
    logger = logging.getLogger(__name__)
    
    # Setup report directory
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    logger.info(f"Setting up data complete universe: {universe_name}")
    
    # Step 1: Create universe with data completeness analysis
    creator = DataCompleteUniverseCreator()
    
    logger.info("Step 1: Analyzing data completeness...")
    if report_dir:
        quality_report_file = f"{report_dir}/data_quality_analysis_{timestamp}.md"
        await creator.generate_quality_report(quality_report_file)
        logger.info(f"Data quality analysis saved to: {quality_report_file}")
    
    logger.info("Step 2: Creating universe...")
    universe_id = await creator.create_data_complete_universe(universe_name)
    logger.info(f"Created universe '{universe_name}' with ID: {universe_id}")
    
    # Step 3: Validate universe quality
    if validate:
        logger.info("Step 3: Validating universe data quality...")
        validator = DataQualityValidator()
        validation_results = await validator.validate_universe_quality(universe_id)
        
        validation_report = validator.generate_validation_report(validation_results)
        
        if report_dir:
            validation_report_file = f"{report_dir}/validation_report_{timestamp}.md"
            with open(validation_report_file, 'w') as f:
                f.write(validation_report)
            logger.info(f"Validation report saved to: {validation_report_file}")
        else:
            print("\n" + "="*60)
            print("VALIDATION REPORT")
            print("="*60)
            print(validation_report)
    
    logger.info(f"Universe setup complete! Universe ID: {universe_id}")
    
    return universe_id

async def list_universes() -> None:
    """List all available universes"""
    import asyncpg
    
    env = Environment()
    pool = await asyncpg.create_pool(env.get_database_url())
    
    try:
        async with pool.acquire() as conn:
            query = f"""
            SELECT 
                u.id,
                u.name,
                u.description,
                COUNT(um.symbol) as member_count,
                MIN(um.start_at) as earliest_membership,
                MAX(um.start_at) as latest_membership
            FROM {env.get_table_name('universe')} u
            LEFT JOIN {env.get_table_name('universe_membership')} um ON u.id = um.universe_id
            GROUP BY u.id, u.name, u.description
            ORDER BY u.id
            """
            
            rows = await conn.fetch(query)
            
            print("\nAvailable Universes:")
            print("="*80)
            print(f"{'ID':<4} {'Name':<25} {'Members':<8} {'Description':<30}")
            print("-"*80)
            
            for row in rows:
                print(f"{row['id']:<4} {row['name']:<25} {row['member_count'] or 0:<8} {row['description'] or 'N/A':<30}")
            
            print("-"*80)
            
    finally:
        await pool.close()

async def show_universe_members(universe_id: int, limit: int = 50) -> None:
    """Show members of a specific universe"""
    import asyncpg
    
    env = Environment()
    pool = await asyncpg.create_pool(env.get_database_url())
    
    try:
        async with pool.acquire() as conn:
            # Get universe info
            universe_query = f"""
            SELECT name, description 
            FROM {env.get_table_name('universe')} 
            WHERE id = $1
            """
            universe_row = await conn.fetchrow(universe_query, universe_id)
            
            if not universe_row:
                print(f"Universe with ID {universe_id} not found")
                return
            
            print(f"\nUniverse: {universe_row['name']}")
            print(f"Description: {universe_row['description']}")
            
            # Get members
            members_query = f"""
            SELECT 
                symbol,
                start_at,
                end_at,
                CASE WHEN end_at IS NULL THEN 'Active' ELSE 'Inactive' END as status
            FROM {env.get_table_name('universe_membership')}
            WHERE universe_id = $1
            ORDER BY symbol
            LIMIT $2
            """
            
            rows = await conn.fetch(members_query, universe_id, limit)
            
            print(f"\nMembers (showing up to {limit}):")
            print("="*60)
            print(f"{'Symbol':<10} {'Start Date':<12} {'End Date':<12} {'Status':<8}")
            print("-"*60)
            
            for row in rows:
                end_date = row['end_at'].strftime('%Y-%m-%d') if row['end_at'] else 'N/A'
                print(f"{row['symbol']:<10} {row['start_at'].strftime('%Y-%m-%d'):<12} {end_date:<12} {row['status']:<8}")
            
            print("-"*60)
            print(f"Total members shown: {len(rows)}")
            
    finally:
        await pool.close()

async def validate_existing_universe(universe_id: int, report_file: str = None) -> None:
    """Validate an existing universe"""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Validating universe {universe_id}...")
    
    validator = DataQualityValidator()
    results = await validator.validate_universe_quality(universe_id)
    
    report = validator.generate_validation_report(results)
    
    if report_file:
        with open(report_file, 'w') as f:
            f.write(report)
        logger.info(f"Validation report saved to: {report_file}")
    else:
        print("\n" + "="*60)
        print("VALIDATION REPORT")
        print("="*60)
        print(report)

def main():
    parser = argparse.ArgumentParser(description="Setup and manage data complete universes")
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Create universe command
    create_parser = subparsers.add_parser('create', help='Create new data complete universe')
    create_parser.add_argument('--name', default='data_complete_5y',
                              help='Universe name (default: data_complete_5y)')
    create_parser.add_argument('--no-validate', action='store_true',
                              help='Skip validation after creation')
    create_parser.add_argument('--report-dir', 
                              help='Directory to save reports (default: current directory)')
    
    # List universes command
    list_parser = subparsers.add_parser('list', help='List all universes')
    
    # Show universe members command
    show_parser = subparsers.add_parser('show', help='Show universe members')
    show_parser.add_argument('universe_id', type=int, help='Universe ID to show')
    show_parser.add_argument('--limit', type=int, default=50, help='Limit number of members shown')
    
    # Validate universe command
    validate_parser = subparsers.add_parser('validate', help='Validate existing universe')
    validate_parser.add_argument('universe_id', type=int, help='Universe ID to validate')
    validate_parser.add_argument('--report-file', help='Output file for validation report')
    
    # Generate quality report command
    quality_parser = subparsers.add_parser('quality-report', help='Generate data quality analysis')
    quality_parser.add_argument('--output-file', help='Output file for quality report')
    
    # Common arguments
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--env', default='dev', help='Environment (dev/test/prod)')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set environment
    os.environ['ENVIRONMENT'] = args.env
    
    try:
        if args.command == 'create':
            asyncio.run(setup_universe(
                universe_name=args.name,
                validate=not args.no_validate,
                report_dir=args.report_dir
            ))
            
        elif args.command == 'list':
            asyncio.run(list_universes())
            
        elif args.command == 'show':
            asyncio.run(show_universe_members(args.universe_id, args.limit))
            
        elif args.command == 'validate':
            asyncio.run(validate_existing_universe(args.universe_id, args.report_file))
            
        elif args.command == 'quality-report':
            async def generate_report():
                creator = DataCompleteUniverseCreator()
                report = await creator.generate_quality_report(args.output_file)
                if not args.output_file:
                    print(report)
            
            asyncio.run(generate_report())
            
        else:
            parser.print_help()
            
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