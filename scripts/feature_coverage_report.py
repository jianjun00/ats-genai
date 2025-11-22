#!/usr/bin/env python3
"""
Feature Coverage Report Generator

Generates a table view of feature coverage for given symbols showing:
- Date range coverage
- Feature group availability  
- Record counts per feature
- Production tag status

Usage:
    python3 scripts/feature_coverage_report.py --symbols TSLA AAPL --start-date 2025-07-01 --end-date 2025-11-14
"""

import argparse
import asyncio
import asyncpg
import pandas as pd
from datetime import date, datetime
from typing import List, Dict, Any
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

async def generate_feature_coverage_report(symbols: List[str], start_date: str, end_date: str, environment: str = 'intg'):
    """Generate feature coverage report for given symbols and date range."""
    
    # Database connection
    if environment == 'intg':
        db_url = 'postgresql://postgres:intg_password@localhost:4432/intg_db'
        table_prefix = 'intg'
    else:
        db_url = 'postgresql://postgres:dev_password@localhost:3432/dev_db'
        table_prefix = 'dev'
    
    conn = await asyncpg.connect(db_url)
    
    print(f'📊 Feature Coverage Report for {", ".join(symbols)}')
    print(f'📅 Date Range: {start_date} to {end_date}')
    print(f'🏷️  Environment: {environment} (production tags only)')
    print('=' * 80)
    
    try:
        # Convert date strings to year_month format for database query
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Generate list of year_months to query
        year_months = []
        current = start_dt.replace(day=1)
        while current <= end_dt:
            year_months.append(f"{current.year}_{current.month:02d}")
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        
        # Query production feature data
        query = f"""
        SELECT 
            fgf.symbol,
            fgf.year_month,
            fgf.feature_group,
            fgf.dataset_id,
            fgf.timeframes,
            fgf.file_paths,
            fgf.record_count,
            fgf.status,
            fgf.created_at
        FROM {table_prefix}_feature_group_files fgf
        WHERE fgf.symbol = ANY($1)
          AND fgf.year_month = ANY($2)
          AND fgf.status = 'prod'
        ORDER BY fgf.symbol, fgf.year_month, fgf.feature_group
        """
        
        rows = await conn.fetch(query, symbols, year_months)
        
        if not rows:
            print('❌ No production feature data found for specified symbols/dates')
            return
        
        # Process data into coverage matrix
        coverage_data = []
        for row in rows:
            # Parse file_paths JSON to count actual files
            file_paths = row['file_paths'] if row['file_paths'] else {}
            total_files = sum(len(files) for files in file_paths.values()) if isinstance(file_paths, dict) else 0
            
            coverage_data.append({
                'Symbol': row['symbol'],
                'Date': row['year_month'], 
                'Feature_Group': row['feature_group'],
                'Records': row['record_count'] or 0,
                'Files': total_files,
                'Dataset_ID': row['dataset_id'],
                'Status': row['status'],
                'Created': row['created_at'].strftime('%Y-%m-%d') if row['created_at'] else 'Unknown'
            })
        
        # Create DataFrame for easier manipulation
        df = pd.DataFrame(coverage_data)
        
        if df.empty:
            print('❌ No data to display')
            return
        
        # Print summary by symbol
        print('\n📋 Coverage Summary by Symbol:')
        print('-' * 50)
        
        for symbol in symbols:
            symbol_data = df[df['Symbol'] == symbol]
            if symbol_data.empty:
                print(f'{symbol:<8} ❌ No production data found')
                continue
                
            total_records = symbol_data['Records'].sum()
            unique_months = symbol_data['Date'].nunique()
            unique_features = symbol_data['Feature_Group'].nunique()
            
            print(f'{symbol:<8} ✅ {unique_months} months, {unique_features} feature groups, {total_records:,} total records')
        
        # Print detailed table
        print('\n📊 Detailed Coverage Table:')
        print('-' * 100)
        
        # Group by symbol for cleaner display
        for symbol in symbols:
            symbol_data = df[df['Symbol'] == symbol]
            if symbol_data.empty:
                continue
                
            print(f'\n🎯 {symbol}:')
            print(f"{'Date':<12} {'Feature Group':<20} {'Records':<10} {'Files':<6} {'Dataset ID':<20}")
            print('-' * 70)
            
            for _, row in symbol_data.iterrows():
                print(f"{row['Date']:<12} {row['Feature_Group']:<20} {row['Records']:>8,} {row['Files']:>4} {row['Dataset_ID']:<20}")
        
        # Print feature group coverage matrix
        print('\n📈 Feature Group Coverage Matrix:')
        print('-' * 80)
        
        # Create pivot table
        pivot_df = df.pivot_table(
            index='Date',
            columns='Feature_Group', 
            values='Records',
            aggfunc='sum',
            fill_value=0
        )
        
        if not pivot_df.empty:
            print(f"{'Date':<12}", end='')
            for col in pivot_df.columns:
                print(f'{col[:15]:<16}', end='')
            print()
            print('-' * (12 + 16 * len(pivot_df.columns)))
            
            for date_idx in pivot_df.index:
                print(f'{date_idx:<12}', end='')
                for col in pivot_df.columns:
                    value = pivot_df.loc[date_idx, col]
                    if value > 0:
                        print(f'{value:>8,} ✅     ', end='')
                    else:
                        print(f'{"❌":<16}', end='')
                print()
        
        # Print missing coverage warnings
        print('\n⚠️  Coverage Gaps:')
        print('-' * 40)
        
        expected_features = ['ohlcv_basic', 'technical_momentum', 'technical_volatility', 'fundamental_quarterly']
        
        gaps_found = False
        for symbol in symbols:
            symbol_data = df[df['Symbol'] == symbol]
            available_months = set(symbol_data['Date'].unique())
            expected_months = set(year_months)
            missing_months = expected_months - available_months
            
            if missing_months:
                gaps_found = True
                print(f'{symbol}: Missing data for months: {", ".join(sorted(missing_months))}')
            
            # Check feature group completeness for available months
            for month in available_months:
                month_data = symbol_data[symbol_data['Date'] == month]
                available_features = set(month_data['Feature_Group'].unique())
                missing_features = set(expected_features) - available_features
                
                if missing_features:
                    gaps_found = True
                    print(f'{symbol} {month}: Missing features: {", ".join(sorted(missing_features))}')
        
        if not gaps_found:
            print('✅ No coverage gaps detected!')
        
        print(f'\n📊 Report completed for {len(symbols)} symbols, {len(year_months)} months')
        
    finally:
        await conn.close()

def main():
    parser = argparse.ArgumentParser(
        description='Generate feature coverage report for symbols',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single symbol coverage
  python3 scripts/feature_coverage_report.py --symbols TSLA --start-date 2025-07-01 --end-date 2025-11-14
  
  # Multiple symbols 
  python3 scripts/feature_coverage_report.py --symbols TSLA AAPL MSFT --start-date 2025-07-01 --end-date 2025-11-14
  
  # Development environment
  python3 scripts/feature_coverage_report.py --symbols TSLA --start-date 2025-07-01 --end-date 2025-11-14 --environment dev
        """
    )
    
    parser.add_argument(
        '--symbols',
        nargs='+',
        required=True,
        help='Stock symbols to analyze (e.g., TSLA AAPL MSFT)'
    )
    
    parser.add_argument(
        '--start-date',
        required=True,
        help='Start date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--end-date', 
        required=True,
        help='End date in YYYY-MM-DD format'
    )
    
    parser.add_argument(
        '--environment',
        choices=['dev', 'intg'],
        default='intg',
        help='Database environment (default: intg)'
    )
    
    args = parser.parse_args()
    
    # Validate date formats
    try:
        datetime.strptime(args.start_date, '%Y-%m-%d')
        datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        print(f'❌ Invalid date format: {e}')
        return 1
    
    # Run coverage report
    try:
        asyncio.run(generate_feature_coverage_report(
            symbols=args.symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            environment=args.environment
        ))
        return 0
    except Exception as e:
        print(f'❌ Error generating coverage report: {e}')
        return 1

if __name__ == '__main__':
    exit(main())