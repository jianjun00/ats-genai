#!/usr/bin/env python3
"""
Split complete FirstRate parquet files into monthly files

Converts files like:
/mnt/d/ats-data/minute-bars/firstrate/M/MSFT/MSFT_complete.parquet

Into monthly structure like:
/mnt/d/ats-data/minute-bars/firstrate/M/MSFT/2025/08/MSFT_2025_08.parquet
/mnt/d/ats-data/minute-bars/firstrate/M/MSFT/2025/09/MSFT_2025_09.parquet
"""

import pandas as pd
import os
import sys
from pathlib import Path
from datetime import datetime
import argparse

def split_complete_parquet_to_monthly(complete_file_path, output_base_dir=None, symbol=None):
    """
    Split a complete parquet file into monthly files
    
    Args:
        complete_file_path: Path to the complete parquet file
        output_base_dir: Base directory for monthly files (defaults to same as input)
        symbol: Symbol name (extracted from filename if not provided)
    """
    complete_file = Path(complete_file_path)
    
    if not complete_file.exists():
        print(f"❌ File not found: {complete_file_path}")
        return False
        
    # Extract symbol from filename if not provided
    if not symbol:
        symbol = complete_file.stem.replace('_complete', '')
    
    # Set output directory
    if not output_base_dir:
        output_base_dir = complete_file.parent
    else:
        output_base_dir = Path(output_base_dir)
    
    print(f"🔄 Processing {symbol}: {complete_file_path}")
    
    try:
        # Read the complete parquet file
        df = pd.read_parquet(complete_file_path)
        print(f"📊 Loaded {len(df):,} records from {complete_file.name}")
        
        # Ensure timestamp column exists and is datetime
        timestamp_cols = [col for col in df.columns if 'timestamp' in col.lower() or 'time' in col.lower() or 'date' in col.lower()]
        if not timestamp_cols:
            print(f"❌ No timestamp column found in {complete_file.name}")
            print(f"Available columns: {list(df.columns)}")
            return False
            
        timestamp_col = timestamp_cols[0]
        print(f"📅 Using timestamp column: {timestamp_col}")
        
        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Extract year-month combinations
        df['year'] = df[timestamp_col].dt.year
        df['month'] = df[timestamp_col].dt.month
        
        # Group by year-month and save monthly files
        monthly_groups = df.groupby(['year', 'month'])
        total_monthly_files = 0
        total_records_written = 0
        
        for (year, month), group_df in monthly_groups:
            # Create monthly directory structure
            monthly_dir = output_base_dir / str(year) / f"{month:02d}"
            monthly_dir.mkdir(parents=True, exist_ok=True)
            
            # Create monthly filename
            monthly_file = monthly_dir / f"{symbol}_{year}_{month:02d}.parquet"
            
            # Remove the temporary year/month columns before saving
            monthly_data = group_df.drop(['year', 'month'], axis=1)
            
            # Save monthly file
            monthly_data.to_parquet(monthly_file, engine='auto', index=False)
            
            record_count = len(monthly_data)
            total_monthly_files += 1
            total_records_written += record_count
            
            print(f"✅ Created {monthly_file.relative_to(output_base_dir)} ({record_count:,} records)")
        
        print(f"🎉 Split complete! Created {total_monthly_files} monthly files with {total_records_written:,} total records")
        
        # Verify record count matches
        if total_records_written == len(df):
            print(f"✅ Record count verified: {total_records_written:,} records")
            return True
        else:
            print(f"⚠️ Record count mismatch: Original {len(df):,}, Written {total_records_written:,}")
            return False
            
    except Exception as e:
        print(f"❌ Error processing {complete_file_path}: {e}")
        return False

def find_complete_parquet_files(base_dir="/mnt/d/ats-data/minute-bars/firstrate"):
    """Find all *_complete.parquet files in the firstrate directory"""
    base_path = Path(base_dir)
    complete_files = []
    
    if not base_path.exists():
        print(f"❌ Directory not found: {base_dir}")
        return complete_files
    
    # Search recursively for *_complete.parquet files
    for complete_file in base_path.rglob("*_complete.parquet"):
        complete_files.append(complete_file)
    
    return complete_files

def main():
    parser = argparse.ArgumentParser(description="Split complete FirstRate parquet files into monthly files")
    parser.add_argument('--file', type=str, help='Specific complete parquet file to process')
    parser.add_argument('--symbol', type=str, help='Symbol name (auto-detected if not provided)')
    parser.add_argument('--base-dir', type=str, default='/mnt/d/ats-data/minute-bars/firstrate', 
                       help='Base directory to search for complete parquet files')
    parser.add_argument('--list-only', action='store_true', help='Only list complete parquet files found')
    
    args = parser.parse_args()
    
    if args.file:
        # Process specific file
        success = split_complete_parquet_to_monthly(args.file, symbol=args.symbol)
        sys.exit(0 if success else 1)
    else:
        # Find and process all complete parquet files
        complete_files = find_complete_parquet_files(args.base_dir)
        
        if not complete_files:
            print(f"❌ No *_complete.parquet files found in {args.base_dir}")
            sys.exit(1)
        
        print(f"📋 Found {len(complete_files)} complete parquet files:")
        for i, file_path in enumerate(complete_files, 1):
            print(f"  {i}. {file_path}")
        
        if args.list_only:
            sys.exit(0)
        
        # Process all files
        print(f"\n🚀 Processing {len(complete_files)} complete parquet files...")
        
        successful = 0
        failed = 0
        
        for file_path in complete_files:
            if split_complete_parquet_to_monthly(str(file_path)):
                successful += 1
            else:
                failed += 1
        
        print(f"\n📊 Summary: {successful} successful, {failed} failed")
        sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()