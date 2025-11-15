#!/usr/bin/env python3
"""
FirstRate Targeted Coverage - Sample Analysis

Analyzes a targeted sample of symbols for coverage reporting.

Usage:
    python3 scripts/firstrate_targeted_coverage.py --days 30
"""

import os
import sys
import json
from datetime import datetime, date, timedelta
from pathlib import Path
import pandas as pd

def get_file_metadata(parquet_file):
    """Get or generate metadata for a parquet file."""
    metadata_file = parquet_file.parent / f"{parquet_file.name}.metadata.json"
    
    # Read existing metadata if it exists
    if metadata_file.exists():
        try:
            with open(metadata_file, 'r') as f:
                return json.load(f)
        except:
            pass
    
    # Generate new metadata
    try:
        df = pd.read_parquet(parquet_file)
        if df.empty:
            return None
            
        metadata = {
            "file_path": str(parquet_file.name),
            "created_at": datetime.now().isoformat(),
            "file_size_bytes": parquet_file.stat().st_size,
            "records_count": len(df),
            "symbols": [],
            "date_range": {"start": None, "end": None}
        }
        
        # Extract symbols and date range if columns exist
        if 'symbol' in df.columns:
            metadata['symbols'] = df['symbol'].unique().tolist()
        
        if 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp']).dt.date
            metadata['date_range']['start'] = str(df['date'].min())
            metadata['date_range']['end'] = str(df['date'].max())
        
        # Save metadata
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        return metadata
    except Exception:
        return None

def analyze_targeted_coverage(days=30, data_path="/mnt/d/ats-data/minute-bars/firstrate"):
    """Analyze coverage for key symbols only."""
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    # Discover available letters and limit to first few for performance
    data_path = Path(data_path)
    available_letters = [d.name for d in data_path.iterdir() if d.is_dir() and len(d.name) == 1][:3]  # First 3 letters only
    
    print(f"Date         Symbols    Records    Status")
    print("-" * 45)
    
    current_date = start_date
    
    while current_date <= end_date:
        is_trading = current_date.weekday() < 5
        
        if is_trading:
            symbols_found = set()
            total_records = 0
            
            # Check available letters dynamically
            for letter in available_letters:
                letter_dir = data_path / letter
                if letter_dir.exists():
                    # Dynamically discover the actual structure for this letter
                    # Check both {letter}/{year}/{month} and {letter}/{symbol}/{year}/{month} patterns
                    
                    # Pattern 1: {letter}/{year}/{month}
                    year_dir = letter_dir / str(current_date.year)
                    if year_dir.exists():
                        month_dir = year_dir / f"{current_date.month:02d}"
                        if month_dir.exists():
                            for parquet_file in month_dir.glob("*.parquet"):
                                if parquet_file.stat().st_size > 1000:
                                    metadata = get_file_metadata(parquet_file)
                                    if metadata and metadata.get('date_range'):
                                        try:
                                            start_dt = datetime.strptime(metadata['date_range']['start'], '%Y-%m-%d').date()
                                            end_dt = datetime.strptime(metadata['date_range']['end'], '%Y-%m-%d').date()
                                            
                                            if start_dt <= current_date <= end_dt:
                                                symbols_found.update(metadata.get('symbols', []))
                                                total_days = (end_dt - start_dt).days + 1
                                                daily_records = metadata['records_count'] // total_days
                                                total_records += daily_records
                                        except:
                                            continue
                    
                    # Pattern 2: {letter}/{symbol}/{year}/{month} - check first few symbol dirs
                    symbol_count = 0
                    for symbol_dir in letter_dir.iterdir():
                        if symbol_dir.is_dir() and symbol_count < 3:  # Limit to first 3 symbols
                            symbol_count += 1
                            year_dir = symbol_dir / str(current_date.year)
                            if year_dir.exists():
                                month_dir = year_dir / f"{current_date.month:02d}"
                                if month_dir.exists():
                                    for parquet_file in month_dir.glob("*.parquet"):
                                        if parquet_file.stat().st_size > 1000:
                                            metadata = get_file_metadata(parquet_file)
                                            if metadata and metadata.get('date_range'):
                                                try:
                                                    start_dt = datetime.strptime(metadata['date_range']['start'], '%Y-%m-%d').date()
                                                    end_dt = datetime.strptime(metadata['date_range']['end'], '%Y-%m-%d').date()
                                                    
                                                    if start_dt <= current_date <= end_dt:
                                                        symbols_found.update(metadata.get('symbols', []))
                                                        total_days = (end_dt - start_dt).days + 1
                                                        daily_records = metadata['records_count'] // total_days
                                                        total_records += daily_records
                                                except:
                                                    continue
            
            # Determine status
            if len(symbols_found) >= 5:
                status = "GOOD"
            elif len(symbols_found) >= 2:
                status = "PARTIAL"
            elif len(symbols_found) >= 1:
                status = "LIMITED"
            else:
                status = "NO_DATA"
            
            print(f"{current_date}   {len(symbols_found):7,}  {total_records:9,}  {status}")
            
        else:
            print(f"{current_date}         0          0  HOLIDAY")
        
        current_date += timedelta(days=1)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate targeted FirstRate coverage analysis")
    parser.add_argument("--days", type=int, default=30, help="Number of days to analyze")
    parser.add_argument("--data-path", type=str, default="/mnt/d/ats-data/minute-bars/firstrate",
                       help="Path to FirstRate data")
    
    args = parser.parse_args()
    analyze_targeted_coverage(args.days, args.data_path)

if __name__ == "__main__":
    main()