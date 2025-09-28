#!/usr/bin/env python3
"""
FirstRate Minute Bar Validation - 90 Days
Focused validation for FirstRate data only
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Set
from pathlib import Path
import pandas as pd
from collections import defaultdict
import glob

# Add src to path
sys.path.insert(0, 'src')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def get_trading_days(start_date: date, end_date: date) -> List[date]:
    """Get list of trading days (excluding weekends)."""
    trading_days = []
    current = start_date
    while current <= end_date:
        # Skip weekends (Saturday = 5, Sunday = 6)
        if current.weekday() < 5:
            trading_days.append(current)
        current += timedelta(days=1)
    return trading_days

def get_all_firstrate_instruments() -> Set[str]:
    """Get all available FirstRate instruments from file structure."""
    instruments = set()
    base_path = Path('/mnt/d/ats-data/minute-bars/firstrate')
    
    if not base_path.exists():
        logger.error(f"FirstRate data path not found: {base_path}")
        return instruments
    
    # Scan all first-letter directories
    for letter_dir in base_path.iterdir():
        if not letter_dir.is_dir() or letter_dir.name in ['2', '2000', '2020']:
            continue
            
        # Scan symbol directories under each letter
        for symbol_dir in letter_dir.iterdir():
            if symbol_dir.is_dir():
                instruments.add(symbol_dir.name)
    
    return instruments

def validate_firstrate_coverage(days_back: int = 90) -> Dict:
    """Validate FirstRate minute bar coverage."""
    logger.info(f"🔍 FIRSTRATE VALIDATION: Past {days_back} days")
    logger.info("="*60)
    
    # Calculate date range
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    trading_days = get_trading_days(start_date, end_date)
    
    logger.info(f"📅 Date range: {start_date} to {end_date}")
    logger.info(f"📊 Trading days: {len(trading_days)}")
    
    # Get all instruments
    logger.info("🔍 Scanning FirstRate instrument universe...")
    all_instruments = get_all_firstrate_instruments()
    logger.info(f"📋 Found {len(all_instruments)} FirstRate instruments")
    
    # Sample some instruments to show
    sample_instruments = sorted(list(all_instruments))[:10]
    logger.info(f"📋 Sample instruments: {sample_instruments}")
    
    # Analyze file coverage by month
    base_path = Path('/mnt/d/ats-data/minute-bars/firstrate')
    months_in_range = set()
    
    current_month = start_date.replace(day=1)
    end_month = end_date.replace(day=1)
    
    while current_month <= end_month:
        months_in_range.add((current_month.year, current_month.month))
        if current_month.month == 12:
            current_month = current_month.replace(year=current_month.year + 1, month=1)
        else:
            current_month = current_month.replace(month=current_month.month + 1)
    
    logger.info(f"📅 Months in analysis: {sorted(months_in_range)}")
    
    # Track coverage statistics
    instruments_with_files = set()
    instruments_missing_months = defaultdict(list)
    file_stats = {
        'total_files_expected': 0,
        'total_files_found': 0,
        'files_by_month': defaultdict(int),
        'instruments_by_month': defaultdict(set)
    }
    
    # Check each instrument for each month
    logger.info("🔍 Analyzing file coverage...")
    for i, instrument in enumerate(sorted(all_instruments)):
        if i % 500 == 0:
            logger.info(f"  Processed {i}/{len(all_instruments)} instruments...")
        
        first_letter = instrument[0]
        instrument_path = base_path / first_letter / instrument / "2025"
        
        if not instrument_path.exists():
            # Missing entire year directory
            for year, month in months_in_range:
                if year == 2025:
                    instruments_missing_months[instrument].append(f"{year}-{month:02d}")
                    file_stats['total_files_expected'] += 1
            continue
        
        has_any_files = False
        for year, month in months_in_range:
            if year == 2025:  # Only checking 2025 for now
                month_path = instrument_path / f"{month:02d}"
                file_path = month_path / f"{instrument}_2025_{month:02d}.parquet"
                
                file_stats['total_files_expected'] += 1
                
                if file_path.exists():
                    file_stats['total_files_found'] += 1
                    file_stats['files_by_month'][(year, month)] += 1
                    file_stats['instruments_by_month'][(year, month)].add(instrument)
                    has_any_files = True
                else:
                    instruments_missing_months[instrument].append(f"{year}-{month:02d}")
        
        if has_any_files:
            instruments_with_files.add(instrument)
    
    # Calculate summary statistics
    instruments_with_complete_coverage = len(all_instruments) - len(instruments_missing_months)
    coverage_rate = (file_stats['total_files_found'] / file_stats['total_files_expected'] * 100) if file_stats['total_files_expected'] > 0 else 0
    
    # Results summary
    results = {
        'total_instruments': len(all_instruments),
        'instruments_with_files': len(instruments_with_files),
        'instruments_missing_data': len(instruments_missing_months),
        'complete_coverage_instruments': instruments_with_complete_coverage,
        'total_files_expected': file_stats['total_files_expected'],
        'total_files_found': file_stats['total_files_found'],
        'coverage_rate': coverage_rate,
        'trading_days': len(trading_days),
        'months_analyzed': len(months_in_range)
    }
    
    # Print detailed results
    logger.info("="*60)
    logger.info("📊 FIRSTRATE VALIDATION RESULTS")
    logger.info("="*60)
    logger.info(f"📋 Total instruments: {results['total_instruments']:,}")
    logger.info(f"✅ Instruments with files: {results['instruments_with_files']:,}")
    logger.info(f"❌ Instruments missing data: {results['instruments_missing_data']:,}")
    logger.info(f"🎯 Complete coverage: {results['complete_coverage_instruments']:,}")
    logger.info(f"📄 Files found: {results['total_files_found']:,}/{results['total_files_expected']:,}")
    logger.info(f"📈 Coverage rate: {results['coverage_rate']:.1f}%")
    
    # Show monthly breakdown
    logger.info("\n📅 MONTHLY BREAKDOWN:")
    for (year, month), count in sorted(file_stats['files_by_month'].items()):
        instruments_count = len(file_stats['instruments_by_month'][(year, month)])
        logger.info(f"  {year}-{month:02d}: {count:,} files, {instruments_count:,} instruments")
    
    # Show some examples of missing data
    if instruments_missing_months:
        logger.info("\n❌ EXAMPLES OF MISSING DATA:")
        sample_missing = dict(list(instruments_missing_months.items())[:5])
        for instrument, missing_months in sample_missing.items():
            logger.info(f"  {instrument}: Missing {len(missing_months)} months - {missing_months[:3]}{'...' if len(missing_months) > 3 else ''}")
    
    # Show recent updates (files modified in last 24 hours)
    logger.info("\n🔄 RECENT UPDATES (Last 24 hours):")
    recent_updates = 0
    cutoff_time = datetime.now() - timedelta(hours=24)
    
    for letter_dir in base_path.iterdir():
        if not letter_dir.is_dir() or letter_dir.name in ['2', '2000', '2020']:
            continue
        
        for symbol_dir in letter_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
                
            year_dir = symbol_dir / "2025"
            if not year_dir.exists():
                continue
                
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir():
                    continue
                    
                parquet_files = list(month_dir.glob("*.parquet"))
                for file_path in parquet_files:
                    mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if mod_time > cutoff_time:
                        recent_updates += 1
                        if recent_updates <= 10:  # Show first 10
                            logger.info(f"  ✅ {file_path.name} (updated: {mod_time.strftime('%Y-%m-%d %H:%M')})")
    
    if recent_updates > 10:
        logger.info(f"  ... and {recent_updates - 10} more files updated recently")
    elif recent_updates == 0:
        logger.info("  No files updated in the last 24 hours")
    
    logger.info(f"\n🔄 Total recent updates: {recent_updates:,} files")
    
    return results

if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("🚀 FIRSTRATE 90-DAY VALIDATION")
    
    results = validate_firstrate_coverage(90)
    
    duration = datetime.now() - start_time
    logger.info("="*60)
    logger.info("🏁 VALIDATION COMPLETE")
    logger.info("="*60)
    logger.info(f"⏱️ Duration: {duration}")
    logger.info(f"🎯 Summary: {results['coverage_rate']:.1f}% coverage across {results['total_instruments']:,} instruments")
    
    if results['coverage_rate'] > 90:
        logger.info("✅ EXCELLENT: High coverage achieved")
    elif results['coverage_rate'] > 70:
        logger.info("⚠️ GOOD: Moderate coverage, some gaps")
    else:
        logger.info("❌ POOR: Significant coverage gaps need attention")
        
    except Exception as e:
        logger.error(f"💥 Validation failed: {e}")
        import traceback
        logger.error(traceback.format_exc())