#!/usr/bin/env python3
"""
FirstRate Minute Bar Coverage Report Generator

Generates comprehensive coverage reports for FirstRate minute bar data showing:
- Daily symbol counts and minute bar record counts for specified date range
- Trading day vs non-trading day analysis
- Symbol coverage by first letter
- Missing data identification and recommendations

Usage:
    # Generate full 30-day report
    PYTHONPATH=src python3 scripts/firstrate_30day_coverage_report.py

    # Custom date range (60 days)
    PYTHONPATH=src python3 scripts/firstrate_30day_coverage_report.py --start-date 2025-09-15 --end-date 2025-11-15

    # Specific symbols for 60 days
    PYTHONPATH=src python3 scripts/firstrate_30day_coverage_report.py --symbols SPY QQQ --start-date 2025-09-15 --end-date 2025-11-15

    # Export to file
    PYTHONPATH=src python3 scripts/firstrate_30day_coverage_report.py --output-file firstrate_coverage_report.json
"""

import os
import sys
import logging
import argparse
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import pandas as pd
from collections import defaultdict, Counter
from dataclasses import dataclass, asdict

# Add src to path for imports
sys.path.insert(0, '/workspace/src')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class DayCoverage:
    """Coverage data for a single day."""
    date: str
    is_trading_day: bool
    total_symbols: int
    total_records: int
    symbols_by_letter: Dict[str, int]
    files_found: List[str]
    missing_symbols: List[str]

@dataclass
class CoverageReport:
    """Complete coverage report."""
    start_date: str
    end_date: str
    total_days: int
    trading_days: int
    non_trading_days: int
    daily_coverage: List[DayCoverage]
    summary_stats: Dict[str, any]

class FirstRateCoverageAnalyzer:
    """Analyzes FirstRate minute bar coverage across date ranges."""
    
    def __init__(self, data_path: str = "/mnt/d/ats-data/minute-bars/firstrate", symbols: Optional[List[str]] = None):
        self.data_path = Path(data_path)
        self.symbols_filter = set(symbols) if symbols else None
        self.trading_calendar = self._build_trading_calendar()
        
    def _build_trading_calendar(self) -> set:
        """Build a simple trading calendar (excludes weekends)."""
        # Simple implementation - excludes weekends
        # In production, this would use a proper holiday calendar
        trading_days = set()
        start = date(2025, 1, 1)
        end = date(2025, 12, 31)
        current = start
        
        while current <= end:
            # Exclude weekends (Monday=0, Sunday=6)
            if current.weekday() < 5:  # Monday-Friday
                trading_days.add(current)
            current += timedelta(days=1)
            
        return trading_days
    
    def _is_trading_day(self, check_date: date) -> bool:
        """Check if a date is a trading day."""
        return check_date in self.trading_calendar
    
    def _get_symbols_for_date(self, check_date: date) -> Tuple[List[str], int, Dict[str, int]]:
        """Get symbols and record counts for a specific date using file metadata."""
        symbols_found = []
        total_records = 0
        symbols_by_letter = defaultdict(int)
        
        # If symbols filter is provided, only check those specific symbols
        if self.symbols_filter:
            for symbol in self.symbols_filter:
                letter = symbol[0].upper()
                symbol_dir = self.data_path / letter / symbol
                
                if symbol_dir.exists():
                    # Look for year/month structure: {symbol}/{year}/{month}/
                    year_dir = symbol_dir / str(check_date.year)
                    if year_dir.exists():
                        month_dir = year_dir / f"{check_date.month:02d}"
                        if month_dir.exists():
                            symbol_file = month_dir / f"{symbol}_{check_date.year}_{check_date.month:02d}.parquet"
                            
                            if symbol_file.exists():
                                # Get or generate metadata for this file
                                metadata = self._get_file_metadata(symbol_file)
                                if metadata:
                                    # Check if this file contains data for the specific date
                                    if metadata.get('date_range'):
                                        try:
                                            start_date = datetime.strptime(metadata['date_range']['start'], '%Y-%m-%d').date()
                                            end_date = datetime.strptime(metadata['date_range']['end'], '%Y-%m-%d').date()
                                            
                                            if start_date <= check_date <= end_date:
                                                # Count records for this symbol on this specific date
                                                symbol_daily_records = self._get_symbol_daily_records(symbol_file, check_date)
                                                if symbol_daily_records > 0:
                                                    symbols_found.append(symbol)
                                                    symbols_by_letter[letter] += 1
                                                    total_records += symbol_daily_records
                                        except:
                                            continue
        else:
            # Search through all letter directories (original logic)
            for letter_dir in self.data_path.iterdir():
                if letter_dir.is_dir() and len(letter_dir.name) == 1:
                    letter = letter_dir.name
                    
                    # Search through symbol directories within each letter
                    for symbol_dir in letter_dir.iterdir():
                        if symbol_dir.is_dir():
                            symbol = symbol_dir.name
                            
                            # Look for year/month structure: {symbol}/{year}/{month}/
                            year_dir = symbol_dir / str(check_date.year)
                            if year_dir.exists():
                                month_dir = year_dir / f"{check_date.month:02d}"
                                if month_dir.exists():
                                    symbol_file = month_dir / f"{symbol}_{check_date.year}_{check_date.month:02d}.parquet"
                                    
                                    if symbol_file.exists():
                                        # Get or generate metadata for this file
                                        metadata = self._get_file_metadata(symbol_file)
                                        if metadata:
                                            # Check if this file contains data for the specific date
                                            if metadata.get('date_range'):
                                                try:
                                                    start_date = datetime.strptime(metadata['date_range']['start'], '%Y-%m-%d').date()
                                                    end_date = datetime.strptime(metadata['date_range']['end'], '%Y-%m-%d').date()
                                                    
                                                    if start_date <= check_date <= end_date:
                                                        file_symbols = metadata.get('symbols', [])
                                                        symbols_found.extend(file_symbols)
                                                        symbols_by_letter[letter] += len(file_symbols)
                                                        
                                                        # Estimate records for this specific date
                                                        total_days = (end_date - start_date).days + 1
                                                        daily_records = metadata['records_count'] // total_days
                                                        total_records += daily_records
                                                except:
                                                    continue
        
        return symbols_found, total_records, dict(symbols_by_letter)
    
    def _get_symbol_daily_records(self, symbol_file: Path, target_date: date) -> int:
        """Get actual record count for a specific symbol on a specific date."""
        try:
            df = pd.read_parquet(symbol_file)
            if df.empty:
                return 0
                
            # Filter for the specific date
            df['date'] = pd.to_datetime(df['timestamp']).dt.date
            daily_data = df[df['date'] == target_date]
            return len(daily_data)
        except:
            return 0
    
    def _get_file_metadata(self, parquet_file):
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
    
    def _get_all_available_symbols(self) -> set:
        """Get all symbols that have any data."""
        if self.symbols_filter:
            return self.symbols_filter
            
        all_symbols = set()
        
        for letter_dir in self.data_path.iterdir():
            if letter_dir.is_dir():
                for symbol_dir in letter_dir.iterdir():
                    if symbol_dir.is_dir():
                        symbol = symbol_dir.name
                        all_symbols.add(symbol)
        
        return all_symbols
    
    def analyze_date_range(self, start_date: date, end_date: date) -> CoverageReport:
        """Analyze coverage for a date range."""
        logger.info(f"🔍 Analyzing FirstRate coverage from {start_date} to {end_date}")
        
        daily_coverage = []
        all_symbols = self._get_all_available_symbols()
        current_date = start_date
        trading_days = 0
        non_trading_days = 0
        
        while current_date <= end_date:
            logger.info(f"📅 Analyzing {current_date}")
            
            is_trading = self._is_trading_day(current_date)
            symbols_found, total_records, symbols_by_letter = self._get_symbols_for_date(current_date)
            
            # Find missing symbols (symbols that exist but have no data for this day)
            missing_symbols = list(all_symbols - set(symbols_found)) if is_trading else []
            
            # Find files that exist for this date
            files_found = []
            year_month = f"{current_date.year}/{current_date.month:02d}"
            for letter_dir in self.data_path.iterdir():
                if letter_dir.is_dir() and len(letter_dir.name) == 1:
                    month_file = letter_dir / str(current_date.year) / f"{current_date.month:02d}.parquet"
                    if month_file.exists():
                        files_found.append(str(month_file.relative_to(self.data_path)))
            
            day_coverage = DayCoverage(
                date=current_date.strftime('%Y-%m-%d'),
                is_trading_day=is_trading,
                total_symbols=len(symbols_found),
                total_records=total_records,
                symbols_by_letter=symbols_by_letter,
                files_found=files_found,
                missing_symbols=missing_symbols[:10]  # Limit to first 10 for brevity
            )
            
            daily_coverage.append(day_coverage)
            
            if is_trading:
                trading_days += 1
            else:
                non_trading_days += 1
            
            current_date += timedelta(days=1)
        
        # Calculate summary statistics
        total_symbols_per_day = [d.total_symbols for d in daily_coverage if d.is_trading_day]
        total_records_per_day = [d.total_records for d in daily_coverage if d.is_trading_day]
        
        summary_stats = {
            "avg_symbols_per_trading_day": sum(total_symbols_per_day) / len(total_symbols_per_day) if total_symbols_per_day else 0,
            "max_symbols_single_day": max(total_symbols_per_day) if total_symbols_per_day else 0,
            "min_symbols_single_day": min(total_symbols_per_day) if total_symbols_per_day else 0,
            "avg_records_per_trading_day": sum(total_records_per_day) / len(total_records_per_day) if total_records_per_day else 0,
            "total_records_all_days": sum(d.total_records for d in daily_coverage),
            "total_unique_symbols_found": len(all_symbols),
            "days_with_data": len([d for d in daily_coverage if d.total_symbols > 0]),
            "days_without_data": len([d for d in daily_coverage if d.total_symbols == 0 and d.is_trading_day])
        }
        
        return CoverageReport(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            total_days=len(daily_coverage),
            trading_days=trading_days,
            non_trading_days=non_trading_days,
            daily_coverage=daily_coverage,
            summary_stats=summary_stats
        )
    
    def print_report(self, report: CoverageReport):
        """Print a human-readable coverage report."""
        print("\n" + "="*80)
        print(f"🚀 FirstRate Minute Bar Coverage Report")
        print(f"📅 Period: {report.start_date} to {report.end_date}")
        print("="*80)
        
        print(f"\n📊 SUMMARY STATISTICS:")
        print(f"  • Total Days Analyzed: {report.total_days}")
        print(f"  • Trading Days: {report.trading_days}")
        print(f"  • Non-Trading Days: {report.non_trading_days}")
        print(f"  • Days with Data: {report.summary_stats['days_with_data']}")
        print(f"  • Days without Data (Trading): {report.summary_stats['days_without_data']}")
        print(f"  • Total Unique Symbols: {report.summary_stats['total_unique_symbols_found']:,}")
        print(f"  • Avg Symbols/Trading Day: {report.summary_stats['avg_symbols_per_trading_day']:,.1f}")
        print(f"  • Max Symbols (Single Day): {report.summary_stats['max_symbols_single_day']:,}")
        print(f"  • Min Symbols (Single Day): {report.summary_stats['min_symbols_single_day']:,}")
        print(f"  • Total Records (All Days): {report.summary_stats['total_records_all_days']:,}")
        print(f"  • Avg Records/Trading Day: {report.summary_stats['avg_records_per_trading_day']:,.0f}")
        
        print(f"\n📅 DETAILED DAILY TABLE:")
        print("="*100)
        print(f"{'Date':<12} {'Day':<4} {'Trading':<8} {'Health':<7} {'Symbols':<8} {'Records':<10} {'Files':<6} {'Size_MB':<8} {'Status'}")
        print("-" * 100)
        
        for day in report.daily_coverage:
            # Day of week
            date_obj = datetime.strptime(day.date, '%Y-%m-%d').date()
            day_name = date_obj.strftime('%a')
            
            # Trading status
            trading_flag = "YES" if day.is_trading_day else "NO"
            
            # Health indicator
            if not day.is_trading_day:
                health = "➖"
                status = "HOLIDAY"
            elif day.total_symbols > 5000:
                health = "✅"
                status = "EXCELLENT"
            elif day.total_symbols > 1000:
                health = "✅"
                status = "GOOD"
            elif day.total_symbols > 100:
                health = "⚠️"
                status = "PARTIAL"
            elif day.total_symbols > 0:
                health = "⚠️"
                status = "LIMITED"
            else:
                health = "❌"
                status = "NO DATA"
            
            # Estimate file count and size based on symbols found
            file_count = len(day.symbols_by_letter) if day.symbols_by_letter else 0
            estimated_size_mb = day.total_records * 0.0001 if day.total_records > 0 else 0  # Rough estimate
            
            print(f"{day.date:<12} {day_name:<4} {trading_flag:<8} {health:<7} {day.total_symbols:<8,} "
                  f"{day.total_records:<10,} {file_count:<6} {estimated_size_mb:<8.1f} {status}")
        
        # Coverage quality summary
        trading_days = [d for d in report.daily_coverage if d.is_trading_day]
        excellent_days = len([d for d in trading_days if d.total_symbols > 5000])
        good_days = len([d for d in trading_days if 1000 < d.total_symbols <= 5000])
        partial_days = len([d for d in trading_days if 100 < d.total_symbols <= 1000])
        limited_days = len([d for d in trading_days if 0 < d.total_symbols <= 100])
        no_data_days = len([d for d in trading_days if d.total_symbols == 0])
        
        print("\n📊 COVERAGE QUALITY BREAKDOWN:")
        print("-" * 50)
        if trading_days:
            print(f"✅ EXCELLENT (5000+ symbols): {excellent_days} days ({excellent_days/len(trading_days)*100:.1f}%)")
            print(f"✅ GOOD (1000-5000 symbols): {good_days} days ({good_days/len(trading_days)*100:.1f}%)")
            print(f"⚠️  PARTIAL (100-1000 symbols): {partial_days} days ({partial_days/len(trading_days)*100:.1f}%)")
            print(f"⚠️  LIMITED (1-100 symbols): {limited_days} days ({limited_days/len(trading_days)*100:.1f}%)")
            print(f"❌ NO DATA (0 symbols): {no_data_days} days ({no_data_days/len(trading_days)*100:.1f}%)")
        
        # Show symbols by letter for trading days with data
        print(f"\n📝 SYMBOL DISTRIBUTION BY LETTER (Recent Trading Days):")
        recent_trading_days = [d for d in report.daily_coverage if d.is_trading_day and d.total_symbols > 0][-5:]
        
        if recent_trading_days:
            all_letters = set()
            for day in recent_trading_days:
                all_letters.update(day.symbols_by_letter.keys())
            
            print(f"{'Letter':<8}", end="")
            for day in recent_trading_days:
                print(f"{day.date:<12}", end="")
            print()
            print("-" * (8 + 12 * len(recent_trading_days)))
            
            for letter in sorted(all_letters):
                print(f"{letter:<8}", end="")
                for day in recent_trading_days:
                    count = day.symbols_by_letter.get(letter, 0)
                    print(f"{count:<12,}", end="")
                print()
        
        print("\n" + "="*80)

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Generate FirstRate 30-day coverage report")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output-file", type=str, help="Output JSON file path")
    parser.add_argument("--data-path", type=str, default="/mnt/d/ats-data/minute-bars/firstrate",
                       help="Path to FirstRate minute bar data")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to analyze (e.g., --symbols SPY QQQ)")
    
    args = parser.parse_args()
    
    # Set default date range (past 30 days)
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    else:
        end_date = date.today()
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    else:
        start_date = end_date - timedelta(days=30)
    
    # Generate report
    analyzer = FirstRateCoverageAnalyzer(data_path=args.data_path, symbols=args.symbols)
    report = analyzer.analyze_date_range(start_date, end_date)
    
    # Print to console
    analyzer.print_report(report)
    
    # Save to file if requested
    if args.output_file:
        output_path = Path(args.output_file)
        with open(output_path, 'w') as f:
            json.dump(asdict(report), f, indent=2, default=str)
        logger.info(f"📁 Report saved to {output_path}")

if __name__ == "__main__":
    main()