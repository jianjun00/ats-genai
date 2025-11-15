#!/usr/bin/env python3
"""
FirstRate Quick Coverage Report Generator

Fast coverage analysis based on file structure rather than reading all parquet files.

Usage:
    PYTHONPATH=src python3 scripts/firstrate_quick_coverage_report.py --days 30
"""

import os
import sys
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from collections import defaultdict, Counter
from dataclasses import dataclass, asdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class QuickCoverage:
    """Quick coverage data for a single day."""
    date: str
    is_trading_day: bool
    symbols_with_files: int
    total_file_count: int
    file_size_mb: float
    symbols_by_letter: Dict[str, int]

class FirstRateQuickAnalyzer:
    """Fast FirstRate coverage analyzer based on file structure."""
    
    def __init__(self, data_path: str = "/mnt/d/ats-data/minute-bars/firstrate"):
        self.data_path = Path(data_path)
        self.trading_calendar = self._build_trading_calendar()
        
    def _build_trading_calendar(self) -> set:
        """Build a simple trading calendar (excludes weekends)."""
        trading_days = set()
        start = date(2025, 1, 1)
        end = date(2025, 12, 31)
        current = start
        
        while current <= end:
            if current.weekday() < 5:  # Monday-Friday
                trading_days.add(current)
            current += timedelta(days=1)
            
        return trading_days
    
    def _is_trading_day(self, check_date: date) -> bool:
        """Check if a date is a trading day."""
        return check_date in self.trading_calendar
    
    def _analyze_date_files(self, check_date: date) -> QuickCoverage:
        """Analyze file coverage for a specific date."""
        year = check_date.year
        month = check_date.month
        
        symbols_found = set()
        file_count = 0
        total_size_bytes = 0
        symbols_by_letter = defaultdict(int)
        
        # Search through letter directories
        for letter_dir in self.data_path.iterdir():
            if letter_dir.is_dir() and len(letter_dir.name) == 1:
                letter = letter_dir.name
                
                # Search through symbol directories within each letter
                for symbol_dir in letter_dir.iterdir():
                    if symbol_dir.is_dir():
                        symbol = symbol_dir.name
                        
                        # Look for year/month structure: {symbol}/{year}/{month}/
                        year_dir = symbol_dir / str(year)
                        if year_dir.exists():
                            month_dir = year_dir / f"{month:02d}"
                            if month_dir.exists():
                                # Look for parquet files in month directory
                                parquet_files = list(month_dir.glob("*.parquet"))
                                
                                for parquet_file in parquet_files:
                                    try:
                                        # Check if file exists and has reasonable size
                                        if parquet_file.exists():
                                            file_size = parquet_file.stat().st_size
                                            if file_size > 1000:  # Files larger than 1KB likely have data
                                                symbols_found.add(symbol)
                                                file_count += 1
                                                total_size_bytes += file_size
                                                symbols_by_letter[letter] += 1
                                                break  # Found data for this symbol, move to next
                                                
                                    except Exception:
                                        # Skip problematic files
                                        continue
        
        return QuickCoverage(
            date=check_date.strftime('%Y-%m-%d'),
            is_trading_day=self._is_trading_day(check_date),
            symbols_with_files=len(symbols_found),
            total_file_count=file_count,
            file_size_mb=total_size_bytes / (1024 * 1024),
            symbols_by_letter=dict(symbols_by_letter)
        )
    
    def analyze_date_range(self, start_date: date, end_date: date) -> List[QuickCoverage]:
        """Quick analyze coverage for a date range."""
        logger.info(f"🔍 Quick analyzing FirstRate coverage from {start_date} to {end_date}")
        
        coverage_data = []
        current_date = start_date
        
        while current_date <= end_date:
            logger.info(f"📅 Quick analyzing {current_date}")
            
            day_coverage = self._analyze_date_files(current_date)
            coverage_data.append(day_coverage)
            
            current_date += timedelta(days=1)
        
        return coverage_data
    
    def print_report(self, coverage_data: List[QuickCoverage]):
        """Print a human-readable coverage report."""
        print("\n" + "="*80)
        print(f"🚀 FirstRate Quick Coverage Report")
        print(f"📅 Period: {coverage_data[0].date} to {coverage_data[-1].date}")
        print("="*80)
        
        # Summary stats
        trading_days = [d for d in coverage_data if d.is_trading_day]
        non_trading_days = [d for d in coverage_data if not d.is_trading_day]
        
        avg_symbols = sum(d.symbols_with_files for d in trading_days) / len(trading_days) if trading_days else 0
        max_symbols = max((d.symbols_with_files for d in trading_days), default=0)
        min_symbols = min((d.symbols_with_files for d in trading_days), default=0)
        total_size_gb = sum(d.file_size_mb for d in coverage_data) / 1024
        
        print(f"\n📊 SUMMARY STATISTICS:")
        print(f"  • Total Days Analyzed: {len(coverage_data)}")
        print(f"  • Trading Days: {len(trading_days)}")
        print(f"  • Non-Trading Days: {len(non_trading_days)}")
        print(f"  • Days with Data: {len([d for d in trading_days if d.symbols_with_files > 0])}")
        print(f"  • Days without Data: {len([d for d in trading_days if d.symbols_with_files == 0])}")
        print(f"  • Avg Symbols/Trading Day: {avg_symbols:.1f}")
        print(f"  • Max Symbols (Single Day): {max_symbols:,}")
        print(f"  • Min Symbols (Single Day): {min_symbols:,}")
        print(f"  • Total Data Size: {total_size_gb:.2f} GB")
        
        print(f"\n📈 DAILY BREAKDOWN:")
        print(f"{'Date':<12} {'Trading':<8} {'Symbols':<8} {'Files':<8} {'Size(MB)':<10} {'Status'}")
        print("-" * 70)
        
        for day in coverage_data:
            trading_flag = "✅" if day.is_trading_day else "⛔"
            status = "✅ Good" if day.symbols_with_files > 0 else ("❌ Missing" if day.is_trading_day else "➖ Holiday")
            
            print(f"{day.date:<12} {trading_flag:<8} {day.symbols_with_files:<8,} {day.total_file_count:<8,} {day.file_size_mb:<10.1f} {status}")
        
        # Show symbols by letter for recent trading days
        if trading_days:
            print(f"\n📝 SYMBOL DISTRIBUTION BY LETTER (Most Recent Trading Days):")
            recent_days = [d for d in trading_days if d.symbols_with_files > 0][-5:]
            
            if recent_days:
                all_letters = set()
                for day in recent_days:
                    all_letters.update(day.symbols_by_letter.keys())
                
                print(f"{'Letter':<8}", end="")
                for day in recent_days:
                    print(f"{day.date:<12}", end="")
                print()
                print("-" * (8 + 12 * len(recent_days)))
                
                for letter in sorted(all_letters):
                    print(f"{letter:<8}", end="")
                    for day in recent_days:
                        count = day.symbols_by_letter.get(letter, 0)
                        print(f"{count:<12,}", end="")
                    print()
        
        print("\n" + "="*80)

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Generate FirstRate quick coverage report")
    parser.add_argument("--days", type=int, default=30, help="Number of days to analyze")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--data-path", type=str, default="/mnt/d/ats-data/minute-bars/firstrate",
                       help="Path to FirstRate minute bar data")
    
    args = parser.parse_args()
    
    # Set date range
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    else:
        end_date = date.today()
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    else:
        start_date = end_date - timedelta(days=args.days)
    
    # Generate report
    analyzer = FirstRateQuickAnalyzer(data_path=args.data_path)
    coverage_data = analyzer.analyze_date_range(start_date, end_date)
    
    # Print to console
    analyzer.print_report(coverage_data)

if __name__ == "__main__":
    main()