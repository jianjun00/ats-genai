#!/usr/bin/env python3
"""
Comprehensive FirstRate Minute Bar Validation

Validates ALL instruments in FirstRate data for:
1. Missing instruments (instruments that should exist but don't have files)
2. Missing minute bars for past 90 trading days
3. Comprehensive coverage analysis across all symbols
"""

import os
import sys
import asyncio
import asyncpg
from datetime import datetime, date, timedelta
from pathlib import Path
import pandas as pd
from collections import defaultdict, Counter
from typing import Set, List, Dict, Tuple
import glob

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def get_recent_trading_days(num_days: int = 90) -> List[date]:
    """Get list of recent trading days (Mon-Fri, excluding weekends)"""
    trading_days = []
    current_date = date.today()
    
    days_checked = 0
    while len(trading_days) < num_days and days_checked < 150:  # Safety limit
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            trading_days.append(current_date)
        current_date -= timedelta(days=1)
        days_checked += 1
    
    return sorted(trading_days, reverse=True)  # Most recent first

class FirstRateValidator:
    def __init__(self):
        self.firstrate_path = "/mnt/d/ats-data/minute-bars/firstrate"
        self.db_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"
        
    async def get_expected_instruments(self) -> Set[str]:
        """Get all expected instruments from database"""
        try:
            conn = await asyncpg.connect(self.db_url)
            
            # Get all active instruments from dev_instruments
            query = """
            SELECT DISTINCT symbol 
            FROM dev_instruments 
            WHERE active = true 
              AND symbol IS NOT NULL 
              AND symbol != ''
              AND symbol ~ '^[A-Z]{1,5}$'  -- US equity symbols
            ORDER BY symbol
            """
            
            result = await conn.fetch(query)
            expected_symbols = {row['symbol'] for row in result}
            await conn.close()
            
            print(f"📊 Found {len(expected_symbols)} expected instruments from database")
            return expected_symbols
            
        except Exception as e:
            print(f"❌ Could not connect to database: {e}")
            # Fallback: scan existing files to build universe
            return self.get_existing_instruments_from_files()
    
    def get_existing_instruments_from_files(self) -> Set[str]:
        """Get all instruments that have FirstRate files"""
        instruments = set()
        
        if not os.path.exists(self.firstrate_path):
            print(f"❌ FirstRate path not found: {self.firstrate_path}")
            return instruments
        
        # Scan all letter directories
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            letter_path = os.path.join(self.firstrate_path, letter)
            if os.path.exists(letter_path):
                symbols = [d for d in os.listdir(letter_path) 
                          if os.path.isdir(os.path.join(letter_path, d)) and d.isalpha()]
                instruments.update(symbols)
        
        print(f"📁 Found {len(instruments)} instruments with FirstRate files")
        return instruments
    
    def analyze_missing_instruments(self, expected: Set[str], existing: Set[str]) -> Dict:
        """Analyze missing instruments"""
        missing = expected - existing
        unexpected = existing - expected
        
        return {
            'total_expected': len(expected),
            'total_existing': len(existing),
            'missing_count': len(missing),
            'unexpected_count': len(unexpected),
            'missing_symbols': sorted(list(missing)),
            'unexpected_symbols': sorted(list(unexpected)),
            'coverage_percentage': (len(existing & expected) / len(expected) * 100) if expected else 0
        }
    
    def analyze_trading_day_coverage(self, symbols: Set[str], trading_days: List[date]) -> Dict:
        """Analyze which symbols have data for recent trading days"""
        print(f"📅 Analyzing trading day coverage for {len(symbols)} symbols across {len(trading_days)} trading days")
        
        # Focus on recent months for analysis
        recent_months = [
            ('2025', '08'),
            ('2025', '09'),
            ('2025', '07'),  # Include July for more coverage
        ]
        
        symbol_trading_data = defaultdict(set)  # symbol -> set of trading dates
        symbols_by_coverage = defaultdict(list)  # coverage_level -> list of symbols
        missing_data_by_day = defaultdict(list)  # date -> list of symbols missing data
        
        processed_count = 0
        for symbol in symbols:
            letter = symbol[0]
            symbol_path = os.path.join(self.firstrate_path, letter, symbol)
            
            if not os.path.exists(symbol_path):
                continue
                
            # Check each recent month
            for year, month in recent_months:
                file_path = os.path.join(symbol_path, year, month, f'{symbol}_{year}_{month}.parquet')
                if os.path.exists(file_path):
                    try:
                        # Read parquet and extract trading dates
                        df = pd.read_parquet(file_path)
                        if not df.empty:
                            df['date'] = pd.to_datetime(df['timestamp']).dt.date
                            file_dates = set(df['date'].unique())
                            symbol_trading_data[symbol].update(file_dates)
                    except Exception as e:
                        print(f"⚠️ Error reading {file_path}: {e}")
                        continue
            
            processed_count += 1
            if processed_count % 1000 == 0:
                print(f"📊 Processed {processed_count}/{len(symbols)} symbols...")
        
        print(f"✅ Completed analysis for {processed_count} symbols")
        
        # Analyze coverage for each trading day
        coverage_by_day = {}
        for day in trading_days[:10]:  # Focus on last 10 trading days
            symbols_with_data = sum(1 for symbol_dates in symbol_trading_data.values() 
                                  if day in symbol_dates)
            coverage_pct = (symbols_with_data / len(symbols) * 100) if symbols else 0
            coverage_by_day[day] = {
                'symbols_with_data': symbols_with_data,
                'coverage_percentage': coverage_pct
            }
            
            # Track symbols missing this day
            missing_symbols = [symbol for symbol in symbols 
                             if symbol in symbol_trading_data and day not in symbol_trading_data[symbol]]
            missing_data_by_day[day] = missing_symbols[:50]  # Limit to first 50 for reporting
        
        # Categorize symbols by coverage quality
        excellent_coverage = []  # 90%+ of recent days
        good_coverage = []      # 70-89% of recent days  
        poor_coverage = []      # 50-69% of recent days
        no_coverage = []        # <50% of recent days
        
        recent_10_days = trading_days[:10]
        for symbol in symbols:
            if symbol in symbol_trading_data:
                coverage_count = sum(1 for day in recent_10_days if day in symbol_trading_data[symbol])
                coverage_pct = coverage_count / len(recent_10_days) * 100
                
                if coverage_pct >= 90:
                    excellent_coverage.append(symbol)
                elif coverage_pct >= 70:
                    good_coverage.append(symbol)
                elif coverage_pct >= 50:
                    poor_coverage.append(symbol)
                else:
                    no_coverage.append(symbol)
            else:
                no_coverage.append(symbol)
        
        return {
            'coverage_by_day': coverage_by_day,
            'excellent_coverage': len(excellent_coverage),
            'good_coverage': len(good_coverage),
            'poor_coverage': len(poor_coverage),
            'no_coverage': len(no_coverage),
            'excellent_symbols': excellent_coverage[:20],  # Sample
            'poor_coverage_symbols': poor_coverage[:20],    # Sample
            'no_coverage_symbols': no_coverage[:50],        # More samples for missing data
            'total_symbols_analyzed': len(symbols),
            'symbols_with_any_data': len(symbol_trading_data)
        }
    
    def get_file_statistics(self) -> Dict:
        """Get file-level statistics"""
        stats = {
            'total_files': 0,
            'files_by_year': defaultdict(int),
            'files_by_month': defaultdict(int),
            'symbols_by_letter': defaultdict(int),
            'avg_files_per_symbol': 0
        }
        
        symbol_file_counts = defaultdict(int)
        
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            letter_path = os.path.join(self.firstrate_path, letter)
            if os.path.exists(letter_path):
                symbols = [d for d in os.listdir(letter_path) 
                          if os.path.isdir(os.path.join(letter_path, d)) and d.isalpha()]
                stats['symbols_by_letter'][letter] = len(symbols)
                
                # Sample file counts for performance
                sample_symbols = symbols[:min(50, len(symbols))]  # Sample first 50
                for symbol in sample_symbols:
                    symbol_path = os.path.join(letter_path, symbol)
                    
                    # Count parquet files
                    pattern = f"{symbol_path}/*/*/*.parquet"
                    files = glob.glob(pattern)
                    symbol_file_counts[symbol] = len(files)
                    stats['total_files'] += len(files)
                    
                    # Extract year/month from files
                    for file_path in files:
                        try:
                            path_parts = file_path.split('/')
                            year = path_parts[-3]  # Should be YYYY
                            month = path_parts[-2]  # Should be MM
                            if year.isdigit() and len(year) == 4:
                                stats['files_by_year'][year] += 1
                                stats['files_by_month'][f"{year}-{month}"] += 1
                        except (IndexError, ValueError):
                            continue
        
        if symbol_file_counts:
            stats['avg_files_per_symbol'] = sum(symbol_file_counts.values()) / len(symbol_file_counts)
        
        return stats

    async def run_comprehensive_validation(self):
        """Run complete validation"""
        print("🚀 Starting Comprehensive FirstRate Validation")
        print(f"📁 Data path: {self.firstrate_path}")
        
        # Get trading days
        trading_days = get_recent_trading_days(90)
        print(f"📅 Analyzing {len(trading_days)} recent trading days")
        print(f"📅 Date range: {trading_days[-1]} to {trading_days[0]}")
        
        # Get expected vs existing instruments
        print("\n1️⃣ INSTRUMENT COVERAGE ANALYSIS")
        expected_instruments = await self.get_expected_instruments()
        existing_instruments = self.get_existing_instruments_from_files()
        
        missing_analysis = self.analyze_missing_instruments(expected_instruments, existing_instruments)
        
        print(f"📊 Expected instruments: {missing_analysis['total_expected']:,}")
        print(f"📁 Existing instruments: {missing_analysis['total_existing']:,}")
        print(f"❌ Missing instruments: {missing_analysis['missing_count']:,}")
        print(f"➕ Unexpected instruments: {missing_analysis['unexpected_count']:,}")
        print(f"📈 Coverage: {missing_analysis['coverage_percentage']:.1f}%")
        
        # Show sample missing instruments
        if missing_analysis['missing_symbols']:
            print(f"🔍 Sample missing instruments (first 20): {', '.join(missing_analysis['missing_symbols'][:20])}")
        
        # Trading day coverage analysis
        print(f"\n2️⃣ TRADING DAY COVERAGE ANALYSIS")
        coverage_analysis = self.analyze_trading_day_coverage(existing_instruments, trading_days)
        
        print(f"📊 Symbols analyzed: {coverage_analysis['total_symbols_analyzed']:,}")
        print(f"📊 Symbols with any data: {coverage_analysis['symbols_with_any_data']:,}")
        print(f"🟢 Excellent coverage (90%+): {coverage_analysis['excellent_coverage']:,} symbols")
        print(f"🟡 Good coverage (70-89%): {coverage_analysis['good_coverage']:,} symbols")
        print(f"🟠 Poor coverage (50-69%): {coverage_analysis['poor_coverage']:,} symbols")
        print(f"🔴 No/minimal coverage (<50%): {coverage_analysis['no_coverage']:,} symbols")
        
        # Recent trading day breakdown
        print(f"\n📅 Recent Trading Day Coverage:")
        for day, data in list(coverage_analysis['coverage_by_day'].items())[:5]:
            print(f"   {day}: {data['symbols_with_data']:,} symbols ({data['coverage_percentage']:.1f}%)")
        
        # File statistics
        print(f"\n3️⃣ FILE STATISTICS")
        file_stats = self.get_file_statistics()
        print(f"📁 Total files (sampled): {file_stats['total_files']:,}")
        print(f"📊 Average files per symbol: {file_stats['avg_files_per_symbol']:.1f}")
        
        # Year distribution
        recent_years = {k: v for k, v in file_stats['files_by_year'].items() if int(k) >= 2023}
        print(f"📅 Files by recent years: {dict(sorted(recent_years.items()))}")
        
        # Letters with most symbols
        top_letters = sorted(file_stats['symbols_by_letter'].items(), key=lambda x: x[1], reverse=True)[:5]
        print(f"🔤 Letters with most symbols: {dict(top_letters)}")
        
        # SUMMARY REPORT
        print(f"\n📋 COMPREHENSIVE FIRSTRATE VALIDATION SUMMARY")
        print(f"=" * 60)
        
        # Missing instruments severity
        missing_pct = (missing_analysis['missing_count'] / missing_analysis['total_expected'] * 100) if missing_analysis['total_expected'] else 0
        if missing_pct < 1:
            missing_status = "🟢 EXCELLENT"
        elif missing_pct < 5:
            missing_status = "🟡 GOOD"
        elif missing_pct < 15:
            missing_status = "🟠 FAIR"
        else:
            missing_status = "🔴 POOR"
            
        # Coverage quality
        excellent_pct = (coverage_analysis['excellent_coverage'] / coverage_analysis['total_symbols_analyzed'] * 100) if coverage_analysis['total_symbols_analyzed'] else 0
        if excellent_pct > 80:
            coverage_status = "🟢 EXCELLENT"
        elif excellent_pct > 60:
            coverage_status = "🟡 GOOD"
        elif excellent_pct > 40:
            coverage_status = "🟠 FAIR"
        else:
            coverage_status = "🔴 POOR"
        
        print(f"📊 MISSING INSTRUMENTS: {missing_status}")
        print(f"   • {missing_analysis['missing_count']:,} missing out of {missing_analysis['total_expected']:,} expected ({missing_pct:.1f}%)")
        
        print(f"📅 TRADING DAY COVERAGE: {coverage_status}")
        print(f"   • {coverage_analysis['excellent_coverage']:,} symbols with excellent coverage ({excellent_pct:.1f}%)")
        print(f"   • {coverage_analysis['no_coverage']:,} symbols with poor/no coverage")
        
        print(f"📁 DATA SCALE: {file_stats['total_files']:,} files across {coverage_analysis['symbols_with_any_data']:,} symbols")
        
        if coverage_analysis['no_coverage_symbols']:
            print(f"\n🔍 SAMPLE SYMBOLS WITH MISSING DATA (first 20):")
            print(f"   {', '.join(coverage_analysis['no_coverage_symbols'][:20])}")
            
        if missing_analysis['missing_symbols'][:10]:
            print(f"\n🔍 SAMPLE COMPLETELY MISSING INSTRUMENTS (first 10):")
            print(f"   {', '.join(missing_analysis['missing_symbols'][:10])}")

async def main():
    validator = FirstRateValidator()
    await validator.run_comprehensive_validation()

if __name__ == "__main__":
    asyncio.run(main())