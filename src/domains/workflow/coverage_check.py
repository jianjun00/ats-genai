#!/usr/bin/env python3
"""
Quick FirstRate Minute Bar Coverage Check

Fast validation of FirstRate data for past 30 days covering key symbols only.
"""

import os
import sys
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import logging

# Add src to Python path
# Module is now part of src package - no path manipulation needed

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FirstRateQuickValidator:
    """Quick FirstRate data validator for key symbols"""
    
    def __init__(self, data_path: str = "/mnt/d/ats-data/minute-bars/firstrate"):
        self.data_path = Path(data_path)
        
        # Key symbols to check - major stocks and ETFs
        self.key_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
            'SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'XLK', 'XLF', 'XLE'
        ]
    
    def get_trading_days(self, num_days: int = 30) -> List[date]:
        """Get recent trading days (weekdays only)"""
        trading_days = []
        current_date = date.today()
        
        days_checked = 0
        while len(trading_days) < num_days and days_checked < 50:
            if current_date.weekday() < 5:  # Monday=0, Friday=4
                trading_days.append(current_date)
            current_date -= timedelta(days=1)
            days_checked += 1
        
        return sorted(trading_days, reverse=True)  # Most recent first
    
    def check_symbol_coverage(self, symbol: str, trading_days: List[date]) -> Dict:
        """Check coverage for a single symbol"""
        symbol_path = self.data_path / symbol[0] / symbol
        if not symbol_path.exists():
            return {
                'symbol': symbol,
                'exists': False,
                'recent_data': False,
                'coverage_days': 0,
                'total_records': 0,
                'months_available': [],
                'latest_date': None
            }
        
        months_available = []
        total_records = 0
        coverage_days = 0
        latest_date = None
        symbol_dates = set()
        
        # Check 2025 data for August and September
        for month in ['08', '09']:
            month_path = symbol_path / f'2025/{month}' / f'{symbol}_2025_{month}.parquet'
            if month_path.exists():
                months_available.append(f'2025-{month}')
                df = pd.read_parquet(month_path)
                df['date'] = pd.to_datetime(df['timestamp']).dt.date
                
                # Get unique dates in this file
                file_dates = set(df['date'].unique())
                symbol_dates.update(file_dates)
                
                # Count records for recent trading days
                recent_dates = set(trading_days[:10])  # Last 10 trading days
                recent_records = df[df['date'].isin(recent_dates)]
                total_records += len(recent_records)
                
                # Track latest date
                if file_dates:
                    file_latest = max(file_dates)
                    if latest_date is None or file_latest > latest_date:
                        latest_date = file_latest
                        
        recent_10_days = set(trading_days[:10])
        coverage_days = len(recent_10_days & symbol_dates)
        recent_data = coverage_days > 0
        
        return {
            'symbol': symbol,
            'exists': True,
            'recent_data': recent_data,
            'coverage_days': coverage_days,
            'total_records': total_records,
            'months_available': months_available,
            'latest_date': latest_date
        }
    
    def run_quick_validation(self) -> Dict:
        """Run quick validation on key symbols"""
        logger.info("🚀 Starting FirstRate quick coverage check")
        logger.info(f"📋 Checking {len(self.key_symbols)} key symbols")
        
        trading_days = self.get_trading_days(30)
        logger.info(f"📅 Recent trading days: {trading_days[:5]} ... ({len(trading_days)} total)")
        
        results = []
        
        for symbol in self.key_symbols:
            logger.info(f"🔍 Checking {symbol}...")
            coverage = self.check_symbol_coverage(symbol, trading_days)
            results.append(coverage)
            
            status = "✅" if coverage['recent_data'] else "❌"
            logger.info(f"   {status} {symbol}: {coverage['coverage_days']}/10 recent days, "
                       f"{coverage['total_records']:,} records, "
                       f"latest: {coverage['latest_date']}")
        
        # Summary
        symbols_with_data = sum(1 for r in results if r['exists'])
        symbols_with_recent = sum(1 for r in results if r['recent_data'])
        total_records = sum(r['total_records'] for r in results)
        
        summary = {
            'total_symbols_checked': len(self.key_symbols),
            'symbols_with_data': symbols_with_data,
            'symbols_with_recent_data': symbols_with_recent,
            'total_recent_records': total_records,
            'coverage_percentage': (symbols_with_recent / len(self.key_symbols)) * 100,
            'details': results
        }
        
        logger.info("📊 FirstRate Coverage Summary:")
        logger.info(f"   • Symbols checked: {summary['total_symbols_checked']}")
        logger.info(f"   • Symbols with data: {summary['symbols_with_data']}")
        logger.info(f"   • Symbols with recent data: {summary['symbols_with_recent_data']}")
        logger.info(f"   • Coverage percentage: {summary['coverage_percentage']:.1f}%")
        logger.info(f"   • Total recent records: {summary['total_recent_records']:,}")
        
        return summary

def main():
    """Main function"""
    validator = FirstRateQuickValidator()
    summary = validator.run_quick_validation()
    
    print("\n" + "="*60)
    print("FirstRate 30-Day Coverage Report")
    print("="*60)
    
    print(f"Coverage: {summary['symbols_with_recent_data']}/{summary['total_symbols_checked']} symbols ({summary['coverage_percentage']:.1f}%)")
    print(f"Total Records: {summary['total_recent_records']:,}")
    
    print("\nPer-Symbol Details:")
    for detail in summary['details']:
        status = "✅" if detail['recent_data'] else "❌"
        print(f"  {status} {detail['symbol']:6} | {detail['coverage_days']:2}/10 days | "
              f"{detail['total_records']:6,} records | Latest: {detail['latest_date']}")
    
    # Return appropriate exit code
    if summary['coverage_percentage'] >= 80:
        print("\n✅ FirstRate data coverage is acceptable (≥80%)")
        return 0
    else:
        print(f"\n⚠️  FirstRate data coverage is low ({summary['coverage_percentage']:.1f}% < 80%)")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())