#!/usr/bin/env python3
"""
FirstRate Trading Days Validation

Validates FirstRate minute bar coverage specifically for trading days (weekdays only).
Focuses on T-0, T-1, T-2, etc. analysis for the past 30 trading days.
"""

import os
import sys
import pandas as pd
import asyncio
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-data/src')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class TradingDayMetrics:
    """Trading day coverage metrics"""
    vendor: str
    total_symbols: int
    symbols_with_t0_data: int  # Most recent trading day
    symbols_with_t1_data: int  # Previous trading day
    symbols_with_t2_data: int  # 2 trading days ago
    symbols_with_t3_data: int  # 3 trading days ago
    symbols_with_t4_data: int  # 4 trading days ago
    symbols_with_recent_5_days: int  # Any of last 5 trading days
    symbols_with_recent_10_days: int # Any of last 10 trading days
    trading_days: List[date]
    validation_timestamp: datetime

    @property
    def t0_coverage_percentage(self) -> float:
        return (self.symbols_with_t0_data / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

    @property
    def t1_coverage_percentage(self) -> float:
        return (self.symbols_with_t1_data / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

    @property
    def recent_5_coverage_percentage(self) -> float:
        return (self.symbols_with_recent_5_days / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

def get_trading_days(num_days: int = 30) -> List[date]:
    """Get recent trading days (weekdays only, excluding weekends)"""
    trading_days = []
    current_date = date.today()
    
    days_checked = 0
    while len(trading_days) < num_days and days_checked < 60:  # Safety limit
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            trading_days.append(current_date)
        current_date -= timedelta(days=1)
        days_checked += 1
    
    return sorted(trading_days, reverse=True)  # Most recent first

class FirstRateTradingDaysValidator:
    """FirstRate trading days validator"""
    
    def __init__(self, data_path: str = "/mnt/d/ats-data/minute-bars/firstrate"):
        self.data_path = Path(data_path)
        
        # Sample of symbols to check for faster validation
        self.sample_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
            'SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'XLK', 'XLF', 'XLE', 'XLV', 'XLI',
            'JPM', 'BAC', 'WFC', 'C', 'GS', 'MS', 'V', 'MA', 'PYPL', 'SQ'
        ]
    
    def get_all_symbols_fast(self) -> List[str]:
        """Get all symbols quickly by scanning directory structure"""
        all_symbols = []
        
        # Scan A-Z directories
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            letter_path = self.data_path / letter
            if letter_path.exists():
                try:
                    symbols = [d.name for d in letter_path.iterdir() 
                             if d.is_dir() and not d.name.isdigit() and len(d.name) <= 5]
                    all_symbols.extend(symbols)
                except Exception as e:
                    logger.debug(f"Error scanning {letter_path}: {e}")
        
        return sorted(all_symbols)
    
    def check_symbol_trading_coverage(self, symbol: str, trading_days: List[date]) -> Dict:
        """Check trading day coverage for a single symbol"""
        symbol_path = self.data_path / symbol[0] / symbol
        if not symbol_path.exists():
            return {
                'symbol': symbol,
                'exists': False,
                'trading_dates': set(),
                't0': False, 't1': False, 't2': False, 't3': False, 't4': False,
                'recent_5': False, 'recent_10': False
            }
        
        symbol_trading_dates = set()
        
        # Check recent months (August and September 2025)
        for month in ['08', '09']:
            month_path = symbol_path / f'2025/{month}' / f'{symbol}_2025_{month}.parquet'
            if month_path.exists():
                try:
                    df = pd.read_parquet(month_path)
                    df['date'] = pd.to_datetime(df['timestamp']).dt.date
                    
                    # Get unique trading dates from this file
                    file_dates = set(df['date'].unique())
                    symbol_trading_dates.update(file_dates)
                    
                except Exception as e:
                    logger.debug(f"Error reading {month_path}: {e}")
        
        # Check coverage flags
        t0_date = trading_days[0] if len(trading_days) > 0 else None
        t1_date = trading_days[1] if len(trading_days) > 1 else None
        t2_date = trading_days[2] if len(trading_days) > 2 else None
        t3_date = trading_days[3] if len(trading_days) > 3 else None
        t4_date = trading_days[4] if len(trading_days) > 4 else None
        
        has_t0 = t0_date in symbol_trading_dates if t0_date else False
        has_t1 = t1_date in symbol_trading_dates if t1_date else False
        has_t2 = t2_date in symbol_trading_dates if t2_date else False
        has_t3 = t3_date in symbol_trading_dates if t3_date else False
        has_t4 = t4_date in symbol_trading_dates if t4_date else False
        
        # Check recent coverage
        recent_5_found = any(td in symbol_trading_dates for td in trading_days[:5])
        recent_10_found = any(td in symbol_trading_dates for td in trading_days[:10])
        
        return {
            'symbol': symbol,
            'exists': True,
            'trading_dates': symbol_trading_dates,
            't0': has_t0, 't1': has_t1, 't2': has_t2, 't3': has_t3, 't4': has_t4,
            'recent_5': recent_5_found, 'recent_10': recent_10_found
        }
    
    def validate_trading_days_coverage(self, use_sample: bool = True) -> TradingDayMetrics:
        """Validate trading day coverage for FirstRate data"""
        logger.info("🚀 Starting FirstRate trading days validation")
        
        # Get trading days
        trading_days = get_trading_days(30)
        logger.info(f"📅 Trading days to check: {len(trading_days)}")
        logger.info(f"📅 T-0: {trading_days[0]}, T-1: {trading_days[1]}, T-2: {trading_days[2]}")
        
        # Choose symbols to check
        if use_sample:
            symbols_to_check = self.sample_symbols
            logger.info(f"📊 Using sample of {len(symbols_to_check)} key symbols")
        else:
            symbols_to_check = self.get_all_symbols_fast()
            logger.info(f"📊 Checking all {len(symbols_to_check)} symbols")
        
        # Initialize counters
        total_symbols = 0
        symbols_with_t0 = 0
        symbols_with_t1 = 0
        symbols_with_t2 = 0
        symbols_with_t3 = 0
        symbols_with_t4 = 0
        symbols_with_recent_5 = 0
        symbols_with_recent_10 = 0
        
        # Check each symbol
        for i, symbol in enumerate(symbols_to_check, 1):
            if i % 10 == 0:
                logger.info(f"📈 Progress: {i}/{len(symbols_to_check)} symbols checked")
            
            coverage = self.check_symbol_trading_coverage(symbol, trading_days)
            
            if coverage['exists']:
                total_symbols += 1
                if coverage['t0']: symbols_with_t0 += 1
                if coverage['t1']: symbols_with_t1 += 1
                if coverage['t2']: symbols_with_t2 += 1
                if coverage['t3']: symbols_with_t3 += 1
                if coverage['t4']: symbols_with_t4 += 1
                if coverage['recent_5']: symbols_with_recent_5 += 1
                if coverage['recent_10']: symbols_with_recent_10 += 1
                
                # Log details for key symbols
                if symbol in ['AAPL', 'SPY', 'QQQ', 'MSFT', 'TSLA']:
                    status = "✅" if coverage['recent_5'] else "❌"
                    latest_date = max(coverage['trading_dates']) if coverage['trading_dates'] else "None"
                    logger.info(f"   {status} {symbol}: T-0:{coverage['t0']}, T-1:{coverage['t1']}, "
                               f"T-2:{coverage['t2']}, Latest:{latest_date}")
        
        # Create metrics
        metrics = TradingDayMetrics(
            vendor='firstrate',
            total_symbols=total_symbols,
            symbols_with_t0_data=symbols_with_t0,
            symbols_with_t1_data=symbols_with_t1,
            symbols_with_t2_data=symbols_with_t2,
            symbols_with_t3_data=symbols_with_t3,
            symbols_with_t4_data=symbols_with_t4,
            symbols_with_recent_5_days=symbols_with_recent_5,
            symbols_with_recent_10_days=symbols_with_recent_10,
            trading_days=trading_days,
            validation_timestamp=datetime.now()
        )
        
        return metrics
    
    def print_detailed_report(self, metrics: TradingDayMetrics):
        """Print detailed trading days coverage report"""
        logger.info("📊 FirstRate Trading Days Coverage Report:")
        logger.info(f"   📈 Total symbols analyzed: {metrics.total_symbols}")
        logger.info(f"   📅 Trading days period: {len(metrics.trading_days)} days")
        
        logger.info("📊 Coverage by Trading Day:")
        logger.info(f"   📅 T-0 ({metrics.trading_days[0]}): {metrics.symbols_with_t0_data} symbols ({metrics.t0_coverage_percentage:.1f}%)")
        logger.info(f"   📅 T-1 ({metrics.trading_days[1]}): {metrics.symbols_with_t1_data} symbols ({metrics.t1_coverage_percentage:.1f}%)")
        logger.info(f"   📅 T-2 ({metrics.trading_days[2]}): {metrics.symbols_with_t2_data} symbols ({metrics.symbols_with_t2_data/metrics.total_symbols*100:.1f}%)")
        logger.info(f"   📅 T-3 ({metrics.trading_days[3]}): {metrics.symbols_with_t3_data} symbols ({metrics.symbols_with_t3_data/metrics.total_symbols*100:.1f}%)")
        logger.info(f"   📅 T-4 ({metrics.trading_days[4]}): {metrics.symbols_with_t4_data} symbols ({metrics.symbols_with_t4_data/metrics.total_symbols*100:.1f}%)")
        
        logger.info("📊 Recent Period Coverage:")
        logger.info(f"   📊 Last 5 trading days: {metrics.symbols_with_recent_5_days} symbols ({metrics.recent_5_coverage_percentage:.1f}%)")
        logger.info(f"   📊 Last 10 trading days: {metrics.symbols_with_recent_10_days} symbols ({metrics.symbols_with_recent_10_days/metrics.total_symbols*100:.1f}%)")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="FirstRate Trading Days Validation")
    parser.add_argument("--full", action="store_true", help="Check all symbols (slower)")
    parser.add_argument("--sample", action="store_true", default=True, help="Check sample symbols only (default)")
    args = parser.parse_args()
    
    use_sample = not args.full
    
    validator = FirstRateTradingDaysValidator()
    metrics = validator.validate_trading_days_coverage(use_sample=use_sample)
    
    validator.print_detailed_report(metrics)
    
    print("\n" + "="*60)
    print("FirstRate Trading Days Coverage Summary")
    print("="*60)
    
    print(f"Analysis Type: {'Sample' if use_sample else 'Full'} ({metrics.total_symbols} symbols)")
    print(f"T-0 Coverage: {metrics.symbols_with_t0_data}/{metrics.total_symbols} ({metrics.t0_coverage_percentage:.1f}%)")
    print(f"T-1 Coverage: {metrics.symbols_with_t1_data}/{metrics.total_symbols} ({metrics.t1_coverage_percentage:.1f}%)")
    print(f"Recent 5 Days: {metrics.symbols_with_recent_5_days}/{metrics.total_symbols} ({metrics.recent_5_coverage_percentage:.1f}%)")
    
    # Determine status
    if metrics.t0_coverage_percentage >= 80:
        print("\n✅ FirstRate T-0 coverage is good (≥80%)")
        status = 0
    elif metrics.recent_5_coverage_percentage >= 70:
        print(f"\n⚠️  FirstRate T-0 coverage is low ({metrics.t0_coverage_percentage:.1f}%) but recent coverage acceptable")
        status = 0
    else:
        print(f"\n❌ FirstRate coverage is insufficient (T-0: {metrics.t0_coverage_percentage:.1f}%, Recent 5: {metrics.recent_5_coverage_percentage:.1f}%)")
        status = 1
    
    print(f"\nMost Recent Trading Day: {metrics.trading_days[0]}")
    print(f"Analysis completed at: {metrics.validation_timestamp}")
    
    return status

if __name__ == "__main__":
    import sys
    sys.exit(main())