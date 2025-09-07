#!/usr/bin/env python3
"""
Create sample minute-level test data for hourly training data generation tests.

This creates realistic minute-level OHLCV data in the expected Parquet format
that FileBasedMinuteManager can read.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging

def create_minute_test_data(symbol: str, start_date: datetime, end_date: datetime, base_path: Path):
    """Create realistic minute-level OHLCV data for testing."""

    # Create directory structure: symbol/year/month/
    for year in range(start_date.year, end_date.year + 1):
        for month in range(1, 13):
            month_start = datetime(year, month, 1)
            if month_start > end_date:
                break
            if month_start < start_date and month_start.replace(month=month+1 if month < 12 else 1, year=year+1 if month == 12 else year) <= start_date:
                continue

            symbol_dir = base_path / symbol / str(year) / f"{month:02d}"
            symbol_dir.mkdir(parents=True, exist_ok=True)

            # Generate minute data for this month
            minute_data = []

            # Generate trading days (Mon-Fri) for the month
            current_date = max(month_start, start_date)
            next_month = month + 1 if month < 12 else 1
            next_year = year if month < 12 else year + 1
            month_end = datetime(next_year, next_month, 1) - timedelta(days=1)
            month_end = min(month_end, end_date)

            base_price = 200.0 + np.random.normal(0, 10)  # Starting price around $200

            while current_date <= month_end:
                # Skip weekends
                if current_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
                    current_date += timedelta(days=1)
                    continue

                # Generate market hours: 9:30 AM to 4:00 PM = 6.5 hours = 390 minutes
                market_open = current_date.replace(hour=9, minute=30, second=0, microsecond=0)

                daily_open = base_price + np.random.normal(0, 2)
                daily_trend = np.random.normal(0, 0.1)  # Daily price trend

                for minute in range(390):  # 390 minutes in trading day
                    timestamp = market_open + timedelta(minutes=minute)

                    # Create realistic price movement
                    price_change = np.random.normal(0, 0.5) + daily_trend * (minute / 390)
                    current_price = daily_open + price_change

                    # Generate OHLC for this minute
                    minute_volatility = np.random.uniform(0.1, 1.0)
                    minute_high = current_price + minute_volatility * np.random.uniform(0, 0.5)
                    minute_low = current_price - minute_volatility * np.random.uniform(0, 0.5)
                    minute_open = current_price + np.random.uniform(-0.2, 0.2)
                    minute_close = current_price + np.random.uniform(-0.2, 0.2)

                    # Ensure OHLC logic is correct
                    minute_high = max(minute_high, minute_open, minute_close)
                    minute_low = min(minute_low, minute_open, minute_close)

                    # Generate realistic volume
                    base_volume = np.random.randint(100, 5000)
                    if 9.5 <= timestamp.hour + timestamp.minute/60 <= 10.5:  # Opening hour
                        volume = int(base_volume * np.random.uniform(1.5, 3.0))
                    elif 15.5 <= timestamp.hour + timestamp.minute/60 <= 16.0:  # Closing hour
                        volume = int(base_volume * np.random.uniform(1.2, 2.0))
                    else:
                        volume = int(base_volume * np.random.uniform(0.8, 1.5))

                    minute_data.append({
                        'timestamp': timestamp,
                        'open': round(minute_open, 2),
                        'high': round(minute_high, 2),
                        'low': round(minute_low, 2),
                        'close': round(minute_close, 2),
                        'volume': volume,
                        'vwap': round((minute_high + minute_low + minute_close) / 3, 2),
                        'trade_count': np.random.randint(10, 200),
                        'vendor': 'test_data',
                        'quality_score': 1.0
                    })

                    # Update base price for next minute
                    base_price = minute_close

                current_date += timedelta(days=1)

            # Save to Parquet file if we have data for this month
            if minute_data:
                df = pd.DataFrame(minute_data)
                df.set_index('timestamp', inplace=True)

                file_path = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
                df.to_parquet(file_path, engine='pyarrow')

                print(f"Created {len(minute_data)} minute bars for {symbol} {year}-{month:02d} -> {file_path}")

def create_test_dataset():
    """Create comprehensive test dataset for multiple symbols and date ranges."""

    base_path = Path(__file__).parent / "test_data" / "minute-files"
    base_path.mkdir(parents=True, exist_ok=True)

    # Create test data for multiple scenarios
    test_scenarios = [
        {
            'symbol': 'AAPL',
            'start_date': datetime(2025, 8, 1),
            'end_date': datetime(2025, 8, 31),  # Full month
            'description': 'Full month of AAPL data'
        },
        {
            'symbol': 'MSFT',
            'start_date': datetime(2025, 8, 15),
            'end_date': datetime(2025, 8, 25),  # Partial month
            'description': 'Partial month of MSFT data'
        },
        {
            'symbol': 'GOOGL',
            'start_date': datetime(2025, 7, 20),
            'end_date': datetime(2025, 8, 10),  # Cross-month
            'description': 'Cross-month GOOGL data'
        }
    ]

    print("Creating minute-level test data...")
    for scenario in test_scenarios:
        print(f"\n{scenario['description']}")
        create_minute_test_data(
            scenario['symbol'],
            scenario['start_date'],
            scenario['end_date'],
            base_path
        )

    print(f"\nTest data created in: {base_path}")
    print("\nFile structure:")
    for item in sorted(base_path.rglob("*")):
        if item.is_file():
            relative_path = item.relative_to(base_path)
            print(f"  {relative_path}")

if __name__ == "__main__":
    create_test_dataset()