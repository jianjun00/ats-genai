#!/usr/bin/env python3
"""
Test AAPL Hourly Training Data Generation - Small Scale

Generate a small sample to test the hourly training data approach with datetime features.
"""

import asyncio
import sys
import os
import json
import asyncpg
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
import logging

# Add src to path
sys.path.append('src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generate_hourly_test_data(symbol: str = "AAPL", days: int = 30) -> pd.DataFrame:
    """Generate small test dataset with hourly intervals."""
    
    start_date = date.today() - timedelta(days=days)
    end_date = date.today()
    
    logger.info(f"Generating {days} days of hourly data for {symbol}")
    logger.info(f"Date range: {start_date} to {end_date}")
    
    # Generate hourly datetime range (market hours only)
    datetimes = []
    current = datetime.combine(start_date, datetime.min.time().replace(hour=9))
    end_datetime = datetime.combine(end_date, datetime.min.time().replace(hour=16))
    
    while current <= end_datetime:
        # Only market hours (9 AM to 4 PM, Monday-Friday)
        if current.weekday() < 5 and 9 <= current.hour <= 16:
            datetimes.append(current)
        current += timedelta(hours=1)
    
    logger.info(f"Generated {len(datetimes)} hourly timestamps")
    
    # Generate synthetic data
    rows = []
    base_price = 150.0
    current_price = base_price
    
    for i, dt in enumerate(datetimes):
        # Random walk price movement
        price_change = np.random.normal(0, 0.02)
        current_price = max(50.0, current_price * (1 + price_change))
        
        # Generate OHLCV with proper relationships
        volatility = 0.015
        
        # Generate open first
        open_price = current_price * (1 + np.random.normal(0, volatility / 2))
        
        # Generate high and low ensuring proper OHLC relationships
        price_range = [open_price, current_price]
        base_high = max(price_range) * (1 + abs(np.random.normal(0, volatility)))
        base_low = min(price_range) * (1 - abs(np.random.normal(0, volatility)))
        
        # Ensure high is highest and low is lowest
        high = max(base_high, open_price, current_price)
        low = min(base_low, open_price, current_price)
        close = current_price
        volume = int(1000000 * (1 + np.random.exponential(0.5)))
        
        # Simple technical indicators
        sma_20 = current_price + np.random.normal(0, 5)  # Simplified
        ema_12 = current_price + np.random.normal(0, 3)
        ema_26 = current_price + np.random.normal(0, 4)
        
        volatility_est = abs(np.random.normal(5, 2))
        etop = sma_20 + (2 * volatility_est)  # Envelope Top (should be > price)
        ebot = sma_20 - (2 * volatility_est)  # Envelope Bottom (should be < price)
        
        # Ensure envelope makes sense relative to current price
        if etop <= close:
            etop = close + np.random.uniform(5, 15)  # Force etop above price
        if ebot >= close:
            ebot = close - np.random.uniform(5, 15)  # Force ebot below price
        pldot = np.random.normal(0, 50)  # Simplified momentum
        
        # Multi-timeframe (simplified)
        tf_5m_high = high * (1 + np.random.uniform(0, 0.005))
        tf_5m_low = low * (1 - np.random.uniform(0, 0.005))
        tf_5m_close = close * (1 + np.random.normal(0, 0.002))
        
        tf_15m_high = high * (1 + np.random.uniform(0, 0.01))
        tf_15m_low = low * (1 - np.random.uniform(0, 0.01))
        tf_15m_close = close * (1 + np.random.normal(0, 0.005))
        
        # Returns (simplified)
        return_1d = np.random.normal(0, 0.02)
        return_5d = np.random.normal(0, 0.05)
        return_20d = np.random.normal(0, 0.1)
        
        volatility_20d = abs(np.random.normal(15, 5))
        volume_sma_20 = volume * (1 + np.random.normal(0, 0.2))
        volume_ratio = volume / volume_sma_20 if volume_sma_20 > 0 else 1.0
        
        # Create row with datetime as feature
        row = {
            'datetime': dt.isoformat(),
            'symbol': symbol,
            'timestamp': dt.timestamp(),  # Unix timestamp
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'hour': dt.hour,
            'weekday': dt.weekday(),
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': volume,
            'sma_20': round(sma_20, 2),
            'ema_12': round(ema_12, 2),
            'ema_26': round(ema_26, 2),
            'etop': round(etop, 2),
            'ebot': round(ebot, 2),
            'pldot': round(pldot, 2),
            '5m_high': round(tf_5m_high, 2),
            '5m_low': round(tf_5m_low, 2),
            '5m_close': round(tf_5m_close, 2),
            '15m_high': round(tf_15m_high, 2),
            '15m_low': round(tf_15m_low, 2),
            '15m_close': round(tf_15m_close, 2),
            '1h_high': round(high, 2),
            '1h_low': round(low, 2),
            '1h_close': round(close, 2),
            'return_1d': round(return_1d, 4),
            'return_5d': round(return_5d, 4),
            'return_20d': round(return_20d, 4),
            'volatility_20d': round(volatility_20d, 2),
            'volume_sma_20': int(volume_sma_20),
            'volume_ratio': round(volume_ratio, 4)
        }
        
        rows.append(row)
    
    df = pd.DataFrame(rows)
    logger.info(f"Generated {len(df):,} hourly training rows with {len(df.columns)} features")
    
    return df


async def register_test_training_dataset(df: pd.DataFrame, csv_file: Path, metadata_file: Path) -> int:
    """Register test training dataset in database."""
    
    # Connect directly to database
    db_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"
    conn = await asyncpg.connect(db_url)
    
    try:
        # Calculate file size
        csv_size_mb = csv_file.stat().st_size / (1024 * 1024)
        
        # Create run record
        run_query = """
        INSERT INTO dev_runs (
            run_type, status, start_time, end_time, created_by, error_message, parameters
        ) VALUES ($1, $2, $3, $4, $5, $6, $7) 
        RETURNING id
        """
        
        now = datetime.now()
        run_parameters = {
            "symbol": "AAPL",
            "data_format": "one_row_per_hour",
            "test_dataset": True,
            "num_rows": len(df),
            "num_features": len(df.columns),
            "generation_method": "test_hourly_training_data"
        }
        
        run_id = await conn.fetchval(
            run_query,
            "test_hourly_training_data_generation",
            "completed",
            now,
            now,
            "test_hourly_training_data",
            None,
            json.dumps(run_parameters)
        )
        
        logger.info(f"📝 Created run record: {run_id}")
        
        # Create training dataset record
        dataset_query = """
        INSERT INTO dev_training_datasets (
            dataset_name, run_id, total_sequences, sequence_length, feature_count, label_count,
            symbols, date_range_start, date_range_end, data_quality_score, feature_completeness,
            label_completeness, generation_duration_seconds, file_size_mb, data_sources, status,
            features_file_path, labels_file_path, metadata_file_path, feature_metadata,
            technical_indicators, prediction_horizon, created_by, generation_parameters
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24
        ) RETURNING id
        """
        
        dataset_name = f"aapl_test_hourly_training_{now.strftime('%Y%m%d_%H%M%S')}"
        
        datetime_features = ['datetime', 'timestamp', 'year', 'month', 'day', 'hour', 'weekday']
        technical_indicators = ['sma_20', 'ema_12', 'ema_26', 'etop', 'ebot', 'pldot']
        multi_timeframe = ['5m_high', '5m_low', '5m_close', '15m_high', '15m_low', '15m_close', '1h_high', '1h_low', '1h_close']
        
        dataset_id = await conn.fetchval(
            dataset_query,
            dataset_name,
            run_id,
            len(df),  # total_sequences (rows)
            1,  # sequence_length (each row is independent)
            len(df.columns),  # feature_count
            0,  # label_count (no labels)
            ["AAPL"],  # symbols array
            date.today() - timedelta(days=30),  # date_range_start
            date.today(),  # date_range_end
            1.0,  # data_quality_score
            1.0,  # feature_completeness
            1.0,  # label_completeness
            0,    # generation_duration_seconds
            csv_size_mb,  # file_size_mb
            ["test_hourly_generator"],  # data_sources array
            "completed",  # status
            str(csv_file),  # features_file_path
            "",  # labels_file_path (no labels)
            str(metadata_file),  # metadata_file_path
            json.dumps({
                "data_format": "one_row_per_hour",
                "datetime_features": datetime_features,
                "feature_names": list(df.columns),
                "technical_indicators": technical_indicators,
                "multi_timeframe_features": multi_timeframe,
                "generation_method": "test_synthetic_with_datetime_features"
            }),  # feature_metadata
            ",".join(technical_indicators),  # technical_indicators
            0,  # prediction_horizon (no predictions)
            "test_hourly_training_callback",  # created_by
            json.dumps({
                "symbol": "AAPL",
                "data_format": "one_row_per_hour",
                "test_dataset": True,
                "datetime_as_features": True,
                "technical_indicators": technical_indicators,
                "multi_timeframe": True,
                "csv_file": str(csv_file)
            })  # generation_parameters
        )
        
        logger.info(f"\n✅ Successfully registered test hourly training dataset!")
        logger.info(f"   Run ID: {run_id}")
        logger.info(f"   Dataset ID: {dataset_id}")
        logger.info(f"   Dataset name: {dataset_name}")
        logger.info(f"   Rows: {len(df):,}")
        logger.info(f"   Features: {len(df.columns)}")
        logger.info(f"   File size: {csv_size_mb:.1f} MB")
        logger.info(f"   CSV file: {csv_file}")
        logger.info(f"   Metadata file: {metadata_file}")
        
        return dataset_id
        
    finally:
        await conn.close()


async def main():
    """Generate test AAPL hourly training data."""
    
    logger.info("🚀 Starting AAPL test hourly training data generation")
    
    # Generate small test dataset (30 days)
    df = generate_hourly_test_data(symbol="AAPL", days=30)
    
    # Create output directory
    output_dir = Path("/mnt/d/ats-data/training/test_hourly")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save data
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_filename = f"aapl_test_hourly_{timestamp}"
    
    # Save as CSV
    csv_file = output_dir / f"{base_filename}.csv"
    df.to_csv(csv_file, index=False)
    logger.info(f"Saved CSV: {csv_file} ({csv_file.stat().st_size / 1024 / 1024:.1f} MB)")
    
    # Create metadata
    datetime_features = ['datetime', 'timestamp', 'year', 'month', 'day', 'hour', 'weekday']
    technical_indicators = ['sma_20', 'ema_12', 'ema_26', 'etop', 'ebot', 'pldot']
    multi_timeframe = ['5m_high', '5m_low', '5m_close', '15m_high', '15m_low', '15m_close', '1h_high', '1h_low', '1h_close']
    
    metadata = {
        'symbol': 'AAPL',
        'num_rows': len(df),
        'num_features': len(df.columns),
        'feature_names': list(df.columns),
        'date_range': [str(date.today() - timedelta(days=30)), str(date.today())],
        'datetime_features': datetime_features,
        'technical_indicators': technical_indicators,
        'multi_timeframe_features': multi_timeframe,
        'data_format': 'one_row_per_hour',
        'test_dataset': True,
        'generation_timestamp': datetime.now().isoformat(),
        'csv_file': str(csv_file)
    }
    
    # Save metadata
    metadata_file = output_dir / f"{base_filename}_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Saved metadata: {metadata_file}")
    
    # Register in database
    dataset_id = await register_test_training_dataset(df, csv_file, metadata_file)
    
    # Show sample data
    logger.info(f"\n📊 SAMPLE DATA (first 5 rows):")
    print(df.head())
    
    logger.info(f"\n🎯 TEST HOURLY TRAINING DATA GENERATION COMPLETE!")
    logger.info(f"   Dataset ID: {dataset_id}")
    logger.info(f"   Format: One row per hour (not sequences)")
    logger.info(f"   Datetime features: {datetime_features}")
    logger.info(f"   Technical indicators: {technical_indicators}")
    logger.info(f"   Multi-timeframe features: {multi_timeframe}")
    logger.info(f"   Total rows: {len(df):,}")
    logger.info(f"   Total features: {len(df.columns)}")
    logger.info(f"   Available at: http://localhost:3000/training-eda")
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)